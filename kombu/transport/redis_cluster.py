from contextlib import contextmanager
from time import time, sleep
from queue import Empty
from collections import defaultdict
from typing import Set, Dict, Optional, List
import random

from kombu.log import get_logger
from kombu.utils.encoding import bytes_to_str
from kombu.utils.eventio import READ, ERR
from kombu.utils.json import loads, dumps

from . import virtual
from .redis import (
    Channel as RedisChannel,
    MultiChannelPoller,
    MutexHeld,
    QoS as RedisQoS,
    Transport as RedisTransport,
    Mutex
)

try:
    import redis
    from redis.exceptions import MovedError, RedisClusterException, SlotNotCoveredError, AskError, TryAgainError, ClusterDownError, ConnectionError, TimeoutError
except ImportError:
    redis = None

logger = get_logger(__name__)


# Override this method to use other redis client
def create_redis_cluster_connection(hostname, port, password, ssl):
    params = {'skip_full_coverage_check': True, 'host': hostname, 'port': port, 'password': password}
    if ssl:
        params['ssl'] = True

    return redis.RedisCluster(**params)


class QoS(RedisQoS):
    def __init__(self, *args, **kwargs):
        super(QoS, self).__init__(*args, **kwargs)
        self._vrestore_count = defaultdict(int)

    def append(self, message, delivery_tag):
        delivery = message.delivery_info
        EX, RK = delivery['exchange'], delivery['routing_key']
        zadd_args = [{delivery_tag: time()}]

        # RK is queue
        unacked_index_key = self.unacked_index_key.format(queue=RK)
        unacked_key = self.unacked_key.format(queue=RK)

        with self.pipe_or_acquire() as pipe:
            pipe.zadd(unacked_index_key, *zadd_args) \
                .hset(unacked_key, delivery_tag,
                      dumps([message._raw, EX, RK])) \
                .execute()
            super(RedisQoS, self).append(message, delivery_tag)

    def restore_unacked(self, client=None):
        with self.channel.conn_or_acquire(client) as client:
            for [tag, message] in self._delivered.items():
                routing_key = message.delivery_info['routing_key']
                self.restore_by_tag(tag, client=client, queue=routing_key)
        self._delivered.clear()

    def ack(self, delivery_tag):
        # Message is not added to _delivered if no_ack is true
        if delivery_tag not in self._delivered:
            super(RedisQoS, self).ack(delivery_tag)
            return
        message = self._delivered[delivery_tag]
        routing_key = message.delivery_info['routing_key']
        self._remove_from_indices(delivery_tag, queue=routing_key).execute()
        super(RedisQoS, self).ack(delivery_tag)

    def reject(self, delivery_tag, requeue=False):
        message = self._delivered[delivery_tag]
        if requeue:
            routing_key = message.delivery_info['routing_key']
            self.restore_by_tag(delivery_tag, leftmost=True, queue=routing_key)
        self.ack(delivery_tag)

    def _remove_from_indices(self, delivery_tag, pipe=None, queue=''):
        assert queue

        unacked_index_key = self.unacked_index_key.format(queue=queue)
        unacked_key = self.unacked_key.format(queue=queue)

        with self.pipe_or_acquire(pipe) as pipe:
            return pipe.zrem(unacked_index_key, delivery_tag) \
                       .hdel(unacked_key, delivery_tag)

    def restore_visible(self, start=0, num=10, interval=100, queue=''):
        assert queue

        self._vrestore_count[queue] += 1
        if (self._vrestore_count[queue] - 1) % interval:
            return
        with self.channel.conn_or_acquire() as client:
            ceil = time() - self.visibility_timeout

            unacked_mutex_key = self.unacked_mutex_key.format(queue=queue)
            unacked_index_key = self.unacked_index_key.format(queue=queue)

            try:
                with Mutex(
                    client,
                    unacked_mutex_key,
                    self.unacked_mutex_expire,
                ):
                    visible = client.zrevrangebyscore(
                        unacked_index_key,
                        ceil,
                        0,
                        start=num and start,
                        num=num,
                        withscores=True
                    )

                    for tag, score in visible or []:
                        self.restore_by_tag(tag, client, queue=queue)
            except MutexHeld:
                pass

    def restore_by_tag(self, tag, client=None, leftmost=False, queue=''):
        assert queue

        unacked_key = self.unacked_key.format(queue=queue)

        with self.channel.conn_or_acquire(client) as client:
            with client.pipeline() as pipe:
                p, _, _ = self._remove_from_indices(
                    tag, pipe.hget(unacked_key, tag), queue=queue).execute()
            if p:
                M, EX, RK = loads(bytes_to_str(p))  # json is unicode
                self.channel._do_restore_message(M, EX, RK, client, leftmost)


class RedisNodeConnection():
    def __init__(self, queue, physical_queue):
        self.client = None
        self.in_poll = False
        self.queue = queue
        self.physical_queue = physical_queue

class ClusterPoller(MultiChannelPoller):
    def __init__(self):
        super().__init__()
        self._sock_to_fd = {}

    def _register(self, channel, client, conn, cmd):
        ident = (channel, client, conn, cmd)

        if ident in self._chan_to_sock:
            self._unregister(*ident)

        if not conn.client:
            tries = 0
            backoff = [0, 0.1, 0.2, 0.4]
            while True:
                if tries > 3:
                    raise ValueError('Cannot find node for key: {}'.format(conn.physical_queue))
                try:
                    if conn.physical_queue in channel.ask_errors:
                        ask_error = channel.ask_errors[conn.physical_queue]
                        node = channel.client.get_node(ask_error.host, ask_error.port)
                    else:
                        node = channel.client.get_node_from_key(conn.physical_queue)
                    if node:
                        break
                except Exception as e:
                    logger.error('Error while getting node from key', extra={"e": e, "key": conn.physical_queue})

                sleep(backoff[tries])
                channel.client.nodes_manager.initialize()
                tries += 1

            redis_connection = channel.client.get_redis_connection(node)
            conn.client = redis_connection.client()

        sock = conn.client.connection._sock
        self._fd_to_chan[sock.fileno()] = (channel, conn, cmd)
        self._chan_to_sock[ident] = sock
        self._sock_to_fd[sock] = sock.fileno()
        self.poller.register(sock, self.eventflags)
        logger.debug(f'registering to queue {conn.physical_queue}')

    def _unregister(self, channel, client, conn, cmd):
        sock = self._chan_to_sock[(channel, client, conn, cmd)]
        fd = self._sock_to_fd[sock]

        self.poller.unregister(sock)
        if conn.client:
            if conn.client.connection:
                # There might be pending BRPOP response on the connection, so we disconnect to ensure safety
                conn.client.connection.disconnect()
            conn.client.close()
            conn.client = None

        del self._fd_to_chan[fd]
        del self._chan_to_sock[(channel, client, conn, cmd)]
        del self._sock_to_fd[sock]

    def discard(self, channel):
        super().discard(channel)

        # Channel is being removed, unregister all connection belong to channel
        conns_to_unregister = [conn for conn in self._chan_to_sock if conn[0] == channel]
        for conn in conns_to_unregister:
            self._unregister(*conn)

    def _register_BRPOP(self, channel):
        conns = self._get_conns_for_channel(channel)

        for conn in conns:
            ident = (channel, channel.client, conn, 'BRPOP')

            if (ident not in self._chan_to_sock):
                try:
                    self._register(*ident)
                except Exception as e:
                    logger.error('Error while registering BRPOP', extra={"e": e, "key": conn.physical_queue})

        channel._brpop_start()

    def on_poll_init(self, poller):
        self.poller = poller
        for channel in self._channels:
            for queue in channel.active_queues:
                return channel.qos.restore_visible(
                    num=channel.unacked_restore_limit,
                    queue=queue,
                )

    def maybe_restore_messages(self):
        for channel in self._channels:
            for queue in channel.active_queues:
                return channel.qos.restore_visible(
                    num=channel.unacked_restore_limit,
                    queue=queue,
                )

    def _get_conns_for_channel(self, channel):
        result = []
        conns = [conn for _, _, conn, _ in self._chan_to_sock]

        physical_queues = channel.get_physical_queues(channel.active_queues)

        for queue_name, physical_queue in physical_queues.items():
            for physical_queue_name in physical_queue.alive_queues():
                try:
                    conn = next(x for x in conns if x.queue == queue_name and x.physical_queue == physical_queue_name)
                    conns.remove(conn)
                except StopIteration:
                    conn = RedisNodeConnection(queue_name, physical_queue_name)
                result.append(conn)

        return result

    def handle_event(self, fileno, event):
        if event & READ:
            return self.on_readable(fileno)
        elif event & ERR:
            chan, conn, cmd = self._fd_to_chan[fileno]
            chan._poll_error(cmd, conn)

    def on_readable(self, fileno):
        try:
            chan, conn, cmd = self._fd_to_chan[fileno]
        except KeyError:
            self.poller.unregister(fileno)
            return

        if chan.qos.can_consume():
            return chan.handlers[cmd](**{'conn': conn})


class RedisClusterConnection():
    connections = {}
    connection_to_key = {}
    refcounts = {}

    @classmethod
    def get_connection(cls, host, port, password, ssl):
        key = (host, port, password, ssl)
        if key not in cls.connections:
            connection = create_redis_cluster_connection(host, port, password, ssl)
            cls.connections[key] = connection
            cls.connection_to_key[connection] = key
            cls.refcounts[key] = 0

        cls.refcounts[key] += 1

        return cls.connections[key]

    @classmethod
    def close(cls, connection):
        key = cls.connection_to_key[connection]

        cls.refcounts[key] -= 1
        if cls.refcounts[key] == 0:
            connection.close()
            del cls.refcounts[key]
            del cls.connection_to_key[connection]
            del cls.connections[key]


class RedisNodeConfiguration():
    def __init__(self, name: str, slots: Set[int]):
        self.name = name
        self.slots = slots

    def keyslot_in_node(self, keyslot: int) -> bool:
        return keyslot in self.slots


# We create physical queue as a list on each redis cluster node to ensure even distribution.
# On scale-in/out, we should listen to old queue names for a while to ensure no message is lost.
# To do this, we compute new physical queue names and set expiry for old queue names on redis slot change event.
# Also, we store queue names on redis to ensure we don't lose old queue names on worker start.
class PhysicalQueue():

    def __init__(self, queues: Dict[str, Optional[int]]):
        self.queues = queues

    def alive_queues(self) -> List[str]:
        now = time()

        return [x for x in self.queues if self.queues[x] is None or self.queues[x] > now]

    def queue_expiry(self, queue) -> Optional[int]:
        return self.queues[queue]



class Channel(RedisChannel):

    QoS = QoS
    socket_keepalive = True

    unacked_key = '_kombu.unacked.{{{queue}}}'
    unacked_index_key = '_kombu.unacked_index.{{{queue}}}'
    unacked_mutex_key = '_kombu.unacked_mutex.{{{queue}}}'
    physical_queue_cache_key = '_kombu.physical_queue.{{{queue}}}'
    physical_queue_timeout = 600000 # 10 minutes

    min_priority = 0
    max_priority = 0
    priority_steps = [min_priority]

    from_transport_options = RedisChannel.from_transport_options + (
        'namespace',
        'keyprefix_queue',
        'keyprefix_fanout',
        'brpop_timeout',
        'queue_names_per_slot'
    )

    def __init__(self, conn, *args, **kwargs):
        super().__init__(conn, *args, **kwargs)

        self.ask_errors = {}
        self.physical_queues = {}

    def _restore(self, message, leftmost=False):
        if not self.ack_emulation:
            return super(Channel, self)._restore(message)
        tag = message.delivery_tag
        routing_key = message.delivery_info['routing_key']
        unacked_key = self.unacked_key.format(queue=routing_key)

        with self.conn_or_acquire() as client:
            with client.pipeline() as pipe:
                P, _ = pipe.hget(unacked_key, tag) \
                           .hdel(unacked_key, tag) \
                           .execute()
            if P:
                M, EX, RK = loads(bytes_to_str(P))  # json is unicode
                self._do_restore_message(M, EX, RK, client, leftmost)

    def get_redis_configuration(self) -> Dict[str, RedisNodeConfiguration]:
        nodes: Dict[str, RedisNodeConfiguration] = {}

        for slot, node in self.client.nodes_manager.slots_cache.items():
            found = nodes.get(node[0].name)
            if found is None:
                nodes[node[0].name] = RedisNodeConfiguration(name=node[0].name, slots={int(slot)})
            else:
                found.slots.add(int(slot))
        return nodes

    def redis_configuration_changed(self):
        self.physical_queues = {}  # Will be recomputed later

    def get_physical_queues(self, queues):
        result = {k: v for k, v in self.physical_queues.items() if k in queues}

        remaining_queues = [x for x in queues if x not in result]
        if remaining_queues:
            new_physical_queues = self.compute_physical_queue_names(remaining_queues)
            for queue in remaining_queues:
                # Load queue names from redis to ensure listening physical queues before last redis slot configuration change.
                try:
                    cached_physical_queues = self.client.hgetall(self.physical_queue_cache_key.format(queue=queue))
                except:
                    logger.exception('Failed to get cache', extra={'queue': queue})
                    cached_physical_queues = {}

                # Merge cached and computed queue
                # if cached_queue_names is not in new_physical_queues and has no expire, we should set its expiry
                merged_physical_queues: Dict[str, Optional[int]] = {x: None for x in new_physical_queues[queue]}

                for queue_name, timeout in cached_physical_queues.items():
                    queue_name = queue_name.decode('utf-8')
                    timeout = int(timeout)
                    if queue_name not in merged_physical_queues:
                        if timeout == 0:
                            timeout = int(time() + self.physical_queue_timeout)
                        merged_physical_queues[queue_name] = timeout

                physical_queue = PhysicalQueue(merged_physical_queues)

                result[queue] = physical_queue
                self.physical_queues[queue] = physical_queue

                # And update cache..
                for queue_name, timeout in merged_physical_queues.items():
                    value = 0 if timeout is None else timeout
                    try:
                        self.client.hset(self.physical_queue_cache_key.format(queue=queue), queue_name, value)
                    except:
                        logger.exception('Failed to set cache', extra={'queue': queue, 'queue_name': queue_name, 'value': value})

        return result

    def get_brpop_timeout(self, queue, physical_queue_name):
        timeout = self.connection.client.transport_options.get('brpop_timeout', 1)

        physical_queue = self.get_physical_queues([queue])[queue]
        expiry = physical_queue.queue_expiry(physical_queue_name)

        if expiry:
            if not timeout:
                return expiry - time()
            else:
                return min(expiry - time(), timeout)

        return 0

    def should_listen(self, queue, physical_queue_name):
        physical_queue = self.get_physical_queues([queue])[queue]
        expiry = physical_queue.queue_expiry(physical_queue_name)
        if expiry and expiry < time():
            del self.physical_queue[queue].queues[physical_queue_name]
            self.client.hdel(self.physical_queue_cache_key.format(queue=queue), physical_queue_name)
            return False
        return True

    def compute_physical_queue_names(self, queues):
        redis_configuration = self.get_redis_configuration()

        queue_names_per_slot = self.connection.client.transport_options.get('queue_names_per_slot', None)
        if not queue_names_per_slot:
            result = {}
            for queue in queues:
                result[queue] = [queue]
            return result

        result = {}
        for queue in queues:
            result[queue] = []
            if queue in queue_names_per_slot:
                for node in redis_configuration.values():
                    first_slot = next(iter(node.slots))

                    result[queue].append(queue_names_per_slot[queue][first_slot])
            else:
                logger.warning('no %s in queue_names_per_slot option, defaulting to single queue', queue)
                result[queue] = [queue]

        return result

    def _q_for_pri(self, queue, pri):
        queues = self.get_physical_queues([queue])
        queue = random.choice(queues[queue].alive_queues())

        pri = self.priority(pri)
        if pri:
            return "{}{}{}".format(queue, self.sep, pri)
        return queue


    @contextmanager
    def conn_or_acquire(self, client=None):
        if client:
            yield client
        else:
            yield self.client

    def _create_client(self, asynchronous=False):
        conninfo = self.connection.client

        hostname = conninfo.hostname
        port = conninfo.port
        password = conninfo.password
        transport = self.connection.client.transport_cls
        ssl = transport == 'rediss-cluster'

        return RedisClusterConnection.get_connection(hostname, port, password, ssl)

    def close(self):
        super().close()

        RedisClusterConnection.close(self.client)

    def _brpop_start(self):
        queues = self._queue_cycle.consume(len(self.active_queues))
        if not queues:
            return

        physical_queues = self.get_physical_queues(queues)

        for queue, physical_queue in physical_queues.items():
            for physical_queue_name in physical_queue.alive_queues():
                for _, _, conn, _ in self.connection.cycle._chan_to_sock:
                    if conn.physical_queue == physical_queue_name and conn.in_poll == False:
                        conn.in_poll = True
                        if conn.physical_queue in self.ask_errors:
                            del self.ask_errors[conn.physical_queue]
                            try:
                                conn.client.execute_command('ASKING')
                            except Exception as e:
                                logger.warning('Error while sending ASKING', extra={"e": e, "key": conn.physical_queue})
                                continue
                        try:
                            brpop_timeout = self.get_brpop_timeout(queue, physical_queue_name)
                            conn.client.connection.send_command('BRPOP', physical_queue_name, brpop_timeout)
                        except:
                            logger.exception('Error while sending BRPOP', extra={"key": conn.physical_queue})
                            self.connection.cycle._unregister(self, self.client, conn, 'BRPOP')
                        break

    def _brpop_read(self, **options):
        conn = options.pop('conn')

        try:
            resp = self.parse_response(conn, 'BRPOP', **options)
        except:
            # We should not throw error on this method to make kombu to continue operation
            raise Empty()

        brpop_timeout = self.get_brpop_timeout(conn.queue, conn.physical_queue)
        if self.should_listen(conn.queue, conn.physical_queue):
            conn.client.connection.send_command('BRPOP', conn.physical_queue, brpop_timeout)  # schedule next BRPOP
        else:
            self.connection.cycle._unregister(self, self.client, conn, 'BRPOP')

        if resp:
            self.deliver_response(conn.queue, resp)
            return True

    def _poll_error(self, cmd, conn, **options):
        try:
            resp = self.parse_response(conn, 'BRPOP', **options)
            if resp:
                self.deliver_response(conn.queue, resp)
        except Exception:
            # We should not throw error on this method to make kombu to continue operation
            # Error is logged at `parse_response`
            pass

        self.connection.cycle._unregister(self, self.client, conn, 'BRPOP')

    def deliver_response(self, queue, resp):
        dest, item = resp
        dest = bytes_to_str(queue).rsplit(self.sep, 1)[0]
        self._queue_cycle.rotate(dest)
        self.connection._deliver(loads(bytes_to_str(item)), dest)

    def parse_response(self, conn, cmd, **options):
        try:
            return conn.client.parse_response(conn.client.connection, cmd, **options)
        except Exception as e:
            logger.error('Error while reading from Redis', extra={"e": e, "key": conn.physical_queue})

            # Mostly copied from https://github.com/sendbird/redis-py/blob/master/redis/cluster.py#L1173
            if isinstance(e, ConnectionError) or isinstance(e, TimeoutError):
                try:
                    node = channel.client.get_node_from_key(conn.physical_queue)
                    self.client.nodes_manager.startup_nodes.pop(node.name, None)
                except Exception as e:
                    logger.error('Error while removing node', extra={"e": e, "key": conn.physical_queue})
                self.client.nodes_manager.initialize()
                self.redis_configuration_changed()
            elif isinstance(e, MovedError):
                self.client.reinitialize_counter += 1
                if self.client._should_reinitialized():
                    self.client.nodes_manager.initialize()
                    self.client.reinitialize_counter = 0
                else:
                    self.client.nodes_manager.update_moved_exception(e)
                self.redis_configuration_changed()
            elif isinstance(e, SlotNotCoveredError):
                self.client.reinitialize_counter += 1
                if self.client._should_reinitialized():
                    self.client.nodes_manager.initialize()
                    self.client.reinitialize_counter = 0
                    self.redis_configuration_changed()
            elif isinstance(e, TryAgainError):
                return  # try again in next BRPOP
            elif isinstance(e, AskError):
                self.add_ask_error(e, conn)
            elif isinstance(e, ClusterDownError):
                self.client.nodes_manager.initialize()
                self.redis_configuration_changed()

            self.connection.cycle._unregister(self, self.client, conn, cmd)
            raise

    def add_ask_error(self, e, conn):
        self.ask_errors[conn.physical_queue] = e

    def _lookup(self, exchange, routing_key, default=None):
        return [self._q_for_pri(routing_key, 0)]


class Transport(RedisTransport):

    Channel = Channel

    driver_type = 'redis-cluster'
    driver_name = driver_type

    implements = virtual.Transport.implements.extend(
        asynchronous=True, exchange_type=frozenset(['direct'])
    )

    def __init__(self, *args, **kwargs):
        if redis is None:
            raise ImportError('dependency missing: redis')

        super().__init__(*args, **kwargs)
        self.cycle = ClusterPoller()

    def driver_version(self):
        return redis.__version__

    def _get_errors(self):
        connection_errors, channel_errors = super()._get_errors()
        connection_errors += (RedisClusterException, ConnectionError, TimeoutError, MovedError, TryAgainError, ClusterDownError, SlotNotCoveredError, AskError)

        return connection_errors, channel_errors

