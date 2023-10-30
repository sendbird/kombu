from contextlib import contextmanager
from time import time, sleep
from queue import Empty
from collections import defaultdict

from kombu.log import get_logger
from kombu.utils.encoding import bytes_to_str
from kombu.utils.eventio import READ, ERR
from kombu.utils.json import loads, dumps
from kombu.utils.objects import cached_property
from kombu.utils.url import parse_url

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


# Override create_redis_cluster_connection_for_{producer,consumer} to use other redis client
def create_redis_cluster_connection_for_consumer(hostname, port, password, ssl):
    params = {'require_full_coverage': False, 'host': hostname, 'port': port, 'password': password, 'dynamic_startup_nodes': True}
    if ssl:
        params['ssl'] = True

    return redis.RedisCluster(**params)


create_redis_cluster_connection_for_producer = create_redis_cluster_connection_for_consumer


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
    def __init__(self, key, cluster_connection):
        self.cluster_connection = cluster_connection
        self.redis_connection = None
        self.in_poll = False
        self.key = key
        self.timeout = None

class ClusterPoller(MultiChannelPoller):
    def __init__(self):
        super().__init__()
        self._sock_to_fd = {}

    def _register(self, channel, conn, cmd):
        ident = (channel, conn, cmd)

        if ident in self._chan_to_sock:
            self._unregister(*ident)

        if not conn.redis_connection:
            tries = 0
            backoff = [0, 0.1, 0.2, 0.4]
            while True:
                if tries > 3:
                    raise ValueError('Cannot find node for key: {}'.format(conn.key))
                try:
                    if conn.key in channel.ask_errors:
                        ask_error = channel.ask_errors[conn.key]
                        node = conn.cluster_connection.get_node(ask_error.host, ask_error.port)
                    else:
                        node = conn.cluster_connection.get_node_from_key(conn.key)
                    if node:
                        break
                except:
                    logger.exception('Error while getting node from key', extra={"key": conn.key})

                sleep(backoff[tries])
                conn.cluster_connection.nodes_manager.initialize()
                tries += 1

            redis_connection = conn.cluster_connection.get_redis_connection(node)
            conn.redis_connection = redis_connection.client()

        sock = conn.redis_connection.connection._sock
        self._fd_to_chan[sock.fileno()] = (channel, conn, cmd)
        self._chan_to_sock[ident] = sock
        self._sock_to_fd[sock] = sock.fileno()
        self.poller.register(sock, self.eventflags)

    def _unregister(self, channel, conn, cmd):
        sock = self._chan_to_sock[(channel, conn, cmd)]
        fd = self._sock_to_fd[sock]

        self.poller.unregister(sock)
        if conn.redis_connection:
            if conn.redis_connection.connection:
                # There might be pending BRPOP response on the connection, so we disconnect to ensure safety
                conn.redis_connection.connection.disconnect()
            conn.redis_connection.close()
            conn.redis_connection = None

        del self._fd_to_chan[fd]
        del self._chan_to_sock[(channel, conn, cmd)]
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
            ident = (channel, conn, 'BRPOP')

            if (ident not in self._chan_to_sock):
                try:
                    self._register(*ident)
                except:
                    logger.exception('Error while registering BRPOP', extra={"key": conn.key})

        timeout = channel.connection.client.transport_options.get('brpop_timeout', 1)
        channel._brpop_start(timeout)

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
        conns = [conn for _, conn, _ in self._chan_to_sock]
        for key in channel.active_queues:
            for client in channel.consumer_clients:
                try:
                    conn = next(x for x in conns if x.key == key and x.cluster_connection == client)
                    conns.remove(conn)
                except StopIteration:
                    conn = RedisNodeConnection(key, client)
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
    producer_connections = {}
    consumer_connections = {}
    connection_to_key = {}
    refcounts = {}

    @classmethod
    def get_consumer_connection(cls, host, port, password, ssl):
        key = (host, port, password, ssl, 'consumer')
        if key not in cls.consumer_connections:
            connection = create_redis_cluster_connection_for_consumer(host, port, password, ssl)
            cls.consumer_connections[key] = connection
            cls.connection_to_key[connection] = key
            cls.refcounts[key] = 0

        cls.refcounts[key] += 1

        return cls.consumer_connections[key]

    @classmethod
    def get_producer_connection(cls, host, port, password, ssl):
        key = (host, port, password, ssl, 'producer')
        if key not in cls.producer_connections:
            connection = create_redis_cluster_connection_for_producer(host, port, password, ssl)
            cls.producer_connections[key] = connection
            cls.connection_to_key[connection] = key
            cls.refcounts[key] = 0

        cls.refcounts[key] += 1

        return cls.producer_connections[key]

    @classmethod
    def close(cls, connection):
        key = cls.connection_to_key[connection]

        cls.refcounts[key] -= 1
        if cls.refcounts[key] == 0:
            connection.close()
            del cls.refcounts[key]
            del cls.connection_to_key[connection]
            if key[4] == 'producer':
                del cls.producer_connections[key]
            elif key[4] == 'consumer':
                del cls.consumer_connections[key]
            else:
                raise ValueError(f'Unknown connection type: {key[4]}')


class Channel(RedisChannel):

    QoS = QoS
    socket_keepalive = True

    unacked_key = '_kombu.unacked.{{{queue}}}'
    unacked_index_key = '_kombu.unacked_index.{{{queue}}}'
    unacked_mutex_key = '_kombu.unacked_mutex.{{{queue}}}'

    min_priority = 0
    max_priority = 0
    priority_steps = [min_priority]

    from_transport_options = RedisChannel.from_transport_options + (
        'namespace',
        'keyprefix_queue',
        'keyprefix_fanout',
        'brpop_timeout'
    )

    def __init__(self, conn, *args, **kwargs):
        super().__init__(conn, *args, **kwargs)

        self.ask_errors = {}
        self.consumer_created = False

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

    @contextmanager
    def conn_or_acquire(self, client=None):
        if client:
            yield client
        else:
            yield self.client

    @cached_property
    def consumer_clients(self):
        self.consumer_created = True

        conninfo = self.connection.client

        parsed = parse_url(conninfo.hostname)
        ssl = parsed['transport'] == 'rediss-cluster'

        connection = RedisClusterConnection.get_consumer_connection(parsed['hostname'], parsed['port'], parsed['password'], ssl)

        # Additional redis cluster
        # redis-cluster://172.16.0.1:7000?alt=redis-cluster://172.16.0.2:7000
        if 'alt' in parsed:
            alt_parsed = parse_url(parsed['alt'])
            alt_ssl = alt_parsed['transport'] == 'rediss-cluster'
            alt_connection = RedisClusterConnection.get_consumer_connection(alt_parsed['hostname'], alt_parsed['port'], alt_parsed['password'], alt_ssl)

            return [connection, alt_connection]

        return [connection]

    @cached_property
    def client(self):
        conninfo = self.connection.client

        parsed = parse_url(conninfo.hostname)
        transport = self.connection.client.transport_cls
        ssl = transport == 'rediss-cluster'

        return RedisClusterConnection.get_producer_connection(parsed['hostname'], parsed['port'], parsed['password'], ssl)

    def close(self):
        super().close()

        RedisClusterConnection.close(self.client)
        if self.consumer_created is True:
            for client in self.consumer_clients:
                RedisClusterConnection.close(client)

    def _brpop_start(self, timeout):
        queues = self._queue_cycle.consume(len(self.active_queues))
        if not queues:
            return

        for key in queues:
            for client in self.consumer_clients:
                for _, conn, _ in self.connection.cycle._chan_to_sock:
                    if conn.key == key and conn.in_poll == False and conn.cluster_connection == client:
                        conn.in_poll = True
                        conn.timeout = timeout
                        if conn.key in self.ask_errors:
                            del self.ask_errors[conn.key]
                            try:
                                conn.redis_connection.execute_command('ASKING')
                            except:
                                logger.exception('Error while sending ASKING', extra={"key": conn.key})
                                continue

                        try:
                            conn.redis_connection.connection.send_command('BRPOP', key, timeout)
                        except:
                            logger.exception('Error while sending BRPOP', extra={"key": conn.key})
                            self.connection.cycle._unregister(self, conn, 'BRPOP')
                        break

    def _brpop_read(self, **options):
        conn = options.pop('conn')

        try:
            resp = self.parse_response(conn, 'BRPOP', **options)
        except:
            # We should not throw error on this method to make kombu to continue operation
            raise Empty()

        conn.redis_connection.connection.send_command('BRPOP', conn.key, conn.timeout)  # schedule next BRPOP

        if resp:
            self.deliver_response(resp)
            return True

    def _poll_error(self, cmd, conn, **options):
        try:
            resp = self.parse_response(conn, 'BRPOP', **options)
            if resp:
                self.deliver_response(resp)
        except:
            # We should not throw error on this method to make kombu to continue operation
            # Error is logged at `parse_response`
            pass

        self.connection.cycle._unregister(self, conn, 'BRPOP')

    def deliver_response(self, resp):
        dest, item = resp
        dest = bytes_to_str(dest).rsplit(self.sep, 1)[0]
        self._queue_cycle.rotate(dest)
        self.connection._deliver(loads(bytes_to_str(item)), dest)

    def parse_response(self, conn, cmd, **options):
        try:
            return conn.redis_connection.parse_response(conn.redis_connection.connection, cmd, **options)
        except Exception as e:
            logger.exception('Error while reading from Redis', extra={"key": conn.key})

            # Mostly copied from https://github.com/sendbird/redis-py/blob/master/redis/cluster.py#L1173
            if isinstance(e, ConnectionError) or isinstance(e, TimeoutError):
                try:
                    node = conn.cluster_connection.get_node_from_key(conn.key)
                    conn.cluster_connection.nodes_manager.startup_nodes.pop(node.name, None)
                except:
                    logger.exception('Error while removing node', extra={"key": conn.key})
                conn.cluster_connection.nodes_manager.initialize()
            elif isinstance(e, MovedError):
                conn.cluster_connection.reinitialize_counter += 1
                if conn.cluster_connection._should_reinitialized():
                    conn.cluster_connection.nodes_manager.initialize()
                    conn.cluster_connection.reinitialize_counter = 0
                else:
                    conn.cluster_connection.nodes_manager.update_moved_exception(e)
            elif isinstance(e, SlotNotCoveredError):
                conn.cluster_connection.reinitialize_counter += 1
                if conn.cluster_connection._should_reinitialized():
                    conn.cluster_connection.nodes_manager.initialize()
                    conn.cluster_connection.reinitialize_counter = 0
            elif isinstance(e, TryAgainError):
                return  # try again in next BRPOP
            elif isinstance(e, AskError):
                self.add_ask_error(e, conn)
            elif isinstance(e, ClusterDownError):
                conn.cluster_connection.nodes_manager.initialize()

            self.connection.cycle._unregister(self, conn, cmd)
            raise

    def add_ask_error(self, e, conn):
        self.ask_errors[conn.key] = e


class Transport(RedisTransport):

    Channel = Channel

    driver_type = 'redis-cluster'
    driver_name = driver_type
    can_parse_url = True

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

