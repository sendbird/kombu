from contextlib import contextmanager
from time import time, sleep
from queue import Empty
from collections import defaultdict

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
    def __init__(self, key):
        self.client = None
        self.in_poll = False
        self.key = key
        self.timeout = None

class ClusterPoller(MultiChannelPoller):

    def _register(self, channel, client, conn, cmd):
        ident = (channel, client, conn, cmd)

        if ident in self._chan_to_sock:
            self._unregister(*ident)

        if not conn.client:
            tries = 0
            backoff = [0, 0.1, 0.2, 0.4]
            while True:
                if tries > 3:
                    raise ValueError('Cannot find node for key: {}'.format(conn.key))
                try:
                    if conn.key in channel.ask_errors:
                        ask_error = channel.ask_errors[conn.key]
                        node = channel.client.get_node(ask_error.host, ask_error.port)
                    else:
                        node = channel.client.get_node_from_key(conn.key)
                    if node:
                        break
                except Exception as e:
                    logger.error('Error while getting node from key', extra={"e": e, "key": conn.key})

                sleep(backoff[tries])
                channel.client.nodes_manager.initialize()
                tries += 1

            redis_connection = channel.client.get_redis_connection(node)
            conn.client = redis_connection.client()

        sock = conn.client.connection._sock
        self._fd_to_chan[sock.fileno()] = (channel, conn, cmd)
        self._chan_to_sock[ident] = sock
        self.poller.register(sock, self.eventflags)

    def _unregister(self, channel, client, conn, cmd):
        sock = self._chan_to_sock[(channel, client, conn, cmd)]
        fileno = sock.fileno()

        if conn.client:
            conn.client.close()
            conn.client = None

        del self._fd_to_chan[fileno]
        del self._chan_to_sock[(channel, client, conn, cmd)]

        self.poller.unregister(sock)

    def _register_BRPOP(self, channel):
        conns = self._get_conns_for_channel(channel)

        for conn in conns:
            ident = (channel, channel.client, conn, 'BRPOP')

            if (ident not in self._chan_to_sock):
                try:
                    self._register(*ident)
                except Exception as e:
                    logger.error('Error while registering BRPOP', extra={"e": e, "key": conn.key})

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
        for key in channel.active_queues:
            try:
                conn = next(x for x in conns if x.key == key)
                conns.remove(conn)
            except StopIteration:
                conn = RedisNodeConnection(key)
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
            return

        if chan.qos.can_consume():
            return chan.handlers[cmd](**{'conn': conn})

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
    )

    def __init__(self, conn, *args, **kwargs):
        super().__init__(conn, *args, **kwargs)

        self.client.info()
        self.ask_errors = {}

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

    def _create_client(self, asynchronous=False):
        conninfo = self.connection.client

        hostname = conninfo.hostname
        port = conninfo.port
        password = conninfo.password
        transport = self.connection.client.transport_cls
        ssl = transport == 'rediss-cluster'

        return create_redis_cluster_connection(hostname, port, password, ssl)

    def close(self):
        super().close()

        self.client.close()

    def _brpop_start(self, timeout=1):
        queues = self._queue_cycle.consume(len(self.active_queues))
        if not queues:
            return

        timeout = timeout or 0

        for key in queues:
            for _, _, conn, _ in self.connection.cycle._chan_to_sock:
                if conn.key == key and conn.in_poll == False:
                    conn.in_poll = True
                    conn.timeout = timeout
                    if conn.key in self.ask_errors:
                        del self.ask_errors[conn.key]
                        try:
                            conn.client.execute_command('ASKING')
                        except Exception as e:
                            logger.warning('Error while sending ASKING', extra={"e": e, "key": conn.key})
                            continue

                    conn.client.connection.send_command('BRPOP', key, timeout)
                    break

    def _brpop_read(self, **options):
        conn = options.pop('conn')

        try:
            resp = self.parse_response(conn, 'BRPOP', **options)
        except self.connection_errors:
            raise Empty()
        conn.client.connection.send_command('BRPOP', conn.key, conn.timeout) # schedule next BRPOP

        if resp:
            self.deliver_response(resp)
            return True

    def _poll_error(self, cmd, conn, **options):
        try:
            resp = self.parse_response(conn, 'BRPOP', **options)
            if resp:
                self.deliver_response(resp)
        except self.connection_errors as e:
            # We should not throw error on this method to make kombu to continue operation
            logger.error('Error while reading from Redis', extra={"e": e, "key": conn.key})

        self.connection.cycle._unregister(self, self.client, conn, 'BRPOP')

    def deliver_response(self, resp):
        dest, item = resp
        dest = bytes_to_str(dest).rsplit(self.sep, 1)[0]
        self._queue_cycle.rotate(dest)
        self.connection._deliver(loads(bytes_to_str(item)), dest)

    def parse_response(self, conn, cmd, **options):
        try:
            return conn.client.parse_response(conn.client.connection, cmd, **options)
        except Exception as e:
            logger.error('Error while reading from Redis', extra={"e": e, "key": conn.key})
            # Mostly copied from https://github.com/sendbird/redis-py/blob/master/redis/cluster.py#L1173
            if isinstance(e, ConnectionError) or isinstance(e, TimeoutError):
                try:
                    node = channel.client.get_node_from_key(conn.key)
                    self.client.nodes_manager.startup_nodes.pop(node.name, None)
                except Exception as e:
                    logger.error('Error while removing node', extra={"e": e, "key": conn.key})
                self.client.nodes_manager.initialize()
            elif isinstance(e, MovedError):
                self.client.reinitialize_counter += 1
                if self.client._should_reinitialized():
                    self.client.nodes_manager.initialize()
                    self.client.reinitialize_counter = 0
                else:
                    self.client.nodes_manager.update_moved_exception(e)
            elif isinstance(e, SlotNotCoveredError):
                self.client.reinitialize_counter += 1
                if self.client._should_reinitialized():
                    self.client.nodes_manager.initialize()
                    self.client.reinitialize_counter = 0
            elif isinstance(e, TryAgainError):
                return  # try again in next BRPOP
            elif isinstance(e, AskError):
                self.add_ask_error(e, conn)
            elif isinstance(e, ClusterDownError):
                self.client.nodes_manager.initialize()

            self.connection.cycle._unregister(self, self.client, conn, cmd)
            raise

    def add_ask_error(self, e, conn):
        self.ask_errors[conn.key] = e


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

