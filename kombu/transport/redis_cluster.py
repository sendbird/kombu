from contextlib import contextmanager
from time import time

from kombu.five import Empty
from kombu.utils.compat import _detect_environment
from kombu.utils.encoding import bytes_to_str
from kombu.utils.eventio import READ, ERR
from kombu.utils.json import loads
from kombu.utils.uuid import uuid

from . import virtual
from .redis import (
    Channel as RedisChannel,
    MultiChannelPoller,
    MutexHeld,
    QoS as RedisQoS,
    Transport as RedisTransport,
)

try:
    import redis
    from redis.exceptions import MovedError, RedisClusterException
except ImportError:
    redis = None


# copied from `kombu.transport.redis` and disable pipeline transcation
@contextmanager
def Mutex(client, name, expire):
    lock_id = uuid().encode('utf-8')
    acquired = client.set(name, lock_id, ex=expire, nx=True)

    try:
        if acquired:
            yield
        else:
            raise MutexHeld()
    finally:
        if acquired:
            if client.get(name) == lock_id:
                client.delete(name)


class QoS(RedisQoS):

    def restore_visible(self, start=0, num=10, interval=10):
        with self.channel.conn_or_acquire() as client:
            ceil = time() - self.visibility_timeout

            try:
                with Mutex(
                    client,
                    self.unacked_mutex_key,
                    self.unacked_mutex_expire,
                ):
                    env = _detect_environment()
                    if env == 'gevent':
                        ceil = time()

                    visible = client.zrevrangebyscore(
                        self.unacked_index_key,
                        ceil,
                        0,
                        start=num and start,
                        num=num,
                        withscores=True
                    )

                    for tag, score in visible or []:
                        self.restore_by_tag(tag, client)
            except MutexHeld:
                pass

class RedisNodeConnection():
    def __init__(self, node):
        self.node = node
        self.client = self.node.redis_connection.client()


class ClusterPoller(MultiChannelPoller):

    def _register(self, channel, client, conn, cmd):
        ident = (channel, client, conn, cmd)

        if ident in self._chan_to_sock:
            self._unregister(*ident)

        sock = conn.client.connection._sock
        self._fd_to_chan[sock.fileno()] = (channel, conn, cmd)
        self._chan_to_sock[ident] = sock
        self.poller.register(sock, self.eventflags)

    def _unregister(self, channel, client, conn, cmd):
        sock = self._chan_to_sock[(channel, client, conn, cmd)]
        self.poller.unregister(sock)

    def _register_BRPOP(self, channel):
        conns = self._get_conns_for_channel(channel)

        for conn in conns:
            ident = (channel, channel.client, conn, 'BRPOP')

            if (ident not in self._chan_to_sock):
                channel._in_poll = False
                self._register(*ident)

        if not channel._in_poll:  # send BRPOP
            channel._brpop_start()

    def _get_conns_for_channel(self, channel):
        if self._chan_to_sock:
            return [conn for _, _, conn, _ in self._chan_to_sock]

        return [
            RedisNodeConnection(channel.client.nodes_manager.get_node_from_slot(channel.client.keyslot(key)))
            for key in channel.active_queues
        ]

    def handle_event(self, fileno, event):
        if event & READ:
            return self.on_readable(fileno), self
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

    namespace = 'default'
    keyprefix_queue = '/{namespace}/_kombu/binding%s'
    keyprefix_fanout = '/{namespace}/_kombu/fanout.'
    unacked_key = '/{namespace}/_kombu/unacked'
    unacked_index_key = '/{namespace}/_kombu/unacked_index'
    unacked_mutex_key = '/{namespace}/_kombu/unacked_mutex'

    min_priority = 0
    max_priority = 0
    priority_steps = [min_priority]

    from_transport_options = RedisChannel.from_transport_options + (
        'namespace',
        'keyprefix_queue',
        'keyprefix_fanout',
    )

    def __init__(self, conn, *args, **kwargs):
        options = conn.client.transport_options
        namespace = options.get('namespace', self.namespace)
        keys = [
            'keyprefix_queue',
            'keyprefix_fanout',
            'unacked_key',
            'unacked_index_key',
            'unacked_mutex_key',
        ]

        for key in keys:
            value = options.get(key, getattr(self, key))
            options[key] = value.format(namespace=namespace)

        super().__init__(conn, *args, **kwargs)
        self.client.info()

    @contextmanager
    def conn_or_acquire(self, client=None):
        if client:
            yield client
        else:
            yield self.client

    def _get_pool(self, asynchronous=False):
        raise NotImplementedError

    def _get_client(self):
        return redis.RedisCluster

    def _create_client(self, asynchronous=False):
        conninfo = self.connection.client
        params = {'skip_full_coverage_check': True, 'host': conninfo.hostname, 'port': conninfo.port}

        return self.Client(**params)

    def _brpop_start(self, timeout=1):
        queues = self._queue_cycle.consume(len(self.active_queues))
        if not queues:
            return

        self._in_poll = True
        timeout = timeout or 0
        node_to_keys = {}

        for key in queues:
            node = self.client.nodes_manager.get_node_from_slot(self.client.keyslot(key))
            node_to_keys.setdefault(node.name, []).append(key)

        for chan, client, conn, cmd in self.connection.cycle._chan_to_sock:
            expected = (self, self.client, 'BRPOP')
            keys = node_to_keys.get(conn.node.name)

            if keys and (chan, client, cmd) == expected:
                for key in keys:
                    conn.client.connection.send_command('BRPOP', key, timeout)

    def _brpop_read(self, **options):
        try:
            conn = options.pop('conn')

            try:
                resp = conn.client.parse_response(conn.client.connection, 'BRPOP', **options)
            except self.connection_errors:
                conn.client.connection.disconnect()
                raise Empty()
            except MovedError as e:
                # Copied from redis-py cluster.py
                self.client.reinitialize_counter += 1
                if self.client._should_reinitialized():
                    self.client.nodes_manager.initialize()
                    # Reset the counter
                    self.client.reinitialize_counter = 0
                else:
                    self.client.nodes_manager.update_moved_exception(e)
                raise Empty()

            if resp:
                dest, item = resp
                dest = bytes_to_str(dest).rsplit(self.sep, 1)[0]
                self._queue_cycle.rotate(dest)
                self.connection._deliver(loads(bytes_to_str(item)), dest)
                return True
        finally:
            self._in_poll = False

    def _poll_error(self, cmd, conn, **options):
        if cmd == 'BRPOP':
            self.client.parse_response(conn, cmd)


class Transport(RedisTransport):

    Channel = Channel

    driver_type = 'redis-cluster'
    driver_name = driver_type
    connection_errors = RedisTransport.connection_errors + (RedisClusterException,)

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
