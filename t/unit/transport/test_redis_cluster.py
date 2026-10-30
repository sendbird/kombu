"""Regression coverage for Redis Cluster consumer recovery."""
import socket
from queue import Empty
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from redis.exceptions import (
    ClusterDownError, MovedError, RedisClusterException, SlotNotCoveredError,
    TimeoutError, TryAgainError,
)

from kombu.transport.redis_cluster import Channel, ClusterPoller
from kombu.utils.eventio import _epoll, _select, epoll


@pytest.fixture(params=[_select] + ([_epoll] if epoll else []))
def consumer(request):
    cycle = ClusterPoller()
    cycle.poller.close()
    cycle.poller = request.param()
    sockets = []
    clients = []

    def make_client():
        sock, peer = socket.socketpair()
        sockets.extend((sock, peer))
        client = Mock()
        client.connection._sock = sock
        client.connection.disconnect.side_effect = sock.close
        client.peer = peer
        clients.append(client)
        return client

    cluster = Mock()
    cluster.get_node_from_key.return_value = SimpleNamespace(name='node-a')
    cluster.nodes_manager.startup_nodes = {'node-a': object()}
    cluster.get_redis_connection.return_value.client.side_effect = make_client
    channel = object.__new__(Channel)
    channel.connection = SimpleNamespace(
        cycle=cycle, client=SimpleNamespace(transport_options={}),
        _deliver=Mock(),
    )
    channel.__dict__['consumer_clients'] = [cluster]
    channel._active_queues = {'queue'}
    channel.active_fanout_queues = set()
    channel._queue_cycle = Mock()
    channel._queue_cycle.consume.return_value = ['queue']
    channel.ask_errors = {}
    channel._qos = Mock()
    channel.handlers = {'BRPOP': channel._brpop_read}
    cycle._register_BRPOP(channel)
    conn = next(iter(cycle._chan_to_sock))[1]
    yield SimpleNamespace(cycle=cycle, channel=channel, conn=conn,
                          cluster=cluster, clients=clients, sockets=sockets)
    cycle.close()
    cycle.poller.close()
    for sock in sockets:
        sock.close()


@pytest.mark.parametrize('refresh_fails', [False, True])
def test_read_failure_recovers_and_delivers(consumer, refresh_fails, caplog):
    c = consumer
    c.clients[0].connection.disconnect()
    c.clients[0].parse_response.side_effect = TimeoutError('read timeout')
    if refresh_fails:
        c.cluster.nodes_manager.initialize.side_effect = RedisClusterException(
            'topology unavailable')
    with pytest.raises(Empty):
        c.channel._brpop_read(conn=c.conn)
    assert not c.cycle._chan_to_sock
    assert not c.cycle._fd_to_chan
    assert not c.cycle._sock_to_fd
    assert not c.conn.in_poll
    if refresh_fails:
        assert 'topology unavailable' in caplog.text

    c.cluster.nodes_manager.initialize.side_effect = None
    c.cycle._register_BRPOP(c.channel)
    client = c.clients[-1]
    client.connection.send_command.assert_called_once_with('BRPOP', 'queue', 1)
    client.parse_response.return_value = [b'queue', b'{"recovered": true}']
    client.peer.send(b'ready')
    events = c.cycle.poller.poll(0.1)
    assert events
    for fd, event in events:
        c.cycle.handle_event(fd, event)
    c.channel.connection._deliver.assert_called_once_with(
        {'recovered': True}, 'queue')


def test_repeated_cleanup_after_poll_error(consumer):
    c = consumer
    c.clients[0].parse_response.side_effect = TimeoutError('read timeout')
    c.channel._poll_error('BRPOP', c.conn)
    c.cycle._unregister(c.channel, c.conn, 'BRPOP')
    assert not c.cycle._chan_to_sock
    assert not c.cycle._fd_to_chan
    assert not c.cycle._sock_to_fd


@pytest.mark.parametrize('reuse_fd', [False, True])
def test_socket_replacement_rearms_brpop(consumer, reuse_fd):
    c = consumer
    old = c.clients[0].connection._sock
    old_fd = old.fileno()
    if reuse_fd:
        old.close()
    new, peer = socket.socketpair()
    c.sockets.extend((new, peer))
    if reuse_fd:
        assert new.fileno() == old_fd
    c.clients[0].connection._sock = new
    c.clients[0].connection.disconnect.side_effect = new.close
    c.cycle._register_BRPOP(c.channel)
    assert len(c.clients) == 2
    current = c.clients[-1]
    current.connection.send_command.assert_called_once_with('BRPOP', 'queue', 1)
    assert c.cycle._chan_to_sock[(c.channel, c.conn, 'BRPOP')] is current.connection._sock


def test_cleanup_preserves_reused_fd_owner(consumer):
    c = consumer
    old = c.clients[0].connection._sock
    fd = old.fileno()
    old.close()
    other = (Mock(), Mock(), 'BRPOP')
    c.cycle._fd_to_chan[fd] = other
    c.cycle.poller.close()
    c.cycle.poller = Mock()
    c.cycle._unregister(c.channel, c.conn, 'BRPOP')
    assert c.cycle._fd_to_chan[fd] == other
    c.cycle.poller.unregister.assert_not_called()


@pytest.mark.parametrize('during_read', [False, True])
def test_reconnect_during_send_registers_new_socket(consumer, during_read):
    c = consumer
    client = c.clients[0]

    def reconnect(*args):
        client.connection._sock.close()
        new, peer = socket.socketpair()
        c.sockets.extend((new, peer))
        client.connection._sock = new
        client.connection.disconnect.side_effect = new.close
        client.peer = peer

    client.connection.send_command.side_effect = reconnect
    if during_read:
        client.parse_response.return_value = None
        c.channel._brpop_read(conn=c.conn)
    else:
        c.conn.in_poll = False
        c.cycle._register_BRPOP(c.channel)
    assert c.cycle._client_registered(c.channel, c.conn, 'BRPOP')
    client.connection.send_command.side_effect = None
    client.parse_response.return_value = [b'queue', b'{"reconnected": true}']
    client.peer.send(b'ready')
    events = c.cycle.poller.poll(0.1)
    assert events
    for fd, event in events:
        c.cycle.handle_event(fd, event)
    c.channel.connection._deliver.assert_called_once_with(
        {'reconnected': True}, 'queue')


def test_next_send_failure_does_not_drop_received_message(consumer):
    c = consumer
    client = c.clients[0]
    client.parse_response.return_value = [b'queue', b'{"received": true}']
    client.connection.send_command.side_effect = TimeoutError('send timeout')
    assert c.channel._brpop_read(conn=c.conn)
    c.channel.connection._deliver.assert_called_once_with(
        {'received': True}, 'queue')
    assert not c.cycle._chan_to_sock
    assert not c.conn.in_poll
    c.cycle._register_BRPOP(c.channel)
    c.clients[-1].connection.send_command.assert_called_once_with(
        'BRPOP', 'queue', 1)


def test_try_again_keeps_connection_and_reissues_brpop(consumer):
    c = consumer
    c.clients[0].parse_response.side_effect = TryAgainError('try again')
    c.channel._brpop_read(conn=c.conn)
    assert c.cycle._client_registered(c.channel, c.conn, 'BRPOP')
    assert c.clients[0].connection.send_command.call_count == 2
    c.cluster.nodes_manager.initialize.assert_not_called()


def test_failed_registration_is_cleaned_up(consumer):
    c = consumer
    c.cycle._unregister(c.channel, c.conn, 'BRPOP')
    poller = c.cycle.poller
    c.cycle.poller = Mock()
    c.cycle.poller.register.side_effect = OSError('registration failed')
    c.cycle._register_BRPOP(c.channel)
    assert not c.cycle._chan_to_sock
    assert not c.cycle._fd_to_chan
    assert not c.cycle._sock_to_fd
    c.clients[-1].close.assert_called_once()
    c.clients[-1].connection.send_command.assert_not_called()
    c.cycle.poller = poller


@pytest.mark.parametrize('error', [
    MovedError('123 localhost:7001'), SlotNotCoveredError('missing slot'),
    ClusterDownError('cluster down'),
])
def test_other_topology_refresh_failures_are_cleaned_up(consumer, error):
    c = consumer
    c.cluster.reinitialize_counter = 0
    c.cluster._should_reinitialized.return_value = True
    c.cluster.nodes_manager.initialize.side_effect = RedisClusterException(
        'topology unavailable')
    c.clients[0].parse_response.side_effect = error
    with pytest.raises(Empty):
        c.channel._brpop_read(conn=c.conn)
    assert not c.cycle._chan_to_sock
    assert not c.cycle._fd_to_chan
    assert not c.cycle._sock_to_fd


def test_connections_are_scoped_to_channel(consumer):
    c = consumer
    other = Mock()
    other.active_queues = ['queue']
    other.consumer_clients = [c.cluster]
    assert c.cycle._get_conns_for_channel(other)[0] is not c.conn
