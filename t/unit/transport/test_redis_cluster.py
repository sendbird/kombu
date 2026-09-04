"""
Tests for the redis_cluster transport's connection-error recovery.

Regression cover for an incident where a Celery consumer detached from an
ElastiCache Serverless broker and never reattached, while the process stayed
alive and the pod stayed Ready for 2.5 days.
"""

from queue import Empty
from unittest.mock import Mock

import pytest

pytest.importorskip('redis')

from redis.exceptions import (  # noqa: E402
    ConnectionError,
    RedisClusterException,
    TimeoutError,
)

from kombu.transport.redis_cluster import Channel  # noqa: E402


class _NodesManager:
    def __init__(self, startup_nodes, initialize_raises=False):
        self.startup_nodes = dict(startup_nodes)
        self.initialize_raises = initialize_raises
        self.initialize_calls = 0

    def initialize(self):
        self.initialize_calls += 1
        if self.initialize_raises:
            raise RedisClusterException(
                'Redis Cluster cannot be connected. Please provide at least '
                'one reachable node'
            )
        # redis-py rebuilds startup_nodes from the discovered topology when
        # dynamic_startup_nodes is on, which is how the pop undoes itself.
        self.startup_nodes = {'host:6379': object(), 'host:6380': object()}


def _make_conn(nodes_manager, error):
    """A RedisNodeConnection whose read raises `error`."""
    node = Mock()
    node.name = 'host:6379'

    cluster_connection = Mock()
    cluster_connection.nodes_manager = nodes_manager
    cluster_connection.get_node_from_key.return_value = node

    conn = Mock()
    conn.key = 'host:6379'
    conn.cluster_connection = cluster_connection
    conn.redis_connection.parse_response.side_effect = error
    return conn


class _Channel:
    """
    Minimal stand-in exercising the real parse_response.

    Mock(spec=Channel) cannot be used: `connection` is assigned at runtime, so
    the spec rejects it.
    """

    parse_response = Channel.parse_response
    _brpop_read = Channel._brpop_read

    def __init__(self):
        self.connection = Mock()
        self.ask_errors = {}

    def add_ask_error(self, e, conn):
        self.ask_errors[conn.key] = e


def _make_channel():
    return _Channel()


# ElastiCache Serverless: one hostname, two startup entries, so an endpoint-level
# event takes out every seed at once.
SERVERLESS_SEEDS = {'host:6379': object(), 'host:6380': object()}


@pytest.mark.parametrize('error', [ConnectionError('closed by server'), TimeoutError('read timed out')])
def test_successful_reinitialize_restores_the_popped_node(error):
    """The pop is undone by initialize(); nothing should be lost."""
    nodes_manager = _NodesManager(SERVERLESS_SEEDS)
    conn = _make_conn(nodes_manager, error)
    channel = _make_channel()

    with pytest.raises(type(error)):
        channel.parse_response(conn, 'BRPOP')

    assert nodes_manager.initialize_calls == 1
    assert set(nodes_manager.startup_nodes) == {'host:6379', 'host:6380'}


def test_failed_reinitialize_does_not_shrink_the_seed_list():
    """
    The core regression.

    Before the fix a failed initialize() left the popped node gone, so repeated
    failures drained startup_nodes to empty and the consumer could never
    rediscover the cluster.
    """
    nodes_manager = _NodesManager(SERVERLESS_SEEDS, initialize_raises=True)
    conn = _make_conn(nodes_manager, ConnectionError('closed by server'))
    channel = _make_channel()

    with pytest.raises(ConnectionError):
        channel.parse_response(conn, 'BRPOP')

    assert set(nodes_manager.startup_nodes) == {'host:6379', 'host:6380'}


def test_repeated_failures_never_empty_the_seed_list():
    """Ten consecutive endpoint failures must still leave a node to retry from."""
    nodes_manager = _NodesManager(SERVERLESS_SEEDS, initialize_raises=True)
    channel = _make_channel()

    for _ in range(10):
        conn = _make_conn(nodes_manager, ConnectionError('closed by server'))
        with pytest.raises(ConnectionError):
            channel.parse_response(conn, 'BRPOP')

    assert nodes_manager.startup_nodes, 'startup_nodes drained; consumer can never recover'


def test_failed_reinitialize_still_raises_the_original_error():
    """
    A RedisClusterException from initialize() must not mask the real cause.

    The caller keys its recovery off ConnectionError/TimeoutError; swapping in a
    different exception type hides what actually happened.
    """
    nodes_manager = _NodesManager(SERVERLESS_SEEDS, initialize_raises=True)
    conn = _make_conn(nodes_manager, ConnectionError('closed by server'))
    channel = _make_channel()

    with pytest.raises(ConnectionError, match='closed by server'):
        channel.parse_response(conn, 'BRPOP')


def test_failed_reinitialize_still_unregisters_the_dead_connection():
    """
    Cleanup below the reinitialize must run.

    Previously an exception from initialize() jumped past _unregister, leaving a
    dead socket registered in the poller.
    """
    nodes_manager = _NodesManager(SERVERLESS_SEEDS, initialize_raises=True)
    conn = _make_conn(nodes_manager, ConnectionError('closed by server'))
    channel = _make_channel()

    with pytest.raises(ConnectionError):
        channel.parse_response(conn, 'BRPOP')

    channel.connection.cycle._unregister.assert_called_once_with(channel, conn, 'BRPOP')


def test_successful_read_is_returned_untouched():
    nodes_manager = _NodesManager(SERVERLESS_SEEDS)
    conn = _make_conn(nodes_manager, None)
    conn.redis_connection.parse_response.side_effect = None
    conn.redis_connection.parse_response.return_value = [b'queue', b'{}']
    channel = _make_channel()

    assert channel.parse_response(conn, 'BRPOP') == [b'queue', b'{}']
    assert nodes_manager.initialize_calls == 0


# A `close()`-time BRPOP drain that times out on an idle queue is expected,
# not a failure: `close()` swallows it via `except Empty`. Logging it at
# ERROR (logger.exception) turns every idle worker shutdown into a Sentry
# event. `is_close=True` must downgrade that one case to DEBUG while every
# other call site keeps logging at ERROR.


def test_close_time_timeout_is_not_logged_as_error(monkeypatch):
    nodes_manager = _NodesManager(SERVERLESS_SEEDS)
    conn = _make_conn(nodes_manager, TimeoutError('Timeout reading from socket'))
    channel = _make_channel()

    mock_logger = Mock()
    monkeypatch.setattr('kombu.transport.redis_cluster.logger', mock_logger)

    with pytest.raises(TimeoutError):
        channel.parse_response(conn, 'BRPOP', is_close=True)

    mock_logger.exception.assert_not_called()
    mock_logger.debug.assert_called_once()


def test_close_time_connection_error_is_still_logged_as_error(monkeypatch):
    """Only the expected-timeout case is downgraded; real errors during
    close still surface at ERROR."""
    nodes_manager = _NodesManager(SERVERLESS_SEEDS)
    conn = _make_conn(nodes_manager, ConnectionError('closed by server'))
    channel = _make_channel()

    mock_logger = Mock()
    monkeypatch.setattr('kombu.transport.redis_cluster.logger', mock_logger)

    with pytest.raises(ConnectionError):
        channel.parse_response(conn, 'BRPOP', is_close=True)

    mock_logger.exception.assert_called_once()


def test_non_close_timeout_is_still_logged_as_error(monkeypatch):
    """Normal-operation BRPOP timeouts (is_close unset) keep logging at
    ERROR -- only the close()-drain path is special-cased."""
    nodes_manager = _NodesManager(SERVERLESS_SEEDS)
    conn = _make_conn(nodes_manager, TimeoutError('Timeout reading from socket'))
    channel = _make_channel()

    mock_logger = Mock()
    monkeypatch.setattr('kombu.transport.redis_cluster.logger', mock_logger)

    with pytest.raises(TimeoutError):
        channel.parse_response(conn, 'BRPOP')

    mock_logger.exception.assert_called_once()
    mock_logger.debug.assert_not_called()


def test_close_calls_brpop_read_with_is_close(monkeypatch):
    """
    Wiring check for the actual regression path: `close()` -> `_brpop_read`
    -> `parse_response` must carry `is_close=True` end to end, not just when
    `parse_response` is called directly.
    """
    nodes_manager = _NodesManager(SERVERLESS_SEEDS)
    conn = _make_conn(nodes_manager, TimeoutError('Timeout reading from socket'))
    channel = _make_channel()

    mock_logger = Mock()
    monkeypatch.setattr('kombu.transport.redis_cluster.logger', mock_logger)

    with pytest.raises(Empty):
        channel._brpop_read(conn=conn, is_close=True)

    mock_logger.exception.assert_not_called()
    mock_logger.debug.assert_called_once()
    assert conn.in_poll is False
