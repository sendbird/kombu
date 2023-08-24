from __future__ import annotations

import os
from case import patch

import pytest
import redis
from redis.exceptions import MovedError, AskError
from redis.crc import key_slot, REDIS_CLUSTER_HASH_SLOTS

import kombu

from .common import (BasicFunctionality)


def get_connection(
        hostname, port, user_name=None, password=None,
        transport_options=None):

    credentials = f'{user_name}:{password}@' if user_name else ''

    return kombu.Connection(
        f'redis-cluster://{credentials}{hostname}:{port}',
        transport_options=transport_options
    )


@pytest.fixture()
def connection():
    # this fixture yields plain connections to broker and TLS encrypted
    return get_connection(
        hostname=os.environ.get('REDIS_HOST', 'localhost'),
        port=os.environ.get('REDIS_6379_TCP', '7000')
    )


@pytest.fixture()
def invalid_connection():
    return kombu.Connection('redis-cluster://localhost:12345')


def test_brpop_timeout():
    def patched_brpop_start(self, timeout):
        assert timeout == 10


    with patch('kombu.transport.redis_cluster.Channel._brpop_start', patched_brpop_start):
        conn = kombu.Connection('redis-cluster://localhost:7000', transport_options={'brpop_timeout': 10})

        queue = conn.SimpleQueue('test_connectionerror')
        queue.put({'Hello': 'World'}, headers={'k1': 'v1'})
        try:
            _ = queue.get(timeout=1)
        except queue.Empty:
            pass

        conn.close()


def test_connection_reuse(connection):
    from kombu.transport.redis_cluster import RedisClusterConnection

    assert len(RedisClusterConnection.connections) == 0
    with connection as conn:
        queue = conn.SimpleQueue('test_connectionerror')
        queue.put({'Hello': 'World'}, headers={'k1': 'v1'})
        _ = queue.get(timeout=1)

        assert len(RedisClusterConnection.connections) == 1

    assert len(RedisClusterConnection.connections) == 0


def test_brpop_send_error(connection):
    with connection as conn:
        queue = conn.SimpleQueue('test_connectionerror')
        queue.put({'Hello': 'World'}, headers={'k1': 'v1'})

        original_send_command = redis.connection.Connection.send_command
        def send_command(*args, **kwargs):
            if args[1] == 'BRPOP':
                raise redis.exceptions.ConnectionError()
            else:
                return original_send_command(*args)

        with patch('redis.connection.Connection.send_command', send_command):
            try:
                _ = queue.get(timeout=1)
            except queue.Empty:
                pass
            except:
                raise


def test_ssl_connection():
    def patched_init(self, **kwargs):
        assert kwargs['password'] == 'test_password'
        assert kwargs['host'] == 'localhost'
        assert kwargs['port'] == 7000
        assert kwargs['ssl'] is True

    with patch('redis.RedisCluster.__init__', patched_init):
        with patch('redis.RedisCluster.execute_command'):
            conn = kombu.Connection('rediss-cluster://:test_password@localhost:7000')
            conn.default_channel

def test_connectionerror(connection):
    with connection as conn:
        queue = conn.SimpleQueue('test_connectionerror')
        queue.put({'Hello': 'World'}, headers={'k1': 'v1'})

        original_parse_response = redis.Redis.parse_response
        def parse_response(*args, **kwargs):
            if args[2] == 'BRPOP':
                raise redis.exceptions.ConnectionError()
            else:
                return original_parse_response(*args)

        with patch('redis.Redis.parse_response', parse_response):
            try:
                _ = queue.get(timeout=1)
            except queue.Empty:
                pass
            except:
                raise

def test_movederror(connection):
    with connection as conn:
        queue = conn.SimpleQueue('test_movederror')
        queue.put({'Hello': 'World'}, headers={'k1': 'v1'})

        original_parse_response = redis.Redis.parse_response

        def parse_response(*args, **kwargs):
            if args[2] == 'BRPOP':
                slot = 123
                r_host = 'nosuchhost'
                r_port = 7001

                raise MovedError(f"{slot} {r_host}:{r_port}")
            else:
                return original_parse_response(*args)

        with patch('redis.Redis.parse_response', parse_response):
            try:
                message = queue.get(timeout=1)
            except queue.Empty:
                pass
            except:
                raise
            assert conn.default_channel.client.reinitialize_counter != 0


def test_askerror(connection):
    with connection as conn:
        queue = conn.SimpleQueue('test_askerror')
        queue.put({'Hello': 'World'}, headers={'k1': 'v1'})

        original_parse_response = redis.Redis.parse_response

        def parse_response(*args, **kwargs):
            if args[2] == 'BRPOP':
                slot = 123
                r_host = 'nosuchhost'
                r_port = 7001

                raise AskError(f"{slot} {r_host}:{r_port}")
            else:
                return original_parse_response(*args)

        with patch('redis.Redis.parse_response', parse_response):
            try:
                message = queue.get(timeout=1)
            except queue.Empty:
                pass
            except:
                raise
            assert conn.default_channel.ask_errors.get('test_askerror') is not None


@pytest.mark.env('redis-cluster')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_RedisBasicFunctionality(BasicFunctionality):
    def test_failed_connection__ConnectionError(self, invalid_connection):
        # method raises transport exception
        with pytest.raises(redis.exceptions.RedisClusterException) as ex:
            invalid_connection.connection


def test_many_queue(connection):
    with connection as conn:
        queues = []
        for i in range(50):
            queue = conn.SimpleQueue(f'simple_queue_test_{i}')
            queue.put({'Hello': 'World'}, headers={'k1': 'v1'})

            queues.append(queue)

        for i in range(50):
            message = queues[i].get(timeout=10)
            assert message.payload == {'Hello': 'World'}
            assert message.content_type == 'application/json'
            assert message.content_encoding == 'utf-8'
            assert message.headers == {'k1': 'v1'}
            message.ack()


def test_physical_queue_names_precomputed():
    queues = {}
    remaining = REDIS_CLUSTER_HASH_SLOTS
    for i in range(0, 2**32):
        key = f'test:{{queue{i}}}'
        keyslot = key_slot(key.encode('utf-8'))

        if keyslot not in queues:
            queues[keyslot] = key
            remaining -= 1

        if remaining == 0:
            break

    conn = kombu.Connection('redis-cluster://localhost:7000', transport_options={'queue_names_per_slot': {'test': queues}})
    conn.default_channel._active_queues.append('test')
    queues = conn.default_channel.get_physical_queues()
    assert queues == {'test': ['test:{queue937}', 'test:{queue20909}', 'test:{queue9161}']}

    conn.close()


def test_physical_queue_names():
    conn = kombu.Connection('redis-cluster://localhost:7000')
    conn.default_channel._active_queues.append('test')
    queues = conn.default_channel.get_physical_queues()
    assert queues == {'test': ['test']}

    conn.close()
