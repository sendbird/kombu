from __future__ import annotations

import os

import pytest
import redis

from case import patch

from redis.exceptions import MovedError, AskError

import kombu
from kombu.transport.redis_cluster import Transport

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

def test_movederror(connection):
    with connection as conn:
        queue = conn.SimpleQueue('test_movederror')
        queue.put({'Hello': 'World'}, headers={'k1': 'v1'})

        def parse_response(*args, **kwargs):
            slot = 123
            r_host = 'nosuchhost'
            r_port = 7001

            raise MovedError(f"{slot} {r_host}:{r_port}")

        with patch('redis.Redis.parse_response', parse_response):
            try:
                message = queue.get(timeout=1)
            except Exception as e:
                pass
            assert conn.default_channel.client.reinitialize_counter != 0


def test_askerror(connection):
    with connection as conn:
        queue = conn.SimpleQueue('test_askerror')
        queue.put({'Hello': 'World'}, headers={'k1': 'v1'})

        def parse_response(*args, **kwargs):
            slot = 123
            r_host = 'nosuchhost'
            r_port = 7001

            raise AskError(f"{slot} {r_host}:{r_port}")

        with patch('redis.Redis.parse_response', parse_response):
            try:
                message = queue.get(timeout=1)
            except Exception as e:
                pass
            assert conn.default_channel.ask_errors.get('test_askerror') is not None


@pytest.mark.env('redis-cluster')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_RedisBasicFunctionality(BasicFunctionality):
    def test_failed_connection__ConnectionError(self, invalid_connection):
        # method raises transport exception
        with pytest.raises(redis.exceptions.RedisClusterException) as ex:
            invalid_connection.connection
        assert ex.type in Transport.connection_errors


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
