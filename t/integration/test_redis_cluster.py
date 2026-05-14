from __future__ import annotations

import os
import queue
import random

import pytest
import redis

from unittest.mock import patch

from redis.exceptions import MovedError, AskError

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

    assert len(RedisClusterConnection.producer_connections) == 0
    assert len(RedisClusterConnection.consumer_connections) == 0
    with connection as conn:
        queue = conn.SimpleQueue('test_connectionerror')
        queue.put({'Hello': 'World'}, headers={'k1': 'v1'})
        _ = queue.get(timeout=1)

        assert len(RedisClusterConnection.producer_connections) == 1
        assert len(RedisClusterConnection.consumer_connections) == 1

    assert len(RedisClusterConnection.producer_connections) == 0
    assert len(RedisClusterConnection.consumer_connections) == 0


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
            assert conn.default_channel.consumer_clients[0].reinitialize_counter != 0


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


def test_multiple_consume():
    consumer_conn = kombu.Connection('redis-cluster://localhost:7000?alt=redis-cluster://localhost:8000')
    producer_conn1 = kombu.Connection('redis-cluster://localhost:7000')
    producer_conn2 = kombu.Connection('redis-cluster://localhost:8000')

    with producer_conn1 as producer:
        queue = producer.SimpleQueue('test_multiple_consume')
        queue.put({'Hello': 'World'}, headers={'k1': 'v1'})
        queue.close()

    with consumer_conn as consumer:
        queue = consumer.SimpleQueue('test_multiple_consume')
        message = queue.get(timeout=10)
        assert message.payload == {'Hello': 'World'}
        assert message.content_type == 'application/json'
        assert message.content_encoding == 'utf-8'
        assert message.headers == {'k1': 'v1'}
        message.ack()
        queue.close()

    with producer_conn2 as producer:
        queue = producer.SimpleQueue('test_multiple_consume1')
        queue.put({'Hello': 'World'}, headers={'k1': 'v1'})
        queue.close()

    with consumer_conn as consumer:
        queue = consumer.SimpleQueue('test_multiple_consume1')
        message = queue.get(timeout=10)
        assert message.payload == {'Hello': 'World'}
        assert message.content_type == 'application/json'
        assert message.content_encoding == 'utf-8'
        assert message.headers == {'k1': 'v1'}
        message.ack()
        queue.close()

def test_close_in_poll():
    send_connection = kombu.Connection('redis-cluster://localhost:7000')
    recv_connection = kombu.Connection('redis-cluster://localhost:7000')

    queue_name = f"test_close_in_poll_{random.randint(0, 10000)}"
    send_queue = send_connection.SimpleQueue(queue_name)
    recv_queue = recv_connection.SimpleQueue(queue_name)
    with pytest.raises(queue.Empty):
        recv_queue.get(timeout=0.1)  # Register brpop

    send_queue.put({'Hello': 'World'}, headers={'k1': 'v1'})
    recv_connection.close() # Should receive pending message from brpop

    message = recv_queue.get(timeout=0)
    assert message.payload == {'Hello': 'World'}
