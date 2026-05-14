from __future__ import annotations

from queue import Empty
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

pytest.importorskip('redis')

from kombu.transport import redis_cluster


class Conn:
    def __init__(self, key):
        self.in_poll = True
        self.key = key


def test_channel_close_uses_snapshot_of_cluster_poll_map(monkeypatch):
    monkeypatch.setattr(redis_cluster.RedisChannel, 'close', lambda self: None)
    monkeypatch.setattr(redis_cluster.RedisClusterConnection, 'close', Mock())

    channel = object.__new__(redis_cluster.Channel)
    channel.client = Mock()
    channel.consumer_created = False

    conn1 = Conn('queue-1')
    conn2 = Conn('queue-2')
    chan_to_sock = {
        (channel, conn1, 'BRPOP'): Mock(),
        (channel, conn2, 'BRPOP'): Mock(),
    }
    channel.connection = SimpleNamespace(
        cycle=SimpleNamespace(_chan_to_sock=chan_to_sock),
    )

    def brpop_read(conn):
        chan_to_sock.popitem()
        raise Empty()

    channel._brpop_read = Mock(side_effect=brpop_read)

    channel.close()

    assert channel._brpop_read.call_count == 2
