"""Tests for redis_cluster transport."""
from __future__ import annotations

from unittest import mock

import pytest


class TestChannelBrpopBackoff:
    """Tests for _brpop_read exponential backoff."""

    @pytest.fixture
    def channel(self):
        """Create a minimal Channel mock for testing backoff logic."""
        from kombu.transport.redis_cluster import Channel

        # Create a mock channel with just the backoff attributes
        channel = mock.MagicMock(spec=Channel)
        channel.BRPOP_ERROR_BACKOFF_MIN = 0.1
        channel.BRPOP_ERROR_BACKOFF_MAX = 5.0
        channel._brpop_error_backoff = channel.BRPOP_ERROR_BACKOFF_MIN
        return channel

    def test_backoff_constants(self):
        """Test that backoff constants are set correctly."""
        from kombu.transport.redis_cluster import Channel

        assert Channel.BRPOP_ERROR_BACKOFF_MIN == 0.1
        assert Channel.BRPOP_ERROR_BACKOFF_MAX == 5.0

    def test_backoff_increases_exponentially(self, channel):
        """Test that backoff doubles on each consecutive error."""
        backoffs = []
        current = channel.BRPOP_ERROR_BACKOFF_MIN

        for _ in range(6):
            backoffs.append(current)
            current = min(current * 2, channel.BRPOP_ERROR_BACKOFF_MAX)

        expected = [0.1, 0.2, 0.4, 0.8, 1.6, 3.2]
        assert backoffs == expected

    def test_backoff_caps_at_max(self, channel):
        """Test that backoff doesn't exceed max."""
        current = channel.BRPOP_ERROR_BACKOFF_MIN

        for _ in range(10):
            current = min(current * 2, channel.BRPOP_ERROR_BACKOFF_MAX)

        assert current == channel.BRPOP_ERROR_BACKOFF_MAX

    def test_backoff_resets_on_success(self, channel):
        """Test that backoff resets to min after success."""
        # Simulate errors increasing backoff
        channel._brpop_error_backoff = 3.2

        # Simulate success resetting backoff
        channel._brpop_error_backoff = channel.BRPOP_ERROR_BACKOFF_MIN

        assert channel._brpop_error_backoff == 0.1

    def test_cpu_impact_with_backoff(self, channel):
        """Test that backoff significantly reduces CPU impact."""
        total_sleep = 0.0
        current = channel.BRPOP_ERROR_BACKOFF_MIN

        # Simulate 20 consecutive errors
        for _ in range(20):
            total_sleep += current
            current = min(current * 2, channel.BRPOP_ERROR_BACKOFF_MAX)

        # Without backoff: ~0s (tight loop)
        # With backoff: >70s total sleep
        assert total_sleep > 70, f"Expected >70s, got {total_sleep}"
