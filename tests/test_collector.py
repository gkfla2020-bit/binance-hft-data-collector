"""Collector 테스트
Feature: binance-data-collector
Property 1: 이중 타임스탬프 기록
Property 2: 지수 백오프 범위
"""

import json
import asyncio

import pytest
from hypothesis import given, strategies as st, settings

from src.collector import Collector
from src.models import DepthDiffEvent, AggTradeEvent


# ── Property 1: 이중 타임스탬프 기록 ──

class TestDualTimestamp:
    """Validates: Requirements 1.2, 1.3"""

    @given(
        event_time=st.integers(min_value=1, max_value=10**13),
        recv_time=st.floats(min_value=0.001, max_value=2e10),
    )
    @settings(max_examples=100)
    def test_depth_diff_has_both_timestamps(self, event_time, recv_time):
        """depth_diff 레코드에 event_time과 recv_time 모두 존재, recv_time > 0"""
        event = DepthDiffEvent(
            symbol="BTCUSDT", event_time=event_time, recv_time=recv_time,
            first_update_id=1, final_update_id=1, bids=[], asks=[],
        )
        assert event.event_time == event_time
        assert event.recv_time == recv_time
        assert event.recv_time > 0

    @given(
        trade_time=st.integers(min_value=1, max_value=10**13),
        recv_time=st.floats(min_value=0.001, max_value=2e10),
    )
    @settings(max_examples=100)
    def test_agg_trade_has_both_timestamps(self, trade_time, recv_time):
        """aggTrade 레코드에 trade_time과 recv_time 모두 존재, recv_time > 0"""
        event = AggTradeEvent(
            symbol="BTCUSDT", trade_id=1, price="100", quantity="1",
            first_trade_id=1, last_trade_id=1,
            trade_time=trade_time, recv_time=recv_time, is_buyer_maker=False,
        )
        assert event.trade_time == trade_time
        assert event.recv_time == recv_time
        assert event.recv_time > 0


# ── Property 2: 지수 백오프 범위 ──

class TestExponentialBackoff:
    """Validates: Requirements 1.4"""

    @given(attempt=st.integers(min_value=0, max_value=100))
    @settings(max_examples=200)
    def test_backoff_range(self, attempt):
        """딜레이는 min(2^N, 60), 항상 1 <= delay <= 60"""
        delay = Collector.compute_reconnect_delay(attempt)
        expected = min(2 ** attempt, 60.0)
        assert delay == expected
        assert 1.0 <= delay <= 60.0


# ── 단위 테스트 ──

class TestCollectorUnit:

    def test_build_ws_url(self):
        from src.config import Config
        from src.orderbook_manager import OrderBookManager
        from src.buffer import DataBuffer

        config = Config(symbols=["btcusdt", "ethusdt"])
        mgr = OrderBookManager(symbols=config.symbols)
        buf = DataBuffer()
        collector = Collector(config, mgr, buf)

        url = collector.build_ws_url()
        assert "btcusdt@depth@100ms" in url
        assert "ethusdt@aggTrade" in url
        assert "btcusdt@kline_1m" in url
        assert url.startswith("wss://")

    def test_build_futures_ws_url(self):
        from src.config import Config
        from src.orderbook_manager import OrderBookManager
        from src.buffer import DataBuffer

        config = Config(symbols=["btcusdt"])
        mgr = OrderBookManager(symbols=config.symbols)
        buf = DataBuffer()
        collector = Collector(config, mgr, buf)

        url = collector.build_futures_ws_url()
        assert "btcusdt@forceOrder" in url
        assert "fstream.binance.com" in url

    def test_reconnect_delay_increases(self):
        from src.config import Config
        from src.orderbook_manager import OrderBookManager
        from src.buffer import DataBuffer

        config = Config()
        mgr = OrderBookManager(symbols=config.symbols)
        buf = DataBuffer()
        collector = Collector(config, mgr, buf)

        assert collector.reconnect_delay == 1.0
        collector._increase_reconnect_delay()
        assert collector.reconnect_delay == 2.0
        collector._increase_reconnect_delay()
        assert collector.reconnect_delay == 4.0
        collector._reset_reconnect_delay()
        assert collector.reconnect_delay == 1.0
