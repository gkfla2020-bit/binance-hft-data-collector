"""DataBuffer 테스트
Feature: binance-data-collector
Property 6: 버퍼 데이터 격리
Property 7: 플러시 후 버퍼 비움
"""

import asyncio
import pytest
from hypothesis import given, strategies as st, settings

from src.buffer import DataBuffer


# ── 전략 ──

symbol_st = st.sampled_from(["BTCUSDT", "ETHUSDT", "XRPUSDT"])
record_st = st.fixed_dictionaries({"ts": st.floats(min_value=1.0, max_value=2e9), "val": st.integers()})


def run_async(coro):
    return asyncio.run(coro)


# ── Property 6: 버퍼 데이터 격리 ──

class TestBufferIsolation:
    """Validates: Requirements 3.1, 3.2"""

    @given(
        symbol=symbol_st,
        ob_record=record_st,
        trade_record=record_st,
    )
    @settings(max_examples=100)
    def test_data_isolation(self, symbol, ob_record, trade_record):
        """심볼 S, 데이터타입 T의 레코드는 해당 버퍼에만 존재"""
        buf = DataBuffer()
        run_async(buf.add_orderbook(symbol, ob_record))
        run_async(buf.add_trade(symbol, trade_record))

        other_symbols = [s for s in ["BTCUSDT", "ETHUSDT", "XRPUSDT"] if s != symbol]

        # 오더북: 해당 심볼에만 존재
        assert len(buf._orderbook_data[symbol]) == 1
        assert buf._orderbook_data[symbol][0] == ob_record
        for other in other_symbols:
            assert len(buf._orderbook_data[other]) == 0

        # 체결: 해당 심볼에만 존재
        assert len(buf._trade_data[symbol]) == 1
        assert buf._trade_data[symbol][0] == trade_record
        for other in other_symbols:
            assert len(buf._trade_data[other]) == 0

        # 크로스 타입: 오더북 레코드가 체결에 없고 그 반대도 마찬가지
        assert ob_record not in buf._trade_data[symbol] or ob_record == trade_record


# ── Property 7: 플러시 후 버퍼 비움 ──

class TestBufferFlush:
    """Validates: Requirements 3.3"""

    @given(
        symbols=st.lists(symbol_st, min_size=1, max_size=3),
        records=st.lists(record_st, min_size=1, max_size=20),
    )
    @settings(max_examples=100)
    def test_flush_returns_all_and_clears(self, symbols, records):
        """flush 후 반환 데이터는 모든 레코드 포함, 버퍼는 비어있어야 함"""
        buf = DataBuffer()
        expected_counts = {}

        for i, rec in enumerate(records):
            sym = symbols[i % len(symbols)]
            run_async(buf.add_orderbook(sym, rec))
            expected_counts[sym] = expected_counts.get(sym, 0) + 1

        result = run_async(buf.flush())

        # 반환된 데이터에 모든 레코드 포함
        total_returned = sum(len(v) for v in result["orderbook"].values())
        assert total_returned == len(records)

        for sym, count in expected_counts.items():
            assert len(result["orderbook"].get(sym, [])) == count

        # flush 후 버퍼 비어있음
        assert len(buf._orderbook_data) == 0
        assert len(buf._trade_data) == 0
        assert len(buf._liquidation_data) == 0
        assert len(buf._kline_data) == 0
        assert len(buf._funding_data) == 0


# ── 단위 테스트 ──

class TestBufferUnit:

    def test_force_flush_threshold(self):
        buf = DataBuffer(max_memory_mb=0)  # 0MB = 항상 초과
        run_async(buf.add_orderbook("BTCUSDT", {"ts": 1.0}))
        assert buf.needs_force_flush()

    def test_empty_flush(self):
        buf = DataBuffer()
        result = run_async(buf.flush())
        assert result["orderbook"] == {}
        assert result["trade"] == {}
        assert result["funding"] == []
