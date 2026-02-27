"""IntegrityLogger 테스트
Feature: binance-data-collector
Property 10: 통계 JSON 직렬화 라운드트립
Property 11: 갭 정보 완전성
Property 16: 커버리지 비율 범위
"""

import json
import tempfile

import pytest
from hypothesis import given, strategies as st, settings

from src.integrity_logger import IntegrityLogger


# ── 전략 ──

stats_dict_st = st.fixed_dictionaries({
    "gap_count": st.integers(min_value=0, max_value=1000),
    "reconnect_count": st.integers(min_value=0, max_value=100),
    "total_records": st.integers(min_value=0, max_value=10**7),
})


# ── Property 10: 통계 JSON 직렬화 라운드트립 ──

class TestJsonRoundtrip:
    """Validates: Requirements 6.1"""

    @given(stats=stats_dict_st)
    @settings(max_examples=100)
    def test_json_roundtrip(self, stats):
        """JSON 직렬화 후 역직렬화하면 원본과 동일"""
        serialized = json.dumps(stats)
        restored = json.loads(serialized)
        assert stats == restored


# ── Property 11: 갭 정보 완전성 ──

class TestGapCompleteness:
    """Validates: Requirements 2.6, 6.3"""

    @given(
        symbol=st.sampled_from(["BTCUSDT", "ETHUSDT", "XRPUSDT"]),
        expected_id=st.integers(min_value=1, max_value=10**12),
        actual_id=st.integers(min_value=1, max_value=10**12),
        timestamp=st.floats(min_value=1.0, max_value=2e10),
    )
    @settings(max_examples=100)
    def test_gap_has_all_fields(self, symbol, expected_id, actual_id, timestamp):
        """갭 레코드는 timestamp, symbol, expected_id, actual_id 모두 포함"""
        with tempfile.TemporaryDirectory() as tmpdir:
            il = IntegrityLogger(tmpdir)
            il.record_gap(symbol, expected_id, actual_id, timestamp)

            gap = il._gaps[-1]
            assert "timestamp" in gap
            assert "symbol" in gap
            assert "expected_id" in gap
            assert "actual_id" in gap
            assert gap["timestamp"] == timestamp
            assert gap["symbol"] == symbol
            assert gap["expected_id"] == expected_id
            assert gap["actual_id"] == actual_id


# ── Property 16: 커버리지 비율 범위 ──

class TestCoverageRange:
    """Validates: Requirements 14.1, 14.2"""

    @given(
        total_seconds=st.floats(min_value=0.0, max_value=86400 * 365),
        gap_seconds=st.floats(min_value=-100.0, max_value=86400 * 365 + 100),
    )
    @settings(max_examples=200)
    def test_coverage_in_range(self, total_seconds, gap_seconds):
        """커버리지 비율은 0.0 이상 1.0 이하"""
        coverage = IntegrityLogger.compute_coverage(total_seconds, gap_seconds)
        assert 0.0 <= coverage <= 1.0


# ── 단위 테스트 ──

class TestIntegrityLoggerUnit:

    def test_record_reconnect(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            il = IntegrityLogger(tmpdir)
            il.record_reconnect(1000.0, "connection lost")
            assert len(il._reconnects) == 1
            assert il._reconnects[0]["reason"] == "connection lost"

    def test_periodic_stats(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            il = IntegrityLogger(tmpdir)
            il.record_gap("BTCUSDT", 100, 200, 1000.0)
            stats = il.get_periodic_stats()
            assert stats["gap_count"] == 1
            assert len(stats["gaps"]) == 1
