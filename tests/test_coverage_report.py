"""커버리지 리포트 테스트 - Task 15
Feature: binance-data-collector
커버리지 계산, 누적 통계 갱신 검증
"""

import asyncio
import json
import tempfile
from pathlib import Path

import pytest

from src.integrity_logger import IntegrityLogger


class TestCoverageSummary:
    """coverage_summary.json 누적 갱신 검증"""

    def test_creates_coverage_summary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            il = IntegrityLogger(tmpdir)
            stats = {
                "BTCUSDT": {"total_seconds": 3600, "gap_seconds": 60, "msg_count": 35000},
                "ETHUSDT": {"total_seconds": 3600, "gap_seconds": 0, "msg_count": 30000},
            }
            asyncio.run(il.update_coverage_summary(stats))

            filepath = Path(tmpdir) / "coverage_summary.json"
            assert filepath.exists()
            with open(filepath) as f:
                data = json.load(f)
            assert "BTCUSDT" in data
            assert "ETHUSDT" in data
            assert 0.0 <= data["BTCUSDT"]["coverage"] <= 1.0
            assert data["ETHUSDT"]["coverage"] == 1.0

    def test_updates_existing_summary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            il = IntegrityLogger(tmpdir)

            # 첫 번째 갱신
            asyncio.run(il.update_coverage_summary({
                "BTCUSDT": {"total_seconds": 3600, "gap_seconds": 100, "msg_count": 35000},
            }))

            # 두 번째 갱신 (다른 심볼 추가)
            asyncio.run(il.update_coverage_summary({
                "ETHUSDT": {"total_seconds": 3600, "gap_seconds": 0, "msg_count": 30000},
            }))

            filepath = Path(tmpdir) / "coverage_summary.json"
            with open(filepath) as f:
                data = json.load(f)
            # 두 심볼 모두 존재
            assert "BTCUSDT" in data
            assert "ETHUSDT" in data

    def test_coverage_calculation_accuracy(self):
        """커버리지 = (total - gap) / total"""
        assert IntegrityLogger.compute_coverage(3600, 360) == pytest.approx(0.9)
        assert IntegrityLogger.compute_coverage(3600, 0) == 1.0
        assert IntegrityLogger.compute_coverage(3600, 3600) == 0.0
        assert IntegrityLogger.compute_coverage(0, 0) == 0.0

    def test_empty_stats_creates_empty_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            il = IntegrityLogger(tmpdir)
            asyncio.run(il.update_coverage_summary(None))

            filepath = Path(tmpdir) / "coverage_summary.json"
            assert filepath.exists()
            with open(filepath) as f:
                data = json.load(f)
            assert data == {}
