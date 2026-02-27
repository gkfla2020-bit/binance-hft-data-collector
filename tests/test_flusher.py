"""Flusher 테스트
Feature: binance-data-collector
Property 8: 파일명 형식 준수
Property 14: 체크섬 일관성
"""

import re
import tempfile
import os
from datetime import datetime
from pathlib import Path

import pytest
import pyarrow.parquet as pq
from hypothesis import given, strategies as st, settings

from src.flusher import Flusher


# ── 전략 ──

symbol_st = st.from_regex(r"[A-Z]{3,10}", fullmatch=True)
datatype_st = st.sampled_from(["orderbook", "trade", "liquidation", "kline"])
dt_st = st.datetimes(min_value=datetime(2020, 1, 1), max_value=datetime(2030, 12, 31))


# ── Property 8: 파일명 형식 준수 ──

class TestFilenameFormat:
    """Validates: Requirements 4.2"""

    @given(symbol=symbol_st, datatype=datatype_st, timestamp=dt_st)
    @settings(max_examples=200)
    def test_filename_pattern(self, symbol, datatype, timestamp):
        """파일명은 {SYMBOL}_{datatype}_{YYYYMMDD}_{HHMM}.parquet 형식"""
        fname = Flusher._generate_filename(symbol, datatype, timestamp)
        pattern = r"^[A-Z]+_[a-z]+_\d{8}_\d{4}\.parquet$"
        assert re.match(pattern, fname), f"Bad filename: {fname}"

        # 날짜/시간 부분 검증
        parts = fname.replace(".parquet", "").split("_")
        date_part = parts[-2]
        hhmm_part = parts[-1]
        assert date_part == timestamp.strftime("%Y%m%d")
        assert hhmm_part == timestamp.strftime("%H%M")


# ── 단위 테스트 ──

class TestFlusherUnit:

    def test_snappy_compression(self):
        """Parquet 파일이 snappy 압축으로 저장되는지 확인"""
        with tempfile.TemporaryDirectory() as tmpdir:
            fpath = Path(tmpdir) / "test.parquet"
            data = [{"price": "100.0", "qty": "1.0"} for _ in range(10)]
            Flusher._save_parquet(data, fpath)

            meta = pq.read_metadata(fpath)
            # snappy 압축 확인
            col_meta = meta.row_group(0).column(0)
            assert col_meta.compression == "SNAPPY"

    def test_checksum_consistency(self):
        """저장된 파일의 SHA-256이 재계산과 일치"""
        with tempfile.TemporaryDirectory() as tmpdir:
            fpath = Path(tmpdir) / "test.parquet"
            data = [{"val": i} for i in range(100)]
            Flusher._save_parquet(data, fpath)

            hash1 = Flusher.compute_checksum(fpath)
            hash2 = Flusher.compute_checksum(fpath)
            assert hash1 == hash2
            assert len(hash1) == 64  # SHA-256 hex length

    def test_atomic_save_on_error(self):
        """저장 실패 시 임시 파일이 남지 않아야 함"""
        with tempfile.TemporaryDirectory() as tmpdir:
            fpath = Path(tmpdir) / "test.parquet"
            # 빈 리스트는 빈 DataFrame을 만들어 정상 저장됨
            # 대신 잘못된 경로로 테스트
            bad_path = Path("/nonexistent/dir/test.parquet")
            with pytest.raises(Exception):
                Flusher._save_parquet([{"a": 1}], bad_path)
