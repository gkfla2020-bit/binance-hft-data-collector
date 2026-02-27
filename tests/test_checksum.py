"""파일 체크섬 테스트 - Task 13
Feature: binance-data-collector
Property 14: 체크섬 일관성
SHA-256 계산, checksums.json 누적 저장 검증
"""

import json
import tempfile
from pathlib import Path

import pytest
from hypothesis import given, settings, strategies as st

from src.config import Config
from src.buffer import DataBuffer
from src.flusher import Flusher


# ── Property 14: 체크섬 일관성 ──

class TestProperty14ChecksumConsistency:
    """Property 14: 체크섬 일관성
    *For any* 저장된 Parquet 파일에 대해, checksums.json에 기록된 SHA-256 해시를
    파일에서 다시 계산한 해시와 비교하면 동일해야 한다.
    Validates: Requirements 12.1, 12.2
    """

    @given(
        data=st.lists(
            st.fixed_dictionaries({
                "col_a": st.integers(min_value=0, max_value=10000),
                "col_b": st.text(min_size=1, max_size=20),
            }),
            min_size=1,
            max_size=50,
        )
    )
    @settings(max_examples=50)
    def test_checksum_matches_recomputed(self, data):
        """저장 후 체크섬 기록 → 재계산 시 동일"""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = Config(data_dir=tmpdir)
            buf = DataBuffer()
            flusher = Flusher(config, buf)

            fpath = Path(tmpdir) / "test_data.parquet"
            Flusher._save_parquet(data, fpath)

            sha = Flusher.compute_checksum(fpath)
            flusher.record_checksum(fpath, sha, len(data), fpath.stat().st_size)

            # 재계산
            sha_recomputed = Flusher.compute_checksum(fpath)
            assert sha == sha_recomputed

            # checksums.json 확인
            checksum_file = Path(tmpdir) / "checksums.json"
            assert checksum_file.exists()
            with open(checksum_file) as f:
                entries = json.load(f)
            assert len(entries) == 1
            assert entries[0]["sha256"] == sha_recomputed


# ── 단위 테스트 ──

class TestChecksumUnit:

    def test_sha256_deterministic(self):
        """같은 파일은 항상 같은 해시"""
        with tempfile.TemporaryDirectory() as tmpdir:
            fpath = Path(tmpdir) / "test.parquet"
            data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
            Flusher._save_parquet(data, fpath)

            h1 = Flusher.compute_checksum(fpath)
            h2 = Flusher.compute_checksum(fpath)
            assert h1 == h2
            assert len(h1) == 64  # SHA-256 hex

    def test_checksums_json_accumulates(self):
        """checksums.json에 여러 파일 체크섬 누적"""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = Config(data_dir=tmpdir)
            buf = DataBuffer()
            flusher = Flusher(config, buf)

            for i in range(3):
                fpath = Path(tmpdir) / f"file_{i}.parquet"
                Flusher._save_parquet([{"x": i}], fpath)
                sha = Flusher.compute_checksum(fpath)
                flusher.record_checksum(fpath, sha, 1, fpath.stat().st_size)

            checksum_file = Path(tmpdir) / "checksums.json"
            with open(checksum_file) as f:
                entries = json.load(f)
            assert len(entries) == 3

    def test_different_files_different_checksums(self):
        """다른 내용의 파일은 다른 해시"""
        with tempfile.TemporaryDirectory() as tmpdir:
            f1 = Path(tmpdir) / "a.parquet"
            f2 = Path(tmpdir) / "b.parquet"
            Flusher._save_parquet([{"x": 1}], f1)
            Flusher._save_parquet([{"x": 999}], f2)

            assert Flusher.compute_checksum(f1) != Flusher.compute_checksum(f2)
