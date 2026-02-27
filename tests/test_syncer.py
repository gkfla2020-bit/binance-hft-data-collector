"""Syncer 모듈 테스트 - Property 9 (파일 삭제 조건) + 단위 테스트"""

from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from hypothesis import given, settings, assume
from hypothesis import strategies as st

from src.config import Config
from src.integrity_logger import IntegrityLogger
from src.syncer import Syncer


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def config(tmp_path):
    return Config(
        data_dir=str(tmp_path / "data"),
        cloud_remote="myremote",
        cloud_path="backup/data",
        cleanup_days=7,
    )


@pytest.fixture
def integrity_logger(tmp_path):
    return IntegrityLogger(log_dir=tmp_path / "logs")


@pytest.fixture
def syncer(config, integrity_logger):
    return Syncer(config, integrity_logger)


# ---------------------------------------------------------------------------
# Hypothesis strategies
# ---------------------------------------------------------------------------

def unique_file_list_strategy():
    """고유한 경로를 가진 파일 목록 생성"""
    return st.lists(
        st.tuples(
            st.floats(min_value=0.0, max_value=365.0, allow_nan=False, allow_infinity=False),
            st.booleans(),
        ),
        min_size=0,
        max_size=30,
    ).map(lambda items: [
        {"path": f"/data/file_{i}.parquet", "age_days": age, "synced": synced}
        for i, (age, synced) in enumerate(items)
    ])


# ---------------------------------------------------------------------------
# PBT: Property 9 - 파일 삭제 조건
# **Validates: Requirements 5.3**
# ---------------------------------------------------------------------------

class TestProperty9FileDeletionCondition:
    """Property 9: 파일 삭제 조건

    *For any* 로컬 파일 목록에서, 삭제 대상은 (a) 생성일이 7일 이상 경과하고
    (b) 클라우드 동기화가 완료된 파일만이어야 한다.
    두 조건 중 하나라도 충족하지 않는 파일은 삭제되지 않아야 한다.
    """

    @given(files=unique_file_list_strategy())
    @settings(max_examples=200)
    def test_only_old_and_synced_files_are_deleted(self, files):
        """**Validates: Requirements 5.3**

        삭제 대상은 age_days >= cleanup_days AND synced == True인 파일만이어야 한다.
        """
        config = Config(cleanup_days=7)
        logger = IntegrityLogger(log_dir="/tmp/test_logs")
        syncer = Syncer(config, logger)

        to_delete = syncer.get_files_to_delete(files)
        to_delete_set = set(to_delete)

        for f in files:
            should_delete = f["age_days"] >= 7 and f["synced"]
            if should_delete:
                assert f["path"] in to_delete_set, (
                    f"파일이 삭제 대상이어야 함: age={f['age_days']}, synced={f['synced']}"
                )
            else:
                assert f["path"] not in to_delete_set, (
                    f"파일이 삭제되면 안 됨: age={f['age_days']}, synced={f['synced']}"
                )

    @given(files=unique_file_list_strategy())
    @settings(max_examples=200)
    def test_not_old_enough_files_never_deleted(self, files):
        """**Validates: Requirements 5.3**

        cleanup_days 미만 파일은 동기화 여부와 관계없이 삭제되지 않아야 한다.
        """
        config = Config(cleanup_days=7)
        logger = IntegrityLogger(log_dir="/tmp/test_logs")
        syncer = Syncer(config, logger)

        to_delete = set(syncer.get_files_to_delete(files))

        for f in files:
            if f["age_days"] < 7:
                assert f["path"] not in to_delete

    @given(files=unique_file_list_strategy())
    @settings(max_examples=200)
    def test_unsynced_files_never_deleted(self, files):
        """**Validates: Requirements 5.3**

        동기화되지 않은 파일은 경과일과 관계없이 삭제되지 않아야 한다.
        """
        config = Config(cleanup_days=7)
        logger = IntegrityLogger(log_dir="/tmp/test_logs")
        syncer = Syncer(config, logger)

        to_delete = set(syncer.get_files_to_delete(files))

        for f in files:
            if not f["synced"]:
                assert f["path"] not in to_delete


# ---------------------------------------------------------------------------
# 단위 테스트: rclone 명령어 구성
# ---------------------------------------------------------------------------

class TestRcloneCommandConstruction:
    """rclone 명령어가 올바르게 구성되는지 검증"""

    def test_sync_file_builds_correct_rclone_command(self, syncer, tmp_path):
        """rclone copy {filepath} {remote}:{cloud_path} 형식으로 명령어가 구성되어야 한다."""
        data_dir = tmp_path / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        test_file = data_dir / "BTCUSDT_orderbook_20240115_10.parquet"
        test_file.write_bytes(b"fake parquet data")

        captured_cmd = []

        async def mock_subprocess(*cmd, **kwargs):
            captured_cmd.extend(cmd)
            proc = MagicMock()
            proc.returncode = 0
            proc.communicate = AsyncMock(return_value=(b"", b""))
            return proc

        async def run_test():
            with patch("asyncio.create_subprocess_exec", side_effect=mock_subprocess):
                return await syncer.sync_file(test_file)

        result = asyncio.run(run_test())

        assert result is True
        assert captured_cmd[0] == "rclone"
        assert captured_cmd[1] == "copy"
        assert captured_cmd[2] == str(test_file)
        assert captured_cmd[3] == "myremote:backup/data"

    def test_sync_file_skips_when_no_remote(self, tmp_path, integrity_logger):
        """cloud_remote가 비어있으면 동기화를 건너뛰어야 한다."""
        config = Config(cloud_remote="", data_dir=str(tmp_path / "data"))
        syncer = Syncer(config, integrity_logger)

        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"data")

        result = asyncio.run(syncer.sync_file(test_file))
        assert result is False


# ---------------------------------------------------------------------------
# 단위 테스트: 동기화 실패 대기열
# ---------------------------------------------------------------------------

class TestSyncFailureQueue:
    """동기화 실패 시 대기열에 추가되는지 검증"""

    def test_failed_sync_adds_to_pending_queue(self, syncer, tmp_path):
        """rclone 실패 시 파일이 _pending_queue에 추가되어야 한다."""
        data_dir = tmp_path / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        test_file = data_dir / "BTCUSDT_orderbook_20240115_10.parquet"
        test_file.write_bytes(b"fake data")

        async def mock_subprocess(*cmd, **kwargs):
            proc = MagicMock()
            proc.returncode = 1
            proc.communicate = AsyncMock(return_value=(b"", b"upload error"))
            return proc

        async def run_test():
            with patch("asyncio.create_subprocess_exec", side_effect=mock_subprocess):
                return await syncer.sync_file(test_file)

        result = asyncio.run(run_test())

        assert result is False
        assert test_file in syncer._pending_queue

    def test_exception_during_sync_adds_to_pending_queue(self, syncer, tmp_path):
        """rclone 실행 중 예외 발생 시에도 대기열에 추가되어야 한다."""
        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"data")

        async def run_test():
            with patch("asyncio.create_subprocess_exec", side_effect=FileNotFoundError("rclone not found")):
                return await syncer.sync_file(test_file)

        result = asyncio.run(run_test())

        assert result is False
        assert test_file in syncer._pending_queue

    def test_successful_sync_adds_to_synced_files(self, syncer, tmp_path):
        """동기화 성공 시 _synced_files에 추가되어야 한다."""
        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"data")

        async def mock_subprocess(*cmd, **kwargs):
            proc = MagicMock()
            proc.returncode = 0
            proc.communicate = AsyncMock(return_value=(b"", b""))
            return proc

        async def run_test():
            with patch("asyncio.create_subprocess_exec", side_effect=mock_subprocess):
                return await syncer.sync_file(test_file)

        result = asyncio.run(run_test())

        assert result is True
        assert str(test_file) in syncer._synced_files
        assert test_file not in syncer._pending_queue

    def test_sync_records_event_in_integrity_logger(self, syncer, tmp_path):
        """동기화 결과가 IntegrityLogger에 기록되어야 한다."""
        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"data")

        async def mock_subprocess(*cmd, **kwargs):
            proc = MagicMock()
            proc.returncode = 0
            proc.communicate = AsyncMock(return_value=(b"", b""))
            return proc

        async def run_test():
            with patch("asyncio.create_subprocess_exec", side_effect=mock_subprocess):
                return await syncer.sync_file(test_file)

        asyncio.run(run_test())

        assert len(syncer.logger._sync_events) == 1
        assert syncer.logger._sync_events[0]["status"] == "success"


# ---------------------------------------------------------------------------
# 단위 테스트: cleanup_old_files
# ---------------------------------------------------------------------------

class TestCleanupOldFiles:
    """cleanup_old_files 메서드의 실제 파일 삭제 동작 검증"""

    def test_deletes_old_synced_files(self, syncer, tmp_path):
        """7일 이상 경과 + 동기화 완료 파일만 삭제되어야 한다."""
        data_dir = tmp_path / "data"
        data_dir.mkdir(parents=True, exist_ok=True)

        old_synced = data_dir / "old_synced.parquet"
        old_synced.write_bytes(b"data")
        old_time = time.time() - (8 * 86400)
        os.utime(old_synced, (old_time, old_time))
        syncer._synced_files.add(str(old_synced))

        old_unsynced = data_dir / "old_unsynced.parquet"
        old_unsynced.write_bytes(b"data")
        os.utime(old_unsynced, (old_time, old_time))

        new_synced = data_dir / "new_synced.parquet"
        new_synced.write_bytes(b"data")
        syncer._synced_files.add(str(new_synced))

        asyncio.run(syncer.cleanup_old_files())

        assert not old_synced.exists(), "오래되고 동기화된 파일은 삭제되어야 함"
        assert old_unsynced.exists(), "동기화되지 않은 파일은 삭제되면 안 됨"
        assert new_synced.exists(), "최근 파일은 삭제되면 안 됨"

    def test_no_deletion_when_data_dir_missing(self, syncer):
        """data_dir이 존재하지 않으면 에러 없이 종료되어야 한다."""
        asyncio.run(syncer.cleanup_old_files())  # 예외 없이 완료
