"""클라우드 동기화 및 로컬 정리 모듈 - rclone 업로드, 실패 대기열, 오래된 파일 삭제"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path

from src.config import Config
from src.integrity_logger import IntegrityLogger

logger = logging.getLogger(__name__)


class Syncer:
    """클라우드 동기화 및 로컬 정리"""

    def __init__(self, config: Config, integrity_logger: IntegrityLogger):
        self.config = config
        self.logger = integrity_logger
        self._pending_queue: list[Path] = []
        self._synced_files: set[str] = set()

    def enqueue_file(self, filepath: Path) -> None:
        """Flusher 콜백 - 새 파일을 동기화 대기열에 추가"""
        self._pending_queue.append(filepath)

    async def run(self) -> None:
        """주기적 동기화 루프 - 대기열의 파일을 동기화하고 오래된 파일 정리"""
        while True:
            # 대기열에 있는 파일들 재시도
            retry_queue = list(self._pending_queue)
            self._pending_queue.clear()
            for filepath in retry_queue:
                if filepath.exists():
                    await self.sync_file(filepath)

            await self.cleanup_old_files()
            await asyncio.sleep(self.config.flush_interval)

    async def sync_file(self, filepath: Path) -> bool:
        """단일 파일 클라우드 업로드. 성공 시 True, 실패 시 False."""
        remote = self.config.cloud_remote
        cloud_path = self.config.cloud_path

        if not remote:
            logger.warning("[Syncer] cloud_remote 미설정, 동기화 건너뜀")
            return False

        cmd = [
            "rclone", "copy",
            str(filepath),
            f"{remote}:{cloud_path}",
        ]

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await proc.communicate()

            if proc.returncode == 0:
                self._synced_files.add(str(filepath))
                self.logger.record_sync(str(filepath), "success")
                logger.info(f"[Syncer] 업로드 성공: {filepath}")
                return True
            else:
                self._pending_queue.append(filepath)
                self.logger.record_sync(str(filepath), "failed")
                logger.error(
                    f"[Syncer] 업로드 실패: {filepath} "
                    f"returncode={proc.returncode} stderr={stderr.decode()}"
                )
                return False
        except Exception as e:
            self._pending_queue.append(filepath)
            self.logger.record_sync(str(filepath), "failed")
            logger.error(f"[Syncer] 업로드 예외: {filepath} {e}")
            return False

    async def cleanup_old_files(self) -> None:
        """cleanup_days 이상 경과하고 동기화 완료된 파일만 삭제"""
        data_dir = Path(self.config.data_dir)
        if not data_dir.exists():
            return

        now = time.time()
        cutoff_seconds = self.config.cleanup_days * 86400

        for filepath in data_dir.glob("*.parquet"):
            file_age = now - filepath.stat().st_mtime
            is_old_enough = file_age >= cutoff_seconds
            is_synced = str(filepath) in self._synced_files

            if is_old_enough and is_synced:
                try:
                    filepath.unlink()
                    logger.info(f"[Syncer] 삭제: {filepath}")
                except OSError as e:
                    logger.error(f"[Syncer] 삭제 실패: {filepath} {e}")

    def get_files_to_delete(self, files: list[dict]) -> list[str]:
        """삭제 대상 파일 판별 - 테스트용 순수 함수.

        Args:
            files: 각 항목은 {"path": str, "age_days": float, "synced": bool}

        Returns:
            삭제 대상 파일 경로 목록
        """
        return [
            f["path"]
            for f in files
            if f["age_days"] >= self.config.cleanup_days and f["synced"]
        ]
