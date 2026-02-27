"""Parquet 파일 저장 모듈 - 주기적 플러시, 파일명 생성, snappy 압축"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Callable

import pandas as pd

if TYPE_CHECKING:
    from src.buffer import DataBuffer
    from src.config import Config
    from src.integrity_logger import IntegrityLogger
    from src.syncer import Syncer

logger = logging.getLogger(__name__)


class Flusher:
    """주기적 Parquet 파일 저장"""

    def __init__(self, config: Config, buffer: DataBuffer,
                 integrity_logger: IntegrityLogger | None = None,
                 on_file_created: Callable[[Path], None] | None = None):
        self.config = config
        self.buffer = buffer
        self.integrity_logger = integrity_logger
        self.on_file_created = on_file_created  # Syncer 연결용 콜백
        self.data_dir = Path(config.data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

    async def run(self) -> None:
        """주기적 플러시 루프"""
        while True:
            await asyncio.sleep(self.config.flush_interval)
            try:
                await self.flush_now()
            except Exception as e:
                logger.error(f"[플러시 에러] {e}")

    async def flush_now(self) -> list[Path]:
        """즉시 플러시 실행, 생성된 파일 경로 반환"""
        data = await self.buffer.flush()
        now = datetime.now(timezone.utc)
        created_files = []

        # 심볼별 데이터 저장 (오더북, 체결, 청산, 캔들)
        for datatype in ["orderbook", "trade", "liquidation", "kline"]:
            symbol_data = data.get(datatype, {})
            if isinstance(symbol_data, dict):
                for symbol, records in symbol_data.items():
                    if not records:
                        continue
                    fname = self._generate_filename(symbol, datatype, now)
                    fpath = self.data_dir / fname
                    count = self._save_parquet(records, fpath)
                    file_size = fpath.stat().st_size
                    created_files.append(fpath)
                    logger.info(f"[저장] {fpath} ({count}건)")

                    # 체크섬 기록 (#2)
                    sha256 = self.compute_checksum(fpath)
                    self.record_checksum(fpath, sha256, count, file_size)

                    # IntegrityLogger 통보 (#3)
                    if self.integrity_logger:
                        time_range = (0.0, 0.0)
                        if records:
                            times = [r.get("recv_time", 0.0) or r.get("event_time", 0.0) for r in records]
                            times = [t for t in times if t]
                            if times:
                                time_range = (min(times), max(times))
                        self.integrity_logger.record_flush(
                            symbol=symbol, datatype=datatype,
                            record_count=count, file_size=file_size,
                            time_range=time_range,
                        )

                    # Syncer 콜백 (#4)
                    if self.on_file_created:
                        self.on_file_created(fpath)

        # 펀딩비 (심볼 통합)
        funding = data.get("funding", [])
        if funding:
            fname = f"funding_rate_{now.strftime('%Y%m%d_%H%M')}.parquet"
            fpath = self.data_dir / fname
            count = self._save_parquet(funding, fpath)
            file_size = fpath.stat().st_size
            created_files.append(fpath)
            logger.info(f"[저장] {fpath} ({count}건)")

            sha256 = self.compute_checksum(fpath)
            self.record_checksum(fpath, sha256, count, file_size)

            if self.on_file_created:
                self.on_file_created(fpath)

        return created_files

    @staticmethod
    def _generate_filename(symbol: str, datatype: str, timestamp: datetime) -> str:
        """파일명 생성: {SYMBOL}_{datatype}_{YYYYMMDD}_{HHMM}.parquet"""
        return f"{symbol.upper()}_{datatype}_{timestamp.strftime('%Y%m%d_%H%M')}.parquet"

    @staticmethod
    def _save_parquet(data: list[dict], filepath: Path) -> int:
        """Parquet 저장 (snappy 압축), 레코드 수 반환. 원자적 저장."""
        df = pd.DataFrame(data)
        # 임시 파일에 먼저 쓰고 rename (원자적 저장)
        tmp_fd, tmp_path = tempfile.mkstemp(
            suffix=".parquet.tmp", dir=filepath.parent
        )
        os.close(tmp_fd)
        try:
            df.to_parquet(tmp_path, index=False, compression="snappy")
            os.replace(tmp_path, filepath)
        except Exception:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise
        return len(df)

    @staticmethod
    def compute_checksum(filepath: Path) -> str:
        """SHA-256 해시 계산"""
        h = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()

    def record_checksum(self, filepath: Path, sha256: str,
                        record_count: int, file_size: int) -> None:
        """checksums.json에 체크섬 기록 추가"""
        checksum_file = self.data_dir / "checksums.json"
        entries = []
        if checksum_file.exists():
            with open(checksum_file, "r") as f:
                entries = json.load(f)
        entries.append({
            "filename": filepath.name,
            "sha256": sha256,
            "record_count": record_count,
            "file_size": file_size,
            "created_at": datetime.now(timezone.utc).isoformat(),
        })
        with open(checksum_file, "w") as f:
            json.dump(entries, f, indent=2)
