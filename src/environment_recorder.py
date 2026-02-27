"""수집 환경 메타데이터 기록 모듈 - OS, Python, 패키지, CPU/RAM, 설정 정보"""

from __future__ import annotations

import json
import logging
import os
import platform
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.config import Config

logger = logging.getLogger(__name__)


class EnvironmentRecorder:
    """수집 환경 메타데이터 기록"""

    SENSITIVE_FIELDS = {"telegram_bot_token", "telegram_chat_id"}

    def __init__(self, config: Config, log_dir: Path | str):
        self.config = config
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

    def record(self) -> Path:
        """환경 메타데이터를 JSON으로 저장, 파일 경로 반환"""
        now = datetime.now(timezone.utc)
        filename = f"environment_{now.strftime('%Y%m%d_%H%M%S')}.json"
        filepath = self.log_dir / filename

        metadata = {
            "timestamp": now.isoformat(),
            "system": self._get_system_info(),
            "python": self._get_python_info(),
            "config": self._mask_sensitive_config(self.config),
        }

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

        logger.info(f"[환경] 메타데이터 저장: {filepath}")
        return filepath

    @staticmethod
    def _get_system_info() -> dict:
        """OS, CPU, RAM 정보 수집"""
        info = {
            "os": platform.system(),
            "os_version": platform.version(),
            "os_release": platform.release(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "cpu_count": os.cpu_count(),
        }
        try:
            import psutil
            mem = psutil.virtual_memory()
            info["ram_total_gb"] = round(mem.total / (1024**3), 2)
            info["ram_available_gb"] = round(mem.available / (1024**3), 2)
        except ImportError:
            info["ram_total_gb"] = "psutil 미설치"
        return info

    @staticmethod
    def _get_python_info() -> dict:
        """Python 버전, 패키지 목록 수집"""
        info = {
            "version": sys.version,
            "executable": sys.executable,
        }
        try:
            import importlib.metadata
            packages = {
                d.metadata["Name"]: d.metadata["Version"]
                for d in importlib.metadata.distributions()
            }
            info["packages"] = packages
        except Exception:
            info["packages"] = {}
        return info

    @staticmethod
    def _mask_sensitive_config(config: Config) -> dict:
        """텔레그램 토큰 등 민감 정보 마스킹"""
        d = config.to_dict()
        for field in EnvironmentRecorder.SENSITIVE_FIELDS:
            if field in d and d[field]:
                d[field] = "***"
        return d
