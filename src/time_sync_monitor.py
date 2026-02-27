"""시간 동기화 및 레이턴시 측정 모듈 - NTP 오프셋, 바이낸스 RTT"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import aiohttp

if TYPE_CHECKING:
    from src.config import Config
    from src.integrity_logger import IntegrityLogger
    from src.telegram_reporter import TelegramReporter

logger = logging.getLogger(__name__)

MEASURE_INTERVAL = 600  # 10분
NTP_ALERT_THRESHOLD = 0.1  # 100ms


class TimeSyncMonitor:
    """NTP 시간 동기화 및 네트워크 레이턴시 측정"""

    BINANCE_TIME_URL = "https://api.binance.com/api/v3/time"

    def __init__(self, config: Config, integrity_logger: IntegrityLogger | None = None,
                 telegram: TelegramReporter | None = None):
        self.config = config
        self.integrity_logger = integrity_logger
        self.telegram = telegram
        self.log_dir = Path(config.log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

    async def run(self) -> None:
        """10분 주기 측정 루프"""
        while True:
            try:
                ntp_offset = await self.measure_ntp_offset()
                binance_offset, rtt = await self.measure_binance_offset()
                await self.save_measurement(ntp_offset, binance_offset, rtt)

                if abs(ntp_offset) > NTP_ALERT_THRESHOLD:
                    logger.warning(f"[시간동기화] NTP 오프셋 경고: {ntp_offset:.3f}초")
                    if self.telegram:
                        await self.telegram.send_message(
                            f"⏰ NTP 오프셋 경고: {ntp_offset*1000:.1f}ms"
                        )
            except Exception as e:
                logger.error(f"[시간동기화] 측정 실패: {e}")
            await asyncio.sleep(MEASURE_INTERVAL)

    async def measure_ntp_offset(self) -> float:
        """NTP 서버와 로컬 시계 오프셋 측정 (초). ntplib 없으면 0.0 반환."""
        try:
            import ntplib
            client = ntplib.NTPClient()
            loop = asyncio.get_running_loop()
            response = await loop.run_in_executor(
                None, lambda: client.request("pool.ntp.org", version=3)
            )
            return response.offset
        except ImportError:
            logger.warning("[시간동기화] ntplib 미설치, NTP 오프셋 측정 건너뜀")
            return 0.0
        except Exception as e:
            logger.warning(f"[시간동기화] NTP 측정 실패: {e}")
            return 0.0

    async def measure_binance_offset(self) -> tuple[float, float]:
        """바이낸스 서버 시간 차이 및 ping RTT 측정. (offset_sec, rtt_sec)"""
        try:
            async with aiohttp.ClientSession() as session:
                t1 = time.time()
                async with session.get(
                    self.BINANCE_TIME_URL,
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    t2 = time.time()
                    data = await resp.json()
                    server_time = data.get("serverTime", 0) / 1000.0
                    rtt = t2 - t1
                    local_mid = (t1 + t2) / 2.0
                    offset = server_time - local_mid
                    return offset, rtt
        except Exception as e:
            logger.warning(f"[시간동기화] 바이낸스 측정 실패: {e}")
            return 0.0, 0.0

    async def save_measurement(self, ntp_offset: float,
                               binance_offset: float, rtt: float) -> None:
        """측정 결과를 time_sync_{YYYYMMDD}.json에 저장"""
        now = datetime.now(timezone.utc)
        filename = f"time_sync_{now.strftime('%Y%m%d')}.json"
        filepath = self.log_dir / filename

        entry = {
            "timestamp": now.isoformat(),
            "ntp_offset_sec": ntp_offset,
            "binance_offset_sec": binance_offset,
            "rtt_sec": rtt,
        }

        entries = []
        if filepath.exists():
            with open(filepath, "r") as f:
                entries = json.load(f)
        entries.append(entry)
        with open(filepath, "w") as f:
            json.dump(entries, f, indent=2)
