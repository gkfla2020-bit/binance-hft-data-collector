"""펀딩비 수집 모듈 - 8시간 주기 REST API 조회"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING

import aiohttp

from src.models import FundingRateRecord

if TYPE_CHECKING:
    from src.buffer import DataBuffer
    from src.config import Config
    from src.integrity_logger import IntegrityLogger

logger = logging.getLogger(__name__)

FUNDING_INTERVAL = 8 * 3600  # 8시간


class FundingRateCollector:
    """8시간 주기 펀딩비 수집 (REST API)"""

    FUTURES_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"

    def __init__(self, config: Config, buffer: DataBuffer,
                 integrity_logger: IntegrityLogger | None = None):
        self.config = config
        self.buffer = buffer
        self.integrity_logger = integrity_logger
        self.max_retries = 3

    async def run(self) -> None:
        """8시간 주기 펀딩비 조회 루프"""
        while True:
            for symbol in self.config.symbols:
                record = await self.fetch_funding_rate(symbol)
                if record:
                    from dataclasses import asdict
                    await self.buffer.add_funding_rate(asdict(record))
            await asyncio.sleep(FUNDING_INTERVAL)

    async def fetch_funding_rate(self, symbol: str) -> FundingRateRecord | None:
        """단일 심볼 펀딩비 조회 (최대 3회 재시도)"""
        sym = symbol.upper()
        for attempt in range(self.max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        self.FUTURES_URL,
                        params={"symbol": sym},
                        timeout=aiohttp.ClientTimeout(total=10),
                    ) as resp:
                        if resp.status != 200:
                            logger.warning(f"[펀딩비] {sym} HTTP {resp.status}")
                            continue
                        data = await resp.json()
                        return FundingRateRecord(
                            symbol=sym,
                            funding_rate=str(data.get("lastFundingRate", "0")),
                            funding_time=int(data.get("time", 0)),
                            next_funding_time=int(data.get("nextFundingTime", 0)),
                            recv_time=time.time(),
                        )
            except Exception as e:
                logger.warning(f"[펀딩비] {sym} 조회 실패 (시도 {attempt+1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
        return None
