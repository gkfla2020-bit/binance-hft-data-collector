"""메모리 버퍼 모듈 - 심볼별 데이터 분리 저장 및 플러시"""

from __future__ import annotations

import asyncio
import sys
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


class DataBuffer:
    """메모리 버퍼 - 심볼별 오더북/체결/청산/캔들/펀딩비 데이터 저장"""

    def __init__(self, max_memory_mb: int = 500):
        self.max_memory_bytes = max_memory_mb * 1024 * 1024
        self._orderbook_data: dict[str, list[dict]] = defaultdict(list)
        self._trade_data: dict[str, list[dict]] = defaultdict(list)
        self._liquidation_data: dict[str, list[dict]] = defaultdict(list)
        self._kline_data: dict[str, list[dict]] = defaultdict(list)
        self._funding_data: list[dict] = []
        self._lock = asyncio.Lock()

    async def add_orderbook(self, symbol: str, record: dict) -> None:
        async with self._lock:
            self._orderbook_data[symbol].append(record)

    async def add_trade(self, symbol: str, record: dict) -> None:
        async with self._lock:
            self._trade_data[symbol].append(record)

    async def add_liquidation(self, symbol: str, record: dict) -> None:
        async with self._lock:
            self._liquidation_data[symbol].append(record)

    async def add_kline(self, symbol: str, record: dict) -> None:
        async with self._lock:
            self._kline_data[symbol].append(record)

    async def add_funding_rate(self, record: dict) -> None:
        async with self._lock:
            self._funding_data.append(record)

    async def flush(self) -> dict[str, dict[str, list[dict]] | list[dict]]:
        """모든 데이터를 반환하고 버퍼 초기화"""
        async with self._lock:
            result = {
                "orderbook": dict(self._orderbook_data),
                "trade": dict(self._trade_data),
                "liquidation": dict(self._liquidation_data),
                "kline": dict(self._kline_data),
                "funding": list(self._funding_data),
            }
            self._orderbook_data = defaultdict(list)
            self._trade_data = defaultdict(list)
            self._liquidation_data = defaultdict(list)
            self._kline_data = defaultdict(list)
            self._funding_data = []
            return result

    def estimate_memory_usage(self) -> int:
        """현재 메모리 사용량 추정 (바이트)"""
        total = 0
        for store in [self._orderbook_data, self._trade_data,
                      self._liquidation_data, self._kline_data]:
            for records in store.values():
                total += sys.getsizeof(records)
                for r in records:
                    total += sys.getsizeof(r)
        total += sys.getsizeof(self._funding_data)
        for r in self._funding_data:
            total += sys.getsizeof(r)
        return total

    def needs_force_flush(self) -> bool:
        """강제 플러시 필요 여부"""
        return self.estimate_memory_usage() >= self.max_memory_bytes
