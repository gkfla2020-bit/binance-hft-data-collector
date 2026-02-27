"""오더북 재구성 및 시퀀스 검증 모듈"""

from __future__ import annotations

import json
import logging
import time
from typing import TYPE_CHECKING

import aiohttp

from src.models import DepthDiffEvent, OrderBookState, OrderBookSnapshot

if TYPE_CHECKING:
    from src.integrity_logger import IntegrityLogger

logger = logging.getLogger(__name__)


class OrderBookManager:
    """오더북 재구성 및 시퀀스 검증"""

    BASE_URL = "https://api.binance.com"

    def __init__(self, symbols: list[str], integrity_logger: IntegrityLogger | None = None):
        self.symbols = symbols
        self.integrity_logger = integrity_logger
        self.books: dict[str, OrderBookState] = {
            s.upper(): OrderBookState() for s in symbols
        }

    async def initialize(self, symbol: str, depth: int = 1000) -> None:
        """REST API로 초기 스냅샷 가져오기"""
        sym = symbol.upper()
        url = f"{self.BASE_URL}/api/v3/depth?symbol={sym}&limit={depth}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                data = await resp.json()

        state = OrderBookState(
            bids={p: q for p, q in data.get("bids", [])},
            asks={p: q for p, q in data.get("asks", [])},
            last_update_id=data.get("lastUpdateId", 0),
            initialized=True,
            init_time=time.time(),
        )
        self.books[sym] = state
        logger.info(f"[오더북] {sym} 스냅샷 초기화 완료 (lastUpdateId={state.last_update_id})")

    def validate_sequence(self, symbol: str, first_update_id: int,
                          final_update_id: int) -> bool:
        """lastUpdateId 연속성 검증: F <= last_update_id+1 <= L"""
        sym = symbol.upper()
        state = self.books.get(sym)
        if not state or not state.initialized:
            return False
        expected = state.last_update_id + 1
        return first_update_id <= expected <= final_update_id

    def apply_diff(self, symbol: str, event: DepthDiffEvent) -> OrderBookSnapshot | None:
        """diff 적용 및 시퀀스 검증. 갭 감지 시 None 반환"""
        sym = symbol.upper()
        state = self.books.get(sym)
        if not state or not state.initialized:
            return None

        expected = state.last_update_id + 1

        # 이미 처리된 오래된 diff → 무시 (갭 아님)
        if event.final_update_id < expected:
            return None

        # 시퀀스 검증
        if not self.validate_sequence(sym, event.first_update_id, event.final_update_id):
            # 초기화 직후 3초 grace period: 재초기화 트리거하지 않고 skip
            if time.time() - state.init_time < 3.0:
                return None
            # 진짜 갭: first_update_id > expected (미래 diff가 도착)
            if self.integrity_logger:
                self.integrity_logger.record_gap(
                    symbol=sym,
                    expected_id=expected,
                    actual_id=event.first_update_id,
                    timestamp=time.time(),
                )
            logger.warning(
                f"[갭] {sym} expected={expected}, "
                f"got F={event.first_update_id} L={event.final_update_id}"
            )
            state.initialized = False
            return None

        # diff 적용
        self._apply_updates(state.bids, event.bids)
        self._apply_updates(state.asks, event.asks)
        state.last_update_id = event.final_update_id

        return self.get_top_levels(sym, event_time=event.event_time,
                                   recv_time=event.recv_time)

    @staticmethod
    def _apply_updates(book_side: dict[str, str], updates: list[list[str]]) -> None:
        """오더북 한쪽(bids 또는 asks)에 업데이트 적용"""
        for price, qty in updates:
            if qty == "0" or qty == "0.00000000":
                book_side.pop(price, None)
            else:
                book_side[price] = qty

    def get_top_levels(self, symbol: str, levels: int = 20,
                       event_time: int = 0, recv_time: float = 0.0) -> OrderBookSnapshot:
        """상위 N호가 반환 (bids 내림차순, asks 오름차순)"""
        sym = symbol.upper()
        state = self.books[sym]

        sorted_bids = sorted(state.bids.items(), key=lambda x: float(x[0]), reverse=True)[:levels]
        sorted_asks = sorted(state.asks.items(), key=lambda x: float(x[0]))[:levels]

        return OrderBookSnapshot(
            symbol=sym,
            event_time=event_time,
            recv_time=recv_time,
            last_update_id=state.last_update_id,
            bids=[[p, q] for p, q in sorted_bids],
            asks=[[p, q] for p, q in sorted_asks],
        )
