"""WebSocket 데이터 수집 모듈 - 바이낸스 스트림 수신 및 라우팅"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import asdict
from typing import TYPE_CHECKING

import websockets

from src.models import (
    DepthDiffEvent, AggTradeEvent, LiquidationEvent, KlineEvent,
)

if TYPE_CHECKING:
    from src.buffer import DataBuffer
    from src.config import Config
    from src.orderbook_manager import OrderBookManager
    from src.integrity_logger import IntegrityLogger
    from src.telegram_reporter import TelegramReporter

logger = logging.getLogger(__name__)


class Collector:
    """바이낸스 WebSocket 스트림 수신기"""

    SPOT_WS = "wss://stream.binance.com:9443/stream"
    FUTURES_WS = "wss://fstream.binance.com/stream"

    def __init__(self, config: Config, orderbook_manager: OrderBookManager,
                 buffer: DataBuffer, integrity_logger: IntegrityLogger | None = None,
                 telegram: TelegramReporter | None = None):
        self.config = config
        self.ob_manager = orderbook_manager
        self.buffer = buffer
        self.integrity_logger = integrity_logger
        self.telegram = telegram
        self.reconnect_delay = 1.0
        self._disconnect_time: float | None = None

    def build_ws_url(self) -> str:
        """combined stream URL 생성 (spot: depth, aggTrade, kline_1m)"""
        streams = []
        for s in self.config.symbols:
            streams.append(f"{s}@depth@100ms")
            streams.append(f"{s}@aggTrade")
            streams.append(f"{s}@kline_1m")
        return f"{self.SPOT_WS}?streams={'/'.join(streams)}"

    def build_futures_ws_url(self) -> str:
        """선물 WebSocket URL (forceOrder)"""
        streams = [f"{s}@forceOrder" for s in self.config.symbols]
        return f"{self.FUTURES_WS}?streams={'/'.join(streams)}"

    async def run(self) -> None:
        """메인 수집 루프 - spot + futures 동시 연결"""
        tasks = [self._run_spot()]
        if self.config.use_futures:
            tasks.append(self._run_futures())
        await asyncio.gather(*tasks)

    async def _run_spot(self) -> None:
        """스팟 WebSocket 루프 (depth, aggTrade, kline)"""
        while True:
            try:
                await self._connect_and_collect()
            except Exception as e:
                self._disconnect_time = self._disconnect_time or time.time()
                logger.error(f"[에러-스팟] {e} — {self.reconnect_delay}초 후 재연결...")
                if self.telegram:
                    await self.telegram.send_disconnect_alert(str(e))
                if self.integrity_logger:
                    self.integrity_logger.record_reconnect(time.time(), str(e))
                await asyncio.sleep(self.reconnect_delay)
                self._increase_reconnect_delay()

    async def _run_futures(self) -> None:
        """선물 WebSocket 루프 (forceOrder)"""
        delay = 1.0
        while True:
            try:
                url = self.build_futures_ws_url()
                async with websockets.connect(url, ping_interval=20) as ws:
                    delay = 1.0
                    logger.info("[연결] 바이낸스 선물 WebSocket 연결 성공")
                    async for raw_msg in ws:
                        await self._handle_message(raw_msg)
            except Exception as e:
                logger.error(f"[에러-선물] {e} — {delay}초 후 재연결...")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60.0)

    async def _connect_and_collect(self) -> None:
        """WebSocket 연결 및 메시지 수신"""
        url = self.build_ws_url()
        async with websockets.connect(url, ping_interval=20) as ws:
            # 재연결 성공
            if self._disconnect_time:
                downtime = time.time() - self._disconnect_time
                if self.telegram:
                    await self.telegram.send_reconnect_alert(downtime)
                self._disconnect_time = None

            self._reset_reconnect_delay()
            # 오더북 스냅샷 재초기화
            for sym in self.config.symbols:
                await self.ob_manager.initialize(sym, self.config.orderbook_depth)
            logger.info("[연결] 바이낸스 WebSocket 연결 성공")

            async for raw_msg in ws:
                await self._handle_message(raw_msg)

    async def _handle_message(self, raw_msg: str) -> None:
        """수신 메시지 파싱 및 라우팅"""
        recv_time = time.time()
        data = json.loads(raw_msg)
        stream = data.get("stream", "")
        payload = data.get("data", {})

        if "depth" in stream:
            event = DepthDiffEvent(
                symbol=stream.split("@")[0].upper(),
                event_time=payload.get("E", 0),
                recv_time=recv_time,
                first_update_id=payload.get("U", 0),
                final_update_id=payload.get("u", 0),
                bids=payload.get("b", []),
                asks=payload.get("a", []),
            )
            snapshot = self.ob_manager.apply_diff(event.symbol, event)
            if snapshot:
                await self.buffer.add_orderbook(event.symbol, asdict(snapshot))
            else:
                # 갭 감지 → 자동 재초기화
                state = self.ob_manager.books.get(event.symbol)
                if state and not state.initialized:
                    logger.info(f"[재초기화] {event.symbol} 오더북 갭 감지, 스냅샷 재요청")
                    try:
                        await self.ob_manager.initialize(event.symbol, self.config.orderbook_depth)
                    except Exception as e:
                        logger.error(f"[재초기화 실패] {event.symbol}: {e}")

        elif "aggTrade" in stream:
            event = AggTradeEvent(
                symbol=payload.get("s", ""),
                trade_id=payload.get("a", 0),
                price=payload.get("p", "0"),
                quantity=payload.get("q", "0"),
                first_trade_id=payload.get("f", 0),
                last_trade_id=payload.get("l", 0),
                trade_time=payload.get("T", 0),
                recv_time=recv_time,
                is_buyer_maker=payload.get("m", False),
            )
            await self.buffer.add_trade(event.symbol, asdict(event))

        elif "forceOrder" in stream:
            o = payload.get("o", {})
            event = LiquidationEvent(
                symbol=o.get("s", ""),
                side=o.get("S", ""),
                order_type=o.get("o", ""),
                price=o.get("p", "0"),
                quantity=o.get("q", "0"),
                trade_time=o.get("T", 0),
                recv_time=recv_time,
            )
            await self.buffer.add_liquidation(event.symbol, asdict(event))

        elif "kline" in stream:
            k = payload.get("k", {})
            if k.get("x", False):  # 확정된 캔들만
                event = KlineEvent(
                    symbol=k.get("s", ""),
                    open_time=k.get("t", 0),
                    close_time=k.get("T", 0),
                    open=k.get("o", "0"),
                    high=k.get("h", "0"),
                    low=k.get("l", "0"),
                    close=k.get("c", "0"),
                    volume=k.get("v", "0"),
                    quote_volume=k.get("q", "0"),
                    trade_count=k.get("n", 0),
                    recv_time=recv_time,
                )
                await self.buffer.add_kline(event.symbol, asdict(event))

    def _reset_reconnect_delay(self) -> None:
        self.reconnect_delay = 1.0

    def _increase_reconnect_delay(self) -> None:
        self.reconnect_delay = min(self.reconnect_delay * 2, 60.0)

    @staticmethod
    def compute_reconnect_delay(attempt: int) -> float:
        """지수 백오프 계산 (테스트용 정적 메서드)"""
        return min(2 ** attempt, 60.0)
