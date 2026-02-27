"""청산 데이터 수집 테스트 - Task 9
Feature: binance-data-collector
forceOrder 메시지 파싱, Buffer 적재, Parquet 저장 검증
"""

import asyncio
import json
import tempfile
from pathlib import Path
from dataclasses import asdict

import pytest
import pandas as pd

from src.models import LiquidationEvent
from src.buffer import DataBuffer
from src.config import Config
from src.flusher import Flusher


class TestForceOrderParsing:
    """forceOrder WebSocket 메시지 파싱 검증"""

    def test_parse_force_order_message(self):
        """forceOrder 메시지에서 LiquidationEvent를 올바르게 파싱"""
        raw = {
            "stream": "btcusdt@forceOrder",
            "data": {
                "e": "forceOrder",
                "E": 1700000000000,
                "o": {
                    "s": "BTCUSDT",
                    "S": "SELL",
                    "o": "LIMIT",
                    "f": "IOC",
                    "q": "0.014",
                    "p": "43000.00",
                    "ap": "43010.50",
                    "X": "FILLED",
                    "l": "0.014",
                    "z": "0.014",
                    "T": 1700000000123,
                }
            }
        }
        payload = raw["data"]
        o = payload["o"]
        event = LiquidationEvent(
            symbol=o["s"],
            side=o["S"],
            order_type=o["o"],
            price=o["p"],
            quantity=o["q"],
            trade_time=o["T"],
            recv_time=1700000000.5,
        )
        assert event.symbol == "BTCUSDT"
        assert event.side == "SELL"
        assert event.price == "43000.00"
        assert event.quantity == "0.014"
        assert event.trade_time == 1700000000123

    def test_liquidation_event_to_dict(self):
        """LiquidationEvent가 dict로 변환 가능"""
        event = LiquidationEvent(
            symbol="ETHUSDT", side="BUY", order_type="LIMIT",
            price="2500.00", quantity="1.5",
            trade_time=1700000000000, recv_time=1700000000.1,
        )
        d = asdict(event)
        assert d["symbol"] == "ETHUSDT"
        assert d["recv_time"] == 1700000000.1


class TestLiquidationBuffer:
    """청산 데이터 Buffer 적재 검증"""

    def test_add_liquidation_to_buffer(self):
        buf = DataBuffer()
        record = {"symbol": "BTCUSDT", "side": "SELL", "price": "43000",
                  "quantity": "0.01", "trade_time": 1700000000000, "recv_time": 1.0}

        asyncio.run(buf.add_liquidation("BTCUSDT", record))

        assert len(buf._liquidation_data["BTCUSDT"]) == 1
        assert buf._liquidation_data["BTCUSDT"][0]["price"] == "43000"

    def test_liquidation_isolated_from_other_data(self):
        buf = DataBuffer()
        liq = {"symbol": "BTCUSDT", "side": "SELL", "price": "43000",
               "quantity": "0.01", "trade_time": 1, "recv_time": 1.0}
        trade = {"symbol": "BTCUSDT", "trade_id": 1, "price": "43000",
                 "quantity": "0.01", "trade_time": 1, "recv_time": 1.0}

        async def run():
            await buf.add_liquidation("BTCUSDT", liq)
            await buf.add_trade("BTCUSDT", trade)

        asyncio.run(run())

        assert len(buf._liquidation_data["BTCUSDT"]) == 1
        assert len(buf._trade_data["BTCUSDT"]) == 1

    def test_liquidation_included_in_flush(self):
        buf = DataBuffer()
        record = {"symbol": "BTCUSDT", "side": "SELL", "price": "43000",
                  "quantity": "0.01", "trade_time": 1, "recv_time": 1.0}

        async def run():
            await buf.add_liquidation("BTCUSDT", record)
            return await buf.flush()

        data = asyncio.run(run())
        assert "BTCUSDT" in data["liquidation"]
        assert len(data["liquidation"]["BTCUSDT"]) == 1


class TestLiquidationParquet:
    """청산 데이터 Parquet 저장 검증"""

    def test_save_liquidation_parquet(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = Config(data_dir=tmpdir)
            buf = DataBuffer()
            flusher = Flusher(config, buf)

            records = [
                {"symbol": "BTCUSDT", "side": "SELL", "price": "43000",
                 "quantity": "0.01", "trade_time": 1700000000000,
                 "recv_time": 1700000000.1, "order_type": "LIMIT"},
                {"symbol": "BTCUSDT", "side": "BUY", "price": "42000",
                 "quantity": "0.05", "trade_time": 1700000001000,
                 "recv_time": 1700000001.1, "order_type": "LIMIT"},
            ]

            fpath = Path(tmpdir) / "BTCUSDT_liquidation_20240115_10.parquet"
            count = Flusher._save_parquet(records, fpath)

            assert count == 2
            assert fpath.exists()

            df = pd.read_parquet(fpath)
            assert len(df) == 2
            assert "side" in df.columns
            assert df["side"].iloc[0] == "SELL"

    def test_liquidation_filename_format(self):
        from datetime import datetime
        fname = Flusher._generate_filename("BTCUSDT", "liquidation",
                                           datetime(2024, 1, 15, 10, 30))
        assert fname == "BTCUSDT_liquidation_20240115_1030.parquet"
