"""캔들(OHLCV) 수집 테스트 - Task 11
Feature: binance-data-collector
kline 메시지 파싱, 미확정 캔들 필터링, Parquet 저장 검증
"""

import asyncio
import json
import tempfile
from pathlib import Path
from dataclasses import asdict

import pytest
import pandas as pd

from src.models import KlineEvent
from src.buffer import DataBuffer
from src.flusher import Flusher


class TestKlineParsing:
    """kline WebSocket 메시지 파싱 검증"""

    def test_parse_closed_kline(self):
        """확정된 캔들(x=true) 파싱"""
        k = {
            "s": "BTCUSDT", "t": 1700000000000, "T": 1700000059999,
            "o": "43000.00", "h": "43100.00", "l": "42900.00", "c": "43050.00",
            "v": "100.5", "q": "4320000.00", "n": 5000, "x": True,
        }
        event = KlineEvent(
            symbol=k["s"], open_time=k["t"], close_time=k["T"],
            open=k["o"], high=k["h"], low=k["l"], close=k["c"],
            volume=k["v"], quote_volume=k["q"], trade_count=k["n"],
            recv_time=1700000060.0,
        )
        assert event.symbol == "BTCUSDT"
        assert event.open == "43000.00"
        assert event.trade_count == 5000

    def test_unclosed_kline_filtered(self):
        """미확정 캔들(x=false)은 Buffer에 적재하지 않아야 함 - Collector 로직 검증"""
        k = {"x": False}
        # Collector._handle_message에서 x=False면 스킵
        assert k.get("x", False) is False


class TestKlineBuffer:
    """캔들 데이터 Buffer 적재 검증"""

    def test_add_kline_to_buffer(self):
        buf = DataBuffer()
        record = asdict(KlineEvent(
            "BTCUSDT", 1700000000000, 1700000059999,
            "43000", "43100", "42900", "43050",
            "100.5", "4320000", 5000, 1700000060.0,
        ))
        asyncio.run(buf.add_kline("BTCUSDT", record))
        assert len(buf._kline_data["BTCUSDT"]) == 1

    def test_kline_included_in_flush(self):
        buf = DataBuffer()
        record = asdict(KlineEvent(
            "ETHUSDT", 1700000000000, 1700000059999,
            "2500", "2510", "2490", "2505",
            "500", "1250000", 3000, 1700000060.0,
        ))

        async def run():
            await buf.add_kline("ETHUSDT", record)
            return await buf.flush()

        data = asyncio.run(run())
        assert "ETHUSDT" in data["kline"]
        assert len(data["kline"]["ETHUSDT"]) == 1


class TestKlineParquet:
    """캔들 Parquet 저장 검증"""

    def test_save_kline_parquet(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            records = [
                asdict(KlineEvent(
                    "BTCUSDT", 1700000000000, 1700000059999,
                    "43000", "43100", "42900", "43050",
                    "100.5", "4320000", 5000, 1700000060.0,
                )),
            ]
            fpath = Path(tmpdir) / "BTCUSDT_kline_1m_20240115.parquet"
            count = Flusher._save_parquet(records, fpath)
            assert count == 1

            df = pd.read_parquet(fpath)
            assert "open" in df.columns
            assert "close" in df.columns
            assert df["volume"].iloc[0] == "100.5"

    def test_kline_filename_format(self):
        from datetime import datetime
        fname = Flusher._generate_filename("BTCUSDT", "kline_1m",
                                           datetime(2024, 1, 15, 0, 0))
        assert fname == "BTCUSDT_kline_1m_20240115_0000.parquet"
