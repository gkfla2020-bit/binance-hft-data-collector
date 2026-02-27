"""펀딩비 수집 테스트 - Task 10
Feature: binance-data-collector
FundingRateCollector 조회, 재시도, Parquet 저장 검증
"""

import asyncio
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from dataclasses import asdict

import pytest
import pandas as pd

from src.config import Config
from src.buffer import DataBuffer
from src.integrity_logger import IntegrityLogger
from src.flusher import Flusher
from src.funding_rate_collector import FundingRateCollector
from src.models import FundingRateRecord


@pytest.fixture
def config():
    return Config(symbols=["btcusdt", "ethusdt"])


@pytest.fixture
def buffer():
    return DataBuffer()


@pytest.fixture
def collector(config, buffer):
    return FundingRateCollector(config, buffer)


class TestFetchFundingRate:
    """펀딩비 REST 조회 검증"""

    def test_successful_fetch(self, collector):
        """정상 응답 시 FundingRateRecord 반환"""
        mock_data = {
            "symbol": "BTCUSDT",
            "lastFundingRate": "0.00010000",
            "time": 1700000000000,
            "nextFundingTime": 1700028800000,
        }

        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=mock_data)
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        async def run():
            with patch("aiohttp.ClientSession", return_value=mock_session):
                return await collector.fetch_funding_rate("btcusdt")

        result = asyncio.run(run())
        assert result is not None
        assert result.symbol == "BTCUSDT"
        assert result.funding_rate == "0.00010000"

    def test_retry_on_failure(self, collector):
        """실패 시 최대 3회 재시도"""
        call_count = 0

        mock_session_ctx = MagicMock()

        def make_session(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            raise ConnectionError("timeout")

        # ClientSession() 자체가 예외를 던지도록
        async def run():
            with patch("aiohttp.ClientSession", side_effect=make_session):
                return await collector.fetch_funding_rate("btcusdt")

        result = asyncio.run(run())
        assert result is None
        assert call_count == 3

    def test_returns_none_on_http_error(self, collector):
        """HTTP 에러 시 None 반환"""
        mock_resp = MagicMock()
        mock_resp.status = 429
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        async def run():
            with patch("aiohttp.ClientSession", return_value=mock_session):
                return await collector.fetch_funding_rate("btcusdt")

        result = asyncio.run(run())
        assert result is None


class TestFundingRateParquet:
    """펀딩비 Parquet 저장 검증"""

    def test_save_funding_rate_parquet(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            records = [
                asdict(FundingRateRecord("BTCUSDT", "0.0001", 1700000000000, 1700028800000, 1.0)),
                asdict(FundingRateRecord("ETHUSDT", "-0.0002", 1700000000000, 1700028800000, 1.1)),
            ]
            fpath = Path(tmpdir) / "funding_rate_20240115.parquet"
            count = Flusher._save_parquet(records, fpath)

            assert count == 2
            df = pd.read_parquet(fpath)
            assert "funding_rate" in df.columns
            assert len(df) == 2

    def test_funding_data_in_flush(self):
        """Buffer flush에 펀딩비 데이터 포함"""
        buf = DataBuffer()
        record = asdict(FundingRateRecord("BTCUSDT", "0.0001", 1700000000000, 1700028800000, 1.0))

        async def run():
            await buf.add_funding_rate(record)
            return await buf.flush()

        data = asyncio.run(run())
        assert len(data["funding"]) == 1
        assert data["funding"][0]["symbol"] == "BTCUSDT"
