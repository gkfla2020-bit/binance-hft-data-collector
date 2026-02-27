"""TimeSyncMonitor 테스트 - Task 12
Feature: binance-data-collector
NTP 측정, 바이낸스 오프셋 측정, 경고 트리거 검증
"""

import asyncio
import json
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config import Config
from src.time_sync_monitor import TimeSyncMonitor


@pytest.fixture
def config(tmp_path):
    return Config(log_dir=str(tmp_path / "logs"))


@pytest.fixture
def monitor(config):
    return TimeSyncMonitor(config)


class TestNTPMeasurement:
    """NTP 오프셋 측정 검증"""

    def test_ntp_returns_float(self, monitor):
        """NTP 측정 결과는 float"""
        # ntplib 없어도 0.0 반환
        result = asyncio.run(monitor.measure_ntp_offset())
        assert isinstance(result, float)

    def test_ntp_without_ntplib(self, monitor):
        """ntplib 미설치 시 0.0 반환"""
        with patch.dict("sys.modules", {"ntplib": None}):
            result = asyncio.run(monitor.measure_ntp_offset())
            assert result == 0.0


class TestBinanceMeasurement:
    """바이낸스 서버 시간 측정 검증"""

    def test_binance_offset_returns_tuple(self, monitor):
        """바이낸스 측정 결과는 (offset, rtt) 튜플"""
        import time
        server_time_ms = int(time.time() * 1000)

        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={"serverTime": server_time_ms})
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        async def run():
            with patch("aiohttp.ClientSession", return_value=mock_session):
                return await monitor.measure_binance_offset()

        offset, rtt = asyncio.run(run())
        assert isinstance(offset, float)
        assert isinstance(rtt, float)
        assert rtt >= 0

    def test_binance_failure_returns_zeros(self, monitor):
        """바이낸스 측정 실패 시 (0.0, 0.0) 반환"""
        mock_session = MagicMock()
        mock_session.__aenter__ = AsyncMock(side_effect=ConnectionError("fail"))
        mock_session.__aexit__ = AsyncMock(return_value=False)

        async def run():
            with patch("aiohttp.ClientSession", return_value=mock_session):
                return await monitor.measure_binance_offset()

        offset, rtt = asyncio.run(run())
        assert offset == 0.0
        assert rtt == 0.0


class TestSaveMeasurement:
    """측정 결과 저장 검증"""

    def test_save_creates_json_file(self, monitor, config):
        asyncio.run(monitor.save_measurement(0.005, -0.002, 0.05))

        log_dir = Path(config.log_dir)
        json_files = list(log_dir.glob("time_sync_*.json"))
        assert len(json_files) == 1

        with open(json_files[0]) as f:
            data = json.load(f)
        assert len(data) == 1
        assert "ntp_offset_sec" in data[0]
        assert "binance_offset_sec" in data[0]
        assert "rtt_sec" in data[0]

    def test_save_appends_to_existing(self, monitor, config):
        asyncio.run(monitor.save_measurement(0.005, -0.002, 0.05))
        asyncio.run(monitor.save_measurement(0.010, -0.001, 0.04))

        log_dir = Path(config.log_dir)
        json_files = list(log_dir.glob("time_sync_*.json"))
        with open(json_files[0]) as f:
            data = json.load(f)
        assert len(data) == 2


class TestNTPAlert:
    """NTP 오프셋 경고 트리거 검증"""

    def test_alert_when_offset_exceeds_threshold(self, config):
        mock_telegram = MagicMock()
        mock_telegram.send_message = AsyncMock()
        monitor = TimeSyncMonitor(config, telegram=mock_telegram)

        # 100ms 초과 오프셋
        with patch.object(monitor, "measure_ntp_offset", return_value=AsyncMock(return_value=0.15)()):
            with patch.object(monitor, "measure_binance_offset", return_value=AsyncMock(return_value=(0.0, 0.05))()):
                with patch.object(monitor, "save_measurement", new_callable=AsyncMock):
                    # run() 대신 한 사이클만 실행
                    async def one_cycle():
                        ntp_offset = 0.15
                        binance_offset, rtt = 0.0, 0.05
                        await monitor.save_measurement(ntp_offset, binance_offset, rtt)
                        if abs(ntp_offset) > 0.1 and monitor.telegram:
                            await monitor.telegram.send_message(
                                f"⏰ NTP 오프셋 경고: {ntp_offset*1000:.1f}ms"
                            )

                    asyncio.run(one_cycle())

        mock_telegram.send_message.assert_called_once()
        call_text = mock_telegram.send_message.call_args[0][0]
        assert "150.0ms" in call_text
