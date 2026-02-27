"""통합 테스트 - Task 16
Feature: binance-data-collector
모듈 간 연결 및 초기화 흐름 검증
"""

import tempfile
from pathlib import Path

import pytest

from src.config import Config
from src.buffer import DataBuffer
from src.orderbook_manager import OrderBookManager
from src.flusher import Flusher
from src.collector import Collector
from src.integrity_logger import IntegrityLogger
from src.syncer import Syncer
from src.telegram_reporter import TelegramReporter
from src.funding_rate_collector import FundingRateCollector
from src.time_sync_monitor import TimeSyncMonitor
from src.environment_recorder import EnvironmentRecorder


class TestModuleInitialization:
    """모든 모듈이 올바르게 초기화되는지 검증"""

    @pytest.fixture
    def config(self, tmp_path):
        return Config(
            data_dir=str(tmp_path / "data"),
            log_dir=str(tmp_path / "logs"),
            symbols=["btcusdt", "ethusdt"],
        )

    def test_all_modules_initialize(self, config):
        """모든 모듈이 예외 없이 초기화"""
        il = IntegrityLogger(config.log_dir)
        telegram = TelegramReporter(config)
        buffer = DataBuffer(config.max_buffer_mb)
        ob_manager = OrderBookManager(config.symbols, il)
        flusher = Flusher(config, buffer, il)
        collector = Collector(config, ob_manager, buffer, il, telegram)
        syncer = Syncer(config, il)
        funding = FundingRateCollector(config, buffer, il)
        time_sync = TimeSyncMonitor(config, il, telegram)
        env_recorder = EnvironmentRecorder(config, config.log_dir)

        assert collector is not None
        assert flusher is not None
        assert syncer is not None
        assert telegram.enabled is False  # 토큰 미설정

    def test_config_from_yaml(self, tmp_path):
        """config.yaml 로드 검증"""
        yaml_path = tmp_path / "config.yaml"
        config = Config(symbols=["btcusdt"], flush_interval=1800)
        config.to_yaml(str(yaml_path))

        loaded = Config.from_yaml(str(yaml_path))
        assert loaded.symbols == ["btcusdt"]
        assert loaded.flush_interval == 1800

    def test_environment_recorder_saves_metadata(self, config):
        """환경 메타데이터 저장 검증"""
        recorder = EnvironmentRecorder(config, config.log_dir)
        filepath = recorder.record()
        assert filepath.exists()
        assert filepath.suffix == ".json"

    def test_collector_builds_urls(self, config):
        """Collector가 WebSocket URL을 올바르게 생성"""
        il = IntegrityLogger(config.log_dir)
        buffer = DataBuffer()
        ob = OrderBookManager(config.symbols, il)
        collector = Collector(config, ob, buffer, il)

        url = collector.build_ws_url()
        assert "btcusdt@depth@100ms" in url
        assert "ethusdt@aggTrade" in url
        assert "kline_1m" in url

        futures_url = collector.build_futures_ws_url()
        assert "forceOrder" in futures_url

    def test_flusher_creates_data_dir(self, config):
        """Flusher가 data_dir을 자동 생성"""
        buffer = DataBuffer()
        flusher = Flusher(config, buffer)
        assert Path(config.data_dir).exists()

    def test_integrity_logger_creates_log_dir(self, config):
        """IntegrityLogger가 log_dir을 자동 생성"""
        il = IntegrityLogger(config.log_dir)
        assert Path(config.log_dir).exists()

    def test_orderbook_manager_has_all_symbols(self, config):
        """OrderBookManager가 모든 심볼을 관리"""
        il = IntegrityLogger(config.log_dir)
        ob = OrderBookManager(config.symbols, il)
        assert "BTCUSDT" in ob.books
        assert "ETHUSDT" in ob.books
