"""EnvironmentRecorder 테스트 - Task 14
Feature: binance-data-collector
Property 15: 민감정보 마스킹
시스템 정보 수집, config 마스킹 검증
"""

import json
import tempfile
from pathlib import Path

import pytest
from hypothesis import given, settings, strategies as st

from src.config import Config
from src.environment_recorder import EnvironmentRecorder


# ── Property 15: 환경 메타데이터 민감정보 마스킹 ──

class TestProperty15SensitiveMasking:
    """Property 15: 환경 메타데이터 민감정보 마스킹
    *For any* 기록된 환경 메타데이터 JSON에서, telegram_bot_token 필드는
    원본 토큰 값을 포함하지 않아야 하며, 마스킹된 형태여야 한다.
    Validates: Requirements 13.3
    """

    @given(
        token=st.text(min_size=1, max_size=100).filter(lambda x: x.strip()),
        chat_id=st.text(min_size=1, max_size=50).filter(lambda x: x.strip()),
    )
    @settings(max_examples=100)
    def test_token_never_in_output(self, token, chat_id):
        """원본 토큰이 마스킹된 config에 절대 나타나지 않아야 함"""
        config = Config(telegram_bot_token=token, telegram_chat_id=chat_id)
        masked = EnvironmentRecorder._mask_sensitive_config(config)

        assert masked["telegram_bot_token"] == "***"
        assert masked["telegram_chat_id"] == "***"
        # 원본 값이 마스킹된 dict의 values에 없어야 함
        assert token not in str(masked["telegram_bot_token"])
        assert chat_id not in str(masked["telegram_chat_id"])

    @given(
        token=st.text(min_size=10, max_size=100, alphabet=st.characters(whitelist_categories=("L", "N"))),
    )
    @settings(max_examples=100)
    def test_token_not_in_saved_json(self, token):
        """저장된 JSON 파일의 config.telegram_bot_token 필드가 마스킹되어야 함"""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = Config(telegram_bot_token=token, telegram_chat_id="test_chat_99999")
            recorder = EnvironmentRecorder(config, tmpdir)
            filepath = recorder.record()

            with open(filepath) as f:
                data = json.load(f)
            assert data["config"]["telegram_bot_token"] == "***"
            assert data["config"]["telegram_chat_id"] == "***"


# ── 단위 테스트 ──

class TestEnvironmentRecorderUnit:

    def test_record_creates_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = Config()
            recorder = EnvironmentRecorder(config, tmpdir)
            filepath = recorder.record()

            assert filepath.exists()
            assert filepath.suffix == ".json"

            with open(filepath) as f:
                data = json.load(f)
            assert "system" in data
            assert "python" in data
            assert "config" in data
            assert "timestamp" in data

    def test_system_info_has_os(self):
        info = EnvironmentRecorder._get_system_info()
        assert "os" in info
        assert "cpu_count" in info
        assert info["cpu_count"] > 0

    def test_python_info_has_version(self):
        info = EnvironmentRecorder._get_python_info()
        assert "version" in info
        assert "executable" in info

    def test_empty_token_not_masked(self):
        """빈 토큰은 마스킹하지 않음"""
        config = Config(telegram_bot_token="", telegram_chat_id="")
        masked = EnvironmentRecorder._mask_sensitive_config(config)
        assert masked["telegram_bot_token"] == ""
        assert masked["telegram_chat_id"] == ""

    def test_non_sensitive_fields_preserved(self):
        """민감하지 않은 필드는 원본 유지"""
        config = Config(symbols=["btcusdt"], flush_interval=1800)
        masked = EnvironmentRecorder._mask_sensitive_config(config)
        assert masked["symbols"] == ["btcusdt"]
        assert masked["flush_interval"] == 1800
