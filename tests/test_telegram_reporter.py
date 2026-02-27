"""TelegramReporter 테스트 - Property 13 (전송 실패 격리) + 단위 테스트"""

import asyncio
from unittest.mock import AsyncMock, patch, MagicMock

import pytest
from hypothesis import given, strategies as st, settings

from src.config import Config
from src.telegram_reporter import TelegramReporter


# --- Fixtures ---

@pytest.fixture
def config_with_token():
    """봇 토큰과 채팅 ID가 설정된 Config"""
    return Config(telegram_bot_token="test-bot-token", telegram_chat_id="12345")


@pytest.fixture
def config_without_token():
    """봇 토큰이 없는 Config"""
    return Config(telegram_bot_token="", telegram_chat_id="")


@pytest.fixture
def reporter(config_with_token):
    return TelegramReporter(config_with_token)


@pytest.fixture
def disabled_reporter(config_without_token):
    return TelegramReporter(config_without_token)


# --- Property 13: 텔레그램 전송 실패 격리 ---
# **Validates: Requirements 7.8**

exception_types = st.sampled_from([
    ConnectionError, TimeoutError, ValueError, RuntimeError,
    OSError, IOError, Exception, TypeError, AttributeError,
    KeyError, IndexError, PermissionError, BrokenPipeError,
])


class TestProperty13TelegramFailureIsolation:
    """Property 13: 텔레그램 전송 실패 격리
    *For any* 텔레그램 메시지 전송 시도에서 예외가 발생하더라도,
    호출자에게 예외가 전파되지 않아야 한다.
    """

    @given(
        exc_type=exception_types,
        message=st.text(min_size=0, max_size=200),
        exc_msg=st.text(min_size=0, max_size=100),
    )
    @settings(max_examples=100)
    def test_send_message_never_raises_on_http_exception(self, exc_type, message, exc_msg):
        """**Validates: Requirements 7.8**
        send_message는 aiohttp 세션에서 어떤 예외가 발생해도 전파하지 않는다."""
        config = Config(telegram_bot_token="tok", telegram_chat_id="123")
        reporter = TelegramReporter(config)

        mock_session = MagicMock()
        mock_session.__aenter__ = AsyncMock(side_effect=exc_type(exc_msg))
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            # Must not raise
            asyncio.run(reporter.send_message(message))

    @given(
        exc_type=exception_types,
        exc_msg=st.text(min_size=0, max_size=100),
    )
    @settings(max_examples=100)
    def test_send_message_never_raises_on_post_exception(self, exc_type, exc_msg):
        """**Validates: Requirements 7.8**
        send_message는 POST 요청 중 어떤 예외가 발생해도 전파하지 않는다."""
        config = Config(telegram_bot_token="tok", telegram_chat_id="123")
        reporter = TelegramReporter(config)

        mock_resp_ctx = MagicMock()
        mock_resp_ctx.__aenter__ = AsyncMock(side_effect=exc_type(exc_msg))
        mock_resp_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_session_instance = MagicMock()
        mock_session_instance.post = MagicMock(return_value=mock_resp_ctx)
        mock_session_ctx = MagicMock()
        mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_session_instance)
        mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=mock_session_ctx):
            # Must not raise
            asyncio.run(reporter.send_message("test message"))


# --- 단위 테스트: 봇 토큰 미설정 시 비활성화 ---

class TestDisabledWhenNoToken:
    """봇 토큰 또는 채팅 ID 미설정 시 비활성화"""

    def test_disabled_when_no_token(self, config_without_token):
        reporter = TelegramReporter(config_without_token)
        assert reporter.enabled is False

    def test_disabled_when_no_chat_id(self):
        config = Config(telegram_bot_token="some-token", telegram_chat_id="")
        reporter = TelegramReporter(config)
        assert reporter.enabled is False

    def test_disabled_when_no_bot_token(self):
        config = Config(telegram_bot_token="", telegram_chat_id="12345")
        reporter = TelegramReporter(config)
        assert reporter.enabled is False

    def test_enabled_when_both_set(self, config_with_token):
        reporter = TelegramReporter(config_with_token)
        assert reporter.enabled is True

    def test_send_message_returns_immediately_when_disabled(self, disabled_reporter):
        """비활성화 상태에서 send_message는 HTTP 요청 없이 즉시 반환"""
        with patch("aiohttp.ClientSession") as mock_cls:
            asyncio.run(disabled_reporter.send_message("hello"))
            mock_cls.assert_not_called()

    def test_send_startup_report_noop_when_disabled(self, disabled_reporter, config_without_token):
        with patch("aiohttp.ClientSession") as mock_cls:
            asyncio.run(disabled_reporter.send_startup_report(config_without_token))
            mock_cls.assert_not_called()

    def test_send_flush_report_noop_when_disabled(self, disabled_reporter):
        with patch("aiohttp.ClientSession") as mock_cls:
            asyncio.run(disabled_reporter.send_flush_report({"BTC": {"record_count": 10}}))
            mock_cls.assert_not_called()

    def test_send_disconnect_alert_noop_when_disabled(self, disabled_reporter):
        with patch("aiohttp.ClientSession") as mock_cls:
            asyncio.run(disabled_reporter.send_disconnect_alert("timeout"))
            mock_cls.assert_not_called()

    def test_send_reconnect_alert_noop_when_disabled(self, disabled_reporter):
        with patch("aiohttp.ClientSession") as mock_cls:
            asyncio.run(disabled_reporter.send_reconnect_alert(5.0))
            mock_cls.assert_not_called()

    def test_send_gap_alert_noop_when_disabled(self, disabled_reporter):
        with patch("aiohttp.ClientSession") as mock_cls:
            asyncio.run(disabled_reporter.send_gap_alert("BTCUSDT", 100, 105))
            mock_cls.assert_not_called()

    def test_send_daily_report_noop_when_disabled(self, disabled_reporter):
        with patch("aiohttp.ClientSession") as mock_cls:
            asyncio.run(disabled_reporter.send_daily_report({"total_records": 1000}))
            mock_cls.assert_not_called()


# --- 단위 테스트: 메시지 포맷 검증 ---

class TestMessageFormat:
    """각 send_* 메서드가 올바른 포맷의 메시지를 생성하는지 검증"""

    @pytest.fixture(autouse=True)
    def capture_send(self, reporter):
        """send_message를 모킹하여 전송된 텍스트를 캡처"""
        self.sent_messages = []
        original = reporter.send_message

        async def capture(text):
            self.sent_messages.append(text)

        reporter.send_message = capture
        self.reporter = reporter

    def test_startup_report_contains_symbols(self, config_with_token):
        asyncio.run(self.reporter.send_startup_report(config_with_token))
        assert len(self.sent_messages) == 1
        msg = self.sent_messages[0]
        assert "시작" in msg
        assert "BTCUSDT" in msg
        assert "ETHUSDT" in msg
        assert "XRPUSDT" in msg

    def test_startup_report_contains_flush_interval(self, config_with_token):
        asyncio.run(self.reporter.send_startup_report(config_with_token))
        msg = self.sent_messages[0]
        assert str(config_with_token.flush_interval) in msg

    def test_flush_report_contains_symbol_stats(self):
        stats = {
            "BTCUSDT": {"record_count": 1000, "file_size": 51200, "gaps": 2},
            "ETHUSDT": {"record_count": 500, "file_size": 25600, "gaps": 0},
        }
        asyncio.run(self.reporter.send_flush_report(stats))
        msg = self.sent_messages[0]
        assert "BTCUSDT" in msg
        assert "1000" in msg
        assert "ETHUSDT" in msg
        assert "갭" in msg

    def test_disconnect_alert_contains_reason(self):
        asyncio.run(self.reporter.send_disconnect_alert("connection reset"))
        msg = self.sent_messages[0]
        assert "끊김" in msg
        assert "connection reset" in msg

    def test_reconnect_alert_contains_downtime(self):
        asyncio.run(self.reporter.send_reconnect_alert(12.5))
        msg = self.sent_messages[0]
        assert "재연결" in msg
        assert "12.5" in msg

    def test_gap_alert_contains_ids(self):
        asyncio.run(self.reporter.send_gap_alert("BTCUSDT", 100, 105))
        msg = self.sent_messages[0]
        assert "갭" in msg
        assert "BTCUSDT" in msg
        assert "100" in msg
        assert "105" in msg

    def test_daily_report_contains_stats(self):
        daily = {
            "total_records": 50000,
            "coverage": 0.985,
            "disk_usage_mb": 120.5,
            "memory_usage_mb": 45.2,
            "gap_count": 3,
            "reconnect_count": 1,
        }
        asyncio.run(self.reporter.send_daily_report(daily))
        msg = self.sent_messages[0]
        assert "50,000" in msg
        assert "98.5%" in msg
        assert "120.5" in msg
