"""텔레그램 봇을 통한 상태 리포트 및 알림 모듈"""

import logging
from datetime import datetime, timezone

import aiohttp

from src.config import Config

logger = logging.getLogger(__name__)


class TelegramReporter:
    """텔레그램 봇을 통한 상태 리포트 및 알림"""

    def __init__(self, config: Config):
        self.bot_token = config.telegram_bot_token
        self.chat_id = config.telegram_chat_id
        self.enabled = bool(self.bot_token and self.chat_id)

    async def send_message(self, text: str) -> None:
        """텔레그램 메시지 전송 (실패 시 로깅만, 수집에 영향 없음)"""
        if not self.enabled:
            return
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.warning("텔레그램 전송 실패 (status=%d): %s", resp.status, body)
        except Exception:
            logger.warning("텔레그램 메시지 전송 중 예외 발생", exc_info=True)

    async def send_startup_report(self, config: Config) -> None:
        """시스템 시작 알림 (설정 정보 포함)"""
        if not self.enabled:
            return
        symbols = ", ".join(s.upper() for s in config.symbols)
        text = (
            "🚀 <b>수집 시스템 시작</b>\n"
            f"심볼: {symbols}\n"
            f"플러시 주기: {config.flush_interval}초\n"
            f"데이터 경로: {config.data_dir}\n"
            f"클라우드: {config.cloud_remote or '미설정'}\n"
            f"시작 시각: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
        )
        await self.send_message(text)

    async def send_flush_report(self, stats: dict) -> None:
        """플러시 완료 리포트 (심볼별 레코드 수, 갭 횟수, 파일 크기)"""
        if not self.enabled:
            return
        lines = ["📊 <b>플러시 완료 리포트</b>"]
        for symbol, info in stats.items():
            record_count = info.get("record_count", 0)
            file_size = info.get("file_size", 0)
            gaps = info.get("gaps", 0)
            size_kb = file_size / 1024 if file_size else 0
            lines.append(f"  {symbol}: {record_count}건, {size_kb:.1f}KB, 갭 {gaps}회")
        await self.send_message("\n".join(lines))

    async def send_disconnect_alert(self, reason: str) -> None:
        """WebSocket 연결 끊김 알림"""
        if not self.enabled:
            return
        text = (
            "⚠️ <b>WebSocket 연결 끊김</b>\n"
            f"사유: {reason}\n"
            f"시각: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
        )
        await self.send_message(text)

    async def send_reconnect_alert(self, downtime_seconds: float) -> None:
        """재연결 성공 알림 (끊김 지속 시간 포함)"""
        if not self.enabled:
            return
        text = (
            "✅ <b>WebSocket 재연결 성공</b>\n"
            f"끊김 지속: {downtime_seconds:.1f}초\n"
            f"시각: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
        )
        await self.send_message(text)

    async def send_gap_alert(self, symbol: str, expected_id: int, actual_id: int) -> None:
        """데이터 갭 감지 알림"""
        if not self.enabled:
            return
        text = (
            "🔴 <b>데이터 갭 감지</b>\n"
            f"심볼: {symbol}\n"
            f"예상 ID: {expected_id}\n"
            f"실제 ID: {actual_id}\n"
            f"시각: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
        )
        await self.send_message(text)

    async def send_daily_report(self, daily_stats: dict) -> None:
        """일별 종합 리포트 (총 레코드, 커버리지, 디스크/메모리 사용량)"""
        if not self.enabled:
            return
        total_records = daily_stats.get("total_records", 0)
        coverage = daily_stats.get("coverage", 0)
        disk_usage = daily_stats.get("disk_usage_mb", 0)
        memory_usage = daily_stats.get("memory_usage_mb", 0)
        gap_count = daily_stats.get("gap_count", 0)
        reconnect_count = daily_stats.get("reconnect_count", 0)
        text = (
            "📅 <b>일별 종합 리포트</b>\n"
            f"총 레코드: {total_records:,}건\n"
            f"커버리지: {coverage:.1%}\n"
            f"갭: {gap_count}회\n"
            f"재연결: {reconnect_count}회\n"
            f"디스크: {disk_usage:.1f}MB\n"
            f"메모리: {memory_usage:.1f}MB"
        )
        await self.send_message(text)
