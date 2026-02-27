"""텔레그램 봇을 통한 상태 리포트 및 알림 모듈"""

import logging
from datetime import datetime, timezone

import aiohttp

from src.config import Config

logger = logging.getLogger(__name__)



class TelegramReporter:
    """텔레그램 봇을 통한 대시보드 스타일 상태 리포트 및 알림"""

    def __init__(self, config: Config):
        self.bot_token = config.telegram_bot_token
        self.chat_id = config.telegram_chat_id
        self.enabled = bool(self.bot_token and self.chat_id)

    @staticmethod
    def _now_str() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    @staticmethod
    def _bar(ratio: float, length: int = 10) -> str:
        """비율(0~1)을 시각적 프로그레스 바로 변환"""
        filled = int(ratio * length)
        return "█" * filled + "░" * (length - filled)

    @staticmethod
    def _format_bytes(size_bytes: float) -> str:
        """바이트를 사람이 읽기 쉬운 단위로 변환"""
        if size_bytes < 1024:
            return f"{size_bytes:.0f}B"
        elif size_bytes < 1024 ** 2:
            return f"{size_bytes / 1024:.1f}KB"
        elif size_bytes < 1024 ** 3:
            return f"{size_bytes / 1024 ** 2:.1f}MB"
        return f"{size_bytes / 1024 ** 3:.2f}GB"

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
        """시스템 시작 알림 — 대시보드 스타일"""
        if not self.enabled:
            return
        sym_list = " ".join(f"<code>{s.upper()}</code>" for s in config.symbols)
        cloud_status = f"✅ {config.cloud_remote}" if config.cloud_remote else "⛔ 미설정"
        futures_status = "✅ ON" if config.use_futures else "⛔ OFF"
        text = (
            "━━━━━━━━━━━━━━━━━━━━\n"
            "🚀 <b>SYSTEM ONLINE</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "\n"
            f"🕐 {self._now_str()}\n"
            "\n"
            f"📌 <b>심볼</b>\n"
            f"   {sym_list}\n"
            "\n"
            "┌─────────────────────┐\n"
            f"│ ⏱ 플러시     │ <code>{config.flush_interval:>6}s</code> │\n"
            f"│ 💾 버퍼 상한  │ <code>{config.max_buffer_mb:>4}MB</code>  │\n"
            f"│ 📂 데이터     │ <code>{config.data_dir:<8}</code>│\n"
            f"│ ☁️ 클라우드   │ {cloud_status:<8}│\n"
            f"│ 📈 선물 API   │ {futures_status:<8}│\n"
            "└─────────────────────┘\n"
            "\n"
            "━━━━━━━━━━━━━━━━━━━━"
        )
        await self.send_message(text)

    async def send_flush_report(self, stats: dict) -> None:
        """플러시 완료 리포트 — 심볼별 테이블"""
        if not self.enabled:
            return
        total_records = 0
        total_size = 0
        total_gaps = 0
        rows = []
        for symbol, info in stats.items():
            rc = info.get("record_count", 0)
            fs = info.get("file_size", 0)
            gp = info.get("gaps", 0)
            total_records += rc
            total_size += fs
            total_gaps += gp
            gap_icon = "🔴" if gp > 0 else "🟢"
            rows.append(
                f"  {gap_icon} <code>{symbol:<10}</code> "
                f"<code>{rc:>7,}</code>건  "
                f"<code>{self._format_bytes(fs):>7}</code>"
            )

        text = (
            "━━━━━━━━━━━━━━━━━━━━\n"
            "📊 <b>FLUSH COMPLETE</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🕐 {self._now_str()}\n"
            "\n"
            f"📦 총 <b>{total_records:,}</b>건 │ "
            f"💾 <b>{self._format_bytes(total_size)}</b> │ "
            f"⚡ 갭 <b>{total_gaps}</b>회\n"
            "\n"
            "┌──────────────────────────┐\n"
            "│  상태  심볼        건수     크기  │\n"
            "├──────────────────────────┤\n"
            + "\n".join(rows) + "\n"
            "└──────────────────────────┘\n"
            "━━━━━━━━━━━━━━━━━━━━"
        )
        await self.send_message(text)

    async def send_disconnect_alert(self, reason: str) -> None:
        """WebSocket 연결 끊김 — 긴급 알림 스타일"""
        if not self.enabled:
            return
        text = (
            "🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴\n"
            "⚠️ <b>CONNECTION LOST</b>\n"
            "🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴\n"
            "\n"
            f"🕐 {self._now_str()}\n"
            "\n"
            f"📡 <b>사유</b>: {reason}\n"
            "\n"
            "🔄 자동 재연결 시도 중...\n"
            "━━━━━━━━━━━━━━━━━━━━"
        )
        await self.send_message(text)

    async def send_reconnect_alert(self, downtime_seconds: float) -> None:
        """재연결 성공 — 복구 알림"""
        if not self.enabled:
            return
        if downtime_seconds < 5:
            severity = "🟢 경미"
        elif downtime_seconds < 30:
            severity = "🟡 보통"
        else:
            severity = "🔴 심각"

        text = (
            "━━━━━━━━━━━━━━━━━━━━\n"
            "✅ <b>RECONNECTED</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "\n"
            f"🕐 {self._now_str()}\n"
            "\n"
            f"⏱ 다운타임: <b>{downtime_seconds:.1f}s</b>\n"
            f"📊 심각도: {severity}\n"
            "\n"
            "📡 데이터 수신 재개\n"
            "━━━━━━━━━━━━━━━━━━━━"
        )
        await self.send_message(text)

    async def send_gap_alert(self, symbol: str, expected_id: int, actual_id: int) -> None:
        """데이터 갭 감지 — 경고 알림"""
        if not self.enabled:
            return
        missed = actual_id - expected_id
        text = (
            "━━━━━━━━━━━━━━━━━━━━\n"
            "🔴 <b>GAP DETECTED</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "\n"
            f"🕐 {self._now_str()}\n"
            "\n"
            f"💱 심볼: <code>{symbol}</code>\n"
            f"📍 예상 ID: <code>{expected_id}</code>\n"
            f"📍 실제 ID: <code>{actual_id}</code>\n"
            f"❌ 누락: <b>{missed}</b>건\n"
            "\n"
            "🔄 오더북 스냅샷 재로드 필요\n"
            "━━━━━━━━━━━━━━━━━━━━"
        )
        await self.send_message(text)

    async def send_daily_report(self, daily_stats: dict) -> None:
        """일별 종합 리포트 — 풀 대시보드"""
        if not self.enabled:
            return
        total_records = daily_stats.get("total_records", 0)
        coverage = daily_stats.get("coverage", 0)
        disk_usage = daily_stats.get("disk_usage_mb", 0)
        memory_usage = daily_stats.get("memory_usage_mb", 0)
        gap_count = daily_stats.get("gap_count", 0)
        reconnect_count = daily_stats.get("reconnect_count", 0)

        # 커버리지 바
        cov_bar = self._bar(coverage)
        cov_icon = "🟢" if coverage >= 0.999 else "🟡" if coverage >= 0.99 else "🔴"

        # 상태 판정
        if gap_count == 0 and reconnect_count == 0:
            health = "🟢 EXCELLENT"
        elif gap_count <= 3 and reconnect_count <= 2:
            health = "🟡 GOOD"
        else:
            health = "🔴 DEGRADED"

        text = (
            "━━━━━━━━━━━━━━━━━━━━\n"
            "📅 <b>DAILY REPORT</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🕐 {self._now_str()}\n"
            "\n"
            f"🏥 시스템 상태: {health}\n"
            "\n"
            "┌─── 📊 수집 통계 ───┐\n"
            f"│ 총 레코드  <b>{total_records:>10,}</b> │\n"
            f"│ 갭 발생    <b>{gap_count:>10}</b> │\n"
            f"│ 재연결     <b>{reconnect_count:>10}</b> │\n"
            "└────────────────────┘\n"
            "\n"
            f"{cov_icon} 커버리지: <b>{coverage:.2%}</b>\n"
            f"   {cov_bar}\n"
            "\n"
            "┌─── 💻 리소스 ──────┐\n"
            f"│ 💾 디스크  <b>{disk_usage:>8.1f}MB</b> │\n"
            f"│ 🧠 메모리  <b>{memory_usage:>8.1f}MB</b> │\n"
            "└────────────────────┘\n"
            "━━━━━━━━━━━━━━━━━━━━"
        )
        await self.send_message(text)

    async def send_live_ticker(self, ob_manager, buffer) -> None:
        """3분마다 실시간 시세 + 스프레드 + 수집 현황 리포트"""
        if not self.enabled:
            return

        rows = []
        for sym_upper, state in ob_manager.books.items():
            if not state.initialized or not state.bids or not state.asks:
                rows.append(f"  ⚪ <code>{sym_upper:<10}</code> 초기화 중...")
                continue

            best_bid = max(state.bids.keys(), key=float)
            best_ask = min(state.asks.keys(), key=float)
            bid_f = float(best_bid)
            ask_f = float(best_ask)
            spread = ask_f - bid_f
            spread_bps = (spread / ask_f) * 10000 if ask_f else 0
            mid = (bid_f + ask_f) / 2

            # 스프레드 상태 아이콘
            if spread_bps < 1:
                sp_icon = "🟢"
            elif spread_bps < 3:
                sp_icon = "🟡"
            else:
                sp_icon = "🔴"

            rows.append(
                f"  {sp_icon} <code>{sym_upper:<10}</code> "
                f"<b>${mid:>10,.2f}</b>  "
                f"sp:<code>{spread_bps:.1f}bp</code>"
            )

        # 버퍼 수집 현황
        ob_total = sum(len(v) for v in buffer._orderbook_data.values())
        tr_total = sum(len(v) for v in buffer._trade_data.values())
        mem_mb = buffer.estimate_memory_usage() / (1024 * 1024)

        text = (
            "━━━━━━━━━━━━━━━━━━━━\n"
            "📡 <b>LIVE TICKER</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🕐 {self._now_str()}\n"
            "\n"
            "┌── 💱 시세 / 스프레드 ──┐\n"
            + "\n".join(rows) + "\n"
            "└────────────────────────┘\n"
            "\n"
            "┌── 📊 버퍼 현황 ───────┐\n"
            f"│ 오더북  <code>{ob_total:>8,}</code>건     │\n"
            f"│ 체결    <code>{tr_total:>8,}</code>건     │\n"
            f"│ 메모리  <code>{mem_mb:>7.1f}MB</code>     │\n"
            "└────────────────────────┘\n"
            "\n"
            "🟢 수집 정상 가동 중\n"
            "━━━━━━━━━━━━━━━━━━━━"
        )
        await self.send_message(text)


