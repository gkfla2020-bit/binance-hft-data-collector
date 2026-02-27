"""데이터 무결성 로깅 모듈 - 갭 추적, 통계, 일별 요약, 커버리지"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


class IntegrityLogger:
    """데이터 무결성 로깅"""

    def __init__(self, log_dir: Path | str):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._gaps: list[dict] = []
        self._reconnects: list[dict] = []
        self._flush_stats: list[dict] = []
        self._sync_events: list[dict] = []
        self._message_counts: dict[str, int] = defaultdict(int)
        self._period_start: float = 0.0

    MAX_GAP_BUFFER = 10000  # 갭 기록 최대 보관 수

    def record_gap(self, symbol: str, expected_id: int, actual_id: int,
                   timestamp: float) -> None:
        """시퀀스 갭 기록"""
        if len(self._gaps) >= self.MAX_GAP_BUFFER:
            self._gaps = self._gaps[-self.MAX_GAP_BUFFER // 2:]
        self._gaps.append({
            "timestamp": timestamp,
            "symbol": symbol,
            "expected_id": expected_id,
            "actual_id": actual_id,
        })
        logger.warning(f"[갭] {symbol} expected={expected_id} actual={actual_id}")

    def record_flush(self, symbol: str, datatype: str, record_count: int,
                     file_size: int, time_range: tuple[float, float]) -> None:
        """플러시 통계 기록"""
        self._flush_stats.append({
            "symbol": symbol,
            "datatype": datatype,
            "record_count": record_count,
            "file_size": file_size,
            "time_start": time_range[0],
            "time_end": time_range[1],
        })

    def record_reconnect(self, timestamp: float, reason: str) -> None:
        """재연결 이벤트 기록"""
        self._reconnects.append({
            "timestamp": timestamp,
            "reason": reason,
        })

    def record_sync(self, filepath: str, status: str) -> None:
        """동기화 상태 기록"""
        self._sync_events.append({
            "filepath": filepath,
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    def increment_message_count(self, symbol: str) -> None:
        """메시지 수신 카운트 증가"""
        self._message_counts[symbol] += 1

    def get_periodic_stats(self) -> dict:
        """현재 주기 통계 반환"""
        now = datetime.now(timezone.utc)
        stats = {
            "timestamp": now.isoformat(),
            "gaps": list(self._gaps),
            "gap_count": len(self._gaps),
            "reconnect_count": len(self._reconnects),
            "flush_stats": list(self._flush_stats),
            "message_counts": dict(self._message_counts),
        }
        return stats

    async def write_periodic_log(self) -> None:
        """주기적 통계 JSON 로그 작성"""
        stats = self.get_periodic_stats()
        now = datetime.now(timezone.utc)
        log_file = self.log_dir / f"stats_{now.strftime('%Y%m%d_%H')}.json"
        with open(log_file, "w") as f:
            json.dump(stats, f, indent=2, default=str)
        # 주기 통계 리셋
        self._gaps.clear()
        self._reconnects.clear()
        self._flush_stats.clear()
        self._message_counts.clear()
        logger.info(f"[로그] {log_file}")

    async def write_daily_summary(self) -> None:
        """일별 요약 리포트 생성"""
        now = datetime.now(timezone.utc)
        summary = {
            "date": now.strftime("%Y-%m-%d"),
            "total_gaps": len(self._gaps),
            "total_reconnects": len(self._reconnects),
            "total_flushes": len(self._flush_stats),
        }
        log_file = self.log_dir / f"daily_{now.strftime('%Y%m%d')}.json"
        with open(log_file, "w") as f:
            json.dump(summary, f, indent=2, default=str)

    @staticmethod
    def compute_coverage(total_seconds: float, gap_seconds: float) -> float:
        """데이터 커버리지 비율 계산 (0.0 ~ 1.0)"""
        if total_seconds <= 0:
            return 0.0
        gap_seconds = max(0.0, min(gap_seconds, total_seconds))
        return (total_seconds - gap_seconds) / total_seconds

    async def update_coverage_summary(self, symbol_stats: dict[str, dict] | None = None) -> None:
        """누적 커버리지 통계 갱신 (coverage_summary.json)

        symbol_stats: {symbol: {"total_seconds": float, "gap_seconds": float, "msg_count": int}}
        """
        filepath = self.log_dir / "coverage_summary.json"
        existing = {}
        if filepath.exists():
            with open(filepath, "r") as f:
                existing = json.load(f)

        if symbol_stats:
            for symbol, stats in symbol_stats.items():
                total = stats.get("total_seconds", 0)
                gaps = stats.get("gap_seconds", 0)
                coverage = self.compute_coverage(total, gaps)
                existing[symbol] = {
                    "coverage": coverage,
                    "total_seconds": total,
                    "gap_seconds": gaps,
                    "msg_count": stats.get("msg_count", 0),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }

        with open(filepath, "w") as f:
            json.dump(existing, f, indent=2, default=str)

