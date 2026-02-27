"""메인 애플리케이션 - 모든 모듈 초기화 및 동시 실행"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
from pathlib import Path

from src.config import Config
from src.models import *
from src.orderbook_manager import OrderBookManager
from src.buffer import DataBuffer
from src.flusher import Flusher
from src.collector import Collector
from src.integrity_logger import IntegrityLogger
from src.syncer import Syncer
from src.telegram_reporter import TelegramReporter
from src.funding_rate_collector import FundingRateCollector
from src.time_sync_monitor import TimeSyncMonitor
from src.environment_recorder import EnvironmentRecorder

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


async def main(config_path: str = "config.yaml") -> None:
    """모든 모듈 초기화 및 asyncio.gather로 동시 실행"""
    config = Config.from_yaml(config_path)

    # 디렉토리 생성 (로깅 FileHandler보다 먼저)
    Path(config.data_dir).mkdir(parents=True, exist_ok=True)
    Path(config.log_dir).mkdir(parents=True, exist_ok=True)

    # 파일 핸들러 추가 (디렉토리 생성 후)
    file_handler = logging.FileHandler(
        Path(config.log_dir) / "collector.log", encoding="utf-8"
    )
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    )
    logging.getLogger().addHandler(file_handler)

    # 모듈 초기화
    integrity_logger = IntegrityLogger(config.log_dir)
    telegram = TelegramReporter(config)
    buffer = DataBuffer(config.max_buffer_mb)
    ob_manager = OrderBookManager(config.symbols, integrity_logger)
    syncer = Syncer(config, integrity_logger)
    flusher = Flusher(config, buffer, integrity_logger,
                      on_file_created=syncer.enqueue_file)
    collector = Collector(config, ob_manager, buffer, integrity_logger, telegram)
    funding_collector = FundingRateCollector(config, buffer, integrity_logger)
    time_sync = TimeSyncMonitor(config, integrity_logger, telegram)

    # 환경 메타데이터 기록
    env_recorder = EnvironmentRecorder(config, config.log_dir)
    env_recorder.record()

    # 시작 알림
    await telegram.send_startup_report(config)
    logger.info("=== 바이낸스 데이터 수집 시스템 시작 ===")
    logger.info(f"심볼: {[s.upper() for s in config.symbols]}")
    logger.info(f"플러시 주기: {config.flush_interval}초")

    # 주기적 로그/리포트 태스크
    async def periodic_log():
        while True:
            await asyncio.sleep(config.flush_interval)
            await integrity_logger.write_periodic_log()

    async def daily_summary():
        while True:
            await asyncio.sleep(86400)
            await integrity_logger.write_daily_summary()
            stats = integrity_logger.get_periodic_stats()
            await telegram.send_daily_report(stats)

    # 강제 플러시 감시
    async def force_flush_monitor():
        while True:
            await asyncio.sleep(30)
            if buffer.needs_force_flush():
                logger.warning("[강제 플러시] 메모리 임계값 초과")
                await flusher.flush_now()

    # 모든 태스크 동시 실행
    tasks = [
        collector.run(),
        flusher.run(),
        syncer.run(),
        time_sync.run(),
        periodic_log(),
        daily_summary(),
        force_flush_monitor(),
    ]

    # 선물 API 사용 시 펀딩비 수집 추가
    if config.use_futures:
        tasks.append(funding_collector.run())

    # graceful shutdown
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def _signal_handler():
        logger.info("종료 신호 수신, 마지막 플러시 실행 중...")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    # 태스크 실행
    gathered = asyncio.gather(*tasks, return_exceptions=True)

    # shutdown 대기
    done, _ = await asyncio.wait(
        [asyncio.create_task(shutdown_event.wait()), gathered],
        return_when=asyncio.FIRST_COMPLETED,
    )

    # 종료 시 마지막 플러시
    logger.info("마지막 플러시 실행...")
    try:
        await flusher.flush_now()
    except Exception as e:
        logger.error(f"마지막 플러시 실패: {e}")

    logger.info("=== 시스템 종료 ===")


if __name__ == "__main__":
    config_file = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    asyncio.run(main(config_file))
