"""
바이낸스 오더북 & 체결 데이터 수집기
- BTC/USDT, ETH/USDT, XRP/USDT
- WebSocket 스트리밍 (100ms 오더북 + 체결 틱)
- 메모리 버퍼링 → 1시간마다 Parquet 저장
- 7일 지난 파일 자동 삭제
"""

import asyncio
import json
import time
import os
import glob
from datetime import datetime, timedelta
from pathlib import Path

import websockets
import pandas as pd

# ── 설정 ──
SYMBOLS = ["btcusdt", "ethusdt", "xrpusdt"]
DATA_DIR = Path("./data")
FLUSH_INTERVAL = 3600  # 1시간(초)
CLEANUP_DAYS = 7       # 7일 지난 파일 삭제

# 메모리 버퍼
orderbook_buffer = []
trade_buffer = []
buffer_lock = asyncio.Lock()


def build_ws_url():
    """바이낸스 combined stream URL 생성"""
    streams = []
    for s in SYMBOLS:
        streams.append(f"{s}@depth@100ms")
        streams.append(f"{s}@aggTrade")
    return f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}"


async def collect(ws):
    """웹소켓에서 데이터 수신 → 메모리 버퍼에 적재"""
    async for msg in ws:
        data = json.loads(msg)
        stream = data.get("stream", "")
        payload = data.get("data", {})
        ts = time.time()

        async with buffer_lock:
            if "depth" in stream:
                orderbook_buffer.append({
                    "ts": ts,
                    "symbol": stream.split("@")[0].upper(),
                    "bids": json.dumps(payload.get("b", [])[:20]),  # 상위 20호가
                    "asks": json.dumps(payload.get("a", [])[:20]),
                    "event_time": payload.get("E", 0),
                })
            elif "aggTrade" in stream:
                trade_buffer.append({
                    "ts": ts,
                    "symbol": payload.get("s", ""),
                    "price": float(payload.get("p", 0)),
                    "qty": float(payload.get("q", 0)),
                    "trade_time": payload.get("T", 0),
                    "is_buyer_maker": payload.get("m", False),
                })


async def flush_to_parquet():
    """주기적으로 메모리 버퍼 → Parquet 파일 저장"""
    DATA_DIR.mkdir(exist_ok=True)

    while True:
        await asyncio.sleep(FLUSH_INTERVAL)
        now = datetime.now()
        tag = now.strftime("%Y%m%d_%H")

        async with buffer_lock:
            ob_data = orderbook_buffer.copy()
            tr_data = trade_buffer.copy()
            orderbook_buffer.clear()
            trade_buffer.clear()

        if ob_data:
            df = pd.DataFrame(ob_data)
            path = DATA_DIR / f"orderbook_{tag}.parquet"
            df.to_parquet(path, index=False)
            print(f"[저장] {path} ({len(ob_data)}건)")

        if tr_data:
            df = pd.DataFrame(tr_data)
            path = DATA_DIR / f"trades_{tag}.parquet"
            df.to_parquet(path, index=False)
            print(f"[저장] {path} ({len(tr_data)}건)")

        # 7일 지난 파일 자동 삭제
        cleanup_old_files()


def cleanup_old_files():
    """CLEANUP_DAYS일 지난 parquet 파일 삭제"""
    cutoff = time.time() - (CLEANUP_DAYS * 86400)
    for f in glob.glob(str(DATA_DIR / "*.parquet")):
        if os.path.getmtime(f) < cutoff:
            os.remove(f)
            print(f"[삭제] {f}")


async def main():
    url = build_ws_url()
    print(f"[시작] 바이낸스 오더북 수집기")
    print(f"[대상] {', '.join(s.upper() for s in SYMBOLS)}")
    print(f"[저장] {FLUSH_INTERVAL}초마다 → {DATA_DIR}/")
    print(f"[정리] {CLEANUP_DAYS}일 지난 파일 자동 삭제")
    print()

    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                print(f"[연결] 바이낸스 WebSocket 연결 성공")
                # 수집과 저장을 동시에 실행
                await asyncio.gather(
                    collect(ws),
                    flush_to_parquet(),
                )
        except Exception as e:
            print(f"[에러] {e} — 5초 후 재연결...")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
