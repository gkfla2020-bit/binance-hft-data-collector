"""6심볼 5분 수집 테스트 → CSV 출력"""

import asyncio
import json
import logging
import time
import sys
from pathlib import Path
from dataclasses import asdict

import websockets
import pandas as pd

from src.models import AggTradeEvent, KlineEvent, OrderBookSnapshot, DepthDiffEvent
from src.orderbook_manager import OrderBookManager
from src.buffer import DataBuffer
from src.integrity_logger import IntegrityLogger

# 갭 로그 너무 많이 찍히는 거 방지
logging.basicConfig(level=logging.ERROR)

SYMBOLS = ["btcusdt", "ethusdt", "xrpusdt", "solusdt", "bnbusdt", "dogeusdt"]
COLLECT_SECONDS = 120
DATA_DIR = Path("./data_test2")
DATA_DIR.mkdir(exist_ok=True)


async def main():
    print(f"🚀 바이낸스 데이터 수집 시작 ({COLLECT_SECONDS}초)...")

    buffer = DataBuffer()
    il = IntegrityLogger("./logs")
    ob_manager = OrderBookManager(SYMBOLS, il)

    # WebSocket 먼저 연결 (바이낸스 공식 가이드: WS 연결 → diff 버퍼링 → 스냅샷)
    streams = []
    for s in SYMBOLS:
        streams.append(f"{s}@depth@100ms")
        streams.append(f"{s}@aggTrade")
        streams.append(f"{s}@kline_1m")
    url = f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}"

    counts = {"orderbook": 0, "trade": 0, "kline": 0}
    reinit_lock = set()
    start = time.time()

    print(f"  🔗 WebSocket 연결 중...")
    async with websockets.connect(url, ping_interval=20) as ws:
        print(f"  ✅ 연결 성공!")

        # WS 연결 후 스냅샷 가져오기 (공식 가이드 순서)
        for sym in SYMBOLS:
            try:
                await ob_manager.initialize(sym, 1000)
                print(f"  ✅ {sym.upper()} 오더북 스냅샷 로드 완료")
            except Exception as e:
                print(f"  ❌ {sym.upper()} 스냅샷 실패: {e}")

        print(f"\n  📡 데이터 수신 중...\n")

        while time.time() - start < COLLECT_SECONDS:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=5)
                recv_time = time.time()
                data = json.loads(raw)
                stream = data.get("stream", "")
                payload = data.get("data", {})

                if "depth" in stream:
                    sym_name = stream.split("@")[0].upper()
                    event = DepthDiffEvent(
                        symbol=sym_name,
                        event_time=payload.get("E", 0),
                        recv_time=recv_time,
                        first_update_id=payload.get("U", 0),
                        final_update_id=payload.get("u", 0),
                        bids=payload.get("b", []),
                        asks=payload.get("a", []),
                    )
                    snapshot = ob_manager.apply_diff(sym_name, event)
                    if snapshot:
                        await buffer.add_orderbook(sym_name, asdict(snapshot))
                        counts["orderbook"] += 1
                    elif not ob_manager.books[sym_name].initialized and sym_name not in reinit_lock:
                        reinit_lock.add(sym_name)
                        try:
                            await ob_manager.initialize(sym_name.lower(), 1000)
                            print(f"\n  🔄 {sym_name} 오더북 재초기화 완료")
                        except Exception as e:
                            print(f"\n  ❌ {sym_name} 재초기화 실패: {e}")
                        finally:
                            reinit_lock.discard(sym_name)

                elif "aggTrade" in stream:
                    event = AggTradeEvent(
                        symbol=payload.get("s", ""),
                        trade_id=payload.get("a", 0),
                        price=payload.get("p", "0"),
                        quantity=payload.get("q", "0"),
                        first_trade_id=payload.get("f", 0),
                        last_trade_id=payload.get("l", 0),
                        trade_time=payload.get("T", 0),
                        recv_time=recv_time,
                        is_buyer_maker=payload.get("m", False),
                    )
                    await buffer.add_trade(event.symbol, asdict(event))
                    counts["trade"] += 1

                elif "kline" in stream:
                    k = payload.get("k", {})
                    if k.get("x", False):
                        event = KlineEvent(
                            symbol=k.get("s", ""),
                            open_time=k.get("t", 0),
                            close_time=k.get("T", 0),
                            open=k.get("o", "0"), high=k.get("h", "0"),
                            low=k.get("l", "0"), close=k.get("c", "0"),
                            volume=k.get("v", "0"), quote_volume=k.get("q", "0"),
                            trade_count=k.get("n", 0), recv_time=recv_time,
                        )
                        await buffer.add_kline(event.symbol, asdict(event))
                        counts["kline"] += 1

                elapsed = int(time.time() - start)
                sys.stdout.write(
                    f"\r  ⏱ {elapsed}s | 오더북: {counts['orderbook']} | "
                    f"체결: {counts['trade']} | 캔들: {counts['kline']}"
                )
                sys.stdout.flush()

            except asyncio.TimeoutError:
                continue

    print(f"\n\n📊 수집 완료! 총 {sum(counts.values())}건")

    # 심볼별 카운트 출력
    print("\n📊 심볼별 수집 현황:")
    sym_counts = {}
    for sym in SYMBOLS:
        sym_upper = sym.upper()
        ob_cnt = len(buffer._orderbook_data.get(sym_upper, []))
        tr_cnt = len(buffer._trade_data.get(sym_upper, []))
        kl_cnt = len(buffer._kline_data.get(sym_upper, []))
        sym_counts[sym_upper] = {"orderbook": ob_cnt, "trade": tr_cnt, "kline": kl_cnt}
        total = ob_cnt + tr_cnt + kl_cnt
        print(f"  {sym_upper:12s} | 오더북: {ob_cnt:>5,} | 체결: {tr_cnt:>5,} | 캔들: {kl_cnt:>3} | 합계: {total:>6,}")

    data = await buffer.flush()

    for symbol in SYMBOLS:
        sym = symbol.upper()
        ob_records = data["orderbook"].get(sym, [])
        if ob_records:
            df = pd.DataFrame(ob_records)
            df["datetime_utc"] = pd.to_datetime(df["event_time"], unit="ms").dt.strftime("%Y-%m-%d %H:%M:%S.%f")
            df["recv_datetime_utc"] = pd.to_datetime(df["recv_time"], unit="s").dt.strftime("%Y-%m-%d %H:%M:%S.%f")
            csv_path = DATA_DIR / f"{sym}_orderbook.csv"
            df.to_csv(csv_path, index=False)
            print(f"  📁 {csv_path} ({len(df)}건)")

        trade_records = data["trade"].get(sym, [])
        if trade_records:
            df = pd.DataFrame(trade_records)
            df["datetime_utc"] = pd.to_datetime(df["trade_time"], unit="ms").dt.strftime("%Y-%m-%d %H:%M:%S.%f")
            df["recv_datetime_utc"] = pd.to_datetime(df["recv_time"], unit="s").dt.strftime("%Y-%m-%d %H:%M:%S.%f")
            csv_path = DATA_DIR / f"{sym}_trades.csv"
            df.to_csv(csv_path, index=False)
            print(f"  📁 {csv_path} ({len(df)}건)")

    print("\n" + "="*60)
    print("📋 체결 데이터 미리보기 (BTCUSDT 최근 10건)")
    print("="*60)
    btc_trades = data["trade"].get("BTCUSDT", [])
    if btc_trades:
        df = pd.DataFrame(btc_trades[-10:])
        df["trade_time_str"] = pd.to_datetime(df["trade_time"], unit="ms")
        print(df[["trade_time_str", "price", "quantity", "is_buyer_maker"]].to_string(index=False))

    print("\n" + "="*60)
    print("📋 오더북 데이터 미리보기 (BTCUSDT 최근 3건)")
    print("="*60)
    btc_ob = data["orderbook"].get("BTCUSDT", [])
    if btc_ob:
        for i, rec in enumerate(btc_ob[-3:]):
            bids = json.loads(rec["bids"]) if isinstance(rec["bids"], str) else rec["bids"]
            asks = json.loads(rec["asks"]) if isinstance(rec["asks"], str) else rec["asks"]
            print(f"\n  [{i+1}] update_id={rec['last_update_id']}")
            print(f"      최우선 매수: {bids[0][0]} x {bids[0][1]}")
            print(f"      최우선 매도: {asks[0][0]} x {asks[0][1]}")
            spread = float(asks[0][0]) - float(bids[0][0])
            print(f"      스프레드: {spread:.2f}")

    print(f"\n✅ CSV 파일 저장 위치: {DATA_DIR.absolute()}")


if __name__ == "__main__":
    asyncio.run(main())
