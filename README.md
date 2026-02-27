# 🔗 Binance HFT Data Collector

바이낸스 거래소의 고빈도 시장 데이터를 실시간으로 수집하는 프로덕션급 Python 시스템입니다.

WebSocket 스트림을 통해 오더북(L2), 체결(aggTrade), 캔들(kline), 청산(forceOrder), 펀딩비 데이터를 동시에 수집하고, Parquet 포맷으로 저장합니다.

## 주요 기능

| 기능 | 설명 |
|------|------|
| 오더북 재구성 | REST 스냅샷 + WebSocket diff 방식으로 L2 오더북 실시간 유지 (바이낸스 공식 가이드 준수) |
| 시퀀스 검증 | `lastUpdateId` 연속성 검증, 갭 감지 시 자동 재초기화 |
| 멀티 심볼 | 6개 이상 심볼 동시 수집 (combined stream) |
| Parquet 저장 | Snappy 압축, 원자적 쓰기 (tmp → rename), SHA-256 체크섬 |
| 클라우드 동기화 | rclone 기반 자동 업로드 + 로컬 파일 정리 |
| 텔레그램 알림 | 시작/종료, 연결 끊김, 재연결, 갭 감지, 일별 리포트 |
| 시간 동기화 | NTP 오프셋 + 바이낸스 서버 RTT 10분 주기 측정 |
| 무결성 로깅 | 갭 추적, 플러시 통계, 커버리지 계산, 일별 요약 |
| 환경 기록 | OS, Python, 패키지 버전, CPU/RAM 등 메타데이터 자동 기록 |
| Graceful Shutdown | SIGINT/SIGTERM 수신 시 마지막 플러시 후 종료 |

## 아키텍처

```
┌─────────────────────────────────────────────────────────┐
│                    Binance WebSocket                     │
│         (depth@100ms, aggTrade, kline_1m, forceOrder)    │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
              ┌─────────────────┐
              │    Collector     │  ← 메시지 파싱 & 라우팅
              │  (spot+futures)  │     지수 백오프 재연결
              └────────┬────────┘
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
   ┌─────────────┐ ┌────────┐ ┌──────────┐
   │ OrderBook   │ │ Buffer │ │ Funding  │
   │ Manager     │ │        │ │ Rate     │
   │ (검증+재구성)│ │(메모리) │ │ Collector│
   └─────────────┘ └───┬────┘ └──────────┘
                       │
                       ▼
              ┌─────────────────┐
              │     Flusher      │  ← Parquet 저장 (snappy)
              │  (주기적/강제)    │     SHA-256 체크섬
              └────────┬────────┘
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
   ┌──────────┐ ┌───────────┐ ┌──────────────┐
   │  Syncer  │ │ Integrity │ │   Telegram   │
   │ (rclone) │ │  Logger   │ │  Reporter    │
   └──────────┘ └───────────┘ └──────────────┘
```

## 수집 데이터

| 데이터 | 소스 | 주기 | 설명 |
|--------|------|------|------|
| 오더북 (L2) | `depth@100ms` WS | 100ms | 상위 20호가, update_id 포함 |
| 체결 | `aggTrade` WS | 실시간 | 가격, 수량, 방향(maker/taker) |
| 캔들 | `kline_1m` WS | 1분 (확정만) | OHLCV + 거래 횟수 |
| 청산 | `forceOrder` WS (선물) | 실시간 | 강제 청산 이벤트 |
| 펀딩비 | REST API (선물) | 8시간 | 펀딩비율, 다음 펀딩 시각 |

## 프로젝트 구조

```
├── config.yaml              # 시스템 설정
├── src/
│   ├── main.py              # 엔트리포인트 (모듈 초기화 & 동시 실행)
│   ├── collector.py         # WebSocket 수신 & 메시지 라우팅
│   ├── orderbook_manager.py # 오더북 재구성 & 시퀀스 검증
│   ├── buffer.py            # 메모리 버퍼 (심볼별 분리)
│   ├── flusher.py           # Parquet 저장 & 체크섬
│   ├── syncer.py            # rclone 클라우드 동기화
│   ├── integrity_logger.py  # 무결성 로깅 & 커버리지
│   ├── telegram_reporter.py # 텔레그램 알림
│   ├── funding_rate_collector.py # 펀딩비 수집
│   ├── time_sync_monitor.py # NTP/바이낸스 시간 동기화
│   ├── environment_recorder.py # 환경 메타데이터 기록
│   ├── config.py            # Config 데이터클래스
│   └── models.py            # 데이터 모델 정의
├── tests/                   # 단위 테스트
├── data/                    # Parquet 저장 디렉토리
└── logs/                    # 로그 & 통계 JSON
```

## 설치 및 실행

### 요구사항

- Python 3.12+
- pip 패키지: `websockets`, `aiohttp`, `pandas`, `pyarrow`, `pyyaml`
- (선택) `ntplib`, `psutil`, `rclone`

### 설치

```bash
git clone https://github.com/gkfla2020-bit/binance-hft-data-collector.git
cd binance-hft-data-collector

pip install websockets aiohttp pandas pyarrow pyyaml
pip install ntplib psutil  # 선택
```

### 설정

`config.yaml`을 수정합니다:

```yaml
symbols:
  - btcusdt
  - ethusdt
  - xrpusdt
  - solusdt
  - bnbusdt
  - dogeusdt

flush_interval: 3600        # 플러시 주기 (초)
data_dir: "./data"
orderbook_depth: 1000       # REST 스냅샷 깊이
orderbook_top_levels: 20    # 저장할 호가 수
use_futures: true            # 선물 API (청산/펀딩비)

# 클라우드 동기화 (선택)
cloud_remote: "gdrive"
cloud_path: "crypto_data"
cleanup_days: 7

# 텔레그램 알림 (선택)
telegram_bot_token: ""
telegram_chat_id: ""
```

### 실행

```bash
python -m src.main
# 또는 설정 파일 지정
python -m src.main config.yaml
```

### 빠른 테스트 (5분 수집 → CSV)

```bash
python quick_test.py
```

6개 심볼의 오더북/체결 데이터를 5분간 수집하고 `data_test/` 디렉토리에 CSV로 저장합니다.

## 오더북 재구성 방식

[바이낸스 공식 가이드](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#how-to-manage-a-local-order-book-correctly)를 준수합니다:

1. WebSocket `depth@100ms` 스트림 연결
2. diff 이벤트 버퍼링 시작
3. REST API로 스냅샷 요청 (`/api/v3/depth?limit=1000`)
4. `U <= lastUpdateId+1 <= u` 검증으로 연속성 확인
5. 검증 실패(갭) 시 자동 재초기화 (3초 grace period 적용)

## 데이터 무결성

- 모든 Parquet 파일에 SHA-256 체크섬 기록 (`checksums.json`)
- 원자적 파일 쓰기 (임시 파일 → `os.replace`)
- 시퀀스 갭 감지 및 자동 복구
- 메모리 임계값(500MB) 초과 시 강제 플러시
- 데이터 커버리지 비율 추적

## 테스트

```bash
pytest tests/ -v
```

## 라이선스

MIT License
