"""데이터 모델 정의 - 바이낸스 WebSocket/REST 이벤트 및 내부 상태"""

from dataclasses import dataclass, field


# ── 오더북 관련 ──

@dataclass
class DepthDiffEvent:
    """바이낸스 depth_diff WebSocket 이벤트"""
    symbol: str
    event_time: int              # 거래소 이벤트 시각 (ms)
    recv_time: float             # 로컬 수신 시각 (unix timestamp)
    first_update_id: int         # U
    final_update_id: int         # u
    bids: list[list[str]]        # [[price, qty], ...]
    asks: list[list[str]]        # [[price, qty], ...]


@dataclass
class OrderBookState:
    """심볼별 오더북 내부 상태"""
    bids: dict[str, str] = field(default_factory=dict)
    asks: dict[str, str] = field(default_factory=dict)
    last_update_id: int = 0
    initialized: bool = False
    init_time: float = 0.0       # 초기화 시각 (grace period용)


@dataclass
class OrderBookSnapshot:
    """버퍼에 저장되는 오더북 스냅샷 레코드"""
    symbol: str
    event_time: int
    recv_time: float
    last_update_id: int
    bids: list[list[str]]        # 상위 N호가
    asks: list[list[str]]        # 상위 N호가


# ── 체결 관련 ──

@dataclass
class AggTradeEvent:
    """바이낸스 aggTrade WebSocket 이벤트"""
    symbol: str
    trade_id: int
    price: str
    quantity: str
    first_trade_id: int
    last_trade_id: int
    trade_time: int              # T (ms)
    recv_time: float
    is_buyer_maker: bool


# ── 청산 관련 ──

@dataclass
class LiquidationEvent:
    """바이낸스 forceOrder WebSocket 이벤트"""
    symbol: str
    side: str                    # SELL / BUY
    order_type: str
    price: str
    quantity: str
    trade_time: int              # T (ms)
    recv_time: float


# ── 펀딩비 관련 ──

@dataclass
class FundingRateRecord:
    """바이낸스 펀딩비 REST API 응답"""
    symbol: str
    funding_rate: str
    funding_time: int            # ms
    next_funding_time: int       # ms
    recv_time: float


# ── 캔들 관련 ──

@dataclass
class KlineEvent:
    """바이낸스 kline WebSocket 이벤트 (확정된 캔들만)"""
    symbol: str
    open_time: int               # t (ms)
    close_time: int              # T (ms)
    open: str
    high: str
    low: str
    close: str
    volume: str
    quote_volume: str
    trade_count: int
    recv_time: float
