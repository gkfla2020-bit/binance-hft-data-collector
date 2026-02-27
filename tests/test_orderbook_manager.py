"""OrderBookManager 테스트
Feature: binance-data-collector
Property 3: 시퀀스 연속성 검증
Property 4: diff 적용 정확성
Property 5: 상위 N호가 정렬
"""

import pytest
from hypothesis import given, strategies as st, settings, assume

from src.orderbook_manager import OrderBookManager
from src.models import DepthDiffEvent, OrderBookState


# ── 공통 전략 ──

price_st = st.decimals(min_value=0.01, max_value=100000, places=2).map(str)
qty_st = st.decimals(min_value=0.001, max_value=1000, places=3).map(str)
price_qty_st = st.tuples(price_st, qty_st).map(list)
update_id_st = st.integers(min_value=1, max_value=10**12)


def make_manager_with_state(symbol: str, bids: dict, asks: dict,
                            last_update_id: int) -> OrderBookManager:
    """테스트용 초기화된 OrderBookManager 생성"""
    mgr = OrderBookManager(symbols=[symbol])
    sym = symbol.upper()
    mgr.books[sym] = OrderBookState(
        bids=dict(bids), asks=dict(asks),
        last_update_id=last_update_id, initialized=True,
    )
    return mgr


# ── Property 3: 시퀀스 연속성 검증 ──

class TestSequenceValidation:
    """Validates: Requirements 2.2"""

    @given(
        last_id=st.integers(min_value=1, max_value=10**9),
        first_id=st.integers(min_value=1, max_value=10**9),
        span=st.integers(min_value=0, max_value=100),
    )
    @settings(max_examples=200)
    def test_sequence_continuity(self, last_id: int, first_id: int, span: int):
        """F <= last_update_id+1 <= L 일 때만 유효"""
        final_id = first_id + span
        mgr = make_manager_with_state("BTCUSDT", {}, {}, last_id)
        expected = last_id + 1
        is_valid = mgr.validate_sequence("BTCUSDT", first_id, final_id)

        if first_id <= expected <= final_id:
            assert is_valid, f"Should be valid: F={first_id} <= {expected} <= L={final_id}"
        else:
            assert not is_valid, f"Should be invalid: F={first_id}, expected={expected}, L={final_id}"


# ── Property 4: diff 적용 정확성 ──

class TestDiffApplication:
    """Validates: Requirements 2.4"""

    @given(
        existing_prices=st.dictionaries(price_st, qty_st, min_size=1, max_size=20),
        update_prices=st.lists(price_qty_st, min_size=1, max_size=10),
        zero_removals=st.lists(price_st, min_size=0, max_size=5),
    )
    @settings(max_examples=200)
    def test_diff_correctness(self, existing_prices, update_prices, zero_removals):
        """diff 적용 후: 0 qty는 제거, 비-0 qty는 갱신, 미포함 가격은 불변"""
        last_id = 100
        mgr = make_manager_with_state("BTCUSDT", dict(existing_prices), {}, last_id)
        original_bids = dict(existing_prices)

        # diff 구성: 업데이트 + 제거
        bid_updates = list(update_prices)
        for p in zero_removals:
            bid_updates.append([p, "0"])

        event = DepthDiffEvent(
            symbol="BTCUSDT", event_time=1000, recv_time=1.0,
            first_update_id=last_id + 1, final_update_id=last_id + 1,
            bids=bid_updates, asks=[],
        )
        mgr.apply_diff("BTCUSDT", event)
        state = mgr.books["BTCUSDT"]

        # 최종 상태 계산 (순서대로 적용했을 때의 기대값)
        expected_bids = dict(original_bids)
        for p, q in bid_updates:
            if q == "0":
                expected_bids.pop(p, None)
            else:
                expected_bids[p] = q

        # 검증: 실제 상태가 기대 상태와 일치
        assert state.bids == expected_bids

        # 미포함 가격은 불변
        updated_prices = {p for p, _ in bid_updates}
        for p, q in original_bids.items():
            if p not in updated_prices:
                assert state.bids.get(p) == q, f"Untouched price {p} changed"


# ── Property 5: 상위 N호가 정렬 ──

class TestTopLevelsSorting:
    """Validates: Requirements 2.5"""

    @given(
        bids=st.dictionaries(price_st, qty_st, min_size=1, max_size=50),
        asks=st.dictionaries(price_st, qty_st, min_size=1, max_size=50),
        levels=st.integers(min_value=1, max_value=30),
    )
    @settings(max_examples=200)
    def test_top_levels_sorted(self, bids, asks, levels):
        """bids는 내림차순, asks는 오름차순, 각각 최대 N개"""
        mgr = make_manager_with_state("BTCUSDT", bids, asks, 100)
        snap = mgr.get_top_levels("BTCUSDT", levels=levels)

        # 개수 제한
        assert len(snap.bids) <= levels
        assert len(snap.asks) <= levels

        # bids 내림차순
        if len(snap.bids) > 1:
            bid_prices = [float(p) for p, _ in snap.bids]
            assert bid_prices == sorted(bid_prices, reverse=True)

        # asks 오름차순
        if len(snap.asks) > 1:
            ask_prices = [float(p) for p, _ in snap.asks]
            assert ask_prices == sorted(ask_prices)


# ── 단위 테스트 ──

class TestOrderBookManagerUnit:

    def test_uninitialized_returns_none(self):
        mgr = OrderBookManager(symbols=["btcusdt"])
        event = DepthDiffEvent("BTCUSDT", 1000, 1.0, 1, 1, [], [])
        assert mgr.apply_diff("BTCUSDT", event) is None

    def test_gap_sets_uninitialized(self):
        mgr = make_manager_with_state("BTCUSDT", {}, {}, 100)
        # 갭: expected=101 but first_id=200
        event = DepthDiffEvent("BTCUSDT", 1000, 1.0, 200, 200, [], [])
        result = mgr.apply_diff("BTCUSDT", event)
        assert result is None
        assert mgr.books["BTCUSDT"].initialized is False

    def test_valid_diff_updates_last_update_id(self):
        mgr = make_manager_with_state("BTCUSDT", {"100.0": "1.0"}, {}, 100)
        event = DepthDiffEvent("BTCUSDT", 1000, 1.0, 101, 105, [], [])
        result = mgr.apply_diff("BTCUSDT", event)
        assert result is not None
        assert mgr.books["BTCUSDT"].last_update_id == 105
