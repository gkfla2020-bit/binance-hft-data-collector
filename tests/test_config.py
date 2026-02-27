"""Config YAML 라운드트립 테스트
Feature: binance-data-collector, Property 12: 설정 YAML 라운드트립
"""

import tempfile
import os

import pytest
from hypothesis import given, strategies as st, settings

from src.config import Config


# ── Hypothesis 전략 ──

symbol_st = st.text(
    alphabet=st.characters(whitelist_categories=("Ll",), whitelist_characters="0123456789"),
    min_size=3, max_size=10,
)

config_st = st.builds(
    Config,
    symbols=st.lists(symbol_st, min_size=1, max_size=5),
    flush_interval=st.integers(min_value=60, max_value=86400),
    data_dir=st.just("./data"),
    log_dir=st.just("./logs"),
    max_buffer_mb=st.integers(min_value=50, max_value=2000),
    cleanup_days=st.integers(min_value=1, max_value=90),
    cloud_remote=st.from_regex(r"[a-z0-9]{0,20}", fullmatch=True),
    cloud_path=st.from_regex(r"[a-z0-9/]{0,50}", fullmatch=True),
    orderbook_depth=st.sampled_from([100, 500, 1000, 5000]),
    orderbook_top_levels=st.integers(min_value=5, max_value=50),
    telegram_bot_token=st.from_regex(r"[a-zA-Z0-9:_\-]{0,50}", fullmatch=True),
    telegram_chat_id=st.from_regex(r"[0-9\-]{0,20}", fullmatch=True),
    use_futures=st.booleans(),
)


# ── Property 12: Config YAML 라운드트립 ──

class TestConfigYamlRoundtrip:
    """Validates: Requirements 8.3"""

    @given(config=config_st)
    @settings(max_examples=100)
    def test_yaml_roundtrip(self, config: Config):
        """For any valid Config, YAML serialize then deserialize produces identical Config."""
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False, mode="w") as f:
            tmp_path = f.name

        try:
            config.to_yaml(tmp_path)
            restored = Config.from_yaml(tmp_path)
            assert config == restored, f"Roundtrip failed: {config} != {restored}"
        finally:
            os.unlink(tmp_path)


# ── 단위 테스트 ──

class TestConfigUnit:

    def test_default_config(self):
        c = Config()
        assert c.symbols == ["btcusdt", "ethusdt", "xrpusdt"]
        assert c.flush_interval == 3600
        assert c.use_futures is True

    def test_from_yaml_missing_file(self):
        c = Config.from_yaml("/nonexistent/path.yaml")
        assert c == Config()

    def test_from_yaml_ignores_unknown_keys(self):
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False, mode="w") as f:
            f.write("symbols: [btcusdt]\nunknown_key: 42\n")
            tmp_path = f.name
        try:
            c = Config.from_yaml(tmp_path)
            assert c.symbols == ["btcusdt"]
        finally:
            os.unlink(tmp_path)
