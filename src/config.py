"""시스템 설정 모듈 - config.yaml 로드 및 Config 데이터클래스"""

from dataclasses import dataclass, field, asdict
from pathlib import Path

import yaml


@dataclass
class Config:
    """시스템 설정 (config.yaml에서 로드)"""
    symbols: list[str] = field(default_factory=lambda: ["btcusdt", "ethusdt", "xrpusdt"])
    flush_interval: int = 3600
    data_dir: str = "./data"
    log_dir: str = "./logs"
    max_buffer_mb: int = 500
    cleanup_days: int = 7
    cloud_remote: str = ""
    cloud_path: str = ""
    orderbook_depth: int = 1000
    orderbook_top_levels: int = 20
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    use_futures: bool = True

    @classmethod
    def from_yaml(cls, path: str) -> "Config":
        """YAML 파일에서 Config 객체 생성"""
        p = Path(path)
        if not p.exists():
            return cls()
        with open(p, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    def to_yaml(self, path: str) -> None:
        """Config 객체를 YAML 파일로 저장"""
        with open(path, "w", encoding="utf-8") as f:
            yaml.dump(asdict(self), f, default_flow_style=False, allow_unicode=True)

    def to_dict(self) -> dict:
        """Config를 딕셔너리로 변환"""
        return asdict(self)
