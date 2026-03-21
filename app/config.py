from __future__ import annotations

import os
from pathlib import Path
from typing import List

from pydantic import field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # MT5 connection
    mt5_login: int = 0
    mt5_password: str = ""
    mt5_server: str = ""
    mt5_path: str = ""  # optional: path to MT5 terminal executable

    # Storage
    data_path: Path = Path("data/mt5")

    # Symbols & timeframes to track
    default_symbols: List[str] = ["XAUUSDm", "EURUSD", "USDJPY"]
    default_timeframes: List[str] = ["M1", "M5", "M15", "H1", "D1"]

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    debug: bool = False

    # Cache
    cache_max_size: int = 50  # max number of dataframes to keep in memory

    # Download
    max_retries: int = 3
    retry_delay_seconds: float = 2.0
    batch_size: int = 100_000  # rows per batch when downloading

    # DuckDB
    duckdb_enabled: bool = True

    # Feature engineering
    atr_period: int = 14
    tick_volume_window: int = 100
    tick_volume_scoring_enabled: bool = True

    # Spread filter
    spread_filter_enabled: bool = True
    spread_threshold: float = 3.0       # fixed-mode: max spread in price units
    use_dynamic_spread: bool = True     # True → ATR-ratio filter; False → fixed
    spread_atr_ratio_threshold: float = 0.3  # dynamic-mode: max spread/ATR ratio

    @field_validator("data_path", mode="before")
    @classmethod
    def resolve_data_path(cls, v: str | Path) -> Path:
        return Path(v).resolve()

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()

# Ensure data directory exists
settings.data_path.mkdir(parents=True, exist_ok=True)

# MT5 timeframe string → MT5 constant mapping (populated in mt5_client.py)
TIMEFRAME_MAP: dict[str, int] = {
    "M1": 1,
    "M2": 2,
    "M3": 3,
    "M4": 4,
    "M5": 5,
    "M6": 6,
    "M10": 10,
    "M12": 12,
    "M15": 15,
    "M20": 20,
    "M30": 30,
    "H1": 16385,
    "H2": 16386,
    "H3": 16387,
    "H4": 16388,
    "H6": 16390,
    "H8": 16392,
    "H12": 16396,
    "D1": 16408,
    "W1": 32769,
    "MN1": 49153,
}
