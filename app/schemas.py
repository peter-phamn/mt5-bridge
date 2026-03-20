from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from typing import Union

from pydantic import BaseModel, field_validator


class OHLCBar(BaseModel):
    time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int           # MT5 tick_volume
    spread: Optional[int] = None  # raw spread in points

    model_config = {"json_encoders": {datetime: lambda v: v.isoformat()}}


class OHLCBarWithFeatures(OHLCBar):
    """OHLCBar extended with engineered features.  All feature fields are
    Optional because ATR/percentile columns have a NaN warmup period."""

    atr: Optional[float] = None
    spread_abs: Optional[float] = None       # spread in price units
    spread_pct: Optional[float] = None       # spread_abs / close
    spread_to_atr: Optional[float] = None    # spread_abs / ATR
    tick_volume_zscore: Optional[float] = None
    tick_volume_percentile: Optional[float] = None
    tick_volume_score: Optional[float] = None
    spread_ok: Optional[bool] = None         # True = spread filter passed


class HistoryResponse(BaseModel):
    symbol: str
    timeframe: str
    from_: datetime
    to: datetime
    count: int
    data: List[Union[OHLCBarWithFeatures, OHLCBar]]

    model_config = {"populate_by_name": True}


class DownloadRequest(BaseModel):
    symbol: str
    timeframe: str
    from_date: datetime
    to_date: Optional[datetime] = None  # defaults to now

    @field_validator("timeframe")
    @classmethod
    def validate_timeframe(cls, v: str) -> str:
        from app.config import TIMEFRAME_MAP

        v = v.upper()
        if v not in TIMEFRAME_MAP:
            raise ValueError(f"Invalid timeframe '{v}'. Valid: {list(TIMEFRAME_MAP)}")
        return v

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, v: str) -> str:
        return v.upper().strip()


class DownloadResponse(BaseModel):
    symbol: str
    timeframe: str
    rows_downloaded: int
    rows_new: int
    from_date: datetime
    to_date: datetime
    duration_seconds: float


class SymbolInfo(BaseModel):
    name: str
    description: str
    currency_base: str
    currency_profit: str
    digits: int
    trade_contract_size: float


class HealthResponse(BaseModel):
    status: str  # "ok" | "degraded" | "error"
    mt5_connected: bool
    mt5_version: Optional[str] = None
    available_symbols_cached: int
    storage_path: str


class MultiDownloadRequest(BaseModel):
    symbols: List[str]
    timeframe: str
    from_date: datetime
    to_date: Optional[datetime] = None

    @field_validator("timeframe")
    @classmethod
    def validate_timeframe(cls, v: str) -> str:
        from app.config import TIMEFRAME_MAP

        v = v.upper()
        if v not in TIMEFRAME_MAP:
            raise ValueError(f"Invalid timeframe '{v}'.")
        return v
