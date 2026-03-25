from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, field_validator


class DownloadRequest(BaseModel):
    symbol: str
    timeframe: str
    from_date: datetime
    to_date: Optional[datetime] = None  # defaults to now
    force: bool = False  # if True, ignore incremental check and re-download from from_date

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
        return v.strip()  # preserve case — MT5 symbols are case-sensitive (e.g. XAUUSDm)


class DownloadResponse(BaseModel):
    symbol: str
    timeframe: str
    rows_downloaded: int
    rows_new: int
    from_date: datetime
    effective_from: Optional[datetime] = None
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


# ── Trading schemas ────────────────────────────────────────────────────────


class AccountInfo(BaseModel):
    balance: float
    equity: float
    margin: float
    margin_free: float
    currency: str
    login: int
    server: str


class PositionInfo(BaseModel):
    ticket: int
    symbol: str
    type: str            # "buy" | "sell"
    volume: float
    price_open: float
    sl: float
    tp: float
    price_current: float
    profit: float
    comment: str
    magic: int
    time: int            # UTC unix timestamp of open time


class OrderRequest(BaseModel):
    symbol: str
    side: str            # "BUY" | "SELL"
    volume: float
    sl: float = 0.0
    tp: float = 0.0
    magic: int = 0       # 0 = use server default (settings.trade_magic)
    comment: str = "bridge"


class OrderResult(BaseModel):
    retcode: int
    ticket: int
    fill_price: float
    volume: float
    comment: str
    done: bool


class ModifyRequest(BaseModel):
    sl: float
    tp: float


class CloseRequest(BaseModel):
    lot: Optional[float] = None  # None = close full position


class DealInfo(BaseModel):
    deal: int
    ticket: int
    time: int
    type: int
    entry: int           # 0=IN 1=OUT
    volume: float
    price: float
    profit: float
    comment: str


class TickInfo(BaseModel):
    symbol: str
    bid: float
    ask: float
    last: float
    spread_points: float  # raw ask-bid difference in points
    time: int             # UTC unix timestamp
