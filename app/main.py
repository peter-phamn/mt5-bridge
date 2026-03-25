from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse

from app.config import settings
from app.data_service import data_service
from app.mt5_client import MT5Error, mt5_client
from app.schemas import (
    AccountInfo,
    CloseRequest,
    DealInfo,
    DownloadRequest,
    DownloadResponse,
    HealthResponse,
    ModifyRequest,
    MultiDownloadRequest,
    OrderRequest,
    OrderResult,
    PositionInfo,
    SymbolInfo,
    TickInfo,
)
from app.storage import storage
from app.utils import setup_logging

setup_logging("DEBUG" if settings.debug else "INFO")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting MT5 Bridge…")
    mt5_client.initialize()
    yield
    logger.info("Shutting down MT5 Bridge…")
    mt5_client.shutdown()


app = FastAPI(
    title="MT5 Bridge",
    description="Historical OHLC data + live trading API backed by MetaTrader 5.",
    version="2.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@app.get("/health", response_model=HealthResponse, tags=["System"])
def health():
    connected = mt5_client.is_connected
    version = mt5_client.get_version() if connected else None
    return HealthResponse(
        status="ok" if connected else "degraded",
        mt5_connected=connected,
        mt5_version=version,
        available_symbols_cached=len(storage.list_symbols()),
        storage_path=str(settings.data_path),
    )


# ---------------------------------------------------------------------------
# Symbols
# ---------------------------------------------------------------------------


@app.get("/symbols", response_model=List[str], tags=["Data"])
def list_symbols(source: str = Query("local", enum=["local", "mt5"])):
    """
    - **local**: symbols that have data stored on disk
    - **mt5**: all symbols available from the connected MT5 terminal
    """
    if source == "mt5":
        try:
            return mt5_client.get_symbols()
        except MT5Error as exc:
            raise HTTPException(status_code=503, detail=str(exc))
    return storage.list_symbols()


@app.get("/symbols/{symbol}", response_model=SymbolInfo, tags=["Data"])
def symbol_info(symbol: str):
    try:
        info = mt5_client.get_symbol_info(symbol)
    except MT5Error as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    if info is None:
        raise HTTPException(status_code=404, detail=f"Symbol '{symbol}' not found.")
    return SymbolInfo(
        name=info.name,
        description=info.description,
        currency_base=info.currency_base,
        currency_profit=info.currency_profit,
        digits=info.digits,
        trade_contract_size=info.trade_contract_size,
    )


# ---------------------------------------------------------------------------
# Download (fetch from MT5, write to Parquet)
# ---------------------------------------------------------------------------


@app.post("/download", response_model=DownloadResponse, tags=["Data"])
def download(req: DownloadRequest):
    try:
        result = data_service.download(
            symbol=req.symbol,
            timeframe=req.timeframe,
            from_date=req.from_date,
            to_date=req.to_date,
            force=req.force,
        )
    except MT5Error as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    return DownloadResponse(
        symbol=req.symbol,
        timeframe=req.timeframe,
        rows_downloaded=result["rows_downloaded"],
        rows_new=result["rows_new"],
        from_date=result["from_date"],
        to_date=result["to_date"],
        duration_seconds=result["duration_seconds"],
    )


@app.post("/download/multi", tags=["Data"])
async def download_multi(req: MultiDownloadRequest):
    """Download multiple symbols concurrently."""
    results = await data_service.download_multi(
        symbols=req.symbols,
        timeframe=req.timeframe,
        from_date=req.from_date,
        to_date=req.to_date,
    )
    return {"results": results}


# ---------------------------------------------------------------------------
# History query
# ---------------------------------------------------------------------------


@app.get("/history", tags=["Data"])
def history(
    symbol: str = Query(...),
    timeframe: str = Query(...),
    from_: datetime = Query(..., alias="from"),
    to: Optional[datetime] = Query(None),
):
    """Query stored OHLC+spread data by date range. Returns {symbol, timeframe, count, data[]}."""
    if to is None:
        to = datetime.now(timezone.utc)
    try:
        df = data_service.get_history(
            symbol=symbol,
            timeframe=timeframe,
            from_dt=from_,
            to_dt=to,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    records = df.to_dict(orient="records")
    return {"symbol": symbol, "timeframe": timeframe, "count": len(records), "data": records}


# ---------------------------------------------------------------------------
# Replay (SSE stream for backtesting)
# ---------------------------------------------------------------------------


@app.get("/replay", tags=["Backtest"])
async def replay(
    symbol: str = Query(...),
    timeframe: str = Query(...),
    from_: datetime = Query(..., alias="from"),
    to: Optional[datetime] = Query(None),
    speed: float = Query(0.0, description="Seconds between bars. 0=instant."),
):
    """Server-Sent Events stream that replays stored bars one by one."""
    if to is None:
        to = datetime.now(timezone.utc)

    async def event_stream():
        async for bar in data_service._replay_gen(symbol, timeframe, from_, to, speed):
            yield f"data: {bar}\n\n"
        yield "data: {\"done\": true}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


# ---------------------------------------------------------------------------
# DuckDB query endpoint
# ---------------------------------------------------------------------------


@app.post("/query", tags=["Analytics"])
def duckdb_query(
    symbol: str = Query(...),
    timeframe: str = Query(...),
    from_: Optional[datetime] = Query(None, alias="from"),
    to: Optional[datetime] = Query(None),
    extra_sql: str = Query("", description="Extra SQL appended after WHERE clause"),
):
    """Execute a DuckDB query against stored Parquet files."""
    if not settings.duckdb_enabled:
        raise HTTPException(status_code=400, detail="DuckDB is disabled.")
    try:
        df = data_service.query_symbol_duckdb(symbol, timeframe, from_, to, extra_sql)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return JSONResponse(content=df.to_dict(orient="records"))


# ---------------------------------------------------------------------------
# Trading — account info, positions, order execution
# ---------------------------------------------------------------------------


@app.get("/account", response_model=AccountInfo, tags=["Trading"])
def get_account():
    """Return current account balance, equity, margin."""
    try:
        return mt5_client.get_account_info()
    except MT5Error as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.get("/positions", response_model=List[PositionInfo], tags=["Trading"])
def get_positions(
    magic: Optional[int] = Query(None, description="Filter by magic number (e.g. 20260101)"),
):
    """Return open positions. Filter by magic to see only bot orders."""
    try:
        return mt5_client.get_positions(magic=magic)
    except MT5Error as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.get("/tick", response_model=TickInfo, tags=["Trading"])
def get_tick(symbol: str = Query(...)):
    """Return current bid/ask/last tick for a symbol."""
    try:
        return mt5_client.get_tick(symbol)
    except MT5Error as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.post("/order", response_model=OrderResult, tags=["Trading"])
def place_order(req: OrderRequest):
    """Place a market order. Uses settings.trade_magic when req.magic == 0."""
    magic = req.magic if req.magic != 0 else settings.trade_magic
    try:
        return mt5_client.place_order(
            symbol=req.symbol,
            side=req.side,
            volume=req.volume,
            sl=req.sl,
            tp=req.tp,
            magic=magic,
            comment=req.comment,
        )
    except MT5Error as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.post("/modify/{ticket}", tags=["Trading"])
def modify_position(ticket: int, req: ModifyRequest):
    """Update SL/TP of an open position (used for breakeven moves)."""
    try:
        return mt5_client.modify_position(ticket, req.sl, req.tp)
    except MT5Error as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.post("/close/{ticket}", tags=["Trading"])
def close_position(ticket: int, req: CloseRequest = CloseRequest()):
    """Close a position fully (lot=null) or partially (lot=0.01)."""
    try:
        return mt5_client.close_position(ticket, req.lot)
    except MT5Error as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.get("/history/{ticket}", response_model=List[DealInfo], tags=["Trading"])
def deal_history(ticket: int):
    """Return all deals (entry + exit) for a closed position ticket."""
    try:
        return mt5_client.get_deal_history(ticket)
    except MT5Error as exc:
        raise HTTPException(status_code=503, detail=str(exc))
