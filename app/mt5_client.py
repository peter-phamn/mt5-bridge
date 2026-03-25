from __future__ import annotations

import logging
import random
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd

from app.config import TIMEFRAME_MAP, settings

logger = logging.getLogger(__name__)

# MetaTrader5 is Windows-only; import conditionally so the service can run
# in mock/offline mode on Linux/macOS for testing.
try:
    import MetaTrader5 as mt5  # type: ignore

    MT5_AVAILABLE = True
except ImportError:
    mt5 = None  # type: ignore
    MT5_AVAILABLE = False
    logger.warning("MetaTrader5 package not available — running in offline mode.")


class MT5Error(Exception):
    pass


class MT5ConnectionError(MT5Error):
    """Raised when the terminal cannot be reached after all reconnect attempts."""


class MT5DataError(MT5Error):
    """Raised when the terminal is connected but returns no/invalid data."""


class MT5Client:
    """Thread-safe wrapper around the MetaTrader5 package.

    Retry / reconnect strategy
    --------------------------
    - ``initialize()``        — one raw attempt; call ``reconnect()`` for retried version.
    - ``reconnect()``         — retries ``initialize()`` with exponential back-off + jitter.
    - ``ensure_connected()``  — calls ``reconnect()`` only when the terminal is not live.
    - ``fetch_with_retry()``  — outer retry loop; on each failure it drops the connection
                                so the next iteration triggers a full reconnect cycle.
    """

    def __init__(self) -> None:
        self._initialized = False

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def initialize(self) -> bool:
        """Single attempt to open the MT5 connection. Returns True on success."""
        if not MT5_AVAILABLE:
            logger.warning("MT5 package unavailable (non-Windows environment).")
            return False

        kwargs: dict = {}
        if settings.mt5_login:
            kwargs["login"] = settings.mt5_login
        if settings.mt5_password:
            kwargs["password"] = settings.mt5_password
        if settings.mt5_server:
            kwargs["server"] = settings.mt5_server
        if settings.mt5_path:
            kwargs["path"] = settings.mt5_path

        ok = mt5.initialize(**kwargs) if kwargs else mt5.initialize()
        if not ok:
            code, msg = mt5.last_error()
            logger.error("MT5 init failed — code=%d msg=%r", code, msg)
            self._initialized = False
            return False

        info = mt5.terminal_info()
        version = mt5.version()
        logger.info(
            "MT5 connected — build=%s version=%s path=%r",
            info.build if info else "?",
            ".".join(str(x) for x in version) if version else "?",
            info.path if info else "?",
        )
        self._initialized = True
        return True

    def reconnect(self, max_attempts: int = 3, base_delay: float = 2.0) -> None:
        """Retry ``initialize()`` with exponential back-off + jitter.

        Raises ``MT5ConnectionError`` if all attempts fail.
        """
        for attempt in range(1, max_attempts + 1):
            logger.info(
                "Reconnect attempt %d/%d…", attempt, max_attempts
            )
            # Shut down cleanly before re-initialising
            if MT5_AVAILABLE and self._initialized:
                mt5.shutdown()
                self._initialized = False

            if self.initialize():
                logger.info("Reconnect succeeded on attempt %d.", attempt)
                return

            if attempt < max_attempts:
                delay = _backoff(attempt, base_delay)
                logger.warning(
                    "Reconnect attempt %d failed — retrying in %.1fs…",
                    attempt,
                    delay,
                )
                time.sleep(delay)

        raise MT5ConnectionError(
            f"Cannot connect to MetaTrader 5 after {max_attempts} attempts."
        )

    def shutdown(self) -> None:
        if MT5_AVAILABLE and self._initialized:
            mt5.shutdown()
            self._initialized = False
            logger.info("MT5 connection closed.")

    @property
    def is_connected(self) -> bool:
        if not MT5_AVAILABLE or not self._initialized:
            return False
        info = mt5.terminal_info()
        return info is not None and info.connected

    def ensure_connected(self) -> None:
        """Guarantee a live connection; reconnects (with retry) if needed."""
        if not self.is_connected:
            logger.info("MT5 not connected — initiating reconnect…")
            self.reconnect(
                max_attempts=settings.max_retries,
                base_delay=settings.retry_delay_seconds,
            )

    def get_version(self) -> Optional[str]:
        if not MT5_AVAILABLE or not self._initialized:
            return None
        v = mt5.version()
        return ".".join(str(x) for x in v) if v else None

    # ------------------------------------------------------------------
    # Symbol helpers
    # ------------------------------------------------------------------

    def get_symbols(self) -> list[str]:
        self.ensure_connected()
        symbols = mt5.symbols_get()
        if symbols is None:
            code, msg = mt5.last_error()
            logger.error("symbols_get() returned None — code=%d msg=%r", code, msg)
            return []
        return [s.name for s in symbols if s.visible]

    def get_symbol_info(self, symbol: str):
        self.ensure_connected()
        info = mt5.symbol_info(symbol)
        if info is None:
            code, msg = mt5.last_error()
            logger.warning(
                "symbol_info(%r) returned None — code=%d msg=%r", symbol, code, msg
            )
        return info

    # ------------------------------------------------------------------
    # Data fetching
    # ------------------------------------------------------------------

    def _ensure_symbol_selected(self, symbol: str) -> None:
        """Make symbol visible in Market Watch so MT5 will serve its data.

        MT5 returns code=-2 (Invalid params) for symbols that are not selected
        in Market Watch, even if they are valid.  ``symbol_select`` is a no-op
        when the symbol is already visible.
        """
        if not mt5.symbol_select(symbol, True):
            code, msg = mt5.last_error()
            logger.warning(
                "symbol_select(%r) failed — code=%d msg=%r. "
                "Data request may still succeed if symbol is already visible.",
                symbol, code, msg,
            )

    def copy_rates_range(
        self,
        symbol: str,
        timeframe_str: str,
        from_dt: datetime,
        to_dt: datetime,
    ) -> pd.DataFrame:
        self.ensure_connected()
        self._ensure_symbol_selected(symbol)
        tf = self._resolve_timeframe(timeframe_str)
        from_utc = _to_utc(from_dt)
        to_utc   = _to_utc(to_dt)

        logger.debug(
            "copy_rates_range params → symbol=%r tf_int=%d from=%r to=%r",
            symbol, tf, from_utc, to_utc,
        )

        t0 = time.perf_counter()
        rates = mt5.copy_rates_range(symbol, tf, from_utc, to_utc)
        elapsed = time.perf_counter() - t0

        df = self._rates_to_df(rates, symbol, timeframe_str)
        logger.info(
            "copy_rates_range(%s, %s) → %d rows in %.3fs",
            symbol, timeframe_str, len(df), elapsed,
        )
        return df

    def copy_rates_from_pos(
        self,
        symbol: str,
        timeframe_str: str,
        start_pos: int,
        count: int,
    ) -> pd.DataFrame:
        self.ensure_connected()
        tf = self._resolve_timeframe(timeframe_str)

        t0 = time.perf_counter()
        rates = mt5.copy_rates_from_pos(symbol, tf, start_pos, count)
        elapsed = time.perf_counter() - t0

        df = self._rates_to_df(rates, symbol, timeframe_str)
        logger.debug(
            "copy_rates_from_pos(%s, %s, pos=%d, count=%d) → %d rows in %.3fs",
            symbol, timeframe_str, start_pos, count, len(df), elapsed,
        )
        return df

    # ------------------------------------------------------------------
    # Trading execution
    # ------------------------------------------------------------------

    def get_account_info(self) -> dict:
        """Return account balance, equity, margin, currency."""
        self.ensure_connected()
        info = mt5.account_info()
        if info is None:
            code, msg = mt5.last_error()
            raise MT5DataError(f"account_info() returned None — code={code} msg={msg}")
        return {
            "balance":     info.balance,
            "equity":      info.equity,
            "margin":      info.margin,
            "margin_free": info.margin_free,
            "currency":    info.currency,
            "login":       info.login,
            "server":      info.server,
        }

    def get_positions(self, magic: int | None = None) -> list[dict]:
        """Return open positions, optionally filtered by magic number."""
        self.ensure_connected()
        positions = mt5.positions_get()
        if positions is None:
            return []
        result = []
        for p in positions:
            if magic is not None and p.magic != magic:
                continue
            result.append({
                "ticket":        p.ticket,
                "symbol":        p.symbol,
                "type":          "buy" if p.type == 0 else "sell",
                "volume":        p.volume,
                "price_open":    p.price_open,
                "sl":            p.sl,
                "tp":            p.tp,
                "price_current": p.price_current,
                "profit":        p.profit,
                "swap":          getattr(p, "swap", 0.0),
                "comment":       p.comment,
                "magic":         p.magic,
                "time":          int(p.time),
            })
        return result

    def get_tick(self, symbol: str) -> dict:
        """Return current bid/ask/last for a symbol."""
        self.ensure_connected()
        self._ensure_symbol_selected(symbol)
        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            code, msg = mt5.last_error()
            raise MT5DataError(f"symbol_info_tick({symbol!r}) returned None — code={code} msg={msg}")
        return {
            "symbol":        symbol,
            "bid":           tick.bid,
            "ask":           tick.ask,
            "last":          tick.last,
            "spread_points": round(tick.ask - tick.bid, 5),
            "time":          int(tick.time),
        }

    def place_order(
        self,
        symbol: str,
        side: str,
        volume: float,
        sl: float,
        tp: float,
        magic: int = 0,
        comment: str = "bridge",
    ) -> dict:
        """Place a market order. side: 'BUY' | 'SELL'.

        Returns OrderResult-compatible dict including latency_ms for execution
        quality monitoring.
        """
        self.ensure_connected()
        request = {
            "action":       mt5.TRADE_ACTION_DEAL,
            "symbol":       symbol,
            "volume":       volume,
            "type":         mt5.ORDER_TYPE_BUY if side.upper() == "BUY" else mt5.ORDER_TYPE_SELL,
            "sl":           sl,
            "tp":           tp,
            "magic":        magic,
            "type_filling": mt5.ORDER_FILLING_IOC,
            "comment":      comment,
        }
        logger.info(
            "place_order: symbol=%s side=%s volume=%.2f sl=%.5f tp=%.5f magic=%d",
            symbol, side, volume, sl, tp, magic,
        )
        t0 = time.perf_counter()
        result = mt5.order_send(request)
        latency_ms = round((time.perf_counter() - t0) * 1000, 1)

        if result is None:
            code, msg = mt5.last_error()
            raise MT5Error(f"order_send returned None — code={code} msg={msg}")
        return {
            "retcode":    result.retcode,
            "ticket":     result.order,
            "fill_price": result.price,
            "volume":     result.volume,
            "comment":    result.comment,
            "done":       result.retcode == mt5.TRADE_RETCODE_DONE,
            "latency_ms": latency_ms,
        }

    def modify_position(self, ticket: int, sl: float, tp: float) -> dict:
        """Update SL/TP of an open position."""
        self.ensure_connected()
        result = mt5.order_send({
            "action":   mt5.TRADE_ACTION_SLTP,
            "position": ticket,
            "sl":       sl,
            "tp":       tp,
        })
        if result is None:
            code, msg = mt5.last_error()
            raise MT5Error(f"modify_position({ticket}) returned None — code={code} msg={msg}")
        return {
            "retcode": result.retcode,
            "comment": result.comment,
            "done":    result.retcode == mt5.TRADE_RETCODE_DONE,
        }

    def close_position(self, ticket: int, lot: float | None = None) -> dict:
        """Close a position by ticket. lot=None closes the full volume."""
        self.ensure_connected()
        positions = mt5.positions_get(ticket=ticket)
        if not positions:
            raise MT5DataError(f"Position {ticket} not found")
        pos = positions[0]
        close_volume = round(lot, 2) if lot else pos.volume
        result = mt5.order_send({
            "action":       mt5.TRADE_ACTION_DEAL,
            "position":     ticket,
            "symbol":       pos.symbol,
            "volume":       close_volume,
            "type":         mt5.ORDER_TYPE_SELL if pos.type == 0 else mt5.ORDER_TYPE_BUY,
            "type_filling": mt5.ORDER_FILLING_IOC,
            "comment":      f"close:{ticket}",
        })
        if result is None:
            code, msg = mt5.last_error()
            raise MT5Error(f"close_position({ticket}) returned None — code={code} msg={msg}")
        return {
            "retcode":    result.retcode,
            "ticket":     result.order,
            "fill_price": result.price,
            "volume":     result.volume,
            "comment":    result.comment,
            "done":       result.retcode == mt5.TRADE_RETCODE_DONE,
        }

    def get_deal_history(self, ticket: int) -> list[dict]:
        """Return all deals associated with a position ticket.

        Includes commission and swap for accurate net P&L calculation.
        """
        self.ensure_connected()
        deals = mt5.history_deals_get(position=ticket)
        if deals is None:
            return []
        return [
            {
                "deal":       d.ticket,
                "ticket":     d.position_id,
                "time":       int(d.time),
                "type":       d.type,
                "entry":      d.entry,   # 0=IN 1=OUT
                "volume":     d.volume,
                "price":      d.price,
                "profit":     d.profit,
                "commission": getattr(d, "commission", 0.0),
                "swap":       getattr(d, "swap", 0.0),
                "comment":    d.comment,
            }
            for d in deals
        ]

    # ------------------------------------------------------------------
    # Large-range fetch helpers
    # ------------------------------------------------------------------

    def fetch_with_retry(
        self,
        symbol: str,
        timeframe_str: str,
        from_dt: datetime,
        to_dt: datetime,
        retries: int | None = None,
        max_bars_per_chunk: int = 50_000,
    ) -> pd.DataFrame:
        """Fetch OHLC data with chunking for large date ranges.

        MT5 enforces a per-request bar limit (~99,999).  When the requested
        range exceeds ``max_bars_per_chunk`` bars, the range is split into
        smaller chunks that each stay within the limit.  Results are
        concatenated and deduplicated by time.

        Connection errors trigger a full ``reconnect()`` before the next attempt.
        Data errors (e.g. empty response) are retried without reconnecting.
        ``MT5ConnectionError`` is re-raised immediately.
        """
        bar_sec = _TF_SECONDS.get(timeframe_str.upper(), 300)
        span_sec = (_to_utc(to_dt) - _to_utc(from_dt)).total_seconds()
        estimated_bars = int(span_sec / bar_sec)

        if estimated_bars <= max_bars_per_chunk:
            return self._fetch_chunk(symbol, timeframe_str, from_dt, to_dt, retries)

        # Split into chunks of at most max_bars_per_chunk bars
        chunk_delta = timedelta(seconds=bar_sec * max_bars_per_chunk)
        chunks: list[pd.DataFrame] = []
        chunk_start = _to_utc(from_dt)
        final_end   = _to_utc(to_dt)
        total_chunks = -(-estimated_bars // max_bars_per_chunk)  # ceiling division

        logger.info(
            "[%s/%s] Large range (~%d bars) — splitting into %d chunks of ≤%d bars",
            symbol, timeframe_str, estimated_bars, total_chunks, max_bars_per_chunk,
        )

        chunk_idx = 0
        while chunk_start < final_end:
            chunk_idx += 1
            chunk_end = min(chunk_start + chunk_delta, final_end)
            logger.info(
                "[%s/%s] Chunk %d/%d: %s → %s",
                symbol, timeframe_str, chunk_idx, total_chunks,
                chunk_start.date(), chunk_end.date(),
            )
            try:
                df_chunk = self._fetch_chunk(
                    symbol, timeframe_str,
                    chunk_start, chunk_end,
                    retries,
                )
                chunks.append(df_chunk)
            except MT5DataError as exc:
                # Some chunks may legitimately have no data (e.g. gaps)
                logger.warning(
                    "[%s/%s] Chunk %d/%d returned no data: %s",
                    symbol, timeframe_str, chunk_idx, total_chunks, exc,
                )
            chunk_start = chunk_end

        if not chunks:
            raise MT5DataError(
                f"[{symbol}/{timeframe_str}] All chunks returned no data."
            )

        combined = pd.concat(chunks, ignore_index=True)
        combined = combined.drop_duplicates(subset="time").sort_values("time").reset_index(drop=True)
        logger.info(
            "[%s/%s] Combined %d chunks → %d rows total",
            symbol, timeframe_str, len(chunks), len(combined),
        )
        return combined

    def _fetch_chunk(
        self,
        symbol: str,
        timeframe_str: str,
        from_dt: datetime,
        to_dt: datetime,
        retries: int | None = None,
    ) -> pd.DataFrame:
        """Single date-range fetch with retry logic (no chunking)."""
        retries = retries if retries is not None else settings.max_retries
        last_exc: Exception = MT5Error("Unknown error")

        for attempt in range(1, retries + 1):
            try:
                self.ensure_connected()
                df = self.copy_rates_range(symbol, timeframe_str, from_dt, to_dt)
                logger.info(
                    "[%s/%s] Fetched %d rows (%s → %s) on attempt %d",
                    symbol, timeframe_str, len(df),
                    _to_utc(from_dt).date(), _to_utc(to_dt).date(), attempt,
                )
                return df

            except MT5ConnectionError:
                raise

            except MT5DataError as exc:
                last_exc = exc
                logger.warning(
                    "[%s/%s] Data error on attempt %d/%d: %s",
                    symbol, timeframe_str, attempt, retries, exc,
                )

            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "[%s/%s] Unexpected error on attempt %d/%d: %s",
                    symbol, timeframe_str, attempt, retries, exc,
                )
                self._initialized = False  # force reconnect on next iteration

            if attempt < retries:
                delay = _backoff(attempt, settings.retry_delay_seconds)
                logger.info(
                    "[%s/%s] Waiting %.1fs before attempt %d/%d…",
                    symbol, timeframe_str, delay, attempt + 1, retries,
                )
                time.sleep(delay)

        raise MT5Error(
            f"[{symbol}/{timeframe_str}] All {retries} fetch attempts failed: {last_exc}"
        ) from last_exc

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_timeframe(tf_str: str) -> int:
        tf_str = tf_str.upper()
        if tf_str not in TIMEFRAME_MAP:
            raise MT5Error(f"Unknown timeframe: '{tf_str}'")
        return TIMEFRAME_MAP[tf_str]

    @staticmethod
    def _rates_to_df(rates, symbol: str, timeframe_str: str) -> pd.DataFrame:
        if rates is None or len(rates) == 0:
            code, msg = mt5.last_error() if MT5_AVAILABLE else (-1, "no data")
            raise MT5DataError(
                f"No data for {symbol}/{timeframe_str} — MT5 error code={code} msg={msg!r}"
            )

        df = pd.DataFrame(rates)
        df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
        df = df.sort_values("time").reset_index(drop=True)

        keep = ["time", "open", "high", "low", "close", "tick_volume"]
        for extra in ("spread", "real_volume"):
            if extra in df.columns:
                keep.append(extra)
        return df[keep]


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------

# Timeframe → bar duration in seconds (used to estimate bar counts for chunking)
_TF_SECONDS: dict[str, int] = {
    "M1": 60,    "M2": 120,   "M3": 180,   "M4": 240,   "M5": 300,
    "M6": 360,   "M10": 600,  "M12": 720,  "M15": 900,  "M20": 1_200,
    "M30": 1_800, "H1": 3_600, "H2": 7_200, "H3": 10_800, "H4": 14_400,
    "H6": 21_600, "H8": 28_800, "H12": 43_200, "D1": 86_400,
    "W1": 604_800, "MN1": 2_592_000,
}


def _backoff(attempt: int, base: float, max_delay: float = 60.0) -> float:
    """Exponential back-off with ±20 % jitter, capped at *max_delay* seconds."""
    delay = min(base * (2 ** (attempt - 1)), max_delay)
    jitter = delay * 0.2 * (random.random() * 2 - 1)  # ±20 %
    return max(0.1, delay + jitter)


@contextmanager
def mt5_session(client: MT5Client):
    """Context manager that guarantees a live connection for the block."""
    client.ensure_connected()
    try:
        yield client
    finally:
        pass  # keep the connection alive across requests


def _to_utc(dt: datetime) -> datetime:
    """Convert to naive UTC datetime for MT5 API.

    MT5's copy_rates_range / copy_rates_from require naive datetime objects
    and always interpret them as UTC.  Passing timezone-aware datetimes
    causes error code -2 ('Terminal: Invalid params').
    """
    if dt.tzinfo is None:
        return dt  # already naive — MT5 treats it as UTC
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


# Singleton
mt5_client = MT5Client()
