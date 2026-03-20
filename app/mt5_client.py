from __future__ import annotations

import logging
import random
import time
from contextlib import contextmanager
from datetime import datetime, timezone
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
        to_utc = _to_utc(to_dt)

        t0 = time.perf_counter()
        rates = mt5.copy_rates_range(symbol, tf, from_utc, to_utc)
        elapsed = time.perf_counter() - t0

        df = self._rates_to_df(rates, symbol, timeframe_str)
        logger.debug(
            "copy_rates_range(%s, %s, %s, %s) → %d rows in %.3fs",
            symbol, timeframe_str, from_utc.date(), to_utc.date(), len(df), elapsed,
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

    def fetch_with_retry(
        self,
        symbol: str,
        timeframe_str: str,
        from_dt: datetime,
        to_dt: datetime,
        retries: int | None = None,
    ) -> pd.DataFrame:
        """Fetch OHLC data, reconnecting and retrying on transient failures.

        Connection errors trigger a full ``reconnect()`` before the next attempt.
        Data errors (e.g. empty response) are retried without reconnecting.
        ``MT5ConnectionError`` is re-raised immediately — no point retrying
        if the terminal itself is unreachable.
        """
        retries = retries if retries is not None else settings.max_retries
        last_exc: Exception = MT5Error("Unknown error")

        for attempt in range(1, retries + 1):
            try:
                self.ensure_connected()
                df = self.copy_rates_range(symbol, timeframe_str, from_dt, to_dt)
                logger.info(
                    "[%s/%s] Fetched %d rows (%s → %s) on attempt %d",
                    symbol, timeframe_str, len(df),
                    from_dt.date(), to_dt.date(), attempt,
                )
                return df

            except MT5ConnectionError:
                # Terminal unreachable — no point burning remaining attempts
                raise

            except MT5DataError as exc:
                # Connected but no data — log and retry without reconnect
                last_exc = exc
                logger.warning(
                    "[%s/%s] Data error on attempt %d/%d: %s",
                    symbol, timeframe_str, attempt, retries, exc,
                )

            except Exception as exc:
                # Unexpected error — drop connection so next attempt reconnects
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
