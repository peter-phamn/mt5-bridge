from __future__ import annotations

import asyncio
import logging

from app.config import TIMEFRAME_MAP
from app.streaming.publisher import publish_bar

try:
    import MetaTrader5 as mt5  # type: ignore
    MT5_AVAILABLE = True
except ImportError:
    mt5 = None
    MT5_AVAILABLE = False

logger = logging.getLogger(__name__)

# Adaptive poll interval per timeframe — no need to poll M5 every 100 ms
_POLL_INTERVALS: dict[str, float] = {
    "M1":  1.0,
    "M5":  2.0,
    "M15": 5.0,
    "H1":  10.0,
    "H4":  30.0,
    "D1":  60.0,
}


async def run(
    symbol: str,
    timeframe: str,
    poll_interval: float | None = None,
) -> None:
    """Detect bar closes and publish closed bars to Redis.

    Design rules
    ------------
    - Fetch 3 bars via copy_rates_from_pos(symbol, tf, 0, 3)
    - rates[-1] = current FORMING bar  → NEVER published (no lookahead bias)
    - rates[-2] = last CLOSED bar      → published when its timestamp changes
    - No partial bars, no lookahead, strict alignment with MT5 server time

    Parameters
    ----------
    symbol        : MT5 symbol name, case-sensitive (e.g. "XAUUSDm")
    timeframe     : timeframe string, e.g. "M5", "H1"
    poll_interval : override default poll rate (seconds)
    """
    tf_int   = TIMEFRAME_MAP[timeframe.upper()]
    interval = poll_interval or _POLL_INTERVALS.get(timeframe.upper(), 2.0)
    last_bar_time: int | None = None
    consecutive_errors: int = 0

    logger.info(
        "Bar engine starting: symbol=%s tf=%s poll=%.1fs",
        symbol, timeframe, interval,
    )

    while True:
        try:
            if not MT5_AVAILABLE:
                await asyncio.sleep(interval)
                continue

            rates = mt5.copy_rates_from_pos(symbol, tf_int, 0, 3)

            if rates is None or len(rates) < 2:
                consecutive_errors += 1
                if consecutive_errors == 1 or consecutive_errors % 10 == 0:
                    code, msg = mt5.last_error()
                    logger.warning(
                        "bar_engine[%s/%s] no rates — code=%d msg=%r (attempt %d)",
                        symbol, timeframe, code, msg, consecutive_errors,
                    )
                await asyncio.sleep(interval)
                continue

            consecutive_errors = 0

            # rates[-2] is always the last FULLY CLOSED bar
            closed = rates[-2]
            bar_time = int(closed["time"])

            if bar_time != last_bar_time:
                last_bar_time = bar_time
                payload = {
                    "time":        bar_time,
                    "open":        float(closed["open"]),
                    "high":        float(closed["high"]),
                    "low":         float(closed["low"]),
                    "close":       float(closed["close"]),
                    "tick_volume": int(closed["tick_volume"]),
                }
                await publish_bar(symbol, timeframe, payload)
                logger.info(
                    "BAR CLOSE [%s/%s] time=%d close=%.5f vol=%d",
                    symbol, timeframe,
                    bar_time, payload["close"], payload["tick_volume"],
                )

        except asyncio.CancelledError:
            logger.info("bar_engine[%s/%s] stopped", symbol, timeframe)
            return
        except Exception as exc:
            logger.error("bar_engine[%s/%s] unexpected error: %s", symbol, timeframe, exc)
            await asyncio.sleep(interval)
            continue

        await asyncio.sleep(interval)
