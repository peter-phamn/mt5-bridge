from __future__ import annotations

import asyncio
import logging

from app.streaming.publisher import publish_tick

try:
    import MetaTrader5 as mt5  # type: ignore
    MT5_AVAILABLE = True
except ImportError:
    mt5 = None
    MT5_AVAILABLE = False

logger = logging.getLogger(__name__)


async def run(symbol: str, interval: float = 0.1) -> None:
    """Poll MT5 for ticks and publish to Redis.

    Guarantees
    ----------
    - Deduplication: skips ticks with the same or older time_msc
    - Monotonicity:  drops backward jumps > 1 second (clock skew guard)
    - Reconnect:     backs off on consecutive MT5 failures

    Parameters
    ----------
    symbol   : MT5 symbol name, case-sensitive (e.g. "XAUUSDm")
    interval : poll interval in seconds (default 0.1 = 100 ms)
    """
    last_time_msc: int = 0
    consecutive_errors: int = 0

    logger.info("Tick engine starting: symbol=%s interval=%.0fms", symbol, interval * 1000)

    while True:
        try:
            if not MT5_AVAILABLE:
                await asyncio.sleep(1.0)
                continue

            tick = mt5.symbol_info_tick(symbol)

            if tick is None:
                consecutive_errors += 1
                # Log only on first and every 10th failure to avoid spam
                if consecutive_errors == 1 or consecutive_errors % 10 == 0:
                    code, msg = mt5.last_error()
                    logger.warning(
                        "tick_engine[%s] no tick — code=%d msg=%r (attempt %d)",
                        symbol, code, msg, consecutive_errors,
                    )
                await asyncio.sleep(interval * 5)
                continue

            # Dedup: same or older timestamp → nothing new
            if tick.time_msc <= last_time_msc:
                await asyncio.sleep(interval)
                continue

            # Monotonicity guard: reject backward jumps > 1 second
            if last_time_msc > 0 and tick.time_msc < last_time_msc - 1_000:
                logger.warning(
                    "tick_engine[%s] non-monotonic tick dropped: %d < %d",
                    symbol, tick.time_msc, last_time_msc,
                )
                await asyncio.sleep(interval)
                continue

            consecutive_errors = 0
            last_time_msc = tick.time_msc

            await publish_tick(symbol, {
                "time":   tick.time_msc,
                "bid":    tick.bid,
                "ask":    tick.ask,
                "spread": round(tick.ask - tick.bid, 5),
            })

        except asyncio.CancelledError:
            logger.info("tick_engine[%s] stopped", symbol)
            return
        except Exception as exc:
            logger.error("tick_engine[%s] unexpected error: %s", symbol, exc)
            await asyncio.sleep(1.0)

        await asyncio.sleep(interval)
