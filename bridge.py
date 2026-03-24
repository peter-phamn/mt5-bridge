"""MT5 Bridge — real-time data streaming + order execution.

Run
---
    python bridge.py

Environment
-----------
    Configure via .env (same as the REST service):
    MT5_LOGIN, MT5_PASSWORD, MT5_SERVER, REDIS_URL, ...

Architecture
------------
    For each symbol:
      - tick_engine    → stream:ticks:{symbol}  + pubsub:ticks:{symbol}
      - bar_engine x3  → stream:bars:{symbol}:{M5|M15|H1}

    One shared:
      - order_engine   → consumes stream:orders → publishes stream:fills
      - metrics loop   → flushes spread/slippage stats to Redis hashes
"""
from __future__ import annotations

import asyncio
import logging
import signal

from app.config import settings
from app.execution.order_engine import run as run_order_engine
from app.execution.slippage_monitor import SpreadMonitor, metrics_flush_loop
from app.mt5_client import mt5_client
from app.redis_client import redis_client
from app.streaming import bar_engine, tick_engine

logger = logging.getLogger(__name__)

SYMBOLS    = settings.default_symbols   # e.g. ["XAUUSDm"]
TIMEFRAMES = ["M5", "M15", "H1"]


async def main() -> None:
    # ── Connect MT5 ───────────────────────────────────────────────
    mt5_client.reconnect(
        max_attempts=settings.max_retries,
        base_delay=settings.retry_delay_seconds,
    )

    # ── Connect Redis ─────────────────────────────────────────────
    await redis_client.connect()

    # ── Build task list ───────────────────────────────────────────
    tasks: list[asyncio.Task] = []
    spread_monitors: list[SpreadMonitor] = []

    for symbol in SYMBOLS:
        # Tick stream (100 ms)
        tasks.append(asyncio.create_task(
            tick_engine.run(symbol, interval=0.1),
            name=f"tick:{symbol}",
        ))

        # Bar streams
        for tf in TIMEFRAMES:
            tasks.append(asyncio.create_task(
                bar_engine.run(symbol, tf),
                name=f"bar:{symbol}:{tf}",
            ))

        spread_monitors.append(SpreadMonitor(symbol))

    # Order execution engine
    tasks.append(asyncio.create_task(
        run_order_engine(),
        name="order-engine",
    ))

    # Metrics flush (every 10 s)
    tasks.append(asyncio.create_task(
        metrics_flush_loop(spread_monitors, interval=10.0),
        name="metrics-flush",
    ))

    logger.info(
        "Bridge running — %d symbols, %d timeframes, %d tasks total",
        len(SYMBOLS), len(TIMEFRAMES), len(tasks),
    )

    # ── Graceful shutdown ─────────────────────────────────────────
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _handle_signal(sig: signal.Signals) -> None:
        logger.info("Signal %s received — shutting down…", sig.name)
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal, sig)

    await stop_event.wait()

    logger.info("Cancelling %d tasks…", len(tasks))
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    await redis_client.close()
    mt5_client.shutdown()
    logger.info("Bridge stopped cleanly.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    asyncio.run(main())
