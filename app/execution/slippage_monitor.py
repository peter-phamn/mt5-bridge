from __future__ import annotations

import asyncio
import logging
from collections import deque

from app.redis_client import redis_client

logger = logging.getLogger(__name__)

_MAX_SAMPLES = 1_000


class SpreadMonitor:
    """Track spread statistics per symbol from the tick stream.

    Call record() on every tick, flush() periodically to persist to Redis.

    Redis key: metrics:spread:{symbol}
    Fields   : avg, max, samples
    """

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self._samples: deque[float] = deque(maxlen=_MAX_SAMPLES)
        self._max: float = 0.0

    def record(self, spread: float) -> None:
        self._samples.append(spread)
        if spread > self._max:
            self._max = spread

    async def flush(self) -> None:
        if not self._samples:
            return
        avg = sum(self._samples) / len(self._samples)
        await redis_client.hset(
            f"metrics:spread:{self.symbol}",
            {
                "avg":     round(avg, 5),
                "max":     round(self._max, 5),
                "samples": len(self._samples),
            },
        )


class SlippageMonitor:
    """Track slippage statistics per symbol from the fills stream.

    Call record() on every fill, flush() periodically to persist to Redis.

    Redis key: metrics:slippage:{symbol}
    Fields   : avg, max, samples
    """

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self._samples: deque[float] = deque(maxlen=_MAX_SAMPLES)
        self._max: float = 0.0

    def record(self, slippage: float) -> None:
        abs_slip = abs(slippage)
        self._samples.append(abs_slip)
        if abs_slip > self._max:
            self._max = abs_slip

    async def flush(self) -> None:
        if not self._samples:
            return
        avg = sum(self._samples) / len(self._samples)
        await redis_client.hset(
            f"metrics:slippage:{self.symbol}",
            {
                "avg":     round(avg, 5),
                "max":     round(self._max, 5),
                "samples": len(self._samples),
            },
        )


async def metrics_flush_loop(
    monitors: list[SpreadMonitor | SlippageMonitor],
    interval: float = 10.0,
) -> None:
    """Periodically flush all monitors to Redis."""
    logger.info("Metrics flush loop started (interval=%.0fs)", interval)
    while True:
        try:
            await asyncio.sleep(interval)
            for monitor in monitors:
                await monitor.flush()
        except asyncio.CancelledError:
            logger.info("Metrics flush loop stopped")
            return
        except Exception as exc:
            logger.error("metrics_flush_loop error: %s", exc)
