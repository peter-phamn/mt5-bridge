from __future__ import annotations

import json
import logging

from app.config import settings
from app.redis_client import redis_client

logger = logging.getLogger(__name__)


async def publish_tick(symbol: str, payload: dict) -> None:
    """Publish a tick to both the persistent stream and the low-latency pub/sub channel.

    stream:ticks:{symbol}     — persistent, replayable, consumer-group compatible
    pubsub:ticks:{symbol}     — fire-and-forget, ultra-low latency
    """
    stream  = f"stream:ticks:{symbol}"
    channel = f"pubsub:ticks:{symbol}"
    fields  = {k: str(v) for k, v in payload.items()}

    await redis_client.xadd(stream, fields, maxlen=settings.redis_tick_maxlen)
    await redis_client.publish(channel, json.dumps(payload))


async def publish_bar(symbol: str, timeframe: str, payload: dict) -> None:
    """Publish a closed bar to the persistent bar stream.

    stream:bars:{symbol}:{timeframe}
    """
    stream = f"stream:bars:{symbol}:{timeframe}"
    fields = {k: str(v) for k, v in payload.items()}
    await redis_client.xadd(stream, fields, maxlen=settings.redis_bar_maxlen)
