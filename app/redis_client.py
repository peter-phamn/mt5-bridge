from __future__ import annotations

import logging
from typing import Any, Optional

from redis.asyncio import Redis
from redis.asyncio.connection import ConnectionPool
from redis.exceptions import ResponseError

from app.config import settings

logger = logging.getLogger(__name__)


class RedisClient:
    """Async Redis client with connection pooling and stream helpers.

    Usage
    -----
    await redis_client.connect()   # once at startup
    await redis_client.xadd(...)
    await redis_client.close()     # once at shutdown
    """

    def __init__(self) -> None:
        self._pool: Optional[ConnectionPool] = None
        self._client: Optional[Redis] = None

    async def connect(self) -> None:
        self._pool = ConnectionPool.from_url(
            settings.redis_url,
            max_connections=20,
            decode_responses=True,
        )
        self._client = Redis(connection_pool=self._pool)
        await self._client.ping()
        logger.info("Redis connected: %s", settings.redis_url)

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
        if self._pool:
            await self._pool.aclose()
        logger.info("Redis connection closed.")

    @property
    def client(self) -> Redis:
        if self._client is None:
            raise RuntimeError("Redis not connected — call connect() first.")
        return self._client

    # ------------------------------------------------------------------
    # Stream helpers
    # ------------------------------------------------------------------

    async def xadd(
        self,
        stream: str,
        fields: dict[str, Any],
        maxlen: int = 10_000,
    ) -> str:
        return await self.client.xadd(stream, fields, maxlen=maxlen, approximate=True)

    async def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[str, str],
        count: int = 10,
        block: int = 500,
    ) -> list:
        return await self.client.xreadgroup(
            group, consumer, streams, count=count, block=block
        )

    async def xack(self, stream: str, group: str, *ids: str) -> int:
        return await self.client.xack(stream, group, *ids)

    async def xgroup_create(
        self,
        stream: str,
        group: str,
        id: str = "0",
        mkstream: bool = True,
    ) -> None:
        """Create consumer group, ignoring BUSYGROUP if it already exists."""
        try:
            await self.client.xgroup_create(stream, group, id=id, mkstream=mkstream)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    # ------------------------------------------------------------------
    # Pub/Sub helpers
    # ------------------------------------------------------------------

    async def publish(self, channel: str, message: str) -> int:
        return await self.client.publish(channel, message)

    # ------------------------------------------------------------------
    # Hash helpers
    # ------------------------------------------------------------------

    async def hset(self, name: str, mapping: dict[str, Any]) -> int:
        return await self.client.hset(name, mapping=mapping)

    async def hgetall(self, name: str) -> dict[str, str]:
        return await self.client.hgetall(name)

    # ------------------------------------------------------------------
    # Set helpers (idempotency)
    # ------------------------------------------------------------------

    async def sismember(self, name: str, value: str) -> bool:
        return await self.client.sismember(name, value)

    async def sadd(self, name: str, *values: str) -> int:
        return await self.client.sadd(name, *values)

    async def expire(self, name: str, seconds: int) -> bool:
        return await self.client.expire(name, seconds)


# Singleton
redis_client = RedisClient()
