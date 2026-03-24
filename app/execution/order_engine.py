from __future__ import annotations

import asyncio
import logging
import time
import uuid

from app.redis_client import redis_client

try:
    import MetaTrader5 as mt5  # type: ignore
    MT5_AVAILABLE = True
except ImportError:
    mt5 = None
    MT5_AVAILABLE = False

logger = logging.getLogger(__name__)

# Redis keys
STREAM_ORDERS = "stream:orders"
STREAM_FILLS  = "stream:fills"
SEEN_KEY      = "orders:seen"
SEEN_TTL      = 86_400   # 24 hours — idempotency window

GROUP    = "bridge"
CONSUMER = "order-executor"


async def run() -> None:
    """Consume stream:orders, execute via MT5, publish results to stream:fills.

    Flow
    ----
    1. XREADGROUP blocks up to 500 ms waiting for orders
    2. Idempotency check via SISMEMBER orders:seen
    3. Execute market order via mt5.order_send()
    4. XADD result to stream:fills
    5. XACK the consumed message

    Fault tolerance
    ---------------
    - On MT5 error: publish REJECTED fill, still ACK (no retry loop on execution)
    - On Redis error: retry with backoff (order not ACKed → re-delivered on reconnect)
    - On crash: unACKed messages are re-delivered via consumer group on restart
    """
    await redis_client.xgroup_create(STREAM_ORDERS, GROUP, id="0", mkstream=True)
    logger.info("Order engine started (group=%s consumer=%s)", GROUP, CONSUMER)

    while True:
        try:
            messages = await redis_client.xreadgroup(
                GROUP, CONSUMER,
                {STREAM_ORDERS: ">"},
                count=1,
                block=500,
            )
            if not messages:
                continue

            for _, entries in messages:
                for entry_id, data in entries:
                    try:
                        await _process(entry_id, data)
                    except Exception as exc:
                        logger.error(
                            "order_engine: unhandled error entry=%s: %s", entry_id, exc
                        )
                        # ACK anyway — operator must inspect stream:fills for REJECTED
                        await redis_client.xack(STREAM_ORDERS, GROUP, entry_id)

        except asyncio.CancelledError:
            logger.info("Order engine stopped")
            return
        except Exception as exc:
            logger.error("order_engine: unexpected error: %s", exc)
            await asyncio.sleep(1.0)


async def _process(entry_id: str, data: dict) -> None:
    order_id = data.get("id") or str(uuid.uuid4())

    # Idempotency: skip already-processed orders
    if await redis_client.sismember(SEEN_KEY, order_id):
        logger.warning("Duplicate order ignored: id=%s", order_id)
        await redis_client.xack(STREAM_ORDERS, GROUP, entry_id)
        return

    await redis_client.sadd(SEEN_KEY, order_id)
    await redis_client.expire(SEEN_KEY, SEEN_TTL)

    if not MT5_AVAILABLE:
        logger.error("MT5 unavailable — rejecting order %s", order_id)
        await _publish_fill(order_id, {"status": "REJECTED", "reason": "MT5 unavailable"})
        await redis_client.xack(STREAM_ORDERS, GROUP, entry_id)
        return

    symbol = data["symbol"]
    side   = data["side"].upper()
    volume = float(data["volume"])
    sl     = float(data.get("sl", 0.0))
    tp     = float(data.get("tp", 0.0))

    request = {
        "action":       mt5.TRADE_ACTION_DEAL,
        "symbol":       symbol,
        "volume":       volume,
        "type":         mt5.ORDER_TYPE_BUY if side == "BUY" else mt5.ORDER_TYPE_SELL,
        "sl":           sl,
        "tp":           tp,
        "type_filling": mt5.ORDER_FILLING_IOC,
        "comment":      f"bridge:{order_id[:8]}",
    }

    logger.info(
        "Executing order id=%s symbol=%s side=%s volume=%.2f sl=%.5f tp=%.5f",
        order_id, symbol, side, volume, sl, tp,
    )

    t0 = time.perf_counter()
    result = mt5.order_send(request)
    latency_ms = round((time.perf_counter() - t0) * 1000)

    if result is None:
        code, msg = mt5.last_error()
        logger.error("order_send returned None — code=%d msg=%r", code, msg)
        await _publish_fill(order_id, {
            "status":     "REJECTED",
            "reason":     f"MT5 code={code} msg={msg}",
            "latency_ms": latency_ms,
        })

    elif result.retcode == mt5.TRADE_RETCODE_DONE:
        expected = float(data.get("expected_price", result.price))
        slippage = round(result.price - expected, 5)
        logger.info(
            "FILLED id=%s ticket=%d price=%.5f slippage=%.5f latency=%dms",
            order_id, result.order, result.price, slippage, latency_ms,
        )
        await _publish_fill(order_id, {
            "status":     "FILLED",
            "fill_price": result.price,
            "slippage":   slippage,
            "volume":     result.volume,
            "ticket":     result.order,
            "latency_ms": latency_ms,
        })

    else:
        logger.error(
            "REJECTED id=%s retcode=%d comment=%r",
            order_id, result.retcode, result.comment,
        )
        await _publish_fill(order_id, {
            "status":     "REJECTED",
            "reason":     result.comment,
            "retcode":    result.retcode,
            "latency_ms": latency_ms,
        })

    await redis_client.xack(STREAM_ORDERS, GROUP, entry_id)


async def _publish_fill(order_id: str, data: dict) -> None:
    fields = {"id": order_id, **{k: str(v) for k, v in data.items()}}
    await redis_client.xadd(STREAM_FILLS, fields, maxlen=10_000)
