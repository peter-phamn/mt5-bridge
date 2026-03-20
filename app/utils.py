from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone


def setup_logging(level: str = "INFO") -> None:
    fmt = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    logging.basicConfig(
        stream=sys.stdout,
        level=getattr(logging, level.upper(), logging.INFO),
        format=fmt,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Quiet down noisy libraries
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    # Try ISO 8601
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            dt = datetime.strptime(value, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    raise ValueError(f"Cannot parse datetime: '{value}'")


def timeframe_to_seconds(tf: str) -> int:
    """Return the approximate duration of one bar in seconds."""
    mapping = {
        "M1": 60,
        "M2": 120,
        "M3": 180,
        "M4": 240,
        "M5": 300,
        "M6": 360,
        "M10": 600,
        "M12": 720,
        "M15": 900,
        "M20": 1200,
        "M30": 1800,
        "H1": 3600,
        "H2": 7200,
        "H3": 10800,
        "H4": 14400,
        "H6": 21600,
        "H8": 28800,
        "H12": 43200,
        "D1": 86400,
        "W1": 604800,
        "MN1": 2592000,
    }
    return mapping.get(tf.upper(), 60)
