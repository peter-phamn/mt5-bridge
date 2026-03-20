#!/usr/bin/env python3
"""
CLI tool to download full historical OHLC data from MT5.

Usage examples:
    python scripts/download_full_history.py --symbol XAUUSD --timeframe M5
    python scripts/download_full_history.py --symbol EURUSD --timeframe H1 \
        --from 2020-01-01 --to 2024-01-01
    python scripts/download_full_history.py --symbols XAUUSD EURUSD USDJPY \
        --timeframe H1 --from 2022-01-01
    python scripts/download_full_history.py --all-symbols --timeframe D1
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Ensure project root is on sys.path when run directly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import settings
from app.data_service import data_service
from app.mt5_client import MT5Error, mt5_client
from app.utils import setup_logging

setup_logging("INFO")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Download MT5 historical OHLC data to Parquet."
    )

    sym_group = p.add_mutually_exclusive_group(required=True)
    sym_group.add_argument("--symbol", help="Single symbol, e.g. XAUUSD")
    sym_group.add_argument(
        "--symbols", nargs="+", help="Multiple symbols, e.g. XAUUSD EURUSD"
    )
    sym_group.add_argument(
        "--all-symbols",
        action="store_true",
        help="Download all symbols in config.default_symbols",
    )

    p.add_argument(
        "--timeframe",
        required=True,
        help="Timeframe string, e.g. M1 M5 M15 H1 D1",
    )
    p.add_argument(
        "--from",
        dest="from_date",
        default="2010-01-01",
        help="Start date (YYYY-MM-DD). Default: 2010-01-01",
    )
    p.add_argument(
        "--to",
        dest="to_date",
        default=None,
        help="End date (YYYY-MM-DD). Default: now",
    )
    p.add_argument(
        "--no-incremental",
        action="store_true",
        help="Ignore existing data and re-download everything",
    )
    return p.parse_args()


def resolve_symbols(args: argparse.Namespace) -> list[str]:
    if args.all_symbols:
        return [s.upper() for s in settings.default_symbols]
    if args.symbols:
        return [s.upper() for s in args.symbols]
    return [args.symbol.upper()]


def parse_date(s: str | None, default: datetime) -> datetime:
    if s is None:
        return default
    dt = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return dt


async def main() -> None:
    args = parse_args()
    symbols = resolve_symbols(args)
    timeframe = args.timeframe.upper()
    from_date = parse_date(args.from_date, datetime(2010, 1, 1, tzinfo=timezone.utc))
    to_date = parse_date(args.to_date, datetime.now(timezone.utc))

    print(f"\n{'='*60}")
    print(f"  MT5 Historical Data Downloader")
    print(f"{'='*60}")
    print(f"  Symbols   : {', '.join(symbols)}")
    print(f"  Timeframe : {timeframe}")
    print(f"  From      : {from_date.date()}")
    print(f"  To        : {to_date.date()}")
    print(f"  Storage   : {settings.data_path}")
    print(f"{'='*60}\n")

    # Connect to MT5
    if not mt5_client.initialize():
        print("ERROR: Could not connect to MetaTrader 5.")
        print("Make sure MT5 terminal is running and credentials are set in .env")
        sys.exit(1)

    t0 = time.perf_counter()
    total_new = 0

    if len(symbols) == 1:
        # Single symbol — run synchronously for cleaner output
        sym = symbols[0]
        try:
            result = data_service.download(sym, timeframe, from_date, to_date)
            total_new += result["rows_new"]
            print(
                f"  [{sym}] {result['rows_downloaded']:>8,} fetched | "
                f"{result['rows_new']:>8,} new | "
                f"{result['duration_seconds']:.1f}s"
            )
        except MT5Error as exc:
            print(f"  [{sym}] ERROR: {exc}")
    else:
        # Multiple symbols — run concurrently
        results = await data_service.download_multi(symbols, timeframe, from_date, to_date)
        for r in results:
            sym = r["symbol"]
            if r["status"] == "ok":
                total_new += r.get("rows_new", 0)
                print(
                    f"  [{sym}] {r.get('rows_downloaded', 0):>8,} fetched | "
                    f"{r.get('rows_new', 0):>8,} new | "
                    f"{r.get('duration_seconds', 0):.1f}s"
                )
            else:
                print(f"  [{sym}] ERROR: {r.get('error')}")

    elapsed = time.perf_counter() - t0
    print(f"\n  Done. {total_new:,} new rows total in {elapsed:.1f}s\n")
    mt5_client.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
