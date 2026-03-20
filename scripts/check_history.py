#!/usr/bin/env python3
"""
Kiểm tra giới hạn history MT5 cho một symbol.

Usage:
    python scripts/check_history.py
    python scripts/check_history.py --symbol EURUSD --timeframe H1
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from app.mt5_client import mt5_client
from app.utils import setup_logging

setup_logging("WARNING")  # suppress INFO noise


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Check MT5 history availability.")
    p.add_argument("--symbol",    default="XAUUSD")
    p.add_argument("--timeframe", default="M5")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    symbol    = args.symbol.upper()
    timeframe = args.timeframe.upper()

    if not mt5_client.initialize():
        print("ERROR: Cannot connect to MT5. Make sure terminal is running.")
        sys.exit(1)

    print(f"\nChecking history for {symbol} / {timeframe} ...\n")

    # Fetch as many bars as possible (99_999 is MT5's practical cap per call)
    try:
        df = mt5_client.copy_rates_from_pos(symbol, timeframe, 0, 99_999)
    except Exception as exc:
        print(f"ERROR: {exc}")
        mt5_client.shutdown()
        sys.exit(1)

    if df.empty:
        print("No data returned. Check symbol name and Market Watch.")
        mt5_client.shutdown()
        return

    oldest = df["time"].iloc[0]
    newest = df["time"].iloc[-1]
    n_bars = len(df)

    tf_seconds = {
        "M1": 60, "M2": 120, "M3": 180, "M4": 240, "M5": 300,
        "M6": 360, "M10": 600, "M12": 720, "M15": 900, "M20": 1200,
        "M30": 1800, "H1": 3600, "H2": 7200, "H3": 10800, "H4": 14400,
        "H6": 21600, "H8": 28800, "H12": 43200, "D1": 86400,
        "W1": 604800, "MN1": 2592000,
    }
    bar_sec   = tf_seconds.get(timeframe, 300)
    span_days = (newest - oldest).total_seconds() / 86400

    print(f"  Symbol      : {symbol}")
    print(f"  Timeframe   : {timeframe}")
    print(f"  Bars found  : {n_bars:,}")
    print(f"  Oldest bar  : {oldest}  ← use this as from_date")
    print(f"  Newest bar  : {newest}")
    print(f"  Span        : {span_days:.1f} days  ({span_days/30:.1f} months)")
    print()

    if n_bars >= 99_999:
        print("  NOTE: hit 99,999-bar limit — broker may have even more history.")
        print("        Re-run after scrolling the chart back in MT5 to load more.")
    else:
        print(f"  This is all history the broker provides for {symbol} {timeframe}.")

    print(f"\n  Safe from_date to use in /download:")
    print(f'  "{oldest.strftime("%Y-%m-%dT%H:%M:%SZ")}"\n')

    mt5_client.shutdown()


if __name__ == "__main__":
    main()
