#!/usr/bin/env python3
"""
Kiểm tra giới hạn history MT5 cho một symbol.

Usage:
    python scripts/check_history.py
    python scripts/check_history.py --symbol XAUUSDm --timeframe M5
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.mt5_client import mt5_client
from app.utils import setup_logging

setup_logging("WARNING")  # suppress INFO noise

TF_SECONDS = {
    "M1": 60,   "M2": 120,  "M3": 180,  "M4": 240,  "M5": 300,
    "M6": 360,  "M10": 600, "M12": 720, "M15": 900, "M20": 1_200,
    "M30": 1_800, "H1": 3_600, "H2": 7_200, "H3": 10_800, "H4": 14_400,
    "H6": 21_600, "H8": 28_800, "H12": 43_200, "D1": 86_400,
    "W1": 604_800, "MN1": 2_592_000,
}

CHUNK = 99_999  # MT5 per-call cap


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Check MT5 history availability.")
    p.add_argument("--symbol",    default="XAUUSDm")
    p.add_argument("--timeframe", default="M5")
    return p.parse_args()


def main() -> None:
    args   = parse_args()
    symbol = args.symbol                # preserve original case — MT5 is case-sensitive
    tf     = args.timeframe.upper()

    if not mt5_client.initialize():
        print("ERROR: Cannot connect to MT5. Make sure terminal is running.")
        sys.exit(1)

    print(f"\nChecking history for {symbol} / {tf} ...\n")

    bar_sec = TF_SECONDS.get(tf, 300)

    # ── Walk backwards in 99,999-bar steps until broker returns nothing ──
    oldest_bar = None
    newest_bar = None
    total_bars = 0
    start_pos  = 0

    while True:
        try:
            df = mt5_client.copy_rates_from_pos(symbol, tf, start_pos, CHUNK)
        except Exception as exc:
            if start_pos == 0:
                print(f"ERROR: {exc}")
                mt5_client.shutdown()
                sys.exit(1)
            break  # no more data at this position

        if df.empty:
            break

        total_bars += len(df)

        # copy_rates_from_pos returns bars oldest-first within the window
        batch_oldest = df["time"].iloc[0]
        batch_newest = df["time"].iloc[-1]

        if newest_bar is None:
            newest_bar = batch_newest   # first batch = most recent bars
        oldest_bar = batch_oldest       # keep updating — last batch = oldest

        if len(df) < CHUNK:
            break   # broker returned fewer than requested → reached the beginning

        start_pos += CHUNK

    if oldest_bar is None:
        print("No data returned. Check symbol name and Market Watch.")
        mt5_client.shutdown()
        return

    span_days = (newest_bar - oldest_bar).total_seconds() / 86400

    print(f"  Symbol      : {symbol}")
    print(f"  Timeframe   : {tf}")
    print(f"  Bars found  : {total_bars:,}")
    print(f"  Oldest bar  : {oldest_bar}  ← use this as from_date")
    print(f"  Newest bar  : {newest_bar}")
    print(f"  Span        : {span_days:.1f} days  ({span_days/30:.1f} months)")
    print()
    print(f"  This is all history the broker provides for {symbol} {tf}.")
    print(f"\n  Safe from_date to use in /download:")
    print(f'  "{oldest_bar.strftime("%Y-%m-%dT%H:%M:%SZ")}"\n')

    mt5_client.shutdown()


if __name__ == "__main__":
    main()
