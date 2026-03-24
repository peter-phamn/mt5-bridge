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
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import TIMEFRAME_MAP
from app.mt5_client import MT5_AVAILABLE, mt5_client
from app.utils import setup_logging

setup_logging("WARNING")  # suppress INFO noise

TF_SECONDS = {
    "M1": 60,   "M2": 120,  "M3": 180,  "M4": 240,  "M5": 300,
    "M6": 360,  "M10": 600, "M12": 720, "M15": 900, "M20": 1_200,
    "M30": 1_800, "H1": 3_600, "H2": 7_200, "H3": 10_800, "H4": 14_400,
    "H6": 21_600, "H8": 28_800, "H12": 43_200, "D1": 86_400,
    "W1": 604_800, "MN1": 2_592_000,
}

# Probe start — go far enough back to catch any broker history
PROBE_FROM = datetime(2000, 1, 1, tzinfo=timezone.utc)


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

    if not MT5_AVAILABLE:
        print("ERROR: MetaTrader5 package not available.")
        sys.exit(1)

    import MetaTrader5 as mt5  # type: ignore

    print(f"\nChecking history for {symbol} / {tf} ...\n")

    tf_int  = TIMEFRAME_MAP.get(tf)
    if tf_int is None:
        print(f"ERROR: Unknown timeframe '{tf}'")
        sys.exit(1)

    mt5.symbol_select(symbol, True)

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    probe_from = PROBE_FROM.replace(tzinfo=None)

    rates = mt5.copy_rates_range(symbol, tf_int, probe_from, now)

    if rates is None or len(rates) == 0:
        code, msg = mt5.last_error()
        print(f"ERROR: No data for {symbol}/{tf} — MT5 error code={code} msg={msg!r}")
        mt5_client.shutdown()
        sys.exit(1)

    import pandas as pd  # noqa: PLC0415
    df      = pd.DataFrame(rates)
    df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
    df      = df.sort_values("time").reset_index(drop=True)

    oldest    = df["time"].iloc[0]
    newest    = df["time"].iloc[-1]
    n_bars    = len(df)
    span_days = (newest - oldest).total_seconds() / 86400

    print(f"  Symbol      : {symbol}")
    print(f"  Timeframe   : {tf}")
    print(f"  Bars found  : {n_bars:,}")
    print(f"  Oldest bar  : {oldest}  ← use this as from_date")
    print(f"  Newest bar  : {newest}")
    print(f"  Span        : {span_days:.1f} days  ({span_days/30:.1f} months)")
    print()
    print(f"  This is all history available for {symbol} {tf}.")
    print(f"\n  Safe from_date to use in /download:")
    print(f'  "{oldest.strftime("%Y-%m-%dT%H:%M:%SZ")}"\n')

    mt5_client.shutdown()


if __name__ == "__main__":
    main()
