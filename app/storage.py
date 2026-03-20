from __future__ import annotations

import logging
from collections import OrderedDict
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from app.config import settings

logger = logging.getLogger(__name__)

# Canonical pyarrow schema for OHLC parquet files.
# tick_volume : MT5 tick count per bar (integer, always present)
# atr         : Wilder's ATR (float, NaN for raw downloads; filled when features are persisted)
OHLC_SCHEMA = pa.schema(
    [
        pa.field("time", pa.timestamp("us", tz="UTC")),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("tick_volume", pa.int64()),
        pa.field("spread", pa.int32()),
        pa.field("atr", pa.float64()),
    ]
)


class ParquetStorage:
    """
    Reads and writes OHLC data as per-year Parquet files.

    Layout:  {data_path}/{SYMBOL}/{TIMEFRAME}/{YEAR}.parquet
    """

    def __init__(self, data_path: Path | None = None) -> None:
        self.root = (data_path or settings.data_path).resolve()
        self._cache: OrderedDict[str, pd.DataFrame] = OrderedDict()
        self._cache_max = settings.cache_max_size

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def save(self, df: pd.DataFrame, symbol: str, timeframe: str) -> int:
        """Merge *df* into existing storage, deduplicating and sorting.

        Returns number of NEW rows written.
        """
        if df.empty:
            return 0

        df = self._normalize(df)

        # Split incoming data by year
        df["_year"] = df["time"].dt.year
        new_total = 0

        for year, year_df in df.groupby("_year"):
            year_df = year_df.drop(columns="_year")
            path = self._parquet_path(symbol, timeframe, int(year))
            path.parent.mkdir(parents=True, exist_ok=True)

            if path.exists():
                existing = self._read_file(path)
                n_existing = len(existing)
                combined = pd.concat([existing, year_df], ignore_index=True)
            else:
                n_existing = 0
                combined = year_df

            cleaned = self._dedupe_and_sort(combined)
            n_new = len(cleaned) - n_existing
            self._write_file(cleaned, path)

            # Invalidate cache entry
            cache_key = self._cache_key(symbol, timeframe, int(year))
            self._cache.pop(cache_key, None)
            new_total += n_new

        df.drop(columns="_year", inplace=True, errors="ignore")
        logger.info(
            "Saved %d new rows for %s/%s", new_total, symbol, timeframe
        )
        return new_total

    def load(
        self,
        symbol: str,
        timeframe: str,
        from_dt: Optional[pd.Timestamp] = None,
        to_dt: Optional[pd.Timestamp] = None,
    ) -> pd.DataFrame:
        """Load data for *symbol/timeframe* within optional date range."""
        symbol_dir = self.root / symbol.upper() / timeframe.upper()
        if not symbol_dir.exists():
            return _empty_df()

        # Determine which year files overlap the requested range
        years = self._years_in_range(symbol_dir, from_dt, to_dt)
        if not years:
            return _empty_df()

        frames: list[pd.DataFrame] = []
        for year in years:
            path = self._parquet_path(symbol, timeframe, year)
            if not path.exists():
                continue
            cache_key = self._cache_key(symbol, timeframe, year)
            if cache_key in self._cache:
                frames.append(self._cache[cache_key])
            else:
                df = self._read_file(path)
                self._put_cache(cache_key, df)
                frames.append(df)

        if not frames:
            return _empty_df()

        result = pd.concat(frames, ignore_index=True)
        result = result.sort_values("time").reset_index(drop=True)

        if from_dt is not None:
            from_dt = _ensure_utc(from_dt)
            result = result[result["time"] >= from_dt]
        if to_dt is not None:
            to_dt = _ensure_utc(to_dt)
            result = result[result["time"] <= to_dt]

        return result.reset_index(drop=True)

    def get_latest_timestamp(
        self, symbol: str, timeframe: str
    ) -> Optional[pd.Timestamp]:
        """Return the most recent bar timestamp stored, or None."""
        symbol_dir = self.root / symbol.upper() / timeframe.upper()
        if not symbol_dir.exists():
            return None

        parquet_files = sorted(symbol_dir.glob("*.parquet"), reverse=True)
        for path in parquet_files:
            try:
                df = self._read_file(path)
                if not df.empty:
                    return df["time"].max()
            except Exception:
                continue
        return None

    def list_symbols(self) -> list[str]:
        """Return symbols that have local data."""
        if not self.root.exists():
            return []
        return [p.name for p in sorted(self.root.iterdir()) if p.is_dir()]

    def list_timeframes(self, symbol: str) -> list[str]:
        sym_dir = self.root / symbol.upper()
        if not sym_dir.exists():
            return []
        return [p.name for p in sorted(sym_dir.iterdir()) if p.is_dir()]

    def clear_cache(self) -> None:
        self._cache.clear()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parquet_path(self, symbol: str, timeframe: str, year: int) -> Path:
        return self.root / symbol.upper() / timeframe.upper() / f"{year}.parquet"

    @staticmethod
    def _cache_key(symbol: str, timeframe: str, year: int) -> str:
        return f"{symbol.upper()}:{timeframe.upper()}:{year}"

    def _put_cache(self, key: str, df: pd.DataFrame) -> None:
        if key in self._cache:
            self._cache.move_to_end(key)
        else:
            self._cache[key] = df
            if len(self._cache) > self._cache_max:
                evicted = next(iter(self._cache))
                del self._cache[evicted]

    @staticmethod
    def _normalize(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # Ensure 'time' is UTC timestamp
        if not pd.api.types.is_datetime64_any_dtype(df["time"]):
            df["time"] = pd.to_datetime(df["time"], utc=True)
        elif df["time"].dt.tz is None:
            df["time"] = df["time"].dt.tz_localize("UTC")
        else:
            df["time"] = df["time"].dt.tz_convert("UTC")

        df["time"] = df["time"].astype("datetime64[us, UTC]")

        # Ensure required OHLC columns
        for col in ("open", "high", "low", "close"):
            df[col] = df[col].astype("float64")

        # tick_volume: accept either name; old data may arrive as "volume"
        if "tick_volume" not in df.columns:
            df["tick_volume"] = df["volume"].astype("int64") if "volume" in df.columns else 0
        df["tick_volume"] = df["tick_volume"].astype("int64")

        if "spread" not in df.columns:
            df["spread"] = 0
        df["spread"] = df["spread"].fillna(0).astype("int32")

        # atr is optional at write time (NaN for raw downloads)
        if "atr" not in df.columns:
            df["atr"] = np.nan
        df["atr"] = df["atr"].astype("float64")

        return df[["time", "open", "high", "low", "close", "tick_volume", "spread", "atr"]]

    @staticmethod
    def _dedupe_and_sort(df: pd.DataFrame) -> pd.DataFrame:
        return df.drop_duplicates(subset=["time"]).sort_values("time").reset_index(drop=True)

    @staticmethod
    def _read_file(path: Path) -> pd.DataFrame:
        df = pd.read_parquet(path)
        # Ensure UTC-aware timestamp (old files may lack timezone)
        if df["time"].dt.tz is None:
            df["time"] = df["time"].dt.tz_localize("UTC")
        df["time"] = df["time"].astype("datetime64[us, UTC]")
        # Backward compat: files written before spread column existed
        if "spread" not in df.columns:
            df["spread"] = 0
        df["spread"] = df["spread"].fillna(0).astype("int32")
        # Backward compat: files written with "volume" before tick_volume rename
        if "tick_volume" not in df.columns:
            df["tick_volume"] = df["volume"].astype("int64") if "volume" in df.columns else 0
        df["tick_volume"] = df["tick_volume"].astype("int64")
        # Backward compat: files written before atr column existed
        if "atr" not in df.columns:
            df["atr"] = np.nan
        return df

    @staticmethod
    def _write_file(df: pd.DataFrame, path: Path) -> None:
        table = pa.Table.from_pandas(df, schema=OHLC_SCHEMA, preserve_index=False)
        pq.write_table(
            table,
            path,
            compression="snappy",
            write_statistics=True,
        )

    @staticmethod
    def _years_in_range(
        symbol_dir: Path,
        from_dt: Optional[pd.Timestamp],
        to_dt: Optional[pd.Timestamp],
    ) -> list[int]:
        available = []
        for p in symbol_dir.glob("*.parquet"):
            try:
                available.append(int(p.stem))
            except ValueError:
                pass

        if not available:
            return []

        min_year = from_dt.year if from_dt is not None else min(available)
        max_year = to_dt.year if to_dt is not None else max(available)
        return [y for y in available if min_year <= y <= max_year]


def _empty_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["time", "open", "high", "low", "close", "tick_volume", "spread", "atr"]
    )


def _ensure_utc(ts) -> pd.Timestamp:
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


# Singleton
storage = ParquetStorage()
