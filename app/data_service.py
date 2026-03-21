from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import AsyncIterator, Optional

import pandas as pd

from app.config import settings
from app.feature_engineering import add_atr
from app.mt5_client import MT5Error, mt5_client
from app.storage import storage

logger = logging.getLogger(__name__)


class DataService:
    """Orchestrates MT5 fetching + Parquet storage."""

    # ------------------------------------------------------------------
    # Download / incremental update
    # ------------------------------------------------------------------

    def download(
        self,
        symbol: str,
        timeframe: str,
        from_date: datetime,
        to_date: Optional[datetime] = None,
    ) -> dict:
        """
        Download OHLC data from MT5 and persist to Parquet.
        Automatically skips already-stored data (incremental update).
        """
        t0 = time.perf_counter()

        if to_date is None:
            to_date = datetime.now(timezone.utc)

        timeframe = timeframe.upper()

        # Incremental: advance from_date to just after last stored bar
        latest = storage.get_latest_timestamp(symbol, timeframe)
        effective_from = from_date
        if latest is not None:
            candidate = latest.to_pydatetime() + pd.Timedelta(seconds=1)
            if candidate > effective_from:
                effective_from = candidate
                logger.info(
                    "[%s %s] Incremental update from %s",
                    symbol,
                    timeframe,
                    effective_from,
                )

        if effective_from >= to_date:
            logger.info("[%s %s] Already up to date.", symbol, timeframe)
            return {
                "rows_downloaded": 0,
                "rows_new": 0,
                "from_date": from_date,
                "effective_from": effective_from,
                "to_date": to_date,
                "duration_seconds": time.perf_counter() - t0,
            }

        df = mt5_client.fetch_with_retry(symbol, timeframe, effective_from, to_date)
        rows_downloaded = len(df)

        rows_new = storage.save(df, symbol, timeframe)

        # Recompute ATR on the full stored dataset (all years) so that
        # cross-year boundaries get correct warmup context, then persist.
        df_all = storage.load(symbol, timeframe)
        if not df_all.empty:
            df_all = add_atr(df_all)
            storage.write_enriched(df_all, symbol, timeframe)
            logger.info("[%s %s] ATR recomputed on %d rows", symbol, timeframe, len(df_all))

        duration = time.perf_counter() - t0
        logger.info(
            "[%s %s] Download complete: %d fetched, %d new rows in %.2fs",
            symbol,
            timeframe,
            rows_downloaded,
            rows_new,
            duration,
        )

        return {
            "rows_downloaded": rows_downloaded,
            "rows_new": rows_new,
            "from_date": from_date,
            "effective_from": effective_from,
            "to_date": to_date,
            "duration_seconds": duration,
        }

    async def download_multi(
        self,
        symbols: list[str],
        timeframe: str,
        from_date: datetime,
        to_date: Optional[datetime] = None,
    ) -> list[dict]:
        """Download multiple symbols concurrently."""

        loop = asyncio.get_event_loop()

        async def _download_one(sym: str) -> dict:
            try:
                result = await loop.run_in_executor(
                    None, self.download, sym, timeframe, from_date, to_date
                )
                return {"symbol": sym, "status": "ok", **result}
            except MT5Error as exc:
                logger.error("Failed to download %s: %s", sym, exc)
                return {"symbol": sym, "status": "error", "error": str(exc)}

        tasks = [_download_one(sym) for sym in symbols]
        results = await asyncio.gather(*tasks)
        return list(results)

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def get_history(
        self,
        symbol: str,
        timeframe: str,
        from_dt: datetime,
        to_dt: datetime,
    ) -> pd.DataFrame:
        """Load OHLC from local Parquet storage."""
        return storage.load(
            symbol.upper(),
            timeframe.upper(),
            pd.Timestamp(from_dt),
            pd.Timestamp(to_dt),
        )

    # ------------------------------------------------------------------
    # Replay / streaming
    # ------------------------------------------------------------------

    def replay(
        self,
        symbol: str,
        timeframe: str,
        from_dt: datetime,
        to_dt: datetime,
        speed: float = 1.0,
    ) -> AsyncIterator[dict]:
        """
        Async generator that yields OHLC bars one at a time, optionally
        throttled to simulate live feed.

        speed=0 → yield as fast as possible (useful for backtesting)
        speed=1 → real-time (1 bar per bar-duration is *not* simulated here;
                  the caller decides pacing).  Use speed=0 for instant replay.
        """
        return self._replay_gen(symbol, timeframe, from_dt, to_dt, speed)

    async def _replay_gen(
        self,
        symbol: str,
        timeframe: str,
        from_dt: datetime,
        to_dt: datetime,
        speed: float,
    ) -> AsyncIterator[dict]:
        df = self.get_history(symbol, timeframe, from_dt, to_dt)
        if df.empty:
            return

        # Build column list: always include OHLCVS, add any feature columns present
        base_cols = ["time", "open", "high", "low", "close", "tick_volume", "spread"]
        feature_cols = [
            c for c in df.columns
            if c not in base_cols and c not in ("_year",)
        ]
        all_cols = base_cols + feature_cols

        for row in df[all_cols].itertuples(index=False):
            d = row._asdict()
            d["time"] = d["time"].isoformat()
            # Replace NaN with None for JSON compatibility
            bar = {k: (None if isinstance(v, float) and v != v else v) for k, v in d.items()}
            yield bar
            if speed > 0:
                await asyncio.sleep(speed)

    # ------------------------------------------------------------------
    # DuckDB querying
    # ------------------------------------------------------------------

    def query_duckdb(self, sql: str) -> pd.DataFrame:
        """Run an arbitrary SQL query against the Parquet files via DuckDB."""
        if not settings.duckdb_enabled:
            raise RuntimeError("DuckDB is disabled in config.")
        try:
            import duckdb
        except ImportError:
            raise RuntimeError("duckdb package is not installed.")

        conn = duckdb.connect(database=":memory:", read_only=False)
        # Make all parquet files accessible as a wildcard glob
        parquet_glob = str(settings.data_path / "**" / "*.parquet")
        conn.execute(f"SET file_search_path='{settings.data_path}'")
        result = conn.execute(sql).df()
        conn.close()
        return result

    def query_symbol_duckdb(
        self,
        symbol: str,
        timeframe: str,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
        extra_sql: str = "",
    ) -> pd.DataFrame:
        """Convenience wrapper: query a specific symbol/timeframe via DuckDB."""
        try:
            import duckdb
        except ImportError:
            raise RuntimeError("duckdb package is not installed.")

        sym_dir = settings.data_path / symbol.upper() / timeframe.upper()
        if not sym_dir.exists():
            return pd.DataFrame()

        glob_path = str(sym_dir / "*.parquet")
        conditions = []
        if from_dt:
            conditions.append(f"time >= '{from_dt.isoformat()}'")
        if to_dt:
            conditions.append(f"time <= '{to_dt.isoformat()}'")
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        sql = f"""
            SELECT * FROM read_parquet('{glob_path}')
            {where}
            {extra_sql}
            ORDER BY time
        """
        conn = duckdb.connect(database=":memory:")
        result = conn.execute(sql).df()
        conn.close()
        return result


# Singleton
data_service = DataService()
