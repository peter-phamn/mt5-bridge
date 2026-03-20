"""Unit tests for ParquetStorage."""
from __future__ import annotations

import pytest
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

from app.storage import ParquetStorage


@pytest.fixture
def tmp_storage(tmp_path: Path) -> ParquetStorage:
    return ParquetStorage(data_path=tmp_path)


def make_df(start: str, periods: int, freq: str = "5min") -> pd.DataFrame:
    times = pd.date_range(start, periods=periods, freq=freq, tz="UTC")
    return pd.DataFrame(
        {
            "time": times,
            "open": 1800.0,
            "high": 1802.0,
            "low": 1798.0,
            "close": 1801.0,
            "tick_volume": 100,
            "spread": 3,
        }
    )


class TestSaveLoad:
    def test_save_and_load_roundtrip(self, tmp_storage: ParquetStorage):
        df = make_df("2023-01-01", 10)
        n_new = tmp_storage.save(df, "XAUUSD", "M5")
        assert n_new == 10

        result = tmp_storage.load("XAUUSD", "M5")
        assert len(result) == 10
        assert list(result.columns[:6]) == ["time", "open", "high", "low", "close", "tick_volume"]

    def test_no_duplicates_on_second_save(self, tmp_storage: ParquetStorage):
        df = make_df("2023-06-01", 20)
        tmp_storage.save(df, "EURUSD", "H1")
        # Save same data again — should not grow
        tmp_storage.save(df, "EURUSD", "H1")
        result = tmp_storage.load("EURUSD", "H1")
        assert len(result) == 20

    def test_incremental_append(self, tmp_storage: ParquetStorage):
        df1 = make_df("2023-01-01 00:00", 5)
        df2 = make_df("2023-01-01 00:25", 5)  # starts right after df1
        tmp_storage.save(df1, "XAUUSD", "M5")
        tmp_storage.save(df2, "XAUUSD", "M5")
        result = tmp_storage.load("XAUUSD", "M5")
        assert len(result) == 10
        # Times must be sorted
        assert result["time"].is_monotonic_increasing

    def test_date_range_filter(self, tmp_storage: ParquetStorage):
        df = make_df("2023-03-01", 100, freq="1h")
        tmp_storage.save(df, "XAUUSD", "H1")

        from_dt = pd.Timestamp("2023-03-02", tz="UTC")
        to_dt = pd.Timestamp("2023-03-03", tz="UTC")
        result = tmp_storage.load("XAUUSD", "H1", from_dt, to_dt)
        assert (result["time"] >= from_dt).all()
        assert (result["time"] <= to_dt).all()

    def test_cross_year_storage(self, tmp_storage: ParquetStorage):
        df = make_df("2022-12-31 23:00", 5, freq="30min")
        tmp_storage.save(df, "XAUUSD", "M30")
        result = tmp_storage.load("XAUUSD", "M30")
        assert len(result) == 5

    def test_empty_load_returns_empty_df(self, tmp_storage: ParquetStorage):
        result = tmp_storage.load("NONEXISTENT", "M1")
        assert result.empty

    def test_get_latest_timestamp(self, tmp_storage: ParquetStorage):
        df = make_df("2023-01-01", 10, freq="1h")
        tmp_storage.save(df, "XAUUSD", "H1")
        latest = tmp_storage.get_latest_timestamp("XAUUSD", "H1")
        assert latest is not None
        expected = pd.Timestamp("2023-01-01 09:00", tz="UTC")
        assert latest == expected

    def test_list_symbols(self, tmp_storage: ParquetStorage):
        tmp_storage.save(make_df("2023-01-01", 5), "XAUUSD", "M5")
        tmp_storage.save(make_df("2023-01-01", 5), "EURUSD", "M5")
        symbols = tmp_storage.list_symbols()
        assert set(symbols) == {"XAUUSD", "EURUSD"}

    def test_lru_cache_eviction(self, tmp_storage: ParquetStorage):
        tmp_storage._cache_max = 2
        for sym in ("AAA", "BBB", "CCC"):
            tmp_storage.save(make_df("2023-01-01", 3), sym, "M1")
        # Load 3 files — oldest should be evicted
        for sym in ("AAA", "BBB", "CCC"):
            tmp_storage.load(sym, "M1")
        assert len(tmp_storage._cache) <= 2

    def test_timezone_normalization(self, tmp_storage: ParquetStorage):
        """Naive datetime input should be treated as UTC."""
        times = pd.date_range("2023-05-01", periods=5, freq="1h")  # tz-naive
        df = pd.DataFrame(
            {
                "time": times,
                "open": 1.1,
                "high": 1.2,
                "low": 1.0,
                "close": 1.15,
                "tick_volume": 50,
            }
        )
        tmp_storage.save(df, "EURUSD", "H1")
        result = tmp_storage.load("EURUSD", "H1")
        assert result["time"].dt.tz is not None
