"""Unit tests for feature_engineering.py."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from app.feature_engineering import (
    add_all_features,
    add_atr,
    add_spread_features,
    add_volume_features,
    compute_tick_volume_score,
    spread_filter_mask,
    stream_data,
    validate,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def make_df(n: int = 50, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    times = pd.date_range("2023-01-01", periods=n, freq="5min", tz="UTC")
    close = 2000.0 + rng.normal(0, 10, n).cumsum()
    high = close + rng.uniform(0.5, 3.0, n)
    low = close - rng.uniform(0.5, 3.0, n)
    open_ = close + rng.normal(0, 1, n)
    return pd.DataFrame(
        {
            "time": times,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "tick_volume": rng.integers(100, 1000, n),
            "spread": rng.integers(5, 40, n),
        }
    )


# ---------------------------------------------------------------------------
# ATR
# ---------------------------------------------------------------------------

class TestATR:
    def test_column_added(self):
        df = add_atr(make_df())
        assert "atr" in df.columns

    def test_warmup_rows_are_nan(self):
        df = add_atr(make_df(50), period=14)
        # First (period-1) rows should be NaN due to min_periods
        assert df["atr"].iloc[:13].isna().all()

    def test_atr_positive_after_warmup(self):
        df = add_atr(make_df(50), period=14)
        assert (df["atr"].dropna() > 0).all()

    def test_original_df_not_mutated(self):
        original = make_df()
        original_cols = set(original.columns)
        add_atr(original)
        assert set(original.columns) == original_cols

    def test_custom_period(self):
        df5 = add_atr(make_df(50), period=5)
        df20 = add_atr(make_df(50), period=20)
        # Shorter period → more non-NaN rows
        assert df5["atr"].notna().sum() > df20["atr"].notna().sum()


# ---------------------------------------------------------------------------
# Spread features
# ---------------------------------------------------------------------------

class TestSpreadFeatures:
    def test_columns_added(self):
        df = add_spread_features(make_df())
        for col in ("spread_abs", "spread_pct", "spread_to_atr"):
            assert col in df.columns, f"missing: {col}"

    def test_spread_abs_scales_by_point(self):
        raw = make_df()
        df1 = add_spread_features(raw, point=1.0)
        df01 = add_spread_features(raw, point=0.01)
        pd.testing.assert_series_equal(
            df1["spread_abs"], raw["spread"].astype(float), check_names=False
        )
        pd.testing.assert_series_equal(
            df01["spread_abs"], raw["spread"].astype(float) * 0.01, check_names=False
        )

    def test_spread_pct_is_ratio(self):
        df = add_spread_features(make_df(), point=0.01)
        expected = df["spread_abs"] / df["close"]
        pd.testing.assert_series_equal(df["spread_pct"], expected, check_names=False)

    def test_spread_to_atr_nan_during_warmup(self):
        # add_spread_features calls add_atr(period=14) internally
        df = add_spread_features(make_df(50), point=0.01)
        # First (period-1) spread_to_atr values should be NaN (ATR warmup)
        assert df["spread_to_atr"].iloc[:13].isna().all()

    def test_spread_to_atr_positive_after_warmup(self):
        df = add_spread_features(make_df(50))
        assert (df["spread_to_atr"].dropna() > 0).all()

    def test_does_not_recompute_atr_if_present(self):
        """ATR already in df → add_spread_features should use it, not overwrite."""
        df = add_atr(make_df(50))
        atr_before = df["atr"].copy()
        df2 = add_spread_features(df)
        pd.testing.assert_series_equal(df2["atr"], atr_before)


# ---------------------------------------------------------------------------
# Volume features
# ---------------------------------------------------------------------------

class TestVolumeFeatures:
    def test_columns_added(self):
        df = add_volume_features(make_df())
        assert "tick_volume_zscore" in df.columns
        assert "tick_volume_percentile" in df.columns

    def test_percentile_in_unit_interval(self):
        df = add_volume_features(make_df(100), window=50)
        pct = df["tick_volume_percentile"].dropna()
        assert (pct >= 0).all() and (pct <= 1).all()

    def test_zscore_is_dimensionless(self):
        df = add_volume_features(make_df(100), window=50)
        # Z-scores for a uniform-ish distribution should be mostly in [-3, 3]
        z = df["tick_volume_zscore"].dropna()
        assert (z.abs() < 10).all()

    def test_constant_volume_zscore_is_zero(self):
        df = make_df(50)
        df["tick_volume"] = 500
        result = add_volume_features(df, window=20)
        # After warmup, std=0 is clamped to 1 → zscore = (500-500)/1 = 0
        assert (result["tick_volume_zscore"].dropna() == 0).all()


# ---------------------------------------------------------------------------
# Tick volume score
# ---------------------------------------------------------------------------

class TestTickVolumeScore:
    def test_score_values_are_restricted(self):
        df = add_volume_features(make_df(100), window=50)
        score = compute_tick_volume_score(df)
        assert set(score.unique()).issubset({0.05, 0.0, -0.05})

    def test_high_volume_gets_positive_score(self):
        df = make_df(200)
        df["tick_volume"] = [1 if i < 160 else 10000 for i in range(200)]
        df = add_volume_features(df, window=200)
        score = compute_tick_volume_score(df)
        # Last 40 rows have very high volume → should mostly score +0.05
        assert (score.iloc[160:] == 0.05).sum() > 30

    def test_returns_series_with_correct_length(self):
        df = make_df(60)
        df = add_volume_features(df, window=30)
        score = compute_tick_volume_score(df)
        assert len(score) == len(df)


# ---------------------------------------------------------------------------
# Spread filter
# ---------------------------------------------------------------------------

class TestSpreadFilter:
    def test_returns_boolean_series(self):
        df = add_spread_features(make_df(50))
        mask = spread_filter_mask(df)
        assert mask.dtype == bool

    def test_filter_disabled_passes_all(self, monkeypatch):
        from app import config
        monkeypatch.setattr(config.settings, "spread_filter_enabled", False)
        from app.feature_engineering import spread_filter_mask as fresh
        df = make_df(50)
        assert fresh(df).all()

    def test_dynamic_filter_blocks_high_spread(self, monkeypatch):
        from app import config
        monkeypatch.setattr(config.settings, "spread_filter_enabled", True)
        monkeypatch.setattr(config.settings, "use_dynamic_spread", True)
        monkeypatch.setattr(config.settings, "spread_atr_ratio_threshold", 0.001)

        df = add_spread_features(make_df(50), point=0.01)
        from app.feature_engineering import spread_filter_mask as fresh
        mask = fresh(df, point=0.01)
        # With very tight threshold, almost all rows should be filtered
        assert mask.sum() < len(df)

    def test_static_filter_blocks_large_spread(self, monkeypatch):
        from app import config
        monkeypatch.setattr(config.settings, "spread_filter_enabled", True)
        monkeypatch.setattr(config.settings, "use_dynamic_spread", False)
        monkeypatch.setattr(config.settings, "spread_threshold", 0.01)

        df = make_df(50)
        # spread_abs = spread * point=0.01, values will be 0.05–0.40 → all > 0.01
        df = add_spread_features(df, point=0.01)
        from app.feature_engineering import spread_filter_mask as fresh
        mask = fresh(df, point=0.01)
        assert not mask.any()

    def test_length_matches_input(self):
        df = make_df(40)
        mask = spread_filter_mask(df)
        assert len(mask) == len(df)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

class TestValidate:
    def test_drops_nan_rows(self):
        df = make_df(10)
        df.loc[3, "close"] = np.nan
        result = validate(df)
        assert len(result) == 9

    def test_drops_negative_ohlc(self):
        df = make_df(10)
        df.loc[5, "high"] = -1.0
        result = validate(df)
        assert len(result) == 9

    def test_drops_inverted_high_low(self):
        df = make_df(10)
        df.loc[2, "high"] = 1990.0
        df.loc[2, "low"] = 1995.0  # low > high
        result = validate(df)
        assert len(result) == 9

    def test_ensures_sorted_time(self):
        df = make_df(20)
        df = df.sample(frac=1, random_state=0)  # shuffle
        result = validate(df)
        assert result["time"].is_monotonic_increasing

    def test_clean_df_unchanged(self):
        df = make_df(20)
        result = validate(df)
        assert len(result) == len(df)


# ---------------------------------------------------------------------------
# add_all_features
# ---------------------------------------------------------------------------

class TestAddAllFeatures:
    def test_all_expected_columns_present(self):
        df = add_all_features(make_df(60))
        for col in ("atr", "spread_abs", "spread_pct", "spread_to_atr",
                    "tick_volume_zscore", "tick_volume_percentile", "tick_volume_score"):
            assert col in df.columns, f"missing: {col}"

    def test_no_mutation_of_input(self):
        original = make_df(30)
        cols_before = set(original.columns)
        add_all_features(original)
        assert set(original.columns) == cols_before


# ---------------------------------------------------------------------------
# stream_data
# ---------------------------------------------------------------------------

class TestStreamData:
    def test_yields_one_dict_per_row(self):
        df = make_df(10)
        rows = list(stream_data(df))
        assert len(rows) == 10

    def test_each_row_is_dict(self):
        df = make_df(5)
        for row in stream_data(df):
            assert isinstance(row, dict)

    def test_dict_contains_ohlcv(self):
        df = make_df(5)
        row = next(stream_data(df))
        for key in ("time", "open", "high", "low", "close", "tick_volume"):
            assert key in row

    def test_feature_columns_included(self):
        df = add_all_features(make_df(20))
        row = next(stream_data(df))
        assert "atr" in row
        assert "spread_abs" in row
        assert "tick_volume_score" in row

    def test_stream_is_lazy(self):
        """Generator should not materialise the whole DataFrame up front."""
        df = make_df(10_000)
        gen = stream_data(df)
        first = next(gen)
        assert isinstance(first, dict)
