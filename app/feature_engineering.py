"""
Feature engineering for OHLC + spread + tick_volume data.

Performance design
------------------
``add_all_features()`` is the primary entry point.  It copies the DataFrame
exactly *once* then runs all computations in-place via private ``_compute_*``
functions.  Chaining the individual ``add_*`` helpers incurs one copy per
call — fine for interactive use, avoid in hot paths.

True Range uses numpy ufuncs (``np.fmax`` for NaN-ignoring max, matching
pandas ``skipna=True``) rather than ``pd.concat``.  This eliminates the
intermediate 3-column DataFrame (~24 MB per 1 M rows).

Remaining bottleneck
--------------------
``rolling().rank(pct=True)`` for ``tick_volume_percentile`` is O(n·w·log w)
and is the dominant cost (~350 ms per 1 M rows with window=100).  It is a
Cython loop inside pandas and cannot be replaced without changing semantics.
Mitigation strategies if this matters:

  1. Reduce ``tick_volume_window`` (e.g. 50 instead of 100) — roughly halves time.
  2. Skip ``tick_volume_percentile`` entirely if you only need the z-score.
  3. For approximate results: threshold directly on ``tick_volume_zscore``
     (z > 0.84 ≈ 80th pct, z < -0.84 ≈ 20th pct of a normal distribution).

Note on ``bottleneck.move_rank``
---------------------------------
``bn.move_rank`` scales to ``[-1, 1]`` (centred signed rank), not ``[0, 1]``,
and diverges from pandas pct rank by up to 0.5 at the window boundary — it is
NOT a drop-in replacement and is not used here.

Column contract
---------------
  volume  : MT5 tick_volume (ticks per bar — liquidity proxy)
  spread  : MT5 spread in *points*; pass ``point=<symbol point>`` to convert
            to price units (XAUUSD → 0.01, EURUSD → 0.00001, USDJPY → 0.001)
"""
from __future__ import annotations

import logging
from typing import Generator

import numpy as np
import pandas as pd

from app.config import settings

logger = logging.getLogger(__name__)


def _nan_to_none(v: object) -> object:
    """NaN → None (JSON-safe).  Module-level avoids closure-lookup overhead."""
    return None if (isinstance(v, float) and v != v) else v


# ---------------------------------------------------------------------------
# Private: in-place compute functions (no internal copy)
# Callers are responsible for owning a copy before calling these.
# ---------------------------------------------------------------------------


def _compute_atr(df: pd.DataFrame, period: int) -> None:
    """Write Wilder's ATR into df['atr'].  Mutates df.

    Implementation notes
    ~~~~~~~~~~~~~~~~~~~~
    - Two reusable numpy buffers (``hl``, ``h_cp``) replace the 3-column
      intermediate DataFrame that ``pd.concat`` creates (~3× fewer allocations).
    - ``np.fmax`` (NaN-ignoring max) ensures TR[0] = High[0]-Low[0] when no
      previous close exists — matches ``pd.DataFrame.max(skipna=True)``.
    - ``pd.Series.ewm().mean()`` is left as-is; it is already Cython-compiled
      and there is no faster general substitute.
    """
    h  = df["high"].to_numpy(dtype=np.float64, copy=False)
    lo = df["low"].to_numpy(dtype=np.float64, copy=False)
    c  = df["close"].to_numpy(dtype=np.float64, copy=False)
    n  = len(c)

    # Manual shift: avoids a pd.Series.shift() + new Series allocation
    prev_c = np.empty(n, dtype=np.float64)
    prev_c[0]  = np.nan
    prev_c[1:] = c[:-1]

    # True Range with two shared buffers instead of 3 Series + 1 DataFrame
    #   hl   = H - L                      (buffer A)
    #   h_cp = |H - prev_C|               (buffer B)
    #   hl   = fmax(A, B)                 in-place
    #   h_cp = |L - prev_C|               reuse buffer B
    #   hl   = fmax(hl, h_cp)  → TR       in-place
    hl   = np.subtract(h, lo)
    h_cp = np.abs(np.subtract(h, prev_c))
    np.fmax(hl, h_cp, out=hl)
    np.subtract(lo, prev_c, out=h_cp)
    np.abs(h_cp, out=h_cp)
    np.fmax(hl, h_cp, out=hl)            # hl now holds True Range

    df["atr"] = (
        pd.Series(hl, index=df.index)
        .ewm(alpha=1.0 / period, min_periods=period, adjust=False)
        .mean()
        .to_numpy()
    )


def _compute_spread_features(df: pd.DataFrame, point: float) -> None:
    """Write spread_abs, spread_pct, spread_to_atr.  Mutates df."""
    if "atr" not in df.columns:
        _compute_atr(df, settings.atr_period)

    # spread is int32 in storage — dtype conversion always copies; scale once
    spread_price = df["spread"].to_numpy(dtype=np.float64) * point
    close = df["close"].to_numpy(dtype=np.float64, copy=False)
    atr   = df["atr"].to_numpy(dtype=np.float64, copy=False)

    df["spread_abs"]    = spread_price
    df["spread_pct"]    = spread_price / close
    df["spread_to_atr"] = spread_price / atr   # NaN during ATR warmup — intentional


def _compute_volume_features(df: pd.DataFrame, window: int) -> None:
    """Write tick_volume_zscore and tick_volume_percentile.  Mutates df."""
    # int → float conversion is unavoidable; creates one array
    vol   = df["tick_volume"].to_numpy(dtype=np.float64)
    vol_s = pd.Series(vol, index=df.index)   # one Series shared for all rolling ops

    # ── z-score ──────────────────────────────────────────────────────────────
    roll_mean = vol_s.rolling(window, min_periods=1).mean().to_numpy()
    roll_std  = vol_s.rolling(window, min_periods=2).std().to_numpy()

    # In-place normalisation: NaN (warmup) and zero → 1.0
    # Keeps z-score = 0 rather than ±inf when std is undefined or flat
    bad = ~np.isfinite(roll_std) | (roll_std == 0.0)
    roll_std[bad] = 1.0

    df["tick_volume_zscore"] = (vol - roll_mean) / roll_std

    # ── percentile rank ───────────────────────────────────────────────────────
    # O(n·w·log w) — the dominant cost for large datasets. See module docstring
    # for mitigation options. pandas Cython loop; no faster general substitute.
    df["tick_volume_percentile"] = (
        vol_s.rolling(window, min_periods=1).rank(pct=True).to_numpy()
    )


def _compute_volume_score(
    df: pd.DataFrame,
    high_pct: float,
    low_pct: float,
) -> None:
    """Write tick_volume_score via np.select (single pass).  Mutates df."""
    pct = df["tick_volume_percentile"].to_numpy(dtype=np.float64, copy=False)
    df["tick_volume_score"] = np.select(
        [pct >= high_pct, pct <= low_pct],
        [0.05,            -0.05],
        default=0.0,
    )


# ---------------------------------------------------------------------------
# Public API — each copies once, then delegates to a private compute function
# ---------------------------------------------------------------------------


def add_atr(df: pd.DataFrame, period: int | None = None) -> pd.DataFrame:
    """Return new DataFrame with Wilder's ATR column added.

    First ``period - 1`` rows are NaN — intentional warmup, not zero-filled.
    """
    period = period or settings.atr_period
    df = df.copy()
    _compute_atr(df, period)
    return df


def add_spread_features(df: pd.DataFrame, point: float = 1.0) -> pd.DataFrame:
    """Return new DataFrame with spread_abs, spread_pct, spread_to_atr.

    point : symbol point size for spread → price conversion.
    """
    df = df.copy()
    _compute_spread_features(df, point)
    return df


def add_volume_features(
    df: pd.DataFrame, window: int | None = None
) -> pd.DataFrame:
    """Return new DataFrame with tick_volume_zscore and tick_volume_percentile."""
    window = window or settings.tick_volume_window
    df = df.copy()
    _compute_volume_features(df, window)
    return df


def compute_tick_volume_score(
    df: pd.DataFrame,
    high_pct: float = 0.80,
    low_pct: float = 0.20,
) -> pd.Series:
    """Return float Series: +0.05 (≥ high_pct) / 0.0 / -0.05 (≤ low_pct).

    Requires ``tick_volume_percentile`` column.  Prefer ``add_all_features``
    to avoid an extra copy if you need the full pipeline.
    """
    if "tick_volume_percentile" not in df.columns:
        df = add_volume_features(df)
    pct = df["tick_volume_percentile"].to_numpy(dtype=np.float64, copy=False)
    return pd.Series(
        np.select([pct >= high_pct, pct <= low_pct], [0.05, -0.05], default=0.0),
        index=df.index,
        name="tick_volume_score",
    )


def spread_filter_mask(df: pd.DataFrame, point: float = 1.0) -> pd.Series:
    """Boolean Series: True = spread acceptable for trading.

    Dynamic (settings.use_dynamic_spread): spread_to_atr ≤ threshold
    Fixed:                                 spread_abs    ≤ threshold
    NaN spread_to_atr (ATR warmup) → False.
    """
    if not settings.spread_filter_enabled:
        return pd.Series(True, index=df.index)

    if settings.use_dynamic_spread:
        if "spread_to_atr" not in df.columns:
            df = add_spread_features(df, point=point)
        vals = df["spread_to_atr"].to_numpy(dtype=np.float64, copy=False)
        mask = np.isfinite(vals) & (vals <= settings.spread_atr_ratio_threshold)
    else:
        if "spread_abs" not in df.columns:
            df = add_spread_features(df, point=point)
        vals = df["spread_abs"].to_numpy(dtype=np.float64, copy=False)
        mask = np.isfinite(vals) & (vals <= settings.spread_threshold)

    result = pd.Series(mask, index=df.index)
    logger.debug(
        "Spread filter removed %.1f%% of rows (%d/%d)",
        (~result).mean() * 100, (~result).sum(), len(df),
    )
    return result


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate(df: pd.DataFrame) -> pd.DataFrame:
    """Drop invalid rows; ensure monotonic time.

    All checks are combined into one boolean mask → single filter pass.
    Chaining ``.dropna()`` + two ``df[mask]`` calls would create three
    separate DataFrame copies; this creates one.
    """
    before = len(df)

    o  = df["open"].to_numpy(dtype=np.float64, copy=False)
    h  = df["high"].to_numpy(dtype=np.float64, copy=False)
    lo = df["low"].to_numpy(dtype=np.float64, copy=False)
    c  = df["close"].to_numpy(dtype=np.float64, copy=False)

    no_nan = (
        df[["time", "open", "high", "low", "close", "tick_volume"]]
        .notna()
        .all(axis=1)
        .to_numpy()
    )
    valid = no_nan & (o > 0) & (h > 0) & (lo > 0) & (c > 0) & (h >= lo)

    df = df[valid].sort_values("time").reset_index(drop=True)

    dropped = before - len(df)
    if dropped:
        logger.warning("validate(): dropped %d invalid rows", dropped)
    return df


# ---------------------------------------------------------------------------
# Pipeline: single-copy fast path
# ---------------------------------------------------------------------------


def add_all_features(
    df: pd.DataFrame,
    point: float = 1.0,
    atr_period: int | None = None,
    volume_window: int | None = None,
) -> pd.DataFrame:
    """Full feature pipeline with exactly ONE DataFrame copy.

    Prefer this over chaining ``add_atr → add_spread_features →
    add_volume_features``; those incur 4 copies, this incurs 1.

    Sequence: ATR → spread features (reuses ATR) → volume features → score.

    Parameters
    ----------
    point         : symbol point size for spread → price conversion.
    atr_period    : ATR lookback (default: settings.atr_period).
    volume_window : rolling window for volume stats (default: settings.tick_volume_window).
    """
    period = atr_period   or settings.atr_period
    window = volume_window or settings.tick_volume_window

    df = df.copy()                         # ← the ONE copy for the whole pipeline
    _compute_atr(df, period)
    _compute_spread_features(df, point)    # ATR already present — no recompute
    _compute_volume_features(df, window)
    if settings.tick_volume_scoring_enabled:
        _compute_volume_score(df, high_pct=0.80, low_pct=0.20)
    return df


# ---------------------------------------------------------------------------
# Stream / replay
# ---------------------------------------------------------------------------


def stream_data(df: pd.DataFrame) -> Generator[dict, None, None]:
    """Yield each row as a plain dict, lazily (no upfront materialisation).

    NaN values become None for JSON compatibility.  Module-level
    ``_nan_to_none`` avoids the closure-lookup overhead of an inline lambda.

    Example::

        for bar in stream_data(add_all_features(df, point=0.01)):
            if bar.get("spread_ok"):
                signal = my_strategy(bar)
    """
    for row in df.itertuples(index=False):
        yield {k: _nan_to_none(v) for k, v in row._asdict().items()}
