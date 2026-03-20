"""Unit tests for MT5Client (offline mode — no real MT5 needed)."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from app.mt5_client import MT5Client, MT5ConnectionError, MT5DataError, MT5Error, _backoff


def make_rates(n: int = 5):
    """Return a structured numpy array that mimics mt5.copy_rates_range output."""
    import numpy as np

    times = np.arange(
        int(pd.Timestamp("2023-01-01", tz="UTC").timestamp()),
        int(pd.Timestamp("2023-01-01", tz="UTC").timestamp()) + n * 300,
        300,
        dtype=np.int64,
    )
    dtype = np.dtype(
        [
            ("time", np.int64),
            ("open", np.float64),
            ("high", np.float64),
            ("low", np.float64),
            ("close", np.float64),
            ("tick_volume", np.int64),
            ("spread", np.int32),
            ("real_volume", np.int64),
        ]
    )
    rates = np.zeros(n, dtype=dtype)
    rates["time"] = times
    rates["open"] = 1800.0
    rates["high"] = 1805.0
    rates["low"] = 1795.0
    rates["close"] = 1802.0
    rates["tick_volume"] = 100
    rates["spread"] = 3
    return rates


class TestMT5ClientOffline:
    def test_is_connected_false_when_not_initialized(self):
        client = MT5Client()
        assert not client.is_connected

    def test_resolve_timeframe_valid(self):
        assert MT5Client._resolve_timeframe("M5") == 5
        assert MT5Client._resolve_timeframe("H1") == 16385
        assert MT5Client._resolve_timeframe("D1") == 16408

    def test_resolve_timeframe_invalid(self):
        with pytest.raises(MT5Error, match="Unknown timeframe"):
            MT5Client._resolve_timeframe("X999")

    def test_rates_to_df_empty_raises_data_error(self):
        with pytest.raises(MT5DataError):
            MT5Client._rates_to_df(None, "XAUUSD", "M5")

    def test_rates_to_df_conversion(self):
        rates = make_rates(5)
        with patch("app.mt5_client.MT5_AVAILABLE", True):
            df = MT5Client._rates_to_df(rates, "XAUUSD", "M5")
        assert len(df) == 5
        assert "time" in df.columns
        assert pd.api.types.is_datetime64_any_dtype(df["time"])
        assert df["time"].dt.tz is not None  # UTC-aware
        assert df["time"].is_monotonic_increasing

    def test_rates_to_df_column_rename(self):
        rates = make_rates(3)
        with patch("app.mt5_client.MT5_AVAILABLE", True):
            df = MT5Client._rates_to_df(rates, "XAUUSD", "H1")
        assert "tick_volume" in df.columns
        assert "volume" not in df.columns

    @patch("app.mt5_client.MT5_AVAILABLE", True)
    @patch("app.mt5_client.mt5")
    def test_initialize_success(self, mock_mt5):
        mock_mt5.initialize.return_value = True
        mock_mt5.terminal_info.return_value = MagicMock(build=3815, connected=True)
        mock_mt5.version.return_value = (5, 0, 3815)

        client = MT5Client()
        result = client.initialize()
        assert result is True
        assert client._initialized is True

    @patch("app.mt5_client.MT5_AVAILABLE", True)
    @patch("app.mt5_client.mt5")
    def test_initialize_failure(self, mock_mt5):
        mock_mt5.initialize.return_value = False
        mock_mt5.last_error.return_value = (-10004, "No IPC connection")

        client = MT5Client()
        result = client.initialize()
        assert result is False
        assert client._initialized is False

    @patch("app.mt5_client.MT5_AVAILABLE", True)
    @patch("app.mt5_client.mt5")
    def test_fetch_with_retry_success_first_attempt(self, mock_mt5):
        mock_mt5.terminal_info.return_value = MagicMock(connected=True)
        mock_mt5.copy_rates_range.return_value = make_rates(10)

        client = MT5Client()
        client._initialized = True

        from datetime import timezone
        from_dt = pd.Timestamp("2023-01-01", tz="UTC").to_pydatetime()
        to_dt = pd.Timestamp("2023-01-02", tz="UTC").to_pydatetime()

        df = client.fetch_with_retry("XAUUSD", "M5", from_dt, to_dt, retries=3)
        assert len(df) == 10
        assert mock_mt5.copy_rates_range.call_count == 1

    @patch("app.mt5_client.MT5_AVAILABLE", True)
    @patch("app.mt5_client.mt5")
    @patch("app.mt5_client.time.sleep")
    def test_fetch_with_retry_retries_on_failure(self, mock_sleep, mock_mt5):
        mock_mt5.terminal_info.return_value = MagicMock(connected=True)
        # Fail twice, succeed on 3rd
        mock_mt5.copy_rates_range.side_effect = [
            Exception("timeout"),
            Exception("timeout"),
            make_rates(5),
        ]

        client = MT5Client()
        client._initialized = True

        from_dt = pd.Timestamp("2023-01-01", tz="UTC").to_pydatetime()
        to_dt = pd.Timestamp("2023-01-02", tz="UTC").to_pydatetime()

        df = client.fetch_with_retry("XAUUSD", "M5", from_dt, to_dt, retries=3)
        assert len(df) == 5
        assert mock_mt5.copy_rates_range.call_count == 3

    @patch("app.mt5_client.MT5_AVAILABLE", True)
    @patch("app.mt5_client.mt5")
    @patch("app.mt5_client.time.sleep")
    def test_fetch_exhausts_retries(self, mock_sleep, mock_mt5):
        mock_mt5.terminal_info.return_value = MagicMock(connected=True)
        mock_mt5.copy_rates_range.side_effect = Exception("always fail")

        client = MT5Client()
        client._initialized = True

        from_dt = pd.Timestamp("2023-01-01", tz="UTC").to_pydatetime()
        to_dt = pd.Timestamp("2023-01-02", tz="UTC").to_pydatetime()

        with pytest.raises(MT5Error, match="All 2 fetch attempts failed"):
            client.fetch_with_retry("XAUUSD", "M5", from_dt, to_dt, retries=2)

    @patch("app.mt5_client.MT5_AVAILABLE", True)
    @patch("app.mt5_client.mt5")
    @patch("app.mt5_client.time.sleep")
    def test_connection_error_exits_retry_loop_immediately(self, mock_sleep, mock_mt5):
        """MT5ConnectionError must not be swallowed by the fetch retry loop."""
        mock_mt5.terminal_info.return_value = MagicMock(connected=False)
        mock_mt5.initialize.return_value = False
        mock_mt5.last_error.return_value = (-10004, "No IPC")

        client = MT5Client()
        client._initialized = False

        from_dt = pd.Timestamp("2023-01-01", tz="UTC").to_pydatetime()
        to_dt = pd.Timestamp("2023-01-02", tz="UTC").to_pydatetime()

        with pytest.raises(MT5ConnectionError):
            client.fetch_with_retry("XAUUSD", "M5", from_dt, to_dt, retries=5)

        # Should have tried to reconnect (max_retries times) but not burned all 5 fetch slots
        assert mock_mt5.copy_rates_range.call_count == 0

    @patch("app.mt5_client.MT5_AVAILABLE", True)
    @patch("app.mt5_client.mt5")
    @patch("app.mt5_client.time.sleep")
    def test_reconnect_succeeds_on_second_attempt(self, mock_sleep, mock_mt5):
        mock_mt5.initialize.side_effect = [False, True]
        mock_mt5.last_error.return_value = (-1, "transient")
        mock_mt5.terminal_info.return_value = MagicMock(build=3815, connected=True, path="/mt5")
        mock_mt5.version.return_value = (5, 0, 3815)

        client = MT5Client()
        client.reconnect(max_attempts=3, base_delay=0.01)

        assert client._initialized is True
        assert mock_mt5.initialize.call_count == 2

    @patch("app.mt5_client.MT5_AVAILABLE", True)
    @patch("app.mt5_client.mt5")
    @patch("app.mt5_client.time.sleep")
    def test_reconnect_raises_after_all_attempts_fail(self, mock_sleep, mock_mt5):
        mock_mt5.initialize.return_value = False
        mock_mt5.last_error.return_value = (-10004, "No IPC")

        client = MT5Client()
        with pytest.raises(MT5ConnectionError, match="3 attempts"):
            client.reconnect(max_attempts=3, base_delay=0.01)

        assert mock_mt5.initialize.call_count == 3

    @patch("app.mt5_client.MT5_AVAILABLE", True)
    @patch("app.mt5_client.mt5")
    @patch("app.mt5_client.time.sleep")
    def test_reconnect_shuts_down_before_reinit(self, mock_sleep, mock_mt5):
        mock_mt5.initialize.side_effect = [False, True]
        mock_mt5.last_error.return_value = (-1, "err")
        mock_mt5.terminal_info.return_value = MagicMock(build=0, connected=True, path="")
        mock_mt5.version.return_value = (5, 0, 0)

        client = MT5Client()
        client._initialized = True  # simulate stale connection
        client.reconnect(max_attempts=2, base_delay=0.01)

        mock_mt5.shutdown.assert_called_once()

    def test_backoff_grows_exponentially(self):
        """Each attempt should produce a larger base delay."""
        import random
        random.seed(0)
        d1 = _backoff(1, base=1.0)
        random.seed(0)
        d2 = _backoff(2, base=1.0)
        random.seed(0)
        d3 = _backoff(3, base=1.0)
        assert d1 < d2 < d3

    def test_backoff_capped_at_max_delay(self):
        delay = _backoff(100, base=2.0, max_delay=10.0)
        # With ±20 % jitter the result should still be near the cap
        assert delay <= 12.0  # cap + max jitter
