"""Integration tests for the FastAPI endpoints (no real MT5 required).

Uses httpx.AsyncClient + ASGITransport which works with httpx >= 0.20.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import pandas as pd
import pytest
import httpx

from app.main import app
from app.storage import storage


@pytest.fixture(scope="module")
def anyio_backend():
    return "asyncio"


@pytest.fixture(scope="module")
async def ac():
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client


@pytest.fixture
def sample_df():
    times = pd.date_range("2023-06-01", periods=5, freq="1h", tz="UTC")
    return pd.DataFrame(
        {
            "time": times,
            "open": 1900.0,
            "high": 1905.0,
            "low": 1895.0,
            "close": 1902.0,
            "tick_volume": 200,
            "spread": 5,
        }
    )


# ── Health ──────────────────────────────────────────────────────────────────

@pytest.mark.anyio
async def test_health_returns_200(ac):
    resp = await ac.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert "status" in data
    assert "mt5_connected" in data


@pytest.mark.anyio
async def test_health_has_expected_fields(ac):
    resp = await ac.get("/health")
    data = resp.json()
    assert data["status"] in ("ok", "degraded", "error")
    assert isinstance(data["mt5_connected"], bool)
    assert "storage_path" in data


# ── Symbols ─────────────────────────────────────────────────────────────────

@pytest.mark.anyio
async def test_list_local_symbols_returns_list(ac):
    resp = await ac.get("/symbols?source=local")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


@pytest.mark.anyio
async def test_list_mt5_symbols_503_when_not_connected(ac):
    from app.mt5_client import MT5Error

    with patch("app.main.mt5_client.get_symbols") as mock_sym:
        mock_sym.side_effect = MT5Error("not connected")
        resp = await ac.get("/symbols?source=mt5")
    assert resp.status_code == 503


# ── History ──────────────────────────────────────────────────────────────────

@pytest.mark.anyio
async def test_empty_history_returns_200(ac):
    resp = await ac.get(
        "/history",
        params={
            "symbol": "ZZZNONE",
            "timeframe": "M5",
            "from": "2023-01-01T00:00:00Z",
            "to": "2023-01-02T00:00:00Z",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 0
    assert data["data"] == []


@pytest.mark.anyio
async def test_history_with_stored_data(ac, sample_df):
    storage.save(sample_df, "TESTHIST", "H1")
    resp = await ac.get(
        "/history",
        params={
            "symbol": "TESTHIST",
            "timeframe": "H1",
            "from": "2023-06-01T00:00:00Z",
            "to": "2023-06-02T00:00:00Z",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 5
    bar = data["data"][0]
    assert "time" in bar
    assert "open" in bar
    assert "close" in bar


@pytest.mark.anyio
async def test_history_missing_required_params(ac):
    resp = await ac.get("/history", params={"symbol": "XAUUSD"})
    assert resp.status_code == 422


# ── Download ─────────────────────────────────────────────────────────────────

@pytest.mark.anyio
async def test_download_calls_data_service(ac):
    with patch("app.main.data_service.download") as mock_dl:
        mock_dl.return_value = {
            "rows_downloaded": 100,
            "rows_new": 100,
            "from_date": datetime(2023, 1, 1, tzinfo=timezone.utc),
            "to_date": datetime(2023, 1, 2, tzinfo=timezone.utc),
            "duration_seconds": 0.5,
        }
        resp = await ac.post(
            "/download",
            json={
                "symbol": "XAUUSD",
                "timeframe": "M5",
                "from_date": "2023-01-01T00:00:00Z",
                "to_date": "2023-01-02T00:00:00Z",
            },
        )
    assert resp.status_code == 200
    data = resp.json()
    assert data["rows_downloaded"] == 100
    assert data["rows_new"] == 100


@pytest.mark.anyio
async def test_download_invalid_timeframe_returns_422(ac):
    resp = await ac.post(
        "/download",
        json={
            "symbol": "XAUUSD",
            "timeframe": "INVALID",
            "from_date": "2023-01-01T00:00:00Z",
        },
    )
    assert resp.status_code == 422


@pytest.mark.anyio
async def test_download_mt5_error_returns_503(ac):
    from app.mt5_client import MT5Error

    with patch("app.main.data_service.download") as mock_dl:
        mock_dl.side_effect = MT5Error("terminal not running")
        resp = await ac.post(
            "/download",
            json={
                "symbol": "XAUUSD",
                "timeframe": "H1",
                "from_date": "2023-01-01T00:00:00Z",
            },
        )
    assert resp.status_code == 503
    assert "terminal not running" in resp.json()["detail"]
