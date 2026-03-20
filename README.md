# MT5 Data Service

A production-ready FastAPI service that connects to MetaTrader 5, downloads
historical OHLC data, stores it in partitioned Parquet files, and exposes a
clean REST API for backtesting and analytics.

---

## Architecture

```
MetaTrader 5 Terminal
        │
        ▼ MetaTrader5 package (Windows only)
  mt5_client.py          ← safe connection management, retry logic
        │
        ▼
  data_service.py        ← orchestration: incremental updates, multi-symbol,
        │                   replay generator, DuckDB queries
        ├──▶ storage.py  ← Parquet I/O, LRU cache, dedup/sort
        │        data/mt5/{SYMBOL}/{TIMEFRAME}/{YEAR}.parquet
        │
        ▼
  main.py (FastAPI)      ← REST API endpoints
```

---

## Setup

### 1. Clone & install

```bash
git clone <repo>
cd mt5-data-service
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

On **Windows** (where MT5 runs), also install the MT5 package:

```bash
pip install MetaTrader5
```

### 2. Configure

```bash
cp .env.example .env
# Edit .env with your MT5 credentials and paths
```

Key settings in `.env`:

| Variable | Description | Default |
|---|---|---|
| `MT5_LOGIN` | MT5 account number | (empty — uses open terminal) |
| `MT5_PASSWORD` | MT5 account password | |
| `MT5_SERVER` | Broker server name | |
| `MT5_PATH` | Path to `terminal64.exe` | (auto-detect) |
| `DATA_PATH` | Parquet storage root | `data/mt5` |
| `DEFAULT_SYMBOLS` | Symbols to download | `["XAUUSD","EURUSD","USDJPY"]` |
| `DUCKDB_ENABLED` | Enable DuckDB queries | `true` |

### 3. Start MT5 terminal

Make sure MetaTrader 5 is running on the **same machine** (it must be open for
the Python package to connect). Auto-trading must be enabled.

### 4. Run the service

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

Interactive docs: http://localhost:8000/docs

---

## API Reference

### `GET /health`
Check MT5 connection and service status.

```bash
curl http://localhost:8000/health
```
```json
{
  "status": "ok",
  "mt5_connected": true,
  "mt5_version": "5.0.3815",
  "available_symbols_cached": 3,
  "storage_path": "/app/data/mt5"
}
```

---

### `GET /symbols`
List available symbols.

```bash
# Symbols with local data
curl "http://localhost:8000/symbols?source=local"

# All symbols from MT5 terminal
curl "http://localhost:8000/symbols?source=mt5"
```

---

### `POST /download`
Download data from MT5 and persist to Parquet. Supports **incremental updates**
— only fetches bars newer than the latest stored bar.

```bash
curl -X POST http://localhost:8000/download \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "XAUUSD",
    "timeframe": "M5",
    "from_date": "2020-01-01T00:00:00Z",
    "to_date": "2024-01-01T00:00:00Z"
  }'
```
```json
{
  "symbol": "XAUUSD",
  "timeframe": "M5",
  "rows_downloaded": 287340,
  "rows_new": 12480,
  "from_date": "2023-11-15T08:00:00Z",
  "to_date": "2024-01-01T00:00:00Z",
  "duration_seconds": 4.2
}
```

---

### `POST /download/multi`
Download multiple symbols concurrently.

```bash
curl -X POST http://localhost:8000/download/multi \
  -H "Content-Type: application/json" \
  -d '{
    "symbols": ["XAUUSD", "EURUSD", "USDJPY"],
    "timeframe": "H1",
    "from_date": "2022-01-01T00:00:00Z"
  }'
```

---

### `GET /history`
Query stored OHLC data.

```bash
curl "http://localhost:8000/history?symbol=XAUUSD&timeframe=H1&from=2023-01-01T00:00:00Z&to=2023-06-01T00:00:00Z"
```
```json
{
  "symbol": "XAUUSD",
  "timeframe": "H1",
  "from_": "2023-01-01T00:00:00Z",
  "to": "2023-06-01T00:00:00Z",
  "count": 3624,
  "data": [
    {"time": "2023-01-02T00:00:00+00:00", "open": 1823.4, "high": 1828.1, "low": 1820.5, "close": 1826.2, "volume": 4312, "spread": 3},
    ...
  ]
}
```

---

### `GET /replay`
Server-Sent Events stream that replays stored bars one-by-one for backtesting.

```bash
curl -N "http://localhost:8000/replay?symbol=XAUUSD&timeframe=M5&from=2023-01-02T00:00:00Z&to=2023-01-03T00:00:00Z&speed=0"
```

```
data: {'time': '2023-01-02T00:00:00+00:00', 'open': 1823.4, 'high': 1824.1, 'low': 1822.8, 'close': 1823.9, 'volume': 312}
data: {'time': '2023-01-02T00:05:00+00:00', 'open': 1823.9, ...}
...
data: {"done": true}
```

Python client example:
```python
import httpx

with httpx.stream("GET", "http://localhost:8000/replay", params={
    "symbol": "XAUUSD", "timeframe": "M5",
    "from": "2023-01-02T00:00:00Z",
    "to": "2023-01-03T00:00:00Z",
    "speed": 0
}) as r:
    for line in r.iter_lines():
        if line.startswith("data:"):
            bar = eval(line[5:].strip())
            # your strategy logic here
            print(bar)
```

---

### `POST /query`
Run DuckDB SQL against stored Parquet files.

```bash
curl -X POST "http://localhost:8000/query?symbol=XAUUSD&timeframe=H1&from=2023-01-01T00:00:00Z&extra_sql=LIMIT+10"
```

---

## CLI Tool

```bash
# Single symbol, full history
python scripts/download_full_history.py --symbol XAUUSD --timeframe M5

# Date range
python scripts/download_full_history.py \
  --symbol EURUSD --timeframe H1 \
  --from 2020-01-01 --to 2023-12-31

# Multiple symbols concurrently
python scripts/download_full_history.py \
  --symbols XAUUSD EURUSD USDJPY GBPUSD \
  --timeframe M15 --from 2022-01-01

# All configured symbols
python scripts/download_full_history.py --all-symbols --timeframe D1
```

---

## Storage Layout

```
data/mt5/
  XAUUSD/
    M5/
      2020.parquet
      2021.parquet
      2022.parquet
      2023.parquet
    H1/
      2020.parquet
      ...
  EURUSD/
    H1/
      ...
```

Each `.parquet` file contains one year of OHLC data, compressed with Snappy,
with columns: `time (UTC), open, high, low, close, volume, spread`.

### Direct DuckDB queries

```python
import duckdb

conn = duckdb.connect()
df = conn.execute("""
    SELECT
        time_bucket(INTERVAL '1 hour', time) AS hour,
        first(open ORDER BY time) AS open,
        max(high) AS high,
        min(low) AS low,
        last(close ORDER BY time) AS close,
        sum(volume) AS volume
    FROM read_parquet('data/mt5/XAUUSD/M5/*.parquet')
    WHERE time BETWEEN '2023-01-01' AND '2023-12-31'
    GROUP BY 1
    ORDER BY 1
""").df()
```

---

## Running Tests

```bash
pytest tests/ -v
```

Tests run fully offline (no MT5 terminal needed) using mocks and a temporary
filesystem.

---

## Backtesting Integration

The `/replay` endpoint or `data_service.replay()` generator can feed any
backtesting framework bar-by-bar:

```python
import asyncio
from app.data_service import data_service
from datetime import datetime, timezone

async def run_backtest():
    strategy_position = None
    async for bar in data_service._replay_gen(
        symbol="XAUUSD",
        timeframe="M5",
        from_dt=datetime(2023, 1, 1, tzinfo=timezone.utc),
        to_dt=datetime(2023, 12, 31, tzinfo=timezone.utc),
        speed=0,  # instant
    ):
        close = bar["close"]
        # Your strategy logic here
        print(f"{bar['time']} close={close}")

asyncio.run(run_backtest())
```

---

## Performance Notes

- **LRU cache**: per-year Parquet files are cached in memory (default: 50 files).
  Adjust `CACHE_MAX_SIZE` in `.env` based on available RAM.
- **Parquet + Snappy**: ~10x smaller than CSV, column-oriented reads are fast.
- **DuckDB**: executes SQL directly on Parquet without loading into pandas first
  — ideal for large aggregation queries.
- **Incremental updates**: `POST /download` only fetches bars newer than the
  latest stored timestamp, so repeated calls are cheap.
- **Multi-symbol parallel downloads**: `POST /download/multi` uses
  `asyncio.gather` to download symbols concurrently.
