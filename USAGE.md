# MT5 Bridge — Hướng dẫn sử dụng

MT5 Bridge là một REST API service dùng để tải, lưu trữ và truy vấn dữ liệu OHLC từ MetaTrader 5, kèm theo bộ tính toán feature engineering (ATR, spread, tick volume) phục vụ backtest và phân tích định lượng.

---

## Mục lục

1. [Yêu cầu hệ thống](#1-yêu-cầu-hệ-thống)
2. [Cài đặt](#2-cài-đặt)
3. [Cấu hình](#3-cấu-hình)
4. [Khởi động server](#4-khởi-động-server)
5. [API Endpoints](#5-api-endpoints)
   - [Health check](#health-check)
   - [Symbols](#symbols)
   - [Download dữ liệu từ MT5](#download-dữ-liệu-từ-mt5)
   - [Truy vấn lịch sử](#truy-vấn-lịch-sử)
   - [Replay (SSE stream)](#replay-sse-stream)
   - [DuckDB query](#duckdb-query)
6. [Feature Engineering](#6-feature-engineering)
7. [Cấu trúc lưu trữ Parquet](#7-cấu-trúc-lưu-trữ-parquet)
8. [Sử dụng trực tiếp trong Python](#8-sử-dụng-trực-tiếp-trong-python)
9. [Chạy tests](#9-chạy-tests)
10. [Offline mode (không cần MT5)](#10-offline-mode-không-cần-mt5)

---

## 1. Yêu cầu hệ thống

| Thành phần            | Yêu cầu                                             |
| --------------------- | --------------------------------------------------- |
| Python                | >= 3.10                                             |
| MetaTrader 5 terminal | Chỉ cần trên Windows khi tải dữ liệu live           |
| Hệ điều hành          | Windows (full), Linux/macOS (offline/backtest mode) |

---

## 2. Cài đặt

```bash
# Clone repo
git clone <repo-url>
cd mt5-bridge

# Tạo virtual environment
python -m venv .venv
source .venv/bin/activate        # Linux/macOS
.venv\Scripts\activate           # Windows

# Cài dependencies
pip install -r requirements.txt

# Trên Windows — bỏ comment dòng MetaTrader5 trong requirements.txt, rồi:
pip install MetaTrader5>=5.0.45
```

---

## 3. Cấu hình

Tạo file `.env` ở thư mục gốc. Tất cả biến đều có giá trị mặc định — chỉ cần khai báo những gì cần thay đổi.

```ini
# .env

# --- Kết nối MT5 (Windows) ---
MT5_LOGIN=123456
MT5_PASSWORD=your_password
MT5_SERVER=YourBroker-Live
MT5_PATH=C:\Program Files\MetaTrader 5\terminal64.exe   # tuỳ chọn

# --- Lưu trữ ---
DATA_PATH=data/mt5          # thư mục chứa file Parquet

# --- API Server ---
API_HOST=0.0.0.0
API_PORT=8000
DEBUG=false

# --- Cache (số lượng DataFrame giữ trong RAM) ---
CACHE_MAX_SIZE=50

# --- Download ---
MAX_RETRIES=3
RETRY_DELAY_SECONDS=2.0

# --- Feature Engineering ---
ATR_PERIOD=14
TICK_VOLUME_WINDOW=100
TICK_VOLUME_SCORING_ENABLED=true

# --- Spread filter ---
SPREAD_FILTER_ENABLED=true
USE_DYNAMIC_SPREAD=true           # true = dùng ATR ratio; false = fixed threshold
SPREAD_ATR_RATIO_THRESHOLD=0.3    # dynamic mode: spread/ATR <= 0.3
SPREAD_THRESHOLD=3.0              # fixed mode: spread (price units) <= 3.0

# --- DuckDB ---
DUCKDB_ENABLED=true
```

### Timeframe hợp lệ

`M1 M2 M3 M4 M5 M6 M10 M12 M15 M20 M30 H1 H2 H3 H4 H6 H8 H12 D1 W1 MN1`

---

## 4. Khởi động server

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

Truy cập Swagger UI tại: `http://localhost:8000/docs`

---

## 5. API Endpoints

### Health check

Kiểm tra trạng thái kết nối MT5 và thông tin storage.

```
GET /health
```

**Response:**

```json
{
  "status": "ok",
  "mt5_connected": true,
  "mt5_version": "5.0.3815",
  "available_symbols_cached": 3,
  "storage_path": "/data/mt5"
}
```

`status` có thể là `"ok"` (MT5 đang kết nối) hoặc `"degraded"` (MT5 không kết nối nhưng có thể đọc data local).

---

### Symbols

**Liệt kê symbols có data trên disk:**

```
GET /symbols?source=local
```

**Liệt kê tất cả symbols từ MT5 terminal:**

```
GET /symbols?source=mt5
```

**Lấy thông tin chi tiết một symbol:**

```
GET /symbols/XAUUSD
```

```json
{
  "name": "XAUUSD",
  "description": "Gold vs US Dollar",
  "currency_base": "XAU",
  "currency_profit": "USD",
  "digits": 2,
  "trade_contract_size": 100.0
}
```

---

### Download dữ liệu từ MT5

Tải dữ liệu OHLC về và lưu vào Parquet. Tự động bỏ qua dữ liệu đã có (incremental update).

**Tải một symbol:**

```
POST /download
Content-Type: application/json

{
  "symbol": "XAUUSD",
  "timeframe": "M5",
  "from_date": "2025-12-10T19:00:00Z",
  "to_date": "2026-02-10T19:00:00Z"
}
```

```json
{
  "symbol": "XAUUSD",
  "timeframe": "M5",
  "rows_downloaded": 105120,
  "rows_new": 12480,
  "from_date": "2024-01-01T00:00:00Z",
  "to_date": "2024-12-31T23:59:59Z",
  "duration_seconds": 3.42
}
```

**Tải nhiều symbols song song:**

```
POST /download/multi
Content-Type: application/json

{
  "symbols": ["XAUUSD", "EURUSD", "USDJPY"],
  "timeframe": "H1",
  "from_date": "2024-01-01T00:00:00Z"
}
```

```json
{
  "results": [
    { "symbol": "XAUUSD", "status": "ok", "rows_new": 8760 },
    { "symbol": "EURUSD", "status": "ok", "rows_new": 8760 },
    { "symbol": "USDJPY", "status": "error", "error": "No data returned" }
  ]
}
```

---

### Truy vấn lịch sử

**Lấy OHLC thuần:**

```
GET /history?symbol=XAUUSD&timeframe=M5&from=2024-06-01T00:00:00Z&to=2024-06-07T00:00:00Z
```

**Lấy OHLC kèm feature engineering:**

```
GET /history?symbol=XAUUSD&timeframe=M5&from=2024-06-01T00:00:00Z&to=2024-06-07T00:00:00Z&include_features=true&point=0.01
```

> **`point`** là giá trị point của symbol lấy từ MT5 `symbol_info().point`:
>
> - XAUUSD: `0.01`
> - EURUSD: `0.00001`
> - USDJPY: `0.001`

**Response (include_features=true):**

```json
{
  "symbol": "XAUUSD",
  "timeframe": "M5",
  "from_": "2024-06-01T00:00:00Z",
  "to": "2024-06-07T00:00:00Z",
  "count": 1440,
  "data": [
    {
      "time": "2024-06-01T00:00:00+00:00",
      "open": 2327.45,
      "high": 2329.1,
      "low": 2326.8,
      "close": 2328.55,
      "volume": 342,
      "spread": 18,
      "atr": 4.23,
      "spread_abs": 0.18,
      "spread_pct": 0.0000773,
      "spread_to_atr": 0.0426,
      "tick_volume_zscore": 1.24,
      "tick_volume_percentile": 0.82,
      "tick_volume_score": 0.05,
      "spread_ok": true
    }
  ]
}
```

**Giải thích các feature fields:**

| Field                    | Ý nghĩa                                                             |
| ------------------------ | ------------------------------------------------------------------- |
| `atr`                    | Wilder's ATR (period = `ATR_PERIOD`). NaN trong `period-1` bars đầu |
| `spread_abs`             | Spread quy đổi sang đơn vị giá: `spread_points × point`             |
| `spread_pct`             | Spread tương đối: `spread_abs / close`                              |
| `spread_to_atr`          | Spread so với ATR: `spread_abs / atr`                               |
| `tick_volume_zscore`     | Z-score tick volume trong rolling window                            |
| `tick_volume_percentile` | Percentile rank tick volume (0–1) trong rolling window              |
| `tick_volume_score`      | `+0.05` nếu ≥ 80th pct, `-0.05` nếu ≤ 20th pct, `0.0` còn lại       |
| `spread_ok`              | `true` = spread chấp nhận được theo config filter                   |

---

### Replay (SSE stream)

Stream từng bar theo thời gian thực để mô phỏng live feed cho backtest.

```
GET /replay?symbol=XAUUSD&timeframe=M5&from=2024-01-01T00:00:00Z&to=2024-01-02T00:00:00Z&speed=0
```

- **`speed=0`**: instant replay (không delay)
- **`speed=0.5`**: 0.5 giây giữa mỗi bar

**Response** — Server-Sent Events:

```
data: {"time": "2024-01-01T00:00:00+00:00", "open": 2065.12, "high": 2067.30, "low": 2064.50, "close": 2066.80, "tick_volume": 289, "spread": 15}

data: {"time": "2024-01-01T00:05:00+00:00", ...}

data: {"done": true}
```

**Ví dụ kết nối bằng Python:**

```python
import httpx

with httpx.stream("GET", "http://localhost:8000/replay", params={
    "symbol": "XAUUSD",
    "timeframe": "M5",
    "from": "2024-01-01T00:00:00Z",
    "speed": 0,
}) as r:
    for line in r.iter_lines():
        if line.startswith("data: "):
            bar = json.loads(line[6:])
            if bar.get("done"):
                break
            process(bar)
```

---

### DuckDB query

Chạy SQL trực tiếp trên các file Parquet (cần `DUCKDB_ENABLED=true`).

```
POST /query?symbol=XAUUSD&timeframe=H1&from=2024-01-01T00:00:00Z&extra_sql=LIMIT 10
```

`extra_sql` được append sau mệnh đề `WHERE` — dùng để `GROUP BY`, `ORDER BY`, `LIMIT`, v.v.

**Ví dụ tính daily VWAP:**

```
POST /query?symbol=XAUUSD&timeframe=M5&from=2024-06-01T00:00:00Z
  &extra_sql=GROUP BY strftime(time, '%Y-%m-%d') ORDER BY 1
```

---

## 6. Feature Engineering

Có thể dùng module `feature_engineering` độc lập trong Python, không cần chạy API server.

```python
from app.feature_engineering import (
    add_all_features,
    add_atr,
    add_spread_features,
    add_volume_features,
    spread_filter_mask,
    validate,
    stream_data,
)

# Load data (ví dụ từ Parquet)
from app.storage import storage
import pandas as pd

df = storage.load("XAUUSD", "M5",
    from_dt=pd.Timestamp("2024-01-01", tz="UTC"),
    to_dt=pd.Timestamp("2024-06-01", tz="UTC"),
)

# Validate: bỏ rows lỗi, đảm bảo sort theo time
df = validate(df)

# Tính tất cả features — chỉ 1 lần copy DataFrame
df = add_all_features(df, point=0.01)

# Hoặc tính từng feature riêng lẻ (mỗi call copy 1 lần)
df = add_atr(df, period=14)
df = add_spread_features(df, point=0.01)
df = add_volume_features(df, window=100)

# Spread filter mask
mask = spread_filter_mask(df, point=0.01)
df_filtered = df[mask]

# Stream từng bar dưới dạng dict (lazy, không materialise toàn bộ)
for bar in stream_data(df):
    print(bar["time"], bar["tick_volume_score"])
```

### Lưu ý ATR warmup

`add_atr(period=14)` → 13 rows đầu tiên có `atr=NaN`. Đây là hành vi cố ý — không zero-fill.
Tương tự `spread_to_atr` sẽ `NaN` trong thời gian warmup.

### Performance

| Pipeline                | 1M rows |
| ----------------------- | ------- |
| `add_atr()`             | ~19ms   |
| `add_spread_features()` | ~23ms   |
| `add_volume_features()` | ~400ms  |
| `add_all_features()`    | ~430ms  |

Bottleneck duy nhất là `rolling().rank(pct=True)` cho `tick_volume_percentile` — O(n·w·log w).
Nếu không cần percentile, tắt bằng cách dùng `tick_volume_zscore` thay thế.

---

## 7. Cấu trúc lưu trữ Parquet

```
data/mt5/
├── XAUUSD/
│   ├── M5/
│   │   ├── 2023.parquet
│   │   ├── 2024.parquet
│   │   └── 2025.parquet
│   └── H1/
│       └── 2024.parquet
└── EURUSD/
    └── H1/
        └── 2024.parquet
```

**Schema mỗi file Parquet:**

| Column        | Type                 | Mô tả                                         |
| ------------- | -------------------- | --------------------------------------------- |
| `time`        | `timestamp[us, UTC]` | Thời điểm mở bar                              |
| `open`        | `float64`            | Giá mở cửa                                    |
| `high`        | `float64`            | Giá cao nhất                                  |
| `low`         | `float64`            | Giá thấp nhất                                 |
| `close`       | `float64`            | Giá đóng cửa                                  |
| `tick_volume` | `int64`              | Số ticks trong bar (liquidity proxy từ MT5)   |
| `spread`      | `int32`              | Spread raw tính bằng points                   |
| `atr`         | `float64`            | ATR (NaN cho raw data; điền khi lưu features) |

- Nén: **Snappy**
- Partition: **1 file per year**
- Deduplication: tự động khi merge — không có duplicate timestamps
- Backward compatible: đọc được file cũ thiếu `spread`, `tick_volume`, `atr`

---

## 8. Sử dụng trực tiếp trong Python

Dùng `DataService` và `ParquetStorage` mà không cần API server:

```python
from app.data_service import data_service
from app.storage import storage
import pandas as pd
from datetime import datetime, timezone

# Download từ MT5 (cần chạy trên Windows có MT5)
result = data_service.download(
    symbol="XAUUSD",
    timeframe="M5",
    from_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
)
print(f"Tải {result['rows_new']} rows mới")

# Load từ disk
df = data_service.get_history("XAUUSD", "M5",
    from_dt=datetime(2024, 1, 1, tzinfo=timezone.utc),
    to_dt=datetime(2024, 6, 1, tzinfo=timezone.utc),
)

# Load kèm features
df = data_service.get_history_with_features("XAUUSD", "M5",
    from_dt=datetime(2024, 1, 1, tzinfo=timezone.utc),
    to_dt=datetime(2024, 6, 1, tzinfo=timezone.utc),
    point=0.01,
)

# Kiểm tra symbols có trên disk
print(storage.list_symbols())           # ['EURUSD', 'USDJPY', 'XAUUSD']
print(storage.list_timeframes("XAUUSD")) # ['H1', 'M5']
```

---

## 9. Chạy tests

```bash
# Chạy tất cả tests
pytest tests/ -v

# Chỉ chạy feature engineering tests
pytest tests/test_feature_engineering.py -v

# Chỉ chạy storage tests
pytest tests/test_storage.py -v

# Chạy nhanh (dừng ở test đầu tiên fail)
pytest tests/ -x -q
```

Toàn bộ test suite chạy **offline** — không cần MT5 terminal.

---

## 10. Offline mode (không cần MT5)

Trên Linux/macOS hoặc khi không có MT5 terminal, service vẫn chạy được với data đã lưu trên disk:

- `GET /health` trả về `"status": "degraded"` — bình thường
- `GET /history` hoạt động đầy đủ từ Parquet
- `GET /replay` hoạt động đầy đủ
- `POST /download` sẽ fail — cần MT5

Để test offline, copy thư mục `data/mt5/` từ máy Windows sang, sau đó:

```bash
uvicorn app.main:app --port 8000
```

Tất cả endpoint đọc dữ liệu đều hoạt động bình thường.
