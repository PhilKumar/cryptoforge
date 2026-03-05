# CryptoForge — Production Architecture Document

## System Overview

CryptoForge is a **production-grade backtesting and live-trading platform** for Delta Exchange crypto perpetual futures (BTCUSDT, ETHUSDT, SOLUSDT, etc.). Designed for **24/7 unattended operation**.

```
┌─────────────────────────────────────────────────────────────────────┐
│                        CryptoForge Platform                         │
├──────────────┬──────────────┬──────────────┬───────────────────────┤
│  Strategy    │  Backtest    │  Paper       │  Live                 │
│  Builder UI  │  Engine      │  Trading     │  Trading              │
│  (HTML/JS)   │  (event loop)│  (sim orders)│  (real orders)        │
├──────────────┴──────────────┴──────────────┴───────────────────────┤
│                      FastAPI Backend (app.py)                       │
│  REST API  ·  WebSocket Hub  ·  Auth  ·  Rate Limiting             │
├──────────────┬──────────────┬──────────────┬───────────────────────┤
│  Delta       │  Data        │  WebSocket   │  Indicator            │
│  Broker API  │  Downloader  │  Feed Mgr    │  Engine               │
│  (REST+WS)   │  (async)     │  (auto-recon)│  (numpy)              │
├──────────────┴──────────────┴──────────────┴───────────────────────┤
│              Data Layer: PostgreSQL/TimescaleDB                     │
│              (optional — falls back to in-memory pandas)            │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 1. Data Engineering Pipeline

### Design Goals
- Download **5 years** of 1m, 3m, 5m OHLCV candles from Delta Exchange
- Handle the **2000-candle per request** API limit with smart pagination
- Respect API rate limits (10 req/sec recommended safe rate)
- Store in **PostgreSQL/TimescaleDB** for fast range queries
- Falls back gracefully to **Binance public API** when Delta has no data

### Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────────────┐
│ Delta REST   │────▶│   Async      │────▶│  TimescaleDB         │
│ /v2/history/ │     │   Paginator  │     │  candles_1m          │
│  candles     │     │   (aiohttp)  │     │  candles_3m          │
├──────────────┤     │              │     │  candles_5m          │
│ Binance      │────▶│  Rate Limiter│     │  (hypertable,        │
│ /api/v3/     │     │  (semaphore) │     │   auto-compression)  │
│  klines      │     └──────────────┘     └──────────────────────┘
└──────────────┘
```

### Key Implementation Details
- **aiohttp.ClientSession** — connection pooling, 60s timeout
- **asyncio.Semaphore(8)** — max 8 concurrent requests
- **Exponential backoff** — 429/5xx → wait 2^n seconds (max 60s)
- **Gap detection** — identify & fill missing candle ranges
- **Incremental sync** — only fetch new candles after initial bulk load
- **Module**: `engine/data_downloader.py`

### Data Schema (TimescaleDB)

```sql
CREATE TABLE candles (
    time        TIMESTAMPTZ   NOT NULL,
    symbol      TEXT          NOT NULL,
    resolution  TEXT          NOT NULL,   -- '1m', '3m', '5m'
    open        DOUBLE PRECISION,
    high        DOUBLE PRECISION,
    low         DOUBLE PRECISION,
    close       DOUBLE PRECISION,
    volume      DOUBLE PRECISION
);

-- Convert to hypertable for time-series optimization
SELECT create_hypertable('candles', 'time');

-- Composite index for fast symbol+resolution queries
CREATE INDEX idx_candles_sym_res ON candles (symbol, resolution, time DESC);

-- Enable compression for old data (> 7 days)
ALTER TABLE candles SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, resolution'
);
SELECT add_compression_policy('candles', INTERVAL '7 days');
```

---

## 2. Backtesting Engine

### Architecture (Enhanced Event Loop)

The existing backtest engine uses a **forward-walk loop** over candle data. This is the correct approach for crypto perpetuals where we need to:
- Track leveraged P&L with proper margin math
- Check SL/TP at every candle
- Apply maker/taker fees per side
- Detect liquidation events

### Fee Schedule (Delta Exchange)
| Type | Fee |
|------|-----|
| Maker | 0.02% |
| Taker (market orders) | 0.05% |
| Funding | Variable, every 8h |

### Enhanced Metrics
- **Sharpe Ratio** — Risk-adjusted return: `mean(daily_returns) / std(daily_returns) * sqrt(365)`
- **Calmar Ratio** — `annualized_return / max_drawdown`
- **Max Drawdown** — Peak-to-trough equity decline
- **Win Rate** — Percentage of profitable trades
- **Profit Factor** — Gross profit / gross loss
- **Average Trade Duration** — Time in position
- **Expectancy** — `(win_rate × avg_win) - (loss_rate × avg_loss)`

---

## 3. Execution Engine

### Testnet ↔ Live Toggle

```
DELTA_TESTNET=true   → https://testnet-api.delta.exchange/v2
DELTA_TESTNET=false  → https://api.india.delta.exchange/v2
```

Single environment variable swap. Both environments use identical API signatures.

### Order Flow

```
Signal Detected
     │
     ▼
┌─────────────────────┐
│  Pre-Flight Checks  │  ← sufficient margin? position limit? rate limit?
│  Validate Symbol    │
│  Check Max Daily    │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  Place Market Order  │  ← POST /v2/orders
│  (with retry logic)  │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  Verify Fill         │  ← GET /v2/orders/{id}  (poll 3x, 2s apart)
│  Confirm Position    │  ← GET /v2/positions
│  Log to event_log    │
└─────────────────────┘
```

### Redundant Order Verification
After placing any order, the system:
1. Waits 2 seconds
2. Queries `GET /v2/orders/{order_id}` to confirm fill
3. Cross-checks against `GET /v2/positions` for position size
4. Retries up to 3 times if fill not confirmed
5. Logs discrepancy if position size doesn't match expected

---

## 4. Reliability & 24/7 Uptime

### WebSocket Feed Manager (`engine/ws_feed.py`)

```
┌──────────────────────┐
│  DeltaWSFeed         │
│                      │
│  connect()           │  ← wss://socket.delta.exchange
│  subscribe(channels) │  ← candlestick, ticker, orders
│  auto_reconnect()    │  ← exponential backoff: 1s, 2s, 4s, 8s, 16s, 32s cap
│  heartbeat()         │  ← ping every 25s (Delta timeout = 30s)
│  on_message(cb)      │  ← dispatch to engine
└──────────────────────┘
```

### Error Recovery Matrix

| Failure | Detection | Recovery |
|---------|----------|----------|
| WebSocket disconnect | `on_close` callback | Auto-reconnect with backoff |
| API timeout (>30s) | `asyncio.timeout` | Retry 3x, then alert |
| 429 Rate Limit | HTTP status code | Wait `Retry-After` header, else 60s |
| 5xx Server Error | HTTP status code | Exponential backoff, max 60s |
| Invalid signature | 401 response | Re-generate timestamp, retry once |
| Network partition | Connection refused | Reconnect with backoff, persist state |
| Process crash | systemd watchdog | Auto-restart via `Restart=always` |
| Stale state | Date check on load | Discard state from previous day |

### Systemd Service (24/7)

```ini
[Unit]
Description=CryptoForge Trading Platform
After=network.target postgresql.service

[Service]
User=ec2-user
WorkingDirectory=/home/ec2-user/cryptoforge
ExecStart=/home/ec2-user/cryptoforge/venv/bin/uvicorn app:app --host 0.0.0.0 --port 9000
Restart=always
RestartSec=5
WatchdogSec=120

[Install]
WantedBy=multi-user.target
```

---

## 5. Module Map

```
CryptoForge/
├── app.py                    # FastAPI backend — routes, WebSocket hub, auth
├── config.py                 # All configuration — env vars, testnet toggle
├── broker/
│   ├── delta.py              # Delta REST client + aiohttp async + order verify
│   └── __init__.py
├── engine/
│   ├── backtest.py           # Backtest engine — walk-forward, fees, Sharpe
│   ├── indicators.py         # Technical indicators — numpy-optimized
│   ├── live.py               # Live trading engine — real orders
│   ├── paper_trading.py      # Paper trading — simulated orders
│   ├── data_downloader.py    # NEW: Async bulk data download + DB storage
│   ├── ws_feed.py            # NEW: WebSocket feed with auto-reconnect
│   └── __init__.py
├── strategy.html             # Frontend SPA
├── login.html                # Auth page
├── requirements.txt          # Python dependencies
└── deploy/
    ├── cryptoforge.service   # systemd unit
    ├── nginx.conf            # Reverse proxy
    └── deploy.sh             # Deployment script
```

---

## 6. Environment Variables

```bash
# Delta Exchange
DELTA_API_KEY=your_key
DELTA_API_SECRET=your_secret
DELTA_TESTNET=false              # true = testnet, false = production
DELTA_REGION=india               # 'india' or 'global'

# Database (optional — system works without it)
DATABASE_URL=postgresql://user:pass@localhost:5432/cryptoforge
USE_TIMESCALEDB=false            # true = use DB for candle storage

# App
CRYPTOFORGE_PIN=your_secure_pin
APP_HOST=0.0.0.0
APP_PORT=9000
DEBUG=false
```
