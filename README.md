# CryptoForge

**Professional algorithmic trading platform for crypto perpetual futures.**
Backtest, paper trade, and go live on [Delta Exchange](https://www.delta.exchange/) — all from a single sleek web UI.

---

## Features

| Module | Description |
|--------|-------------|
| **Backtesting Engine** | Replay strategies against historical OHLCV data with leverage, trading fees (0.05 % per side), and liquidation simulation |
| **Paper Trading** | Simulated live trading with real-time market data — zero risk, full realism |
| **Live Trading** | Place real orders on Delta Exchange perpetual futures via authenticated REST API |
| **Strategy Builder** | Visual condition builder — combine indicators (SuperTrend, EMA, RSI, MACD, Bollinger Bands, VWAP, Stochastic RSI, CPR) with comparison operators |
| **Market Overview** | Top-25 crypto market caps from CoinGecko, live ticker strip, funding rate history |
| **Leverage Control** | Configure 1×–200× leverage per symbol with margin & liquidation estimates |
| **WebSocket Feed** | Real-time trade log streamed to the browser during live / paper sessions |

## Stack

- **Backend:** Python 3.11 · FastAPI · Uvicorn
- **Broker:** Delta Exchange REST API (HMAC-SHA256 auth) + Binance public API fallback
- **Frontend:** Single-page HTML/CSS/JS (no build step)
- **Data:** Pandas · NumPy

## Supported Instruments

| Symbol | Name | Type |
|--------|------|------|
| BTCUSDT | Bitcoin | USDT Perpetual |
| ETHUSDT | Ethereum | USDT Perpetual |
| SOLUSDT | Solana | USDT Perpetual |
| XRPUSDT | Ripple | USDT Perpetual |
| DOGEUSDT | Dogecoin | USDT Perpetual |
| PAXGUSDT | PAX Gold | USDT Perpetual |

## Quick Start

### 1. Clone & install

```bash
git clone https://github.com/YOUR_USERNAME/CryptoForge.git
cd CryptoForge
python3.11 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure

Create a `.env` file in the project root:

```env
DELTA_API_KEY=your_key_here
DELTA_API_SECRET=your_secret_here
APP_HOST=127.0.0.1
APP_PORT=9000
CRYPTOFORGE_PIN=your_pin_here
```

> Get your API credentials at [delta.exchange/app/account/api-keys](https://www.delta.exchange/app/account/api-keys)

### 3. Run

```bash
uvicorn app:app --host 127.0.0.1 --port 9000
```

Open **http://localhost:9000** and log in with your PIN.

## Project Structure

```
CryptoForge/
├── app.py                 # FastAPI backend — all API routes
├── config.py              # Configuration & environment variables
├── requirements.txt       # Python dependencies
├── login.html             # Login page
├── strategy.html          # Main trading UI (single-page app)
├── strategies.json        # Saved strategies
├── runs.json              # Backtest / paper / live run history
├── broker/
│   ├── __init__.py
│   └── delta.py           # Delta Exchange REST client + Binance fallback
├── engine/
│   ├── __init__.py
│   ├── backtest.py        # Backtesting engine with fees & liquidation
│   ├── indicators.py      # Technical indicator library
│   ├── live.py            # Live trading engine
│   └── paper_trading.py   # Paper trading engine
└── deploy/
    ├── cryptoforge.service   # systemd unit file
    ├── nginx.conf            # Nginx reverse-proxy config
    └── deploy.sh             # One-click EC2 deployment script
```

## Deployment (AWS EC2)

```bash
# SSH into your Ubuntu 22.04 instance, then:
bash deploy/deploy.sh YOUR_ELASTIC_IP
```

This installs Python 3.11, nginx, creates a virtualenv, configures systemd, and sets up the reverse proxy. Edit `/home/ubuntu/cryptoforge/.env` with your Delta Exchange credentials, then restart:

```bash
sudo systemctl restart cryptoforge
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/auth/login` | Authenticate with PIN |
| GET | `/api/auth/status` | Check session validity |
| POST | `/api/auth/logout` | End session |
| GET | `/api/symbols` | List available trading symbols |
| GET | `/api/candles/{symbol}` | Fetch OHLCV candle data |
| GET | `/api/ticker/{symbol}` | Live ticker for a symbol |
| GET | `/api/tickers/bulk` | All tickers (cached 30s) |
| GET | `/api/funding/{symbol}` | Funding rate history |
| GET | `/api/leverage/{symbol}` | Leverage & margin info |
| GET | `/api/market/top25` | CoinGecko top-25 market overview |
| GET | `/api/account/balance` | Wallet balance |
| GET | `/api/account/positions` | Open positions |
| POST | `/api/backtest` | Run a backtest |
| GET | `/api/runs` | List all saved runs |
| DELETE | `/api/runs/{rid}` | Delete a run |
| POST | `/api/live/start` | Start live trading |
| POST | `/api/live/stop` | Stop live trading |
| GET | `/api/live/status` | Live engine status |
| POST | `/api/paper/start` | Start paper trading |
| POST | `/api/paper/stop` | Stop paper trading |
| GET | `/api/paper/status` | Paper engine status |
| POST | `/api/emergency-stop` | Emergency stop all engines |
| GET | `/api/strategies` | List saved strategies |
| POST | `/api/strategies` | Save a strategy |
| DELETE | `/api/strategies/{sid}` | Delete a strategy |
| GET | `/api/health` | Health check |
| WS | `/ws` | Real-time trade log stream |

## Indicators

| Indicator | Parameters |
|-----------|------------|
| SuperTrend | period (10), multiplier (3.0) |
| EMA | period (20) |
| RSI | period (14) |
| MACD | fast (12), slow (26), signal (9) |
| Bollinger Bands | period (20), std (2.0) |
| VWAP | — (session-based) |
| Stochastic RSI | period (14) |
| ADX | period (14) |
| CPR | Central Pivot Range |

## License

Private — All rights reserved.
