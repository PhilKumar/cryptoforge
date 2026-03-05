# ============================================================
#  CryptoForge — Configuration
#  Load credentials from .env file (NEVER hardcode them!)
# ============================================================

import os
from dotenv import load_dotenv

load_dotenv()

# ── Delta Exchange API Credentials ──────────────────────────
DELTA_API_KEY    = os.getenv('DELTA_API_KEY', 'YOUR_API_KEY_HERE')
DELTA_API_SECRET = os.getenv('DELTA_API_SECRET', 'YOUR_API_SECRET_HERE')

# ── Delta Exchange Base URLs ────────────────────────────────
# Testnet toggle: set DELTA_TESTNET=true to use Delta testnet
# Same API signatures — just swap keys and this flag
DELTA_TESTNET = os.getenv('DELTA_TESTNET', 'false').lower() == 'true'
DELTA_REGION  = os.getenv('DELTA_REGION', 'india').lower()  # 'india' or 'global'

if DELTA_TESTNET:
    DELTA_BASE_URL = 'https://testnet-api.delta.exchange/v2'
    DELTA_WS_URL   = 'wss://testnet-socket.delta.exchange'
elif DELTA_REGION == 'global':
    DELTA_BASE_URL = 'https://api.delta.exchange/v2'
    DELTA_WS_URL   = 'wss://socket.delta.exchange'
else:
    DELTA_BASE_URL = 'https://api.india.delta.exchange/v2'
    DELTA_WS_URL   = 'wss://socket.india.delta.exchange'

# ── Database (optional — for bulk candle storage) ───────────
# Set USE_TIMESCALEDB=true and DATABASE_URL to enable
DATABASE_URL     = os.getenv('DATABASE_URL', '')
USE_TIMESCALEDB  = os.getenv('USE_TIMESCALEDB', 'false').lower() == 'true'

# ── App Settings ────────────────────────────────────────────
APP_HOST = os.getenv('APP_HOST', '127.0.0.1')
APP_PORT = int(os.getenv('APP_PORT', '9000'))
DEBUG    = os.getenv('DEBUG', 'false').lower() == 'true'

# ── Backtest Defaults ───────────────────────────────────────
DEFAULT_SYMBOL  = "BTCUSDT"
DEFAULT_FROM    = "2025-01-01"
DEFAULT_TO      = "2026-03-01"
DEFAULT_CAPITAL = 10000   # $10,000 USDT

# ── Live Engine Settings ────────────────────────────────────
POLL_INTERVAL_SEC  = 30     # check conditions every 30 seconds
MAX_TRADES_PER_DAY = 5      # crypto is 24/7
MARKET_OPEN        = "00:00"
MARKET_CLOSE       = "23:59"

# ── Indicator Defaults ──────────────────────────────────────
SUPERTREND_PERIOD     = 10
SUPERTREND_MULTIPLIER = 3.0
EMA_PERIOD            = 20
RSI_PERIOD            = 14
BBANDS_PERIOD         = 20
BBANDS_STD            = 2.0
MACD_FAST             = 12
MACD_SLOW             = 26
MACD_SIGNAL           = 9

# ── Delta Exchange Available Perpetual Futures ─────────────
# These are the ACTUAL live perps on Delta Exchange (USDT-settled)
TOP_25_CRYPTOS = [
    {"symbol": "BTCUSDT",    "name": "Bitcoin",    "ticker": "BTC",    "icon": "₿"},
    {"symbol": "ETHUSDT",    "name": "Ethereum",   "ticker": "ETH",    "icon": "Ξ"},
    {"symbol": "SOLUSDT",    "name": "Solana",     "ticker": "SOL",    "icon": "◎"},
    {"symbol": "XRPUSDT",    "name": "Ripple",     "ticker": "XRP",    "icon": "✕"},
    {"symbol": "DOGEUSDT",   "name": "Dogecoin",   "ticker": "DOGE",   "icon": "Ð"},
    {"symbol": "PAXGUSDT",   "name": "PAX Gold",   "ticker": "PAXG",   "icon": "🥇"},
]

# Spot pairs for extra market data display
SPOT_PAIRS = [
    {"symbol": "BTC_USDT",   "name": "Bitcoin",    "ticker": "BTC",    "icon": "₿"},
    {"symbol": "ETH_USDT",   "name": "Ethereum",   "ticker": "ETH",    "icon": "Ξ"},
    {"symbol": "SOL_USDT",   "name": "Solana",     "ticker": "SOL",    "icon": "◎"},
    {"symbol": "XRP_USDT",   "name": "Ripple",     "ticker": "XRP",    "icon": "✕"},
]
