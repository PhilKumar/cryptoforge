# ============================================================
#  CryptoForge — Configuration
#  Load credentials from .env file (NEVER hardcode them!)
# ============================================================

import os
from urllib.parse import urlparse

from dotenv import load_dotenv

load_dotenv()

# ── Delta Exchange API Credentials ──────────────────────────
DELTA_API_KEY = os.getenv("DELTA_API_KEY", "YOUR_API_KEY_HERE")
DELTA_API_SECRET = os.getenv("DELTA_API_SECRET", "YOUR_API_SECRET_HERE")

# ── CoinDCX API Credentials ─────────────────────────────────
COINDCX_API_KEY = os.getenv("COINDCX_API_KEY", "YOUR_COINDCX_API_KEY_HERE")
COINDCX_API_SECRET = os.getenv("COINDCX_API_SECRET", "YOUR_COINDCX_API_SECRET_HERE")


def _env_url(name: str, default: str) -> str:
    raw = (os.getenv(name) or "").strip().rstrip("/")
    parsed = urlparse(raw)
    if not raw or parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return default
    return raw


COINDCX_BASE_URL = _env_url("COINDCX_BASE_URL", "https://api.coindcx.com")
COINDCX_PUBLIC_URL = _env_url("COINDCX_PUBLIC_URL", "https://public.coindcx.com")
COINDCX_MARGIN_CURRENCY = (os.getenv("COINDCX_MARGIN_CURRENCY") or "USDT").upper()

# ── Binance Spot API Credentials ────────────────────────────
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "YOUR_BINANCE_API_KEY_HERE")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "YOUR_BINANCE_API_SECRET_HERE")
BINANCE_TESTNET_API_KEY = os.getenv("BINANCE_TESTNET_API_KEY", "")
BINANCE_TESTNET_API_SECRET = os.getenv("BINANCE_TESTNET_API_SECRET", "")
BINANCE_SPOT_TESTNET = os.getenv("BINANCE_SPOT_TESTNET", "false").lower() == "true"


def binance_spot_credentials(testnet: bool) -> tuple[str, str]:
    """Pick the Binance key pair that matches the environment we point at.

    Testnet keys are issued by testnet.binance.vision and are not valid on
    mainnet; mainnet keys are not valid on the testnet. Falling back from one
    to the other can only ever produce a -2015 auth error, so the two sets
    stay strictly separate and a missing testnet key reads as "not
    configured" instead of a confusing rejection at order time. Enabling the
    testnet therefore never touches the live keys stored in .env.
    """
    if testnet:
        return (
            os.getenv("BINANCE_TESTNET_API_KEY", ""),
            os.getenv("BINANCE_TESTNET_API_SECRET", ""),
        )
    return (
        os.getenv("BINANCE_SPOT_API_KEY") or os.getenv("BINANCE_API_KEY", "YOUR_BINANCE_API_KEY_HERE"),
        os.getenv("BINANCE_SPOT_API_SECRET") or os.getenv("BINANCE_API_SECRET", "YOUR_BINANCE_API_SECRET_HERE"),
    )


BINANCE_SPOT_API_KEY, BINANCE_SPOT_API_SECRET = binance_spot_credentials(BINANCE_SPOT_TESTNET)
BINANCE_SPOT_BASE_URL = _env_url(
    "BINANCE_SPOT_BASE_URL",
    "https://testnet.binance.vision" if BINANCE_SPOT_TESTNET else "https://api.binance.com",
)
BINANCE_SPOT_QUOTE_ASSET = (os.getenv("BINANCE_SPOT_QUOTE_ASSET") or "USDT").upper()

# ── Active Broker ────────────────────────────────────────────
CRYPTOFORGE_BROKER = os.getenv("CRYPTOFORGE_BROKER", os.getenv("BROKER", "binance")).lower()

# ── Delta Exchange Base URLs ────────────────────────────────
# Testnet toggle: set DELTA_TESTNET=true to use Delta testnet
# Same API signatures — just swap keys and this flag
DELTA_TESTNET = os.getenv("DELTA_TESTNET", "false").lower() == "true"
DELTA_REGION = os.getenv("DELTA_REGION", "india").lower()  # 'india' or 'global'

if DELTA_TESTNET:
    DELTA_BASE_URL = "https://testnet-api.delta.exchange/v2"
    DELTA_WS_URL = "wss://testnet-socket.delta.exchange"
elif DELTA_REGION == "global":
    DELTA_BASE_URL = "https://api.delta.exchange/v2"
    DELTA_WS_URL = "wss://socket.delta.exchange"
else:
    DELTA_BASE_URL = "https://api.india.delta.exchange/v2"
    DELTA_WS_URL = "wss://socket.india.delta.exchange"

# ── Database (optional — for bulk candle storage) ───────────
# Set USE_TIMESCALEDB=true and DATABASE_URL to enable
DATABASE_URL = os.getenv("DATABASE_URL", "")
USE_TIMESCALEDB = os.getenv("USE_TIMESCALEDB", "false").lower() == "true"

# ── App Settings ────────────────────────────────────────────
APP_HOST = os.getenv("APP_HOST", "127.0.0.1")
APP_PORT = int(os.getenv("APP_PORT", "9000"))
DEBUG = os.getenv("DEBUG", "false").lower() == "true"

# ── Backtest Defaults ───────────────────────────────────────
DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_FROM = "2025-01-01"
DEFAULT_TO = "2026-03-01"
DEFAULT_CAPITAL = 10000  # $10,000 USDT

# ── Live Engine Settings ────────────────────────────────────
POLL_INTERVAL_SEC = 30  # check conditions every 30 seconds
MAX_TRADES_PER_DAY = 5  # crypto is 24/7
LIQUIDATION_THRESHOLD = -90  # leveraged PnL% at which a position is considered liquidated
MARKET_OPEN = "00:00"
MARKET_CLOSE = "23:59"

# ── Indicator Defaults ──────────────────────────────────────
SUPERTREND_PERIOD = 10
SUPERTREND_MULTIPLIER = 3.0
EMA_PERIOD = 20
RSI_PERIOD = 14
BBANDS_PERIOD = 20
BBANDS_STD = 2.0
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

# ── Tradable symbols shown in the market table ─────────────
# Six, despite the historical name — it was TOP_25_CRYPTOS when this tracked a
# longer Delta perps list, and "only 6 of 25 symbols resolve" is a reading of
# the name, not a symbol-normalisation gap. All six resolve on Binance spot
# exactly as written; verified against /api/v3/ticker/24hr.
# TOP_25_CRYPTOS stays as an alias so nothing outside this file has to change.
TRADABLE_SYMBOLS = [
    {"symbol": "BTCUSDT", "name": "Bitcoin", "ticker": "BTC", "icon": "₿"},
    {"symbol": "ETHUSDT", "name": "Ethereum", "ticker": "ETH", "icon": "Ξ"},
    {"symbol": "SOLUSDT", "name": "Solana", "ticker": "SOL", "icon": "◎"},
    {"symbol": "XRPUSDT", "name": "Ripple", "ticker": "XRP", "icon": "✕"},
    {"symbol": "DOGEUSDT", "name": "Dogecoin", "ticker": "DOGE", "icon": "Ð"},
    {"symbol": "PAXGUSDT", "name": "PAX Gold", "ticker": "PAXG", "icon": "🥇"},
]

TOP_25_CRYPTOS = TRADABLE_SYMBOLS  # legacy name, same list

# Spot pairs for extra market data display
SPOT_PAIRS = [
    {"symbol": "BTC_USDT", "name": "Bitcoin", "ticker": "BTC", "icon": "₿"},
    {"symbol": "ETH_USDT", "name": "Ethereum", "ticker": "ETH", "icon": "Ξ"},
    {"symbol": "SOL_USDT", "name": "Solana", "ticker": "SOL", "icon": "◎"},
    {"symbol": "XRP_USDT", "name": "Ripple", "ticker": "XRP", "icon": "✕"},
]
