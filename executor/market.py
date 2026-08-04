"""
executor/market.py — the buyer's own candles.

They trade their exchange's prices, so they read their exchange's candles. Ours
are what the geometry was DRAWN on, which is a different question — and the
reason `exchange` rides on `campaign.opened` at all.

Public endpoints only: no signing, no credentials, nothing that needs an
account. That keeps the one place credentials are used down to a single file.

Like the rest of `executor/`, this must not import `engine.cascade`.
"""

from __future__ import annotations

import logging
from typing import List

from executor.orders import Candle

_log = logging.getLogger("cascade.executor.market")

BINANCE_PUBLIC = "https://api.binance.com"
COINDCX_PUBLIC = "https://public.coindcx.com"

# Binance takes "5m"; CoinDCX's candle API takes the same words, so the map is
# identity today and exists so a venue that disagrees has somewhere to say so.
_INTERVAL = {"1m": "1m", "5m": "5m", "15m": "15m", "1h": "1h", "4h": "4h", "1d": "1d"}


class ExchangeMarketData:
    """Candles and last price from the venue the buyer actually trades."""

    def __init__(self, adapter, exchange: str, *, http=None):
        self._adapter = adapter
        self._exchange = str(exchange or "binance").lower()
        self._http = http or _get_json

    def last_price(self, symbol: str) -> float:
        if self._exchange == "coindcx":
            rows = self._http(f"{COINDCX_PUBLIC}/exchange/ticker", None) or []
            for row in rows:
                if str(row.get("market") or "").upper() == symbol.upper():
                    return float(row.get("last_price") or 0.0)
            return 0.0
        data = self._http(f"{BINANCE_PUBLIC}/api/v3/ticker/price", {"symbol": symbol}) or {}
        return float(data.get("price") or 0.0)

    def closed_candles_since(self, symbol: str, timeframe: str, since_ts: int) -> List[Candle]:
        """
        CLOSED candles only.

        The last bar an exchange returns is the one still forming, and acting on
        it means acting on a close that has not happened — a red candle that
        turns green before the bar ends would arm a stop that should never have
        existed. It is dropped here rather than in five callers.
        """
        interval = _INTERVAL.get(str(timeframe).lower(), "5m")
        if self._exchange == "coindcx":
            rows = (
                self._http(
                    f"{COINDCX_PUBLIC}/market_data/candles",
                    {"pair": _coindcx_pair(symbol), "interval": interval, "limit": 200},
                )
                or []
            )
            candles = [
                Candle(
                    timestamp=int(row["time"]) // 1000,
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                )
                for row in rows
            ]
            candles.sort(key=lambda candle: candle.timestamp)
        else:
            rows = (
                self._http(
                    f"{BINANCE_PUBLIC}/api/v3/klines",
                    {"symbol": symbol, "interval": interval, "limit": 200},
                )
                or []
            )
            candles = [
                Candle(
                    timestamp=int(row[0]) // 1000,
                    open=float(row[1]),
                    high=float(row[2]),
                    low=float(row[3]),
                    close=float(row[4]),
                )
                for row in rows
            ]
        return [candle for candle in candles[:-1] if candle.timestamp > since_ts]


def _coindcx_pair(symbol: str) -> str:
    """CoinDCX's candle API wants its own pair name, not the market name."""
    upper = symbol.upper()
    if upper.endswith("USDT"):
        return f"B-{upper[:-4]}_USDT"
    return upper


def _get_json(url: str, params):
    import requests

    response = requests.get(url, params=params, timeout=15)
    response.raise_for_status()
    return response.json()
