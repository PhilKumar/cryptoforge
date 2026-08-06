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

import json
import logging
import time
from typing import Dict, List, Sequence

from executor.orders import Candle

_log = logging.getLogger("cascade.executor.market")

BINANCE_PUBLIC = "https://api.binance.com"
COINDCX_PUBLIC = "https://public.coindcx.com"

# The four the top strip quotes, in the order it draws them. Fixed on purpose:
# the strip used to show whatever coins this machine happened to be working,
# so it changed shape with every campaign and was never a reference for
# anything. These four are the reference; what the machine is working is said
# on the cell itself.
STRIP_SYMBOLS = ("BTCUSDT", "ETHUSDT", "PAXGUSDT", "SOLUSDT")

# Decoration refreshes on its own clock, not the trading tick's. At one call
# per half minute this is a rounding error against either venue's budget, and
# the tick keeps its latency for the work that moves money.
STRIP_REFRESH_SEC = 30.0

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

    def ticker_24h(self, symbols: Sequence[str]) -> Dict[str, dict]:
        """
        Last price and 24-hour change for several symbols, in ONE request.

        Quoted from the buyer's own venue like everything else on their page:
        a CoinDCX buyer reading Binance's BTC would be reading a price they
        cannot trade. A symbol the venue does not list is simply absent from
        the answer — an empty cell is honest, an invented one is not.
        """
        wanted = [str(symbol).upper() for symbol in symbols]
        out: Dict[str, dict] = {}
        if self._exchange == "coindcx":
            rows = self._http(f"{COINDCX_PUBLIC}/exchange/ticker", None) or []
            for row in rows:
                symbol = str(row.get("market") or "").upper()
                if symbol in wanted:
                    out[symbol] = {
                        "price": float(row.get("last_price") or 0.0),
                        "change_pct": float(row.get("change_24_hour") or 0.0),
                    }
            return out
        rows = (
            self._http(
                f"{BINANCE_PUBLIC}/api/v3/ticker/24hr",
                {"symbols": json.dumps(wanted, separators=(",", ":"))},
            )
            or []
        )
        for row in rows:
            symbol = str(row.get("symbol") or "").upper()
            if symbol in wanted:
                out[symbol] = {
                    "price": float(row.get("lastPrice") or 0.0),
                    "change_pct": float(row.get("priceChangePercent") or 0.0),
                }
        return out

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


class MarketStrip:
    """
    The four quotes at the top of the buyer's page, cached.

    Two rules, both because this is the only thing on the page that is NOT
    about their money:

    - It never raises. It is called from the trading tick, and the class of
      bug that has bitten this codebase three times is a venue call that
      abandons the work after it. A strip that cannot refresh keeps its last
      figures rather than taking the tick down with it.
    - It never blanks. Losing the venue for one call should dim nothing; the
      buyer sees the last price it knew and the clock beside it tells them
      the machine is alive.
    """

    def __init__(
        self,
        market,
        *,
        symbols: Sequence[str] = STRIP_SYMBOLS,
        refresh_sec: float = STRIP_REFRESH_SEC,
        clock=time.time,
    ):
        self._market = market
        self._symbols = tuple(symbols)
        self._refresh_sec = float(refresh_sec)
        self._clock = clock
        self._rows: Dict[str, dict] = {}
        self._fetched_at = 0.0
        self._at = 0

    def snapshot(self) -> dict:
        now = float(self._clock())
        if now - self._fetched_at >= self._refresh_sec:
            # Stamped before the call, not after: a venue that hangs or fails
            # waits out the same interval as one that answers, instead of
            # being retried on every tick.
            self._fetched_at = now
            try:
                fresh = self._market.ticker_24h(self._symbols)
                if fresh:
                    self._rows.update(fresh)
                    self._at = int(now)
            except Exception as exc:
                _log.debug("ticker strip unavailable: %s", exc)
        return {"symbols": list(self._symbols), "rows": dict(self._rows), "at": self._at}


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
