"""
broker/coindcx_spot.py — CoinDCX SPOT broker adapter for CryptoForge.

Mirrors the Binance Spot contract field-for-field so Cascade and the app run
unchanged: wallet rows carry free/locked balances, orders come back shaped
like Binance order objects (status/executedQty/cummulativeQuoteQty/
clientOrderId), and stop-limit entries with client order ids work natively.

Venue quirks this adapter absorbs, all verified against the live API:
- markets_details names things backwards: their "base_currency" is the QUOTE
  (settlement) currency and "target_currency" is the coin being traded.
- Spot candles only exist at 1m/15m/1h/1d, max 1000 per call, paged with
  startTime/endTime in milliseconds. 5m/30m/4h are resampled locally.
- Private endpoints are HMAC-signed JSON POSTs (same scheme as futures).
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import sys
import time as _time
from datetime import datetime, timezone
from decimal import ROUND_DOWN, Decimal
from typing import Iterable, Optional

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config

from .base import BaseBroker
from .coindcx import _COINDCX_DEFAULT_BASE_URL, _COINDCX_DEFAULT_PUBLIC_URL, _coindcx_clean_url, _request_with_retry

_spot_log = logging.getLogger("cryptoforge.coindcx_spot")

# CoinDCX spot order states → the Binance-style states the engine reads.
_ORDER_STATUS_MAP = {
    "init": "NEW",
    "open": "NEW",
    "partial_entry": "PARTIALLY_FILLED",
    "partially_filled": "PARTIALLY_FILLED",
    "filled": "FILLED",
    "partially_cancelled": "CANCELED",
    "cancelled": "CANCELED",
    "canceled": "CANCELED",
    "rejected": "REJECTED",
}


class CoinDCXSpotClient(BaseBroker):
    broker_name = "coindcx"
    display_name = "CoinDCX Spot"
    supports_funding = False
    # Stop-limit buys with client order ids, and a free/locked spot wallet.
    supports_cascade = True
    # Twice Binance's 0.1%, and confirmed two independent ways.
    #
    # Measured: every BTCUSDT fill on the live account was charged exactly
    # 0.2000% per side. Published (coindcx.com/fees, checked 2026-08-06): the
    # "Spot (C2C)" column — crypto-to-crypto, which is what a USDT pair is —
    # bills 0.17% maker AND taker, plus 18% GST: 0.17 x 1.18 = 0.2006%.
    #
    # The C2C rate is FLAT across every fee level, Regular 1 through VIP 7, so
    # unlike their INR book it does not improve with volume and this constant
    # will not drift. (INR pairs bill 0.50% + GST = 0.59%, which matches the
    # INR fills on the same account and is why this USDT-quoted adapter must
    # never be pointed at an INR market.)
    #
    # Modelling CoinDCX at Binance's rate floors a take-profit below its own
    # commission, so every round exiting on that floor loses money.
    fee_pct_per_side = 0.2

    _SYMBOL_ALIASES = {
        **BaseBroker._SYMBOL_ALIASES,
        "BTCUSD": "BTCUSDT",
        "ETHUSD": "ETHUSDT",
        "SOLUSD": "SOLUSDT",
        "XRPUSD": "XRPUSDT",
        "DOGEUSD": "DOGEUSDT",
        "PAXGUSD": "PAXGUSDT",
    }

    # Native spot candle intervals, and how the missing ones are synthesized.
    _NATIVE_INTERVALS = {"1m": 60, "15m": 900, "1h": 3600, "1d": 86400}
    _SYNTH_INTERVALS = {
        "5m": ("1m", 5),
        "30m": ("15m", 2),
        "4h": ("1h", 4),
        "1D": ("1d", 1),
        "1w": ("1d", 7),
        "1W": ("1d", 7),
    }
    _CANDLE_DEFAULT_BARS = 1000
    _CANDLE_PAGE_LIMIT = 1000
    _CANDLE_MAX_PAGES = 6

    def __init__(self):
        self.api_key = config.COINDCX_API_KEY
        self.api_secret = config.COINDCX_API_SECRET
        self.base_url = _coindcx_clean_url(config.COINDCX_BASE_URL, _COINDCX_DEFAULT_BASE_URL)
        self.public_url = _coindcx_clean_url(config.COINDCX_PUBLIC_URL, _COINDCX_DEFAULT_PUBLIC_URL)
        self.quote_asset = str(getattr(config, "COINDCX_SPOT_QUOTE_ASSET", "USDT") or "USDT").upper()
        self._products_cache = None
        self._products_ts = 0.0
        self._CACHE_TTL = 3600
        # /exchange/ticker returns every market in one response; cache briefly
        # so per-symbol lookups don't each pull 1000+ rows from the network.
        self._ticker_cache: dict[str, dict] = {}
        self._ticker_ts = 0.0
        self._TICKER_TTL = 5
        self._history_cache = None
        self._history_ts = 0.0
        self._HISTORY_TTL = 30

    def get_market_feed_kind(self) -> str:
        return "polling"

    def _is_configured(self) -> bool:
        return (
            self.api_key != "YOUR_COINDCX_API_KEY_HERE"
            and self.api_secret != "YOUR_COINDCX_API_SECRET_HERE"
            and len(str(self.api_key or "")) > 5
            and len(str(self.api_secret or "")) > 5
        )

    # ── plumbing ──────────────────────────────────────────────────

    def _public_get(self, path: str, *, params: dict | None = None, use_public_host: bool = False):
        base = self.public_url if use_public_host else self.base_url
        resp = _request_with_retry(
            "GET", f"{base}{path}", headers={"Content-Type": "application/json"}, params=params, timeout=30
        )
        resp.raise_for_status()
        return resp.json()

    def _private_post(self, path: str, payload: dict | None = None):
        if not self._is_configured():
            raise Exception("CoinDCX API not configured")
        body = dict(payload or {})
        body.setdefault("timestamp", int(round(_time.time() * 1000)))
        json_body = json.dumps(body, separators=(",", ":"), sort_keys=False)
        signature = hmac.new(self.api_secret.encode("utf-8"), json_body.encode("utf-8"), hashlib.sha256).hexdigest()
        headers = {
            "Content-Type": "application/json",
            "X-AUTH-APIKEY": self.api_key,
            "X-AUTH-SIGNATURE": signature,
        }
        resp = _request_with_retry("POST", f"{self.base_url}{path}", headers=headers, data=json_body, timeout=30)
        if resp.status_code >= 400:
            try:
                detail = resp.json()
                message = detail.get("message") or detail.get("error") or str(detail)[:200]
            except Exception:
                message = (resp.text or "")[:200]
            raise Exception(f"CoinDCX {resp.status_code}: {message}")
        return resp.json()

    # ── symbols and products ──────────────────────────────────────

    @classmethod
    def _app_symbol_to_market(cls, symbol: str) -> str:
        raw = cls.normalize_app_symbol(symbol)
        if raw.endswith("USD") and not raw.endswith("USDT"):
            return raw + "T"
        return raw

    def to_broker_symbol(self, symbol: str) -> str:
        return self._app_symbol_to_market(symbol)

    def from_broker_symbol(self, symbol: str) -> str:
        return self.normalize_app_symbol(symbol)

    @staticmethod
    def _tick_from_precision(precision) -> str:
        digits = int(BaseBroker.coerce_float(precision, 2) or 2)
        digits = min(max(digits, 0), 12)
        return format(Decimal(1).scaleb(-digits).normalize(), "f")

    def _normalize_product(self, raw: dict) -> dict:
        # CoinDCX's "base_currency" is the settlement currency; "target" is the
        # coin. Flip them into the app's base/quote vocabulary here, once.
        market = str(raw.get("coindcx_name") or raw.get("symbol") or "")
        return {
            "id": market,
            "symbol": self.from_broker_symbol(market),
            "broker_symbol": market,
            "pair": market,
            "candle_pair": str(raw.get("pair") or ""),
            "state": "live" if str(raw.get("status")) == "active" else str(raw.get("status") or "").lower(),
            "contract_type": "spot",
            "contract_value": "1",
            "notional_type": "linear",
            "base_asset": raw.get("target_currency_short_name"),
            "quote_asset": raw.get("base_currency_short_name"),
            "quantity_precision": raw.get("target_currency_precision"),
            "quote_precision": raw.get("base_currency_precision"),
            "min_qty": raw.get("min_quantity"),
            "max_qty": raw.get("max_quantity"),
            "step_size": raw.get("step"),
            "market_min_qty": raw.get("min_quantity"),
            "market_step_size": raw.get("step"),
            "market_max_qty": raw.get("max_quantity_market"),
            "tick_size": self._tick_from_precision(raw.get("base_currency_precision")),
            "min_notional": raw.get("min_notional"),
            "quote_order_qty_market_allowed": False,
            "order_types": raw.get("order_types") or [],
            "ecode": raw.get("ecode"),
            "max_leverage": 1,
            "default_leverage": 1,
            "raw": raw,
        }

    def get_products(self, force_refresh: bool = False) -> list:
        now = _time.time()
        if self._products_cache and not force_refresh and (now - self._products_ts) < self._CACHE_TTL:
            return list(self._products_cache)
        payload = self._public_get("/exchange/v1/markets_details")
        markets = payload if isinstance(payload, list) else []
        products = [
            self._normalize_product(item)
            for item in markets
            if str(item.get("base_currency_short_name") or "").upper() == self.quote_asset
            and str(item.get("status") or "") == "active"
        ]
        self._products_cache = products
        self._products_ts = now
        return list(products)

    def get_perpetual_futures(self) -> list:
        # Name kept for the legacy engine contract; returns spot products.
        return self.get_products()

    def get_product_by_symbol(self, symbol: str):
        market = self.to_broker_symbol(symbol)
        for product in self.get_products():
            if product.get("broker_symbol") == market:
                return dict(product)
        return None

    def get_supported_symbols(self) -> set[str]:
        return {p["symbol"] for p in self.get_products() if p.get("symbol")}

    def get_leverage_info(self, symbol: str) -> dict:
        return {"max_leverage": 1, "default": 1, "options": [1], "initial_margin": 100.0, "maintenance_margin": 0.0}

    def set_leverage(self, product_id: str, leverage: int) -> dict:
        return {"status": "ok", "note": "Spot has no leverage; request ignored"}

    # ── candles ───────────────────────────────────────────────────

    def _candle_pair(self, symbol: str) -> str:
        product = self.get_product_by_symbol(symbol) or {}
        return str(product.get("candle_pair") or "")

    def _fetch_candle_page(self, pair: str, interval: str, *, end_ms: int | None, start_ms: int | None) -> list:
        params = {"pair": pair, "interval": interval, "limit": self._CANDLE_PAGE_LIMIT}
        if end_ms:
            params["endTime"] = int(end_ms)
        if start_ms:
            params["startTime"] = int(start_ms)
        payload = self._public_get("/market_data/candles", params=params, use_public_host=True)
        return payload if isinstance(payload, list) else payload.get("data", []) or []

    def get_candles(self, symbol: str, resolution: str = "5m", start: str = None, end: str = None) -> pd.DataFrame:
        pair = self._candle_pair(symbol)
        if not pair:
            return pd.DataFrame()
        if resolution in self._NATIVE_INTERVALS:
            fetch_interval, factor = resolution, 1
        else:
            fetch_interval, factor = self._SYNTH_INTERVALS.get(resolution, ("1m", 5))
        bar_seconds = self._NATIVE_INTERVALS[fetch_interval]

        end_ms = None
        if end:
            end_ms = int(datetime.strptime(end, "%Y-%m-%d").timestamp() * 1000)
        if start:
            start_ms = int(datetime.strptime(start, "%Y-%m-%d").timestamp() * 1000)
        else:
            anchor_ms = end_ms or int(_time.time() * 1000)
            start_ms = anchor_ms - bar_seconds * 1000 * self._CANDLE_DEFAULT_BARS * factor

        rows: list = []
        cursor_end = end_ms
        for _ in range(self._CANDLE_MAX_PAGES):
            page = self._fetch_candle_page(pair, fetch_interval, end_ms=cursor_end, start_ms=start_ms)
            if not page:
                break
            rows.extend(page)
            oldest = min(int(self.coerce_float(item.get("time"), 0.0)) for item in page)
            if oldest <= start_ms or len(page) < self._CANDLE_PAGE_LIMIT:
                break
            cursor_end = oldest - 1

        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        if "time" not in df.columns:
            return pd.DataFrame()
        df["datetime"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        for col in ("open", "high", "low", "close", "volume"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df.set_index("datetime", inplace=True)
        df.sort_index(inplace=True)
        df = df[~df.index.duplicated(keep="first")]
        keep = [col for col in ("open", "high", "low", "close", "volume") if col in df.columns]
        df = df[keep]

        if factor > 1:
            rule = f"{bar_seconds * factor}s"
            df = (
                df.resample(rule, label="left", closed="left")
                .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
                .dropna(subset=["open"])
            )
        return df

    async def async_get_candles(self, symbol: str, **kwargs) -> pd.DataFrame:
        return await asyncio.to_thread(self.get_candles, symbol, **kwargs)

    # ── tickers ───────────────────────────────────────────────────

    def _ticker_map(self) -> dict[str, dict]:
        now = _time.time()
        if self._ticker_cache and (now - self._ticker_ts) < self._TICKER_TTL:
            return self._ticker_cache
        payload = self._public_get("/exchange/ticker")
        result = {}
        for item in payload if isinstance(payload, list) else []:
            market = str(item.get("market") or "").upper()
            if market:
                result[market] = item
        self._ticker_cache = result
        self._ticker_ts = now
        return result

    def get_ticker(self, symbol: str) -> dict:
        market = self.to_broker_symbol(symbol)
        try:
            item = self._ticker_map().get(market, {})
            last_price = self.coerce_float(item.get("last_price"), 0.0)
            return {
                "symbol": self.normalize_app_symbol(symbol),
                "broker_symbol": market,
                "mark_price": last_price,
                "last_price": last_price,
                "close": last_price,
                "bid": self.coerce_float(item.get("bid"), 0.0),
                "ask": self.coerce_float(item.get("ask"), 0.0),
                "volume_24h": self.coerce_float(item.get("volume"), 0.0),
                "turnover_24h": 0.0,
                "open_interest": 0.0,
                "funding_rate": 0.0,
                "price_change_24h": self.coerce_float(item.get("change_24_hour"), 0.0),
                "high_24h": self.coerce_float(item.get("high"), 0.0),
                "low_24h": self.coerce_float(item.get("low"), 0.0),
                "market_symbol": market,
            }
        except Exception as exc:
            _spot_log.warning("[COINDCX SPOT] Ticker error for %s: %s", symbol, exc)
            return {"symbol": self.normalize_app_symbol(symbol), "mark_price": 0.0, "last_price": 0.0}

    def get_tickers_bulk(self) -> list:
        try:
            supported = {p["broker_symbol"] for p in self.get_products()}
            tickers = []
            for market, item in self._ticker_map().items():
                if market not in supported:
                    continue
                last_price = self.coerce_float(item.get("last_price"), 0.0)
                tickers.append(
                    {
                        "symbol": market,
                        "mark_price": last_price,
                        "close": last_price,
                        "last_price": last_price,
                        "volume": self.coerce_float(item.get("volume"), 0.0),
                        "funding_rate": 0.0,
                        "price_change_percent_24h": self.coerce_float(item.get("change_24_hour"), 0.0),
                        "high": self.coerce_float(item.get("high"), 0.0),
                        "low": self.coerce_float(item.get("low"), 0.0),
                        "market_symbol": market,
                    }
                )
            return tickers
        except Exception as exc:
            _spot_log.warning("[COINDCX SPOT] Bulk tickers error: %s", exc)
            return []

    # ── wallet and positions ──────────────────────────────────────

    def get_wallet(self) -> dict | list:
        if not self._is_configured():
            return {"error": "API not configured"}
        try:
            payload = self._private_post("/exchange/v1/users/balances")
            rows = []
            for item in payload if isinstance(payload, list) else []:
                asset = str(item.get("currency") or "").upper()
                free = self.coerce_float(item.get("balance"), 0.0)
                locked = self.coerce_float(item.get("locked_balance"), 0.0)
                total = free + locked
                if total <= 0 and asset != self.quote_asset:
                    continue
                rows.append(
                    {
                        "asset_symbol": asset,
                        "asset": asset,
                        "balance": str(total),
                        "wallet_balance": str(total),
                        "total_balance": str(total),
                        "equity": str(total),
                        "available_balance": str(free),
                        "free_balance": str(free),
                        "locked_balance": str(locked),
                        "blocked_margin": str(locked),
                        "order_margin": str(locked),
                        "position_margin": "0",
                        "unrealized_pnl": "0",
                        "account_type": "spot",
                        "raw": item,
                    }
                )
            rows.sort(key=lambda row: (row["asset_symbol"] != self.quote_asset, row["asset_symbol"]))
            return rows
        except Exception as exc:
            _spot_log.warning("[COINDCX SPOT] Wallet error: %s", exc)
            return {"error": str(exc)}

    def _balance_position(self, wallet_row: dict) -> dict | None:
        asset = str(wallet_row.get("asset_symbol") or "").upper()
        total_qty = self.coerce_float(wallet_row.get("total_balance"), 0.0)
        if not asset or asset == self.quote_asset or total_qty <= 0:
            return None
        market = f"{asset}{self.quote_asset}"
        # Unlisted means unpriceable: skip the ticker call and leave the value
        # at zero, the same lesson the Binance adapter learned the hard way.
        if not self.get_product_by_symbol(market):
            mark_price = 0.0
        else:
            ticker = self.get_ticker(market)
            mark_price = self.coerce_float(ticker.get("mark_price") or ticker.get("last_price"), 0.0)
        return {
            "product_id": market,
            "product_symbol": self.from_broker_symbol(market),
            "symbol": self.from_broker_symbol(market),
            "size": round(total_qty * mark_price, 8) if mark_price > 0 else 0.0,
            "base_size": total_qty,
            "entry_price": 0.0,
            "mark_price": mark_price,
            "unrealized_pnl": 0.0,
            "realized_pnl": 0.0,
            "margin": 0.0,
            "liquidation_price": 0.0,
            "leverage": 1,
            "margin_type": "spot_cash",
            "position_side": "LONG",
            "raw": wallet_row.get("raw") or wallet_row,
        }

    def get_positions(self) -> list:
        if not self._is_configured():
            return []
        wallet = self.get_wallet()
        if not isinstance(wallet, list):
            return []
        return [position for position in (self._balance_position(row) for row in wallet) if position]

    def get_position(self, product_id: str, strict: bool = False) -> dict:
        market = self.to_broker_symbol(product_id)
        for position in self.get_positions():
            if str(position.get("product_id") or "").upper() == market:
                return position
        return {}

    # ── orders ────────────────────────────────────────────────────

    @staticmethod
    def _decimal_floor(value: float, step: str) -> str:
        step_dec = Decimal(str(step or "0.001"))
        value_dec = Decimal(str(max(value, 0.0)))
        if step_dec <= 0:
            return format(value_dec, "f")
        qty = (value_dec / step_dec).to_integral_value(rounding=ROUND_DOWN) * step_dec
        return format(qty.normalize(), "f")

    @staticmethod
    def _order_type(order_type: str) -> str:
        raw = str(order_type or "").lower()
        if raw in {"limit", "limit_order"}:
            return "limit_order"
        if raw in {"stop_limit", "stop_limit_order", "stop_loss_limit"}:
            return "stop_limit"
        return "market_order"

    def _normalize_order(self, row: dict) -> dict:
        if not isinstance(row, dict):
            return {}
        market = str(row.get("market") or row.get("symbol") or "")
        total = self.coerce_float(row.get("total_quantity"), 0.0)
        remaining = self.coerce_float(row.get("remaining_quantity"), total)
        executed = max(total - remaining, 0.0)
        avg_price = self.coerce_float(row.get("avg_price"), 0.0)
        limit_price = self.coerce_float(row.get("price_per_unit"), 0.0)
        raw_status = str(row.get("status") or "").lower()
        order_id = str(row.get("id") or row.get("order_id") or "")
        out = {
            **row,
            "id": order_id,
            "orderId": order_id,
            "clientOrderId": row.get("client_order_id") or "",
            "symbol": market,
            "product_symbol": market,
            "status": _ORDER_STATUS_MAP.get(raw_status, raw_status.upper() or "NEW"),
            "origQty": str(total),
            "executedQty": str(executed),
            "cummulativeQuoteQty": str(round(executed * avg_price, 8)) if avg_price > 0 else "0",
            "price": str(limit_price) if limit_price > 0 else str(avg_price),
        }
        if avg_price > 0:
            out.setdefault("avgPrice", str(round(avg_price, 8)))
            out.setdefault("average_fill_price", round(avg_price, 8))
            out.setdefault("fill_price", round(avg_price, 8))
        if executed > 0:
            out.setdefault("filled_size", executed)
            out.setdefault("size", executed)
            if avg_price > 0:
                out.setdefault("quote_size", round(executed * avg_price, 8))
        out.setdefault("paid_commission", self.coerce_float(row.get("fee_amount"), 0.0))
        return out

    @staticmethod
    def _first_order(payload) -> dict:
        if isinstance(payload, dict) and isinstance(payload.get("orders"), list):
            return payload["orders"][0] if payload["orders"] else {}
        if isinstance(payload, list):
            return payload[0] if payload else {}
        return payload if isinstance(payload, dict) else {}

    def _quantity_for(self, product: dict, notional: float, base_qty: Optional[float], price: float) -> tuple[str, str]:
        step = str(product.get("step_size") or "0.000001")
        if base_qty is not None:
            qty = self._decimal_floor(self.coerce_float(base_qty, 0.0), step)
        else:
            if notional <= 0:
                return "", "Spot order size must be greater than zero"
            if price <= 0:
                return "", f"Unable to resolve CoinDCX Spot price for {product.get('broker_symbol')}"
            qty = self._decimal_floor(notional / price, step)
        qty_value = self.coerce_float(qty, 0.0)
        if qty_value <= 0:
            return "", "Spot order quantity rounds to zero at the exchange step"
        min_qty = self.coerce_float(product.get("min_qty"), 0.0)
        if min_qty and qty_value < min_qty:
            return "", f"Spot order quantity {qty} is below CoinDCX minimum quantity {min_qty:g}"
        min_notional = self.coerce_float(product.get("min_notional"), 0.0)
        if min_notional and price > 0 and qty_value * price < min_notional:
            return "", f"Spot order notional {qty_value * price:g} is below CoinDCX minimum notional {min_notional:g}"
        return qty, ""

    def place_order(
        self,
        product_id: str,
        size: float,
        side: str,
        order_type: str = "market_order",
        limit_price: float = None,
        leverage: int = 1,
        reduce_only: bool = False,
        client_order_id: str = None,
        base_qty: float = None,
        stop_price: float = None,
    ) -> dict:
        if not self._is_configured():
            return {"error": "API not configured"}
        market = self.to_broker_symbol(product_id)
        side_lower = str(side or "").lower()
        if side_lower not in {"buy", "sell"}:
            return {"error": "CoinDCX Spot supports only buy and sell orders"}
        product = self.get_product_by_symbol(market)
        if not product:
            return {"error": f"Unknown CoinDCX Spot symbol {market}"}

        mapped_type = self._order_type(order_type)
        if mapped_type != "market_order" and mapped_type not in (product.get("order_types") or [mapped_type]):
            return {"error": f"CoinDCX does not offer {mapped_type} on {market}"}
        ticker = self.get_ticker(market)
        price_for_qty = self.coerce_float(limit_price or ticker.get("last_price"), 0.0)
        quantity, error = self._quantity_for(product, self.coerce_float(size, 0.0), base_qty, price_for_qty)
        if error:
            return {"error": error}

        tick = str(product.get("tick_size") or "0.01")
        payload = {
            "market": market,
            "side": side_lower,
            "order_type": mapped_type,
            "total_quantity": float(quantity),
        }
        if client_order_id:
            payload["client_order_id"] = str(client_order_id)
        if mapped_type in {"limit_order", "stop_limit"}:
            if not limit_price:
                return {"error": "Limit price is required for CoinDCX Spot limit orders"}
            tick_price = self._decimal_floor(self.coerce_float(limit_price, 0.0), tick)
            if self.coerce_float(tick_price, 0.0) <= 0:
                return {"error": f"Unable to resolve a valid limit price for {market}"}
            payload["price_per_unit"] = float(tick_price)
        if mapped_type == "stop_limit":
            if not stop_price:
                return {"error": "Stop price is required for CoinDCX Spot stop-limit orders"}
            tick_stop = self._decimal_floor(self.coerce_float(stop_price, 0.0), tick)
            if self.coerce_float(tick_stop, 0.0) <= 0:
                return {"error": f"Unable to resolve a valid stop price for {market}"}
            payload["stop_price"] = float(tick_stop)
        try:
            result = self._normalize_order(self._first_order(self._private_post("/exchange/v1/orders/create", payload)))
            result.setdefault("symbol", market)
            result.setdefault("product_symbol", market)
            return result
        except Exception as exc:
            _spot_log.warning("[COINDCX SPOT] Order error: %s", exc)
            return {"error": str(exc)}

    async def place_order_verified(
        self,
        product_id: str,
        size: float,
        side: str,
        order_type: str = "market_order",
        limit_price: float = None,
        leverage: int = 1,
        reduce_only: bool = False,
        max_verify_attempts: int = 3,
        client_order_id: str = None,
        base_qty: float = None,
        stop_price: float = None,
    ) -> dict:
        started_at = _time.perf_counter()
        result = self.place_order(
            product_id=product_id,
            size=size,
            side=side,
            order_type=order_type,
            limit_price=limit_price,
            leverage=leverage,
            reduce_only=reduce_only,
            client_order_id=client_order_id,
            base_qty=base_qty,
            stop_price=stop_price,
        )
        order_ack_ms = round((_time.perf_counter() - started_at) * 1000, 1)
        if isinstance(result, dict) and result.get("error"):
            return {
                **result,
                "verified": False,
                "fill_status": "rejected",
                "order_lifecycle": "rejected",
                "exchange_state": "rejected",
                "verification_state": "rejected",
                "verification_summary": "Broker rejected order before verification",
                "order_ack_ms": order_ack_ms,
                "broker_latency_ms": order_ack_ms,
            }
        status = str(result.get("status") or "").upper()
        verified = status in {"FILLED", "PARTIALLY_FILLED"}
        fill_price = self.coerce_float(result.get("avgPrice") or result.get("price"), 0.0)
        if not verified:
            await asyncio.sleep(1)
            checked = self.get_order(product_id, result.get("orderId"), client_order_id=client_order_id)
            status = str(checked.get("status") or status).upper()
            verified = status in {"FILLED", "PARTIALLY_FILLED"}
            fill_price = self.coerce_float(checked.get("avgPrice") or fill_price, fill_price)
        return {
            **result,
            "verified": verified,
            "fill_status": status.lower() or "submitted",
            "order_lifecycle": "filled" if verified else "pending",
            "exchange_state": status.lower() or "submitted",
            "verification_state": "filled" if verified else "pending",
            "verification_summary": "CoinDCX order verified"
            if verified
            else "CoinDCX order submitted but not filled yet",
            "fill_price": fill_price or None,
            "verified_at_attempt": 1,
            "order_ack_ms": order_ack_ms,
            "broker_latency_ms": round((_time.perf_counter() - started_at) * 1000, 1),
        }

    def get_order(self, product_id: str, order_id=None, client_order_id: str = None) -> dict:
        if not self._is_configured() or not (order_id or client_order_id):
            return {}
        payload = {"id": str(order_id)} if order_id else {"client_order_id": str(client_order_id)}
        try:
            return self._normalize_order(self._first_order(self._private_post("/exchange/v1/orders/status", payload)))
        except Exception as exc:
            _spot_log.warning("[COINDCX SPOT] Order status error: %s", exc)
            return {}

    def cancel_order(self, order_id: str, product_id: str = "") -> dict:
        if not self._is_configured():
            return {"error": "API not configured"}
        try:
            resp = self._private_post("/exchange/v1/orders/cancel", {"id": str(order_id)})
            return resp if isinstance(resp, dict) else {"status": "ok"}
        except Exception as exc:
            _spot_log.warning("[COINDCX SPOT] Cancel error: %s", exc)
            return {"error": str(exc)}

    def get_orders(self, product_id: str = None, state: str = "open") -> list:
        if not self._is_configured():
            return []
        if str(state or "").lower() != "open":
            # CoinDCX spot has no account-wide closed-orders listing; settled
            # orders are reconciled individually via get_order.
            return []
        payload = {"market": self.to_broker_symbol(product_id or "BTCUSDT")}
        try:
            resp = self._private_post("/exchange/v1/orders/active_orders", payload)
            orders = resp.get("orders") if isinstance(resp, dict) else resp
            return [self._normalize_order(item) for item in orders or []]
        except Exception as exc:
            _spot_log.warning("[COINDCX SPOT] Open orders error: %s", exc)
            return []

    def get_order_history(self, force_refresh: bool = False, extra_symbols: Optional[Iterable[str]] = None) -> list:
        if not self._is_configured():
            return []
        now = _time.time()
        if self._history_cache is not None and not force_refresh and (now - self._history_ts) < self._HISTORY_TTL:
            return list(self._history_cache)
        try:
            payload = self._private_post("/exchange/v1/orders/trade_history", {"limit": 500, "sort": "desc"})
        except Exception as exc:
            _spot_log.warning("[COINDCX SPOT] Trade history error: %s", exc)
            return []
        trades = []
        for row in payload if isinstance(payload, list) else []:
            row = dict(row)
            market = str(row.get("symbol") or row.get("market") or "")
            price = self.coerce_float(row.get("price"), 0.0)
            qty = self.coerce_float(row.get("quantity"), 0.0)
            trade_id = row.get("id")
            ts_ms = int(self.coerce_float(row.get("timestamp"), 0.0))
            row["trade_id"] = trade_id
            row["id"] = f"{market}-{trade_id}"
            row.setdefault("order_id", row.get("order_id"))
            row.setdefault("symbol", market)
            row.setdefault("product_symbol", self.from_broker_symbol(market))
            row.setdefault("side", str(row.get("side") or "").lower())
            row.setdefault("average_fill_price", price)
            row.setdefault("fill_price", price)
            row.setdefault("qty", qty)
            row.setdefault("size", qty)
            row.setdefault("filled_size", qty)
            row.setdefault("quote_size", round(price * qty, 8))
            row.setdefault("paid_commission", self.coerce_float(row.get("fee_amount"), 0.0))
            row.setdefault("state", "closed")
            row.setdefault("order_type", "spot trade")
            row.setdefault("product", {"notional_type": "linear", "contract_value": "1"})
            row.setdefault("time", ts_ms)
            row.setdefault(
                "updated_at", datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat() if ts_ms else ""
            )
            trades.append(row)
        trades.sort(key=lambda item: item.get("time") or 0, reverse=True)
        self._history_cache = trades
        self._history_ts = now
        return list(trades)

    def get_order_commission(self, symbol: str, order_id) -> Optional[float]:
        """Commission actually charged for one order, in quote currency.

        The order endpoints carry no fee; only trade history does, one row per
        fill, so a partially filled order's cost is the sum of its rows. Returns
        None when nothing can be established — never 0.0, because "no data" and
        "free trade" must not look alike to the caller, which books a modelled
        rate for the former.
        """
        if not self._is_configured() or order_id in (None, ""):
            return None
        wanted = str(order_id).strip()
        try:
            rows = self._private_post("/exchange/v1/orders/trade_history", {"limit": 500, "sort": "desc"})
        except Exception as exc:
            _spot_log.warning("[COINDCX SPOT] Commission lookup failed for %s: %s", wanted, exc)
            return None
        total = 0.0
        seen = False
        for row in rows if isinstance(rows, list) else []:
            if str(row.get("order_id") or "").strip() != wanted:
                continue
            seen = True
            total += self.coerce_float(row.get("fee_amount"), 0.0)
        return round(total, 8) if seen else None

    def get_funding_history(self, symbol: str) -> list:
        return []
