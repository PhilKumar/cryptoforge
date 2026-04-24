"""
broker/coindcx.py — CoinDCX futures broker adapter for CryptoForge.

This adapter keeps the existing Delta-shaped runtime contract so the
current app, live engine, paper engine, and scalp engine can switch
brokers through configuration instead of invasive rewrites.
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
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config

from .base import BaseBroker

_coindcx_log = logging.getLogger("cryptoforge.coindcx")
_RETRYABLE_STATUSES = {429, 500, 502, 503, 504}

_http_session = requests.Session()
_adapter = HTTPAdapter(pool_connections=4, pool_maxsize=10, max_retries=0)
_http_session.mount("https://", _adapter)
_http_session.mount("http://", _adapter)


def _request_with_retry(
    method: str,
    url: str,
    *,
    headers: dict,
    data: str | None = None,
    params: dict | None = None,
    timeout: int = 30,
    max_retries: int = 3,
    base_delay: float = 1.0,
) -> requests.Response:
    last_exc = None
    for attempt in range(max_retries):
        try:
            resp = _http_session.request(
                method,
                url,
                headers=headers,
                data=data,
                params=params,
                timeout=timeout,
            )
            if resp.status_code not in _RETRYABLE_STATUSES:
                return resp
            last_exc = Exception(f"CoinDCX API {resp.status_code}: {resp.text[:200]}")
            _coindcx_log.warning(
                "[COINDCX] %s %s -> %s (attempt %s/%s)",
                method,
                url,
                resp.status_code,
                attempt + 1,
                max_retries,
            )
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            last_exc = exc
            _coindcx_log.warning(
                "[COINDCX] %s %s network error (attempt %s/%s): %s",
                method,
                url,
                attempt + 1,
                max_retries,
                exc,
            )
        if attempt < max_retries - 1:
            _time.sleep(base_delay * (2**attempt))
    raise last_exc or Exception(f"CoinDCX request failed: {method} {url}")


class CoinDCXClient(BaseBroker):
    broker_name = "coindcx"
    display_name = "CoinDCX"

    def __init__(self):
        self.api_key = config.COINDCX_API_KEY
        self.api_secret = config.COINDCX_API_SECRET
        self.base_url = config.COINDCX_BASE_URL.rstrip("/")
        self.public_url = config.COINDCX_PUBLIC_URL.rstrip("/")
        self.margin_currency = config.COINDCX_MARGIN_CURRENCY
        self._products_cache = None
        self._products_ts = 0.0
        self._CACHE_TTL = 3600
        self._instrument_cache: dict[str, dict] = {}
        self._instrument_ts: dict[str, float] = {}

    def get_market_feed_kind(self) -> str:
        return "polling"

    def _is_configured(self) -> bool:
        return (
            self.api_key != "YOUR_COINDCX_API_KEY_HERE"
            and self.api_secret != "YOUR_COINDCX_API_SECRET_HERE"
            and len(self.api_key) > 5
            and len(self.api_secret) > 5
        )

    @staticmethod
    def _compact_json(payload: dict) -> str:
        return json.dumps(payload or {}, separators=(",", ":"), sort_keys=False)

    def _signed_headers(self, payload: dict) -> tuple[dict, str]:
        json_body = self._compact_json(payload)
        signature = hmac.new(self.api_secret.encode("utf-8"), json_body.encode("utf-8"), hashlib.sha256).hexdigest()
        headers = {
            "Content-Type": "application/json",
            "X-AUTH-APIKEY": self.api_key,
            "X-AUTH-SIGNATURE": signature,
        }
        return headers, json_body

    def _public_get(self, path: str, *, params: dict | None = None, use_public_host: bool = False):
        base = self.public_url if use_public_host else self.base_url
        url = f"{base}{path}"
        resp = _request_with_retry("GET", url, headers={"Content-Type": "application/json"}, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _private_request(self, method: str, path: str, *, payload: dict | None = None, params: dict | None = None):
        if not self._is_configured():
            raise Exception("CoinDCX API not configured")
        body = dict(payload or {})
        body.setdefault("timestamp", int(round(_time.time() * 1000)))
        headers, json_body = self._signed_headers(body)
        url = f"{self.base_url}{path}"
        resp = _request_with_retry(method, url, headers=headers, data=json_body, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    @classmethod
    def _pair_to_app_symbol(cls, pair: str) -> str:
        raw = str(pair or "").strip().upper()
        if not raw:
            return ""
        if raw.startswith("B-"):
            raw = raw[2:]
        if "_" in raw:
            base, quote = raw.split("_", 1)
            if base == "PAXG" and quote == "USDT":
                return "PAXGUSD"
            return f"{base}{quote}"
        return cls.normalize_app_symbol(raw)

    @classmethod
    def _app_symbol_to_pair(cls, symbol: str) -> str:
        raw = cls.normalize_app_symbol(symbol)
        if not raw:
            return ""
        if raw.startswith("B-") and "_" in raw:
            return raw
        if raw == "PAXGUSD":
            return "B-PAXG_USDT"
        if raw.endswith("USDT"):
            return f"B-{raw[:-4]}_USDT"
        if raw.endswith("USD"):
            return f"B-{raw[:-3]}_USDT"
        return f"B-{raw}_USDT"

    def to_broker_symbol(self, symbol: str) -> str:
        return self._app_symbol_to_pair(symbol)

    def from_broker_symbol(self, symbol: str) -> str:
        return self._pair_to_app_symbol(symbol)

    def _normalize_product(self, pair: str, instrument: Optional[dict] = None) -> dict:
        details = dict(instrument or {})
        app_symbol = self._pair_to_app_symbol(pair or details.get("pair"))
        max_leverage = int(self.coerce_float(details.get("max_leverage"), 25) or 25)
        min_leverage = int(self.coerce_float(details.get("min_leverage"), 1) or 1)
        return {
            "id": str(pair or details.get("pair") or ""),
            "pair": str(pair or details.get("pair") or ""),
            "symbol": app_symbol,
            "state": "live",
            "contract_type": "perpetual_futures",
            "default_leverage": min(max(10, min_leverage), max_leverage) if max_leverage >= 10 else max_leverage,
            "min_leverage": min_leverage,
            "max_leverage": max_leverage,
            "base_precision": int(self.coerce_float(details.get("base_precision"), 6) or 6),
            "target_precision": int(self.coerce_float(details.get("target_precision"), 2) or 2),
            **details,
        }

    def get_products(self, force_refresh: bool = False) -> list:
        now = _time.time()
        if self._products_cache and not force_refresh and (now - self._products_ts) < self._CACHE_TTL:
            return list(self._products_cache)

        payload = self._public_get(
            "/exchange/v1/derivatives/futures/data/active_instruments",
            params={"margin_currency_short_name[]": self.margin_currency},
        )
        pairs = payload.get("active_instruments", [])
        products = [self._normalize_product(pair) for pair in pairs]
        self._products_cache = products
        self._products_ts = now
        return list(products)

    def get_perpetual_futures(self) -> list:
        return self.get_products()

    def get_supported_symbols(self) -> set[str]:
        return {self.from_broker_symbol(item.get("symbol", "")) for item in self.get_products() if item.get("symbol")}

    def get_instrument_details(self, pair: str) -> dict:
        now = _time.time()
        pair = str(pair or "").strip().upper()
        cached = self._instrument_cache.get(pair)
        cached_ts = self._instrument_ts.get(pair, 0.0)
        if cached and (now - cached_ts) < self._CACHE_TTL:
            return dict(cached)
        payload = self._public_get(
            "/exchange/v1/derivatives/futures/data/instrument",
            params={"pair": pair, "margin_currency_short_name": self.margin_currency},
        )
        instrument = payload.get("instrument", {}) or {}
        self._instrument_cache[pair] = instrument
        self._instrument_ts[pair] = now
        return dict(instrument)

    def get_product_by_symbol(self, symbol: str) -> Optional[dict]:
        pair = self._app_symbol_to_pair(symbol)
        active_pairs = {item.get("pair") or item.get("id") for item in self.get_products()}
        if pair not in active_pairs:
            return None
        try:
            instrument = self.get_instrument_details(pair)
        except Exception:
            instrument = {}
        return self._normalize_product(pair, instrument)

    def get_leverage_info(self, symbol: str) -> dict:
        product = self.get_product_by_symbol(symbol)
        if not product:
            return {"max_leverage": 25, "default": 10, "options": self.build_standard_leverage_options(25)}
        max_leverage = max(int(self.coerce_float(product.get("max_leverage"), 25) or 25), 1)
        min_leverage = max(int(self.coerce_float(product.get("min_leverage"), 1) or 1), 1)
        default = min(max(10, min_leverage), max_leverage)
        return {
            "max_leverage": max_leverage,
            "default": default,
            "options": self.build_standard_leverage_options(max_leverage),
            "initial_margin": round(100 / max_leverage, 4) if max_leverage > 0 else 0.0,
            "maintenance_margin": self.coerce_float(product.get("maintenance_margin"), 0.0),
        }

    def get_candles(self, symbol: str, resolution: str = "5m", start: str = None, end: str = None) -> pd.DataFrame:
        pair = self._app_symbol_to_pair(symbol)
        interval_map = {
            "1m": "1m",
            "3m": "3m",
            "5m": "5m",
            "15m": "15m",
            "30m": "30m",
            "1h": "1h",
            "2h": "2h",
            "4h": "4h",
            "6h": "6h",
            "1d": "1d",
            "1D": "1d",
            "1w": "1w",
            "1W": "1w",
        }
        interval = interval_map.get(resolution, "5m")
        payload = {"pair": pair, "interval": interval}
        if start:
            payload["from_time"] = int(datetime.strptime(start, "%Y-%m-%d").timestamp())
        if end:
            payload["to_time"] = int(datetime.strptime(end, "%Y-%m-%d").timestamp())
        data = self._public_get("/market_data/candlesticks", params=payload, use_public_host=True)
        candles = data if isinstance(data, list) else data.get("data", [])
        if not candles:
            return pd.DataFrame()
        df = pd.DataFrame(candles)
        rename_map = {
            "o": "open",
            "h": "high",
            "l": "low",
            "c": "close",
            "v": "volume",
            "time": "time",
            "T": "time",
            "t": "time",
        }
        df.rename(columns={key: value for key, value in rename_map.items() if key in df.columns}, inplace=True)
        if "time" not in df.columns:
            return pd.DataFrame()
        time_unit = "ms" if int(self.coerce_float(df["time"].iloc[0], 0.0)) > 10_000_000_000 else "s"
        df["datetime"] = pd.to_datetime(df["time"], unit=time_unit, utc=True)
        for col in ("open", "high", "low", "close", "volume"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df.set_index("datetime", inplace=True)
        df.sort_index(inplace=True)
        df = df[~df.index.duplicated(keep="first")]
        keep = [col for col in ("open", "high", "low", "close", "volume") if col in df.columns]
        return df[keep]

    async def async_get_candles(self, symbol: str, **kwargs) -> pd.DataFrame:
        return await asyncio.to_thread(self.get_candles, symbol, **kwargs)

    def _fetch_current_prices(self) -> dict:
        payload = self._public_get("/market_data/v3/current_prices/futures/rt", use_public_host=True)
        if isinstance(payload, dict):
            if isinstance(payload.get("prices"), dict):
                return payload.get("prices") or {}
            if all(isinstance(value, dict) for value in payload.values()):
                return payload
        if isinstance(payload, list):
            result = {}
            for item in payload:
                if not isinstance(item, dict):
                    continue
                pair = str(item.get("pair") or item.get("market") or item.get("symbol") or "").upper()
                if pair:
                    result[pair] = item
            return result
        return {}

    def get_ticker(self, symbol: str) -> dict:
        pair = self._app_symbol_to_pair(symbol)
        try:
            price_map = self._fetch_current_prices()
            item = price_map.get(pair, {})
            mark_price = self.coerce_float(item.get("mp") or item.get("ls"), 0.0)
            last_price = self.coerce_float(item.get("ls") or item.get("mp"), 0.0)
            return {
                "symbol": self.normalize_app_symbol(symbol),
                "broker_symbol": pair,
                "mark_price": mark_price,
                "last_price": last_price,
                "close": last_price,
                "volume_24h": self.coerce_float(item.get("v"), 0.0),
                "turnover_24h": 0.0,
                "open_interest": 0.0,
                "funding_rate": self.coerce_float(item.get("fr"), 0.0),
                "price_change_24h": self.coerce_float(item.get("pc"), 0.0),
                "high_24h": self.coerce_float(item.get("h"), 0.0),
                "low_24h": self.coerce_float(item.get("l"), 0.0),
                "market_symbol": str(item.get("mkt") or "").upper(),
            }
        except Exception as exc:
            _coindcx_log.warning("[COINDCX] Ticker error for %s: %s", symbol, exc)
            return {"symbol": self.normalize_app_symbol(symbol), "mark_price": 0.0, "last_price": 0.0}

    def get_tickers_bulk(self) -> list:
        try:
            price_map = self._fetch_current_prices()
            tickers = []
            for pair, item in price_map.items():
                tickers.append(
                    {
                        "symbol": pair,
                        "mark_price": self.coerce_float(item.get("mp"), 0.0),
                        "close": self.coerce_float(item.get("ls"), 0.0),
                        "last_price": self.coerce_float(item.get("ls") or item.get("mp"), 0.0),
                        "volume": self.coerce_float(item.get("v"), 0.0),
                        "funding_rate": self.coerce_float(item.get("fr"), 0.0),
                        "price_change_percent_24h": self.coerce_float(item.get("pc"), 0.0),
                        "high": self.coerce_float(item.get("h"), 0.0),
                        "low": self.coerce_float(item.get("l"), 0.0),
                        "market_symbol": str(item.get("mkt") or "").upper(),
                    }
                )
            return tickers
        except Exception as exc:
            _coindcx_log.warning("[COINDCX] Bulk tickers error: %s", exc)
            return []

    def get_wallet(self) -> dict:
        if not self._is_configured():
            return {"error": "API not configured"}
        try:
            wallet = self._private_request("POST", "/exchange/v1/derivatives/futures/positions/cross_margin_details")
            return wallet if isinstance(wallet, dict) else {"result": wallet}
        except Exception as exc:
            _coindcx_log.warning("[COINDCX] Wallet error: %s", exc)
            return {"error": str(exc)}

    def _normalize_position(self, payload: dict) -> dict:
        pair = str(payload.get("pair") or "")
        symbol = self._pair_to_app_symbol(pair)
        avg_price = self.coerce_float(payload.get("avg_price"), 0.0)
        mark_price = self.coerce_float(payload.get("mark_price"), 0.0)
        active_qty = self.coerce_float(payload.get("active_pos"), 0.0)
        reference_price = mark_price or avg_price
        signed_notional = active_qty * reference_price if reference_price > 0 else active_qty
        return {
            "id": str(payload.get("id") or ""),
            "product_id": pair,
            "pair": pair,
            "product_symbol": symbol,
            "symbol": symbol,
            "size": round(signed_notional, 8),
            "base_size": active_qty,
            "entry_price": avg_price,
            "mark_price": mark_price,
            "liquidation_price": self.coerce_float(payload.get("liquidation_price"), 0.0),
            "margin": self.coerce_float(payload.get("locked_user_margin") or payload.get("locked_margin"), 0.0),
            "unrealized_pnl": self.coerce_float(payload.get("unrealised_pnl", payload.get("unrealized_pnl")), 0.0),
            "realized_pnl": self.coerce_float(payload.get("realised_pnl", payload.get("realized_pnl")), 0.0),
            "leverage": self.coerce_float(payload.get("leverage"), 0.0),
            "margin_type": payload.get("margin_type") or "crossed",
            "stop_loss_trigger": self.coerce_float(payload.get("stop_loss_trigger"), 0.0),
            "take_profit_trigger": self.coerce_float(payload.get("take_profit_trigger"), 0.0),
        }

    def get_positions(self) -> list:
        if not self._is_configured():
            return []
        try:
            payload = self._private_request(
                "POST",
                "/exchange/v1/derivatives/futures/positions",
                payload={"page": 1, "size": 100},
            )
            positions = payload if isinstance(payload, list) else payload.get("positions", [])
            return [self._normalize_position(item) for item in positions or []]
        except Exception as exc:
            _coindcx_log.warning("[COINDCX] Positions error: %s", exc)
            return []

    def get_position(self, product_id: str, strict: bool = False) -> dict:
        if not self._is_configured():
            return {}
        pair = str(product_id or "").strip().upper()
        try:
            payload = self._private_request(
                "POST",
                "/exchange/v1/derivatives/futures/positions",
                payload={"pairs": [pair], "page": 1, "size": 20},
            )
            positions = payload if isinstance(payload, list) else payload.get("positions", [])
            for item in positions or []:
                if str(item.get("pair") or "").upper() == pair:
                    return self._normalize_position(item)
            return {}
        except Exception as exc:
            if strict:
                raise
            _coindcx_log.warning("[COINDCX] Position error for %s: %s", pair, exc)
            return {}

    def _resolve_quantity(self, product_id: str, size: float, price: float = 0.0) -> float:
        pair = str(product_id or "").strip().upper()
        if size <= 0:
            return 0.0
        instrument = self.get_instrument_details(pair)
        quantity = size
        if price > 0:
            quantity = size / price
        precision = int(self.coerce_float(instrument.get("base_precision"), 6) or 6)
        step = 10 ** (-precision)
        rounded = round(quantity, precision)
        if rounded <= 0:
            rounded = step
        return rounded

    def place_order(
        self,
        product_id: str,
        size: float,
        side: str,
        order_type: str = "market_order",
        limit_price: float = None,
        leverage: int = 10,
        reduce_only: bool = False,
    ) -> dict:
        if not self._is_configured():
            return {"error": "API not configured"}
        pair = str(product_id or "").strip().upper()
        if reduce_only:
            return self.exit_position(pair)
        ticker = self.get_ticker(self.from_broker_symbol(pair))
        price_for_qty = self.coerce_float(ticker.get("mark_price") or ticker.get("last_price"), 0.0)
        quantity = self._resolve_quantity(pair, self.coerce_float(size, 0.0), price=price_for_qty)
        if quantity <= 0:
            return {"error": f"Unable to resolve CoinDCX order quantity for {pair}"}
        payload = {
            "side": side,
            "pair": pair,
            "order_type": order_type,
            "size": quantity,
            "leverage": int(leverage or 1),
            "margin_currency_short_name": [self.margin_currency],
            "position_margin_type": "crossed",
        }
        if order_type != "market_order":
            payload["price"] = self.coerce_float(limit_price, 0.0)
        try:
            resp = self._private_request("POST", "/exchange/v1/derivatives/futures/orders/create", payload=payload)
            if isinstance(resp, list):
                return resp[0] if resp else {}
            if isinstance(resp, dict) and isinstance(resp.get("orders"), list):
                return resp["orders"][0] if resp["orders"] else {}
            return resp
        except Exception as exc:
            _coindcx_log.warning("[COINDCX] Order error: %s", exc)
            return {"error": str(exc)}

    def exit_position(self, product_id: str) -> dict:
        pair = str(product_id or "").strip().upper()
        try:
            resp = self._private_request(
                "POST",
                "/exchange/v1/derivatives/futures/positions/exit",
                payload={"pair": pair},
            )
            if isinstance(resp, list):
                return resp[0] if resp else {}
            if isinstance(resp, dict) and isinstance(resp.get("orders"), list):
                return resp["orders"][0] if resp["orders"] else {}
            return resp
        except Exception as exc:
            _coindcx_log.warning("[COINDCX] Exit position error: %s", exc)
            return {"error": str(exc)}

    async def place_order_verified(
        self,
        product_id: str,
        size: float,
        side: str,
        order_type: str = "market_order",
        limit_price: float = None,
        leverage: int = 10,
        reduce_only: bool = False,
        max_verify_attempts: int = 3,
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

        order_id = str(result.get("id") or result.get("order_id") or result.get("client_order_id") or "")
        pair = str(product_id or "").strip().upper()
        symbol = self.from_broker_symbol(pair)

        if reduce_only:
            for attempt in range(max_verify_attempts):
                await asyncio.sleep(2)
                position = await asyncio.to_thread(self.get_position, pair, True)
                if abs(self.coerce_float(position.get("size"), 0.0)) <= 0:
                    ticker = self.get_ticker(symbol)
                    fill_price = self.coerce_float(ticker.get("mark_price") or ticker.get("last_price"), 0.0)
                    return {
                        **result,
                        "id": order_id or result.get("id") or "exit",
                        "verified": True,
                        "fill_status": "closed",
                        "order_lifecycle": "filled",
                        "exchange_state": "closed",
                        "verification_state": "position_confirmed",
                        "verification_summary": "Position exited and verified",
                        "fill_price": fill_price,
                        "verified_at_attempt": attempt + 1,
                        "order_ack_ms": order_ack_ms,
                        "broker_latency_ms": round((_time.perf_counter() - started_at) * 1000, 1),
                    }
            return {
                **result,
                "verified": False,
                "fill_status": "pending",
                "order_lifecycle": "pending",
                "exchange_state": "pending",
                "verification_state": "pending",
                "verification_summary": "Exit was submitted but the position is still open",
                "error": "Exit order could not be verified",
                "order_ack_ms": order_ack_ms,
                "broker_latency_ms": round((_time.perf_counter() - started_at) * 1000, 1),
            }

        last_state = "submitted"
        for attempt in range(max_verify_attempts):
            await asyncio.sleep(2)
            orders = await asyncio.to_thread(self.get_orders, pair, "closed")
            matched = None
            for order in orders or []:
                if str(order.get("id") or order.get("order_id") or "") == order_id:
                    matched = order
                    break
            state = str((matched or {}).get("status", "")).lower()
            if state:
                last_state = state
            if matched and state in {"filled", "partially_cancelled", "cancelled"}:
                fill_price = self.coerce_float(
                    matched.get("avg_price")
                    or matched.get("average_price")
                    or matched.get("price")
                    or matched.get("last_price")
                    or result.get("price"),
                    0.0,
                )
                position = await asyncio.to_thread(self.get_position, pair, True)
                return {
                    **result,
                    "verified": True,
                    "fill_status": state,
                    "order_lifecycle": "filled" if state == "filled" else "partial",
                    "exchange_state": state,
                    "verification_state": "position_confirmed",
                    "verification_summary": "CoinDCX order verified against order history",
                    "fill_price": fill_price,
                    "position_size": self.coerce_float(position.get("size"), 0.0),
                    "verified_at_attempt": attempt + 1,
                    "order_ack_ms": order_ack_ms,
                    "broker_latency_ms": round((_time.perf_counter() - started_at) * 1000, 1),
                }

        return {
            **result,
            "verified": False,
            "fill_status": last_state,
            "order_lifecycle": "pending",
            "exchange_state": last_state,
            "verification_state": "pending",
            "verification_summary": "CoinDCX order could not be verified from order history",
            "error": f"Order {order_id or '[unknown]'} could not be verified after {max_verify_attempts} attempts",
            "order_ack_ms": order_ack_ms,
            "broker_latency_ms": round((_time.perf_counter() - started_at) * 1000, 1),
        }

    def cancel_order(self, order_id: str, product_id: str = "") -> dict:
        if not self._is_configured():
            return {"error": "API not configured"}
        payload = {"id": order_id}
        if product_id:
            payload["pair"] = str(product_id).strip().upper()
        try:
            resp = self._private_request("POST", "/exchange/v1/derivatives/futures/orders/cancel", payload=payload)
            if isinstance(resp, list):
                return resp[0] if resp else {}
            return resp
        except Exception as exc:
            _coindcx_log.warning("[COINDCX] Cancel error: %s", exc)
            return {"error": str(exc)}

    def get_orders(self, product_id: str = None, state: str = "open") -> list:
        if not self._is_configured():
            return []
        state = str(state or "open").lower()
        status_map = {
            "open": ["open", "partially_open"],
            "closed": ["filled", "partially_cancelled", "cancelled", "rejected"],
            "cancelled": ["cancelled", "partially_cancelled"],
        }
        statuses = status_map.get(state, [state])
        from_date = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")
        to_date = datetime.utcnow().strftime("%Y-%m-%d")
        records = []
        seen = set()
        for side in ("buy", "sell"):
            for status in statuses:
                payload = {
                    "side": side,
                    "status": status,
                    "page": 1,
                    "size": 100,
                    "margin_currency_short_name": [self.margin_currency],
                    "timestamp": int(round(_time.time() * 1000)),
                }
                try:
                    response = self._private_request("POST", "/exchange/v1/derivatives/futures/orders", payload=payload)
                except Exception as exc:
                    _coindcx_log.warning("[COINDCX] List orders error for %s/%s: %s", side, status, exc)
                    continue
                orders = response if isinstance(response, list) else response.get("orders", [])
                for order in orders or []:
                    if product_id and str(order.get("pair") or "").upper() != str(product_id).upper():
                        continue
                    order_key = str(order.get("id") or order.get("order_id") or "")
                    if order_key in seen:
                        continue
                    seen.add(order_key)
                    records.append(order)
        return records

    def get_order_history(self) -> list:
        if not self._is_configured():
            return []
        from_date = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")
        to_date = datetime.utcnow().strftime("%Y-%m-%d")
        trades = []
        for pair in sorted(self.get_supported_symbols()):
            broker_pair = self.to_broker_symbol(pair)
            payload = {
                "pair": broker_pair,
                "from_date": from_date,
                "to_date": to_date,
                "page": 1,
                "size": 100,
                "margin_currency_short_name": [self.margin_currency],
                "timestamp": int(round(_time.time() * 1000)),
            }
            try:
                response = self._private_request("POST", "/exchange/v1/derivatives/futures/trades", payload=payload)
            except Exception as exc:
                _coindcx_log.warning("[COINDCX] Trades error for %s: %s", broker_pair, exc)
                continue
            items = response if isinstance(response, list) else response.get("trades", [])
            trades.extend(items or [])
        trades.sort(key=lambda item: item.get("timestamp") or item.get("created_at") or 0, reverse=True)
        return trades

    def set_leverage(self, product_id: str, leverage: int) -> dict:
        if not self._is_configured():
            return {"error": "API not configured"}
        pair = str(product_id or "").strip().upper()
        payload = {
            "pair": pair,
            "leverage": int(leverage or 1),
            "timestamp": int(round(_time.time() * 1000)),
        }
        try:
            resp = self._private_request("POST", "/exchange/v1/derivatives/futures/positions/leverage", payload=payload)
            if isinstance(resp, list):
                return resp[0] if resp else {}
            return resp
        except Exception as exc:
            _coindcx_log.warning("[COINDCX] Set leverage error: %s", exc)
            return {"error": str(exc)}

    def get_funding_history(self, symbol: str) -> list:
        pair = self._app_symbol_to_pair(symbol)
        try:
            payload = self._private_request(
                "POST",
                "/api/v1/derivatives/futures/data/stats",
                payload={},
                params={"pair": pair},
            )
            return [payload] if isinstance(payload, dict) else list(payload or [])
        except Exception as exc:
            _coindcx_log.warning("[COINDCX] Funding/pair stats error: %s", exc)
            return []
