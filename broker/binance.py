"""
broker/binance.py - Binance Spot adapter for CryptoForge.

BinanceSpotClient targets the Spot market only. Public market data works
without credentials; account and order methods require API keys.
"""

import asyncio
import hashlib
import hmac
import logging
import time as _time
from datetime import datetime, timezone
from decimal import ROUND_DOWN, Decimal
from urllib.parse import urlencode

import pandas as pd
import requests
from requests.adapters import HTTPAdapter

import config

from .base import BaseBroker

_binance_log = logging.getLogger("cryptoforge.binance")
_RETRYABLE_STATUSES = {418, 429, 500, 502, 503, 504}
# Binance default recvWindow is 5s; 10s absorbs normal jitter without
# letting a stale request execute long after it was meant to.
_RECV_WINDOW_MS = 10000

_http_session = requests.Session()
_adapter = HTTPAdapter(pool_connections=4, pool_maxsize=10, max_retries=0)
_http_session.mount("https://", _adapter)
_http_session.mount("http://", _adapter)

_KLINE_INTERVALS = {
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


def _request_with_retry(
    method: str,
    url: str,
    *,
    headers: dict | None = None,
    data: dict | None = None,
    params: dict | None = None,
    timeout: int = 30,
    max_retries: int = 3,
    base_delay: float = 1.0,
) -> "requests.Response":
    last_exc = None
    for attempt in range(max_retries):
        try:
            resp = _http_session.request(
                method,
                url,
                headers=headers or {},
                data=data,
                params=params,
                timeout=timeout,
            )
            if resp.status_code not in _RETRYABLE_STATUSES:
                return resp
            last_exc = Exception(f"Binance API {resp.status_code}: {resp.text[:200]}")
            _binance_log.warning(
                "[BINANCE] %s %s -> %s (attempt %s/%s)",
                method,
                url,
                resp.status_code,
                attempt + 1,
                max_retries,
            )
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            last_exc = exc
            _binance_log.warning("[BINANCE] %s %s network error: %s", method, url, exc)
        if attempt < max_retries - 1:
            _time.sleep(base_delay * (2**attempt))
    raise last_exc or Exception(f"Binance request failed: {method} {url}")


class BinanceSpotClient(BaseBroker):
    broker_name = "binance"
    display_name = "Binance Spot"
    supports_funding = False  # spot has no funding rate or open interest

    _SYMBOL_ALIASES = {
        **BaseBroker._SYMBOL_ALIASES,
        "BTCUSD": "BTCUSDT",
        "ETHUSD": "ETHUSDT",
        "SOLUSD": "SOLUSDT",
        "XRPUSD": "XRPUSDT",
        "DOGEUSD": "DOGEUSDT",
        "PAXGUSD": "PAXGUSDT",
    }

    def __init__(self):
        self.api_key = getattr(config, "BINANCE_SPOT_API_KEY", "") or config.BINANCE_API_KEY
        self.api_secret = getattr(config, "BINANCE_SPOT_API_SECRET", "") or config.BINANCE_API_SECRET
        self.base_url = config.BINANCE_SPOT_BASE_URL.rstrip("/")
        self.testnet = bool(getattr(config, "BINANCE_SPOT_TESTNET", False))
        self.quote_asset = config.BINANCE_SPOT_QUOTE_ASSET.upper()
        self._products_cache = None
        self._products_ts = 0.0
        self._CACHE_TTL = 3600
        # Binance rejects a signed request whose timestamp drifts outside
        # recvWindow of *its* clock (-1021). Track the offset to our own clock
        # and re-sync periodically so a drifting host doesn't start failing orders.
        self._time_offset_ms = 0
        self._time_offset_ts = 0.0
        self._TIME_SYNC_TTL = 300

    def get_market_feed_kind(self) -> str:
        return "polling"

    def _is_configured(self) -> bool:
        placeholders = {
            "YOUR_BINANCE_API_KEY_HERE",
            "YOUR_BINANCE_API_SECRET_HERE",
            "YOUR_BINANCE_SPOT_API_KEY_HERE",
            "YOUR_BINANCE_SPOT_API_SECRET_HERE",
        }
        return (
            str(self.api_key or "") not in placeholders
            and str(self.api_secret or "") not in placeholders
            and len(str(self.api_key or "")) > 5
            and len(str(self.api_secret or "")) > 5
        )

    def _public_get(self, path: str, *, params: dict | None = None):
        url = f"{self.base_url}{path}"
        resp = _request_with_retry("GET", url, headers={"Content-Type": "application/json"}, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _server_time_offset_ms(self) -> int:
        """Milliseconds to add to our clock to match Binance's. Cached briefly."""
        now = _time.monotonic()
        if self._time_offset_ts and now - self._time_offset_ts < self._TIME_SYNC_TTL:
            return self._time_offset_ms
        try:
            local_before = int(round(_time.time() * 1000))
            data = self._public_get("/api/v3/time")
            local_after = int(round(_time.time() * 1000))
            server = int(data.get("serverTime") or 0)
            if server > 0:
                # Compensate for round-trip: compare against the midpoint.
                self._time_offset_ms = server - (local_before + local_after) // 2
                self._time_offset_ts = now
                if abs(self._time_offset_ms) > 1000:
                    _binance_log.warning(
                        "[BINANCE SPOT] clock drift %sms vs exchange - compensating",
                        self._time_offset_ms,
                    )
        except Exception as exc:
            _binance_log.warning("[BINANCE SPOT] server time sync failed: %s", exc)
        return self._time_offset_ms

    @staticmethod
    def _binance_error_text(resp) -> str:
        """Binance puts the real reason in the JSON body of a 4xx. raise_for_status
        throws that away, which turns every failure into an opaque 400."""
        try:
            body = resp.json()
        except Exception:
            return (resp.text or "")[:300]
        if isinstance(body, dict) and ("code" in body or "msg" in body):
            return f"Binance {body.get('code', '?')}: {body.get('msg') or 'unknown error'}"
        return str(body)[:300]

    def _signed_request(self, method: str, path: str, *, params: dict | None = None):
        if not self._is_configured():
            raise Exception("Binance API not configured")
        payload = {key: value for key, value in dict(params or {}).items() if value not in (None, "")}
        payload.setdefault("timestamp", int(round(_time.time() * 1000)) + self._server_time_offset_ms())
        payload.setdefault("recvWindow", _RECV_WINDOW_MS)
        query = urlencode(payload, doseq=True)
        signature = hmac.new(self.api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
        signed_params = {**payload, "signature": signature}
        url = f"{self.base_url}{path}"
        headers = {"Content-Type": "application/json", "X-MBX-APIKEY": self.api_key}
        resp = _request_with_retry(method, url, headers=headers, params=signed_params, timeout=30)
        if resp.status_code >= 400:
            detail = self._binance_error_text(resp)
            # -1021 is timestamp drift: force a re-sync so the next call recovers.
            if "-1021" in detail:
                self._time_offset_ts = 0.0
            raise Exception(detail)
        return resp.json()

    @classmethod
    def _app_symbol_to_pair(cls, symbol: str) -> str:
        raw = cls.normalize_app_symbol(symbol)
        if raw.endswith("USD") and not raw.endswith("USDT"):
            return raw + "T"
        return raw

    def to_broker_symbol(self, symbol: str) -> str:
        return self._app_symbol_to_pair(symbol)

    def from_broker_symbol(self, symbol: str) -> str:
        return self.normalize_app_symbol(symbol)

    @staticmethod
    def _filters_by_type(product: dict) -> dict:
        return {
            str(item.get("filterType") or ""): item
            for item in (product or {}).get("filters", [])
            if isinstance(item, dict)
        }

    def _normalize_product(self, raw: dict) -> dict:
        filters = self._filters_by_type(raw)
        lot = filters.get("LOT_SIZE", {})
        market_lot = filters.get("MARKET_LOT_SIZE", {}) or lot
        price_filter = filters.get("PRICE_FILTER", {})
        min_notional = filters.get("MIN_NOTIONAL", {}) or filters.get("NOTIONAL", {})
        return {
            "id": raw.get("symbol"),
            "symbol": self.from_broker_symbol(raw.get("symbol")),
            "broker_symbol": raw.get("symbol"),
            "pair": raw.get("symbol"),
            "state": "live" if raw.get("status") == "TRADING" else str(raw.get("status") or "").lower(),
            "contract_type": "spot",
            "contract_value": "1",
            "notional_type": "linear",
            "base_asset": raw.get("baseAsset"),
            "quote_asset": raw.get("quoteAsset"),
            "quantity_precision": raw.get("baseAssetPrecision"),
            "quote_precision": raw.get("quoteAssetPrecision"),
            "min_qty": lot.get("minQty"),
            "max_qty": lot.get("maxQty"),
            "step_size": lot.get("stepSize"),
            "market_min_qty": market_lot.get("minQty"),
            "market_step_size": market_lot.get("stepSize"),
            "tick_size": price_filter.get("tickSize"),
            "min_notional": min_notional.get("minNotional") or min_notional.get("notional"),
            "quote_order_qty_market_allowed": bool(raw.get("quoteOrderQtyMarketAllowed", True)),
            "max_leverage": 1,
            "default_leverage": 1,
            **raw,
        }

    def get_products(self, force_refresh: bool = False) -> list:
        now = _time.time()
        if self._products_cache and not force_refresh and (now - self._products_ts) < self._CACHE_TTL:
            return list(self._products_cache)
        payload = self._public_get("/api/v3/exchangeInfo")
        symbols = payload.get("symbols", []) if isinstance(payload, dict) else []
        products = [
            self._normalize_product(item)
            for item in symbols
            if item.get("quoteAsset") == self.quote_asset and item.get("status") == "TRADING"
        ]
        self._products_cache = products
        self._products_ts = now
        return list(products)

    def get_perpetual_futures(self) -> list:
        # Name kept for the legacy engine contract; returns spot products.
        return self.get_products()

    def get_product_by_symbol(self, symbol: str):
        pair = self.to_broker_symbol(symbol)
        for product in self.get_products():
            if str(product.get("broker_symbol") or product.get("symbol") or "").upper() == pair:
                return product
        return None

    def get_leverage_info(self, symbol: str) -> dict:
        return {
            "max_leverage": 1,
            "default": 1,
            "options": [1],
            "initial_margin": 100.0,
            "maintenance_margin": 0.0,
            "mode": "spot",
        }

    def get_candles(self, symbol: str, resolution: str = "5m", start: str = None, end: str = None) -> pd.DataFrame:
        pair = self.to_broker_symbol(symbol)
        params = {"symbol": pair, "interval": _KLINE_INTERVALS.get(resolution, "5m"), "limit": 1000}
        if start:
            params["startTime"] = int(datetime.strptime(start, "%Y-%m-%d").timestamp() * 1000)
        if end:
            params["endTime"] = int(datetime.strptime(end, "%Y-%m-%d").timestamp() * 1000)
        candles = self._public_get("/api/v3/klines", params=params)
        if not candles:
            return pd.DataFrame()
        df = pd.DataFrame(
            candles,
            columns=[
                "time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "quote_volume",
                "trades",
                "taker_base_volume",
                "taker_quote_volume",
                "ignore",
            ],
        )
        df["datetime"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        for col in ("open", "high", "low", "close", "volume"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df.set_index("datetime", inplace=True)
        df.sort_index(inplace=True)
        return df[["open", "high", "low", "close", "volume"]]

    async def async_get_candles(self, symbol: str, **kwargs) -> pd.DataFrame:
        return await asyncio.to_thread(self.get_candles, symbol, **kwargs)

    def get_ticker(self, symbol: str) -> dict:
        pair = self.to_broker_symbol(symbol)
        try:
            ticker = self._public_get("/api/v3/ticker/24hr", params={"symbol": pair})
            last_price = self.coerce_float(ticker.get("lastPrice"), 0.0)
            return {
                "symbol": self.from_broker_symbol(pair),
                "broker_symbol": pair,
                "mark_price": last_price,
                "last_price": last_price,
                "close": last_price,
                "volume_24h": self.coerce_float(ticker.get("volume"), 0.0),
                "turnover_24h": self.coerce_float(ticker.get("quoteVolume"), 0.0),
                "open_interest": 0.0,
                "funding_rate": 0.0,
                "price_change_24h": self.coerce_float(ticker.get("priceChangePercent"), 0.0),
                "high_24h": self.coerce_float(ticker.get("highPrice"), 0.0),
                "low_24h": self.coerce_float(ticker.get("lowPrice"), 0.0),
            }
        except Exception as exc:
            _binance_log.warning("[BINANCE SPOT] Ticker error for %s: %s", pair, exc)
            return {"symbol": self.from_broker_symbol(pair), "mark_price": 0.0, "last_price": 0.0}

    def get_tickers_bulk(self) -> list:
        try:
            rows = self._public_get("/api/v3/ticker/24hr")
            return [
                {
                    "symbol": item.get("symbol"),
                    "mark_price": self.coerce_float(item.get("lastPrice"), 0.0),
                    "last_price": self.coerce_float(item.get("lastPrice"), 0.0),
                    "close": self.coerce_float(item.get("lastPrice"), 0.0),
                    "volume": self.coerce_float(item.get("volume"), 0.0),
                    "turnover": self.coerce_float(item.get("quoteVolume"), 0.0),
                    "price_change_percent_24h": self.coerce_float(item.get("priceChangePercent"), 0.0),
                    "high": self.coerce_float(item.get("highPrice"), 0.0),
                    "low": self.coerce_float(item.get("lowPrice"), 0.0),
                }
                for item in rows or []
                if isinstance(item, dict)
            ]
        except Exception as exc:
            _binance_log.warning("[BINANCE SPOT] Bulk ticker error: %s", exc)
            return []

    def _account(self) -> dict:
        return self._signed_request("GET", "/api/v3/account")

    def _spot_balance_rows(self, account: dict) -> list[dict]:
        rows = []
        for item in account.get("balances", []) if isinstance(account, dict) else []:
            asset = str(item.get("asset") or "").upper()
            free = self.coerce_float(item.get("free"), 0.0)
            locked = self.coerce_float(item.get("locked"), 0.0)
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

    def get_wallet(self) -> dict | list:
        if not self._is_configured():
            return {"error": "API not configured"}
        try:
            return self._spot_balance_rows(self._account())
        except Exception as exc:
            _binance_log.warning("[BINANCE SPOT] Wallet error: %s", exc)
            return {"error": str(exc)}

    def _balance_position(self, wallet_row: dict) -> dict | None:
        asset = str(wallet_row.get("asset_symbol") or "").upper()
        total_qty = self.coerce_float(wallet_row.get("total_balance"), 0.0)
        if not asset or asset == self.quote_asset or total_qty <= 0:
            return None
        pair = f"{asset}{self.quote_asset}"
        ticker = self.get_ticker(pair)
        mark_price = self.coerce_float(ticker.get("mark_price") or ticker.get("last_price"), 0.0)
        return {
            "product_id": pair,
            "product_symbol": self.from_broker_symbol(pair),
            "symbol": self.from_broker_symbol(pair),
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
        pair = self.to_broker_symbol(product_id)
        for position in self.get_positions():
            if str(position.get("product_id") or "").upper() == pair:
                return position
        return {}

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
            return "LIMIT"
        if raw in {"stop_limit", "stop_limit_order", "stop_loss_limit"}:
            return "STOP_LOSS_LIMIT"
        return "MARKET"

    @staticmethod
    def _filter_positive_float(value, default: float = 0.0) -> float:
        parsed = BaseBroker.coerce_float(value, default)
        return parsed if parsed > 0 else default

    def _round_price_to_tick(self, pair: str, price: float) -> str:
        product = self.get_product_by_symbol(pair) or {}
        tick = str(product.get("tick_size") or "") or "0.01"
        return self._decimal_floor(self.coerce_float(price, 0.0), tick)

    def _spot_order_quantity(
        self, pair: str, size: float, side: str, order_type: str, price: float
    ) -> tuple[dict, str]:
        product = self.get_product_by_symbol(pair) or {}
        if not product:
            return {}, f"Unknown Binance Spot symbol {pair}"
        filters = self._filters_by_type(product)
        lot = filters.get("MARKET_LOT_SIZE", {}) if self._order_type(order_type) == "MARKET" else {}
        if not lot or self._filter_positive_float(lot.get("stepSize"), 0.0) <= 0:
            lot = filters.get("LOT_SIZE", {})
        step = lot.get("stepSize") or "0.000001"
        min_qty = self._filter_positive_float(lot.get("minQty"), 0.0)
        min_notional_filter = filters.get("MIN_NOTIONAL", {}) or filters.get("NOTIONAL", {})
        min_notional = self._filter_positive_float(
            min_notional_filter.get("minNotional") or min_notional_filter.get("notional"),
            0.0,
        )
        notional_size = self.coerce_float(size, 0.0)
        if notional_size <= 0:
            return {}, "Spot order size must be greater than zero"
        if min_notional and notional_size < min_notional:
            return {}, f"Spot order size {notional_size:g} is below Binance minimum notional {min_notional:g}"

        side_upper = str(side or "").upper()
        order_type_upper = self._order_type(order_type)
        if side_upper == "BUY" and order_type_upper == "MARKET" and product.get("quote_order_qty_market_allowed", True):
            return {"quoteOrderQty": self._decimal_floor(notional_size, "0.00000001")}, ""

        if price <= 0:
            return {}, f"Unable to resolve Binance Spot price for {pair}"
        qty = self._decimal_floor(notional_size / price, step)
        if self.coerce_float(qty, 0.0) < min_qty:
            return {}, f"Spot order quantity {qty} is below Binance minimum quantity {min_qty:g}"
        return {"quantity": qty}, ""

    def _base_qty_payload(self, pair: str, base_qty: float, price: float) -> tuple[dict, str]:
        product = self.get_product_by_symbol(pair) or {}
        if not product:
            return {}, f"Unknown Binance Spot symbol {pair}"
        filters = self._filters_by_type(product)
        lot = filters.get("LOT_SIZE", {})
        step = lot.get("stepSize") or "0.000001"
        qty = self._decimal_floor(self.coerce_float(base_qty, 0.0), step)
        qty_value = self.coerce_float(qty, 0.0)
        if qty_value <= 0:
            return {}, "Spot order base quantity must be greater than zero"
        min_qty = self._filter_positive_float(lot.get("minQty"), 0.0)
        if qty_value < min_qty:
            return {}, f"Spot order quantity {qty} is below Binance minimum quantity {min_qty:g}"
        min_notional_filter = filters.get("MIN_NOTIONAL", {}) or filters.get("NOTIONAL", {})
        min_notional = self._filter_positive_float(
            min_notional_filter.get("minNotional") or min_notional_filter.get("notional"), 0.0
        )
        if min_notional and price > 0 and qty_value * price < min_notional:
            return {}, (f"Spot order notional {qty_value * price:g} is below Binance minimum notional {min_notional:g}")
        return {"quantity": qty}, ""

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
        pair = self.to_broker_symbol(product_id)
        side_upper = str(side or "").upper()
        if side_upper not in {"BUY", "SELL"}:
            return {"error": "Binance Spot supports only buy and sell orders"}

        order_type_upper = self._order_type(order_type)
        ticker = self.get_ticker(pair)
        price_for_qty = self.coerce_float(limit_price or ticker.get("mark_price") or ticker.get("last_price"), 0.0)
        if base_qty is not None:
            quantity_payload, error = self._base_qty_payload(pair, base_qty, price_for_qty)
        else:
            quantity_payload, error = self._spot_order_quantity(
                pair,
                self.coerce_float(size, 0.0),
                side_upper,
                order_type,
                price_for_qty,
            )
        if error:
            return {"error": error}

        payload = {
            "symbol": pair,
            "side": side_upper,
            "type": order_type_upper,
            "newOrderRespType": "FULL",
            **quantity_payload,
        }
        if client_order_id:
            payload["newClientOrderId"] = str(client_order_id)
        if order_type_upper in {"LIMIT", "STOP_LOSS_LIMIT"}:
            if not limit_price:
                return {"error": "Limit price is required for Binance Spot limit orders"}
            tick_price = self._round_price_to_tick(pair, self.coerce_float(limit_price, 0.0))
            if self.coerce_float(tick_price, 0.0) <= 0:
                return {"error": f"Unable to resolve a valid limit price for {pair}"}
            payload.update({"price": tick_price, "timeInForce": "GTC"})
        if order_type_upper == "STOP_LOSS_LIMIT":
            if not stop_price:
                return {"error": "Stop price is required for Binance Spot stop-limit orders"}
            tick_stop = self._round_price_to_tick(pair, self.coerce_float(stop_price, 0.0))
            if self.coerce_float(tick_stop, 0.0) <= 0:
                return {"error": f"Unable to resolve a valid stop price for {pair}"}
            payload["stopPrice"] = tick_stop
        try:
            result = self._signed_request("POST", "/api/v3/order", params=payload)
            if isinstance(result, dict):
                result.setdefault("id", result.get("orderId"))
                result.setdefault("product_symbol", pair)
                result.setdefault("symbol", pair)
                self._attach_spot_order_fill_fields(result, pair)
            return result
        except Exception as exc:
            _binance_log.warning("[BINANCE SPOT] Order error: %s", exc)
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
            order_id = result.get("orderId")
            checked = self.get_order(product_id, order_id) if order_id else {}
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
            "verification_summary": "Binance order verified"
            if verified
            else "Binance order submitted but not filled yet",
            "fill_price": fill_price or None,
            "verified_at_attempt": 1,
            "order_ack_ms": order_ack_ms,
            "broker_latency_ms": round((_time.perf_counter() - started_at) * 1000, 1),
        }

    def get_order(self, product_id: str, order_id=None, client_order_id: str = None) -> dict:
        if not self._is_configured() or not (order_id or client_order_id):
            return {}
        params = {"symbol": self.to_broker_symbol(product_id)}
        if order_id:
            params["orderId"] = order_id
        else:
            params["origClientOrderId"] = client_order_id
        return self._signed_request("GET", "/api/v3/order", params=params)

    def cancel_order(self, order_id: str, product_id: str = "") -> dict:
        if not self._is_configured():
            return {"error": "API not configured"}
        try:
            return self._signed_request(
                "DELETE", "/api/v3/order", params={"symbol": self.to_broker_symbol(product_id), "orderId": order_id}
            )
        except Exception as exc:
            _binance_log.warning("[BINANCE SPOT] Cancel error: %s", exc)
            return {"error": str(exc)}

    def get_orders(self, product_id: str = None, state: str = "open") -> list:
        if not self._is_configured():
            return []
        pair = self.to_broker_symbol(product_id or "BTCUSDT")
        try:
            if str(state or "").lower() == "open":
                return self._signed_request("GET", "/api/v3/openOrders", params={"symbol": pair})
            return self._signed_request("GET", "/api/v3/allOrders", params={"symbol": pair, "limit": 100})
        except Exception as exc:
            _binance_log.warning("[BINANCE SPOT] Orders error: %s", exc)
            return []

    def _trade_fee_in_quote(self, row: dict) -> float:
        commission = self.coerce_float(row.get("commission"), 0.0)
        commission_asset = str(row.get("commissionAsset") or "").upper()
        symbol = str(row.get("symbol") or "").upper()
        product = self.get_product_by_symbol(symbol) or {}
        base_asset = str(product.get("base_asset") or "").upper()
        quote_asset = str(product.get("quote_asset") or self.quote_asset).upper()
        price = self.coerce_float(row.get("price"), 0.0)
        if commission_asset == quote_asset:
            return round(commission, 8)
        if commission_asset == base_asset and price > 0:
            return round(commission * price, 8)
        return 0.0

    def _attach_spot_order_fill_fields(self, result: dict, pair: str) -> dict:
        if not isinstance(result, dict):
            return result
        fills = result.get("fills") if isinstance(result.get("fills"), list) else []
        executed_qty = self.coerce_float(result.get("executedQty"), 0.0)
        quote_qty = self.coerce_float(result.get("cummulativeQuoteQty"), 0.0)
        weighted_quote = 0.0
        fill_qty = 0.0
        fee_quote = 0.0
        product = self.get_product_by_symbol(pair) or {}
        base_asset = str(product.get("base_asset") or "").upper()
        quote_asset = str(product.get("quote_asset") or self.quote_asset).upper()
        for fill in fills:
            price = self.coerce_float(fill.get("price"), 0.0)
            qty = self.coerce_float(fill.get("qty"), 0.0)
            commission = self.coerce_float(fill.get("commission"), 0.0)
            commission_asset = str(fill.get("commissionAsset") or "").upper()
            weighted_quote += price * qty
            fill_qty += qty
            if commission_asset == quote_asset:
                fee_quote += commission
            elif commission_asset == base_asset and price > 0:
                fee_quote += commission * price

        avg_price = weighted_quote / fill_qty if fill_qty > 0 else 0.0
        if avg_price <= 0 and executed_qty > 0 and quote_qty > 0:
            avg_price = quote_qty / executed_qty
        if avg_price > 0:
            result.setdefault("avgPrice", str(round(avg_price, 8)))
            result.setdefault("average_fill_price", round(avg_price, 8))
            result.setdefault("fill_price", round(avg_price, 8))
        if executed_qty > 0:
            result.setdefault("filled_size", executed_qty)
            result.setdefault("size", executed_qty)
        if quote_qty > 0:
            result.setdefault("quote_size", quote_qty)
        result.setdefault("paid_commission", round(fee_quote, 8))
        return result

    _MAJOR_ASSETS = ("BTC", "ETH", "SOL", "XRP", "DOGE", "BNB")

    def _order_history_symbols(self) -> list[str]:
        """Symbols to pull trade history for: the majors plus every asset the
        account currently holds, so trades in any held coin are included."""
        symbols = [f"{asset}{self.quote_asset}" for asset in self._MAJOR_ASSETS]
        seen = set(symbols)
        try:
            wallet = self.get_wallet()
        except Exception:
            wallet = None
        for row in wallet if isinstance(wallet, list) else []:
            asset = str(row.get("asset_symbol") or "").upper()
            if not asset or asset == self.quote_asset:
                continue
            pair = f"{asset}{self.quote_asset}"
            if pair not in seen:
                seen.add(pair)
                symbols.append(pair)
        return symbols

    def get_order_history(self) -> list:
        if not self._is_configured():
            return []
        # NOTE: Binance /api/v3/myTrades rejects any startTime/endTime window
        # longer than 24h. Omit the window entirely and fetch the most recent
        # trades per symbol (limit only) so history is not silently dropped.
        per_symbol_limit = int(getattr(config, "BINANCE_SPOT_TRADE_LIMIT", 0) or 500)
        per_symbol_limit = max(1, min(per_symbol_limit, 1000))
        trades = []
        for symbol in self._order_history_symbols():
            try:
                rows = self._signed_request(
                    "GET",
                    "/api/v3/myTrades",
                    params={"symbol": symbol, "limit": per_symbol_limit},
                )
            except Exception as exc:
                _binance_log.warning("[BINANCE SPOT] Trades error for %s: %s", symbol, exc)
                continue
            for row in rows or []:
                row = dict(row)
                trade_id = row.get("id")
                order_id = row.get("orderId")
                row["trade_id"] = trade_id
                row["id"] = f"{symbol}-{trade_id}"
                row.setdefault("order_id", order_id)
                row.setdefault("product_symbol", self.from_broker_symbol(row.get("symbol")))
                row.setdefault("side", "buy" if row.get("isBuyer") else "sell")
                row.setdefault("average_fill_price", row.get("price"))
                row.setdefault("fill_price", row.get("price"))
                row.setdefault("size", row.get("qty"))
                row.setdefault("filled_size", row.get("qty"))
                row.setdefault("quote_size", row.get("quoteQty"))
                row.setdefault("paid_commission", self._trade_fee_in_quote(row))
                row.setdefault("paid_commission_native", row.get("commission"))
                row.setdefault("commission_asset", row.get("commissionAsset"))
                row.setdefault("state", "closed")
                row.setdefault("order_type", "spot trade")
                row.setdefault("product", {"notional_type": "linear", "contract_value": "1"})
                row.setdefault(
                    "updated_at",
                    datetime.fromtimestamp(int(row.get("time", 0)) / 1000, tz=timezone.utc).isoformat(),
                )
                trades.append(row)
        trades.sort(key=lambda item: item.get("time") or 0, reverse=True)
        return trades

    def get_convert_history(self, days: int = 30) -> list:
        """
        Binance Convert trades. Convert is an OTC quote engine, NOT the
        orderbook — its fills never appear in /api/v3/myTrades, so a position
        exited via Convert looks permanently open to anything reading spot
        trades alone. This is the usual escape hatch for dust too small to sell
        (below LOT_SIZE or MIN_NOTIONAL), which makes it common, not exotic.

        The endpoint caps each query at 30 days, so longer ranges are paged.
        """
        if not self._is_configured():
            return []
        now_ms = int(round(_time.time() * 1000))
        window_ms = 30 * 24 * 60 * 60 * 1000
        remaining_ms = max(int(days), 1) * 24 * 60 * 60 * 1000
        rows: list = []
        end = now_ms
        while remaining_ms > 0:
            span = min(window_ms, remaining_ms)
            try:
                payload = self._signed_request(
                    "GET",
                    "/sapi/v1/convert/tradeFlow",
                    params={"startTime": end - span, "endTime": end, "limit": 1000},
                )
            except Exception as exc:
                _binance_log.warning("[BINANCE SPOT] Convert history error: %s", exc)
                break
            batch = (payload or {}).get("list") or []
            for raw in batch:
                row = dict(raw)
                from_asset = str(row.get("fromAsset") or "")
                to_asset = str(row.get("toAsset") or "")
                from_amount = self.coerce_float(row.get("fromAmount"), 0.0)
                to_amount = self.coerce_float(row.get("toAmount"), 0.0)
                quote = self.quote_asset
                # Selling the base for quote is a sell; buying it back is a buy.
                if to_asset == quote:
                    side, base, base_qty, quote_qty = "sell", from_asset, from_amount, to_amount
                elif from_asset == quote:
                    side, base, base_qty, quote_qty = "buy", to_asset, to_amount, from_amount
                else:
                    side, base, base_qty, quote_qty = "convert", from_asset, from_amount, to_amount
                price = (quote_qty / base_qty) if base_qty else 0.0
                ts = int(row.get("createTime") or 0)
                rows.append(
                    {
                        "id": f"convert-{row.get('orderId')}",
                        "order_id": row.get("orderId"),
                        "source": "convert",
                        "symbol": f"{base}{quote}" if base and side != "convert" else f"{from_asset}{to_asset}",
                        "product_symbol": f"{base}{quote}" if base and side != "convert" else f"{from_asset}{to_asset}",
                        "side": side,
                        "price": price,
                        "average_fill_price": price,
                        "fill_price": price,
                        "size": base_qty,
                        "filled_size": base_qty,
                        "quote_size": quote_qty,
                        "from_asset": from_asset,
                        "to_asset": to_asset,
                        "from_amount": from_amount,
                        "to_amount": to_amount,
                        "paid_commission": 0.0,  # Convert prices the cost into the spread
                        "state": "closed",
                        "order_type": "convert",
                        "status": row.get("orderStatus"),
                        "time": ts,
                        "updated_at": datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat() if ts else "",
                    }
                )
            if len(batch) < 1000:
                end -= span
                remaining_ms -= span
            else:
                end = min(int(r.get("createTime") or end) for r in batch) - 1
                remaining_ms -= span
        rows.sort(key=lambda item: item.get("time") or 0, reverse=True)
        return rows

    def set_leverage(self, product_id: str, leverage: int) -> dict:
        return {"status": "ok", "message": "Binance Spot uses cash balance only; leverage is fixed at 1x."}

    def get_funding_history(self, symbol: str) -> list:
        return []
