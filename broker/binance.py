"""
broker/binance.py - Binance USD-M futures adapter for CryptoForge.

The adapter targets Binance USDⓈ-M perpetual futures. Public market data works
without credentials; account, position, and order methods require API keys.
"""

import asyncio
import hashlib
import hmac
import logging
import time as _time
from datetime import datetime, timedelta
from decimal import ROUND_DOWN, Decimal
from urllib.parse import urlencode

import pandas as pd
import requests
from requests.adapters import HTTPAdapter

import config

from .base import BaseBroker

_binance_log = logging.getLogger("cryptoforge.binance")
_RETRYABLE_STATUSES = {418, 429, 500, 502, 503, 504}

_http_session = requests.Session()
_adapter = HTTPAdapter(pool_connections=4, pool_maxsize=10, max_retries=0)
_http_session.mount("https://", _adapter)
_http_session.mount("http://", _adapter)


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


class BinanceClient(BaseBroker):
    broker_name = "binance"
    display_name = "Binance Futures"

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
        self.api_key = config.BINANCE_API_KEY
        self.api_secret = config.BINANCE_API_SECRET
        self.base_url = config.BINANCE_FUTURES_BASE_URL.rstrip("/")
        self.testnet = bool(getattr(config, "BINANCE_FUTURES_TESTNET", False))
        self.margin_asset = config.BINANCE_MARGIN_ASSET.upper()
        self._products_cache = None
        self._products_ts = 0.0
        self._CACHE_TTL = 3600

    def get_market_feed_kind(self) -> str:
        return "polling"

    def _is_configured(self) -> bool:
        return (
            self.api_key != "YOUR_BINANCE_API_KEY_HERE"
            and self.api_secret != "YOUR_BINANCE_API_SECRET_HERE"
            and len(str(self.api_key or "")) > 5
            and len(str(self.api_secret or "")) > 5
        )

    def _public_get(self, path: str, *, params: dict | None = None):
        url = f"{self.base_url}{path}"
        resp = _request_with_retry("GET", url, headers={"Content-Type": "application/json"}, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _signed_request(self, method: str, path: str, *, params: dict | None = None):
        if not self._is_configured():
            raise Exception("Binance API not configured")
        payload = {key: value for key, value in dict(params or {}).items() if value not in (None, "")}
        payload.setdefault("timestamp", int(round(_time.time() * 1000)))
        query = urlencode(payload, doseq=True)
        signature = hmac.new(self.api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
        signed_params = {**payload, "signature": signature}
        url = f"{self.base_url}{path}"
        headers = {"Content-Type": "application/json", "X-MBX-APIKEY": self.api_key}
        resp = _request_with_retry(method, url, headers=headers, params=signed_params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    @classmethod
    def _app_symbol_to_pair(cls, symbol: str) -> str:
        raw = cls.normalize_app_symbol(symbol)
        if raw.endswith("USD") and not raw.endswith("USDT"):
            return raw + "T"
        return raw

    @classmethod
    def _pair_to_app_symbol(cls, symbol: str) -> str:
        return cls.normalize_app_symbol(symbol)

    def to_broker_symbol(self, symbol: str) -> str:
        return self._app_symbol_to_pair(symbol)

    def from_broker_symbol(self, symbol: str) -> str:
        return self._pair_to_app_symbol(symbol)

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
        price_filter = filters.get("PRICE_FILTER", {})
        min_notional = filters.get("MIN_NOTIONAL", {})
        return {
            "id": raw.get("symbol"),
            "symbol": self.from_broker_symbol(raw.get("symbol")),
            "broker_symbol": raw.get("symbol"),
            "pair": raw.get("pair") or raw.get("symbol"),
            "state": "live" if raw.get("status") == "TRADING" else str(raw.get("status") or "").lower(),
            "contract_type": "perpetual_futures",
            "contract_value": "1",
            "notional_type": "linear",
            "base_asset": raw.get("baseAsset"),
            "quote_asset": raw.get("quoteAsset"),
            "margin_asset": raw.get("marginAsset"),
            "quantity_precision": raw.get("quantityPrecision"),
            "price_precision": raw.get("pricePrecision"),
            "min_qty": lot.get("minQty"),
            "max_qty": lot.get("maxQty"),
            "step_size": lot.get("stepSize"),
            "tick_size": price_filter.get("tickSize"),
            "min_notional": min_notional.get("notional"),
            "max_leverage": 125,
            "default_leverage": 10,
            **raw,
        }

    def get_products(self, force_refresh: bool = False) -> list:
        now = _time.time()
        if self._products_cache and not force_refresh and (now - self._products_ts) < self._CACHE_TTL:
            return list(self._products_cache)
        payload = self._public_get("/fapi/v1/exchangeInfo")
        symbols = payload.get("symbols", []) if isinstance(payload, dict) else []
        products = [
            self._normalize_product(item)
            for item in symbols
            if item.get("contractType") == "PERPETUAL"
            and item.get("quoteAsset") in {"USDT", "USDC"}
            and item.get("status") == "TRADING"
        ]
        self._products_cache = products
        self._products_ts = now
        return list(products)

    def get_perpetual_futures(self) -> list:
        return self.get_products()

    def get_product_by_symbol(self, symbol: str):
        pair = self.to_broker_symbol(symbol)
        for product in self.get_products():
            if str(product.get("broker_symbol") or product.get("symbol") or "").upper() == pair:
                return product
        return None

    def get_leverage_info(self, symbol: str) -> dict:
        return {
            "max_leverage": 125,
            "default": 10,
            "options": self.build_standard_leverage_options(125),
            "initial_margin": 0.8,
            "maintenance_margin": 0.4,
        }

    def get_candles(self, symbol: str, resolution: str = "5m", start: str = None, end: str = None) -> pd.DataFrame:
        pair = self.to_broker_symbol(symbol)
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
        params = {"symbol": pair, "interval": interval_map.get(resolution, "5m"), "limit": 1500}
        if start:
            params["startTime"] = int(datetime.strptime(start, "%Y-%m-%d").timestamp() * 1000)
        if end:
            params["endTime"] = int(datetime.strptime(end, "%Y-%m-%d").timestamp() * 1000)
        candles = self._public_get("/fapi/v1/klines", params=params)
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
            mark = self._public_get("/fapi/v1/premiumIndex", params={"symbol": pair})
            ticker = self._public_get("/fapi/v1/ticker/24hr", params={"symbol": pair})
            mark_price = self.coerce_float(mark.get("markPrice"), 0.0)
            last_price = self.coerce_float(ticker.get("lastPrice"), mark_price)
            return {
                "symbol": self.from_broker_symbol(pair),
                "broker_symbol": pair,
                "mark_price": mark_price,
                "last_price": last_price,
                "close": last_price,
                "volume_24h": self.coerce_float(ticker.get("volume"), 0.0),
                "turnover_24h": self.coerce_float(ticker.get("quoteVolume"), 0.0),
                "open_interest": 0.0,
                "funding_rate": self.coerce_float(mark.get("lastFundingRate"), 0.0),
                "price_change_24h": self.coerce_float(ticker.get("priceChangePercent"), 0.0),
                "high_24h": self.coerce_float(ticker.get("highPrice"), 0.0),
                "low_24h": self.coerce_float(ticker.get("lowPrice"), 0.0),
            }
        except Exception as exc:
            _binance_log.warning("[BINANCE] Ticker error for %s: %s", pair, exc)
            return {"symbol": self.from_broker_symbol(pair), "mark_price": 0.0, "last_price": 0.0}

    def get_tickers_bulk(self) -> list:
        try:
            rows = self._public_get("/fapi/v1/ticker/24hr")
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
            _binance_log.warning("[BINANCE] Bulk ticker error: %s", exc)
            return []

    def _account(self) -> dict:
        return self._signed_request("GET", "/fapi/v3/account")

    def get_wallet(self) -> dict:
        if not self._is_configured():
            return {"error": "API not configured"}
        try:
            account = self._account()
            row = {
                "asset_symbol": self.margin_asset,
                "asset": self.margin_asset,
                "balance": account.get("totalWalletBalance", "0"),
                "wallet_balance": account.get("totalWalletBalance", "0"),
                "total_balance": account.get("totalWalletBalance", "0"),
                "equity": account.get("totalMarginBalance", "0"),
                "available_balance": account.get("availableBalance", "0"),
                "free_balance": account.get("availableBalance", "0"),
                "blocked_margin": str(
                    max(
                        0.0,
                        self.coerce_float(account.get("totalWalletBalance"), 0.0)
                        - self.coerce_float(account.get("availableBalance"), 0.0),
                    )
                ),
                "order_margin": account.get("totalOpenOrderInitialMargin", "0"),
                "position_margin": account.get("totalPositionInitialMargin", "0"),
                "unrealized_pnl": account.get("totalUnrealizedProfit", "0"),
                "raw": account,
            }
            return [row]
        except Exception as exc:
            _binance_log.warning("[BINANCE] Wallet error: %s", exc)
            return {"error": str(exc)}

    def _normalize_position(self, item: dict) -> dict:
        pair = str(item.get("symbol") or "").upper()
        position_amt = self.coerce_float(item.get("positionAmt"), 0.0)
        mark_price = self.coerce_float(item.get("markPrice"), 0.0)
        notional = self.coerce_float(item.get("notional"), position_amt * mark_price)
        if notional == 0 and position_amt and mark_price:
            notional = position_amt * mark_price
        signed_size = abs(notional) if position_amt >= 0 else -abs(notional)
        return {
            "product_id": pair,
            "product_symbol": self.from_broker_symbol(pair),
            "symbol": self.from_broker_symbol(pair),
            "size": round(signed_size, 8),
            "base_size": position_amt,
            "entry_price": self.coerce_float(item.get("entryPrice"), 0.0),
            "mark_price": mark_price,
            "unrealized_pnl": self.coerce_float(item.get("unRealizedProfit") or item.get("unrealizedProfit"), 0.0),
            "realized_pnl": 0.0,
            "margin": self.coerce_float(item.get("isolatedMargin") or item.get("initialMargin"), 0.0),
            "liquidation_price": self.coerce_float(item.get("liquidationPrice"), 0.0),
            "leverage": self.coerce_float(item.get("leverage"), 0.0),
            "margin_type": item.get("marginType") or "",
            "position_side": item.get("positionSide") or "BOTH",
            "raw": item,
        }

    def get_positions(self) -> list:
        if not self._is_configured():
            return []
        try:
            rows = self._signed_request("GET", "/fapi/v3/positionRisk")
            return [
                self._normalize_position(item)
                for item in rows or []
                if abs(self.coerce_float(item.get("positionAmt"), 0.0)) > 0
            ]
        except Exception as exc:
            _binance_log.warning("[BINANCE] Positions error: %s", exc)
            return []

    def get_position(self, product_id: str, strict: bool = False) -> dict:
        if not self._is_configured():
            return {}
        pair = str(product_id or "").strip().upper()
        try:
            rows = self._signed_request("GET", "/fapi/v3/positionRisk", params={"symbol": pair})
            for item in rows or []:
                if str(item.get("symbol") or "").upper() == pair:
                    return self._normalize_position(item)
            return {}
        except Exception as exc:
            if strict:
                raise
            _binance_log.warning("[BINANCE] Position error for %s: %s", pair, exc)
            return {}

    @staticmethod
    def _decimal_floor(value: float, step: str) -> str:
        step_dec = Decimal(str(step or "0.001"))
        value_dec = Decimal(str(max(value, 0.0)))
        if step_dec <= 0:
            return format(value_dec, "f")
        qty = (value_dec / step_dec).to_integral_value(rounding=ROUND_DOWN) * step_dec
        return format(qty.normalize(), "f")

    def _resolve_quantity(self, pair: str, size: float, price: float = 0.0) -> str:
        product = self.get_product_by_symbol(pair)
        filters = self._filters_by_type(product or {})
        lot = filters.get("LOT_SIZE", {})
        step = lot.get("stepSize") or "0.001"
        min_qty = self.coerce_float(lot.get("minQty"), 0.0)
        qty = self.coerce_float(size, 0.0)
        if price > 0:
            qty = qty / price
        qty = max(qty, min_qty)
        return self._decimal_floor(qty, step)

    @staticmethod
    def _order_type(order_type: str) -> str:
        raw = str(order_type or "").lower()
        if raw in {"limit", "limit_order"}:
            return "LIMIT"
        return "MARKET"

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
        ticker = self.get_ticker(pair)
        price_for_qty = self.coerce_float(limit_price or ticker.get("mark_price") or ticker.get("last_price"), 0.0)
        quantity = self._resolve_quantity(pair, self.coerce_float(size, 0.0), price=price_for_qty)
        payload = {
            "symbol": pair,
            "side": str(side or "").upper(),
            "type": self._order_type(order_type),
            "quantity": quantity,
            "reduceOnly": "true" if reduce_only else "false",
            "newOrderRespType": "RESULT",
        }
        if payload["type"] == "LIMIT":
            payload.update({"price": str(limit_price), "timeInForce": "GTC"})
        try:
            self.set_leverage(pair, leverage)
            result = self._signed_request("POST", "/fapi/v1/order", params=payload)
            if isinstance(result, dict):
                result.setdefault("id", result.get("orderId"))
                result.setdefault("product_symbol", pair)
                result.setdefault("symbol", pair)
            return result
        except Exception as exc:
            _binance_log.warning("[BINANCE] Order error: %s", exc)
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

    def get_order(self, product_id: str, order_id) -> dict:
        if not self._is_configured() or not order_id:
            return {}
        return self._signed_request("GET", "/fapi/v1/order", params={"symbol": product_id, "orderId": order_id})

    def cancel_order(self, order_id: str, product_id: str = "") -> dict:
        if not self._is_configured():
            return {"error": "API not configured"}
        try:
            return self._signed_request("DELETE", "/fapi/v1/order", params={"symbol": product_id, "orderId": order_id})
        except Exception as exc:
            _binance_log.warning("[BINANCE] Cancel error: %s", exc)
            return {"error": str(exc)}

    def get_orders(self, product_id: str = None, state: str = "open") -> list:
        if not self._is_configured():
            return []
        pair = str(product_id or "BTCUSDT").strip().upper()
        try:
            if str(state or "").lower() == "open":
                return self._signed_request("GET", "/fapi/v1/openOrders", params={"symbol": pair})
            return self._signed_request("GET", "/fapi/v1/allOrders", params={"symbol": pair, "limit": 100})
        except Exception as exc:
            _binance_log.warning("[BINANCE] Orders error: %s", exc)
            return []

    def get_order_history(self) -> list:
        if not self._is_configured():
            return []
        end_ms = int(round(_time.time() * 1000))
        start_ms = int((datetime.utcnow() - timedelta(days=7)).timestamp() * 1000)
        trades = []
        for symbol in ("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT"):
            try:
                rows = self._signed_request(
                    "GET",
                    "/fapi/v1/userTrades",
                    params={"symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": 1000},
                )
            except Exception as exc:
                _binance_log.warning("[BINANCE] Trades error for %s: %s", symbol, exc)
                continue
            for row in rows or []:
                row = dict(row)
                row.setdefault("product_symbol", self.from_broker_symbol(row.get("symbol")))
                row.setdefault("side", str(row.get("side") or "").lower())
                row.setdefault("average_fill_price", row.get("price"))
                row.setdefault("fill_price", row.get("price"))
                row.setdefault("size", row.get("quoteQty"))
                row.setdefault("filled_size", row.get("quoteQty"))
                row.setdefault("paid_commission", row.get("commission"))
                row.setdefault("realized_pnl", row.get("realizedPnl"))
                row.setdefault("state", "closed")
                row.setdefault("updated_at", datetime.utcfromtimestamp(int(row.get("time", 0)) / 1000).isoformat())
                trades.append(row)
        trades.sort(key=lambda item: item.get("time") or 0, reverse=True)
        return trades

    def set_leverage(self, product_id: str, leverage: int) -> dict:
        if not self._is_configured():
            return {"error": "API not configured"}
        try:
            return self._signed_request(
                "POST",
                "/fapi/v1/leverage",
                params={"symbol": str(product_id or "").upper(), "leverage": int(leverage or 1)},
            )
        except Exception as exc:
            _binance_log.warning("[BINANCE] Set leverage error: %s", exc)
            return {"error": str(exc)}

    def get_funding_history(self, symbol: str) -> list:
        pair = self.to_broker_symbol(symbol)
        try:
            return self._public_get("/fapi/v1/fundingRate", params={"symbol": pair, "limit": 100})
        except Exception as exc:
            _binance_log.warning("[BINANCE] Funding history error for %s: %s", pair, exc)
            return []
