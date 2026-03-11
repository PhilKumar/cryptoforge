"""
broker/delta.py — Delta Exchange API Wrapper
Handles: historical candles, order placement, positions, wallet,
         product info & leverage for perpetual futures.
Docs: https://docs.delta.exchange/
"""

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

_delta_log = logging.getLogger("cryptoforge.delta")
_RETRYABLE_STATUSES = {429, 500, 502, 503, 504}

# ── Persistent HTTP Session (keeps TCP+TLS warm to Delta servers) ──
_http_session = requests.Session()
_adapter = HTTPAdapter(pool_connections=4, pool_maxsize=10, max_retries=0)
_http_session.mount("https://", _adapter)
_http_session.mount("http://", _adapter)


def _request_with_retry(
    method: str,
    url: str,
    *,
    headers: dict,
    data: str = None,
    params: dict = None,
    timeout: int = 30,
    max_retries: int = 3,
    base_delay: float = 1.0,
) -> "requests.Response":
    """HTTP request with exponential backoff on transient failures."""
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
            last_exc = Exception(f"Delta API {resp.status_code}: {resp.text[:200]}")
            _delta_log.warning(f"[DELTA] {method} {url} → {resp.status_code} (attempt {attempt + 1}/{max_retries})")
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            last_exc = e
            _delta_log.warning(f"[DELTA] {method} {url} network error (attempt {attempt + 1}/{max_retries}): {e}")
        if attempt < max_retries - 1:
            _time.sleep(base_delay * (2**attempt))  # 1s, 2s, 4s …
    raise last_exc


class _CircuitBreaker:
    """Opens after `failure_threshold` consecutive failures; resets after `recovery_timeout` s."""

    CLOSED = "closed"
    OPEN = "open"

    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 60.0):
        self._threshold = failure_threshold
        self._timeout = recovery_timeout
        self._failures = 0
        self._state = self.CLOSED
        self._opened_at = 0.0

    def call_allowed(self) -> bool:
        if self._state == self.CLOSED:
            return True
        if _time.time() - self._opened_at >= self._timeout:
            return True
        return False

    def record_success(self):
        self._failures = 0
        self._state = self.CLOSED

    def record_failure(self):
        self._failures += 1
        if self._failures >= self._threshold:
            if self._state != self.OPEN:
                _delta_log.error(f"[DELTA] Circuit breaker OPEN after {self._failures} consecutive failures")
            self._state = self.OPEN
            self._opened_at = _time.time()

    @property
    def state(self) -> str:
        return self._state


_circuit_breaker = _CircuitBreaker(failure_threshold=5, recovery_timeout=60.0)


class DeltaClient:
    """Delta Exchange REST API client for perpetual futures."""

    def __init__(self):
        self.api_key = config.DELTA_API_KEY
        self.api_secret = config.DELTA_API_SECRET
        self.base_url = config.DELTA_BASE_URL
        self._products_cache = None
        self._products_ts = 0
        self._CACHE_TTL = 3600  # 1 hour
        # Delta India uses USD suffix (BTCUSD), NOT USDT (BTCUSDT)
        self._is_india = getattr(config, "DELTA_REGION", "india").lower() == "india"

    @staticmethod
    def to_delta_symbol(symbol: str) -> str:
        """Convert Binance-style XXXUSDT to Delta India XXXUSD format."""
        if symbol and symbol.upper().endswith("USDT"):
            return symbol[:-1]  # strip trailing 'T'
        return symbol

    @staticmethod
    def from_delta_symbol(symbol: str) -> str:
        """Convert Delta India XXXUSD back to Binance-style XXXUSDT format."""
        if symbol and symbol.upper().endswith("USD") and not symbol.upper().endswith("USDT"):
            return symbol + "T"
        return symbol

    def _is_configured(self) -> bool:
        return (
            self.api_key != "YOUR_API_KEY_HERE" and self.api_secret != "YOUR_API_SECRET_HERE" and len(self.api_key) > 5
        )

    # ── Auth Signature ────────────────────────────────────────────
    def _sign(self, method: str, path: str, query_string: str = "", body: str = "") -> dict:
        """Generate HMAC-SHA256 signature for Delta Exchange API.
        Delta docs: signature_string = method + timestamp + route_path + query_string + body
        route_path must include the /v2 prefix.
        """
        timestamp = str(int(_time.time()))
        # path already starts with / (e.g. "/wallet/balances")
        # prepend /v2 to match the actual URL path Delta expects
        route_path = "/v2" + path
        message = method + timestamp + route_path + query_string + body
        signature = hmac.new(self.api_secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()
        return {
            "api-key": self.api_key,
            "timestamp": timestamp,
            "signature": signature,
            "Content-Type": "application/json",
        }

    def _get(self, path: str, params: dict = None, auth: bool = False) -> dict:
        if not _circuit_breaker.call_allowed():
            raise Exception("Delta API circuit breaker is OPEN — broker unavailable")
        url = f"{self.base_url}{path}"
        query_string = ""
        if auth and params:
            from urllib.parse import urlencode

            query_string = "?" + urlencode(sorted(params.items()))
        headers = self._sign("GET", path, query_string=query_string) if auth else {"Content-Type": "application/json"}
        try:
            resp = _request_with_retry("GET", url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            _circuit_breaker.record_success()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            _circuit_breaker.record_failure()
            _delta_log.warning(f"[DELTA] GET {path} HTTP {e.response.status_code}: {e.response.text[:200]}")
            raise
        except Exception as e:
            _circuit_breaker.record_failure()
            _delta_log.warning(f"[DELTA] GET {path} error: {e}")
            raise

    def _post(self, path: str, data: dict = None) -> dict:
        if not _circuit_breaker.call_allowed():
            raise Exception("Delta API circuit breaker is OPEN — broker unavailable")
        url = f"{self.base_url}{path}"
        body = json.dumps(data) if data else ""
        headers = self._sign("POST", path, body=body)
        try:
            resp = _request_with_retry("POST", url, headers=headers, data=body, timeout=30)
            resp.raise_for_status()
            _circuit_breaker.record_success()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            _circuit_breaker.record_failure()
            _delta_log.warning(f"[DELTA] POST {path} HTTP {e.response.status_code}: {e.response.text[:200]}")
            raise
        except Exception as e:
            _circuit_breaker.record_failure()
            _delta_log.warning(f"[DELTA] POST {path} error: {e}")
            raise

    def _delete(self, path: str, data: dict = None) -> dict:
        url = f"{self.base_url}{path}"
        body = json.dumps(data) if data else ""
        headers = self._sign("DELETE", path, body=body)
        _circuit_breaker.check()
        try:
            resp = _request_with_retry("DELETE", url, headers=headers, data=body, timeout=30)
            resp.raise_for_status()
            _circuit_breaker.record_success()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            _circuit_breaker.record_failure()
            _delta_log.warning(f"[DELTA] DELETE {path} HTTP {e.response.status_code}: {e.response.text[:200]}")
            raise
        except Exception as e:
            _circuit_breaker.record_failure()
            _delta_log.warning(f"[DELTA] DELETE {path} error: {e}")
            raise

    # ── Async wrappers (non-blocking for asyncio event loop) ──────
    # These use asyncio.to_thread for compatibility with the sync methods.
    # For true async (aiohttp), use the native_async_* methods below.
    async def async_get(self, path: str, params: dict = None, auth: bool = False) -> dict:
        return await asyncio.to_thread(self._get, path, params, auth)

    async def async_post(self, path: str, data: dict = None) -> dict:
        return await asyncio.to_thread(self._post, path, data)

    async def async_delete(self, path: str, data: dict = None) -> dict:
        return await asyncio.to_thread(self._delete, path, data)

    async def async_get_candles(self, symbol: str, **kwargs) -> pd.DataFrame:
        return await asyncio.to_thread(self.get_candles, symbol, **kwargs)

    # ── Native Async (aiohttp) — enterprise-grade ─────────────────
    _aio_session = None

    async def _ensure_aio_session(self):
        """Lazy-create a shared aiohttp session with connection pooling."""
        if self._aio_session is None or self._aio_session.closed:
            import aiohttp

            connector = aiohttp.TCPConnector(
                limit=20,  # max parallel connections
                keepalive_timeout=60,  # keep TCP warm for 60s
                enable_cleanup_closed=True,
            )
            timeout = aiohttp.ClientTimeout(total=30, connect=5)
            self._aio_session = aiohttp.ClientSession(timeout=timeout, connector=connector)

    async def aio_close(self):
        """Close the aiohttp session (call on shutdown)."""
        if self._aio_session and not self._aio_session.closed:
            await self._aio_session.close()

    async def _aio_get(
        self, path: str, params: dict = None, auth: bool = False, max_retries: int = 3, base_delay: float = 1.0
    ) -> dict:
        """Native async GET using aiohttp — with circuit breaker + exponential backoff retry."""
        import aiohttp

        _circuit_breaker.check()
        await self._ensure_aio_session()
        url = f"{self.base_url}{path}"
        query_string = ""
        if auth and params:
            from urllib.parse import urlencode

            query_string = "?" + urlencode(sorted(params.items()))
        headers = self._sign("GET", path, query_string=query_string) if auth else {"Content-Type": "application/json"}
        last_exc = None
        for attempt in range(max_retries):
            try:
                async with self._aio_session.get(url, params=params, headers=headers) as resp:
                    if resp.status in _RETRYABLE_STATUSES:
                        last_exc = Exception(f"Delta API {resp.status}")
                        _delta_log.warning(
                            f"[DELTA] aio GET {path} → {resp.status} (attempt {attempt + 1}/{max_retries})"
                        )
                    else:
                        resp.raise_for_status()
                        _circuit_breaker.record_success()
                        return await resp.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_exc = e
                _delta_log.warning(f"[DELTA] aio GET {path} transient error: {e} (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                await asyncio.sleep(base_delay * (2**attempt))
        _circuit_breaker.record_failure()
        raise last_exc or Exception(f"aio GET {path} failed after {max_retries} attempts")

    async def _aio_post(self, path: str, data: dict = None, max_retries: int = 3, base_delay: float = 1.0) -> dict:
        """Native async POST using aiohttp — with circuit breaker + exponential backoff retry."""
        import aiohttp

        _circuit_breaker.check()
        await self._ensure_aio_session()
        url = f"{self.base_url}{path}"
        body = json.dumps(data) if data else ""
        headers = self._sign("POST", path, body=body)
        last_exc = None
        for attempt in range(max_retries):
            try:
                async with self._aio_session.post(url, data=body, headers=headers) as resp:
                    if resp.status in _RETRYABLE_STATUSES:
                        last_exc = Exception(f"Delta API {resp.status}")
                        _delta_log.warning(
                            f"[DELTA] aio POST {path} → {resp.status} (attempt {attempt + 1}/{max_retries})"
                        )
                    else:
                        resp.raise_for_status()
                        _circuit_breaker.record_success()
                        return await resp.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_exc = e
                _delta_log.warning(
                    f"[DELTA] aio POST {path} transient error: {e} (attempt {attempt + 1}/{max_retries})"
                )
            if attempt < max_retries - 1:
                await asyncio.sleep(base_delay * (2**attempt))
        _circuit_breaker.record_failure()
        raise last_exc or Exception(f"aio POST {path} failed after {max_retries} attempts")

    # ── Redundant Order Verification ──────────────────────────────
    async def place_order_verified(
        self,
        product_id: int,
        size: float,
        side: str,
        order_type: str = "market_order",
        limit_price: float = None,
        leverage: int = 10,
        reduce_only: bool = False,
        max_verify_attempts: int = 3,
    ) -> dict:
        """
        Place an order and verify it was filled.
        1. Place order via REST
        2. Wait 2s, then query order status
        3. Cross-check against positions
        4. Retry verification up to max_verify_attempts times
        Returns enriched order dict with 'verified' flag.
        """
        # Place the order
        result = self.place_order(
            product_id=product_id,
            size=size,
            side=side,
            order_type=order_type,
            limit_price=limit_price,
            leverage=leverage,
            reduce_only=reduce_only,
        )

        if isinstance(result, dict) and result.get("error"):
            return {**result, "verified": False}

        order_id = result.get("id")
        if not order_id:
            print(f"[DELTA] Order placed but no ID returned: {result}")
            return {**result, "verified": False}

        print(f"[DELTA] Order {order_id} placed — verifying fill...")

        # Verification loop
        for attempt in range(max_verify_attempts):
            await asyncio.sleep(2)  # Wait for fill

            try:
                # Check order status
                orders = await asyncio.to_thread(self.get_orders, product_id, "closed")
                filled = None
                for o in orders or []:
                    if o.get("id") == order_id:
                        filled = o
                        break

                if filled and filled.get("state") in ("filled", "closed"):
                    print(f"[DELTA] Order {order_id} VERIFIED filled (attempt {attempt + 1})")

                    # Cross-check position
                    position = await asyncio.to_thread(self.get_position, product_id)
                    if position:
                        pos_size = abs(float(position.get("size", 0)))
                        expected_size = abs(float(size))
                        if pos_size > 0:
                            print(f"[DELTA] Position confirmed: size={pos_size}")
                        else:
                            print(f"[DELTA] WARNING: Position size is 0 after fill (expected ~{expected_size})")

                    return {
                        **result,
                        "verified": True,
                        "fill_status": filled.get("state"),
                        "verified_at_attempt": attempt + 1,
                    }

                print(f"[DELTA] Order {order_id} not yet filled (attempt {attempt + 1})")

            except Exception as e:
                print(f"[DELTA] Verify error (attempt {attempt + 1}): {e}")

        print(f"[DELTA] Order {order_id} UNVERIFIED after {max_verify_attempts} attempts")
        return {**result, "verified": False, "verified_at_attempt": max_verify_attempts}

    # ── Products (Instruments) ────────────────────────────────────
    def get_products(self, force_refresh: bool = False) -> list:
        """Fetch all available products from Delta Exchange."""
        now = _time.time()
        if self._products_cache and (now - self._products_ts) < self._CACHE_TTL and not force_refresh:
            return self._products_cache

        print("[DELTA] Fetching products...")
        resp = self._get("/products")
        products = resp.get("result", [])
        self._products_cache = products
        self._products_ts = now
        print(f"[DELTA] Got {len(products)} products")
        return products

    def get_perpetual_futures(self) -> list:
        """Get only perpetual futures contracts."""
        products = self.get_products()
        perps = [p for p in products if p.get("contract_type") == "perpetual_futures" and p.get("state") == "live"]
        return perps

    def get_product_by_symbol(self, symbol: str) -> Optional[dict]:
        """Find a product by its symbol (e.g., BTCUSD)."""
        # Delta India uses USD suffix, map from USDT
        if self._is_india:
            symbol = self.to_delta_symbol(symbol)
        products = self.get_products()
        for p in products:
            if p.get("symbol", "").upper() == symbol.upper():
                return p
        return None

    def get_leverage_info(self, symbol: str) -> dict:
        """Get max leverage and available leverage options for a symbol."""
        # Delta India uses USD suffix, map from USDT
        if self._is_india:
            symbol = self.to_delta_symbol(symbol)
        product = self.get_product_by_symbol(symbol)
        if not product:
            return {"max_leverage": 100, "default": 10, "options": [1, 2, 3, 5, 10, 20, 50, 100]}

        # initial_margin is in PERCENT (e.g. 1 = 1%), so max_lev = 100 / initial_margin
        initial_margin = float(product.get("initial_margin", 1))
        max_lev = int(100 / initial_margin) if initial_margin > 0 else 100
        maintenance_margin = float(product.get("maintenance_margin", 0.5))

        # Delta also provides default_leverage directly
        default_lev_raw = product.get("default_leverage")
        default_lev = int(float(default_lev_raw)) if default_lev_raw else min(10, max_lev)

        # Build standard leverage options up to max
        standard = [1, 2, 3, 5, 10, 15, 20, 25, 50, 75, 100, 125, 150, 200]
        options = [x for x in standard if x <= max_lev]
        if max_lev not in options:
            options.append(max_lev)

        return {
            "max_leverage": max_lev,
            "default": default_lev,
            "options": sorted(options),
            "initial_margin": initial_margin,
            "maintenance_margin": maintenance_margin,
        }

    # ── Historical Candles ────────────────────────────────────────
    def get_candles(self, symbol: str, resolution: str = "5m", start: str = None, end: str = None) -> pd.DataFrame:
        """
        Fetch OHLCV candle data.
        resolution: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 1d, 7d, 30d, 1w, 2w
        """
        # Delta India uses USD suffix (e.g. BTCUSD), map from USDT
        if self._is_india:
            symbol = self.to_delta_symbol(symbol)
        # Map resolution to Delta API format
        res_map = {
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
            "1w": "1w",
            "1D": "1d",
            "1W": "1w",
        }
        delta_res = res_map.get(resolution, "5m")

        # Convert dates to timestamps
        if start:
            start_ts = int(datetime.strptime(start, "%Y-%m-%d").timestamp())
        else:
            start_ts = int((datetime.utcnow() - timedelta(days=30)).timestamp())

        if end:
            end_ts = int(datetime.strptime(end, "%Y-%m-%d").timestamp())
        else:
            end_ts = int(datetime.utcnow().timestamp())

        # Delta API candles endpoint
        # Chunk into segments (Delta has limits per request)
        all_candles = []
        chunk_start = start_ts

        # Determine seconds per candle for chunking
        sec_map = {
            "1m": 60,
            "3m": 180,
            "5m": 300,
            "15m": 900,
            "30m": 1800,
            "1h": 3600,
            "2h": 7200,
            "4h": 14400,
            "6h": 21600,
            "1d": 86400,
            "1w": 604800,
        }
        secs_per_candle = sec_map.get(delta_res, 300)
        max_candles_per_req = 500
        chunk_size = secs_per_candle * max_candles_per_req

        chunk_num = 0
        while chunk_start < end_ts:
            chunk_end = min(chunk_start + chunk_size, end_ts)
            chunk_num += 1

            params = {
                "symbol": symbol,
                "resolution": delta_res,
                "start": chunk_start,
                "end": chunk_end,
            }

            try:
                resp = self._get("/history/candles", params=params)
                candles = resp.get("result", [])
                if candles:
                    all_candles.extend(candles)
                    print(f"[DELTA] Candles chunk {chunk_num}: {len(candles)} bars")
                else:
                    print(f"[DELTA] Candles chunk {chunk_num}: empty")
            except Exception as e:
                print(f"[DELTA] Candles chunk {chunk_num} error: {e}")

            chunk_start = chunk_end

        if not all_candles:
            print(f"[DELTA] No candle data for {symbol}")
            return pd.DataFrame()

        # Build DataFrame
        df = pd.DataFrame(all_candles)
        # Delta returns: time, open, high, low, close, volume
        if "time" in df.columns:
            # time is unix timestamp
            df["datetime"] = pd.to_datetime(df["time"], unit="s", utc=True)
        elif "t" in df.columns:
            df["datetime"] = pd.to_datetime(df["t"], unit="s", utc=True)

        rename_map = {}
        for orig, target in [("o", "open"), ("h", "high"), ("l", "low"), ("c", "close"), ("v", "volume")]:
            if orig in df.columns:
                rename_map[orig] = target
        if rename_map:
            df.rename(columns=rename_map, inplace=True)

        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "datetime" in df.columns:
            df.set_index("datetime", inplace=True)
            df.sort_index(inplace=True)
            df = df[~df.index.duplicated(keep="first")]

        # Keep only OHLCV
        keep = [c for c in ["open", "high", "low", "close", "volume"] if c in df.columns]
        df = df[keep]

        print(f"[DELTA] Total: {len(df)} candles for {symbol} ({delta_res})")
        return df

    # ── Live Ticker (Mark Price / LTP) ────────────────────────────
    def get_ticker(self, symbol: str) -> dict:
        """Get latest ticker data for a symbol."""
        # Delta India uses USD suffix (e.g. BTCUSD), map from USDT
        if self._is_india:
            symbol = self.to_delta_symbol(symbol)
        try:
            resp = self._get(f"/tickers/{symbol}")
            result = resp.get("result", {})
            return {
                "symbol": symbol,
                "mark_price": float(result.get("mark_price", 0)),
                "last_price": float(result.get("close", result.get("last_price", 0))),
                "volume_24h": float(result.get("volume", 0)),
                "turnover_24h": float(result.get("turnover", 0)),
                "open_interest": float(result.get("open_interest", 0)),
                "funding_rate": float(result.get("funding_rate", 0)),
                "price_change_24h": float(result.get("price_change_percent_24h", 0)),
                "high_24h": float(result.get("high", 0)),
                "low_24h": float(result.get("low", 0)),
            }
        except Exception as e:
            print(f"[DELTA] Ticker error for {symbol}: {e}")
            return {"symbol": symbol, "mark_price": 0, "last_price": 0}

    def get_tickers_bulk(self) -> list:
        """Get tickers for all products."""
        try:
            resp = self._get("/tickers")
            return resp.get("result", [])
        except Exception as e:
            print(f"[DELTA] Bulk tickers error: {e}")
            return []

    # ── Wallet / Balance ──────────────────────────────────────────
    def get_wallet(self) -> dict:
        """Get wallet balances."""
        if not self._is_configured():
            return {"error": "API not configured"}
        try:
            resp = self._get("/wallet/balances", auth=True)
            return resp.get("result", {})
        except Exception as e:
            print(f"[DELTA] Wallet error: {e}")
            return {"error": str(e)}

    # ── Positions ─────────────────────────────────────────────────
    def get_positions(self) -> list:
        """Get open positions."""
        if not self._is_configured():
            return []
        try:
            resp = self._get("/positions/margined", auth=True)
            return resp.get("result", [])
        except Exception as e:
            print(f"[DELTA] Positions error: {e}")
            return []

    def get_position(self, product_id: int) -> dict:
        """Get position for a specific product."""
        if not self._is_configured():
            return {}
        try:
            resp = self._get("/positions", params={"product_id": product_id}, auth=True)
            results = resp.get("result", [])
            for p in results:
                if p.get("product_id") == product_id:
                    return p
            return {}
        except Exception as e:
            print(f"[DELTA] Position error: {e}")
            return {}

    # ── Orders ────────────────────────────────────────────────────
    def place_order(
        self,
        product_id: int,
        size: float,
        side: str,
        order_type: str = "market_order",
        limit_price: float = None,
        leverage: int = 10,
        reduce_only: bool = False,
    ) -> dict:
        """
        Place a futures order.
        side: 'buy' or 'sell'
        order_type: 'market_order' or 'limit_order'
        """
        if not self._is_configured():
            return {"error": "API not configured"}

        data = {
            "product_id": product_id,
            "size": int(size),
            "side": side,
            "order_type": order_type,
            "reduce_only": reduce_only,
        }
        if order_type == "limit_order" and limit_price:
            data["limit_price"] = str(limit_price)

        try:
            resp = self._post("/orders", data)
            return resp.get("result", resp)
        except Exception as e:
            print(f"[DELTA] Order error: {e}")
            return {"error": str(e)}

    def cancel_order(self, order_id: int, product_id: int) -> dict:
        """Cancel an open order."""
        if not self._is_configured():
            return {"error": "API not configured"}
        try:
            resp = self._delete("/orders", {"id": order_id, "product_id": product_id})
            return resp.get("result", resp)
        except Exception as e:
            print(f"[DELTA] Cancel error: {e}")
            return {"error": str(e)}

    def get_orders(self, product_id: int = None, state: str = "open") -> list:
        """Get orders. state: open, closed, cancelled"""
        if not self._is_configured():
            return []
        try:
            params = {"state": state}
            if product_id:
                params["product_id"] = product_id
            resp = self._get("/orders", params=params, auth=True)
            return resp.get("result", [])
        except Exception as e:
            print(f"[DELTA] Orders error: {e}")
            return []

    def get_order_history(self) -> list:
        """Get recent filled orders."""
        if not self._is_configured():
            return []
        try:
            resp = self._get("/orders/history", params={"page_size": 100}, auth=True)
            return resp.get("result", [])
        except Exception as e:
            print(f"[DELTA] Order history error: {e}")
            return []

    # ── Set Leverage ──────────────────────────────────────────────
    def set_leverage(self, product_id: int, leverage: int) -> dict:
        """Change leverage for a product."""
        if not self._is_configured():
            return {"error": "API not configured"}
        try:
            resp = self._post(
                "/orders/leverage",
                {
                    "product_id": product_id,
                    "leverage": str(leverage),
                },
            )
            return resp.get("result", resp)
        except Exception as e:
            print(f"[DELTA] Set leverage error: {e}")
            return {"error": str(e)}

    # ── Funding Rate History ──────────────────────────────────────
    def get_funding_history(self, symbol: str) -> list:
        """Get funding rate history for a perpetual contract."""
        try:
            product = self.get_product_by_symbol(symbol)
            if not product:
                return []
            product_id = product.get("id")
            resp = self._get("/funding_rates", params={"product_id": product_id})
            return resp.get("result", [])
        except Exception as e:
            print(f"[DELTA] Funding rate error: {e}")
            return []


# ═══════════════════════════════════════════════════════════════════
#  Binance Public API — fallback candle source for non-Delta symbols
# ═══════════════════════════════════════════════════════════════════


def get_candles_binance(symbol: str, resolution: str = "5m", start: str = None, end: str = None) -> pd.DataFrame:
    """
    Fetch OHLCV candle data from Binance public API (no API key needed).
    symbol: e.g. 'ADAUSDT', 'BNBUSDT'
    resolution: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 1d, 1w
    """
    res_map = {
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
    interval = res_map.get(resolution, "5m")

    from datetime import datetime, timedelta

    if start:
        start_ms = int(datetime.strptime(start, "%Y-%m-%d").timestamp() * 1000)
    else:
        start_ms = int((datetime.utcnow() - timedelta(days=30)).timestamp() * 1000)

    if end:
        end_ms = int(datetime.strptime(end, "%Y-%m-%d").timestamp() * 1000)
    else:
        end_ms = int(datetime.utcnow().timestamp() * 1000)

    all_candles = []
    chunk_start = start_ms
    chunk_num = 0

    while chunk_start < end_ms:
        chunk_num += 1
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": chunk_start,
            "endTime": end_ms,
            "limit": 1000,  # Binance max per request
        }
        try:
            resp = _http_session.get("https://api.binance.com/api/v3/klines", params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if not data:
                break
            all_candles.extend(data)
            print(f"[BINANCE] Chunk {chunk_num}: {len(data)} candles for {symbol}")
            # Move past the last candle's close time
            chunk_start = data[-1][6] + 1  # closeTime + 1ms
            if len(data) < 1000:
                break  # No more data
        except Exception as e:
            print(f"[BINANCE] Chunk {chunk_num} error: {e}")
            break

    if not all_candles:
        print(f"[BINANCE] No candle data for {symbol}")
        return pd.DataFrame()

    # Binance klines: [openTime, open, high, low, close, volume, closeTime, ...]
    df = pd.DataFrame(
        all_candles,
        columns=[
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_volume",
            "trades",
            "taker_buy_base",
            "taker_buy_quote",
            "ignore",
        ],
    )

    df["datetime"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df.set_index("datetime", inplace=True)
    df.sort_index(inplace=True)
    df = df[~df.index.duplicated(keep="first")]
    df = df[["open", "high", "low", "close", "volume"]]

    print(f"[BINANCE] Total: {len(df)} candles for {symbol} ({interval})")
    return df
