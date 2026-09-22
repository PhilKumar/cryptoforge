"""engine/option_seller_live.py — the Option Seller's REAL orders on Delta.

Phil, 22-Sep-2026: "yes build the live side with stop order".

The paper engine (engine/option_seller_paper.py) decides; this module only
EXECUTES, and only when every lock is open:

  1. the server is armed: CRYPTOFORGE_OPTION_SELLER_LIVE=1;
  2. Delta keys are configured and Delta accepts them;
  3. the book is switched to live on the page;
  4. the size is at or under CRYPTOFORGE_OPTION_SELLER_LIVE_MAX_CONTRACTS
     (default 10 contracts = 0.01 BTC — start with 1).

What it does with a trade:

  SELL   an IOC limit at the bid less a small tolerance: fills at the bid or
         better, never chases a thin book. Whatever filled is the position.
  STOP   immediately after, a RESTING stop-loss BUY on Delta itself,
         reduce-only, triggered on the MARK at twice the premium sold — the
         same trigger the paper book and the backtest use. It sits on the
         exchange, so a spike between two 15-second checks is caught there
         (21-Sep-2026 on paper: the stop was 327.58, the 15-second check first
         saw 362.18). If the stop cannot be placed, the position is bought
         back at once — a naked short option is never left without one.
  EXIT   at 17:25 IST: cancel the stop, buy back reduce-only at market, and
         confirm the position is flat. Delta cash-settles at 17:30 IST, so a
         buy-back that fails is still bounded by settlement minutes later.

Order safety: an order POST is NEVER retried blind. Each order carries its own
client_order_id; after an error or a timeout the order is looked up by that id
before anything is sent again. broker/delta.py's shared POST retries on 5xx,
which for a new order could double it, so orders here go through
`_post_once`.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import math
import os
import time
import uuid
from typing import Callable, Optional

_log = logging.getLogger("cryptoforge.option_seller.live")

LIVE_ARMED = os.getenv("CRYPTOFORGE_OPTION_SELLER_LIVE", "").strip().lower() in {"1", "true", "yes", "on"}
MAX_LIVE_CONTRACTS = int(os.getenv("CRYPTOFORGE_OPTION_SELLER_LIVE_MAX_CONTRACTS", "") or 10)
ARM_HINT = "CRYPTOFORGE_OPTION_SELLER_LIVE=1"

BASE_URL = "https://api.india.delta.exchange/v2"
# How far under the bid the entry may fill, as a share of the bid. An IOC at
# the bid itself misses whenever the bid ticks down between the quote and the
# order; 3% lets it fill without selling into an empty book.
ENTRY_TOLERANCE = 0.03


class LiveOrderError(RuntimeError):
    """An order Delta refused, or one whose outcome could not be confirmed."""


def _round_tick(price: float, tick: float, up: bool) -> float:
    if tick <= 0:
        return round(price, 2)
    steps = price / tick
    steps = math.ceil(steps - 1e-9) if up else math.floor(steps + 1e-9)
    return round(max(tick, steps * tick), 8)


class DeltaOptionExecutor:
    """Real orders for one short option at a time. Signs like broker/delta.py."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        http: Optional[Callable] = None,
        armed: Optional[bool] = None,
    ):
        self.api_key = api_key if api_key is not None else os.getenv("DELTA_API_KEY", "")
        self.api_secret = api_secret if api_secret is not None else os.getenv("DELTA_API_SECRET", "")
        self.armed = LIVE_ARMED if armed is None else bool(armed)
        self._http = http or self._requests_http
        self._products: dict = {}

    # ── locks ─────────────────────────────────────────────────────────

    def configured(self) -> bool:
        return (
            len(self.api_key or "") > 5
            and len(self.api_secret or "") > 5
            and "YOUR_API" not in (self.api_key or "")
            and "dummy" not in (self.api_key or "")
        )

    def why_not_live(self, contracts: int) -> str:
        """Empty when a live trade of this size may be placed, else the reason."""
        if not self.armed:
            return f"the server is not armed for live option selling (set {ARM_HINT})"
        if not self.configured():
            return "Delta API keys are not configured on the server"
        if contracts < 1:
            return "live size must be at least 1 contract (0.001 BTC)"
        if contracts > MAX_LIVE_CONTRACTS:
            return (
                f"live size is capped at {MAX_LIVE_CONTRACTS} contracts "
                f"({MAX_LIVE_CONTRACTS * 0.001:g} BTC); lower the size or raise "
                "CRYPTOFORGE_OPTION_SELLER_LIVE_MAX_CONTRACTS on the server"
            )
        return ""

    # ── transport ─────────────────────────────────────────────────────

    def _headers(self, method: str, path: str, query: str = "", body: str = "") -> dict:
        ts = str(int(time.time()))
        message = method + ts + "/v2" + path + query + body
        sig = hmac.new(self.api_secret.encode(), message.encode(), hashlib.sha256).hexdigest()
        return {
            "api-key": self.api_key,
            "timestamp": ts,
            "signature": sig,
            "Content-Type": "application/json",
            "User-Agent": "cryptoforge-option-seller",
        }

    @staticmethod
    async def _requests_http(method: str, url: str, headers: dict, body: str, params: Optional[dict]):
        import requests

        def go():
            r = requests.request(method, url, headers=headers, data=body or None, params=params, timeout=10)
            try:
                payload = r.json()
            except ValueError:
                payload = {"success": False, "error": {"code": f"http_{r.status_code}", "text": r.text[:200]}}
            return r.status_code, payload

        return await asyncio.to_thread(go)  # the app has ONE event loop

    async def _call(self, method: str, path: str, data: Optional[dict] = None, params: Optional[dict] = None):
        from urllib.parse import urlencode

        query = ("?" + urlencode(sorted(params.items()))) if params else ""
        body = json.dumps(data, separators=(",", ":")) if data else ""
        status, payload = await self._http(
            method, BASE_URL + path, self._headers(method, path, query, body), body, params
        )
        if status >= 400 or not payload.get("success", True):
            err = payload.get("error") or {}
            raise LiveOrderError(f"Delta {method} {path} → {status}: {err.get('code') or err or payload}")
        return payload.get("result", payload)

    async def _post_once(self, data: dict) -> dict:
        """Send an order ONCE. On any failure, look it up by client_order_id
        before reporting — it may have reached the book after all."""
        try:
            return await self._call("POST", "/orders", data)
        except Exception as exc:
            found = await self.find_order(data["client_order_id"], data["product_id"])
            if found:
                _log.warning("[OPTION-SELLER] order %s errored (%s) but IS on Delta", data["client_order_id"], exc)
                return found
            raise

    # ── reads ─────────────────────────────────────────────────────────

    async def product(self, symbol: str) -> dict:
        if symbol not in self._products:
            row = await self._call("GET", f"/products/{symbol}")
            self._products[symbol] = {"id": int(row["id"]), "tick": float(row.get("tick_size") or 0.1)}
        return self._products[symbol]

    async def find_order(self, client_order_id: str, product_id: int) -> Optional[dict]:
        for states in ("open,pending", "closed,cancelled"):
            try:
                rows = await self._call("GET", "/orders", params={"product_ids": str(product_id), "states": states})
            except Exception as exc:
                # A lookup that fails must not hide an order that exists: say
                # so, and let the other state list be searched.
                _log.warning("[OPTION-SELLER] order lookup (%s) failed: %s", states, exc)
                rows = []
            for row in rows or []:
                if str(row.get("client_order_id") or "") == client_order_id:
                    return row
        return None

    async def order(self, order_id) -> dict:
        return await self._call("GET", f"/orders/{order_id}")

    async def position_size(self, product_id: int) -> int:
        """Signed contracts held: negative is short."""
        row = await self._call("GET", "/positions", params={"product_id": str(product_id)})
        if isinstance(row, list):
            row = next((r for r in row if int(r.get("product_id") or 0) == product_id), {})
        return int(float((row or {}).get("size") or 0))

    async def balance_check(self) -> dict:
        """Read-only probe the page can show: do the keys and IP work?"""
        rows = await self._call("GET", "/wallet/balances")
        usd = next((r for r in rows or [] if str(r.get("asset_symbol") or "").upper() in ("USD", "USDT")), {})
        return {
            "ok": True,
            "asset": usd.get("asset_symbol", ""),
            "available": float(usd.get("available_balance") or 0),
        }

    # ── the three actions ─────────────────────────────────────────────

    @staticmethod
    def _coid(tag: str, day: str) -> str:
        return f"os-{day.replace('-', '')}-{tag}-{uuid.uuid4().hex[:8]}"[:32]

    async def sell_open(self, symbol: str, contracts: int, bid: float, day: str) -> dict:
        """IOC limit sell. Returns {order_id, filled, avg_price, product_id}."""
        prod = await self.product(symbol)
        floor = _round_tick(bid * (1 - ENTRY_TOLERANCE), prod["tick"], up=True)
        order = await self._post_once(
            {
                "product_id": prod["id"],
                "size": int(contracts),
                "side": "sell",
                "order_type": "limit_order",
                "limit_price": str(floor),
                "time_in_force": "ioc",
                "client_order_id": self._coid("sell", day),
            }
        )
        filled = int(order.get("size") or 0) - int(order.get("unfilled_size") or 0)
        return {
            "order_id": order.get("id"),
            "product_id": prod["id"],
            "filled": max(0, filled),
            "avg_price": float(order.get("average_fill_price") or 0),
            "limit": floor,
        }

    async def place_stop(self, product_id: int, contracts: int, stop_px: float, tick: float, day: str) -> dict:
        """The resting stop on Delta: reduce-only market BUY on the MARK."""
        price = _round_tick(stop_px, tick, up=False)
        order = await self._post_once(
            {
                "product_id": product_id,
                "size": int(contracts),
                "side": "buy",
                "order_type": "market_order",
                "stop_order_type": "stop_loss_order",
                "stop_price": str(price),
                "stop_trigger_method": "mark_price",
                "reduce_only": True,
                "client_order_id": self._coid("stop", day),
            }
        )
        return {"order_id": order.get("id"), "stop_price": price}

    async def cancel(self, order_id, product_id: int) -> None:
        try:
            await self._call("DELETE", "/orders", {"id": int(order_id), "product_id": int(product_id)})
        except LiveOrderError as exc:
            # Already filled or already gone is fine; the caller re-reads it.
            _log.info("[OPTION-SELLER] cancel %s: %s", order_id, exc)

    async def buy_close(self, product_id: int, contracts: int, day: str) -> dict:
        order = await self._post_once(
            {
                "product_id": product_id,
                "size": int(contracts),
                "side": "buy",
                "order_type": "market_order",
                "reduce_only": True,
                "client_order_id": self._coid("exit", day),
            }
        )
        filled = int(order.get("size") or 0) - int(order.get("unfilled_size") or 0)
        return {
            "order_id": order.get("id"),
            "filled": max(0, filled),
            "avg_price": float(order.get("average_fill_price") or 0),
        }
