"""
executor/coindcx.py — a CoinDCX Spot adapter satisfying `ExchangeAdapter`.

Same port as the Binance one, a genuinely different exchange underneath. The
differences that matter are not stylistic:

1. **The signature covers the exact JSON body bytes**, so what is signed must
   be what is sent — byte for byte, no re-serialization in between. The same
   discipline as our own feed frames, for the same reason: any
   canonicalization gap between signing and sending is a silent auth failure.
2. **`base_currency` is the SETTLEMENT currency and `target` is the coin.**
   That is the reverse of every other venue here, and reading it the usual way
   round gives you USDT as the asset you are trying to sell.
3. **Cancel takes the exchange's own order id, not the client id.** So a
   cancel is a lookup and then a cancel, which means a cancel of something
   already gone resolves to "not found" at the lookup — handled, because
   upstream treats a vanished order as success.
4. **Everything private is a POST**, including reads.

Fees are twice Binance's on USDT pairs (0.2% vs 0.1% per side), which is why
the target price is computed per venue upstream rather than from one constant.

Like the rest of `executor/`, this must not import `engine.cascade`.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
from typing import Callable, Dict, List, Optional

from executor.binance import format_decimal
from executor.exchange import (
    BelowMinNotional,
    DuplicateOrder,
    ExchangeError,
    InsufficientBalance,
    OrderRecord,
    SymbolRules,
)

_log = logging.getLogger("cascade.executor.coindcx")

BASE_URL = "https://api.coindcx.com"

_STATUS = {
    "init": "NEW",
    "open": "NEW",
    "partial_entry": "PARTIALLY_FILLED",
    "partially_filled": "PARTIALLY_FILLED",
    "filled": "FILLED",
    "partially_cancelled": "CANCELLED",
    "cancelled": "CANCELLED",
    "canceled": "CANCELLED",
    "rejected": "REJECTED",
}

_ORDER_TYPE = {"limit": "limit_order", "stop_limit": "stop_limit", "market": "market_order"}


class CoinDCXSpotAdapter:
    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        http: Optional[Callable] = None,
        base_url: str = BASE_URL,
        now_ms: Callable[[], int] = lambda: int(time.time() * 1000),
    ):
        self._key = api_key
        self._secret = api_secret.encode("utf-8")
        self._http = http or _requests_http
        self._base = base_url.rstrip("/")
        self._now_ms = now_ms
        self._rules: Dict[str, SymbolRules] = {}
        self._markets: Dict[str, dict] = {}

    # ── plumbing ─────────────────────────────────────────────────

    def _public(self, path: str) -> object:
        status, data = self._http("GET", f"{self._base}{path}", None, {"Content-Type": "application/json"})
        if status >= 400:
            raise _translate(data)
        return data

    def _private(self, path: str, payload: Optional[dict] = None) -> object:
        body = dict(payload or {})
        body.setdefault("timestamp", self._now_ms())
        # Serialized ONCE. The signature covers these exact bytes, so the
        # transport is handed the same string rather than the dict — anything
        # that re-encodes in between is an auth failure with no useful message.
        raw = json.dumps(body, separators=(",", ":"), sort_keys=False)
        headers = {
            "Content-Type": "application/json",
            "X-AUTH-APIKEY": self._key,
            "X-AUTH-SIGNATURE": hmac.new(self._secret, raw.encode("utf-8"), hashlib.sha256).hexdigest(),
        }
        status, data = self._http("POST", f"{self._base}{path}", raw, headers)
        if status >= 400:
            raise _translate(data)
        return data

    # ── the port ─────────────────────────────────────────────────

    def _market(self, symbol: str) -> dict:
        if not self._markets:
            for row in self._public("/exchange/v1/markets_details") or []:
                name = str(row.get("coindcx_name") or row.get("symbol") or "")
                if name:
                    self._markets[name.upper()] = row
        market = self._markets.get(symbol.upper())
        if not market:
            raise ExchangeError(f"{symbol} is not listed on CoinDCX")
        return market

    def symbol_rules(self, symbol: str) -> SymbolRules:
        if symbol in self._rules:
            return self._rules[symbol]
        market = self._market(symbol)
        precision = int(market.get("base_currency_precision") or 2)
        rules = SymbolRules(
            tick_size=float(f"1e-{precision}"),
            step_size=float(market.get("step") or 0.00001),
            min_notional_usd=float(market.get("min_notional") or 5.0),
            # base_currency is the SETTLEMENT currency here and target is the
            # coin. Reading it the usual way round hands back USDT as the asset
            # to sell, which fails in a way that looks like an empty balance.
            base_asset=str(market.get("target_currency_short_name") or ""),
        )
        self._rules[symbol] = rules
        return rules

    def free_balance(self, asset: str) -> float:
        for row in self._private("/exchange/v1/users/balances") or []:
            if str(row.get("currency") or "").upper() == asset.upper():
                return float(row.get("balance") or 0.0)
        return 0.0

    def place(
        self,
        *,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        price: Optional[float],
        stop_price: Optional[float],
        client_order_id: str,
    ) -> OrderRecord:
        rules = self.symbol_rules(symbol)
        mapped = _ORDER_TYPE.get(order_type)
        if not mapped:
            raise ExchangeError(f"unsupported order type {order_type!r}")

        payload = {
            "market": symbol,
            "side": side.lower(),
            "order_type": mapped,
            "total_quantity": float(format_decimal(quantity, rules.step_size)),
            "client_order_id": client_order_id,
        }
        if mapped != "market_order":
            if price is None:
                raise ExchangeError(f"{mapped} needs a limit price")
            payload["price_per_unit"] = float(format_decimal(price, rules.tick_size))
        if mapped == "stop_limit":
            if stop_price is None:
                raise ExchangeError("a stop-limit needs a trigger")
            payload["stop_price"] = float(format_decimal(stop_price, rules.tick_size))

        return _to_record(_first(self._private("/exchange/v1/orders/create", payload)))

    def get_order(self, *, symbol: str, client_order_id: str) -> Optional[OrderRecord]:
        """None for an order that does not exist — the idempotency lookup."""
        try:
            row = _first(self._private("/exchange/v1/orders/status", {"client_order_id": client_order_id}))
        except ExchangeError as exc:
            if _is_missing(str(exc)):
                return None
            raise
        return _to_record(row) if row else None

    def cancel(self, *, symbol: str, client_order_id: str) -> OrderRecord:
        """
        Look up, then cancel by the exchange's own id — CoinDCX's cancel does
        not take a client id. An order already gone resolves to "not found" at
        the lookup, which upstream already treats as success.
        """
        record = self.get_order(symbol=symbol, client_order_id=client_order_id)
        if not record or not record.exchange_order_id:
            raise ExchangeError(f"{client_order_id} is not on the book")
        self._private("/exchange/v1/orders/cancel", {"id": record.exchange_order_id})
        record.status = "CANCELLED"
        return record

    def open_orders(self, symbol: str) -> List[OrderRecord]:
        rows = self._private("/exchange/v1/orders/active_orders", {"market": symbol, "side": "buy"}) or {}
        sells = self._private("/exchange/v1/orders/active_orders", {"market": symbol, "side": "sell"}) or {}
        return [_to_record(row) for row in (_rows(rows) + _rows(sells))]


# ── translation ──────────────────────────────────────────────────────


def _rows(payload) -> List[dict]:
    if isinstance(payload, dict):
        return list(payload.get("orders") or [])
    return list(payload or []) if isinstance(payload, list) else []


def _first(payload) -> dict:
    rows = _rows(payload)
    if rows:
        return rows[0]
    return payload if isinstance(payload, dict) else {}


def _to_record(row: dict) -> OrderRecord:
    total = float(row.get("total_quantity") or 0.0)
    remaining = float(row.get("remaining_quantity") if row.get("remaining_quantity") is not None else total)
    filled = max(total - remaining, 0.0)
    return OrderRecord(
        client_order_id=str(row.get("client_order_id") or ""),
        exchange_order_id=str(row.get("id") or row.get("order_id") or ""),
        status=_STATUS.get(str(row.get("status") or "").lower(), "NEW"),
        filled_qty=filled,
        avg_fill_price=float(row.get("avg_price") or 0.0),
        side=str(row.get("side") or "buy").lower(),
        price=float(row["price_per_unit"]) if row.get("price_per_unit") else None,
        stop_price=float(row["stop_price"]) if row.get("stop_price") else None,
        quantity=total,
    )


def _is_missing(message: str) -> bool:
    lowered = message.lower()
    return "not found" in lowered or "does not exist" in lowered or "invalid order" in lowered


def _translate(data) -> ExchangeError:
    message = str((data or {}).get("message") or (data or {}).get("error") or data or "")
    lowered = message.lower()
    if "duplicate" in lowered:
        return DuplicateOrder(message)
    if "insufficient" in lowered or "balance" in lowered:
        return InsufficientBalance(message)
    if "notional" in lowered or "quantity" in lowered or "precision" in lowered:
        return BelowMinNotional(message)
    return ExchangeError(message)


def _requests_http(method: str, url: str, body, headers: dict):
    import requests

    response = requests.request(method, url, data=body, headers=headers, timeout=15)
    try:
        return response.status_code, response.json()
    except ValueError:
        return response.status_code, {"message": response.text}
