"""
executor/binance.py — a Binance Spot adapter satisfying `ExchangeAdapter`.

Standalone on purpose. The executor ships to a buyer's machine, so it carries
its own small REST client rather than the app's broker layer: fewer moving
parts to audit, and nothing in it knows anything about our server.

**The buyer's API keys are used here and nowhere else.** They are read from
their own machine, signed into requests to Binance, and never logged, never
serialized, and never sent anywhere else. There is no code path in this
repository that transmits them.

Four Binance-shaped details are handled here because getting any of them wrong
is silent rather than loud:

1. **Binance spells it `CANCELED`, with one L.** Mapped explicitly — a status
   that falls through unmapped would read as still-open forever.
2. **Numbers must be plain decimal strings.** `str(0.00001)` is `'1e-05'`,
   which Binance rejects outright, and precision beyond the filter is rejected
   too. Formatted from the filter's own step.
3. **A missing order is not an error.** `get_order` returns None so the
   idempotent adopt-or-place decision upstream can be made at all.
4. **"Duplicate order sent" is a -2010, the same code as insufficient
   balance.** They are told apart by message, and conflating them would turn a
   successful recovery into a refusal to trade.

Like the rest of `executor/`, this must not import `engine.cascade`.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import time
from decimal import ROUND_DOWN, Decimal
from typing import Callable, Dict, List, Optional
from urllib.parse import urlencode

from executor.exchange import (
    BelowMinNotional,
    DuplicateOrder,
    ExchangeError,
    InsufficientBalance,
    OrderRecord,
    SymbolRules,
)

_log = logging.getLogger("cascade.executor.binance")

BASE_URL = "https://api.binance.com"
RECV_WINDOW_MS = 5000

# Binance's own status words. CANCELED has one L, and EXPIRED is what a
# stop-limit becomes when it triggers into a price it cannot fill — both would
# read as "still open" if they fell through unmapped, which is how an executor
# ends up waiting forever for an order that is already gone.
_STATUS = {
    "NEW": "NEW",
    "PARTIALLY_FILLED": "PARTIALLY_FILLED",
    "FILLED": "FILLED",
    "CANCELED": "CANCELLED",
    "PENDING_CANCEL": "CANCELLED",
    "REJECTED": "REJECTED",
    "EXPIRED": "CANCELLED",
    "EXPIRED_IN_MATCH": "CANCELLED",
}

_ORDER_TYPE = {"limit": "LIMIT", "stop_limit": "STOP_LOSS_LIMIT", "market": "MARKET"}


def format_decimal(value: float, step: float) -> str:
    """
    A plain decimal string at the filter's own precision.

    Two failures live here and neither is loud. `str(0.00001)` is `'1e-05'`,
    which Binance rejects as malformed; and a quantity carrying more decimals
    than LOT_SIZE allows is rejected as a filter failure. Both look like "the
    order just did not go through".
    """
    step_decimal = Decimal(str(step or "1"))
    quantized = Decimal(str(value)).quantize(step_decimal, rounding=ROUND_DOWN)
    text = format(quantized.normalize(), "f")
    return text if text not in ("-0", "") else "0"


class BinanceSpotAdapter:
    """
    `http` is injected so the whole adapter is testable without a network and
    without a real account — which matters more than usual here, since the
    alternative is exercising order placement against somebody's money.
    """

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

    # ── plumbing ─────────────────────────────────────────────────

    def _signed(self, params: dict) -> dict:
        payload = dict(params)
        payload["timestamp"] = self._now_ms()
        payload["recvWindow"] = RECV_WINDOW_MS
        query = urlencode(payload)
        payload["signature"] = hmac.new(self._secret, query.encode("utf-8"), hashlib.sha256).hexdigest()
        return payload

    def _call(self, method: str, path: str, params: dict, *, signed: bool = True) -> dict:
        body = self._signed(params) if signed else dict(params)
        headers = {"X-MBX-APIKEY": self._key} if signed else {}
        status, data = self._http(method, f"{self._base}{path}", body, headers)
        if status >= 400:
            raise _translate(data)
        return data

    # ── the port ─────────────────────────────────────────────────

    def symbol_rules(self, symbol: str) -> SymbolRules:
        """
        Always from the exchange's own exchangeInfo, never from the feed.

        The feed ships an advisory copy for convenience and the executor is
        required to prefer this: filters change, and an order rejected on a
        stale tick size is the executor's problem to prevent rather than the
        publisher's to cause.
        """
        if symbol in self._rules:
            return self._rules[symbol]
        data = self._call("GET", "/api/v3/exchangeInfo", {"symbol": symbol}, signed=False)
        rows = data.get("symbols") or []
        if not rows:
            raise ExchangeError(f"{symbol} is not listed")
        row = rows[0]
        filters = {item.get("filterType"): item for item in row.get("filters") or []}
        rules = SymbolRules(
            tick_size=float(filters.get("PRICE_FILTER", {}).get("tickSize") or 0.01),
            step_size=float(filters.get("LOT_SIZE", {}).get("stepSize") or 0.00001),
            min_notional_usd=float(
                filters.get("NOTIONAL", {}).get("minNotional")
                or filters.get("MIN_NOTIONAL", {}).get("minNotional")
                or 5.0
            ),
            base_asset=str(row.get("baseAsset") or ""),
        )
        self._rules[symbol] = rules
        return rules

    def free_balance(self, asset: str) -> float:
        data = self._call("GET", "/api/v3/account", {})
        for row in data.get("balances") or []:
            if str(row.get("asset")).upper() == asset.upper():
                return float(row.get("free") or 0.0)
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
        binance_type = _ORDER_TYPE.get(order_type)
        if not binance_type:
            raise ExchangeError(f"unsupported order type {order_type!r}")

        params = {
            "symbol": symbol,
            "side": side.upper(),
            "type": binance_type,
            "quantity": format_decimal(quantity, rules.step_size),
            "newClientOrderId": client_order_id,
        }
        if binance_type != "MARKET":
            params["timeInForce"] = "GTC"
            if price is None:
                raise ExchangeError(f"{binance_type} needs a limit price")
            params["price"] = format_decimal(price, rules.tick_size)
        if binance_type == "STOP_LOSS_LIMIT":
            if stop_price is None:
                raise ExchangeError("a stop-limit needs a trigger")
            params["stopPrice"] = format_decimal(stop_price, rules.tick_size)

        return _to_record(self._call("POST", "/api/v3/order", params))

    def cancel(self, *, symbol: str, client_order_id: str) -> OrderRecord:
        return _to_record(
            self._call("DELETE", "/api/v3/order", {"symbol": symbol, "origClientOrderId": client_order_id})
        )

    def get_order(self, *, symbol: str, client_order_id: str) -> Optional[OrderRecord]:
        """
        None for an order that does not exist.

        This is the idempotency lookup: upstream uses it to decide whether an
        intent already landed before a crash. Raising here instead would turn
        "not placed yet" into an error and stop the order going out at all.
        """
        try:
            return _to_record(
                self._call("GET", "/api/v3/order", {"symbol": symbol, "origClientOrderId": client_order_id})
            )
        except ExchangeError as exc:
            if "does not exist" in str(exc).lower() or "unknown order" in str(exc).lower():
                return None
            raise

    def open_orders(self, symbol: str) -> List[OrderRecord]:
        return [_to_record(row) for row in self._call("GET", "/api/v3/openOrders", {"symbol": symbol})]


# ── translation ──────────────────────────────────────────────────────


def _to_record(row: dict) -> OrderRecord:
    filled = float(row.get("executedQty") or 0.0)
    quote = float(row.get("cummulativeQuoteQty") or 0.0)  # Binance's own spelling
    return OrderRecord(
        client_order_id=str(row.get("clientOrderId") or row.get("origClientOrderId") or ""),
        exchange_order_id=str(row.get("orderId") or ""),
        status=_STATUS.get(str(row.get("status") or "NEW").upper(), "NEW"),
        filled_qty=filled,
        avg_fill_price=(quote / filled) if filled > 0 else 0.0,
        side=str(row.get("side") or "BUY").lower(),
        price=float(row["price"]) if row.get("price") else None,
        stop_price=float(row["stopPrice"]) if row.get("stopPrice") else None,
        quantity=float(row.get("origQty") or 0.0),
    )


def _translate(data: dict) -> ExchangeError:
    """
    Map Binance's error to one we can act on.

    The important pair: "Duplicate order sent" and "insufficient balance" share
    code -2010. Told apart by message, because conflating them turns a
    successful recovery — the order landed before we crashed — into a refusal
    to trade at all.
    """
    message = str((data or {}).get("msg") or data or "")
    code = int((data or {}).get("code") or 0)
    lowered = message.lower()
    if "duplicate order" in lowered:
        return DuplicateOrder(message)
    if "insufficient balance" in lowered:
        return InsufficientBalance(message)
    if code == -1013 or "notional" in lowered or "lot_size" in lowered:
        return BelowMinNotional(message)
    return ExchangeError(f"{code}: {message}" if code else message)


def _requests_http(method: str, url: str, params: dict, headers: dict):
    import requests

    response = requests.request(method, url, params=params, headers=headers, timeout=15)
    try:
        return response.status_code, response.json()
    except ValueError:
        return response.status_code, {"msg": response.text}
