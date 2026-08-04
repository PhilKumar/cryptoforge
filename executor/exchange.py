"""
executor/exchange.py — turning intents into orders on the buyer's own account.

Deliberately the thinnest layer in the executor. Every decision about WHAT to
place lives in `orders.py`; this only knows how to say it to an exchange, how
to survive saying it twice, and how to read back what actually happened.

The buyer's API keys are used here and nowhere else. They are read from their
own machine and never leave it — there is no path in this codebase that sends
them anywhere, which is the whole reason the feed publishes geometry instead of
orders.

Four exchange-shaped hazards get handled here rather than upstream, because
they are facts about exchanges and not about the strategy:

1. **A client order id can be rejected as a duplicate.** That is not a failure,
   it is the recovery path working: the order landed before we crashed.
2. **Quantities must be floored to the lot step**, and flooring can drop the
   notional back under the minimum — so the check has to happen after.
3. **A resting sell locks its own coin.** Free balance excludes it, so sizing a
   replacement exit against free balance alone sells only the newest buy.
4. **Cancelling an order that is already gone is success**, not an error.

Like the rest of `executor/`, this must not import `engine.cascade`.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Protocol

from executor.orders import OrderIntent

_log = logging.getLogger("cascade.executor.exchange")


class ExchangeError(Exception):
    """Anything the exchange refused. Subclassed where the reason matters."""


class DuplicateOrder(ExchangeError):
    """This client order id is already on the book — it landed before we crashed."""


class InsufficientBalance(ExchangeError):
    """Not enough free balance. Never retried blindly: retrying cannot make money."""


class BelowMinNotional(ExchangeError):
    """Too small to place. Usually means the pot has not filled up yet."""


@dataclass(frozen=True)
class SymbolRules:
    """The exchange's own filters. Always re-fetched, never taken from the feed.

    The feed ships an `advisory` copy for convenience, and the executor is
    required to prefer these: filters change, and an order rejected on a stale
    tick size is the executor's problem to prevent rather than ours to cause.
    """

    tick_size: float = 0.01
    step_size: float = 0.00001
    min_notional_usd: float = 5.0
    base_asset: str = ""


@dataclass
class OrderRecord:
    """What the exchange says about one order. The exchange is the fact."""

    client_order_id: str
    exchange_order_id: str = ""
    status: str = "NEW"  # NEW | PARTIALLY_FILLED | FILLED | CANCELLED | REJECTED
    filled_qty: float = 0.0
    avg_fill_price: float = 0.0
    side: str = "buy"
    price: Optional[float] = None
    stop_price: Optional[float] = None
    quantity: float = 0.0

    @property
    def is_open(self) -> bool:
        return self.status in {"NEW", "PARTIALLY_FILLED"}


class ExchangeAdapter(Protocol):
    """The narrow port the executor needs. Anything satisfying this will do."""

    def symbol_rules(self, symbol: str) -> SymbolRules: ...

    def free_balance(self, asset: str) -> float: ...

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
    ) -> OrderRecord: ...

    def cancel(self, *, symbol: str, client_order_id: str) -> OrderRecord: ...

    def get_order(self, *, symbol: str, client_order_id: str) -> Optional[OrderRecord]: ...

    def open_orders(self, symbol: str) -> List[OrderRecord]: ...


# ── quantization ─────────────────────────────────────────────────────


def quantize_price(price: float, tick_size: float) -> float:
    if tick_size <= 0:
        return float(price)
    return math.floor(float(price) / tick_size + 1e-9) * tick_size


def quantize_qty(quantity: float, step_size: float) -> float:
    """
    Floor, never round. Rounding up invents coin the buyer does not have, and
    an exchange rejects the order — or worse, fills it and leaves them short.
    """
    if step_size <= 0:
        return float(quantity)
    return math.floor(float(quantity) / step_size + 1e-9) * step_size


def qty_for_notional(usd: float, price: float, rules: SymbolRules) -> float:
    """
    How much base to buy for this much quote, floored to the lot step.

    Returns 0 when the floored quantity would no longer clear the minimum —
    which is a real case, not a rounding curiosity: a pot barely over the
    minimum can fall back under it as soon as the step is applied.
    """
    if price <= 0 or usd <= 0:
        return 0.0
    quantity = quantize_qty(usd / price, rules.step_size)
    if quantity <= 0 or quantity * price < rules.min_notional_usd:
        return 0.0
    return quantity


def sellable_qty(
    wanted: float,
    *,
    free: float,
    locked_by_our_exit: float,
    rules: SymbolRules,
) -> float:
    """
    How much may actually be sold, given a resting exit of our own.

    The trap this exists for: a resting sell LOCKS its coin, so free balance
    excludes it. Sizing a replacement exit against free balance alone caps it
    at whatever is left over — which after a second buy is only that newest
    buy, leaving the rest of the position with no target against it. Our own
    resting quantity has to be added back before capping, because cancelling
    that order is about to return it.
    """
    available = float(free) + float(locked_by_our_exit)
    return quantize_qty(min(float(wanted), available), rules.step_size)


# ── applying intents ─────────────────────────────────────────────────


@dataclass
class ApplyResult:
    placed: List[OrderRecord] = field(default_factory=list)
    cancelled: List[str] = field(default_factory=list)
    adopted: List[OrderRecord] = field(default_factory=list)
    skipped: List[tuple] = field(default_factory=list)  # (client_order_id, reason)

    @property
    def resting_entry(self) -> Optional[OrderRecord]:
        for record in self.placed + self.adopted:
            if record.side == "buy" and record.is_open:
                return record
        return None


class IntentExecutor:
    """
    Applies OrderIntents to one symbol on one account.

    Every failure is reported rather than raised: a rejected order is
    information the buyer needs, and one campaign failing to place must not
    stop the others from managing their exits.
    """

    def __init__(self, adapter: ExchangeAdapter, symbol: str, *, quote_asset: str = "USDT"):
        self._adapter = adapter
        self._symbol = symbol
        self._quote = quote_asset

    def apply(self, intents: List[OrderIntent], *, our_resting_exit_qty: float = 0.0) -> ApplyResult:
        result = ApplyResult()
        rules = self._adapter.symbol_rules(self._symbol)
        for intent in intents:
            try:
                if intent.action == "cancel":
                    self._cancel(intent, result)
                else:
                    self._place(intent, rules, result, our_resting_exit_qty)
            except ExchangeError as exc:
                result.skipped.append((intent.client_order_id, str(exc)))
            except Exception as exc:  # an adapter blowing up is not a reason to stop
                _log.exception("placing %s failed", intent.client_order_id)
                result.skipped.append((intent.client_order_id, f"unexpected: {exc}"))
        return result

    def _cancel(self, intent: OrderIntent, result: ApplyResult) -> None:
        try:
            self._adapter.cancel(symbol=self._symbol, client_order_id=intent.client_order_id)
        except ExchangeError:
            # Already filled, already cancelled, never placed. All three mean
            # the same thing to us: it is not on the book, which is what we
            # asked for. Treating this as a failure would block the replacement.
            pass
        result.cancelled.append(intent.client_order_id)

    def _place(
        self,
        intent: OrderIntent,
        rules: SymbolRules,
        result: ApplyResult,
        our_resting_exit_qty: float,
    ) -> None:
        # Idempotency first: if this id is already on the exchange, the order
        # landed and we are recovering from a crash between deciding and
        # placing. Adopt it rather than placing a second one.
        existing = self._adapter.get_order(symbol=self._symbol, client_order_id=intent.client_order_id)
        if existing:
            result.adopted.append(existing)
            return

        price = intent.stop_price if intent.order_type == "stop_limit" else intent.price
        if intent.side == "buy":
            quantity = self._buy_quantity(intent, rules)
        else:
            quantity = self._sell_quantity(intent, rules, our_resting_exit_qty)
        if quantity <= 0:
            raise BelowMinNotional(
                f"{intent.client_order_id}: nothing placeable at {price} "
                f"(step {rules.step_size}, min ${rules.min_notional_usd})"
            )

        record = self._adapter.place(
            symbol=self._symbol,
            side=intent.side,
            order_type=intent.order_type,
            quantity=quantity,
            price=None if intent.price is None else quantize_price(intent.price, rules.tick_size),
            stop_price=None if intent.stop_price is None else quantize_price(intent.stop_price, rules.tick_size),
            client_order_id=intent.client_order_id,
        )
        result.placed.append(record)

    def _buy_quantity(self, intent: OrderIntent, rules: SymbolRules) -> float:
        # Size against the LIMIT price, not the trigger: the limit is the worst
        # price this order can pay, so sizing on the trigger would ask for more
        # quote than the fill may actually need.
        price = intent.price or intent.stop_price or 0.0
        quantity = qty_for_notional(intent.usd_notional or 0.0, price, rules)
        if quantity <= 0:
            return 0.0
        free = self._adapter.free_balance(self._quote)
        if free < quantity * price:
            raise InsufficientBalance(
                f"{intent.client_order_id}: needs ${quantity * price:,.2f} of {self._quote}, free ${free:,.2f}"
            )
        return quantity

    def _sell_quantity(self, intent: OrderIntent, rules: SymbolRules, our_resting_exit_qty: float) -> float:
        wanted = intent.quantity or 0.0
        free = self._adapter.free_balance(rules.base_asset or self._symbol.replace(self._quote, ""))
        quantity = sellable_qty(wanted, free=free, locked_by_our_exit=our_resting_exit_qty, rules=rules)
        if quantity > 0 and intent.price and quantity * intent.price < rules.min_notional_usd:
            # A dust remainder cannot be sold as a limit order. Saying so is
            # better than a rejection the buyer has to decode.
            raise BelowMinNotional(
                f"{intent.client_order_id}: {quantity} is worth less than the ${rules.min_notional_usd} minimum"
            )
        return quantity

    # ── recovery ─────────────────────────────────────────────────

    def reconcile(self) -> Dict[str, OrderRecord]:
        """
        Ask the exchange and believe it over local state.

        Local state is a hypothesis; the exchange is the fact. That ordering is
        the whole recovery rule, and it matters most exactly when local state
        looks confident — after a crash, a sleep, or a network partition where
        we kept reasoning from a picture that had stopped being true.
        """
        return {record.client_order_id: record for record in self._adapter.open_orders(self._symbol)}
