"""Turning intents into orders on the buyer's own account.

The decisions are all in orders.py. What is tested here is the translation and
the four exchange-shaped hazards that live at this boundary — the ones that are
facts about exchanges rather than facts about the strategy.

The one worth reading twice is the locked-balance case. A resting sell locks
its own coin, so free balance excludes it; sizing a replacement exit against
free balance alone caps it at whatever is left over, which after a second buy
is only that newest buy. The rest of the position is then left holding with no
target against it, and nothing about that looks like an error at the time.
"""

import unittest

from executor.exchange import (
    ExchangeError,
    IntentExecutor,
    OrderRecord,
    SymbolRules,
    qty_for_notional,
    quantize_price,
    quantize_qty,
    sellable_qty,
)
from executor.orders import OrderIntent

RULES = SymbolRules(tick_size=0.01, step_size=0.001, min_notional_usd=5.0, base_asset="SOL")


class FakeExchange:
    """Behaves like the real thing in the ways that have bitten us."""

    def __init__(self, *, rules=RULES, balances=None, existing=None, reject_duplicates=True):
        self.rules = rules
        self.balances = dict(balances or {"USDT": 1000.0, "SOL": 0.0})
        self.orders = dict(existing or {})
        self.reject_duplicates = reject_duplicates
        self.placed_calls = []

    def symbol_rules(self, symbol):
        return self.rules

    def free_balance(self, asset):
        return float(self.balances.get(asset, 0.0))

    def place(self, *, symbol, side, order_type, quantity, price, stop_price, client_order_id):
        if self.reject_duplicates and client_order_id in self.orders:
            from executor.exchange import DuplicateOrder

            raise DuplicateOrder(client_order_id)
        self.placed_calls.append(
            {
                "side": side,
                "type": order_type,
                "qty": quantity,
                "price": price,
                "stop": stop_price,
                "cid": client_order_id,
            }
        )
        record = OrderRecord(
            client_order_id=client_order_id,
            exchange_order_id=f"X{len(self.orders) + 1}",
            status="NEW",
            side=side,
            price=price,
            stop_price=stop_price,
            quantity=quantity,
        )
        self.orders[client_order_id] = record
        return record

    def cancel(self, *, symbol, client_order_id):
        record = self.orders.pop(client_order_id, None)
        if not record:
            raise ExchangeError("unknown order")
        record.status = "CANCELLED"
        return record

    def get_order(self, *, symbol, client_order_id):
        return self.orders.get(client_order_id)

    def open_orders(self, symbol):
        return [record for record in self.orders.values() if record.is_open]


def _buy(cid="cfx-c1-e1", usd=11.0, stop=162.00, limit=162.02):
    return OrderIntent(
        action="place",
        kind="entry",
        client_order_id=cid,
        side="buy",
        order_type="stop_limit",
        price=limit,
        stop_price=stop,
        usd_notional=usd,
    )


def _sell(cid="cfx-c1-x0", qty=0.05, price=175.0):
    return OrderIntent(
        action="place",
        kind="exit",
        client_order_id=cid,
        side="sell",
        order_type="limit",
        price=price,
        quantity=qty,
    )


class QuantizationTests(unittest.TestCase):
    def test_quantities_are_floored_never_rounded(self):
        """Rounding up invents coin the buyer does not have."""
        self.assertAlmostEqual(quantize_qty(0.0679, 0.001), 0.067, places=9)
        self.assertAlmostEqual(quantize_qty(0.06999, 0.001), 0.069, places=9)

    def test_prices_are_floored_to_the_tick(self):
        self.assertAlmostEqual(quantize_price(162.0289, 0.01), 162.02, places=9)

    def test_flooring_can_drop_a_pot_back_under_the_minimum(self):
        """A real case, not a rounding curiosity — so the check runs AFTER.

        $5.20 at $105 is 0.0495, which a 0.01 step floors to 0.04 — worth
        $4.20, back under the $5 minimum. Checking the notional before the
        step would have called this placeable and had it rejected.
        """
        rules = SymbolRules(tick_size=0.01, step_size=0.01, min_notional_usd=5.0)
        self.assertEqual(qty_for_notional(5.20, 105.0, rules), 0.0)

    def test_a_pot_that_survives_the_step_is_still_placeable(self):
        """The guard has to bite on the step, not on being near the minimum."""
        rules = SymbolRules(tick_size=0.01, step_size=0.01, min_notional_usd=5.0)
        self.assertAlmostEqual(qty_for_notional(6.00, 105.0, rules), 0.05, places=9)

    def test_a_comfortable_pot_sizes_normally(self):
        self.assertAlmostEqual(qty_for_notional(11.0, 100.0, RULES), 0.11, places=9)

    def test_nothing_is_placeable_at_a_nonsense_price(self):
        self.assertEqual(qty_for_notional(11.0, 0.0, RULES), 0.0)


class LockedBalanceTests(unittest.TestCase):
    """A resting sell locks its own coin. This is the trap."""

    def test_our_own_resting_exit_is_added_back_before_capping(self):
        """Without this the replacement exit sells only the newest buy.

        Position is 0.08 across two buys. The first exit rests on 0.05 of it,
        so free balance shows only the 0.03 from the second buy. Capping
        against free alone would place a 0.03 exit and leave 0.05 held with no
        target against it — and nothing about that looks like an error.
        """
        got = sellable_qty(0.08, free=0.03, locked_by_our_exit=0.05, rules=RULES)
        self.assertAlmostEqual(got, 0.08, places=9)

    def test_capping_against_free_alone_would_have_undersold(self):
        """The bug, stated so the fix cannot be quietly removed."""
        naive = quantize_qty(min(0.08, 0.03), RULES.step_size)
        self.assertAlmostEqual(naive, 0.03, places=9)
        self.assertGreater(sellable_qty(0.08, free=0.03, locked_by_our_exit=0.05, rules=RULES), naive)

    def test_it_still_will_not_sell_coin_that_is_not_there(self):
        """Adding back our own lock is not the same as inventing balance."""
        self.assertAlmostEqual(sellable_qty(0.20, free=0.03, locked_by_our_exit=0.05, rules=RULES), 0.08, places=9)

    def test_the_result_is_floored_to_the_lot_step(self):
        self.assertAlmostEqual(sellable_qty(0.0789, free=0.10, locked_by_our_exit=0.0, rules=RULES), 0.078, places=9)


class PlacementTests(unittest.TestCase):
    def setUp(self):
        self.exchange = FakeExchange()
        self.executor = IntentExecutor(self.exchange, "SOLUSDT")

    def test_a_buy_stop_is_placed_with_both_prices_quantized(self):
        result = self.executor.apply([_buy(stop=162.0289, limit=162.0489)])
        self.assertEqual(len(result.placed), 1)
        call = self.exchange.placed_calls[0]
        self.assertAlmostEqual(call["stop"], 162.02, places=9)
        self.assertAlmostEqual(call["price"], 162.04, places=9)

    def test_a_buy_is_sized_against_the_limit_not_the_trigger(self):
        """The limit is the worst price it can pay; the trigger is not."""
        self.executor.apply([_buy(usd=100.0, stop=100.0, limit=200.0)])
        self.assertAlmostEqual(self.exchange.placed_calls[0]["qty"], 0.5, places=9)

    def test_an_unaffordable_buy_is_reported_not_retried(self):
        self.exchange.balances["USDT"] = 1.0
        result = self.executor.apply([_buy(usd=50.0)])
        self.assertEqual(result.placed, [])
        self.assertIn("free", result.skipped[0][1])

    def test_a_pot_too_small_to_place_is_reported_plainly(self):
        result = self.executor.apply([_buy(usd=1.0)])
        self.assertEqual(result.placed, [])
        self.assertIn("nothing placeable", result.skipped[0][1])

    def test_an_exit_sells_the_whole_position_when_the_coin_is_free(self):
        self.exchange.balances["SOL"] = 0.08
        result = self.executor.apply([_sell(qty=0.08)])
        self.assertAlmostEqual(result.placed[0].quantity, 0.08, places=9)

    def test_an_exit_adds_back_the_coin_its_own_predecessor_locked(self):
        self.exchange.balances["SOL"] = 0.03
        result = self.executor.apply([_sell(qty=0.08)], our_resting_exit_qty=0.05)
        self.assertAlmostEqual(result.placed[0].quantity, 0.08, places=9)

    def test_a_dust_exit_says_so_rather_than_being_rejected_opaquely(self):
        self.exchange.balances["SOL"] = 0.001
        result = self.executor.apply([_sell(qty=0.001, price=100.0)])
        self.assertEqual(result.placed, [])
        self.assertIn("minimum", result.skipped[0][1])


class IdempotencyTests(unittest.TestCase):
    """A crash between deciding and placing is the ordinary case."""

    def test_an_order_that_already_landed_is_adopted_not_replaced(self):
        existing = OrderRecord(client_order_id="cfx-c1-e1", status="NEW", side="buy", quantity=0.06)
        exchange = FakeExchange(existing={"cfx-c1-e1": existing})
        result = IntentExecutor(exchange, "SOLUSDT").apply([_buy()])
        self.assertEqual(result.adopted, [existing])
        self.assertEqual(exchange.placed_calls, [])

    def test_the_same_intent_applied_twice_places_once(self):
        exchange = FakeExchange()
        executor = IntentExecutor(exchange, "SOLUSDT")
        executor.apply([_buy()])
        executor.apply([_buy()])
        self.assertEqual(len(exchange.placed_calls), 1)

    def test_cancelling_an_order_that_is_already_gone_is_success(self):
        """Filled, cancelled, or never placed all mean 'not on the book'."""
        exchange = FakeExchange()
        result = IntentExecutor(exchange, "SOLUSDT").apply(
            [OrderIntent(action="cancel", kind="entry", client_order_id="cfx-c1-e9")]
        )
        self.assertEqual(result.cancelled, ["cfx-c1-e9"])
        self.assertEqual(result.skipped, [])

    def test_a_cancel_then_replace_pair_works_in_one_apply(self):
        exchange = FakeExchange()
        executor = IntentExecutor(exchange, "SOLUSDT")
        exchange.balances["SOL"] = 0.08
        executor.apply([_sell(cid="cfx-c1-x0", qty=0.08)])
        result = executor.apply(
            [
                OrderIntent(action="cancel", kind="exit", client_order_id="cfx-c1-x0"),
                _sell(cid="cfx-c1-x1", qty=0.08, price=176.0),
            ],
            our_resting_exit_qty=0.08,
        )
        self.assertEqual(result.cancelled, ["cfx-c1-x0"])
        self.assertAlmostEqual(result.placed[0].price, 176.0, places=9)


class ResilienceTests(unittest.TestCase):
    """One campaign failing to place must not stop the others managing exits."""

    def test_a_failure_on_one_intent_does_not_abandon_the_rest(self):
        exchange = FakeExchange(balances={"USDT": 1.0, "SOL": 0.08})
        result = IntentExecutor(exchange, "SOLUSDT").apply([_buy(usd=500.0), _sell(qty=0.08)])
        self.assertEqual(len(result.skipped), 1)
        self.assertEqual(len(result.placed), 1)
        self.assertEqual(result.placed[0].side, "sell")

    def test_an_adapter_that_explodes_is_reported_not_propagated(self):
        class Broken(FakeExchange):
            def place(self, **kwargs):
                raise RuntimeError("socket closed")

        result = IntentExecutor(Broken(), "SOLUSDT").apply([_buy()])
        self.assertIn("unexpected", result.skipped[0][1])


class ReconcileTests(unittest.TestCase):
    def test_the_exchange_is_believed_over_local_state(self):
        """Local state is a hypothesis; the exchange is the fact."""
        exchange = FakeExchange(
            existing={
                "cfx-c1-e1": OrderRecord(client_order_id="cfx-c1-e1", status="NEW", side="buy"),
                "cfx-c1-x0": OrderRecord(client_order_id="cfx-c1-x0", status="FILLED", side="sell"),
            }
        )
        open_now = IntentExecutor(exchange, "SOLUSDT").reconcile()
        self.assertEqual(list(open_now), ["cfx-c1-e1"])


if __name__ == "__main__":
    unittest.main()
