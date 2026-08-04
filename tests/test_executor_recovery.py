"""The sleeping laptop.

Most of this file is about ORDER. The wake ladder's steps are not a checklist
where sequence is a nicety — protecting a held position has to happen before
any geometry work, and re-placing entries has to happen after it, because
re-placing against stale levels is worse than not re-placing at all. Both are
asserted directly rather than left to the reader of the list.

The other thing worth stating in tests is what confirmation does NOT gate.
Past six hours the executor stops auto-resuming trading, and it still asks the
exchange, still ingests fills, and still gives a held position its target back.
Making someone click a button before their coin is protected would be enforcing
a policy with their money, and the policy exists to protect them.
"""

import unittest

from executor.orders import CampaignOrders, Candle, Fill, OrderBook
from executor.recovery import (
    ASK_EXCHANGE,
    CONFIRM_GAP_SEC,
    NORMAL_GAP_SEC,
    PROTECT_POSITION,
    REPLACE_ENTRIES,
    REPLAY_GEOMETRY,
    RecoveryPlan,
    ShutdownRecord,
    classify_gap,
    plan_for_sleep,
    plan_recovery,
    record_sleep_outcome,
    tp_catchup_intent,
    wake_report,
)


def _orders(campaign_id="c1", **overrides):
    kwargs = {
        "campaign_id": campaign_id,
        "symbol": "SOLUSDT",
        "mother_high": 178.42,
        "exchange": "binance",
        "tick_size": 0.01,
        "min_notional_usd": 5.0,
        "median_bar_pct": 0.002,
    }
    kwargs.update(overrides)
    return CampaignOrders(**kwargs)


def _armed(orders):
    """Bring a campaign to the state where an entry is actually resting."""
    orders.collect(
        Candle(1, 175, 175, 162.0, 163),
        [{"leg_id": 4, "level": 4, "price": 162.32, "usd": 7.25}],
    )
    orders.advance_stop(Candle(2, 163, 163, 162.0, 162.0))
    orders.advance_stop(Candle(3, 162, 162, 161.5, 161.5))
    orders.entry_resting = True
    return orders


class GapClassificationTests(unittest.TestCase):
    def test_a_short_gap_is_the_ordinary_reconcile(self):
        self.assertEqual(classify_gap(30), "normal")

    def test_a_medium_gap_is_full_recovery(self):
        self.assertEqual(classify_gap(NORMAL_GAP_SEC + 1), "full")
        self.assertEqual(classify_gap(CONFIRM_GAP_SEC), "full")

    def test_a_long_gap_needs_a_human(self):
        self.assertEqual(classify_gap(CONFIRM_GAP_SEC + 1), "confirm")

    def test_sleeping_armed_skips_straight_to_full_however_short_the_gap(self):
        """It went away with an order live, so its picture cannot be trusted."""
        self.assertEqual(classify_gap(5, slept_armed=True), "full")

    def test_sleeping_armed_still_needs_a_human_after_six_hours(self):
        self.assertEqual(classify_gap(CONFIRM_GAP_SEC + 1, slept_armed=True), "confirm")


class LadderOrderTests(unittest.TestCase):
    """The sequence is the design, so it is asserted rather than described."""

    def test_protecting_the_position_comes_before_any_geometry(self):
        """An unprotected position is more urgent than a correct chart."""
        names = plan_recovery(3600).step_names
        self.assertLess(names.index(PROTECT_POSITION.name), names.index(REPLAY_GEOMETRY.name))

    def test_replacing_entries_comes_after_geometry_is_current(self):
        """Re-placing against stale levels is worse than not re-placing."""
        names = plan_recovery(3600).step_names
        self.assertGreater(names.index(REPLACE_ENTRIES.name), names.index(REPLAY_GEOMETRY.name))

    def test_the_exchange_is_asked_first(self):
        self.assertEqual(plan_recovery(3600).step_names[0], ASK_EXCHANGE.name)

    def test_a_short_gap_does_not_replay_or_replace(self):
        names = plan_recovery(30).step_names
        self.assertNotIn(REPLAY_GEOMETRY.name, names)
        self.assertNotIn(REPLACE_ENTRIES.name, names)

    def test_a_short_gap_still_protects_the_position(self):
        self.assertIn(PROTECT_POSITION.name, plan_recovery(30).step_names)


class ConfirmationTests(unittest.TestCase):
    def test_a_long_gap_requires_confirmation(self):
        plan = plan_recovery(CONFIRM_GAP_SEC + 60)
        self.assertTrue(plan.requires_confirmation)
        self.assertIn("no new entries go out", plan.note)
        # The duration belongs to wake_report, which opens with it. Repeating
        # it here made the message stutter: "Away for 24.0h. Away for 24.0
        # hours. Positions are protected…"
        self.assertNotIn("Away for", plan.note)

    def test_confirmation_never_gates_protecting_a_held_position(self):
        """Their coin does not wait for a button. That is the whole point."""
        plan = plan_recovery(CONFIRM_GAP_SEC + 60)
        self.assertIn(PROTECT_POSITION.name, plan.step_names)
        self.assertIn(ASK_EXCHANGE.name, plan.step_names)

    def test_confirmation_does_gate_new_entries(self):
        plan = plan_recovery(CONFIRM_GAP_SEC + 60)
        self.assertNotIn(REPLACE_ENTRIES.name, plan.step_names)

    def test_a_medium_gap_resumes_on_its_own(self):
        self.assertFalse(plan_recovery(3600).requires_confirmation)


class SleepPlanTests(unittest.TestCase):
    """The twin invariants, at the moment the lid closes."""

    def test_a_resting_entry_is_cancelled_and_a_bare_position_gets_an_exit(self):
        book = OrderBook()
        book.track(_armed(_orders("armed")))
        holding = book.track(_orders("holding"))
        holding.on_entry_filled(Fill(price=172.0, quantity=0.03, timestamp=1))

        plan = plan_for_sleep(book, now=1000.0)
        actions = {(campaign_id, intent.action, intent.kind) for campaign_id, intent in plan.intents}
        self.assertIn(("armed", "cancel", "entry"), actions)
        self.assertIn(("holding", "place", "exit"), actions)

    def test_the_record_names_the_exposure_before_the_machine_stops(self):
        book = OrderBook()
        book.track(_armed(_orders("armed")))
        plan = plan_for_sleep(book, now=1000.0)
        self.assertEqual(plan.record.armed_exposure_usd, 7.25)
        self.assertEqual(plan.record.resting_entry_ids, ["cfx-armed-e1"])

    def test_a_clean_sleep_says_so_plainly(self):
        book = OrderBook()
        plan = plan_for_sleep(book, now=1000.0)
        record_sleep_outcome(plan.record, cancelled_ids=[])
        self.assertIn("Nothing can be bought while away", plan.message)

    def test_a_failed_cancel_is_recorded_rather_than_retried(self):
        """The lid is closing and wifi is going. Recording beats preventing."""
        book = OrderBook()
        book.track(_armed(_orders("armed")))
        plan = plan_for_sleep(book, now=1000.0)
        record_sleep_outcome(plan.record, cancelled_ids=[])  # nothing landed
        self.assertTrue(plan.record.slept_armed)
        self.assertEqual(plan.record.armed_exposure_usd, 7.25)
        self.assertIn("can still fill while this machine is away", plan.message)

    def test_a_cancel_that_landed_clears_the_flag(self):
        book = OrderBook()
        book.track(_armed(_orders("armed")))
        plan = plan_for_sleep(book, now=1000.0)
        record_sleep_outcome(plan.record, cancelled_ids=["cfx-armed-e1"])
        self.assertFalse(plan.record.slept_armed)
        self.assertEqual(plan.record.armed_exposure_usd, 0.0)

    def test_the_record_survives_a_round_trip(self):
        """It has to outlive the process that wrote it, or it is worthless."""
        original = ShutdownRecord(
            shutdown_at=1000.0,
            reason="sleep",
            slept_armed=True,
            armed_exposure_usd=7.25,
            resting_entry_ids=["cfx-armed-e1"],
            unprotected_campaigns=["holding"],
        )
        restored = ShutdownRecord.from_dict(original.to_dict())
        self.assertEqual(restored.to_dict(), original.to_dict())

    def test_no_record_restores_as_none_rather_than_an_empty_one(self):
        """An absent record means a crash, which is not the same as a clean stop."""
        self.assertIsNone(ShutdownRecord.from_dict(None))


class TPCatchupTests(unittest.TestCase):
    """Price ran past target while the machine was away."""

    def test_price_past_target_exits_at_market(self):
        intent = tp_catchup_intent("c1", target=175.0, market_price=178.0, quantity=0.03)
        self.assertEqual(intent.order_type, "market")
        self.assertEqual(intent.side, "sell")
        self.assertAlmostEqual(intent.quantity, 0.03)

    def test_price_below_target_does_nothing(self):
        self.assertIsNone(tp_catchup_intent("c1", target=175.0, market_price=173.0, quantity=0.03))

    def test_price_exactly_at_target_takes_it(self):
        self.assertIsNotNone(tp_catchup_intent("c1", target=175.0, market_price=175.0, quantity=0.03))

    def test_nothing_held_means_nothing_to_catch_up(self):
        self.assertIsNone(tp_catchup_intent("c1", target=175.0, market_price=178.0, quantity=0.0))


class WakeReportTests(unittest.TestCase):
    """One paragraph a buyer can act on, not a log they have to decode."""

    def test_it_names_the_gap(self):
        self.assertIn("2.0h", wake_report(plan_recovery(7200), None))

    def test_it_says_when_the_machine_slept_armed(self):
        record = ShutdownRecord(shutdown_at=0, slept_armed=True, armed_exposure_usd=7.25)
        report = wake_report(plan_recovery(300, slept_armed=True), record)
        self.assertIn("$7.25", report)
        self.assertIn("nothing was watching", report)

    def test_it_names_anything_holding_without_a_sell(self):
        record = ShutdownRecord(shutdown_at=0, unprotected_campaigns=["c1"])
        self.assertIn("placing now", wake_report(plan_recovery(300), record))

    def test_a_long_gap_says_no_new_entries_go_out(self):
        report = wake_report(plan_recovery(CONFIRM_GAP_SEC + 60), None)
        self.assertIn("no new entries", report)

    def test_short_gaps_are_reported_in_minutes(self):
        self.assertIn("5m", wake_report(RecoveryPlan(300, "full", [], False), None))


if __name__ == "__main__":
    unittest.main()
