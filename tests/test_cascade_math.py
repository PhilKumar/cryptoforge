"""Unit tests for the pure cascade model in engine/cascade.py."""

import unittest

from engine.cascade import (
    FEE_PCT_PER_SIDE,
    TP_FIB_LEVEL,
    Campaign,
    Candle,
    FibLadder,
    Fill,
    Leg,
    Round,
    Trendline,
    build_fib_ladder_and_pool,
    compute_tp_price,
    exchange_fill_ts,
    leg_broken,
    plan_leg_orders,
    recompute_avg_entry_price,
    round_trip_fee,
    timeframe_for_level,
    trendline_price,
)


def _campaign(capital=2000.0, mother_high=105.0, mother_low=99.0, min_notional=5.0) -> Campaign:
    return Campaign(
        campaign_id="test123",
        symbol="BTCUSDT",
        capital_usd=capital,
        mother_high=mother_high,
        mother_low=mother_low,
        mother_timestamp=0,
        min_notional_usd=min_notional,
    )


def _leg(campaign, low, touch_high, leg_id=1, trendline_id=1) -> Leg:
    leg = Leg(leg_id=leg_id, trendline_id=trendline_id, low=low, touch_high=touch_high, touch_timestamp=100)
    campaign.legs.append(leg)
    return leg


class TrendlineGeometryTests(unittest.TestCase):
    def setUp(self):
        self.tl = Trendline(1, 105.0, 0, 103.0, 2)

    def test_line_price_interpolates(self):
        self.assertAlmostEqual(trendline_price(self.tl, 0), 105.0)
        self.assertAlmostEqual(trendline_price(self.tl, 2), 103.0)
        self.assertAlmostEqual(trendline_price(self.tl, 4), 101.0)

    def test_leg_broken_requires_red_close_below_low(self):
        self.assertTrue(leg_broken(Candle(7, 102.0, 102.2, 98, 98.3), 99.5))
        self.assertFalse(leg_broken(Candle(7, 98.0, 102.2, 97.9, 98.3), 99.5))  # green candle
        self.assertFalse(leg_broken(Candle(7, 102.0, 102.2, 98, 100.0), 99.5))  # close above low


class FibLadderPoolTests(unittest.TestCase):
    def test_level_prices(self):
        fib = FibLadder(high_anchor=102.0, low_anchor=99.5)
        self.assertAlmostEqual(fib.level_price(0), 102.0)
        self.assertAlmostEqual(fib.level_price(1), 99.5)
        self.assertAlmostEqual(fib.level_price(2), 97.0)
        self.assertAlmostEqual(fib.level_price(4), 92.0)
        self.assertAlmostEqual(fib.level_price(8), 82.0)

    def test_first_fib_funds_off_the_mother_high(self):
        campaign = _campaign(capital=2000.0, mother_high=100.0)
        leg1 = _leg(campaign, low=95.0, touch_high=97.0)
        build_fib_ladder_and_pool(campaign, leg1)
        # 5% down from the mother high => pool 5 * (2000/100) = $100
        self.assertAlmostEqual(leg1.allocation_pct, 5.0)
        self.assertAlmostEqual(leg1.pool_usd, 100.0)

    def test_later_fibs_fund_off_the_previous_fib_level_1(self):
        """Each fib after the first only funds the remaining move from the
        previous fib's level 1 down to its own level 1."""
        campaign = _campaign(capital=2000.0, mother_high=100.0)
        leg1 = _leg(campaign, low=95.0, touch_high=97.0)
        build_fib_ladder_and_pool(campaign, leg1)
        leg2 = _leg(campaign, low=92.0, touch_high=95.0, leg_id=2)
        build_fib_ladder_and_pool(campaign, leg2)
        # (95 - 92) / 95 = 3.158%, measured from fib 1 level 1 — not from the mother high
        self.assertAlmostEqual(leg2.allocation_pct, (95.0 - 92.0) / 95.0 * 100, places=6)
        self.assertAlmostEqual(leg2.pool_usd, leg2.allocation_pct * 20, places=6)
        # total fall from the mother high is still reported for display
        self.assertAlmostEqual(leg2.leg_pct_from_mother, 8.0)

    def test_escalation_flag_above_one_percent_touch_depth(self):
        campaign = _campaign(mother_high=100.0)
        shallow = _leg(campaign, low=98.0, touch_high=99.5)
        build_fib_ladder_and_pool(campaign, shallow)
        self.assertFalse(shallow.escalated)
        deep = _leg(campaign, low=95.0, touch_high=98.0, leg_id=2)
        build_fib_ladder_and_pool(campaign, deep)
        self.assertTrue(deep.escalated)
        self.assertEqual(timeframe_for_level(campaign, deep, 2), "5m")
        self.assertEqual(timeframe_for_level(campaign, deep, 4), "15m")
        self.assertEqual(timeframe_for_level(campaign, shallow, 4), "5m")

    def test_order_timeframe_labels_follow_the_campaign_timeframe(self):
        """A 4H campaign must not label its deep rungs 15m. The labels are
        relative to whatever the campaign is actually being stepped on, and 4H
        is the cap, so one rung up from 4H is still 4H."""
        campaign = _campaign(mother_high=100.0)
        campaign.timeframe = "4h"
        deep = _leg(campaign, low=95.0, touch_high=98.0)
        build_fib_ladder_and_pool(campaign, deep)
        self.assertTrue(deep.escalated)
        self.assertEqual(timeframe_for_level(campaign, deep, 2), "4h")
        self.assertEqual(timeframe_for_level(campaign, deep, 4), "4h")
        campaign.timeframe = "15m"
        self.assertEqual(timeframe_for_level(campaign, deep, 4), "1h")


class PlanLegOrdersTests(unittest.TestCase):
    def test_every_level_keeps_its_own_twenty_thirty_fifty(self):
        campaign = _campaign(capital=2000.0, mother_high=100.0, min_notional=5.0)
        leg = _leg(campaign, low=95.0, touch_high=98.0)
        build_fib_ladder_and_pool(campaign, leg)  # $100 pool
        plan_leg_orders(campaign, leg)
        self.assertAlmostEqual(leg.pending_orders[2].usd_notional, 20.0)
        self.assertAlmostEqual(leg.pending_orders[4].usd_notional, 30.0)
        self.assertAlmostEqual(leg.pending_orders[8].usd_notional, 50.0)
        for level in (2, 4, 8):
            order = leg.pending_orders[level]
            self.assertEqual(order.status, "PENDING")
            self.assertAlmostEqual(order.price, leg.fib.level_price(level))

    def test_a_tiny_share_is_kept_not_thrown_away(self):
        """Sixty cents is not an order, but it is not nothing either. It stays on
        its level, and the running total picks it up when price gets there."""
        campaign = _campaign(capital=2000.0, mother_high=100.0, min_notional=5.0)
        leg = _leg(campaign, low=99.85, touch_high=99.95)  # 0.15% depth => $3 pool
        build_fib_ladder_and_pool(campaign, leg)
        plan_leg_orders(campaign, leg)
        self.assertAlmostEqual(leg.pending_orders[2].usd_notional, 0.60)
        self.assertAlmostEqual(leg.pending_orders[4].usd_notional, 0.90)
        self.assertAlmostEqual(leg.pending_orders[8].usd_notional, 1.50)
        for level in (2, 4, 8):
            self.assertEqual(leg.pending_orders[level].status, "PENDING")

    def test_no_fib_hands_money_to_another(self):
        """Each fib splits its own pool and only its own pool. There is no
        carry-forward and no cross-fib merging; overlap is resolved by price
        when the running total collects levels."""
        campaign = _campaign(capital=2000.0, mother_high=100.0, min_notional=5.0)
        leg1 = _leg(campaign, low=99.85, touch_high=99.95)  # too small to place alone
        build_fib_ladder_and_pool(campaign, leg1)
        plan_leg_orders(campaign, leg1)
        leg2 = _leg(campaign, low=95.0, touch_high=99.0, leg_id=2)
        build_fib_ladder_and_pool(campaign, leg2)
        plan_leg_orders(campaign, leg2)

        for leg in (leg1, leg2):
            total = sum(o.usd_notional for o in leg.pending_orders.values())
            self.assertAlmostEqual(total, leg.pool_usd, places=1)
        self.assertAlmostEqual(campaign.carry_forward_usd, 0.0)

    def test_the_ladder_never_allocates_more_than_the_capital_left(self):
        """The cap scales the whole ladder rather than emptying one end of it:
        every level keeps its 20/30/50 share of a smaller pool, because the
        running total is what decides where money actually goes."""
        campaign = _campaign(capital=100.0, mother_high=100.0, min_notional=5.0)
        campaign.all_fills.append(Fill(price=50.0, quantity=1.6, level=2, leg_id=1, timestamp=1))  # $80 spent
        leg = _leg(campaign, low=95.0, touch_high=98.0)
        build_fib_ladder_and_pool(campaign, leg)
        leg.pool_usd = 60.0  # force a pool larger than remaining capital ($20)
        plan_leg_orders(campaign, leg)

        total = sum(o.usd_notional for o in leg.pending_orders.values())
        self.assertLessEqual(total, 20.0 + 0.05)
        self.assertAlmostEqual(leg.pending_orders[2].usd_notional, 4.0, places=1)
        self.assertAlmostEqual(leg.pending_orders[4].usd_notional, 6.0, places=1)
        self.assertAlmostEqual(leg.pending_orders[8].usd_notional, 10.0, places=1)


class AvgEntryAndTpTests(unittest.TestCase):
    def test_avg_and_tp_follow_fills(self):
        campaign = _campaign(capital=2000.0, mother_high=100.0)
        _leg(campaign, low=95.0, touch_high=98.0)
        campaign.all_fills = [
            Fill(price=90.0, quantity=1.0, level=2, leg_id=1, timestamp=1),
            Fill(price=80.0, quantity=1.0, level=4, leg_id=1, timestamp=2),
        ]
        avg = recompute_avg_entry_price(campaign)
        self.assertAlmostEqual(avg, 85.0)
        self.assertAlmostEqual(campaign.filled_base_qty, 2.0)
        # TP is measured FROM the average entry back toward the mother high:
        # tp = avg + 0.25 * (mother_high - avg) = 85 + 0.25*15 = 88.75
        self.assertAlmostEqual(compute_tp_price(campaign), 88.75)

    def test_no_tp_until_an_entry_actually_fills(self):
        """The target only exists once there is a position: it is measured from
        the real average entry, so there is nothing to show before the first fill."""
        campaign = _campaign(capital=2000.0, mother_high=100.0)
        _leg(campaign, low=92.0, touch_high=98.0)
        self.assertIsNone(compute_tp_price(campaign))
        campaign.all_fills = [Fill(price=92.0, quantity=1.0, level=2, leg_id=1, timestamp=1)]
        recompute_avg_entry_price(campaign)
        # 92 + 0.25 * (100 - 92) = 94
        self.assertAlmostEqual(compute_tp_price(campaign), 94.0)

    def test_tp_none_without_legs_or_fills(self):
        campaign = _campaign()
        self.assertIsNone(compute_tp_price(campaign))


class SerializationTests(unittest.TestCase):
    def test_campaign_roundtrip(self):
        campaign = _campaign(capital=1500.0, mother_high=100.0)
        campaign.trendlines.append(Trendline(1, 100.0, 0, 98.0, 600))
        campaign.active_trendline_id = 1
        leg = _leg(campaign, low=95.0, touch_high=98.0)
        build_fib_ladder_and_pool(campaign, leg)
        plan_leg_orders(campaign, leg)
        campaign.all_fills.append(Fill(price=91.0, quantity=0.5, level=2, leg_id=1, timestamp=900, order_id="77"))
        recompute_avg_entry_price(campaign)
        campaign.state = "TRENDLINE_ACTIVE"

        restored = Campaign.from_dict(campaign.to_dict())
        self.assertEqual(restored.campaign_id, campaign.campaign_id)
        self.assertEqual(restored.state, "TRENDLINE_ACTIVE")
        self.assertEqual(len(restored.trendlines), 1)
        self.assertEqual(len(restored.legs), 1)
        self.assertAlmostEqual(restored.legs[0].pool_usd, leg.pool_usd)
        self.assertEqual(set(restored.legs[0].pending_orders), {2, 4, 8})
        self.assertAlmostEqual(restored.legs[0].pending_orders[4].price, leg.pending_orders[4].price)
        self.assertAlmostEqual(restored.avg_entry_price, campaign.avg_entry_price)
        self.assertEqual(restored.all_fills[0].order_id, "77")


class ExchangeFillTimestampTests(unittest.TestCase):
    """A recovered fill must be dated when it filled, not when we noticed."""

    NOW = 1_785_500_000

    def test_prefers_update_time_in_milliseconds(self):
        row = {"updateTime": 1_785_470_000_000, "time": 1_785_460_000_000}
        self.assertEqual(exchange_fill_ts(row, now_ts=self.NOW), 1_785_470_000)

    def test_accepts_a_broker_that_reports_seconds(self):
        self.assertEqual(exchange_fill_ts({"updateTime": 1_785_470_000}, now_ts=self.NOW), 1_785_470_000)

    def test_falls_back_through_the_key_order(self):
        self.assertEqual(
            exchange_fill_ts({"transactTime": 1_785_470_000_000}, now_ts=self.NOW),
            1_785_470_000,
        )
        self.assertEqual(exchange_fill_ts({"time": 1_785_470_000_000}, now_ts=self.NOW), 1_785_470_000)

    def test_missing_or_empty_falls_back_to_now(self):
        self.assertEqual(exchange_fill_ts({}, now_ts=self.NOW), self.NOW)
        self.assertEqual(exchange_fill_ts({"updateTime": 0}, now_ts=self.NOW), self.NOW)
        self.assertEqual(exchange_fill_ts(None, now_ts=self.NOW), self.NOW)

    def test_nonsense_future_stamp_is_refused(self):
        row = {"updateTime": (self.NOW + 86_400) * 1000}
        self.assertEqual(exchange_fill_ts(row, now_ts=self.NOW), self.NOW)

    def test_small_clock_skew_is_accepted(self):
        row = {"updateTime": (self.NOW + 30) * 1000}
        self.assertEqual(exchange_fill_ts(row, now_ts=self.NOW), self.NOW + 30)

    def test_an_hours_old_fill_keeps_its_own_time(self):
        """The whole point: an outage must not drag the fill forward to now."""
        filled_at = self.NOW - 8 * 3600
        row = {"updateTime": filled_at * 1000, "status": "FILLED"}
        self.assertEqual(exchange_fill_ts(row, now_ts=self.NOW), filled_at)


class CascadeFeeAccountingTests(unittest.TestCase):
    """Commission on both sides of a round, and what it costs the ladder.

    Every other engine here models fees; this one did not, so every round,
    campaign and realised total on screen was gross while the backtest it was
    compared against was net (AUDIT §1.2).
    """

    def test_fee_is_charged_on_both_sides(self):
        # $100 in, $110 out, 0.1% each side.
        self.assertAlmostEqual(round_trip_fee(100.0, 110.0), 0.21, places=8)

    def test_a_zero_rate_costs_nothing(self):
        import engine.cascade as c

        original = c.FEE_PCT_PER_SIDE
        try:
            c.FEE_PCT_PER_SIDE = 0.0
            self.assertEqual(c.round_trip_fee(100.0, 110.0), 0.0)
        finally:
            c.FEE_PCT_PER_SIDE = original

    def test_negative_notionals_never_pay_a_negative_fee(self):
        """A fee that could go negative would be a credit — it cannot."""
        self.assertEqual(round_trip_fee(-100.0, -110.0), 0.0)
        self.assertAlmostEqual(round_trip_fee(-100.0, 110.0), 0.11, places=8)

    def test_a_round_written_before_fees_restores_as_its_own_gross(self):
        """`pnl` was the gross figure then, and stays it — never back-dated to
        today's rate, because a stored round is a record of what happened."""
        legacy = Round.from_dict(
            {
                "round_id": 1,
                "leg_id": 1,
                "avg_entry": 100.0,
                "quantity": 1.0,
                "invested_usd": 100.0,
                "exit_price": 110.0,
                "pnl": 10.0,
            }
        )
        self.assertEqual(legacy.pnl, 10.0)
        self.assertEqual(legacy.pnl_gross, 10.0)
        self.assertEqual(legacy.fees_usd, 0.0)

    def test_a_round_written_after_fees_round_trips(self):
        rnd = Round(
            round_id=1,
            leg_id=1,
            avg_entry=100.0,
            quantity=1.0,
            invested_usd=100.0,
            exit_price=110.0,
            pnl=9.79,
            fees_usd=0.21,
            pnl_gross=10.0,
        )
        restored = Round.from_dict(rnd.to_dict())
        self.assertAlmostEqual(restored.pnl, 9.79)
        self.assertAlmostEqual(restored.pnl_gross, 10.0)
        self.assertAlmostEqual(restored.fees_usd, 0.21)

    def test_campaign_totals_are_net_and_fees_are_visible(self):
        campaign = _campaign()
        campaign.rounds = [
            Round(
                round_id=i,
                leg_id=1,
                avg_entry=100.0,
                quantity=1.0,
                invested_usd=100.0,
                exit_price=110.0,
                pnl=9.79,
                fees_usd=0.21,
                pnl_gross=10.0,
            )
            for i in (1, 2)
        ]
        self.assertAlmostEqual(campaign.realized_pnl_total, 19.58)
        self.assertAlmostEqual(campaign.fees_total, 0.42)

    def test_a_target_only_clears_its_fees_past_a_known_fall(self):
        """The finding this work surfaced, pinned so it cannot drift silently.

        TP sits at TP_FIB_LEVEL of the way back from the average entry to the
        mother high, so the gross gain is `0.25 x fall`. The round-trip fee is
        about `2 x rate` of the same notional. They cross at a fall of
        `8 x rate` — 0.80% at 0.1% a side. A round whose average entry is
        nearer the mother high than that closes AT TARGET and still loses
        money. Nothing in the engine prevents it; sizing does.
        """
        rate = FEE_PCT_PER_SIDE / 100.0
        breakeven_fall = 2 * rate / TP_FIB_LEVEL
        mother_high = 100.0

        shallow_entry = mother_high * (1 - breakeven_fall / 2)
        deep_entry = mother_high * (1 - breakeven_fall * 2)

        for entry, expect_profit in ((shallow_entry, False), (deep_entry, True)):
            qty = 100.0 / entry
            tp = entry + TP_FIB_LEVEL * (mother_high - entry)
            gross = (tp - entry) * qty
            net = gross - round_trip_fee(entry * qty, tp * qty)
            self.assertGreater(gross, 0.0, "the target is always above the entry")
            self.assertEqual(net > 0, expect_profit, f"entry {entry:.4f}, net {net:.6f}")

    def test_the_breakeven_fall_is_the_documented_number(self):
        self.assertAlmostEqual(2 * (FEE_PCT_PER_SIDE / 100.0) / TP_FIB_LEVEL * 100, 0.8, places=6)


if __name__ == "__main__":
    unittest.main()
