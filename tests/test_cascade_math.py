"""Unit tests for the pure cascade model in engine/cascade.py."""

import unittest

from engine.cascade import (
    Campaign,
    Candle,
    FibLadder,
    Fill,
    Leg,
    Trendline,
    build_fib_ladder_and_pool,
    cancel_and_carry_forward,
    compute_tp_price,
    leg_broken,
    plan_leg_orders,
    recompute_avg_entry_price,
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
        self.assertEqual(timeframe_for_level(deep, 2), "5m")
        self.assertEqual(timeframe_for_level(deep, 4), "15m")
        self.assertEqual(timeframe_for_level(shallow, 4), "5m")


class PlanLegOrdersTests(unittest.TestCase):
    def test_users_example_l2_merges_into_l4(self):
        """$2000 capital, 0.5% dip: $2/$3/$5 -> L4 gets $5 (L2 merged), L8 keeps $5."""
        campaign = _campaign(capital=2000.0, mother_high=100.0, min_notional=5.0)
        leg = _leg(campaign, low=99.5, touch_high=99.8)
        build_fib_ladder_and_pool(campaign, leg)
        self.assertAlmostEqual(leg.pool_usd, 10.0)
        plan_leg_orders(campaign, leg)

        self.assertEqual(leg.pending_orders[2].status, "MERGED")
        self.assertAlmostEqual(leg.pending_orders[4].usd_notional, 5.0)
        self.assertEqual(leg.pending_orders[4].status, "PENDING")
        self.assertAlmostEqual(leg.pending_orders[8].usd_notional, 5.0)
        self.assertEqual(leg.pending_orders[8].status, "PENDING")
        self.assertAlmostEqual(campaign.carry_forward_usd, 0.0)

    def test_all_levels_meet_minimum_on_deep_pool(self):
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
            self.assertAlmostEqual(order.quantity, order.usd_notional / order.price)

    def test_whole_pool_below_minimum_carries_forward(self):
        campaign = _campaign(capital=2000.0, mother_high=100.0, min_notional=5.0)
        leg = _leg(campaign, low=99.85, touch_high=99.95)  # 0.15% depth => $3 pool
        build_fib_ladder_and_pool(campaign, leg)
        plan_leg_orders(campaign, leg)
        self.assertAlmostEqual(campaign.carry_forward_usd, 3.0)
        for level in (2, 4, 8):
            self.assertEqual(leg.pending_orders[level].usd_notional, 0.0)
            self.assertNotEqual(leg.pending_orders[level].status, "PENDING")
        self.assertEqual(leg.pending_orders[8].status, "CARRIED")

        # The carried pool joins the next leg's pool.
        leg2 = _leg(campaign, low=99.0, touch_high=99.5, leg_id=2)
        build_fib_ladder_and_pool(campaign, leg2)
        # funded off fib 1's level 1 (99.85), not the mother high
        self.assertAlmostEqual(leg2.pool_usd, (99.85 - 99.0) / 99.85 * 100 * 20.0, places=6)
        plan_leg_orders(campaign, leg2)
        total = sum(o.usd_notional for o in leg2.pending_orders.values())
        # per-level notionals round to cents, so allow a sub-cent difference
        self.assertAlmostEqual(total, leg2.pool_usd + 3.0, places=1)
        self.assertAlmostEqual(campaign.carry_forward_usd, 0.0)

    def test_untouched_fib1_pool_carries_into_fib2_in_full(self):
        campaign = _campaign(capital=2000.0, mother_high=100.0, min_notional=5.0)
        leg1 = _leg(campaign, low=95.0, touch_high=98.0)
        build_fib_ladder_and_pool(campaign, leg1)
        plan_leg_orders(campaign, leg1)
        pool1 = leg1.pool_total_usd
        self.assertGreater(pool1, 0.0)

        # Nothing filled on fib 1 — the previous low breaks and fib 2 opens.
        leg2 = _leg(campaign, low=92.0, touch_high=96.0, leg_id=2)
        carried = cancel_and_carry_forward(campaign, leg1)
        self.assertEqual(leg1.pending_orders[8].status, "CARRIED")
        self.assertAlmostEqual(carried, pool1)

        build_fib_ladder_and_pool(campaign, leg2)
        plan_leg_orders(campaign, leg2)
        self.assertAlmostEqual(leg2.carry_in_usd, pool1)
        self.assertAlmostEqual(leg2.pool_total_usd, leg2.pool_usd + pool1)
        # The lump is re-split 20/30/50 with fib 2's own allocation.
        self.assertAlmostEqual(leg2.pending_orders[8].usd_notional, round(leg2.pool_total_usd * 0.50, 2), places=2)

    def test_level_still_held_does_not_carry_but_a_closed_round_does(self):
        campaign = _campaign(capital=2000.0, mother_high=100.0, min_notional=5.0)
        leg1 = _leg(campaign, low=95.0, touch_high=98.0)
        build_fib_ladder_and_pool(campaign, leg1)
        plan_leg_orders(campaign, leg1)
        pool1 = leg1.pool_total_usd

        # L4 fills and is STILL HELD — that notional stays out of the carry.
        held = leg1.pending_orders[4]
        campaign.all_fills.append(Fill(price=held.price, quantity=held.quantity, level=4, leg_id=1, timestamp=1))
        held_usd = held.price * held.quantity
        leg2 = _leg(campaign, low=92.0, touch_high=96.0, leg_id=2)
        self.assertAlmostEqual(cancel_and_carry_forward(campaign, leg1), pool1 - held_usd, places=6)

        # Now the round closes at TP: principal returns, so the rest carries too.
        campaign.all_fills = []
        campaign.carry_forward_usd = 0.0
        leg3 = _leg(campaign, low=90.0, touch_high=94.0, leg_id=3)
        self.assertAlmostEqual(cancel_and_carry_forward(campaign, leg1), pool1, places=6)
        self.assertIsNotNone(leg2)
        self.assertIsNotNone(leg3)

    def test_capital_cap_trims_deepest_level_first(self):
        campaign = _campaign(capital=100.0, mother_high=100.0, min_notional=5.0)
        campaign.all_fills.append(Fill(price=50.0, quantity=1.6, level=2, leg_id=1, timestamp=1))  # $80 spent
        leg = _leg(campaign, low=95.0, touch_high=98.0)
        build_fib_ladder_and_pool(campaign, leg)
        leg.pool_usd = 60.0  # force a pool larger than remaining capital ($20)
        plan_leg_orders(campaign, leg)
        total = sum(o.usd_notional for o in leg.pending_orders.values())
        self.assertLessEqual(total, 20.0 + 1e-9)
        # deepest level trimmed first: L8 (30) fully trimmed, then L4 partially
        self.assertEqual(leg.pending_orders[8].usd_notional, 0.0)


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


if __name__ == "__main__":
    unittest.main()
