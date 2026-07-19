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
    classify_candle,
    compute_tp_price,
    find_valid_anchor2,
    leg_broken,
    plan_leg_orders,
    recompute_avg_entry_price,
    reprice_leg_orders,
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


class TrendlineClassificationTests(unittest.TestCase):
    def setUp(self):
        # Mother high 105 at t=0, anchor2 open 103 at t=2 (matches the user's scenario file).
        self.tl = Trendline(1, 105.0, 0, 103.0, 2)

    def test_line_price_interpolates(self):
        self.assertAlmostEqual(trendline_price(self.tl, 0), 105.0)
        self.assertAlmostEqual(trendline_price(self.tl, 2), 103.0)
        self.assertAlmostEqual(trendline_price(self.tl, 4), 101.0)

    def test_touch_high_crosses_close_below(self):
        candle = Candle(4, 100, 102, 99.8, 100.8)  # line at t=4 is 101
        self.assertEqual(classify_candle(105.0, self.tl, candle), "TOUCH")

    def test_break_close_above_line(self):
        candle = Candle(5, 100.8, 102.5, 100.5, 102.2)  # line at t=5 is 100
        self.assertEqual(classify_candle(105.0, self.tl, candle), "BREAK")

    def test_none_when_high_stays_below_line(self):
        candle = Candle(3, 101.8, 101.9, 99.5, 100.0)  # line at t=3 is 102
        self.assertEqual(classify_candle(105.0, self.tl, candle), "NONE")

    def test_high_reaching_mother_high_is_not_a_touch(self):
        tl = Trendline(1, 105.0, 0, 104.9, 100)
        candle = Candle(100, 104.0, 105.0, 103.0, 104.5)
        self.assertNotEqual(classify_candle(105.0, tl, candle), "TOUCH")

    def test_leg_broken_requires_red_close_below_low(self):
        self.assertTrue(leg_broken(Candle(7, 102.0, 102.2, 98, 98.3), 99.5))
        self.assertFalse(leg_broken(Candle(7, 98.0, 102.2, 97.9, 98.3), 99.5))  # green candle
        self.assertFalse(leg_broken(Candle(7, 102.0, 102.2, 98, 100.0), 99.5))  # close above low


class FindValidAnchor2Tests(unittest.TestCase):
    def test_picks_red_candidate_closest_to_depth(self):
        candles = [
            Candle(1, 104, 104.5, 102, 102.5),
            Candle(2, 103, 103.2, 101.5, 101.8),
            Candle(3, 101.8, 102, 99.5, 100),
        ]
        price, ts = find_valid_anchor2(105.0, 0, candles)
        self.assertEqual((price, ts), (101.8, 3))

    def test_rejects_candidate_whose_line_is_crossed(self):
        candles = [
            Candle(1, 104.5, 104.6, 102, 104.2),  # red; close 104.2 crosses shallow lines
            Candle(2, 100.5, 101, 99.5, 100.0),  # red; deepest candidate
        ]
        price, ts = find_valid_anchor2(105.0, 0, candles)
        # Candidate at t=2 (open 100.5) is violated by t=1 close 104.2 above its line
        # (line at t=1 = 105 + (100.5-105)/2 = 102.75) -> falls back to t=1 candidate.
        self.assertEqual((price, ts), (104.5, 1))

    def test_returns_none_without_red_candles(self):
        candles = [Candle(1, 100, 104, 99, 104)]
        self.assertEqual(find_valid_anchor2(105.0, 0, candles), (None, None))


class FibLadderPoolTests(unittest.TestCase):
    def test_level_prices(self):
        fib = FibLadder(high_anchor=102.0, low_anchor=99.5)
        self.assertAlmostEqual(fib.level_price(0), 102.0)
        self.assertAlmostEqual(fib.level_price(1), 99.5)
        self.assertAlmostEqual(fib.level_price(2), 97.0)
        self.assertAlmostEqual(fib.level_price(4), 92.0)
        self.assertAlmostEqual(fib.level_price(8), 82.0)

    def test_pool_uses_incremental_depth_pct(self):
        campaign = _campaign(capital=2000.0, mother_high=100.0)
        leg1 = _leg(campaign, low=95.0, touch_high=97.0)
        build_fib_ladder_and_pool(campaign, leg1)
        # 5% depth => pool 5 * (2000/100) = $100
        self.assertAlmostEqual(leg1.pool_usd, 100.0)
        self.assertAlmostEqual(campaign.cumulative_used_pct, 5.0)

        leg2 = _leg(campaign, low=92.0, touch_high=95.0, leg_id=2)
        build_fib_ladder_and_pool(campaign, leg2)
        # 8% total depth, 5% already used => incremental 3% => $60
        self.assertAlmostEqual(leg2.pool_usd, 60.0)
        self.assertAlmostEqual(campaign.cumulative_used_pct, 8.0)

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
        self.assertAlmostEqual(leg2.pool_usd, (1.0 - 0.15) * 20.0)
        plan_leg_orders(campaign, leg2)
        total = sum(o.usd_notional for o in leg2.pending_orders.values())
        self.assertAlmostEqual(total, leg2.pool_usd + 3.0, places=6)
        self.assertAlmostEqual(campaign.carry_forward_usd, 0.0)

    def test_carry_forward_quantity_is_valued_at_new_level_price(self):
        campaign = _campaign(capital=2000.0, mother_high=100.0, min_notional=5.0)
        leg1 = _leg(campaign, low=95.0, touch_high=98.0)
        build_fib_ladder_and_pool(campaign, leg1)
        plan_leg_orders(campaign, leg1)
        # L8 never filled; leg 2 begins.
        leg2 = _leg(campaign, low=92.0, touch_high=96.0, leg_id=2)
        cancel_and_carry_forward(leg1, leg2)
        self.assertEqual(leg1.pending_orders[8].status, "CARRIED")
        self.assertAlmostEqual(leg2.carry_forward_qty[8], leg1.pending_orders[8].quantity)

        build_fib_ladder_and_pool(campaign, leg2)
        plan_leg_orders(campaign, leg2)
        base_l8 = (leg2.pool_usd) * 0.50
        carried_value = leg2.carry_forward_qty[8] * leg2.fib.level_price(8)
        self.assertAlmostEqual(leg2.pending_orders[8].usd_notional, round(base_l8 + carried_value, 2), places=2)

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

    def test_reprice_moves_open_orders_and_bumps_rev(self):
        campaign = _campaign(capital=2000.0, mother_high=100.0, min_notional=5.0)
        leg = _leg(campaign, low=95.0, touch_high=97.0)
        build_fib_ladder_and_pool(campaign, leg)
        plan_leg_orders(campaign, leg)
        leg.pending_orders[2].status = "FILLED"
        old_l4_price = leg.pending_orders[4].price
        leg.touch_high = 98.0
        self.assertTrue(reprice_leg_orders(campaign, leg))
        self.assertLess(leg.pending_orders[4].price, old_l4_price)  # deeper after higher high
        self.assertEqual(leg.pending_orders[4].rev, 1)
        self.assertIn("-4-1", leg.pending_orders[4].client_order_id)
        # Filled order untouched
        self.assertEqual(leg.pending_orders[2].status, "FILLED")


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
        # TP = mother_high - 0.25 * (mother_high - avg) = 100 - 0.25*15 = 96.25
        self.assertAlmostEqual(compute_tp_price(campaign), 96.25)

    def test_tp_display_estimate_before_fills_uses_leg1_low(self):
        campaign = _campaign(capital=2000.0, mother_high=100.0)
        _leg(campaign, low=92.0, touch_high=98.0)
        # 100 - 0.25 * (100 - 92) = 98
        self.assertAlmostEqual(compute_tp_price(campaign), 98.0)

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
