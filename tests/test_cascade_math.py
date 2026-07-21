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
    compute_tp_price,
    leg_broken,
    plan_leg_orders,
    recompute_avg_entry_price,
    replan_ladder,
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
    def test_a_short_pool_funds_the_shallowest_rungs_first(self):
        """$2000 capital, 0.5% dip: a $10 pool against a $5.50 rung. That is one
        full rung and change, so the rung nearest the market takes it and the
        deeper two get nothing — a part-rung cannot be placed at all."""
        campaign = _campaign(capital=2000.0, mother_high=100.0, min_notional=5.0)
        leg = _leg(campaign, low=99.5, touch_high=99.8)
        build_fib_ladder_and_pool(campaign, leg)
        self.assertAlmostEqual(leg.pool_usd, 10.0)
        plan_leg_orders(campaign, leg)

        self.assertEqual(leg.pending_orders[2].status, "PENDING")
        self.assertAlmostEqual(leg.pending_orders[2].usd_notional, 10.0)  # rung + all the surplus
        self.assertEqual(leg.pending_orders[4].status, "UNFUNDED")
        self.assertEqual(leg.pending_orders[8].status, "UNFUNDED")

    def test_every_funded_rung_clears_the_exchange_minimum(self):
        """No rung is ever left holding an amount Binance would reject."""
        campaign = _campaign(capital=2000.0, mother_high=100.0, min_notional=5.0)
        leg = _leg(campaign, low=99.6, touch_high=99.95)
        build_fib_ladder_and_pool(campaign, leg)
        plan_leg_orders(campaign, leg)
        for order in leg.pending_orders.values():
            if order.usd_notional > 0:
                self.assertGreaterEqual(order.usd_notional, 5.5)

    def test_all_levels_meet_minimum_on_deep_pool(self):
        campaign = _campaign(capital=2000.0, mother_high=100.0, min_notional=5.0)
        leg = _leg(campaign, low=95.0, touch_high=98.0)
        build_fib_ladder_and_pool(campaign, leg)  # $100 pool
        plan_leg_orders(campaign, leg)
        # Every rung clears the minimum on its own, so the split is exactly
        # 20/30/50 with no top-ups.
        self.assertAlmostEqual(leg.pending_orders[2].usd_notional, 20.0)
        self.assertAlmostEqual(leg.pending_orders[4].usd_notional, 30.0)
        self.assertAlmostEqual(leg.pending_orders[8].usd_notional, 50.0)
        for level in (2, 4, 8):
            order = leg.pending_orders[level]
            self.assertEqual(order.status, "PENDING")
            self.assertAlmostEqual(order.price, leg.fib.level_price(level))
            self.assertAlmostEqual(order.quantity, order.usd_notional / order.price)

    def test_a_pool_under_one_rung_places_nothing_at_all(self):
        campaign = _campaign(capital=2000.0, mother_high=100.0, min_notional=5.0)
        leg = _leg(campaign, low=99.85, touch_high=99.95)  # 0.15% depth => $3 pool
        build_fib_ladder_and_pool(campaign, leg)
        plan_leg_orders(campaign, leg)
        for level in (2, 4, 8):
            self.assertEqual(leg.pending_orders[level].usd_notional, 0.0)
            self.assertEqual(leg.pending_orders[level].status, "UNFUNDED")

        # The money is not lost — it is still in the pool, and the next fib's
        # rungs join the same ladder, so together they can now place.
        leg2 = _leg(campaign, low=99.0, touch_high=99.5, leg_id=2)
        build_fib_ladder_and_pool(campaign, leg2)
        plan_leg_orders(campaign, leg2)
        funded = [o for lg in campaign.legs for o in lg.pending_orders.values() if o.usd_notional > 0]
        self.assertTrue(funded, "the two pools together clear a rung")
        self.assertAlmostEqual(sum(o.usd_notional for o in funded), campaign.total_allocation_usd, places=1)

    def test_the_ladder_is_ordered_by_price_across_every_fib(self):
        """Fibs overlap, so the rungs interleave. Funding follows price, not
        which fib a level happened to belong to."""
        campaign = _campaign(capital=2000.0, mother_high=100.0, min_notional=5.0)
        leg1 = _leg(campaign, low=95.0, touch_high=98.0)
        build_fib_ladder_and_pool(campaign, leg1)
        plan_leg_orders(campaign, leg1)
        leg2 = _leg(campaign, low=94.0, touch_high=97.0, leg_id=2)
        build_fib_ladder_and_pool(campaign, leg2)
        plan_leg_orders(campaign, leg2)

        rungs = sorted(
            (o for lg in campaign.legs for o in lg.pending_orders.values() if o.usd_notional > 0),
            key=lambda o: -o.price,
        )
        # At least one of fib 2's rungs sits above one of fib 1's, which is the
        # interleaving the old per-fib pools could not see.
        self.assertTrue(any(o.leg_id == 2 for o in rungs[: len(rungs) // 2]))
        self.assertAlmostEqual(sum(o.usd_notional for o in rungs), campaign.total_allocation_usd, places=1)

    def test_money_in_an_open_position_is_not_re_offered_on_the_ladder(self):
        """A rung that filled is spent. Its money is in the position, not on the
        ladder, so what is left to place is the allocation minus the fill."""
        campaign = _campaign(capital=2000.0, mother_high=100.0, min_notional=5.0)
        leg1 = _leg(campaign, low=95.0, touch_high=98.0)
        build_fib_ladder_and_pool(campaign, leg1)
        plan_leg_orders(campaign, leg1)
        allocation = campaign.total_allocation_usd

        held = leg1.pending_orders[4]
        held_usd = held.price * held.quantity
        campaign.all_fills.append(Fill(price=held.price, quantity=held.quantity, level=4, leg_id=1, timestamp=1))
        held.status = "FILLED"
        replan_ladder(campaign)

        resting = sum(o.usd_notional for o in leg1.pending_orders.values() if o.is_open)
        self.assertAlmostEqual(resting, allocation - held_usd, places=1)

        # The target hits: principal returns and goes straight back on the ladder.
        campaign.all_fills = []
        held.status = "CLOSED"
        replan_ladder(campaign)
        reoffered = sum(o.usd_notional for o in leg1.pending_orders.values() if o.is_open)
        self.assertAlmostEqual(reoffered, allocation, places=1)
        # ...but never back onto the rung that already bought — it keeps the
        # amount it spent as history and is no longer offerable.
        self.assertFalse(held.is_open)
        self.assertNotIn(held, [o for o in leg1.pending_orders.values() if o.is_open])

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
