"""CascadeEngine tests: paper-mode state machine + live desired-state order sync."""

import time
import unittest

import pandas as pd

from engine.cascade import (
    Campaign,
    Candle,
    CascadeEngine,
    Leg,
    build_fib_ladder_and_pool,
    plan_leg_orders,
    trendline_price,
)

_RECENT_TS = (int(time.time()) - 3600) // 300 * 300  # a truthy, in-window mother timestamp


class FakeCascadeBroker:
    display_name = "Fake Broker"

    def __init__(self):
        self.placed_orders = []
        self.cancelled = []
        self.open_orders = []  # raw binance-style rows returned by get_orders
        self.order_lookup = {}  # order_id -> raw status row for get_order
        self._next_order_id = 1000
        self.configured = True
        self.candles_df = None  # optional DataFrame returned by async_get_candles

    def _is_configured(self):
        return self.configured

    def get_product_by_symbol(self, symbol):
        return {"symbol": symbol, "broker_symbol": symbol, "min_notional": "5.0", "tick_size": "0.01"}

    def get_ticker(self, symbol):
        return {"symbol": symbol, "last_price": 0.0, "mark_price": 0.0}

    async def async_get_candles(self, symbol, **kwargs):
        return self.candles_df

    def place_order(self, product_id, size, side, order_type="market_order", limit_price=None, **kwargs):
        self._next_order_id += 1
        record = {
            "symbol": product_id,
            "size": size,
            "side": side,
            "order_type": order_type,
            "limit_price": limit_price,
            **kwargs,
        }
        self.placed_orders.append(record)
        return {"orderId": self._next_order_id, "id": self._next_order_id, "status": "NEW"}

    def cancel_order(self, order_id, product_id=""):
        self.cancelled.append(str(order_id))
        return {"status": "CANCELED"}

    def get_orders(self, product_id=None, state="open"):
        return list(self.open_orders)

    def get_order(self, product_id, order_id=None, client_order_id=None):
        return self.order_lookup.get(str(order_id), {})


def _mk_engine(broker=None):
    return CascadeEngine(broker or FakeCascadeBroker())


def _mk_campaign(engine, mode="paper", capital=2000.0):
    campaign = Campaign(
        campaign_id="camp1",
        symbol="BTCUSDT",
        capital_usd=capital,
        mother_high=105.0,
        mother_low=99.0,
        mother_timestamp=0,
        mode=mode,
        min_notional_usd=5.0,
    )
    engine.campaigns[campaign.campaign_id] = campaign
    return campaign


def _feed(engine, campaign, candle):
    engine._candles_5m.setdefault(campaign.campaign_id, []).append(candle)
    engine._process_candle(campaign, candle)


# Candles from the user's scenario_two_trendlines.py, timestamps scaled to 5m (300s) steps.
def _scenario_candles():
    raw = [
        (1, 104, 104.5, 102, 102.5),
        (2, 103, 103.2, 101.5, 101.8),  # trendline1 anchor2 (open 103)
        (3, 101.8, 102, 99.5, 100),  # leg1 depth: low 99.5
        (4, 100, 102, 99.8, 100.8),  # LEG 1 TOUCH
        (5, 100.8, 102.5, 100.5, 102.2),  # BREAK of trendline1
        (6, 102.5, 103, 101, 102.0),  # trendline2 anchor2 candidate
        (7, 102.0, 102.2, 98, 98.3),  # decisive low-break -> trendline2 created
        (8, 98.3, 98.5, 96.5, 96.8),
        (9, 96.8, 97, 95, 95.3),
        (10, 95.3, 96, 94.5, 95.7),  # deepest low 94.5
        (11, 95.7, 97, 95.5, 96.8),
        (12, 96.8, 99, 96.5, 98.7),  # still below trendline2
        (13, 98.7, 101, 98.5, 99.4),  # LEG 2 TOUCH begins
        (14, 99.4, 102, 99, 101.5),
        (15, 101.5, 103.5, 101, 103),
        (16, 103, 104, 102.5, 102.8),  # swing peak 104 (below mother 105)
        (17, 102.8, 103, 100, 100.3),
        (18, 100.3, 100.5, 93, 93.5),  # low-break -> fib2 finalized
    ]
    return [Candle(step * 300, o, h, low, c) for step, o, h, low, c in raw]


class CascadeScenarioTests(unittest.TestCase):
    """End-to-end replay of the user's two-trendline scenario in paper mode."""

    def setUp(self):
        self.engine = _mk_engine()
        self.campaign = _mk_campaign(self.engine)

    def _run_scenario(self, upto=18):
        for candle in _scenario_candles()[:upto]:
            _feed(self.engine, self.campaign, candle)

    def test_first_trendline_forms_after_two_red_candles(self):
        """RED_CANDLES_TO_CONFIRM red candles confirm the first depth (the rule
        from cascade_lib), not a fixed fall percentage — a shallow first leg is
        still a valid leg and a % threshold draws the line too late to catch it."""
        self._run_scenario(upto=2)  # t=1 and t=2 are both red
        self.assertEqual(self.campaign.state, "TRENDLINE_ACTIVE")
        self.assertEqual(len(self.campaign.trendlines), 1)
        tl = self.campaign.trendlines[0]
        self.assertEqual(tl.anchor1_price, 105.0)
        self.assertEqual(tl.anchor2_price, 104.0)  # red open at t=1
        self.assertEqual(tl.anchor2_timestamp, 300)
        # Same line the scenario file draws through candles[2] (open 103 @ 600):
        # both anchors sit on slope -1/300 from the mother high.
        self.assertAlmostEqual(trendline_price(tl, 600), 103.0)

    def test_leg1_touch_builds_fib_and_orders(self):
        self._run_scenario(upto=4)
        self.assertEqual(len(self.campaign.legs), 1)
        leg = self.campaign.legs[0]
        self.assertEqual(leg.low, 99.5)
        self.assertEqual(leg.touch_high, 102.0)
        self.assertTrue(self.campaign.swing_tracking)
        # 5.238% depth on $2000 => ~$104.76 pool split 20/30/50
        self.assertAlmostEqual(leg.pool_usd, (105 - 99.5) / 105 * 100 * 20, places=4)
        self.assertAlmostEqual(leg.pending_orders[2].price, 97.0)
        self.assertAlmostEqual(leg.pending_orders[4].price, 92.0)
        self.assertAlmostEqual(leg.pending_orders[8].price, 82.0)
        for level in (2, 4, 8):
            self.assertEqual(leg.pending_orders[level].status, "PENDING")

    def test_swing_high_rises_and_ladder_reprices(self):
        self._run_scenario(upto=6)
        leg = self.campaign.legs[0]
        self.assertEqual(leg.touch_high, 103.0)  # running max over t=4..6
        self.assertTrue(self.campaign.pending_break)  # break at t=5
        self.assertAlmostEqual(leg.pending_orders[2].price, 103 - 2 * 3.5)
        self.assertGreaterEqual(leg.pending_orders[2].rev, 1)

    def test_swing_low_deepens_and_reprices_ladder(self):
        """A new low during the swing (no decisive red close below the leg low)
        must pull fib 1 down with it, so the fib spans the full swing. Verified
        against the user's TradingView reading of 2026-07-19 BTCUSDT."""
        self._run_scenario(upto=4)  # leg 1 created, low = 99.5
        leg = self.campaign.legs[0]
        self.assertEqual(leg.low, 99.5)
        old_l4_price = leg.pending_orders[4].price

        # Green candle that wicks below the leg low but closes above it:
        # not a decisive break, so the swing low must deepen instead.
        _feed(self.engine, self.campaign, Candle(5 * 300, 100.0, 101.0, 98.0, 100.5))

        self.assertEqual(leg.low, 98.0)
        self.assertTrue(self.campaign.swing_tracking)
        self.assertFalse(leg.finalized)
        # fib rebuilt on the deeper low, so levels sit lower than before
        self.assertAlmostEqual(leg.fib.low_anchor, 98.0)
        self.assertAlmostEqual(leg.pending_orders[4].price, leg.fib.level_price(4))
        self.assertLess(leg.pending_orders[4].price, old_l4_price)

    def test_red_close_below_leg_low_still_finalizes_instead_of_deepening(self):
        """A decisive low-break must end the swing, not deepen it."""
        self._run_scenario(upto=4)
        leg = self.campaign.legs[0]
        _feed(self.engine, self.campaign, Candle(5 * 300, 100.0, 100.2, 98.0, 98.5))  # red, closes below 99.5
        self.assertTrue(leg.finalized)
        self.assertFalse(self.campaign.swing_tracking)
        self.assertEqual(leg.low, 99.5)  # unchanged — swing ended here

    def test_deepening_respects_capital_cap(self):
        self._run_scenario(upto=4)
        leg = self.campaign.legs[0]
        self.campaign.capital_usd = 1.0  # no headroom left
        before = sum(o.usd_notional for o in leg.pending_orders.values() if o.is_open)
        _feed(self.engine, self.campaign, Candle(5 * 300, 100.0, 101.0, 97.0, 100.5))
        after = sum(o.usd_notional for o in leg.pending_orders.values() if o.is_open)
        self.assertEqual(leg.low, 97.0)  # anchor still moves
        self.assertAlmostEqual(after, before)  # but no extra capital committed

    def test_low_break_finalizes_leg_and_creates_trendline2(self):
        self._run_scenario(upto=7)
        leg = self.campaign.legs[0]
        self.assertTrue(leg.finalized)
        self.assertFalse(self.campaign.swing_tracking)
        self.assertEqual(len(self.campaign.trendlines), 2)
        tl2 = self.campaign.trendlines[1]
        self.assertEqual(tl2.anchor2_price, 102.5)  # red open at t=6
        self.assertEqual(self.campaign.active_trendline_id, 2)
        self.assertEqual(len(self.campaign.legs), 1)  # no leg for trendline2 yet

    def test_paper_fill_happens_when_price_reaches_level(self):
        self._run_scenario(upto=9)
        leg = self.campaign.legs[0]
        # After the t=6 reprice, L2 sits at 96.0; candle t=9 (low 95) crosses it.
        self.assertEqual(leg.pending_orders[2].status, "FILLED")
        self.assertAlmostEqual(leg.pending_orders[2].fill_price, 96.0)
        self.assertEqual(len(self.campaign.all_fills), 1)
        self.assertAlmostEqual(self.campaign.avg_entry_price, 96.0)
        # TP = 105 - 0.25 * (105 - 96) = 102.75
        self.assertAlmostEqual(self.campaign.tp_price, 102.75)

    def test_leg2_touch_carries_forward_unfilled_levels(self):
        self._run_scenario(upto=13)
        self.assertEqual(len(self.campaign.legs), 2)
        leg1, leg2 = self.campaign.legs
        self.assertEqual(leg2.low, 94.5)
        self.assertEqual(leg2.trendline_id, 2)
        # L4/L8 of leg1 were never filled -> carried into leg2
        self.assertEqual(leg1.pending_orders[4].status, "CARRIED")
        self.assertEqual(leg1.pending_orders[8].status, "CARRIED")
        self.assertGreater(leg2.carry_forward_qty[4], 0)
        self.assertGreater(leg2.carry_forward_qty[8], 0)
        # L2 was filled and must NOT be carried
        self.assertNotIn(2, leg2.carry_forward_qty)

    def test_leg2_swing_finalizes_at_104(self):
        self._run_scenario(upto=18)
        leg2 = self.campaign.legs[1]
        self.assertTrue(leg2.finalized)
        self.assertEqual(leg2.touch_high, 104.0)
        # tl2 was closed above during leg2's swing, and t=18's low-break
        # therefore spawns trendline 3 (anchor2 = red open at t=17).
        self.assertEqual(len(self.campaign.trendlines), 3)
        self.assertEqual(self.campaign.active_trendline_id, 3)
        self.assertEqual(self.campaign.trendlines[2].anchor2_price, 102.8)

    def test_mother_break_ends_campaign(self):
        self._run_scenario(upto=3)  # no legs/fills yet
        _feed(self.engine, self.campaign, Candle(19 * 300, 103, 105.5, 102, 105.2))
        self.assertEqual(self.campaign.state, "MOTHER_BROKEN")
        self.assertTrue(self.campaign.mother_broken_above)

    def test_paper_mother_break_with_fills_completes_at_tp(self):
        self._run_scenario(upto=9)  # L2 filled at 96
        _feed(self.engine, self.campaign, Candle(19 * 300, 103, 105.5, 102, 105.2))
        # Price above mother high implies TP touched: campaign completes at TP.
        self.assertEqual(self.campaign.state, "COMPLETED")
        self.assertAlmostEqual(self.campaign.realized_pnl, (102.75 - 96.0) * self.campaign.filled_base_qty, places=6)


class CascadeLiveSyncTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.broker = FakeCascadeBroker()
        self.engine = _mk_engine(self.broker)
        self.campaign = _mk_campaign(self.engine, mode="live")
        self.campaign.state = "TRENDLINE_ACTIVE"
        leg = Leg(leg_id=1, trendline_id=1, low=99.5, touch_high=102.0, touch_timestamp=1200)
        self.campaign.legs.append(leg)
        build_fib_ladder_and_pool(self.campaign, leg)
        plan_leg_orders(self.campaign, leg)
        self.leg = leg

    async def test_pending_orders_are_placed_with_client_ids(self):
        changed = await self.engine._sync_live_orders(self.campaign)
        self.assertTrue(changed)
        buys = [o for o in self.broker.placed_orders if o["side"] == "buy"]
        self.assertEqual(len(buys), 3)
        for level, order in self.leg.pending_orders.items():
            self.assertEqual(order.status, "PLACED")
            self.assertIsNotNone(order.order_id)
            self.assertIn(f"-1-{level}-0", order.client_order_id)
        client_ids = {o["client_order_id"] for o in buys}
        self.assertEqual(len(client_ids), 3)

    async def test_exchange_fill_records_and_places_tp(self):
        await self.engine._sync_live_orders(self.campaign)
        filled = self.leg.pending_orders[2]
        # Order disappears from open orders and reports FILLED.
        self.broker.order_lookup[str(filled.order_id)] = {
            "status": "FILLED",
            "executedQty": str(filled.quantity),
            "cummulativeQuoteQty": str(filled.quantity * 97.0),
        }
        self.broker.placed_orders.clear()
        await self.engine._sync_live_orders(self.campaign)

        self.assertEqual(filled.status, "FILLED")
        self.assertEqual(len(self.campaign.all_fills), 1)
        self.assertAlmostEqual(self.campaign.avg_entry_price, 97.0)
        sells = [o for o in self.broker.placed_orders if o["side"] == "sell"]
        self.assertEqual(len(sells), 1)
        # TP = 105 - 0.25*(105-97) = 103
        self.assertAlmostEqual(sells[0]["limit_price"], 103.0)
        self.assertAlmostEqual(sells[0]["base_qty"], self.campaign.filled_base_qty)
        self.assertIsNotNone(self.campaign.tp_order_id)

    async def test_externally_cancelled_order_is_replaced(self):
        await self.engine._sync_live_orders(self.campaign)
        order = self.leg.pending_orders[4]
        old_order_id = order.order_id
        self.broker.order_lookup[str(old_order_id)] = {"status": "CANCELED"}
        await self.engine._sync_live_orders(self.campaign)
        self.assertEqual(order.status, "PLACED")
        self.assertNotEqual(order.order_id, old_order_id)
        self.assertEqual(order.rev, 1)

    async def test_tp_fill_completes_campaign_and_cancels_entries(self):
        await self.engine._sync_live_orders(self.campaign)
        filled = self.leg.pending_orders[2]
        self.broker.order_lookup[str(filled.order_id)] = {
            "status": "FILLED",
            "executedQty": str(filled.quantity),
            "cummulativeQuoteQty": str(filled.quantity * 97.0),
        }
        await self.engine._sync_live_orders(self.campaign)
        tp_id = self.campaign.tp_order_id
        self.broker.order_lookup[str(tp_id)] = {
            "status": "FILLED",
            "executedQty": str(self.campaign.filled_base_qty),
            "cummulativeQuoteQty": str(self.campaign.filled_base_qty * 103.0),
        }
        await self.engine._sync_live_orders(self.campaign)
        self.assertEqual(self.campaign.state, "COMPLETED")
        self.assertAlmostEqual(self.campaign.realized_pnl, (103.0 - 97.0) * self.campaign.filled_base_qty, places=6)
        # Remaining resting entries were cancelled on completion.
        self.assertTrue(self.broker.cancelled)

    async def test_paper_campaign_never_touches_broker(self):
        self.campaign.mode = "paper"
        changed = await self.engine._sync_live_orders(self.campaign)
        self.assertFalse(changed)
        self.assertEqual(self.broker.placed_orders, [])

    async def test_ambiguous_placement_recovers_by_client_id(self):
        order = self.leg.pending_orders[2]

        original_place = self.broker.place_order

        def failing_place(product_id, size, side, **kwargs):
            if kwargs.get("client_order_id") == order.client_order_id:
                # Simulate timeout after the exchange accepted the order.
                self.broker.open_orders.append(
                    {"orderId": 4242, "clientOrderId": order.client_order_id, "executedQty": "0"}
                )
                raise TimeoutError("timed out")
            return original_place(product_id, size, side, **kwargs)

        self.broker.place_order = failing_place
        await self.engine._sync_live_orders(self.campaign)
        self.assertEqual(order.status, "PLACED")
        self.assertEqual(str(order.order_id), "4242")


class CascadeEngineApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_start_campaign_validates_inputs(self):
        engine = _mk_engine()
        result = await engine.start_campaign("BTCUSDT", 2000, 0, 100)
        self.assertIn("error", result)
        result = await engine.start_campaign("BTCUSDT", 2000, 100, 105)
        self.assertIn("error", result)
        result = await engine.start_campaign("BTCUSDT", 4, 105, 99)
        self.assertIn("error", result)
        engine.stop()

    async def test_start_and_stop_campaign(self):
        engine = _mk_engine()
        result = await engine.start_campaign("BTCUSDT", 2000, 105, 99, mother_timestamp=_RECENT_TS)
        self.assertEqual(result["status"], "ok")
        campaign_id = result["campaign"]["campaign_id"]
        self.assertEqual(engine.campaigns[campaign_id].state, "WAITING_FIRST_DEPTH")
        self.assertTrue(engine._running)

        stopped = await engine.stop_campaign(campaign_id)
        self.assertEqual(stopped["status"], "ok")
        self.assertEqual(engine.campaigns[campaign_id].state, "STOPPED")
        self.assertEqual(len(engine.closed_campaigns), 1)
        engine.stop()

    async def test_mode_flip_requires_no_fills(self):
        engine = _mk_engine()
        campaign = _mk_campaign(engine, mode="paper")
        campaign.all_fills.append(
            __import__("engine.cascade", fromlist=["Fill"]).Fill(price=96, quantity=1, level=2, leg_id=1, timestamp=1)
        )
        result = await engine.set_mode(campaign.campaign_id, "live")
        self.assertIn("error", result)

    async def test_mode_flip_requires_configured_broker(self):
        broker = FakeCascadeBroker()
        broker.configured = False
        engine = _mk_engine(broker)
        campaign = _mk_campaign(engine, mode="paper")
        result = await engine.set_mode(campaign.campaign_id, "live")
        self.assertIn("error", result)

    async def test_restore_roundtrip(self):
        engine = _mk_engine()
        campaign = _mk_campaign(engine)
        campaign.state = "TRENDLINE_ACTIVE"
        snapshot = [campaign.to_dict()]

        engine2 = _mk_engine()
        restored = engine2.restore_campaigns(snapshot)
        self.assertEqual(restored, 1)
        self.assertIn(campaign.campaign_id, engine2.campaigns)
        self.assertEqual(engine2.campaigns[campaign.campaign_id].state, "TRENDLINE_ACTIVE")


class CascadeUntouchedTrendlineTests(unittest.TestCase):
    """A trendline broken before it ever produced a leg must be discarded and
    redrawn. Otherwise the model's recovery path (break + low-break of the last
    leg's low) can never run — there is no leg — and the campaign is stranded
    forever with a line far below price that can never be touched again."""

    def setUp(self):
        self.engine = _mk_engine()
        self.campaign = _mk_campaign(self.engine)  # mother high 105 / low 99

    def _feed_all(self, rows):
        for step, o, h, low, cl in rows:
            _feed(self.engine, self.campaign, Candle(step * 300, o, h, low, cl))

    def test_break_alone_does_not_discard_the_trendline(self):
        """A break while the line is still above the depth low must be left
        alone — price often comes back and touches it a candle or two later."""
        self._feed_all(
            [
                (1, 104.8, 104.9, 104.5, 104.6),
                (2, 104.6, 104.7, 104.0, 104.1),  # depth -> trendline 1
            ]
        )
        self.assertEqual(self.campaign.state, "TRENDLINE_ACTIVE")
        self._feed_all([(3, 104.1, 104.9, 104.1, 104.8)])  # close above the line
        self.assertTrue(self.campaign.pending_break)
        self.assertEqual(self.campaign.state, "TRENDLINE_ACTIVE")
        self.assertEqual(self.campaign.active_trendline_id, 1)  # still in play

    def test_trendline_below_depth_low_is_discarded_and_redrawn(self):
        self._feed_all(
            [
                (1, 104.8, 104.9, 104.5, 104.6),
                (2, 104.6, 104.7, 104.0, 104.1),  # depth 104.0 -> trendline 1
                (3, 104.1, 104.9, 104.1, 104.8),  # break, line still above depth low
            ]
        )
        self.assertEqual(len(self.campaign.trendlines), 1)
        self.assertTrue(self.campaign.pending_break)

        # Hold price up so the line keeps sliding until it drops under 104.0
        self._feed_all(
            [
                (4, 104.8, 104.9, 104.6, 104.7),
                (5, 104.7, 104.8, 104.6, 104.65),
                (6, 104.7, 104.8, 104.6, 104.75),
            ]
        )
        self.assertEqual(self.campaign.state, "WAITING_FIRST_DEPTH")
        self.assertIsNone(self.campaign.active_trendline_id)
        self.assertFalse(self.campaign.pending_break)
        self.assertEqual(len(self.campaign.trendlines), 1)  # retired, kept for the chart

        # A fresh depth (two more red candles) redraws from the mother high
        self._feed_all(
            [
                (7, 104.75, 104.8, 104.0, 104.1),
                (8, 104.1, 104.2, 103.9, 104.0),
            ]
        )
        self.assertEqual(self.campaign.state, "TRENDLINE_ACTIVE")
        self.assertEqual(len(self.campaign.trendlines), 2)
        self.assertEqual(self.campaign.active_trendline_id, 2)

    def test_break_with_existing_leg_still_awaits_low_break(self):
        """With a leg on the trendline the original model applies: arm
        pending_break and wait for a decisive low-break, do not discard."""
        self._run = None
        campaign = self.campaign
        self._feed_all(
            [
                (1, 104.8, 104.9, 104.5, 104.6),
                (2, 104.6, 104.7, 104.0, 104.1),
            ]
        )
        # Force a leg so the break path takes the model's branch.
        from engine.cascade import Leg, build_fib_ladder_and_pool, plan_leg_orders

        leg = Leg(leg_id=1, trendline_id=campaign.active_trendline_id, low=104.0, touch_high=104.5, touch_timestamp=600)
        campaign.legs.append(leg)
        build_fib_ladder_and_pool(campaign, leg)
        plan_leg_orders(campaign, leg)
        leg.finalized = True
        campaign.swing_tracking = False

        self._feed_all([(3, 104.1, 104.9, 104.1, 104.8)])  # close above line
        self.assertEqual(campaign.state, "TRENDLINE_ACTIVE")
        self.assertTrue(campaign.pending_break)
        self.assertEqual(len(campaign.trendlines), 1)


class CascadeMotherTimestampTests(unittest.IsolatedAsyncioTestCase):
    """A blank mother timestamp must anchor to the historical mother candle
    (found by matching the mother high), not to 'now' — otherwise the engine
    waits for future candles forever and never draws a trendline."""

    def _candles_df(self, rows):
        # rows: list of (ts_seconds, high). Build a UTC DatetimeIndex frame.
        index = pd.to_datetime([ts for ts, _ in rows], unit="s", utc=True)
        return pd.DataFrame(
            {
                "open": [h for _, h in rows],
                "high": [h for _, h in rows],
                "low": [h - 50 for _, h in rows],
                "close": [h - 10 for _, h in rows],
                "volume": [1.0 for _ in rows],
            },
            index=index,
        )

    async def test_blank_timestamp_auto_detects_mother_candle(self):
        broker = FakeCascadeBroker()
        engine = _mk_engine(broker)
        now = int(time.time())
        mother_ts = (now - 4 * 3600) // 300 * 300  # 4h ago, aligned to 5m
        broker.candles_df = self._candles_df(
            [
                (mother_ts - 600, 64800.0),
                (mother_ts, 64967.25),  # the mother candle high
                (mother_ts + 600, 64700.0),
            ]
        )
        result = await engine.start_campaign("BTCUSDT", 2000, 64967.25, 64816.11)
        engine.stop()
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["campaign"]["mother_timestamp"], mother_ts)
        # Anchored in the past, so the engine will replay history rather than
        # sitting at "now" waiting for future candles.
        self.assertLess(result["campaign"]["mother_timestamp"], now - 3600)

    async def test_blank_timestamp_errors_when_no_match(self):
        broker = FakeCascadeBroker()
        engine = _mk_engine(broker)
        now = int(time.time())
        broker.candles_df = self._candles_df([(now - 900, 50000.0), (now - 600, 50100.0)])
        result = await engine.start_campaign("BTCUSDT", 2000, 64967.25, 64816.11)
        engine.stop()
        self.assertIn("error", result)
        self.assertIn("Mother Candle Time", result["error"])

    async def test_explicit_timestamp_is_used_as_is(self):
        broker = FakeCascadeBroker()
        engine = _mk_engine(broker)
        result = await engine.start_campaign("BTCUSDT", 2000, 105, 99, mother_timestamp=_RECENT_TS)
        engine.stop()
        self.assertEqual(result["campaign"]["mother_timestamp"], _RECENT_TS)


if __name__ == "__main__":
    unittest.main()
