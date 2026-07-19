"""CascadeEngine tests: paper-mode state machine + live desired-state order sync."""

import unittest

from engine.cascade import (
    Campaign,
    Candle,
    CascadeEngine,
    Leg,
    build_fib_ladder_and_pool,
    plan_leg_orders,
)


class FakeCascadeBroker:
    display_name = "Fake Broker"

    def __init__(self):
        self.placed_orders = []
        self.cancelled = []
        self.open_orders = []  # raw binance-style rows returned by get_orders
        self.order_lookup = {}  # order_id -> raw status row for get_order
        self._next_order_id = 1000
        self.configured = True

    def _is_configured(self):
        return self.configured

    def get_product_by_symbol(self, symbol):
        return {"symbol": symbol, "broker_symbol": symbol, "min_notional": "5.0", "tick_size": "0.01"}

    def get_ticker(self, symbol):
        return {"symbol": symbol, "last_price": 0.0, "mark_price": 0.0}

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

    def test_first_trendline_forms_after_depth(self):
        self._run_scenario(upto=3)
        self.assertEqual(self.campaign.state, "TRENDLINE_ACTIVE")
        self.assertEqual(len(self.campaign.trendlines), 1)
        tl = self.campaign.trendlines[0]
        self.assertEqual(tl.anchor1_price, 105.0)
        self.assertEqual(tl.anchor2_price, 103.0)  # red open at t=2
        self.assertEqual(tl.anchor2_timestamp, 600)

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
        result = await engine.start_campaign("BTCUSDT", 2000, 105, 99, mother_timestamp=0)
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


if __name__ == "__main__":
    unittest.main()
