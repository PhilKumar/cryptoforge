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


# Real BTCUSDT 5m candles, 2026-07-20 from the mother candle at 00:15 UTC.
# The user verified both fibs off these on TradingView.
_REAL = [
    # index (5m offset from the mother candle), open, high, low, close
    # Real BTCUSDT candles, 2026-07-20 00:15 -> 05:10 UTC. The user verified
    # both fibs off these on TradingView.
    (0, 65020.00, 65107.99, 65002.00, 65051.98),
    (1, 65051.98, 65051.98, 64804.76, 64919.31),
    (2, 64919.31, 64923.67, 64852.01, 64876.01),
    (3, 64876.01, 64878.01, 64792.00, 64800.01),
    (4, 64800.00, 64938.00, 64790.01, 64904.00),
    (5, 64904.00, 64928.00, 64822.24, 64822.24),
    (6, 64822.24, 64822.24, 64639.00, 64665.99),
    (7, 64666.00, 64671.47, 64416.00, 64588.00),
    (8, 64588.50, 64593.98, 64544.00, 64553.84),
    (9, 64553.85, 64606.00, 64510.00, 64606.00),
    (10, 64606.00, 65010.15, 64605.99, 64999.13),
    (11, 64999.13, 65029.40, 64806.37, 64808.00),
    (12, 64808.01, 64839.23, 64702.29, 64709.99),
    (13, 64709.99, 64914.00, 64690.00, 64865.49),
    (14, 64865.49, 64946.00, 64850.00, 64850.42),
    (15, 64850.43, 64898.00, 64837.01, 64874.52),
    (16, 64874.52, 64931.34, 64838.00, 64886.22),
    (17, 64887.71, 64894.05, 64704.41, 64763.99),
    (18, 64763.99, 64775.36, 64712.00, 64770.18),
    (19, 64770.17, 64770.18, 64526.00, 64526.01),
    (20, 64526.00, 64639.89, 64506.00, 64628.01),
    (21, 64628.01, 64931.02, 64624.00, 64916.01),
    (22, 64916.00, 64950.88, 64854.00, 64917.53),
    (23, 64917.54, 64922.92, 64820.26, 64826.01),
    (24, 64826.01, 64830.00, 64652.00, 64652.00),
    (25, 64652.00, 64696.00, 64585.00, 64675.74),
    (26, 64675.75, 64707.67, 64645.70, 64674.01),
    (27, 64674.00, 64858.00, 64660.00, 64780.00),
    (28, 64780.00, 64820.00, 64726.01, 64750.00),
    (29, 64750.00, 64886.00, 64732.01, 64836.00),
    (30, 64836.00, 64964.00, 64836.00, 64912.00),
    (31, 64912.01, 64928.00, 64840.00, 64871.99),
    (32, 64871.99, 64890.00, 64827.19, 64827.20),
    (33, 64827.19, 64881.29, 64822.24, 64874.00),
    (34, 64874.00, 64875.22, 64796.00, 64814.01),
    (35, 64814.01, 64850.24, 64776.00, 64843.96),
    (36, 64843.96, 64887.98, 64790.00, 64792.00),
    (37, 64792.00, 64792.00, 64670.00, 64728.01),
    (38, 64728.00, 64728.01, 64644.01, 64688.01),
    (39, 64688.00, 64764.00, 64666.00, 64736.00),
    (40, 64736.01, 64840.04, 64710.00, 64814.00),
    (41, 64814.00, 64902.63, 64795.11, 64902.63),
    (42, 64902.63, 64914.93, 64836.00, 64880.00),
    (43, 64880.00, 64901.32, 64853.40, 64853.40),
    (44, 64853.40, 64869.80, 64785.10, 64869.80),
    (45, 64869.79, 64869.99, 64802.00, 64805.99),
    (46, 64805.99, 64806.00, 64736.00, 64740.00),
    (47, 64739.99, 64792.00, 64698.26, 64705.37),
    (48, 64705.38, 64709.17, 64640.00, 64682.00),
    (49, 64682.00, 64718.00, 64667.03, 64718.00),
    (50, 64718.00, 64778.00, 64716.78, 64759.41),
    (51, 64759.40, 64788.00, 64721.19, 64746.00),
    (52, 64746.01, 64814.00, 64744.00, 64770.00),
    (53, 64770.00, 64770.00, 64704.01, 64733.98),
    (54, 64733.54, 64733.54, 64629.18, 64650.02),
    (55, 64650.01, 64650.01, 64540.00, 64540.01),
    (56, 64540.01, 64585.00, 64540.00, 64540.01),
    (57, 64540.00, 64562.00, 64450.00, 64454.01),
    (58, 64454.00, 64492.00, 64404.00, 64420.01),
    (59, 64420.01, 64420.01, 64082.70, 64244.00),
]


def _real_campaign(engine):
    mother = _REAL[0]
    campaign = Campaign(
        campaign_id="real",
        symbol="BTCUSDT",
        capital_usd=2000.0,
        mother_high=mother[2],
        mother_low=mother[3],
        mother_timestamp=0,
        mode="paper",
        min_notional_usd=5.0,
    )
    engine.campaigns[campaign.campaign_id] = campaign
    return campaign


class CascadeSwingModelTests(unittest.TestCase):
    """The swing model: a dip, a rise that freezes it, then a red close below
    that dip cuts the swing and draws its trendline + fib."""

    def setUp(self):
        self.engine = _mk_engine()
        self.campaign = _real_campaign(self.engine)

    def _feed_real(self, upto_index):
        """Feed real candles whose offset index is <= upto_index."""
        for idx, o, h, low, c in _REAL[1:]:
            if idx > upto_index:
                break
            _feed(self.engine, self.campaign, Candle(idx * 300, o, h, low, c))

    def test_reproduces_the_users_first_fib_exactly(self):
        """fib 0 is the high of the candle that TOUCHES the trendline (00:40,
        64,928.00) — not the swing's highest high."""
        self._feed_real(6)
        self.assertEqual(len(self.campaign.legs), 1)
        leg = self.campaign.legs[0]
        self.assertAlmostEqual(leg.touch_high, 64928.00)  # fib 0 = touch
        self.assertAlmostEqual(leg.low, 64790.01)  # fib 1 = the dip
        for level, expected in ((2, 64652.02), (4, 64376.04), (8, 63824.08)):
            self.assertAlmostEqual(leg.fib.level_price(level), expected, places=2)

    def test_reproduces_the_users_second_fib_exactly(self):
        """fib 0 is the highest high that reached the trendline — a touch OR a
        break — between the dip and the cut (64,964.00 at 02:45)."""
        self._feed_real(59)
        self.assertEqual(len(self.campaign.legs), 2)
        leg = self.campaign.legs[1]
        self.assertAlmostEqual(leg.touch_high, 64964.00)  # fib 0
        self.assertAlmostEqual(leg.low, 64416.00)  # fib 1 — the "ultimate low"
        self.assertAlmostEqual(leg.fib.level_price(2), 63868.00, places=2)

    def test_no_fib_before_the_trendline_is_touched(self):
        """Cuts during the initial slide draw nothing: the line has not been
        touched yet, so there is no fib 0 to anchor to."""
        self._feed_real(3)
        self.assertEqual(len(self.campaign.legs), 0)

    def test_rise_freezes_the_dip(self):
        self._feed_real(11)  # dip 64,416.00 frozen by the rise off it
        self.assertAlmostEqual(self.campaign.swing_low, 64416.00)
        self.assertTrue(self.campaign.swing_risen)

    def test_trendline_anchors_to_the_latest_high_before_the_depth(self):
        self._feed_real(6)
        tl = self.campaign.trendlines[0]
        self.assertAlmostEqual(tl.anchor1_price, 65107.99)  # mother high
        self.assertAlmostEqual(tl.anchor2_price, 65051.98)  # highest high before the dip

    def test_fall_pct_and_pool_follow_the_leg_low(self):
        self._feed_real(59)
        leg1, leg2 = self.campaign.legs
        self.assertAlmostEqual(leg1.leg_pct_from_mother, 0.488, places=2)
        self.assertAlmostEqual(leg2.leg_pct_from_mother, 1.063, places=2)
        # leg 2 only draws the incremental depth beyond leg 1
        self.assertAlmostEqual(leg2.pool_usd, (1.063 - 0.488) * 2000 / 100, places=1)

    def test_second_leg_carries_forward_unfilled_levels(self):
        self._feed_real(59)
        leg1, leg2 = self.campaign.legs
        carried = [lv for lv, o in leg1.pending_orders.items() if o.status == "CARRIED"]
        self.assertTrue(carried)
        for lv in carried:
            self.assertGreater(leg2.carry_forward_qty.get(lv, 0), 0)

    def test_mother_break_ends_the_campaign(self):
        self._feed_real(6)
        _feed(self.engine, self.campaign, Candle(99 * 300, 65000.0, 65200.0, 64900.0, 65150.0))
        self.assertEqual(self.campaign.state, "MOTHER_BROKEN")
        self.assertTrue(self.campaign.mother_broken_above)


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
        # TP = 97 + 0.25*(105-97) = 99
        self.assertAlmostEqual(sells[0]["limit_price"], 99.0)
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
            "cummulativeQuoteQty": str(self.campaign.filled_base_qty * 99.0),
        }
        await self.engine._sync_live_orders(self.campaign)
        self.assertEqual(self.campaign.state, "COMPLETED")
        self.assertAlmostEqual(self.campaign.realized_pnl, (99.0 - 97.0) * self.campaign.filled_base_qty, places=6)
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
