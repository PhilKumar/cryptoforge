"""CascadeEngine tests: paper-mode state machine + live desired-state order sync."""

import time
import unittest

import pandas as pd

from engine.cascade import (
    MIN_LEG_SEPARATION_PCT,
    Campaign,
    Candle,
    CascadeEngine,
    FibLadder,
    Fill,
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

    def test_dip_freezes_at_the_touch_so_later_wicks_stay_out(self):
        """The 05:05 candle wicks to 64,404 — below the 64,416 dip — but the
        touch at 02:45 already froze the dip, so fib 2's level 1 must ignore it."""
        self._feed_real(59)
        self.assertAlmostEqual(self.campaign.legs[1].low, 64416.00)

    def test_trendline_anchors_to_a_red_candle_open(self):
        """The line is the tightest descending line from the mother high that no
        close has crossed (find_valid_anchor2) — the 6th candle's open here,
        which is what TradingView's magnet snaps to."""
        self._feed_real(6)
        tl = self.campaign.trendlines[0]
        self.assertAlmostEqual(tl.anchor1_price, 65107.99)  # mother high
        self.assertAlmostEqual(tl.anchor2_price, 64904.00)  # 6th candle open

    def test_second_trendline_anchors_to_the_0915_candle_open(self):
        self._feed_real(59)
        self.assertEqual(len(self.campaign.trendlines), 2)
        self.assertAlmostEqual(self.campaign.trendlines[1].anchor2_price, 64902.63)

    def test_fall_pct_and_pool_follow_the_leg_low(self):
        self._feed_real(59)
        leg1, leg2 = self.campaign.legs
        self.assertAlmostEqual(leg1.leg_pct_from_mother, 0.488, places=2)
        self.assertAlmostEqual(leg2.leg_pct_from_mother, 1.063, places=2)
        # leg 2 only draws the incremental depth beyond leg 1
        self.assertAlmostEqual(leg2.pool_usd, (1.063 - 0.488) * 2000 / 100, places=1)

    def test_a_second_fib_leaves_the_first_ladder_resting(self):
        """Fib 2 forming does not retire fib 1. Fib 1's levels sit above the
        market and are exactly where price has to pass on the way back up, so
        they stay live and only the money fib 1 could never place moves on."""
        self._feed_real(59)
        leg1, leg2 = self.campaign.legs
        self.assertTrue(
            [o for o in leg1.pending_orders.values() if o.is_open and o.usd_notional > 0],
            "fib 1 must still have a funded order resting after fib 2 is drawn",
        )
        self.assertFalse([o for o in leg1.pending_orders.values() if o.status == "CARRIED" and o.usd_notional > 0])
        # Nothing was carried out of fib 1 beyond the slice it could not place,
        # so its resting notional accounts for its whole pool.
        self.assertAlmostEqual(self.campaign.leg_resting_usd(leg1.leg_id), leg1.pool_total_usd, places=2)
        # Every dollar of both allocations is still accounted for: resting on
        # fib 1, laddered on fib 2, or waiting in the carry bucket.
        self.assertAlmostEqual(
            self.campaign.resting_usd + self.campaign.carry_forward_usd,
            leg1.pool_usd + leg2.pool_usd,
            places=1,
        )

    def test_round_closed_at_tp_returns_its_principal_to_the_next_fib(self):
        """
        The user's worked example: fib 1 ladders a pool, one level fills and the
        target hits. When the previous low then breaks, fib 2 must inherit the
        WHOLE fib 1 pool — the levels that never filled plus the principal the
        closed round handed back.
        """
        self._feed_real(40)  # far enough to have fib 1 laddered
        leg1 = self.campaign.legs[0]
        # The leg's own allocation, before any slice too small to place was
        # handed straight back — all of it must reach fib 2 either way.
        pool1 = leg1.pool_usd
        self.assertGreater(pool1, 0.0)

        # Fill the deepest planned level, then let the target hit.
        order = next(o for o in leg1.pending_orders.values() if o.is_open and o.usd_notional > 0)
        self.engine._record_fill(self.campaign, leg1, order, order.price, _RECENT_TS + 3600, order_id="PAPER")
        self.assertGreater(self.campaign.spent_usd, 0.0)
        self.engine._close_round(self.campaign, self.campaign.tp_price)

        self.assertEqual(len(self.campaign.rounds), 1)
        self.assertGreater(self.campaign.rounds[0].pnl, 0.0)
        self.assertAlmostEqual(self.campaign.spent_usd, 0.0)  # principal is back

        # Previous low breaks -> fib 2 opens and inherits everything.
        self._feed_real(59)
        self.assertGreaterEqual(len(self.campaign.legs), 2)
        leg2 = self.campaign.legs[1]
        self.assertAlmostEqual(leg2.carry_in_usd, pool1, places=6)
        self.assertEqual(self.campaign.state, "TRENDLINE_ACTIVE")

    def test_mother_break_ends_the_campaign(self):
        self._feed_real(6)
        _feed(self.engine, self.campaign, Candle(99 * 300, 65000.0, 65200.0, 64900.0, 65150.0))
        self.assertEqual(self.campaign.state, "MOTHER_BROKEN")
        self.assertTrue(self.campaign.mother_broken_above)


# Second regression day: BTCUSDT 5m, 2026-07-20 from the mother candle at
# 11:55 UTC (17:25 IST). The user verified fib 1 and fib 2 on TradingView and
# stated the third structure is marked on the 19:20 IST candle. This day is the
# one that exposed the discarded-first-dip bug: the 12:00 monster red candle
# closes below the mother low immediately, and the old swing logic threw the
# 64,716.57 dip away.
_REAL2 = [
    (0, 64965.03, 65068.00, 64934.00, 65002.01),
    (1, 65002.00, 65002.83, 64716.57, 64803.99),
    (2, 64803.99, 64865.79, 64780.34, 64865.79),
    (3, 64865.78, 64865.79, 64723.89, 64750.00),
    (4, 64749.99, 64806.00, 64692.00, 64692.91),
    (5, 64692.92, 64758.24, 64680.00, 64710.00),
    (6, 64710.01, 64747.98, 64670.01, 64699.81),
    (7, 64699.82, 64716.82, 64608.00, 64708.26),
    (8, 64708.26, 64748.30, 64682.00, 64682.00),
    (9, 64682.00, 64699.89, 64599.95, 64599.97),
    (10, 64599.97, 64738.93, 64599.89, 64729.35),
    (11, 64729.34, 64736.00, 64650.65, 64656.00),
    (12, 64656.00, 64689.89, 64622.21, 64640.00),
    (13, 64640.00, 64690.20, 64621.79, 64638.00),
    (14, 64638.00, 64730.00, 64638.00, 64730.00),
    (15, 64729.99, 64753.77, 64692.37, 64692.38),
    (16, 64692.38, 64720.21, 64684.00, 64706.00),
    (17, 64706.00, 64706.00, 64629.28, 64629.28),
    (18, 64629.29, 64629.29, 64530.00, 64554.75),
    (19, 64554.75, 64708.00, 64502.00, 64682.01),
    (20, 64682.00, 64763.67, 64630.01, 64672.87),
    (21, 64672.87, 64707.56, 64570.72, 64650.66),
    (22, 64650.67, 64727.40, 64369.87, 64720.81),
    (23, 64720.82, 64761.62, 64315.56, 64344.45),
    (24, 64344.44, 64414.00, 64208.00, 64344.01),
    (25, 64344.01, 64476.38, 64288.00, 64427.90),
    (26, 64427.90, 64495.99, 64385.17, 64466.01),
    (27, 64466.01, 64466.01, 64294.11, 64322.00),
    (28, 64322.00, 64354.00, 64170.28, 64258.00),
    (29, 64258.01, 64360.00, 64077.76, 64344.00),
]


class CascadeSecondDayRegressionTests(unittest.TestCase):
    """2026-07-20 11:55 UTC mother candle — verified against the user's chart."""

    def setUp(self):
        self.engine = _mk_engine()
        mother = _REAL2[0]
        self.campaign = Campaign(
            campaign_id="real2",
            symbol="BTCUSDT",
            capital_usd=2000.0,
            mother_high=mother[2],
            mother_low=mother[3],
            mother_timestamp=0,
            mode="paper",
            min_notional_usd=5.0,
        )
        self.campaign.window_start_ts = 0
        self.engine.campaigns[self.campaign.campaign_id] = self.campaign

    def _feed(self, upto_index):
        for idx, o, h, low, c in _REAL2[1:]:
            if idx > upto_index:
                break
            _feed(self.engine, self.campaign, Candle(idx * 300, o, h, low, c))

    def test_first_dip_survives_the_opening_monster_candle(self):
        """12:00 closes below the mother low immediately. That is the fall
        forming — not a broken leg — and 64,716.57 must remain the dip."""
        self._feed(4)
        self.assertEqual(len(self.campaign.legs), 1)
        leg = self.campaign.legs[0]
        self.assertAlmostEqual(leg.touch_high, 64865.79)  # fib 0: one-cent touch at 12:10
        self.assertAlmostEqual(leg.low, 64716.57)  # fib 1: the 12:00 dip, not discarded
        tl = self.campaign.trendlines[0]
        self.assertAlmostEqual(tl.anchor2_price, 64865.78)  # 12:10 red candle open

    def test_second_fib_matches_the_user_chart(self):
        self._feed(18)
        self.assertEqual(len(self.campaign.legs), 2)
        leg = self.campaign.legs[1]
        self.assertAlmostEqual(leg.touch_high, 64753.77)  # 13:10 high
        self.assertAlmostEqual(leg.low, 64599.89)  # the dip under 64,600

    def test_indecisive_probe_below_the_dip_does_not_draw(self):
        """12:40 closes 8 dollars below the 64,608 dip — the fall resuming.
        No structure exists there on the user's chart."""
        self._feed(14)
        self.assertEqual(len(self.campaign.legs), 1)

    def test_same_shelf_structure_draws_geometry_only(self):
        """The user's chart: three trendlines, two fibs. The 19:20 IST structure
        is touched 0.015% from fib 2's — same shelf — so its line is drawn but
        carries no fib. Its anchor is the 19:20 open (64,720.82), which is what
        the magnet snaps to; fib-bearing anchors still exclude the cut candle."""
        self._feed(23)
        self.assertEqual(len(self.campaign.trendlines), 3)
        self.assertEqual(len(self.campaign.legs), 2)
        third = self.campaign.trendlines[2]
        self.assertFalse(third.bears_fib)
        self.assertAlmostEqual(third.anchor2_price, 64720.82)
        self.assertEqual(third.anchor2_timestamp, 23 * 300)
        # The fib-bearing lines and their fibs are untouched.
        self.assertTrue(self.campaign.trendlines[0].bears_fib)
        self.assertTrue(self.campaign.trendlines[1].bears_fib)
        self.assertAlmostEqual(self.campaign.legs[1].touch_high, 64753.77)
        self.assertEqual(self.campaign.active_trendline_id, 2)

    def test_the_shelf_check_looks_at_every_fib_not_just_the_last(self):
        """A live SOL campaign drew fib 1 and fib 3 with the identical touch
        high of 78.75, because fib 3 was only ever compared against fib 2.
        Price wanders off a shelf and comes back hours later, so the duplicate
        is usually a couple of fibs back."""
        self._feed(29)
        highs = [leg.touch_high for leg in self.campaign.legs if leg.touch_high]
        for i, a in enumerate(highs):
            for b in highs[i + 1 :]:
                self.assertGreaterEqual(
                    abs(a - b) / b,
                    MIN_LEG_SEPARATION_PCT,
                    f"fibs at {a} and {b} are the same shelf and should not both exist",
                )

    def test_skipping_keeps_the_money_on_one_ladder(self):
        """A same-shelf third fib would split the pool across two sets of levels
        a few ticks apart, and each slice is then small enough that Binance's
        minimum eats it. Skipping keeps the campaign with funded orders."""
        self._feed(29)
        self.assertEqual(len(self.campaign.legs), 2)
        working = [
            o for leg in self.campaign.legs for o in leg.pending_orders.values() if o.is_open and o.usd_notional > 0
        ]
        self.assertTrue(working)

    def test_the_day_now_trades_because_the_pool_lands_where_price_goes(self):
        """This day used to buy nothing: every shallow slice was under Binance's
        minimum and merged DOWN onto level 8, below anything price reached.
        Pooling up into level 2 instead puts the money in reach, and the buy
        stop fills."""
        self._feed(29)
        self.assertTrue(self.campaign.all_fills, "the pooled level 2 should have filled")
        fill = self.campaign.all_fills[0]
        self.assertEqual(fill.level, 2)
        self.assertLess(fill.price, self.campaign.mother_high)

    def test_mother_break_realises_the_round_it_opened(self):
        """Same day, run to the mother break. Price cannot reach the mother high
        without passing the target first, so the round must close in profit."""
        self._feed(29)
        for idx, o, h, low, c in [
            (37, 64416.01, 64608.00, 64398.15, 64604.65),
            (38, 64604.65, 64800.00, 64540.00, 64800.00),
            (39, 64800.00, 64988.00, 64784.00, 64968.00),
            (40, 64967.99, 65100.00, 64898.01, 64994.12),
        ]:
            _feed(self.engine, self.campaign, Candle(idx * 300, o, h, low, c))
        self.assertEqual(self.campaign.state, "MOTHER_BROKEN")
        self.assertEqual(len(self.campaign.rounds), 1)
        self.assertGreater(self.campaign.rounds[0].pnl, 0.0)
        self.assertEqual(self.campaign.filled_base_qty, 0.0)  # closed out, flat


# Third regression day: BTCUSDT 5m from the mother candle at 2026-07-20 18:10
# UTC (23:40 IST), high 65,799. This is the case that exposed the "no structure
# ever forms" bug: price fell steadily, so every candle that reached the falling
# trendline was ALSO printing a lower low, and the old guard rejected all of
# them. The engine sat in WAITING_FIRST_DEPTH for 99 candles while the user drew
# the structure by hand in seconds.
_REAL3 = [
    (0, 65593.64, 65799.00, 65566.30, 65753.05),
    (1, 65753.04, 65770.00, 65656.98, 65671.98),
    (2, 65671.98, 65671.98, 65582.00, 65589.13),
    (3, 65589.14, 65629.98, 65577.05, 65592.00),
    (4, 65592.01, 65592.01, 65479.27, 65496.00),
    (5, 65496.00, 65541.94, 65460.56, 65487.17),
    (6, 65487.18, 65487.18, 65348.73, 65348.74),
    (7, 65348.74, 65348.74, 65254.00, 65288.53),
    (8, 65288.54, 65310.00, 65225.51, 65225.51),
    (9, 65225.52, 65258.67, 65204.00, 65240.01),
    (10, 65240.01, 65274.58, 65160.00, 65186.57),
    (11, 65186.57, 65236.00, 65165.48, 65224.00),
    (12, 65224.00, 65246.00, 65102.00, 65104.00),
    (13, 65103.99, 65196.00, 65082.81, 65164.08),
    (14, 65164.08, 65169.23, 65118.04, 65136.00),
    (15, 65136.01, 65186.98, 65072.20, 65156.24),
    (16, 65156.24, 65158.73, 65061.99, 65078.00),
]


class CascadeThirdDayRegressionTests(unittest.TestCase):
    """2026-07-20 18:10 UTC mother candle — a steady fall, verified by the user
    against TradingView: fib 0 = 65,246.00, fib 1 = 65,160.00, and the buy
    ladder at 65,074 / 64,902 / 64,558."""

    def setUp(self):
        self.engine = _mk_engine()
        mother = _REAL3[0]
        self.campaign = Campaign(
            campaign_id="real3",
            symbol="BTCUSDT",
            capital_usd=2000.0,
            mother_high=mother[2],
            mother_low=mother[3],
            mother_timestamp=0,
            mode="paper",
            min_notional_usd=5.0,
        )
        self.campaign.window_start_ts = 0
        self.engine.campaigns[self.campaign.campaign_id] = self.campaign

    def _feed(self, upto_index):
        for idx, o, h, low, c in _REAL3[1:]:
            if idx > upto_index:
                break
            _feed(self.engine, self.campaign, Candle(idx * 300, o, h, low, c))

    def test_a_steady_fall_still_forms_a_structure(self):
        """Regression: every touching candle here also prints a lower low. The
        engine must still draw, not stall."""
        self._feed(16)
        self.assertTrue(self.campaign.legs, "a steady fall must still form a structure")
        self.assertEqual(self.campaign.state, "TRENDLINE_ACTIVE")

    def test_matches_the_user_chart_exactly(self):
        self._feed(16)
        leg = self.campaign.legs[0]
        self.assertAlmostEqual(leg.touch_high, 65246.00)  # 00:45 IST high
        self.assertAlmostEqual(leg.low, 65160.00)  # 00:30 IST low
        self.assertAlmostEqual(leg.fib.level_price(2), 65074.00)
        self.assertAlmostEqual(leg.fib.level_price(4), 64902.00)
        self.assertAlmostEqual(leg.fib.level_price(8), 64558.00)

    def test_the_dip_candle_high_is_not_its_own_touch(self):
        """The 00:30 candle both set the dip (65,160) and reached the line with a
        65,274.58 high. Its own high must not become fib 0 — the rise has to come
        after the dip, which is why fib 0 is the later 65,246."""
        self._feed(16)
        self.assertNotAlmostEqual(self.campaign.legs[0].touch_high, 65274.58)


class CascadeStopEntryTests(unittest.TestCase):
    """L2/L4 are ONE working buy stop, triggered at the close of the lowest red
    candle so far — above a falling market, dragged down by it, filling only on
    a U-turn back up. A deeper level being crossed adds its money to the working
    order instead of restarting. L8 stays a plain limit at its line."""

    def setUp(self):
        self.engine = _mk_engine()
        self.campaign = Campaign(
            campaign_id="stop1",
            symbol="BTCUSDT",
            capital_usd=2000.0,
            mother_high=65068.0,
            mother_low=64934.0,
            mother_timestamp=0,
            mode="paper",
            min_notional_usd=5.0,
            tick_size=0.01,
        )
        self.campaign.state = "TRENDLINE_ACTIVE"
        self.engine.campaigns[self.campaign.campaign_id] = self.campaign
        # Round anchors so the levels are easy to reason about:
        #   L2 = 64,800   L4 = 64,600   L8 = 64,200
        leg = Leg(leg_id=1, trendline_id=1, low=64900.0, touch_high=65000.0, touch_timestamp=0)
        leg.fib = FibLadder(high_anchor=65000.0, low_anchor=64900.0)
        leg.pool_usd = 300.0
        leg.pool_total_usd = 300.0
        self.campaign.legs.append(leg)
        plan_leg_orders(self.campaign, leg)
        self.leg = leg
        self.l2 = leg.pending_orders[2]
        self.l4 = leg.pending_orders[4]
        self.l8 = leg.pending_orders[8]
        self.assertEqual([self.l2.price, self.l4.price, self.l8.price], [64800.0, 64600.0, 64200.0])
        self.assertEqual([self.l2.usd_notional, self.l4.usd_notional], [60.0, 90.0])
        self._last_close = 64810.0  # just above L2's line, where the fall starts

    def _candle(self, idx, o, h, low, c):
        """Drive the entry machinery in the order _process_candle does, without
        letting the structure detector spawn a new leg out from under the one
        under test. test_process_candle_drives_the_stop_machinery covers that
        the real candle path calls this."""
        candle = Candle(idx * 300, o, h, low, c)
        self.engine._candles_5m.setdefault(self.campaign.campaign_id, []).append(candle)
        if self.campaign.mode == "paper":
            self.engine._paper_fill_check(self.campaign, candle)
        self.engine._advance_stop_entries(self.campaign, candle)

    def _red(self, idx, close, opens=None):
        """A red candle closing at `close`. Real candles open where the last one
        closed, so the open defaults to the previous close — that continuity is
        exactly what makes a trigger sitting on the last close delicate."""
        prev = self._last_close if opens is None else opens
        self._last_close = close
        self._candle(idx, prev, prev + 2, close - 8, close)

    def _fills(self):
        return [(f.level, round(f.price, 2)) for f in self.campaign.all_fills]

    def test_l8_still_rests_on_its_own_line(self):
        self.assertEqual(self.l8.entry_style, "limit")
        self.assertTrue(self.l8.armed)
        self.assertAlmostEqual(self.l8.working_price, 64200.0)

    def test_one_red_below_the_line_is_not_enough_to_place_anything(self):
        self.assertEqual(self.l2.entry_style, "stop")
        self._red(1, 64750.0)
        self.assertFalse(self.l2.armed)
        self.assertIsNone(self.l2.stop_price)
        self.assertAlmostEqual(self.l2.last_red_close, 64750.0)

    def test_second_red_triggers_at_the_previous_red_close(self):
        """Step 2: on the second red close, the stop goes on the FIRST red's
        close — one body back, so it sits above where the market now is."""
        self._red(1, 64750.0)
        self._red(2, 64700.0)
        self.assertTrue(self.l2.armed)
        self.assertAlmostEqual(self.l2.stop_price, 64750.0)  # red #1's close
        self.assertAlmostEqual(self.l2.limit_price, 64750.05)  # + 5 ticks
        self.assertGreater(self.l2.stop_price, 64700.0)  # above the market
        self.assertEqual(self._fills(), [])

    def test_the_stop_is_dragged_down_by_the_fall_and_never_buys_it(self):
        self._red(1, 64750.0)
        self._red(2, 64700.0)
        self.assertAlmostEqual(self.l2.stop_price, 64750.0)
        self._red(3, 64650.0)  # step 4: modify to red #2's close
        self.assertAlmostEqual(self.l2.stop_price, 64700.0)
        self._red(4, 64620.0)
        self.assertAlmostEqual(self.l2.stop_price, 64650.0)
        # Straight down the whole way: the stop walked down and bought none of it.
        self.assertEqual(self._fills(), [])

    def test_the_u_turn_is_what_fills_it(self):
        self._red(1, 64750.0)
        self._red(2, 64700.0)
        self._red(3, 64650.0)  # stop now on red #2's close, 64,700
        self.assertEqual(self._fills(), [])
        self._candle(4, 64650.0, 64720.0, 64645.0, 64715.0)  # turns back up through it
        self.assertEqual(self._fills(), [(2, 64700.05)])

    def test_greens_are_ignored_entirely(self):
        self._red(1, 64750.0)
        self._candle(2, 64740.0, 64760.0, 64735.0, 64755.0)  # green: not counted
        self.assertIsNone(self.l2.stop_price)
        self.assertAlmostEqual(self.l2.last_red_close, 64750.0)
        self._red(3, 64700.0)  # red, lower: pairs with #1
        self.assertAlmostEqual(self.l2.stop_price, 64750.0)  # red #1's close

    def test_a_red_that_closes_higher_does_not_count(self):
        self._red(1, 64700.0)
        self._red(2, 64740.0)  # red, but a HIGHER close
        self.assertIsNone(self.l2.stop_price)
        self.assertAlmostEqual(self.l2.last_red_close, 64700.0)

    def test_the_stop_never_moves_back_up(self):
        self._red(1, 64750.0)
        self._red(2, 64700.0)
        self._red(3, 64650.0)
        self.assertAlmostEqual(self.l2.stop_price, 64700.0)
        self._red(4, 64680.0)  # red, but above the last one: ignored
        self.assertAlmostEqual(self.l2.stop_price, 64700.0)

    def test_crossing_l4_adds_its_money_without_restarting(self):
        self._red(1, 64750.0)
        self._red(2, 64700.0)
        self.assertAlmostEqual(self.l2.usd_notional, 60.0)
        self.assertTrue(self.l4.is_open)
        self._red(3, 64550.0)  # closes under L4's 64,600 line
        # L4 folds into the working order; the sequence carries straight on.
        self.assertEqual(self.l4.status, "MERGED")
        self.assertAlmostEqual(self.l4.usd_notional, 0.0)
        self.assertAlmostEqual(self.l2.usd_notional, 150.0)
        self.assertAlmostEqual(self.l2.stop_price, 64700.0)  # stepped, not reset
        self.assertAlmostEqual(self.l2.quantity, 150.0 / self.l2.limit_price)
        self.assertEqual(self._fills(), [])

    def test_the_merged_order_buys_the_whole_amount_on_the_turn(self):
        self._red(1, 64750.0)
        self._red(2, 64700.0)
        self._red(3, 64550.0)
        self._candle(4, 64550.0, 64720.0, 64545.0, 64715.0)  # U-turn back through it
        self.assertEqual(len(self.campaign.all_fills), 1)
        fill = self.campaign.all_fills[0]
        self.assertAlmostEqual(fill.price, 64700.05)
        self.assertAlmostEqual(fill.price * fill.quantity, 150.0, places=2)

    def test_l8_fills_on_its_own_line_independently(self):
        self._red(1, 64750.0)
        self._red(2, 64700.0)
        self._candle(3, 64700.0, 64705.0, 64150.0, 64180.0)  # blows through L8
        self.assertIn((8, 64200.0), self._fills())

    def test_stop_limit_gap_is_five_ticks_of_the_symbol(self):
        self._red(1, 64750.0)
        self._red(2, 64700.0)
        self.assertAlmostEqual(self.l2.limit_price - self.l2.stop_price, 0.05, places=6)

    def test_live_step_down_recycles_the_order_for_replacement(self):
        self.campaign.mode = "live"
        self._red(1, 64750.0)
        self._red(2, 64700.0)
        self.l2.status = "PLACED"
        self.l2.order_id = "9001"
        self._red(3, 64650.0)
        # The resting stop must be released so the sync loop cancels it and
        # re-places it lower under a fresh client id.
        self.assertEqual(self.l2.status, "PENDING")
        self.assertIsNone(self.l2.order_id)
        self.assertEqual(self.l2.rev, 1)
        self.assertTrue(self.l2.client_order_id.endswith("-2-1"))

    def test_a_stop_at_or_below_market_is_held_back(self):
        """Binance rejects a BUY stop that would trigger immediately (-2010)."""
        self.campaign.mode = "live"
        self._red(1, 64750.0)
        self._red(2, 64700.0)
        self.engine._price_cache["BTCUSDT"] = (64755.0, 0)  # market above the trigger
        self.assertFalse(self.engine._stop_is_placeable(self.campaign, self.l2))
        self.engine._price_cache["BTCUSDT"] = (64740.0, 0)  # back under it
        self.assertTrue(self.engine._stop_is_placeable(self.campaign, self.l2))

    def test_process_candle_drives_the_stop_machinery(self):
        """The wiring: real candles through _process_candle must arm the stop."""
        for idx, close in ((1, 64750.0), (2, 64700.0)):
            _feed(self.engine, self.campaign, Candle(idx * 300, close + 40, close + 42, close - 8, close))
        self.assertTrue(self.l2.armed)
        self.assertAlmostEqual(self.l2.stop_price, 64750.0)


class CascadeAutoRestartTests(unittest.TestCase):
    """A mother break rolls into a fresh campaign anchored on the breaking
    candle — nothing carried over, same rules, no manual step."""

    def setUp(self):
        self.engine = _mk_engine()
        self.parent = Campaign(
            campaign_id="p1",
            symbol="BTCUSDT",
            capital_usd=2000.0,
            mother_high=65068.0,
            mother_low=64934.0,
            mother_timestamp=0,
            seq=1,
            mode="paper",
            min_notional_usd=5.0,
            tick_size=0.01,
        )
        self.engine.campaigns["p1"] = self.parent

    def _break(self, high=65200.0, low=65050.0, ts=3000):
        _feed(self.engine, self.parent, Candle(ts, 65100.0, high, low, 65180.0))

    def _child(self):
        return next((c for c in self.engine.campaigns.values() if c.campaign_id != "p1"), None)

    def test_break_starts_a_new_campaign_on_the_breaking_candle(self):
        self._break(high=65200.0, low=65050.0)
        self.assertEqual(self.parent.state, "MOTHER_BROKEN")
        child = self._child()
        self.assertIsNotNone(child)
        self.assertAlmostEqual(child.mother_high, 65200.0)  # the breaking candle's own high
        self.assertAlmostEqual(child.mother_low, 65050.0)  # and its low
        self.assertEqual(child.mother_timestamp, 3000)
        self.assertEqual(child.state, "WAITING_FIRST_DEPTH")

    def test_nothing_is_carried_over(self):
        self.parent.legs.append(Leg(leg_id=1, trendline_id=1, low=1.0, touch_high=2.0, touch_timestamp=0))
        self.parent.carry_forward_usd = 123.0
        self.parent.cumulative_used_pct = 0.9
        self._break()
        child = self._child()
        self.assertEqual(child.legs, [])
        self.assertEqual(child.trendlines, [])
        self.assertEqual(child.all_fills, [])
        self.assertEqual(child.rounds, [])
        self.assertAlmostEqual(child.carry_forward_usd, 0.0)
        self.assertAlmostEqual(child.cumulative_used_pct, 0.0)
        self.assertIsNone(child.avg_entry_price)

    def test_the_child_keeps_symbol_capital_and_mode_and_links_back(self):
        self.parent.mode = "live"
        self._break()
        child = self._child()
        self.assertEqual(child.symbol, "BTCUSDT")
        self.assertAlmostEqual(child.capital_usd, 2000.0)
        self.assertEqual(child.mode, "live")  # a live cascade keeps running live
        self.assertEqual(child.parent_campaign_id, "p1")
        self.assertEqual(child.generation, 2)
        self.assertGreater(child.seq, self.parent.seq)

    def test_the_parent_still_reaches_closed_history(self):
        self._break()
        self.assertEqual([r["campaign_id"] for r in self.engine.closed_campaigns], ["p1"])

    def test_a_deliberate_stop_does_not_auto_restart(self):
        self.parent.close_reason = "stopped"
        self.engine._auto_restart(self.parent, Candle(3000, 1.0, 2.0, 0.5, 1.5))
        self.assertIsNone(self._child())

    def test_a_barren_chain_is_cut_off(self):
        """A straight rip upward breaks a mother candle every bar. Restarts that
        never draw a fib must not multiply without end."""
        from engine.cascade import MAX_BARREN_AUTO_RESTARTS

        parent = self.parent
        for _ in range(MAX_BARREN_AUTO_RESTARTS + 5):
            parent.close_reason = "mother_broken"
            child = self.engine._auto_restart(parent, Candle(3000, 1.0, 2.0, 0.5, 1.5))
            if child is None:
                break
            parent = child
        self.assertIsNone(child)
        self.assertLessEqual(parent.generation, MAX_BARREN_AUTO_RESTARTS + 2)

    def test_a_chain_that_traded_resets_the_barren_counter(self):
        self._break()
        child = self._child()
        self.assertEqual(child.barren_chain, 1)
        child.legs.append(Leg(leg_id=1, trendline_id=1, low=1.0, touch_high=2.0, touch_timestamp=0))
        child.close_reason = "mother_broken"
        grandchild = self.engine._auto_restart(child, Candle(6000, 1.0, 2.0, 0.5, 1.5))
        self.assertEqual(grandchild.barren_chain, 0)


class CascadeDuplicateTests(unittest.IsolatedAsyncioTestCase):
    """Two campaigns on the same symbol and the same mother candle would draw
    the same structure and place the same orders twice."""

    def setUp(self):
        self.broker = FakeCascadeBroker()
        self.engine = _mk_engine(self.broker)
        self.args = dict(
            symbol="BTCUSDT",
            capital_usd=2000.0,
            mother_high=66411.29,
            mother_low=66200.00,
            mother_timestamp=_RECENT_TS,
        )

    async def test_the_same_mother_candle_cannot_be_started_twice(self):
        first = await self.engine.start_campaign(**self.args)
        self.assertNotIn("error", first)
        second = await self.engine.start_campaign(**self.args)
        self.assertIn("error", second)
        self.assertIn("already running", second["error"])
        self.assertEqual(len(self.engine.campaigns), 1)

    async def test_a_different_mother_candle_is_allowed(self):
        await self.engine.start_campaign(**self.args)
        other = dict(self.args, mother_high=66500.00, mother_timestamp=_RECENT_TS - 300)
        second = await self.engine.start_campaign(**other)
        self.assertNotIn("error", second)
        self.assertEqual(len(self.engine.campaigns), 2)

    async def test_the_slot_frees_up_once_the_first_is_stopped(self):
        first = await self.engine.start_campaign(**self.args)
        await self.engine.stop_campaign(first["campaign"]["campaign_id"], cancel_orders=False)
        again = await self.engine.start_campaign(**self.args)
        self.assertNotIn("error", again)

    def test_auto_restart_will_not_duplicate_a_running_campaign(self):
        parent = Campaign(
            campaign_id="p",
            symbol="BTCUSDT",
            capital_usd=2000.0,
            mother_high=65068.0,
            mother_low=64934.0,
            mother_timestamp=0,
            seq=1,
        )
        parent.close_reason = "mother_broken"
        self.engine.campaigns["p"] = parent
        candle = Candle(3000, 65100.0, 65200.0, 65050.0, 65180.0)
        self.assertIsNotNone(self.engine._auto_restart(parent, candle))
        self.assertIsNone(self.engine._auto_restart(parent, candle))  # same candle again
        self.assertEqual(len(self.engine.campaigns), 2)


class CascadeFibSizeTests(unittest.TestCase):
    """A fib may form anywhere relative to the mother candle — above its low is
    fine. What disqualifies a structure is being too small to be one."""

    def setUp(self):
        self.engine = _mk_engine()
        self.campaign = _real_campaign(self.engine)

    def _first_fib_with_mother_low(self, mother_low):
        """Replay 2026-07-20 00:15 with the mother candle's low moved, and
        return the first fib drawn."""
        engine = _mk_engine()
        campaign = Campaign(
            campaign_id="ml",
            symbol="BTCUSDT",
            capital_usd=2000.0,
            mother_high=_REAL[0][2],
            mother_low=mother_low,
            mother_timestamp=0,
            mode="paper",
            min_notional_usd=5.0,
        )
        engine.campaigns["ml"] = campaign
        for idx, o, h, low, c in _REAL[1:]:
            if idx > 6:
                break
            _feed(engine, campaign, Candle(idx * 300, o, h, low, c))
        return campaign.legs[0] if campaign.legs else None

    def test_the_mother_candle_low_does_not_gate_a_structure(self):
        """The dip is 64,790.01. Move the mother low ABOVE it and the same fib
        must still be drawn — a structure may sit anywhere relative to the
        mother candle's range."""
        real = self._first_fib_with_mother_low(65002.00)  # the true low, dip below it
        raised = self._first_fib_with_mother_low(64700.00)  # dip now ABOVE the low
        self.assertIsNotNone(raised)
        self.assertAlmostEqual(raised.touch_high, 64928.00)
        self.assertAlmostEqual(raised.low, 64790.01)
        self.assertAlmostEqual(raised.touch_high, real.touch_high)
        self.assertAlmostEqual(raised.low, real.low)

    def test_a_few_ticks_of_chop_is_not_a_structure(self):
        """2026-07-20 18:10 opens with two bars 15 points apart. That is 0.023%
        — its level 2 would sit 30 points down, which is noise, not a fib."""
        engine = _mk_engine()
        mother = _REAL3[0]
        campaign = Campaign(
            campaign_id="chop",
            symbol="BTCUSDT",
            capital_usd=2000.0,
            mother_high=mother[2],
            mother_low=mother[3],
            mother_timestamp=0,
            mode="paper",
            min_notional_usd=5.0,
        )
        engine.campaigns["chop"] = campaign
        for idx, o, h, low, c in _REAL3[1:]:
            if idx > 3:
                break
            _feed(engine, campaign, Candle(idx * 300, o, h, low, c))
        self.assertEqual(campaign.legs, [])

    def test_the_real_structure_that_day_still_forms(self):
        engine = _mk_engine()
        mother = _REAL3[0]
        campaign = Campaign(
            campaign_id="real3",
            symbol="BTCUSDT",
            capital_usd=2000.0,
            mother_high=mother[2],
            mother_low=mother[3],
            mother_timestamp=0,
            mode="paper",
            min_notional_usd=5.0,
        )
        engine.campaigns["real3"] = campaign
        for idx, o, h, low, c in _REAL3[1:]:
            _feed(engine, campaign, Candle(idx * 300, o, h, low, c))
        self.assertEqual(len(campaign.legs), 1)
        self.assertAlmostEqual(campaign.legs[0].touch_high, 65246.00)
        self.assertAlmostEqual(campaign.legs[0].low, 65160.00)


class CascadeMotherRetestTests(unittest.TestCase):
    """A rise back to just under the mother high leaves no room for a trendline,
    so that candle takes over as the mother candle."""

    def setUp(self):
        self.engine = _mk_engine()
        self.campaign = Campaign(
            campaign_id="r1",
            symbol="BTCUSDT",
            capital_usd=2000.0,
            mother_high=66354.0,
            mother_low=66200.0,
            mother_timestamp=0,
            seq=1,
            mode="paper",
            min_notional_usd=5.0,
            tick_size=0.01,
        )
        self.engine.campaigns["r1"] = self.campaign

    def _child(self):
        return next((c for c in self.engine.campaigns.values() if c.campaign_id != "r1"), None)

    def test_the_bar_after_the_mother_does_not_count_as_a_retest(self):
        """Price has not left the mother candle's range yet, so a high right
        under the mother high is just the mother candle's own pullback."""
        _feed(self.engine, self.campaign, Candle(300, 66340.0, 66350.0, 66280.0, 66300.0))
        self.assertEqual(self.campaign.state, "WAITING_FIRST_DEPTH")
        self.assertIsNone(self._child())

    def test_a_rise_back_to_the_mother_high_restarts_on_that_candle(self):
        _feed(self.engine, self.campaign, Candle(300, 66300.0, 66310.0, 66000.0, 66050.0))
        self.assertTrue(self.campaign.left_mother_range)
        _feed(self.engine, self.campaign, Candle(600, 66200.0, 66340.0, 66190.0, 66330.0))
        self.assertEqual(self.campaign.state, "COMPLETED")
        self.assertEqual(self.campaign.close_reason, "mother_retested")
        child = self._child()
        self.assertIsNotNone(child)
        self.assertAlmostEqual(child.mother_high, 66340.0)
        self.assertAlmostEqual(child.mother_low, 66190.0)
        self.assertEqual(child.parent_campaign_id, "r1")

    def test_a_rise_that_stays_clear_of_the_mother_high_is_left_alone(self):
        _feed(self.engine, self.campaign, Candle(300, 66300.0, 66310.0, 66000.0, 66050.0))
        # 66,200 is 0.23% under the mother high — a real trendline still fits.
        _feed(self.engine, self.campaign, Candle(600, 66150.0, 66200.0, 66120.0, 66180.0))
        self.assertEqual(self.campaign.state, "WAITING_FIRST_DEPTH")
        self.assertIsNone(self._child())

    def test_a_bar_straddling_the_mother_does_not_arm_the_retest(self):
        """The killer on fast timeframes. A 1m mother candle is a few ticks
        tall, so the very next bar dips under its low and the one after wicks
        back near its high — which used to end the campaign on candle two,
        before any structure could form. A shallow dip is not a departure."""
        _feed(self.engine, self.campaign, Candle(300, 66300.0, 66310.0, 66190.0, 66250.0))
        self.assertFalse(self.campaign.left_mother_range)
        _feed(self.engine, self.campaign, Candle(600, 66250.0, 66340.0, 66240.0, 66330.0))
        self.assertEqual(self.campaign.state, "WAITING_FIRST_DEPTH")
        self.assertIsNone(self._child())

    def test_a_double_top_at_the_exact_mother_high_is_not_a_break(self):
        """Two bars printing the identical high is a double top: the ceiling
        held. Only a high strictly above it ends the campaign."""
        _feed(self.engine, self.campaign, Candle(300, 66300.0, 66354.0, 66280.0, 66300.0))
        self.assertEqual(self.campaign.state, "WAITING_FIRST_DEPTH")
        _feed(self.engine, self.campaign, Candle(600, 66300.0, 66354.01, 66280.0, 66300.0))
        self.assertEqual(self.campaign.state, "MOTHER_BROKEN")

    def test_breaking_above_still_counts_as_a_break_not_a_retest(self):
        _feed(self.engine, self.campaign, Candle(300, 66300.0, 66310.0, 66000.0, 66050.0))
        _feed(self.engine, self.campaign, Candle(600, 66200.0, 66400.0, 66190.0, 66380.0))
        self.assertEqual(self.campaign.state, "MOTHER_BROKEN")
        self.assertEqual(self.campaign.close_reason, "mother_broken")


class CascadeAlertTests(unittest.TestCase):
    """The watchdogs exist to fire while nobody is watching the screen."""

    def setUp(self):
        self.sent = []
        self.engine = _mk_engine()
        self.engine.on_alert = lambda t, b, lvl: self.sent.append((t, lvl))

    def _campaign(self, cid, mode="paper"):
        c = Campaign(
            campaign_id=cid,
            symbol="BTCUSDT",
            capital_usd=2000.0,
            mother_high=65068.0,
            mother_low=64934.0,
            mother_timestamp=0,
            mode=mode,
        )
        self.engine.campaigns[cid] = c
        return c

    def test_alerts_once_the_campaign_count_passes_the_cap(self):
        from engine.cascade import MAX_ACTIVE_BEFORE_ALERT

        for i in range(MAX_ACTIVE_BEFORE_ALERT):
            self._campaign(f"c{i}")
        self.engine._check_watchdogs()
        self.assertEqual(self.sent, [])
        self._campaign("one-too-many")
        self.engine._check_watchdogs()
        self.assertEqual([t for t, _ in self.sent], ["Cascade campaign count high"])

    def test_the_count_alert_does_not_repeat_every_tick(self):
        from engine.cascade import MAX_ACTIVE_BEFORE_ALERT

        for i in range(MAX_ACTIVE_BEFORE_ALERT + 1):
            self._campaign(f"c{i}")
        for _ in range(5):
            self.engine._check_watchdogs()
        self.assertEqual(len(self.sent), 1)

    def test_alerts_when_candles_stop_being_processed(self):
        import time as _t

        from engine.cascade import STALL_ALERT_SEC

        self._campaign("c1")
        self.engine._last_candle_ts = _t.monotonic() - (STALL_ALERT_SEC + 60)
        self.engine._check_watchdogs()
        self.assertIn("Cascade engine STALLED", [t for t, _ in self.sent])

    def test_no_stall_alert_when_nothing_is_running(self):
        import time as _t

        from engine.cascade import STALL_ALERT_SEC

        self.engine._last_candle_ts = _t.monotonic() - (STALL_ALERT_SEC + 60)
        self.engine._check_watchdogs()
        self.assertEqual(self.sent, [])

    def test_an_auto_restart_raises_an_alert(self):
        parent = self._campaign("p", mode="live")
        parent.close_reason = "mother_broken"
        self.engine._auto_restart(parent, Candle(3000, 1.0, 2.0, 0.5, 1.5))
        titles = [t for t, _ in self.sent]
        self.assertIn("Cascade auto-restarted", titles)
        self.assertEqual(dict((t, lvl) for t, lvl in self.sent)["Cascade auto-restarted"], "warn")

    def test_a_missing_alert_hook_is_harmless(self):
        self.engine.on_alert = None
        self._campaign("c1")
        self.engine._check_watchdogs()  # must not raise


class CascadeClosedHistoryTests(unittest.TestCase):
    """A campaign that ended holding a position used to skip archiving, so it
    stayed in the live set and never reached history — which is why Avg Entry
    and Exit were blank in the closed table. The campaign itself was persisted
    intact, so those are recoverable."""

    def setUp(self):
        self.engine = _mk_engine()

    def _orphan(self):
        from engine.cascade import Round

        campaign = Campaign(
            campaign_id="orphan1",
            symbol="BTCUSDT",
            capital_usd=2000.0,
            mother_high=65068.0,
            mother_low=64934.0,
            mother_timestamp=0,
            seq=1,
            mode="paper",
        )
        campaign.state = "MOTHER_BROKEN"
        campaign.close_reason = "mother_broken"
        campaign.rounds = [
            Round(
                round_id=1,
                leg_id=2,
                avg_entry=64138.25,
                quantity=0.00011232,
                invested_usd=7.20,
                exit_price=64370.69,
                pnl=0.0261,
            )
        ]
        return campaign.to_dict()

    def test_restore_adopts_ended_campaigns_that_never_archived(self):
        self.engine.restore_campaigns([self._orphan()])
        self.assertEqual(len(self.engine.closed_campaigns), 1)
        row = self.engine.closed_campaigns[0]
        self.assertEqual(row["close_reason"], "mother_broken")
        # The entry/exit the closed table reads come from the rounds.
        self.assertEqual(len(row["rounds"]), 1)
        self.assertAlmostEqual(row["rounds"][0]["avg_entry"], 64138.25)
        self.assertAlmostEqual(row["rounds"][0]["exit_price"], 64370.69)

    def test_backfill_is_idempotent(self):
        snapshot = self._orphan()
        self.engine.restore_campaigns([snapshot])
        self.engine.restore_campaigns([snapshot])
        self.assertEqual(len(self.engine.closed_campaigns), 1)

    def test_live_campaigns_are_not_adopted(self):
        campaign = _mk_campaign(self.engine)
        campaign.state = "TRENDLINE_ACTIVE"
        self.engine.restore_campaigns([campaign.to_dict()])
        self.assertEqual(self.engine.closed_campaigns, [])

    def test_sequence_numbers_are_not_reused_after_delete(self):
        a = self.engine.campaigns
        first = Campaign(
            campaign_id="a",
            symbol="BTCUSDT",
            capital_usd=2000.0,
            mother_high=100.0,
            mother_low=99.0,
            mother_timestamp=0,
            seq=1,
        )
        a["a"] = first
        self.assertEqual(self.engine._next_seq(), 2)
        first.state = "STOPPED"
        self.engine.delete_campaign("a")
        # The number stays claimed by the archived campaign.
        self.assertEqual(self.engine._next_seq(), 2)


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
        # L2/L4 rest nowhere until two reds print under their line. Arm them at
        # the line itself so these tests stay about the sync mechanics and keep
        # their original prices; arming is covered by CascadeStopEntryTests.
        for order in leg.pending_orders.values():
            if order.entry_style == "stop":
                order.stop_price = order.price
                order.limit_price = order.price
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

    async def test_stop_levels_go_out_as_stop_limits_and_l8_as_a_plain_limit(self):
        """The live-money shape: L2/L4 must reach Binance as STOP_LOSS_LIMIT
        with a stopPrice, not as resting limits, or they would buy the fall."""
        self.leg.pending_orders[2].stop_price = 96.0
        self.leg.pending_orders[2].limit_price = 96.05
        await self.engine._sync_live_orders(self.campaign)
        by_client = {o["client_order_id"]: o for o in self.broker.placed_orders if o["side"] == "buy"}
        l2 = next(o for cid, o in by_client.items() if cid.endswith("-1-2-0"))
        l8 = next(o for cid, o in by_client.items() if cid.endswith("-1-8-0"))
        self.assertEqual(l2["order_type"], "stop_limit")
        self.assertAlmostEqual(l2["stop_price"], 96.0)
        self.assertAlmostEqual(l2["limit_price"], 96.05)
        self.assertEqual(l8["order_type"], "limit_order")
        self.assertIsNone(l8["stop_price"])

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

    async def test_tp_fill_closes_the_round_and_keeps_the_campaign_running(self):
        await self.engine._sync_live_orders(self.campaign)
        filled = self.leg.pending_orders[2]
        self.broker.order_lookup[str(filled.order_id)] = {
            "status": "FILLED",
            "executedQty": str(filled.quantity),
            "cummulativeQuoteQty": str(filled.quantity * 97.0),
        }
        await self.engine._sync_live_orders(self.campaign)
        qty_before = self.campaign.filled_base_qty
        tp_id = self.campaign.tp_order_id
        self.broker.order_lookup[str(tp_id)] = {
            "status": "FILLED",
            "executedQty": str(qty_before),
            "cummulativeQuoteQty": str(qty_before * 99.0),
        }
        await self.engine._sync_live_orders(self.campaign)

        # The campaign lives on — only a mother-high breach ends it.
        self.assertEqual(self.campaign.state, "TRENDLINE_ACTIVE")
        self.assertEqual(len(self.campaign.rounds), 1)
        self.assertAlmostEqual(self.campaign.rounds[0].pnl, (99.0 - 97.0) * qty_before, places=6)
        self.assertAlmostEqual(self.campaign.realized_pnl_total, (99.0 - 97.0) * qty_before, places=6)
        # Position is flat, so the principal is back in available capital.
        self.assertEqual(self.campaign.filled_base_qty, 0.0)
        self.assertAlmostEqual(self.campaign.spent_usd, 0.0)
        self.assertIsNone(self.campaign.avg_entry_price)
        # The filled level is spent; untouched levels keep resting.
        self.assertEqual(self.leg.pending_orders[2].status, "CLOSED")
        self.assertTrue(self.leg.pending_orders[8].is_open)

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


class CascadeRecalculateTests(unittest.IsolatedAsyncioTestCase):
    """Stored campaigns keep the geometry they were built with, so a campaign
    created under older rules keeps stale fibs until it is recalculated."""

    def _stale_campaign(self, engine):
        mother = _REAL[0]
        campaign = Campaign(
            campaign_id="stale",
            symbol="BTCUSDT",
            capital_usd=2000.0,
            mother_high=mother[2],
            mother_low=mother[3],
            mother_timestamp=0,
            mode="paper",
            min_notional_usd=5.0,
            model_version=0,
            state="COMPLETED",
        )
        leg = Leg(leg_id=1, trendline_id=1, low=64629.18, touch_high=64964.00, touch_timestamp=0)
        leg.fib = FibLadder(high_anchor=64964.00, low_anchor=64629.18)
        campaign.legs.append(leg)
        engine.campaigns[campaign.campaign_id] = campaign
        return campaign

    async def test_stale_campaign_is_flagged_and_recalculates(self):
        broker = FakeCascadeBroker()
        broker.candles_df = pd.DataFrame(
            {
                "open": [row[1] for row in _REAL],
                "high": [row[2] for row in _REAL],
                "low": [row[3] for row in _REAL],
                "close": [row[4] for row in _REAL],
            },
            index=pd.to_datetime([row[0] * 300 for row in _REAL], unit="s", utc=True),
        )
        engine = _mk_engine(broker)
        campaign = self._stale_campaign(engine)

        status = engine.get_status()["campaigns"][0]
        self.assertTrue(status["stale_model"])

        result = await engine.recalculate_campaign("stale")
        self.assertEqual(result["status"], "ok")
        self.assertFalse(engine.get_status()["campaigns"][0]["stale_model"])
        # replayed under the current rules -> both verified fibs
        self.assertEqual(len(campaign.legs), 2)
        self.assertAlmostEqual(campaign.legs[0].touch_high, 64928.00)
        self.assertAlmostEqual(campaign.legs[0].low, 64790.01)
        self.assertAlmostEqual(campaign.legs[1].touch_high, 64964.00)
        self.assertAlmostEqual(campaign.legs[1].low, 64416.00)

    async def test_live_campaign_with_fills_refuses_recalculation(self):
        engine = _mk_engine()
        campaign = self._stale_campaign(engine)
        campaign.mode = "live"
        campaign.all_fills.append(Fill(price=64700.0, quantity=0.001, level=2, leg_id=1, timestamp=1))
        result = await engine.recalculate_campaign("stale")
        self.assertIn("error", result)


class CascadeLiveTimingTests(unittest.IsolatedAsyncioTestCase):
    """Regressions for the live-path timing bugs found in the pre-live audit."""

    def setUp(self):
        self.broker = FakeCascadeBroker()
        self.engine = _mk_engine(self.broker)

    def test_sync_timestamps_are_tracked_per_campaign(self):
        """A single engine-level timestamp meant two live campaigns starved each
        other: whichever ticked first blocked the other for a whole interval."""
        a = _mk_campaign(self.engine, mode="live")
        a.campaign_id = "a"
        self.engine.campaigns["a"] = a
        b = Campaign(
            campaign_id="b",
            symbol="ETHUSDT",
            capital_usd=2000.0,
            mother_high=105.0,
            mother_low=99.0,
            mother_timestamp=0,
            mode="live",
            min_notional_usd=5.0,
        )
        self.engine.campaigns["b"] = b

        self.engine._last_sync_ts["a"] = 1234.0
        self.assertEqual(self.engine._last_sync_ts.get("b", 0.0), 0.0)
        self.assertIsInstance(self.engine._last_sync_ts, dict)

    def test_sync_interval_is_tight_enough_to_place_a_tp_promptly(self):
        self.assertLessEqual(self.engine._sync_interval_sec, 10.0)


class BinanceSignedRequestTests(unittest.TestCase):
    """The signed-request path is what stands between the engine and real money."""

    def setUp(self):
        from broker.binance import BinanceSpotClient

        self.client = BinanceSpotClient()

    def test_binance_error_body_is_surfaced_not_swallowed(self):
        class FakeResp:
            status_code = 400
            text = '{"code":-2010,"msg":"Account has insufficient balance."}'

            def json(self):
                return {"code": -2010, "msg": "Account has insufficient balance."}

        detail = self.client._binance_error_text(FakeResp())
        self.assertIn("-2010", detail)
        self.assertIn("insufficient balance", detail)

    def test_recv_window_and_offset_are_applied_to_signed_params(self):
        from broker import binance as binance_mod

        captured = {}

        def fake_request(method, url, *, headers=None, params=None, **kwargs):
            captured.update(params or {})

            class R:
                status_code = 200

                def json(self):
                    return {}

            return R()

        self.client.api_key = "k" * 12
        self.client.api_secret = "s" * 12
        self.client._time_offset_ms = 4321
        self.client._time_offset_ts = 9e18  # keep the cached offset, skip the network
        original = binance_mod._request_with_retry
        binance_mod._request_with_retry = fake_request
        try:
            self.client._signed_request("GET", "/api/v3/account")
        finally:
            binance_mod._request_with_retry = original

        self.assertEqual(captured.get("recvWindow"), binance_mod._RECV_WINDOW_MS)
        self.assertIn("signature", captured)
        self.assertGreater(captured.get("timestamp", 0), 0)
