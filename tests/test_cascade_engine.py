"""CascadeEngine tests: paper-mode state machine + live desired-state order sync."""

import time
import unittest

import pandas as pd

from engine.cascade import (
    ANCHOR_CLOSE_TOLERANCE_PCT,
    MIN_LEG_SEPARATION_PCT,
    Campaign,
    Candle,
    CascadeEngine,
    FibLadder,
    Fill,
    Leg,
    Round,
    build_fib_ladder_and_pool,
    compute_tp_price,
    find_valid_anchor2,
    ladders_overlap,
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

    def test_second_trendline_anchors_to_a_later_red_open_once_closes_have_slack(self):
        """Was 64,902.63 (candle #42) under zero tolerance. With
        ANCHOR_CLOSE_TOLERANCE_PCT the search reaches one swing further right to
        #45's open, 64,869.79 — also a red candle open, which is the rule.

        Nothing Phil has confirmed moves with it: both fibs on this fixture are
        byte-identical either way, and they are what places orders. The anchor
        itself was locked to engine behaviour here, not to one of his charts."""
        self._feed_real(59)
        self.assertEqual(len(self.campaign.trendlines), 2)
        self.assertAlmostEqual(self.campaign.trendlines[1].anchor2_price, 64869.79)
        self.assertAlmostEqual(self.campaign.legs[0].touch_high, 64928.00)
        self.assertAlmostEqual(self.campaign.legs[0].low, 64790.01)
        self.assertAlmostEqual(self.campaign.legs[1].touch_high, 64964.00)
        self.assertAlmostEqual(self.campaign.legs[1].low, 64416.00)

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
        # Every dollar of both pools is accounted for: still marked on a level,
        # collected into the running total, or already spent buying.
        self.assertAlmostEqual(
            self.campaign.resting_usd + self.campaign.pending_usd + self.campaign.spent_usd,
            leg1.pool_usd + leg2.pool_usd,
            places=1,
        )

    def test_a_closed_round_puts_its_principal_back_on_the_ladder(self):
        """A rung fills and the target hits. The principal is not handed to any
        particular fib — it goes back into the one pool, and the ladder is
        re-split so the rungs still waiting get their share of it."""
        self._feed_real(40)
        allocation = self.campaign.total_allocation_usd
        leg1 = self.campaign.legs[0]
        order = next(o for o in leg1.pending_orders.values() if o.usd_notional > 0)
        self.engine._record_fill(self.campaign, leg1, order, order.price, _RECENT_TS + 3600, order_id="PAPER")
        self.assertGreater(self.campaign.spent_usd, 0.0)

        self.engine._close_round(self.campaign, self.campaign.tp_price)
        self.assertEqual(len(self.campaign.rounds), 1)
        self.assertGreater(self.campaign.rounds[0].pnl, 0.0)
        self.assertAlmostEqual(self.campaign.spent_usd, 0.0)  # principal is back

        # The principal is back in the pool and the levels still waiting cover
        # the whole allocation between them again.
        self.assertLessEqual(self.campaign.resting_usd + self.campaign.pending_usd, allocation + 0.05)
        self.assertGreater(self.campaign.resting_usd + self.campaign.pending_usd, 0.0)

        # The cascade keeps running; a later fib joins the same ladder.
        self._feed_real(59)
        self.assertGreaterEqual(len(self.campaign.legs), 2)
        self.assertEqual(self.campaign.state, "TRENDLINE_ACTIVE")
        # The new low released the parked levels back onto the ladder, and every
        # dollar of both fibs' pools is marked on one level or another.
        self.assertIsNone(self.campaign.reuse_below)
        marked = sum(
            o.usd_notional
            for lg in self.campaign.legs
            for o in lg.pending_orders.values()
            if o.status in {"PENDING", "PLACED", "COLLECTED"}
        )
        self.assertAlmostEqual(marked, self.campaign.total_allocation_usd, places=1)

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

    def test_a_deeper_swing_off_the_same_high_is_not_the_same_shelf(self):
        """Real numbers from BTCUSDT campaign #36, 2026-07-21.

        The engine found 0=66,739.89 / 1=66,052.63 — both anchors Phil had
        drawn by hand — and discarded it because its high sat 0.010% from fib
        1's 66,746.68. But fib 1 spans 93 points and this one spans 687: its
        shallowest rung is 65,365 while fib 1's deepest is 65,997, so the two
        ladders share no price at all. Nothing could be split between them,
        which is the only thing the same-shelf rule exists to prevent."""
        fib1_high, fib1_low = 66746.68, 66653.05
        deep_high, deep_low = 66739.89, 66052.63

        # Close enough on the high alone that the old rule dropped it.
        self.assertLess(abs(deep_high - fib1_high) / fib1_high, MIN_LEG_SEPARATION_PCT)
        # The ladders do not touch: shallowest rung of one is below the
        # deepest rung of the other.
        self.assertLess(
            deep_high - 2 * (deep_high - deep_low),
            fib1_high - 8 * (fib1_high - fib1_low),
        )
        self.assertFalse(ladders_overlap(deep_high, deep_low, fib1_high, fib1_low))

    def test_a_genuine_duplicate_shelf_still_overlaps(self):
        """The SOL duplicate this rule was written for must stay caught: same
        high AND a comparable range, so the rungs interleave."""
        self.assertTrue(ladders_overlap(64763.67, 64502.00, 64753.77, 64599.89))
        # And a fib compared against itself is trivially the same shelf.
        self.assertTrue(ladders_overlap(64753.77, 64599.89, 64753.77, 64599.89))

    def test_degenerate_ranges_are_treated_as_the_same_shelf(self):
        """A zero or inverted range has no ladder to compare, so it must not
        fall through the overlap check and be admitted as a fresh structure."""
        self.assertTrue(ladders_overlap(100.0, 100.0, 100.0, 99.0))
        self.assertTrue(ladders_overlap(100.0, 101.0, 100.0, 99.0))

    def test_skipping_keeps_the_money_on_one_ladder(self):
        """A same-shelf third fib adds rungs a few ticks from ones already on
        the ladder, thinning the pool across near-duplicates instead of putting
        it to work. Skipping keeps the money on the rungs that matter."""
        self._feed(29)
        self.assertEqual(len(self.campaign.legs), 2)
        working = [
            o for leg in self.campaign.legs for o in leg.pending_orders.values() if o.is_open and o.usd_notional > 0
        ]
        self.assertTrue(working or self.campaign.all_fills, "the pool went somewhere")
        self.assertAlmostEqual(
            self.campaign.resting_usd + self.campaign.pending_usd + self.campaign.spent_usd,
            self.campaign.total_allocation_usd,
            places=1,
        )

    def test_the_fall_collects_the_levels_it_reaches(self):
        """Price falls through the shallow levels of both fibs, so their money
        joins the running total. Levels it never reached keep theirs."""
        self._feed(29)
        collected = [
            o for leg in self.campaign.legs for o in leg.pending_orders.values() if o.status in {"COLLECTED", "FILLED"}
        ]
        self.assertTrue(collected, "the fall should have reached at least one level")
        self.assertAlmostEqual(
            self.campaign.resting_usd + self.campaign.pending_usd + self.campaign.spent_usd,
            self.campaign.total_allocation_usd,
            places=1,
        )

    def test_mother_break_ends_the_campaign_flat_when_nothing_filled(self):
        """Same day, run to the mother break. The pool waited at level 8 and
        price never got there, so the campaign ends holding nothing."""
        self._feed(29)
        for idx, o, h, low, c in [
            (37, 64416.01, 64608.00, 64398.15, 64604.65),
            (38, 64604.65, 64800.00, 64540.00, 64800.00),
            (39, 64800.00, 64988.00, 64784.00, 64968.00),
            (40, 64967.99, 65100.00, 64898.01, 64994.12),
        ]:
            _feed(self.engine, self.campaign, Candle(idx * 300, o, h, low, c))
        self.assertEqual(self.campaign.state, "MOTHER_BROKEN")
        self.assertEqual(self.campaign.filled_base_qty, 0.0)
        self.assertEqual(self.campaign.realized_pnl_total, 0.0)


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

    @unittest.expectedFailure
    def test_matches_the_user_chart_exactly(self):
        """The verified target for this day, not yet reached.

        Phil's finalised chart puts BOTH anchors on the 00:45 candle: its high
        65,196.00 is fib 0 and its low 65,082.81 is fib 1. His levels solve back
        to exactly that pair (L2 64,969.62, L4 64,743.24), so it is not a
        reading error.

        This test used to assert 65,246.00 with the comment "00:45 IST high" —
        but 65,246.00 is the 00:40 candle's high. The comment named the right
        candle and the number came from the wrong one, so the reference we were
        defending was itself wrong.

        The engine currently touches at 00:40 and freezes fib 1 at the 00:30 low
        of 65,160.00, one candle early on both. Marked expected-failure rather
        than deleted: it is the target, and it should start passing when the
        touch detection and the ultimate-low rule are fixed together.
        """
        self._feed(16)
        leg = self.campaign.legs[0]
        self.assertAlmostEqual(leg.touch_high, 65196.00)  # 00:45 IST high
        self.assertAlmostEqual(leg.low, 65082.81)  # the SAME candle's low
        self.assertAlmostEqual(leg.fib.level_price(2), 64969.62)
        self.assertAlmostEqual(leg.fib.level_price(4), 64743.24)

    def test_the_dip_candle_high_is_not_its_own_touch(self):
        """The 00:30 candle both set the dip (65,160) and reached the line with a
        65,274.58 high. Its own high must not become fib 0 — the rise has to come
        after the dip, which is why fib 0 is the later 65,246."""
        self._feed(16)
        self.assertNotAlmostEqual(self.campaign.legs[0].touch_high, 65274.58)


class CascadeAccumulatorEntryTests(unittest.TestCase):
    """The running total and the one buy stop it arms.

    Levels are markers, not orders. Price reaching one adds its money to a pot;
    once the pot clears a rung the two-red-candle stop takes the whole lot on
    the turn.
    """

    def setUp(self):
        self.engine = _mk_engine()
        self.campaign = Campaign(
            campaign_id="acc1",
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
        # Round anchors: L2 = 64,800  L4 = 64,600  L8 = 64,200
        leg = Leg(leg_id=1, trendline_id=1, low=64900.0, touch_high=65000.0, touch_timestamp=0)
        leg.fib = FibLadder(high_anchor=65000.0, low_anchor=64900.0)
        leg.pool_usd = 300.0
        self.campaign.legs.append(leg)
        plan_leg_orders(self.campaign, leg)
        self.leg = leg
        self.l2 = leg.pending_orders[2]
        self.l4 = leg.pending_orders[4]
        self.l8 = leg.pending_orders[8]
        self.assertEqual([self.l2.price, self.l4.price, self.l8.price], [64800.0, 64600.0, 64200.0])
        # Every level keeps its own 20/30/50 share, however small.
        self.assertEqual([self.l2.usd_notional, self.l4.usd_notional, self.l8.usd_notional], [60.0, 90.0, 150.0])

    def _c(self, ts, o, h, low, c):
        return Candle(ts, o, h, low, c)

    def _feed(self, ts, o, h, low, c):
        _feed(self.engine, self.campaign, self._c(ts, o, h, low, c))

    def test_reaching_a_level_collects_its_money(self):
        self._feed(300, 64900.0, 64910.0, 64790.0, 64795.0)
        self.assertEqual(self.l2.status, "COLLECTED")
        self.assertAlmostEqual(self.campaign.pending_usd, 60.0)
        self.assertAlmostEqual(self.campaign.pending_line, 64800.0)

    def test_a_level_price_never_reached_collects_nothing(self):
        self._feed(300, 64900.0, 64910.0, 64850.0, 64860.0)
        self.assertEqual(self.l2.status, "PENDING")
        self.assertAlmostEqual(self.campaign.pending_usd, 0.0)
        self.assertIsNone(self.campaign.pending_line)

    def test_a_deeper_level_adds_to_the_same_total(self):
        self._feed(300, 64900.0, 64910.0, 64790.0, 64795.0)
        self._feed(600, 64795.0, 64796.0, 64590.0, 64595.0)
        self.assertEqual(self.l4.status, "COLLECTED")
        self.assertAlmostEqual(self.campaign.pending_usd, 150.0)  # 60 + 90, one pot

    def test_one_red_below_the_line_is_not_enough_to_arm(self):
        self._feed(300, 64900.0, 64910.0, 64790.0, 64795.0)
        self.assertIsNone(self.campaign.pending_stop_price)

    def test_the_second_red_sets_the_stop_at_the_previous_red_close(self):
        self._feed(300, 64900.0, 64910.0, 64790.0, 64795.0)
        self._feed(600, 64795.0, 64796.0, 64700.0, 64710.0)
        self.assertAlmostEqual(self.campaign.pending_stop_price, 64795.0)
        self.assertAlmostEqual(self.campaign.pending_limit_price, 64795.05)  # five ticks

    def test_the_fall_walks_the_stop_down_and_never_buys_into_it(self):
        self._feed(300, 64900.0, 64910.0, 64790.0, 64795.0)
        self._feed(600, 64795.0, 64796.0, 64700.0, 64710.0)
        self._feed(900, 64710.0, 64711.0, 64600.0, 64620.0)
        self.assertAlmostEqual(self.campaign.pending_stop_price, 64710.0)
        self.assertFalse(self.campaign.all_fills)

    def test_the_stop_never_moves_back_up(self):
        self._feed(300, 64900.0, 64910.0, 64790.0, 64795.0)
        self._feed(600, 64795.0, 64796.0, 64700.0, 64710.0)
        stop = self.campaign.pending_stop_price
        self._feed(900, 64760.0, 64765.0, 64740.0, 64750.0)  # red, but a higher close
        self.assertAlmostEqual(self.campaign.pending_stop_price, stop)

    def test_greens_are_ignored_entirely(self):
        self._feed(300, 64900.0, 64910.0, 64790.0, 64795.0)
        self._feed(600, 64700.0, 64790.0, 64690.0, 64780.0)  # green
        self.assertIsNone(self.campaign.pending_stop_price)

    def test_the_turn_buys_the_whole_accumulated_amount(self):
        self._feed(300, 64900.0, 64910.0, 64790.0, 64795.0)  # collects L2, $60
        self._feed(600, 64795.0, 64796.0, 64590.0, 64600.0)  # collects L4, $150 total; first red
        self._feed(900, 64600.0, 64601.0, 64500.0, 64520.0)  # second red arms the stop
        self.assertAlmostEqual(self.campaign.pending_usd, 150.0)
        stop = self.campaign.pending_stop_price
        self.assertIsNotNone(stop)
        self._feed(1200, 64520.0, stop + 50.0, 64510.0, stop + 40.0)  # the turn
        self.assertEqual(len(self.campaign.all_fills), 1)
        fill = self.campaign.all_fills[0]
        self.assertAlmostEqual(fill.price * fill.quantity, 150.0, places=2)
        # The pot is spent and reset, ready for the next fall.
        self.assertAlmostEqual(self.campaign.pending_usd, 0.0)
        self.assertIsNone(self.campaign.pending_stop_price)
        self.assertEqual(self.l2.status, "FILLED")
        self.assertEqual(self.l4.status, "FILLED")
        self.assertEqual(self.l8.status, "PENDING")  # never reached, still waiting

    def test_a_total_under_one_rung_is_held_not_bought(self):
        """A tiny fib: price falls through its level 2 but sixty cents is not an
        order, so nothing arms. The money stays on the clock for the next fall."""
        self.campaign.legs.clear()
        leg = Leg(leg_id=1, trendline_id=1, low=64900.0, touch_high=65000.0, touch_timestamp=0)
        leg.fib = FibLadder(high_anchor=65000.0, low_anchor=64900.0)
        leg.pool_usd = 3.0  # 20% of this is $0.60
        self.campaign.legs.append(leg)
        plan_leg_orders(self.campaign, leg)
        self._feed(300, 64900.0, 64910.0, 64790.0, 64795.0)
        self._feed(600, 64795.0, 64796.0, 64700.0, 64710.0)
        self.assertAlmostEqual(self.campaign.pending_usd, 0.60)
        self.assertIsNone(self.campaign.pending_line)
        self.assertIsNone(self.campaign.pending_stop_price)
        self.assertFalse(self.campaign.all_fills)

    def test_levels_from_different_fibs_at_the_same_price_combine(self):
        """The case behind the whole design: fib 2's level 8 and fib 3's level 4
        land a cent apart. Neither is placeable alone; together they are one
        order, and price crossing that point collects both."""
        self.campaign.legs.clear()
        self.campaign.pending_usd = 0.0
        f2 = Leg(leg_id=2, trendline_id=1, low=64900.0, touch_high=65000.0, touch_timestamp=0)
        f2.fib = FibLadder(high_anchor=65000.0, low_anchor=64950.0)  # L8 = 64,600
        f2.pool_usd = 6.0  # L8 share $3.00
        f3 = Leg(leg_id=3, trendline_id=2, low=64900.0, touch_high=64960.0, touch_timestamp=0)
        f3.fib = FibLadder(high_anchor=64960.0, low_anchor=64870.0)  # L4 = 64,600
        f3.pool_usd = 12.0  # L4 share $3.60
        self.campaign.legs.extend([f2, f3])
        plan_leg_orders(self.campaign, f2)
        plan_leg_orders(self.campaign, f3)
        self.assertAlmostEqual(f2.pending_orders[8].price, 64600.0)
        self.assertAlmostEqual(f3.pending_orders[4].price, 64600.0)

        self._feed(300, 64900.0, 64910.0, 64590.0, 64595.0)
        self.assertEqual(f2.pending_orders[8].status, "COLLECTED")
        self.assertEqual(f3.pending_orders[4].status, "COLLECTED")
        # $3.00 + $3.60 = $6.60, which clears the rung neither could reach alone.
        self.assertGreaterEqual(self.campaign.pending_usd, 5.5)
        self.assertIsNotNone(self.campaign.pending_line)


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
        self.leg = leg
        # A fall has collected two levels and two reds have armed the stop.
        # Arming itself is covered by CascadeAccumulatorEntryTests.
        self.campaign.pending_usd = 40.0
        self.campaign.collected = [[1, 2, 16.0, 97.0], [1, 4, 24.0, 96.0]]
        leg.pending_orders[2].status = "COLLECTED"
        leg.pending_orders[4].status = "COLLECTED"
        self.campaign.pending_line = 96.0
        self.campaign.pending_stop_price = 96.5
        self.campaign.pending_limit_price = 96.55
        self.campaign.pending_rev = 1

    async def test_the_accumulated_buy_goes_out_as_one_stop_limit(self):
        """The live-money shape: one STOP_LOSS_LIMIT covering everything the
        fall collected, never a resting limit that would buy into the fall."""
        changed = await self.engine._sync_live_orders(self.campaign)
        self.assertTrue(changed)
        buys = [o for o in self.broker.placed_orders if o["side"] == "buy"]
        self.assertEqual(len(buys), 1)
        self.assertEqual(buys[0]["order_type"], "stop_limit")
        self.assertAlmostEqual(buys[0]["stop_price"], 96.5)
        self.assertAlmostEqual(buys[0]["limit_price"], 96.55)
        self.assertAlmostEqual(buys[0]["size"], 40.0)
        self.assertIn("-buy-1", buys[0]["client_order_id"])
        self.assertIsNotNone(self.campaign.pending_order_id)

    async def test_an_unarmed_total_rests_nothing(self):
        """Collected money with no stop yet is not an order. Two reds have to
        print below the line first."""
        self.campaign.pending_stop_price = None
        self.campaign.pending_limit_price = None
        await self.engine._sync_live_orders(self.campaign)
        self.assertEqual([o for o in self.broker.placed_orders if o["side"] == "buy"], [])

    async def test_the_stop_is_placed_once_not_on_every_sync(self):
        await self.engine._sync_live_orders(self.campaign)
        self.broker.placed_orders.clear()
        await self.engine._sync_live_orders(self.campaign)
        self.assertEqual([o for o in self.broker.placed_orders if o["side"] == "buy"], [])

    async def test_exchange_fill_records_the_whole_amount_and_places_tp(self):
        await self.engine._sync_live_orders(self.campaign)
        order_id = self.campaign.pending_order_id
        qty = 40.0 / 97.0
        self.broker.order_lookup[str(order_id)] = {
            "status": "FILLED",
            "executedQty": str(qty),
            "cummulativeQuoteQty": str(qty * 97.0),
        }
        self.broker.placed_orders.clear()
        await self.engine._sync_live_orders(self.campaign)

        self.assertEqual(len(self.campaign.all_fills), 1)
        self.assertAlmostEqual(self.campaign.avg_entry_price, 97.0)
        # Both collected levels are marked bought, and the pot is reset.
        self.assertEqual(self.leg.pending_orders[2].status, "FILLED")
        self.assertEqual(self.leg.pending_orders[4].status, "FILLED")
        self.assertAlmostEqual(self.campaign.pending_usd, 0.0)
        sells = [o for o in self.broker.placed_orders if o["side"] == "sell"]
        self.assertEqual(len(sells), 1)
        # TP = 97 + 0.25*(105-97) = 99
        self.assertAlmostEqual(sells[0]["limit_price"], 99.0)
        self.assertAlmostEqual(sells[0]["base_qty"], self.campaign.filled_base_qty)
        self.assertIsNotNone(self.campaign.tp_order_id)

    async def test_a_partial_fill_is_booked_and_the_rest_keeps_working(self):
        """A stop-limit can execute in pieces. The part that traded is booked
        immediately — waiting for FILLED would leave real coins unaccounted and
        the target unplaced."""
        await self.engine._sync_live_orders(self.campaign)
        order_id = str(self.campaign.pending_order_id)
        half_qty = 20.0 / 97.0
        self.broker.open_orders = [
            {
                "orderId": int(order_id),
                "clientOrderId": f"cf-csc-{self.campaign.campaign_id}-buy-1",
                "executedQty": str(half_qty),
                "cummulativeQuoteQty": str(half_qty * 97.0),
            }
        ]
        await self.engine._sync_live_orders(self.campaign)

        self.assertEqual(len(self.campaign.all_fills), 1)
        self.assertAlmostEqual(self.campaign.pending_usd, 20.0, places=1)
        # The levels are not marked bought yet — half the money is still working.
        self.assertEqual(self.leg.pending_orders[2].status, "COLLECTED")
        self.assertIsNotNone(self.campaign.tp_price)

        # The rest executes; now everything settles.
        self.broker.open_orders = []
        self.broker.order_lookup[order_id] = {
            "status": "FILLED",
            "executedQty": str(40.0 / 97.0),
            "cummulativeQuoteQty": str(40.0),
        }
        await self.engine._sync_live_orders(self.campaign)
        self.assertAlmostEqual(self.campaign.pending_usd, 0.0)
        self.assertEqual(self.leg.pending_orders[2].status, "FILLED")

    async def test_an_externally_cancelled_stop_is_replaced(self):
        await self.engine._sync_live_orders(self.campaign)
        order_id = self.campaign.pending_order_id
        self.broker.order_lookup[str(order_id)] = {"status": "CANCELED"}
        self.broker.placed_orders.clear()
        await self.engine._sync_live_orders(self.campaign)
        buys = [o for o in self.broker.placed_orders if o["side"] == "buy"]
        self.assertEqual(len(buys), 1)
        self.assertIsNotNone(self.campaign.pending_order_id)

    async def test_tp_fill_closes_the_round_and_keeps_the_campaign_running(self):
        await self.engine._sync_live_orders(self.campaign)
        qty = 40.0 / 97.0
        self.broker.order_lookup[str(self.campaign.pending_order_id)] = {
            "status": "FILLED",
            "executedQty": str(qty),
            "cummulativeQuoteQty": str(qty * 97.0),
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


class CascadeClosedChartTests(unittest.IsolatedAsyncioTestCase):
    """The Chart button on the Closed Campaigns table."""

    def _engine_with_archived(self):
        engine = _mk_engine()
        campaign = Campaign(
            campaign_id="gone",
            symbol="BTCUSDT",
            capital_usd=2000.0,
            mother_high=100.0,
            mother_low=99.0,
            mother_timestamp=_RECENT_TS,
            seq=7,
        )
        campaign.state = "MOTHER_BROKEN"
        engine._archive_campaign(campaign)
        return engine

    async def test_an_ended_campaign_still_has_a_chart(self):
        """Archiving drops a campaign out of engine.campaigns, so a lookup that
        only checked there returned "not found" and the button did nothing."""
        engine = self._engine_with_archived()
        self.assertNotIn("gone", engine.campaigns)
        result = await engine.get_chart_data("gone")
        self.assertIsNone(result.get("error"))
        self.assertEqual(result["symbol"], "BTCUSDT")
        self.assertEqual(result["state"], "MOTHER_BROKEN")
        engine.stop()

    async def test_an_ended_campaign_cannot_be_stopped_or_restarted(self):
        """Reading history is fine; acting on it is not. Only the chart reaches
        into the closed list."""
        engine = self._engine_with_archived()
        self.assertIn("not found", (await engine.stop_campaign("gone")).get("error", ""))
        self.assertIn("not found", (await engine.recalculate_campaign("gone")).get("error", ""))
        engine.stop()


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

    async def _replayable(self, engine, broker):
        broker.candles_df = pd.DataFrame(
            {
                "open": [row[1] for row in _REAL],
                "high": [row[2] for row in _REAL],
                "low": [row[3] for row in _REAL],
                "close": [row[4] for row in _REAL],
            },
            index=pd.to_datetime([row[0] * 300 for row in _REAL], unit="s", utc=True),
        )
        mother = _REAL[0]
        campaign = Campaign(
            campaign_id="repeat",
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

    async def test_recalculating_twice_gives_the_same_campaign(self):
        """The pot is derived from the candles, so replaying the same candles
        must produce the same pot. It did not: `collected` and `pending_usd`
        were missing from the hand-written reset list, so every press re-added
        the same levels on top of the previous run's total — the user's $7.60
        fib showed $46.62 after six presses. Worse, once the inflated total
        cleared the rung it armed a buy stop for money no level had collected,
        and the very next replay booked a fill that never happened."""
        broker = FakeCascadeBroker()
        engine = _mk_engine(broker)
        campaign = await self._replayable(engine, broker)

        await engine.recalculate_campaign("repeat")
        first = (
            campaign.pending_usd,
            [list(row) for row in campaign.collected],
            campaign.pending_line,
            campaign.pending_stop_price,
            len(campaign.legs),
            len(campaign.all_fills),
            campaign.state,
        )
        self.assertGreater(first[0], 0, "test needs a campaign that actually collects")

        for _ in range(5):
            await engine.recalculate_campaign("repeat")
            self.assertEqual(
                (
                    campaign.pending_usd,
                    [list(row) for row in campaign.collected],
                    campaign.pending_line,
                    campaign.pending_stop_price,
                    len(campaign.legs),
                    len(campaign.all_fills),
                    campaign.state,
                ),
                first,
            )

    async def test_every_replay_derived_field_is_reset(self):
        """Guard the reset itself. Anything not explicitly kept has to come back
        to its default, so a field added to Campaign later cannot quietly start
        surviving replays the way `collected` did."""
        broker = FakeCascadeBroker()
        engine = _mk_engine(broker)
        campaign = await self._replayable(engine, broker)

        dirty = {
            "pending_usd": 46.62,
            "collected": [[1, 2, 46.62, 66559.42]],
            "pending_line": 65997.64,
            "pending_stop_price": 65967.40,
            "pending_limit_price": 65967.45,
            "pending_stop_ts": 123,
            "pending_last_red": 65984.0,
            "pending_order_id": "stale-order",
            "pending_filled_qty": 0.5,
            "reuse_below": 65000.0,
            "left_mother_range": True,
            "broken_above": True,
            "tp_order_id": "stale-tp",
            "cumulative_used_pct": 9.9,
        }
        for name, value in dirty.items():
            setattr(campaign, name, value)
        # Monotonic counters back the exchange's client-order-id memory, so
        # these must NOT rewind — a reused id collides with a live order.
        campaign.pending_rev = 7
        campaign.tp_rev = 3

        engine._reset_derived_state(campaign)

        for name in dirty:
            self.assertNotEqual(getattr(campaign, name), dirty[name], f"{name} survived the reset")
        self.assertEqual(campaign.pending_usd, 0.0)
        self.assertEqual(campaign.collected, [])
        self.assertIsNone(campaign.pending_order_id)
        self.assertEqual(campaign.pending_rev, 7)
        self.assertEqual(campaign.tp_rev, 3)
        self.assertEqual(campaign.window_start_ts, campaign.mother_timestamp)
        self.assertEqual(campaign.symbol, "BTCUSDT")
        self.assertEqual(campaign.capital_usd, 2000.0)

    async def test_live_recalc_cancels_resting_orders_first(self):
        """A live campaign can be carrying a buy stop for the pot the replay is
        about to erase. Left working, it is an order for money the campaign no
        longer believes it collected — and a fill lands on a position nothing
        is tracking."""
        broker = FakeCascadeBroker()
        engine = _mk_engine(broker)
        campaign = await self._replayable(engine, broker)
        campaign.mode = "live"
        campaign.pending_order_id = "9001"
        broker.open_orders = [
            {"orderId": "9001", "clientOrderId": f"cf-csc-{campaign.campaign_id}-buy-3"},
            {"orderId": "9002", "clientOrderId": "someone-elses-order"},
        ]

        await engine.recalculate_campaign("repeat")

        self.assertIn("9001", [str(row) for row in broker.cancelled])
        self.assertNotIn("9002", [str(row) for row in broker.cancelled])

    async def test_live_campaign_with_fills_refuses_recalculation(self):
        engine = _mk_engine()
        campaign = self._stale_campaign(engine)
        campaign.mode = "live"
        campaign.all_fills.append(Fill(price=64700.0, quantity=0.001, level=2, leg_id=1, timestamp=1))
        result = await engine.recalculate_campaign("stale")
        self.assertIn("error", result)


class CascadeLivePlacementTests(unittest.IsolatedAsyncioTestCase):
    """The live entry never reached Binance.

    _place_pending_stop read the cached market price with .get("price"), but
    _price_cache holds (price, monotonic) TUPLES. So every sync that had a
    cached price — all of them after the first tick — raised AttributeError.
    The tick's try/except swallowed it, so the symptom was silence: a live
    campaign collected level after level and never placed an order. Because
    the throw happened at step 2 of 3, it also skipped TP management entirely.
    """

    def _armed(self, engine, broker, stop=77.14):
        campaign = Campaign(
            campaign_id="sol10",
            symbol="SOLUSDT",
            capital_usd=2000.0,
            mother_high=78.88,
            mother_low=78.57,
            mother_timestamp=_RECENT_TS,
            mode="live",
            min_notional_usd=5.0,
            state="TRENDLINE_ACTIVE",
        )
        campaign.pending_usd = 14.61
        campaign.pending_stop_price = stop
        campaign.pending_limit_price = round(stop + 0.05, 2)
        engine.campaigns[campaign.campaign_id] = campaign
        return campaign

    async def test_a_cached_price_below_the_trigger_places_the_order(self):
        broker = FakeCascadeBroker()
        engine = _mk_engine(broker)
        campaign = self._armed(engine, broker)
        # Exactly the shape _get_price stores.
        engine._price_cache["SOLUSDT"] = (77.10, time.monotonic())

        placed = await engine._place_pending_stop(campaign)

        self.assertTrue(placed, "the buy stop never reached the exchange")
        self.assertEqual(len(broker.placed_orders), 1)
        order = broker.placed_orders[0]
        self.assertEqual(order["side"], "buy")
        self.assertEqual(order["order_type"], "stop_limit")
        self.assertAlmostEqual(order["stop_price"], 77.14)
        self.assertAlmostEqual(order["size"], 14.61)
        self.assertTrue(campaign.pending_order_id)

    async def test_a_market_at_the_trigger_holds_off_without_raising(self):
        """A buy stop must sit above the market. Declining is correct — but it
        has to decline, not explode."""
        broker = FakeCascadeBroker()
        engine = _mk_engine(broker)
        campaign = self._armed(engine, broker)
        engine._price_cache["SOLUSDT"] = (77.14, time.monotonic())

        self.assertFalse(await engine._place_pending_stop(campaign))
        self.assertEqual(broker.placed_orders, [])
        self.assertIsNone(campaign.pending_order_id)

    async def test_the_tp_still_syncs_when_the_entry_cannot_be_placed(self):
        """Step 2 raising must not take step 3 down with it: a campaign
        holding coin needs its exit resting whatever the entry is doing."""
        broker = FakeCascadeBroker()
        engine = _mk_engine(broker)
        campaign = self._armed(engine, broker)
        campaign.all_fills.append(Fill(price=77.50, quantity=0.1, level=8, leg_id=1, timestamp=_RECENT_TS))
        campaign.filled_base_qty = 0.1
        campaign.avg_entry_price = 77.50

        async def _boom(_campaign):
            raise RuntimeError("exchange unreachable")

        engine._place_pending_stop = _boom
        await engine._sync_live_orders(campaign)

        sells = [o for o in broker.placed_orders if str(o.get("side", "")).lower() == "sell"]
        self.assertEqual(len(sells), 1, "the take-profit was not placed")
        self.assertTrue(campaign.tp_order_id)


class CascadeOrderChurnTests(unittest.IsolatedAsyncioTestCase):
    """A live campaign placed the same buy stop every 10-15s for as long as it
    ran: place -> came back cancelled -> re-place, forever. Two faults fed it —
    a reused client id that Binance refused as a duplicate, and no ceiling on
    retrying a trigger that would not stay resting."""

    def _armed(self, engine, broker):
        campaign = Campaign(
            campaign_id="churn",
            symbol="SOLUSDT",
            capital_usd=2000.0,
            mother_high=78.88,
            mother_low=78.57,
            mother_timestamp=_RECENT_TS,
            mode="live",
            min_notional_usd=5.0,
            state="TRENDLINE_ACTIVE",
        )
        campaign.pending_usd = 14.61
        campaign.pending_stop_price = 77.14
        campaign.pending_limit_price = 77.16
        engine.campaigns[campaign.campaign_id] = campaign
        engine._price_cache["SOLUSDT"] = (77.10, time.monotonic())
        return campaign

    async def test_a_duplicate_rejection_adopts_the_resting_order(self):
        """-2010 means the order we wanted IS on the exchange under our own
        client id. Failing left the id unset, so the next sync sent the same id
        and was refused again — the loop had no exit."""
        broker = FakeCascadeBroker()
        engine = _mk_engine(broker)
        campaign = self._armed(engine, broker)
        client_id = f"cf-csc-{campaign.campaign_id}-buy-{campaign.pending_rev}"
        broker.open_orders = [{"orderId": "7788", "clientOrderId": client_id}]
        broker.place_order = lambda *a, **k: {"error": "Binance -2010: Duplicate order sent."}

        self.assertTrue(await engine._place_pending_stop(campaign))
        self.assertEqual(campaign.pending_order_id, "7788")

    async def test_a_duplicate_with_nothing_resting_is_still_a_failure(self):
        broker = FakeCascadeBroker()
        engine = _mk_engine(broker)
        campaign = self._armed(engine, broker)
        broker.open_orders = []
        broker.place_order = lambda *a, **k: {"error": "Binance -2010: Duplicate order sent."}

        self.assertFalse(await engine._place_pending_stop(campaign))
        self.assertIsNone(campaign.pending_order_id)

    async def test_the_same_trigger_is_not_retried_forever(self):
        broker = FakeCascadeBroker()
        engine = _mk_engine(broker)
        campaign = self._armed(engine, broker)
        attempts = []

        def _refuse(*a, **k):
            attempts.append(k.get("stop_price"))
            return {"error": "Binance -2011: Unknown order sent."}

        broker.place_order = _refuse
        for _ in range(10):
            await engine._place_pending_stop(campaign)

        self.assertLessEqual(len(attempts), 3, "the same trigger was retried without a ceiling")

    async def test_a_trigger_that_moves_is_never_throttled(self):
        """The stop walking down a real fall must keep going out — the brake
        is only for a trigger that will not stay resting."""
        broker = FakeCascadeBroker()
        engine = _mk_engine(broker)
        campaign = self._armed(engine, broker)
        attempts = []

        def _refuse(*a, **k):
            attempts.append(k.get("stop_price"))
            return {"error": "Binance -2011: Unknown order sent."}

        broker.place_order = _refuse
        for i in range(10):
            campaign.pending_stop_price = round(77.14 - i * 0.01, 2)
            campaign.pending_limit_price = round(campaign.pending_stop_price + 0.02, 2)
            engine._price_cache["SOLUSDT"] = (campaign.pending_stop_price - 0.05, time.monotonic())
            await engine._place_pending_stop(campaign)

        self.assertEqual(len(attempts), 10, "a walking stop was throttled")

    async def test_the_log_says_which_terminal_state_came_back(self):
        """CANCELED, EXPIRED and REJECTED mean different things; collapsing all
        three into 'cancelled' made the loop undiagnosable from the log."""
        broker = FakeCascadeBroker()
        engine = _mk_engine(broker)
        campaign = self._armed(engine, broker)
        campaign.pending_order_id = "9999"
        broker.open_orders = []
        broker.order_lookup = {"9999": {"status": "EXPIRED"}}

        await engine._sync_live_orders(campaign)

        messages = " ".join(e.get("message", "") for e in campaign.event_log)
        self.assertIn("EXPIRED", messages)
        # The same sync goes on to re-place, so the id is a NEW one rather than
        # None — that is the loop the brake above bounds, not a leak.
        self.assertNotEqual(campaign.pending_order_id, "9999")


class CascadeAnchorToleranceTests(unittest.TestCase):
    """The trendline anchor rejected a close sitting ONE CENT above the line.

    On SOLUSDT #10 that froze the anchor at 07-21 19:30 for the rest of the
    campaign: every later candidate — including the 07-22 06:20 red open at
    78.53, the swing top before the 11:30 candle broke the previous low — was
    thrown out by three closes 0.01 over, 0.013% of price. No fifth trendline
    could ever be drawn, even though it would sit 0.639% off the fourth, four
    times the separation needed to count as a distinct line.

    The tolerance is a measured band, not a free knob — see the sweep recorded
    on the constant. These bounds are what the confirmed anchors allow.
    """

    def test_the_tolerance_stays_inside_the_measured_band(self):
        self.assertGreaterEqual(
            ANCHOR_CLOSE_TOLERANCE_PCT,
            0.0004,
            "below 0.04% BTC #36 fib 3's dip drifts off Phil's confirmed 66,052.63",
        )
        self.assertLessEqual(
            ANCHOR_CLOSE_TOLERANCE_PCT,
            0.0005,
            "above 0.05% PAXG TL2 slides off Phil's confirmed 4,064.83 @ 16:10",
        )

    def test_a_one_cent_overshoot_no_longer_kills_an_anchor(self):
        """The exact shape that blocked SOL: a close a hair above the line."""
        mother_price, mother_ts = 78.88, 0
        # Candidate anchor: a red open at 78.53, two hours out.
        anchor_ts = 7200
        between = [
            Candle(timestamp=3600, open=78.80, high=78.80, low=78.60, close=78.71),
            # line is at 78.6175 here; this close sits 0.0125 over — 0.016% of
            # price, the same order as the three cents that blocked SOL.
            Candle(timestamp=5400, open=78.66, high=78.70, low=78.60, close=78.63),
            Candle(timestamp=anchor_ts, open=78.53, high=78.53, low=78.43, close=78.49),
        ]
        line_at_5400 = mother_price + ((78.53 - mother_price) / anchor_ts) * 5400
        self.assertAlmostEqual(line_at_5400, 78.6175, places=3)
        self.assertGreater(between[1].close, line_at_5400, "test needs a genuine overshoot")
        self.assertLess((between[1].close - line_at_5400) / line_at_5400, 0.0004, "overshoot must be inside the band")

        price, ts = find_valid_anchor2(mother_price, mother_ts, between)
        self.assertAlmostEqual(price, 78.53)
        self.assertEqual(ts, anchor_ts)

    def test_a_real_break_above_the_line_still_disqualifies(self):
        """Slack is for ticks, not for closes that genuinely broke the line."""
        mother_price, mother_ts = 78.88, 0
        anchor_ts = 7200
        between = [
            Candle(timestamp=3600, open=78.80, high=79.20, low=78.60, close=79.10),  # way above
            Candle(timestamp=anchor_ts, open=78.53, high=78.53, low=78.43, close=78.49),
        ]
        price, ts = find_valid_anchor2(mother_price, mother_ts, between)
        self.assertNotEqual(ts, anchor_ts, "an anchor whose line was truly broken was accepted")


class CascadeLotSizeResidueTests(unittest.IsolatedAsyncioTestCase):
    """Binance floors a sell to the symbol's LOT_SIZE step.

    BTCUSDT #36 bought 0.00011542 and could only offer 0.00011 — stranding
    0.00000542, which is 4.7% of a $7.60 position. Left alone it accumulates
    round after round, and the round's P&L claims coin that never left the
    account.
    """

    def _holding(self, engine, bought, residual=0.0):
        campaign = Campaign(
            campaign_id="lot",
            symbol="BTCUSDT",
            capital_usd=2000.0,
            mother_high=66928.49,
            mother_low=66823.36,
            mother_timestamp=_RECENT_TS,
            mode="live",
            min_notional_usd=5.0,
            state="TRENDLINE_ACTIVE",
        )
        campaign.all_fills.append(Fill(price=65844.03, quantity=bought, level=8, leg_id=1, timestamp=_RECENT_TS))
        campaign.filled_base_qty = bought
        campaign.residual_base_qty = residual
        campaign.avg_entry_price = 65844.03
        engine.campaigns[campaign.campaign_id] = campaign
        return campaign

    async def test_the_sell_offers_the_carried_residue_too(self):
        broker = FakeCascadeBroker()
        engine = _mk_engine(broker)
        campaign = self._holding(engine, bought=0.00011542, residual=0.00000542)
        await engine._sync_tp_order(campaign, {})
        sells = [o for o in broker.placed_orders if str(o.get("side", "")).lower() == "sell"]
        self.assertEqual(len(sells), 1)
        self.assertAlmostEqual(sells[0]["base_qty"], 0.00012084, places=8)

    async def test_an_unsold_remainder_is_carried_not_abandoned(self):
        broker = FakeCascadeBroker()
        engine = _mk_engine(broker)
        campaign = self._holding(engine, bought=0.00011542)
        campaign.tp_order_id = "7001"
        campaign.tp_price = 66109.86
        # The exchange filled only the whole lot steps it could.
        broker.order_lookup = {"7001": {"status": "FILLED", "executedQty": "0.00011", "cummulativeQuoteQty": "7.2721"}}
        await engine._sync_tp_order(campaign, {})

        self.assertEqual(len(campaign.rounds), 1)
        rnd = campaign.rounds[0]
        self.assertAlmostEqual(rnd.quantity, 0.00011, places=8)
        self.assertAlmostEqual(campaign.residual_base_qty, 0.00000542, places=8)

    async def test_the_round_books_what_sold_not_what_was_bought(self):
        """P&L must not claim coin that never left the account."""
        broker = FakeCascadeBroker()
        engine = _mk_engine(broker)
        campaign = self._holding(engine, bought=0.00011542)
        campaign.tp_order_id = "7002"
        campaign.tp_price = 66109.86
        broker.order_lookup = {"7002": {"status": "FILLED", "executedQty": "0.00011", "cummulativeQuoteQty": "7.2721"}}
        await engine._sync_tp_order(campaign, {})

        rnd = campaign.rounds[0]
        expected = round((rnd.exit_price - 65844.03) * 0.00011, 8)
        self.assertAlmostEqual(rnd.pnl, expected, places=8)
        self.assertLess(rnd.quantity, 0.00011542, "booked the bought quantity, not the sold one")

    async def test_residue_clears_once_it_reaches_a_whole_step(self):
        broker = FakeCascadeBroker()
        engine = _mk_engine(broker)
        campaign = self._holding(engine, bought=0.00011542, residual=0.00000542)
        campaign.tp_order_id = "7003"
        campaign.tp_price = 66109.86
        # 0.00012084 offered; the exchange can now take a full 0.00012.
        broker.order_lookup = {"7003": {"status": "FILLED", "executedQty": "0.00012", "cummulativeQuoteQty": "7.9332"}}
        await engine._sync_tp_order(campaign, {})
        self.assertAlmostEqual(campaign.residual_base_qty, 0.00000084, places=8)
        self.assertAlmostEqual(campaign.rounds[0].quantity, 0.00012, places=8)


class CascadeOrderIdTests(unittest.IsolatedAsyncioTestCase):
    """A placement reply with no order id must never read as success.

    `str(result.get("orderId") or result.get("id") or "")` stored an empty
    string, which is falsy — so the next sync believed nothing was resting,
    skipped the cancel, and placed ANOTHER order, while logging "placed" every
    time. On the take-profit that means several sell orders against one
    position, every one of them able to fill. Seen live on SOLUSDT: the same
    0.18937135 @ 77.58 sell going out every ten seconds.
    """

    def _holding(self, engine):
        campaign = Campaign(
            campaign_id="ids",
            symbol="SOLUSDT",
            capital_usd=2000.0,
            mother_high=78.88,
            mother_low=78.57,
            mother_timestamp=_RECENT_TS,
            mode="live",
            min_notional_usd=5.0,
            state="TRENDLINE_ACTIVE",
        )
        campaign.all_fills.append(Fill(price=77.23, quantity=0.18937135, level=8, leg_id=1, timestamp=_RECENT_TS))
        campaign.filled_base_qty = 0.18937135
        campaign.avg_entry_price = 77.23
        engine.campaigns[campaign.campaign_id] = campaign
        return campaign

    async def test_an_idless_reply_does_not_stack_a_second_sell(self):
        broker = FakeCascadeBroker()
        engine = _mk_engine(broker)
        campaign = self._holding(engine)
        # Accepted, but the reply carries neither orderId nor id — while the
        # order really does start resting, which is what makes the stacking
        # dangerous rather than merely noisy.
        sent = []

        def _idless(*a, **k):
            sent.append(k.get("client_order_id"))
            broker.open_orders.append({"orderId": f"e{len(sent)}", "clientOrderId": k.get("client_order_id")})
            return {"status": "NEW"}

        broker.place_order = _idless

        for _ in range(5):
            await engine._sync_tp_order(campaign, {str(o["orderId"]): o for o in broker.open_orders})

        self.assertEqual(len(sent), 1, f"stacked {len(sent)} sells for one position: {sent}")
        self.assertEqual(campaign.tp_order_id, "e1")

    async def test_an_idless_reply_is_recovered_by_client_id(self):
        broker = FakeCascadeBroker()
        engine = _mk_engine(broker)
        campaign = self._holding(engine)
        expected_client = f"cf-csc-{campaign.campaign_id}-tp-1"
        broker.open_orders = [{"orderId": "4242", "clientOrderId": expected_client}]
        broker.place_order = lambda *a, **k: {"status": "NEW"}

        await engine._sync_tp_order(campaign, {})

        self.assertEqual(campaign.tp_order_id, "4242")

    async def test_a_resting_tp_at_the_right_price_is_left_alone(self):
        """The loop's real cost was re-placing something already correct."""
        broker = FakeCascadeBroker()
        engine = _mk_engine(broker)
        campaign = self._holding(engine)

        await engine._sync_tp_order(campaign, {})
        first = campaign.tp_order_id
        self.assertTrue(first)
        placed_once = len(broker.placed_orders)

        for _ in range(4):
            await engine._sync_tp_order(campaign, {str(first): {"clientOrderId": "x"}})

        self.assertEqual(len(broker.placed_orders), placed_once, "re-placed a TP that was already resting")
        self.assertEqual(campaign.tp_order_id, first)

    def test_the_extractor_rejects_unusable_replies(self):
        self.assertEqual(CascadeEngine._order_id_from({"orderId": 55}), "55")
        self.assertEqual(CascadeEngine._order_id_from({"id": "abc"}), "abc")
        self.assertEqual(CascadeEngine._order_id_from({"status": "NEW"}), "")
        self.assertEqual(CascadeEngine._order_id_from({}), "")
        self.assertEqual(CascadeEngine._order_id_from(None), "")


class CascadePaperTpOnReplayTests(unittest.TestCase):
    """A replay must be able to CLOSE a paper round, not only open one.

    The live loop tests the TP against the last traded price. A candle replay
    has no such price, so nothing closed a round during a replay — and Recalc
    is a replay. Pressing it on a paper campaign therefore erased every closed
    round and handed back an open position that should have been sold: the SOL
    07-21 campaign replayed to a position still open, when its round had closed
    at 78.05 and 79 later candles had traded above the target.
    """

    def setUp(self):
        self.engine = _mk_engine()
        self.campaign = Campaign(
            campaign_id="tp",
            symbol="SOLUSDT",
            capital_usd=2000.0,
            mother_high=78.88,
            mother_low=78.57,
            mother_timestamp=_RECENT_TS,
            mode="paper",
            min_notional_usd=5.0,
            state="TRENDLINE_ACTIVE",
        )
        self.campaign.all_fills.append(
            Fill(price=77.77, quantity=0.09013759, level=4, leg_id=3, timestamp=_RECENT_TS + 300)
        )
        self.campaign.filled_base_qty = 0.09013759
        self.campaign.avg_entry_price = 77.77
        self.engine.campaigns["tp"] = self.campaign
        self.engine._candles_5m["tp"] = []
        # 77.77 + 25% of the way to the mother high 78.88
        self.tp = 78.0475

    def _candle(self, ts, high):
        return Candle(timestamp=ts, open=77.9, high=high, low=77.8, close=77.9)

    def test_a_candle_through_the_target_closes_the_round(self):
        self.assertAlmostEqual(compute_tp_price(self.campaign), self.tp, places=4)
        self.engine._paper_tp_check(self.campaign, self._candle(_RECENT_TS + 900, 78.06))
        self.assertEqual(len(self.campaign.rounds), 1)
        rnd = self.campaign.rounds[0]
        self.assertAlmostEqual(rnd.avg_entry, 77.77)
        self.assertAlmostEqual(rnd.exit_price, self.tp, places=4)
        self.assertGreater(rnd.pnl, 0)
        self.assertEqual(self.campaign.filled_base_qty, 0.0)

    def test_a_candle_short_of_the_target_leaves_it_open(self):
        self.engine._paper_tp_check(self.campaign, self._candle(_RECENT_TS + 900, 78.00))
        self.assertEqual(self.campaign.rounds, [])
        self.assertEqual(self.campaign.filled_base_qty, 0.09013759)

    def test_the_candle_that_bought_cannot_also_sell(self):
        """Tick order inside a candle is unknowable, so the pessimistic reading
        is that the target comes on a later candle."""
        self.engine._paper_tp_check(self.campaign, self._candle(_RECENT_TS + 300, 79.00))
        self.assertEqual(self.campaign.rounds, [])

    def test_a_flat_position_is_ignored(self):
        self.campaign.all_fills = []
        self.campaign.filled_base_qty = 0.0
        self.engine._paper_tp_check(self.campaign, self._candle(_RECENT_TS + 900, 99.0))
        self.assertEqual(self.campaign.rounds, [])


class CascadeStopCancelsEverythingTests(unittest.IsolatedAsyncioTestCase):
    """Stopping a live campaign has to leave nothing working on the exchange.

    The accumulated buy stop lives in campaign.pending_order_id, not in
    leg.pending_orders — those are collection markers that only reach "PLACED"
    through a legacy recovery path. Cancelling by marker status missed the one
    order that was actually resting, so Stop pulled the TP and left the buy
    stop live. It could still fill minutes later, buying coin for a campaign
    that had already been archived, with no TP to sell it again.
    """

    def _live_campaign(self, engine, broker):
        campaign = Campaign(
            campaign_id="stopme",
            symbol="BTCUSDT",
            capital_usd=2000.0,
            mother_high=66928.49,
            mother_low=66823.36,
            mother_timestamp=_RECENT_TS,
            mode="live",
            min_notional_usd=5.0,
            state="TRENDLINE_ACTIVE",
        )
        campaign.pending_usd = 7.60
        campaign.pending_order_id = "5001"
        campaign.pending_stop_price = 65871.82
        campaign.pending_limit_price = 65871.87
        broker.open_orders = [
            {"orderId": "5001", "clientOrderId": f"cf-csc-{campaign.campaign_id}-buy-2"},
            {"orderId": "5002", "clientOrderId": f"cf-csc-{campaign.campaign_id}-tp-1"},
            {"orderId": "5003", "clientOrderId": "unrelated-manual-order"},
        ]
        engine.campaigns[campaign.campaign_id] = campaign
        return campaign

    async def test_stop_cancels_the_accumulated_buy_stop(self):
        broker = FakeCascadeBroker()
        engine = _mk_engine(broker)
        campaign = self._live_campaign(engine, broker)
        campaign.tp_order_id = "5002"

        await engine.stop_campaign("stopme")

        self.assertIn("5001", broker.cancelled, "the working buy stop was left on the exchange")
        self.assertIn("5002", broker.cancelled)
        self.assertNotIn("5003", broker.cancelled, "cancelled an order that was not ours")
        self.assertIsNone(campaign.pending_order_id)
        self.assertIsNone(campaign.pending_stop_price)
        self.assertEqual(campaign.state, "STOPPED")

    async def test_stop_does_not_sell_the_position(self):
        """Stop cancels orders; it never liquidates. Coin already bought stays
        in the account, which is why the TP handling below matters."""
        broker = FakeCascadeBroker()
        engine = _mk_engine(broker)
        campaign = self._live_campaign(engine, broker)
        campaign.all_fills.append(Fill(price=66074.47, quantity=0.00011, level=8, leg_id=1, timestamp=1))
        campaign.filled_base_qty = 0.00011

        await engine.stop_campaign("stopme")

        sells = [o for o in broker.placed_orders if str(o.get("side", "")).lower() == "sell"]
        self.assertEqual(sells, [], "Stop must not place a sell")
        self.assertEqual(campaign.filled_base_qty, 0.00011, "the position is untouched")

    async def test_ending_while_holding_keeps_the_exit_but_pulls_the_buy(self):
        """When a campaign ends holding coin the TP is deliberately left
        resting, so the exit still happens. The buy stop must go regardless —
        otherwise an ended campaign keeps buying."""
        broker = FakeCascadeBroker()
        engine = _mk_engine(broker)
        campaign = self._live_campaign(engine, broker)
        campaign.tp_order_id = "5002"

        await engine._cancel_all_live_orders(campaign, include_tp=False)

        self.assertIn("5001", broker.cancelled)
        self.assertNotIn("5002", broker.cancelled, "the resting exit was cancelled")
        self.assertEqual(campaign.tp_order_id, "5002")


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


class CascadeRoundTradeLogTests(unittest.TestCase):
    """Closing a round flattens the position and clears campaign.all_fills, so
    the individual buys vanished the moment the TP landed — only the average
    survived. The round now snapshots them, because an average cannot tell you
    when each rung filled, what it cost, or which fib level it came from."""

    def setUp(self):
        self.engine = _mk_engine()
        self.campaign = Campaign(
            campaign_id="log1",
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
        leg = Leg(leg_id=3, trendline_id=1, low=64900.0, touch_high=65000.0, touch_timestamp=0)
        leg.fib = FibLadder(high_anchor=65000.0, low_anchor=64900.0)
        self.campaign.legs = [leg]
        self.engine.campaigns[self.campaign.campaign_id] = self.campaign
        self.campaign.all_fills = [
            Fill(price=64800.0, quantity=0.00005, level=2, leg_id=3, timestamp=1_700_000_600, order_id="b2"),
            Fill(price=64600.0, quantity=0.00006, level=4, leg_id=3, timestamp=1_700_000_300, order_id="b1"),
        ]
        self.campaign.filled_base_qty = 0.00011
        self.campaign.avg_entry_price = 64690.909

    def test_round_keeps_each_buy(self):
        self.engine._close_round(self.campaign, 65000.0)
        rnd = self.campaign.rounds[0]
        self.assertEqual(len(rnd.fills), 2)
        # Ordered by fill time, not by the order they happened to sit in memory.
        self.assertEqual([f["timestamp"] for f in rnd.fills], [1_700_000_300, 1_700_000_600])
        first = rnd.fills[0]
        self.assertEqual(first["level"], 4)
        self.assertEqual(first["leg_id"], 3)
        self.assertAlmostEqual(first["usd"], 64600.0 * 0.00006, places=8)
        self.assertEqual(rnd.opened_ts, 1_700_000_300)

    def test_log_survives_the_position_reset(self):
        """The clear-down that wipes all_fills must not reach into the round."""
        self.engine._close_round(self.campaign, 65000.0)
        self.assertEqual(self.campaign.all_fills, [])
        self.assertEqual(len(self.campaign.rounds[0].fills), 2)

    def test_log_round_trips_through_persistence(self):
        self.engine._close_round(self.campaign, 65000.0)
        restored = Campaign.from_dict(self.campaign.to_dict())
        self.assertEqual(len(restored.rounds[0].fills), 2)
        self.assertEqual(restored.rounds[0].fills[0]["level"], 4)
        self.assertEqual(restored.rounds[0].opened_ts, 1_700_000_300)

    def test_rounds_closed_before_this_change_still_load(self):
        """Existing persisted rounds have no fills key and must not break."""
        legacy = {
            "round_id": 1,
            "leg_id": 2,
            "avg_entry": 100.0,
            "quantity": 1.0,
            "invested_usd": 100.0,
            "exit_price": 110.0,
            "pnl": 10.0,
            "closed_at": "2026-07-20 10:00:00",
        }
        rnd = Round.from_dict(legacy)
        self.assertEqual(rnd.fills, [])
        self.assertEqual(rnd.opened_ts, 0)
        self.assertEqual(rnd.pnl, 10.0)
