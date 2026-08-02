"""Booking the commission the exchange charged, not the one we assume.

Cascade modelled fees at a hardcoded 0.1% a side. That is Binance's standard
rate, but it is not what Phil pays — with "pay fees with BNB" on, the real rate
is 0.075%, so every round closed after he enabled it was booked with a fee a
third larger than he was charged, understating net P&L. The model also cannot
see a VIP tier or a maker rebate, and would go stale again on any change to
either.

The real figure was already being fetched and converted by the broker layer and
then thrown away. These cover carrying it through to the round, and — just as
important — the cases where it is NOT available, which must fall back to the
model and say so rather than book a free trade.
"""

import asyncio
import unittest
from unittest.mock import patch

from broker.base import BaseBroker
from broker.binance import BinanceSpotClient
from engine.cascade import FEE_PCT_PER_SIDE, Campaign, Fill, Round, round_trip_fee


def _campaign(mode="live") -> Campaign:
    campaign = Campaign(
        campaign_id="fee-1",
        symbol="SOLUSDT",
        capital_usd=500.0,
        mother_high=80.0,
        mother_low=70.0,
        mother_timestamp=0,
        min_notional_usd=5.0,
    )
    campaign.mode = mode
    return campaign


class FillFeePersistenceTests(unittest.TestCase):
    def test_a_fee_survives_a_restart(self):
        fill = Fill(price=73.76, quantity=0.184, level=8, leg_id=1, timestamp=1, order_id="55", fee_usd=0.0102)
        self.assertAlmostEqual(Fill.from_dict(fill.to_dict()).fee_usd, 0.0102)

    def test_an_unpriced_fill_stays_unpriced_and_never_becomes_free(self):
        """The distinction the whole design rests on: absent is not zero."""
        restored = Fill.from_dict({"price": 73.0, "quantity": 0.2, "level": 8, "leg_id": 1, "timestamp": 1})
        self.assertIsNone(restored.fee_usd, "a fill written before fees were read must not restore as free")

    def test_an_explicit_zero_is_kept(self):
        # A genuinely free fill (a rebate, a promo) is a fact, not an absence.
        restored = Fill.from_dict(
            {"price": 73.0, "quantity": 0.2, "level": 8, "leg_id": 1, "timestamp": 1, "fee_usd": 0.0}
        )
        self.assertEqual(restored.fee_usd, 0.0)

    def test_a_round_written_before_this_restores_as_estimated(self):
        legacy = Round.from_dict(
            {
                "round_id": 1,
                "leg_id": 1,
                "avg_entry": 100.0,
                "quantity": 1.0,
                "invested_usd": 100.0,
                "exit_price": 110.0,
                "pnl": 9.79,
                "fees_usd": 0.21,
            }
        )
        self.assertTrue(legacy.fees_estimated)

    def test_a_measured_round_round_trips(self):
        rnd = Round(
            round_id=1,
            leg_id=1,
            avg_entry=100.0,
            quantity=1.0,
            invested_usd=100.0,
            exit_price=110.0,
            pnl=9.84,
            fees_usd=0.1575,
            pnl_gross=10.0,
            fees_estimated=False,
        )
        self.assertFalse(Round.from_dict(rnd.to_dict()).fees_estimated)


class CommissionLookupTests(unittest.TestCase):
    def _client(self):
        client = BinanceSpotClient.__new__(BinanceSpotClient)
        client.quote_asset = "USDT"
        client._fee_price_cache = {}
        client._FEE_PRICE_TTL = 300
        return client

    def test_an_order_that_filled_in_pieces_sums_its_trades(self):
        client = self._client()
        rows = [
            {"symbol": "SOLUSDT", "price": "73.76", "qty": "0.1", "commission": "0.0055", "commissionAsset": "USDT"},
            {"symbol": "SOLUSDT", "price": "73.80", "qty": "0.084", "commission": "0.0047", "commissionAsset": "USDT"},
        ]
        with (
            patch.object(BinanceSpotClient, "_is_configured", return_value=True),
            patch.object(BinanceSpotClient, "_signed_request", return_value=rows),
            patch.object(BinanceSpotClient, "to_broker_symbol", return_value="SOLUSDT"),
            patch.object(
                BinanceSpotClient, "get_product_by_symbol", return_value={"base_asset": "SOL", "quote_asset": "USDT"}
            ),
        ):
            self.assertAlmostEqual(client.get_order_commission("SOLUSDT", "55"), 0.0102, places=8)

    def test_a_bnb_paid_commission_is_converted(self):
        client = self._client()
        rows = [
            {"symbol": "SOLUSDT", "price": "73.76", "qty": "0.184", "commission": "0.00002", "commissionAsset": "BNB"}
        ]
        with (
            patch.object(BinanceSpotClient, "_is_configured", return_value=True),
            patch.object(BinanceSpotClient, "_signed_request", return_value=rows),
            patch.object(BinanceSpotClient, "to_broker_symbol", return_value="SOLUSDT"),
            patch.object(BinanceSpotClient, "_market_get", return_value={"price": "600.0"}),
            patch.object(
                BinanceSpotClient, "get_product_by_symbol", return_value={"base_asset": "SOL", "quote_asset": "USDT"}
            ),
        ):
            self.assertAlmostEqual(client.get_order_commission("SOLUSDT", "55"), 0.012, places=6)

    def test_unknown_is_none_not_zero(self):
        """Every failure mode. None means "ask the model"; 0.0 would mean free."""
        client = self._client()
        with patch.object(BinanceSpotClient, "_is_configured", return_value=True):
            with patch.object(BinanceSpotClient, "to_broker_symbol", return_value="SOLUSDT"):
                with patch.object(BinanceSpotClient, "_signed_request", return_value=[]):
                    self.assertIsNone(client.get_order_commission("SOLUSDT", "55"), "no trades returned")
                with patch.object(BinanceSpotClient, "_signed_request", side_effect=RuntimeError("rate limited")):
                    self.assertIsNone(client.get_order_commission("SOLUSDT", "55"), "lookup failed")
            # A paper marker is not an exchange order id.
            self.assertIsNone(client.get_order_commission("SOLUSDT", "PAPER"))
            self.assertIsNone(client.get_order_commission("SOLUSDT", None))

    def test_a_broker_that_cannot_report_one_says_so(self):
        self.assertIsNone(BaseBroker().get_order_commission("SOLUSDT", "55"))


class _FeeBroker:
    """Reports a fixed commission per order id."""

    def __init__(self, fees):
        self.fees = fees
        self.calls = []

    def get_order_commission(self, symbol, order_id):
        self.calls.append((symbol, str(order_id)))
        return self.fees.get(str(order_id))


def _engine(broker):
    from engine.cascade import CascadeEngine

    return CascadeEngine(broker)


class BuyCommissionAttributionTests(unittest.TestCase):
    def test_one_order_prices_its_own_fill(self):
        campaign = _campaign()
        campaign.all_fills.append(Fill(price=73.0, quantity=0.2, level=8, leg_id=1, timestamp=1, order_id="55"))
        engine = _engine(_FeeBroker({"55": 0.0146}))
        asyncio.run(engine._attribute_buy_commission(campaign, "55"))
        self.assertAlmostEqual(campaign.all_fills[0].fee_usd, 0.0146)

    def test_a_partial_fill_is_not_charged_twice(self):
        """myTrades reports the order's RUNNING total, so the second poll must
        attribute only what the first one did not."""
        campaign = _campaign()
        campaign.all_fills.append(Fill(price=73.0, quantity=0.1, level=8, leg_id=1, timestamp=1, order_id="55"))
        engine = _engine(_FeeBroker({"55": 0.0073}))
        asyncio.run(engine._attribute_buy_commission(campaign, "55"))
        first = campaign.all_fills[0].fee_usd
        self.assertAlmostEqual(first, 0.0073)

        # Second piece arrives; the order's total is now higher.
        campaign.all_fills.append(Fill(price=73.0, quantity=0.1, level=8, leg_id=1, timestamp=2, order_id="55"))
        engine.broker.fees["55"] = 0.0146
        asyncio.run(engine._attribute_buy_commission(campaign, "55"))
        self.assertAlmostEqual(campaign.all_fills[0].fee_usd, first, msg="the first fill must not be re-priced")
        self.assertAlmostEqual(campaign.all_fills[1].fee_usd, 0.0073)
        self.assertAlmostEqual(sum(f.fee_usd for f in campaign.all_fills), 0.0146)

    def test_fills_from_other_orders_are_left_alone(self):
        campaign = _campaign()
        campaign.all_fills.append(Fill(price=73.0, quantity=0.2, level=8, leg_id=1, timestamp=1, order_id="55"))
        campaign.all_fills.append(Fill(price=74.0, quantity=0.2, level=4, leg_id=1, timestamp=2, order_id="66"))
        engine = _engine(_FeeBroker({"55": 0.0146, "66": 0.0148}))
        asyncio.run(engine._attribute_buy_commission(campaign, "55"))
        self.assertAlmostEqual(campaign.all_fills[0].fee_usd, 0.0146)
        self.assertIsNone(campaign.all_fills[1].fee_usd)

    def test_a_paper_campaign_never_asks_the_exchange(self):
        campaign = _campaign(mode="paper")
        campaign.all_fills.append(Fill(price=73.0, quantity=0.2, level=8, leg_id=1, timestamp=1, order_id="PAPER"))
        engine = _engine(_FeeBroker({"PAPER": 0.99}))
        asyncio.run(engine._attribute_buy_commission(campaign, "PAPER"))
        self.assertIsNone(campaign.all_fills[0].fee_usd)
        self.assertEqual(engine.broker.calls, [], "a paper fill has no exchange to bill it")


class RoundFeeSourceTests(unittest.TestCase):
    """Which number a closed round books, and whether it admits to guessing."""

    def _round_with(self, buy_fees, sell_fee):
        campaign = _campaign()
        campaign.avg_entry_price = 73.76
        for i, fee in enumerate(buy_fees):
            campaign.all_fills.append(
                Fill(price=73.76, quantity=0.092, level=8, leg_id=1, timestamp=i + 1, order_id="55", fee_usd=fee)
            )
        campaign.filled_base_qty = sum(f.quantity for f in campaign.all_fills)
        engine = _engine(_FeeBroker({}))
        engine._close_round(campaign, 74.14, sold_qty=campaign.filled_base_qty, sell_fee=sell_fee)
        return campaign.rounds[0]

    def test_both_sides_measured_books_the_exchange_figure(self):
        rnd = self._round_with([0.0051, 0.0051], sell_fee=0.0102)
        self.assertFalse(rnd.fees_estimated)
        self.assertAlmostEqual(rnd.fees_usd, 0.0204)
        self.assertAlmostEqual(rnd.pnl, round(rnd.pnl_gross - 0.0204, 8))

    def test_the_measured_figure_beats_the_model_and_they_differ(self):
        """The point of the exercise: at the BNB rate the model is 33% high."""
        rnd = self._round_with([0.0051, 0.0051], sell_fee=0.0102)
        modelled = round_trip_fee(73.76 * 0.184, 74.14 * 0.184)
        self.assertGreater(modelled, rnd.fees_usd)
        self.assertAlmostEqual(modelled / rnd.fees_usd, FEE_PCT_PER_SIDE / 0.075, places=2)

    def test_an_unpriced_buy_falls_back_to_the_model(self):
        rnd = self._round_with([0.0051, None], sell_fee=0.0102)
        self.assertTrue(rnd.fees_estimated)
        self.assertAlmostEqual(rnd.fees_usd, round_trip_fee(73.76 * 0.184, 74.14 * 0.184))

    def test_an_unknown_sell_falls_back_to_the_model(self):
        """Half measured and half modelled is a third number describing no
        actual trade, so a missing sell fee discards the buy figures too."""
        rnd = self._round_with([0.0051, 0.0051], sell_fee=None)
        self.assertTrue(rnd.fees_estimated)
        self.assertAlmostEqual(rnd.fees_usd, round_trip_fee(73.76 * 0.184, 74.14 * 0.184))

    def test_a_round_with_no_fills_is_estimated(self):
        campaign = _campaign()
        campaign.avg_entry_price = 73.76
        campaign.filled_base_qty = 0.184
        engine = _engine(_FeeBroker({}))
        engine._close_round(campaign, 74.14, sold_qty=0.184, sell_fee=0.0102)
        self.assertTrue(campaign.rounds[0].fees_estimated)


if __name__ == "__main__":
    unittest.main()


class LiveSyncFeeIntegrationTests(unittest.IsolatedAsyncioTestCase):
    """The real tick, not the units: a buy and a target through _sync_live_orders.

    The lookups are wired at the call sites, so a correct helper attached to the
    wrong place still books a modelled fee. This drives the actual path.
    """

    def setUp(self):
        from engine.cascade import Leg, build_fib_ladder_and_pool, plan_leg_orders
        from tests.test_cascade_engine import FakeCascadeBroker, _mk_campaign, _mk_engine

        self.broker = FakeCascadeBroker()
        # Commission per order id, the way Binance would report it via myTrades.
        self.broker.commissions = {}
        self.broker.get_order_commission = lambda symbol, order_id: self.broker.commissions.get(str(order_id))
        self.engine = _mk_engine(self.broker)
        self.campaign = _mk_campaign(self.engine, mode="live")
        self.campaign.state = "TRENDLINE_ACTIVE"
        leg = Leg(leg_id=1, trendline_id=1, low=99.5, touch_high=102.0, touch_timestamp=1200)
        self.campaign.legs.append(leg)
        build_fib_ladder_and_pool(self.campaign, leg)
        plan_leg_orders(self.campaign, leg)
        self.campaign.pending_usd = 40.0
        self.campaign.collected = [[1, 2, 16.0, 97.0], [1, 4, 24.0, 96.0]]
        leg.pending_orders[2].status = "COLLECTED"
        leg.pending_orders[4].status = "COLLECTED"
        self.campaign.pending_line = 96.0
        self.campaign.pending_stop_price = 96.5
        self.campaign.pending_limit_price = 96.55
        self.campaign.pending_rev = 1

    async def _buy_fills(self, buy_fee):
        await self.engine._sync_live_orders(self.campaign)
        order_id = str(self.campaign.pending_order_id)
        qty = 40.0 / 97.0
        self.broker.commissions[order_id] = buy_fee
        self.broker.order_lookup[order_id] = {
            "status": "FILLED",
            "executedQty": str(qty),
            "cummulativeQuoteQty": str(qty * 97.0),
        }
        await self.engine._sync_live_orders(self.campaign)
        return qty

    async def test_a_buy_fill_is_priced_from_the_exchange(self):
        await self._buy_fills(buy_fee=0.031)
        self.assertAlmostEqual(sum(f.fee_usd for f in self.campaign.all_fills), 0.031)

    async def test_the_round_books_both_measured_sides(self):
        qty = await self._buy_fills(buy_fee=0.031)
        tp_id = str(self.campaign.tp_order_id)
        self.assertTrue(tp_id and tp_id != "None", "a target must be resting to fill")
        self.broker.commissions[tp_id] = 0.037
        self.broker.order_lookup[tp_id] = {
            "status": "FILLED",
            "executedQty": str(qty),
            "cummulativeQuoteQty": str(qty * 99.0),
        }
        self.broker.open_orders = []

        await self.engine._sync_live_orders(self.campaign)

        self.assertEqual(len(self.campaign.rounds), 1)
        rnd = self.campaign.rounds[0]
        self.assertFalse(rnd.fees_estimated, "both sides were reported; nothing should be modelled")
        self.assertAlmostEqual(rnd.fees_usd, 0.068)
        self.assertAlmostEqual(rnd.pnl, round(rnd.pnl_gross - 0.068, 8))

    async def test_a_silent_exchange_falls_back_to_the_model(self):
        qty = await self._buy_fills(buy_fee=None)
        tp_id = str(self.campaign.tp_order_id)
        self.broker.order_lookup[tp_id] = {
            "status": "FILLED",
            "executedQty": str(qty),
            "cummulativeQuoteQty": str(qty * 99.0),
        }
        self.broker.open_orders = []

        await self.engine._sync_live_orders(self.campaign)

        rnd = self.campaign.rounds[0]
        self.assertTrue(rnd.fees_estimated)
        self.assertGreater(rnd.fees_usd, 0.0, "an unknown fee is modelled, never booked as free")
