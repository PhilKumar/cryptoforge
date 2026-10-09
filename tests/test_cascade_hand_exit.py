"""Taking a live ladder off the table by hand, at the 25% mark.

Phil, 09-Oct-2026: "I need a exit button on the live trades enabled once it
reaches atleast 25% of the average entry" — 25% of the way from the average
entry back toward the mother high, the house target shape, which his auto
books ride past at 0.5.

The load-bearing tests are the refusals. This sells real coin at market, so
the gate is re-checked in the engine when the button is pressed and not
trusted from the page: `test_the_engine_refuses_even_if_the_page_asks` is the
one that matters, because a page can be seconds stale and a price moves.
"""

import unittest
from unittest.mock import AsyncMock

from engine.cascade import (
    HAND_EXIT_FIB_LEVEL,
    TP_MIN_NET_PCT,
    Campaign,
    Fill,
    campaign_fee_pct,
    recompute_avg_entry_price,
    tp_breakeven_price,
)
from tests.test_cascade_engine import _mk_engine


class HandExitGateTests(unittest.TestCase):
    def setUp(self):
        self.engine = _mk_engine()

    def _campaign(self, *, avg=100.0, mother=200.0, qty=1.0, state="TRENDLINE_ACTIVE", mark=None):
        c = Campaign(
            campaign_id="c1",
            symbol="BTCUSDT",
            capital_usd=2000.0,
            mother_high=mother,
            mother_low=mother * 0.9,
            mother_timestamp=0,
            mode="live",
        )
        c.state = state
        if qty:
            c.all_fills.append(Fill(price=avg, quantity=qty, level=2, leg_id=1, timestamp=0))
            c.filled_base_qty = qty
            recompute_avg_entry_price(c)
        self.engine.campaigns["c1"] = c
        if mark is not None:
            self.engine._price_cache[self.engine._price_key(c)] = (mark, 0.0)
        return c

    def test_the_trigger_is_a_quarter_of_the_way_to_the_mother(self):
        c = self._campaign(avg=100.0, mother=200.0, mark=100.0)
        view = self.engine.hand_exit_view(c)
        self.assertAlmostEqual(view["trigger_price"], 125.0)
        self.assertEqual(HAND_EXIT_FIB_LEVEL, 0.25)

    def test_it_wakes_up_at_the_mark(self):
        c = self._campaign(avg=100.0, mother=200.0, mark=125.0)
        self.assertTrue(self.engine.hand_exit_view(c)["ready"])

    def test_it_stays_asleep_below_the_mark(self):
        c = self._campaign(avg=100.0, mother=200.0, mark=124.9)
        view = self.engine.hand_exit_view(c)
        self.assertFalse(view["ready"])
        self.assertIn("below", view["reason"])

    def test_a_shallow_entry_still_may_not_sell_at_a_loss(self):
        """The fee trap the resting target already has a floor for: a quarter
        of a tiny gap to the mother is less than the round trip costs."""
        c = self._campaign(avg=100.0, mother=100.4, mark=100.2)
        view = self.engine.hand_exit_view(c)
        floor = tp_breakeven_price(100.0, campaign_fee_pct(c)) * (1.0 + TP_MIN_NET_PCT / 100.0)
        self.assertAlmostEqual(view["trigger_price"], floor)
        self.assertGreater(view["trigger_price"], 100.0 + 0.25 * 0.4)
        self.assertFalse(view["ready"])

    def test_a_campaign_holding_nothing_offers_no_button(self):
        c = self._campaign(qty=0.0, mark=999.0)
        view = self.engine.hand_exit_view(c)
        self.assertFalse(view["ready"])
        self.assertEqual(view["held_qty"], 0.0)
        self.assertIn("Nothing held", view["reason"])

    def test_no_price_is_not_permission(self):
        c = self._campaign(avg=100.0, mother=200.0, mark=None)
        view = self.engine.hand_exit_view(c)
        self.assertFalse(view["ready"])
        self.assertIn("No live price", view["reason"])

    def test_the_gate_rides_up_with_the_average_entry(self):
        """Buying another rung lowers the average, so the mark moves down with
        it — the button must follow the position, not the first fill."""
        c = self._campaign(avg=100.0, mother=200.0, qty=1.0, mark=120.0)
        self.assertFalse(self.engine.hand_exit_view(c)["ready"])
        c.all_fills.append(Fill(price=60.0, quantity=1.0, level=4, leg_id=1, timestamp=0))
        c.filled_base_qty = 2.0
        recompute_avg_entry_price(c)
        view = self.engine.hand_exit_view(c)
        self.assertAlmostEqual(view["trigger_price"], 80.0 + 0.25 * 120.0)
        self.assertTrue(view["ready"])


class HandExitActionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.engine = _mk_engine()
        self.engine.stop_campaign = AsyncMock(return_value={"status": "ok"})
        self.engine.liquidate_campaign = AsyncMock(return_value={"status": "ok", "price": 125.0})

    def _campaign(self, *, mark, state="TRENDLINE_ACTIVE"):
        c = Campaign(
            campaign_id="c1",
            symbol="BTCUSDT",
            capital_usd=2000.0,
            mother_high=200.0,
            mother_low=180.0,
            mother_timestamp=0,
            mode="live",
        )
        c.state = state
        c.all_fills.append(Fill(price=100.0, quantity=1.0, level=2, leg_id=1, timestamp=0))
        c.filled_base_qty = 1.0
        recompute_avg_entry_price(c)
        self.engine.campaigns["c1"] = c
        self.engine._price_cache[self.engine._price_key(c)] = (mark, 0.0)
        return c

    async def test_the_engine_refuses_even_if_the_page_asks(self):
        """The page draws the button disabled, but a stale page can still post.
        Nothing is stopped and nothing is sold."""
        self._campaign(mark=110.0)
        result = await self.engine.hand_exit_campaign("c1")
        self.assertIn("Not yet", result["error"])
        self.engine.stop_campaign.assert_not_awaited()
        self.engine.liquidate_campaign.assert_not_awaited()

    async def test_it_stops_before_it_sells(self):
        """A ladder still armed would buy straight back into the position it
        believes it holds — which is why liquidate refuses a running campaign."""
        self._campaign(mark=130.0)
        result = await self.engine.hand_exit_campaign("c1")
        self.assertEqual(result["status"], "ok")
        self.engine.stop_campaign.assert_awaited_once()
        self.engine.liquidate_campaign.assert_awaited_once()
        self.assertAlmostEqual(result["hand_exit"]["trigger_price"], 125.0)

    async def test_a_failed_sell_says_the_coin_is_still_held(self):
        self._campaign(mark=130.0)
        self.engine.liquidate_campaign = AsyncMock(return_value={"error": "exchange said no"})
        result = await self.engine.hand_exit_campaign("c1")
        self.assertIn("exchange said no", result["error"])
        self.assertIn("still held", result["error"])

    async def test_a_failed_stop_does_not_go_on_to_sell(self):
        self._campaign(mark=130.0)
        self.engine.stop_campaign = AsyncMock(return_value={"error": "could not cancel"})
        result = await self.engine.hand_exit_campaign("c1")
        self.assertIn("could not cancel", result["error"])
        self.engine.liquidate_campaign.assert_not_awaited()

    async def test_an_ended_campaign_goes_the_old_way(self):
        """Already stopped: that is what Market Sell is, and it knows how to
        settle against the exchange first."""
        self._campaign(mark=110.0, state="STOPPED")
        result = await self.engine.hand_exit_campaign("c1")
        self.assertEqual(result["status"], "ok")
        self.engine.liquidate_campaign.assert_awaited_once()
        self.engine.stop_campaign.assert_not_awaited()

    async def test_an_unknown_campaign_is_a_plain_refusal(self):
        result = await self.engine.hand_exit_campaign("nope")
        self.assertIn("not found", result["error"])


if __name__ == "__main__":
    unittest.main()


class HandExitButtonTests(unittest.TestCase):
    """What the page draws. The button is the only place this rule is visible,
    so a campaign holding nothing must show none, and one short of the mark
    must show WHY rather than a dead grey square."""

    @staticmethod
    def _js():
        import os

        here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(here, "static", "cryptoforge-app.js"), encoding="utf-8") as handle:
            return handle.read()

    def setUp(self):
        src = self._js()
        self.fn = src[src.index("function _cfCascadeExitButton(") : src.index("async function cfCascadeHandExit(")]

    def test_nothing_held_draws_no_button(self):
        self.assertIn("if (!gate || !(Number(gate.held_qty) > 0)) return ''", self.fn)

    def test_short_of_the_mark_is_disabled_with_the_reason(self):
        self.assertIn("disabled", self.fn)
        self.assertIn("gate.reason", self.fn)

    def test_the_page_never_decides_the_gate_itself(self):
        """It draws what the engine said. No arithmetic here means the two
        cannot disagree about what 25% means."""
        for forbidden in ("0.25", "mother_high", "avg_entry *", "Math."):
            self.assertNotIn(forbidden, self.fn)

    def test_the_route_is_wired_and_rate_limited(self):
        import os

        here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(here, "app.py"), encoding="utf-8") as handle:
            app_src = handle.read()
        route = app_src[app_src.index('@app.post("/api/cascade/campaigns/{campaign_id}/hand-exit")') :][:1200]
        self.assertIn("check_rate_limit", route)
        self.assertIn("hand_exit_campaign", route)

    def test_the_confirmation_quotes_the_numbers_it_gated_on(self):
        src = self._js()
        confirm = src[src.index("async function cfCascadeHandExit(") :][:1600]
        for shown in ("gate.mark", "gate.trigger_price", "gate.avg_entry"):
            self.assertIn(shown, confirm)
        self.assertIn("cannot be undone", confirm)
