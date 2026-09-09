"""A stranded PAPER position must always be closeable.

09-Sep-2026. Four stopped Cascade-Auto rows had a Market Sell button and every
click was a no-op: liquidate_campaign took its paper price from the engine's
in-memory `_price_cache`, an ENDED campaign is never ticked, and the cache is
empty after every restart. The route answered "No price available to close the
paper position against" and the row stayed for ever.

The price now falls back: the cache, then the venue, then the campaign's own
average entry — which books the round flat. For paper money a flat book beats
a row that nothing can ever clear.
"""

import asyncio
import unittest

from engine.cascade import CascadeEngine


class _Campaign:
    def __init__(self, **kw):
        self.campaign_id = kw.get("campaign_id", "c1")
        self.symbol = "BTCUSDT"
        self.exchange = ""
        self.state = "STOPPED"
        self.mode = "paper"
        self.filled_base_qty = 0.5
        self.residual_base_qty = 0.0
        self.avg_entry_price = 100.0
        for key, value in kw.items():
            setattr(self, key, value)


class PaperLiquidationPriceTests(unittest.TestCase):
    def setUp(self):
        self.engine = CascadeEngine.__new__(CascadeEngine)
        self.campaign = _Campaign()
        self.engine.campaigns = {"c1": self.campaign}
        self.engine._price_cache = {}
        self.engine._price_key = lambda c: c.symbol
        self.engine.broker_for = lambda c: None
        self.engine._bar_containing = lambda c: 0
        self.closed = []
        self.engine._close_round = lambda c, price, sold_qty=0.0, at_ts=0: self.closed.append((price, sold_qty))
        # What happens AFTER the round is booked is not what this file is
        # about: the log line, the archive and the broadcast each read fields a
        # real Campaign carries and this fake does not.
        self.engine._log_event = lambda *a, **k: None
        self.engine._archive_campaign = lambda c: None
        self.engine._emit_update = lambda: None

    def _liquidate(self):
        return asyncio.run(self.engine.liquidate_campaign("c1"))

    def test_a_cached_price_is_used_when_there_is_one(self):
        self.engine._price_cache["BTCUSDT"] = (123.0, 0.0)

        async def never(*a, **k):
            raise AssertionError("the venue was asked despite a cached price")

        self.engine._get_price = never
        self._liquidate()
        self.assertEqual(self.closed, [(123.0, 0.5)])

    def test_an_empty_cache_asks_the_venue(self):
        async def price(symbol, max_age=4.0, venue=None):
            return 88.0

        self.engine._get_price = price
        self._liquidate()
        self.assertEqual(self.closed, [(88.0, 0.5)])

    def test_a_venue_that_cannot_answer_books_flat_at_the_average_entry(self):
        async def boom(symbol, max_age=4.0, venue=None):
            raise RuntimeError("no network")

        self.engine._get_price = boom
        result = self._liquidate()
        self.assertNotIn("error", result, result)
        self.assertEqual(self.closed, [(100.0, 0.5)], "a stranded paper row was left unclosable")

    def test_a_venue_returning_zero_also_books_flat(self):
        async def zero(symbol, max_age=4.0, venue=None):
            return 0.0

        self.engine._get_price = zero
        self._liquidate()
        self.assertEqual(self.closed, [(100.0, 0.5)])

    def test_with_no_price_anywhere_it_still_refuses_rather_than_inventing_one(self):
        async def zero(symbol, max_age=4.0, venue=None):
            return 0.0

        self.engine._get_price = zero
        self.campaign.avg_entry_price = 0.0
        result = self._liquidate()
        self.assertIn("error", result)
        self.assertEqual(self.closed, [])

    def test_a_running_campaign_is_still_refused(self):
        self.campaign.state = "ACTIVE"
        result = self._liquidate()
        self.assertIn("error", result)

    def test_a_campaign_holding_nothing_is_still_refused(self):
        self.campaign.filled_base_qty = 0.0
        result = self._liquidate()
        self.assertIn("error", result)


if __name__ == "__main__":
    unittest.main()
