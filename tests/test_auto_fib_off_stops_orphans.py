"""A book that is OFF must not leave lines working — however long it is off.

Stopping at the moment of the switch was not enough. BTCUSDT had been switched
off BEFORE that shipped, so #231, #243 and #272 kept holding and kept taking
targets under a panel reading "Switched off", and nothing would ever have come
along to stop them. The tick reconciles it now, so an already-off book heals
itself on the next cycle.
"""

import asyncio
import unittest

from engine import auto_cascade_fib as auto


class _Campaign:
    def __init__(self, cid, state="ACTIVE"):
        self.campaign_id = cid
        self.state = state


class _Engine:
    def __init__(self):
        self.stopped = []

    async def stop_campaign(self, campaign_id, cancel_orders=True):
        self.stopped.append(campaign_id)
        return {"status": "ok"}


class _Book:
    def __init__(self, symbol="BTCUSDT", enabled=False):
        self.symbol = symbol
        self.exchange = ""
        self.enabled = enabled
        self.note = ""
        self.last_error = ""


class AutoFibOffStopsOrphansTests(unittest.TestCase):
    def setUp(self):
        self.driver = auto.AutoCascadeFib.__new__(auto.AutoCascadeFib)
        self.engine = _Engine()
        self.driver.engine = self.engine
        self.book = _Book()
        self.driver.books = {"btcusdt:": self.book}
        self.driver._last_tick_ts = 0.0
        self.working = [_Campaign("c1"), _Campaign("c2")]
        self.driver._live_campaigns = lambda book: [c for c in self.working if c.state == "ACTIVE"]

    def test_a_book_that_is_already_off_has_its_lines_stopped_on_the_next_tick(self):
        asyncio.run(self.driver.tick())
        self.assertEqual(self.engine.stopped, ["c1", "c2"])

    def test_it_is_idempotent_once_they_are_stopped(self):
        asyncio.run(self.driver.tick())
        for campaign in self.working:
            campaign.state = "STOPPED"
        asyncio.run(self.driver.tick())
        self.assertEqual(self.engine.stopped, ["c1", "c2"], "the tick tried to stop them twice")

    def test_a_book_that_is_ON_keeps_its_lines(self):
        self.book.enabled = True
        # An enabled book runs the real tick body; stub the steps it would take.
        self.driver._bank_and_fold = lambda book: False
        self.driver._apply_wallet_cap = lambda book: False
        self.driver._graduate = lambda book: False

        async def no_seed(book):
            return False

        self.driver._seed_working_line = no_seed
        asyncio.run(self.driver.tick())
        self.assertEqual(self.engine.stopped, [], "an ON book had its lines stopped")

    def test_one_line_refusing_to_stop_does_not_block_the_other(self):
        async def half_broken(campaign_id, cancel_orders=True):
            if campaign_id == "c1":
                raise RuntimeError("exchange unreachable")
            self.engine.stopped.append(campaign_id)

        self.engine.stop_campaign = half_broken
        asyncio.run(self.driver.tick())
        self.assertEqual(self.engine.stopped, ["c2"])

    def test_a_book_with_nothing_working_reports_no_change(self):
        self.working = []
        changed = asyncio.run(self.driver.tick())
        self.assertFalse(changed)


if __name__ == "__main__":
    unittest.main()
