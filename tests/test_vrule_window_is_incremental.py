"""The replay window is fetched once and then only topped up.

Binance was the exception: fetch_window was called with the book's
history_start_ts on EVERY scan, so thirty days of 5m candles — about nine
paged calls and an 8,640-row frame — were rebuilt per book every five
minutes. On 09-Sep-2026 /api/health idled at 8ms and jumped to 2.5-4.7s on
the bar boundary. A venue book already kept its window; this pins that the
default venue does too.
"""

import unittest
from types import SimpleNamespace

import pandas as pd

from engine import vrule_live


def _frame(start_ts: int, count: int) -> pd.DataFrame:
    stamps = [start_ts + i * 300 for i in range(count)]
    frame = pd.DataFrame(
        {"open": [1.0] * count, "high": [2.0] * count, "low": [0.5] * count, "close": [1.5] * count},
        index=pd.to_datetime(stamps, unit="s", utc=True),
    )
    frame.index.name = "datetime"
    return frame


class VRuleWindowIsIncrementalTests(unittest.TestCase):
    def setUp(self):
        self.driver = vrule_live.VRuleLive.__new__(vrule_live.VRuleLive)
        self.driver._venue_windows = {}
        self.start = 1_700_000_000
        self.book = SimpleNamespace(symbol="BTCUSDT", exchange="", key="btcusdt", history_start_ts=self.start)
        self.asked = []

    def _install(self, monkey):
        """fetch_window is imported inside the method, so patch the module."""
        from engine import rule3070_paper

        self._original = rule3070_paper.fetch_window
        rule3070_paper.fetch_window = monkey
        self.addCleanup(lambda: setattr(rule3070_paper, "fetch_window", self._original))

    def test_the_first_scan_asks_from_the_books_own_start(self):
        def fake(symbol, since_ts=0):
            self.asked.append(since_ts)
            return _frame(self.start, 100)

        self._install(fake)
        window = self.driver._default_window_loader(self.book)
        self.assertEqual(self.asked, [self.start])
        self.assertEqual(len(window), 100)

    def test_the_second_scan_asks_only_for_the_tail(self):
        def fake(symbol, since_ts=0):
            self.asked.append(since_ts)
            if len(self.asked) == 1:
                return _frame(self.start, 100)
            return _frame(self.start + 99 * 300, 3)

        self._install(fake)
        self.driver._default_window_loader(self.book)
        window = self.driver._default_window_loader(self.book)
        last_bar = self.start + 99 * 300
        self.assertEqual(self.asked[1], last_bar - 3600, "the whole window was fetched again")
        # 100 kept, the overlapping bar deduped, two genuinely new ones.
        self.assertEqual(len(window), 102)
        self.assertTrue(window.index.is_monotonic_increasing)
        self.assertFalse(window.index.has_duplicates)

    def test_a_warm_up_reaching_further_back_refetches_the_lot(self):
        def fake(symbol, since_ts=0):
            self.asked.append(since_ts)
            return _frame(self.start, 100)

        self._install(fake)
        self.driver._default_window_loader(self.book)
        self.book.history_start_ts = self.start - 86400
        self.driver._default_window_loader(self.book)
        self.assertEqual(self.asked[1], self.start - 86400)

    def test_a_book_turned_off_and_on_trims_to_the_newer_start(self):
        def fake(symbol, since_ts=0):
            self.asked.append(since_ts)
            if len(self.asked) == 1:
                return _frame(self.start, 100)
            return _frame(self.start + 99 * 300, 2)

        self._install(fake)
        self.driver._default_window_loader(self.book)
        self.book.history_start_ts = self.start + 50 * 300
        window = self.driver._default_window_loader(self.book)
        self.assertGreaterEqual(int(window.index[0].timestamp()), self.book.history_start_ts)

    def test_an_empty_fetch_keeps_the_window_we_had(self):
        def fake(symbol, since_ts=0):
            self.asked.append(since_ts)
            if len(self.asked) == 1:
                return _frame(self.start, 100)
            return _frame(self.start, 0)

        self._install(fake)
        first = self.driver._default_window_loader(self.book)
        second = self.driver._default_window_loader(self.book)
        self.assertEqual(len(second), len(first))

    def test_a_venue_book_does_not_take_this_path(self):
        self.book.exchange = "coindcx"
        calls = []
        self.driver._venue_window = lambda book: calls.append(book) or _frame(self.start, 5)
        self.driver._default_window_loader(self.book)
        self.assertEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main()
