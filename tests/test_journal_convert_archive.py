"""The journal must not forget how a trade ended.

Phil, 19-Oct-2026: "why suddently my PL gone down from $16 to $10? Fix
immediately". Nothing had been sold or lost. Thirty of his closed trades had
been exited through Binance Convert, which never appears in spot trade history,
so the journal fetched Convert separately — for the last 90 days only. As each
exit passed 90 days old it left the window, its trade fell back to "open", and
its profit dropped out of Realised P&L. On his account: $12.00 over 60 closed
trades with the 90-day window, $18.92 over 90 with a 120-day one.

The fix keeps every Convert exit the first time it is seen, and reaches back a
year once to recover the ones already lost. The tests that matter are
`test_an_exit_older_than_the_window_still_closes_its_trade` and
`test_a_backfill_cut_short_is_not_marked_done` — the second because a backfill
marked complete over a hole would hide that hole forever.
"""

import asyncio
import os
import tempfile
import unittest
from importlib import import_module

from state_quiesce import quiesce_state_writers

DAY_MS = 86_400_000


def _convert(order_id, *, days_ago, base="BTC", qty=0.001, usd=90.0, now_ms=1_800_000_000_000):
    return {
        "id": f"convert-{order_id}",
        "order_id": order_id,
        "source": "convert",
        "symbol": f"{base}USDT",
        "product_symbol": f"{base}USDT",
        "side": "sell",
        "price": usd / qty,
        "size": qty,
        "from_asset": base,
        "to_asset": "USDT",
        "from_amount": qty,
        "to_amount": usd,
        "time": now_ms - days_ago * DAY_MS,
    }


class _FakeBroker:
    def __init__(self, *, window, backfill, fail_backfill=False):
        self.window = window
        self.backfill = backfill
        self.fail_backfill = fail_backfill
        self.calls = []

    def get_convert_history(self, days=30, raise_on_error=False):
        self.calls.append((days, raise_on_error))
        if days > 90:
            if self.fail_backfill and raise_on_error:
                raise RuntimeError("Binance API 429: Too many requests")
            return list(self.backfill)
        return list(self.window)


class ConvertArchiveTests(unittest.TestCase):
    def setUp(self):
        self.app = import_module("app")
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._orig_db = self.app._STATE_DB_FILE
        self._orig_delta = self.app.delta
        self._orig_configured = self.app._broker_is_configured
        self.app._STATE_DB_FILE = os.path.join(self._tmp.name, "state.db")
        self.addCleanup(quiesce_state_writers, self.app._STATE_DB_FILE)
        self.addCleanup(self._restore)
        self.app._broker_is_configured = lambda: True

    def _restore(self):
        self.app._STATE_DB_FILE = self._orig_db
        self.app.delta = self._orig_delta
        self.app._broker_is_configured = self._orig_configured

    def _run(self, recent):
        return asyncio.run(self.app._journal_convert_archive(recent))

    def test_an_exit_older_than_the_window_still_closes_its_trade(self):
        """The bug itself. Seen once, a Convert is kept — so the day it ages out
        of the 90-day fetch, the trade it closed stays closed."""
        old = _convert("A1", days_ago=95)
        self.app.delta = _FakeBroker(window=[old], backfill=[old])
        self._run([old])
        # Weeks later: the fetch window no longer contains it.
        self.app.delta = _FakeBroker(window=[], backfill=[])
        kept = self._run([])
        self.assertIn("A1", {str(r["order_id"]) for r in kept})

    def test_the_first_load_recovers_exits_already_lost(self):
        """Phil's $6.92: exits that had aged past 90 days before this existed."""
        lost = _convert("L1", days_ago=110)
        recent = _convert("R1", days_ago=10)
        self.app.delta = _FakeBroker(window=[recent], backfill=[lost, recent])
        kept = self._run([recent])
        self.assertEqual({str(r["order_id"]) for r in kept}, {"L1", "R1"})

    def test_the_backfill_happens_once(self):
        """It is a heavy endpoint. After it has succeeded, only the window."""
        broker = _FakeBroker(window=[], backfill=[_convert("L1", days_ago=200)])
        self.app.delta = broker
        self._run([])
        self._run([])
        self._run([])
        self.assertEqual(sum(1 for days, _ in broker.calls if days > 90), 1)

    def test_a_backfill_cut_short_is_not_marked_done(self):
        """Rate-limited half way: the next load must try again, not believe a
        year was read when it was not."""
        failing = _FakeBroker(window=[], backfill=[], fail_backfill=True)
        self.app.delta = failing
        self._run([])
        recovered = _FakeBroker(window=[], backfill=[_convert("L1", days_ago=200)])
        self.app.delta = recovered
        kept = self._run([])
        self.assertIn("L1", {str(r["order_id"]) for r in kept})

    def test_the_backfill_asks_to_be_told_when_it_is_cut_short(self):
        broker = _FakeBroker(window=[], backfill=[])
        self.app.delta = broker
        self._run([])
        self.assertIn((self.app._JOURNAL_CONVERT_BACKFILL_DAYS, True), broker.calls)

    def test_a_convert_seen_twice_is_kept_once(self):
        row = _convert("D1", days_ago=5)
        self.app.delta = _FakeBroker(window=[row], backfill=[row])
        self._run([row])
        kept = self._run([row, dict(row)])
        self.assertEqual([str(r["order_id"]) for r in kept].count("D1"), 1)

    def test_the_archived_exits_actually_close_the_trade(self):
        """End to end through the real pairing: a buy, then a Convert exit that
        is no longer in the fetch window, reads as a closed, profitable trade."""
        from engine.trade_journal import pair_fills_into_trades

        now = 1_800_000_000_000
        buy = {
            "symbol": "BTCUSDT",
            "id": 1,
            "orderId": 11,
            "price": "80000",
            "qty": "0.001",
            "quoteQty": "80",
            "commission": "0",
            "commissionAsset": "USDT",
            "time": now - 100 * DAY_MS,
            "isBuyer": True,
        }
        exit_row = _convert("X1", days_ago=95, qty=0.001, usd=90.0, now_ms=now)
        self.app.delta = _FakeBroker(window=[exit_row], backfill=[exit_row])
        self._run([exit_row])
        self.app.delta = _FakeBroker(window=[], backfill=[])
        kept = self._run([])

        without = pair_fills_into_trades([buy], converts=[])
        with_archive = pair_fills_into_trades([buy], converts=kept)
        self.assertEqual(without[0]["status"], "Open", "the old behaviour: the trade looks unsold")
        self.assertEqual(with_archive[0]["status"], "Closed")
        self.assertGreater(with_archive[0]["pnl_usd"], 0)


if __name__ == "__main__":
    unittest.main()
