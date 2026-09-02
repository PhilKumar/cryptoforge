"""The public snapshot refreshes itself, and can never disturb trading.

ledger.json was only written while /api/journal/trades was being served, so the
figures on the public landing were as fresh as the last time anyone opened the
Journal. This loop refreshes it weekly. Because it runs inside the trading app,
the thing that actually matters is the second assertion: a failure in a
marketing refresh must not escape into the event loop that runs the engines.
"""

import asyncio
import unittest
from unittest.mock import patch

import app


class WeeklyLedgerRefresh(unittest.IsolatedAsyncioTestCase):
    def test_interval_is_a_week(self):
        self.assertEqual(app._LANDING_LEDGER_REFRESH_SEC, 7 * 24 * 60 * 60)

    async def test_a_failing_refresh_never_escapes(self):
        """A Binance outage must cost a log line, not the app."""
        sleeps: list[float] = []

        async def fake_sleep(seconds):
            sleeps.append(seconds)
            # first sleep is the boot delay, then one failing pass, then stop
            if len(sleeps) >= 2:
                raise asyncio.CancelledError

        async def boom():
            raise RuntimeError("binance is down")

        with (
            patch.object(app.asyncio, "sleep", fake_sleep),
            patch.object(app, "journal_trades", boom),
            patch.object(app._logger, "warning") as warned,
        ):
            with self.assertRaises(asyncio.CancelledError):
                await app._refresh_landing_ledger_periodically()

        self.assertEqual(sleeps[0], app._LANDING_LEDGER_FIRST_DELAY_SEC)
        self.assertTrue(warned.called, "a failed refresh should be logged")

    async def test_a_good_refresh_calls_the_journal(self):
        sleeps: list[float] = []
        calls: list[int] = []

        async def fake_sleep(seconds):
            sleeps.append(seconds)
            if len(sleeps) >= 2:
                raise asyncio.CancelledError

        async def ok():
            calls.append(1)
            return {}

        with patch.object(app.asyncio, "sleep", fake_sleep), patch.object(app, "journal_trades", ok):
            with self.assertRaises(asyncio.CancelledError):
                await app._refresh_landing_ledger_periodically()

        self.assertEqual(len(calls), 1)
        self.assertEqual(sleeps[1], app._LANDING_LEDGER_REFRESH_SEC)


if __name__ == "__main__":
    unittest.main()
