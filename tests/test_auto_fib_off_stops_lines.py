"""Switching a Cascade-Auto book off has to stop the lines it started.

Disabling only set book.enabled = False, which stops the tick SEEDING new
lines. Every ladder the book had already started stayed in the engine and
carried on being ticked — so on 09-Sep-2026 Phil switched BTCUSDT off and
watched #231, #243 and #272 keep working, two of them holding coin.

Stopping them is safe by construction: stop_campaign pulls the resting buys
and, when coin is held, deliberately leaves the take-profit resting so the
position still exits at its target. Nothing is stranded.
"""

import os
import tempfile
import unittest
from contextlib import asynccontextmanager
from importlib import import_module

import httpx


class AutoFibOffStopsItsLinesTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.app_module = import_module("app")
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._orig_db = self.app_module._STATE_DB_FILE
        self.addCleanup(lambda: setattr(self.app_module, "_STATE_DB_FILE", self._orig_db))
        self.app_module._STATE_DB_FILE = os.path.join(self._tmp.name, "state.db")
        self.app_module._rate_limits.clear()
        self.transport = httpx.ASGITransport(app=self.app_module.app)

        self.stopped = []
        self.started = []

        class FakeCampaign:
            def __init__(self, cid):
                self.campaign_id = cid

        class FakeEngine:
            def __init__(self, outer):
                self._outer = outer

            def start(self):
                self._outer.started.append("start")

            async def stop_campaign(self, campaign_id, cancel_orders=True):
                self._outer.stopped.append(campaign_id)
                return {"status": "ok"}

        class FakeBook:
            def __init__(self):
                self.symbol = "BTCUSDT"
                self.enabled = True

        class FakeDriver:
            def __init__(self, outer):
                self._outer = outer
                self.book = FakeBook()
                self.books = {"btcusdt:": self.book}

            def _normalise_exchange(self, exchange=""):
                return ""

            def set_book(self, symbol, *, enabled=None, mode=None, capital_usd=None, exchange=""):
                if enabled is not None:
                    self.book.enabled = bool(enabled)
                return self.book

            def _live_campaigns(self, book):
                return [FakeCampaign("c1"), FakeCampaign("c2")]

            async def _stop_orphaned_lines(self, book):
                self._outer.stopped.append(book.symbol)
                return True

            def status(self):
                return {"books": []}

        self.driver = FakeDriver(self)
        self._orig_get_driver = self.app_module._get_auto_fib
        self._orig_get_engine = self.app_module._get_auto_fib_engine
        self._orig_save = self.app_module._save_auto_fib
        self.app_module._get_auto_fib = lambda: self.driver
        self.app_module._get_auto_fib_engine = lambda: FakeEngine(self)
        self.app_module._save_auto_fib = lambda *a, **k: None
        self.addCleanup(lambda: setattr(self.app_module, "_get_auto_fib", self._orig_get_driver))
        self.addCleanup(lambda: setattr(self.app_module, "_get_auto_fib_engine", self._orig_get_engine))
        self.addCleanup(lambda: setattr(self.app_module, "_save_auto_fib", self._orig_save))

    @asynccontextmanager
    async def _client(self):
        async with httpx.AsyncClient(transport=self.transport, base_url="http://testserver.local") as client:
            await client.post("/api/auth/login", json={"password": self.app_module.AUTH_PIN})
            self.headers = {
                "X-CSRF-Token": client.cookies.get("cryptoforge_csrf") or "",
                "X-Requested-With": "XMLHttpRequest",
            }
            yield client

    async def test_switching_a_book_off_clears_its_lines_at_once(self):
        """The switch delegates to the SAME step the tick repeats, so the two
        can never disagree about what off means — see
        tests/test_auto_fib_off_stops_orphans.py for what that step does."""
        async with self._client() as client:
            response = await client.post(
                "/api/auto-fib/books",
                json={"symbol": "BTCUSDT", "enabled": False},
                headers=self.headers,
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.stopped, ["BTCUSDT"], "the book went off and nothing cleared its lines")
        self.assertTrue(response.json()["stopped_lines"])

    async def test_switching_it_on_stops_nothing(self):
        self.driver.book.enabled = False
        async with self._client() as client:
            await client.post(
                "/api/auto-fib/books",
                json={"symbol": "BTCUSDT", "enabled": True},
                headers=self.headers,
            )
        self.assertEqual(self.stopped, [])
        self.assertEqual(self.started, ["start"], "turning a book on must wake the sandbox engine")

    async def test_a_book_already_off_stops_nothing_again(self):
        """Only the ON -> OFF transition acts; a repeat POST is not a re-stop."""
        self.driver.book.enabled = False
        async with self._client() as client:
            await client.post(
                "/api/auto-fib/books",
                json={"symbol": "BTCUSDT", "enabled": False},
                headers=self.headers,
            )
        self.assertEqual(self.stopped, [])

    async def test_a_size_change_alone_never_stops_anything(self):
        async with self._client() as client:
            await client.post(
                "/api/auto-fib/books",
                json={"symbol": "BTCUSDT", "capital_usd": 500},
                headers=self.headers,
            )
        self.assertEqual(self.stopped, [])


if __name__ == "__main__":
    unittest.main()
