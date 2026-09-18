"""The option seller's routes and its one-writer loop."""

import asyncio
import os
import tempfile
import unittest
from contextlib import asynccontextmanager
from importlib import import_module

import httpx
from state_quiesce import quiesce_state_writers


class OptionSellerRouteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.app_module = import_module("app")
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._orig_db = self.app_module._STATE_DB_FILE
        self.addCleanup(lambda: setattr(self.app_module, "_STATE_DB_FILE", self._orig_db))
        self.app_module._STATE_DB_FILE = os.path.join(self._tmp.name, "state.db")
        self.addCleanup(quiesce_state_writers, self.app_module._STATE_DB_FILE)
        orig_dir = self.app_module._STATE_DIR
        self.app_module._STATE_DIR = self._tmp.name
        self.addCleanup(lambda: setattr(self.app_module, "_STATE_DIR", orig_dir))
        self.addCleanup(self.app_module._release_option_seller_lock)
        self.app_module._rate_limits.clear()
        self.transport = httpx.ASGITransport(app=self.app_module.app)

    @asynccontextmanager
    async def _client(self):
        async with httpx.AsyncClient(transport=self.transport, base_url="http://testserver.local") as client:
            await client.post("/api/auth/login", json={"password": self.app_module.AUTH_PIN})
            self.headers = {
                "X-CSRF-Token": client.cookies.get("cryptoforge_csrf") or "",
                "X-Requested-With": "XMLHttpRequest",
            }
            yield client

    async def test_status_starts_off_and_paper_only(self):
        async with self._client() as client:
            r = await client.get("/api/option-seller/status")
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        self.assertFalse(body["enabled"])
        self.assertTrue(body["paper_only"])
        self.assertEqual(body["rule"]["min_votes"], 5)
        self.assertEqual(body["totals"]["trades"], 0)

    async def test_settings_switch_on_and_persist(self):
        async with self._client() as client:
            r = await client.post(
                "/api/option-seller/settings", json={"enabled": True, "size_btc": 0.25}, headers=self.headers
            )
            self.assertEqual(r.status_code, 200, r.text)
            again = (await client.get("/api/option-seller/status")).json()
        self.assertTrue(again["enabled"])
        self.assertEqual(again["size_btc"], 0.25)
        self.assertEqual(again["contracts"], 250)
        self.assertIn("switched ON", again["events"][0]["message"])

    async def test_bad_settings_are_a_400(self):
        async with self._client() as client:
            for body in ({"size_btc": -1}, {"enabled": "yes"}, {"size_btc": 99}):
                r = await client.post("/api/option-seller/settings", json=body, headers=self.headers)
                self.assertEqual(r.status_code, 400, body)
                self.assertTrue(r.json()["error"]["detail"], "the refusal must say why")

    async def test_settings_need_a_logged_in_writer(self):
        async with httpx.AsyncClient(transport=self.transport, base_url="http://testserver.local") as client:
            r = await client.post("/api/option-seller/settings", json={"enabled": True})
        self.assertIn(r.status_code, (401, 403))

    async def test_only_the_lock_holder_ticks(self):
        app = self.app_module
        app._put_option_seller_state({"enabled": True})
        ticked = []

        class Seller:
            enabled = True

            def load(self, state):
                pass

            def status(self):
                return {"open": None}

            async def tick(self):
                ticked.append(1)
                return False

            def dump(self):
                return {}

        orig = app._option_seller
        app._option_seller = Seller()
        self.addCleanup(lambda: setattr(app, "_option_seller", orig))
        # someone else holds the lock
        import fcntl

        other = open(app._option_seller_lock_path(), "a+")
        fcntl.flock(other.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            self.assertFalse(await app._option_seller_cycle())
            self.assertEqual(ticked, [], "a second instance must not move the book")
        finally:
            fcntl.flock(other.fileno(), fcntl.LOCK_UN)
            other.close()
        await app._option_seller_cycle()
        self.assertEqual(ticked, [1], "the holder must tick once the lock is free")

    async def test_the_cycle_reloads_settings_saved_by_the_other_instance(self):
        app = self.app_module
        seen = []

        class Seller(app.OptionSellerPaper):
            async def tick(self):
                seen.append(self.enabled)
                return False

        orig = app._option_seller
        app._option_seller = Seller()
        self.addCleanup(lambda: setattr(app, "_option_seller", orig))
        await app._option_seller_cycle()
        self.assertEqual(seen, [], "an off book with nothing open is not ticked")
        app._put_option_seller_state({"enabled": True})
        await app._option_seller_cycle()
        self.assertEqual(seen, [True])

    async def test_the_chart_route_refuses_a_bad_date_and_draws_nothing_when_empty(self):
        self.app_module._option_seller_chart_cache.clear()
        async with self._client() as client:
            bad = await client.get("/api/option-seller/chart", params={"date": "18-09-2026"})
            empty = await client.get("/api/option-seller/chart")
        self.assertEqual(bad.status_code, 400)
        self.assertEqual(empty.status_code, 200, empty.text)
        self.assertIsNone(empty.json()["trade"])

    async def test_the_chart_route_needs_a_login(self):
        async with httpx.AsyncClient(transport=self.transport, base_url="http://testserver.local") as client:
            r = await client.get("/api/option-seller/chart")
        self.assertIn(r.status_code, (401, 403))

    async def test_the_loop_is_started_with_the_app(self):
        src = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app.py")).read()
        life = src[src.index("async def _app_lifespan") :]
        self.assertIn("_option_seller_loop()", life[:1500])


if __name__ == "__main__":
    asyncio.run(unittest.main())
