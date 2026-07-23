"""Route tests for the /api/cascade endpoints."""

import os
import tempfile
import unittest
from contextlib import asynccontextmanager
from importlib import import_module
from unittest.mock import patch

import httpx

from tests.test_cascade_engine import _RECENT_TS, FakeCascadeBroker


class CascadeRouteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.app_module = import_module("app")
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._orig_state_db = self.app_module._STATE_DB_FILE
        self._orig_engine = getattr(self.app_module, "_cascade_engine", None)
        self._orig_delta = self.app_module.delta
        self.addCleanup(self._restore)

        self.app_module._STATE_DB_FILE = os.path.join(self._tmp.name, "cryptoforge_state.db")
        self.app_module._rate_limits.clear()
        self.broker = FakeCascadeBroker()
        self.app_module.delta = self.broker
        self.app_module._cascade_engine = None

        self.transport = httpx.ASGITransport(app=self.app_module.app)

    def _restore(self):
        engine = getattr(self.app_module, "_cascade_engine", None)
        if engine is not None:
            engine.stop()
        self.app_module._STATE_DB_FILE = self._orig_state_db
        self.app_module._cascade_engine = self._orig_engine
        self.app_module.delta = self._orig_delta

    @asynccontextmanager
    async def _client(self):
        async with httpx.AsyncClient(transport=self.transport, base_url="http://testserver.local") as client:
            await client.post("/api/auth/login", json={"password": self.app_module.AUTH_PIN})
            self._csrf_headers = {
                "X-CSRF-Token": client.cookies.get("cryptoforge_csrf") or "",
                "X-Requested-With": "XMLHttpRequest",
            }
            yield client

    async def test_status_route_returns_empty_state(self):
        async with self._client() as client:
            response = await client.get("/api/cascade/status")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["campaigns"], [])

    async def test_start_campaign_validation_errors(self):
        async with self._client() as client:
            bad_prices = await client.post(
                "/api/cascade/campaigns",
                json={"symbol": "BTCUSDT", "capital_usd": 2000, "mother_high": 99, "mother_low": 105},
                headers=self._csrf_headers,
            )
            self.assertEqual(bad_prices.status_code, 400)

            tiny_capital = await client.post(
                "/api/cascade/campaigns",
                json={"symbol": "BTCUSDT", "capital_usd": 4, "mother_high": 105, "mother_low": 99},
                headers=self._csrf_headers,
            )
            self.assertEqual(tiny_capital.status_code, 400)

    async def test_start_stop_and_delete_campaign(self):
        async with self._client() as client:
            started = await client.post(
                "/api/cascade/campaigns",
                json={
                    "symbol": "BTCUSDT",
                    "capital_usd": 2000,
                    "mother_high": 105,
                    "mother_low": 99,
                    "mother_timestamp": _RECENT_TS,
                },
                headers=self._csrf_headers,
            )
            self.assertEqual(started.status_code, 200)
            campaign = started.json()["campaign"]
            self.assertEqual(campaign["mode"], "paper")
            self.assertEqual(campaign["state"], "WAITING_FIRST_DEPTH")
            cid = campaign["campaign_id"]

            status = await client.get("/api/cascade/status")
            self.assertEqual(len(status.json()["campaigns"]), 1)

            stopped = await client.post(f"/api/cascade/campaigns/{cid}/stop", json={}, headers=self._csrf_headers)
            self.assertEqual(stopped.status_code, 200)
            self.assertEqual(stopped.json()["campaign"]["state"], "STOPPED")

            deleted = await client.request("DELETE", f"/api/cascade/campaigns/{cid}", headers=self._csrf_headers)
            self.assertEqual(deleted.status_code, 200)

            missing = await client.post(f"/api/cascade/campaigns/{cid}/stop", json={}, headers=self._csrf_headers)
            self.assertEqual(missing.status_code, 404)

    async def test_live_campaign_requires_configured_broker(self):
        async with self._client() as client:
            with patch.object(self.app_module, "_broker_is_configured", return_value=False):
                response = await client.post(
                    "/api/cascade/campaigns",
                    json={
                        "symbol": "BTCUSDT",
                        "capital_usd": 2000,
                        "mother_high": 105,
                        "mother_low": 99,
                        "mode": "live",
                    },
                    headers=self._csrf_headers,
                )
        self.assertEqual(response.status_code, 409)

    async def test_mode_flip_route(self):
        async with self._client() as client:
            started = await client.post(
                "/api/cascade/campaigns",
                json={
                    "symbol": "BTCUSDT",
                    "capital_usd": 2000,
                    "mother_high": 105,
                    "mother_low": 99,
                    "mother_timestamp": _RECENT_TS,
                },
                headers=self._csrf_headers,
            )
            cid = started.json()["campaign"]["campaign_id"]
            flipped = await client.post(
                f"/api/cascade/campaigns/{cid}/mode", json={"mode": "live"}, headers=self._csrf_headers
            )
            self.assertEqual(flipped.status_code, 200)
            self.assertEqual(flipped.json()["campaign"]["mode"], "live")

    async def test_broker_lock_reason_while_campaign_active(self):
        async with self._client() as client:
            await client.post(
                "/api/cascade/campaigns",
                json={
                    "symbol": "BTCUSDT",
                    "capital_usd": 2000,
                    "mother_high": 105,
                    "mother_low": 99,
                    "mother_timestamp": _RECENT_TS,
                },
                headers=self._csrf_headers,
            )
        locks = self.app_module._broker_runtime_lock_summary()
        self.assertFalse(locks["switchable"])
        self.assertTrue(any("cascade" in reason.lower() for reason in locks["reasons"]))

    async def test_emergency_stop_includes_cascade(self):
        async with self._client() as client:
            started = await client.post(
                "/api/cascade/campaigns",
                json={
                    "symbol": "BTCUSDT",
                    "capital_usd": 2000,
                    "mother_high": 105,
                    "mother_low": 99,
                    "mother_timestamp": _RECENT_TS,
                },
                headers=self._csrf_headers,
            )
            cid = started.json()["campaign"]["campaign_id"]
            response = await client.post("/api/emergency-stop", headers=self._csrf_headers)
        self.assertEqual(response.status_code, 200)
        results = response.json()["results"]
        self.assertEqual(results.get(f"cascade:campaign:{cid}"), "stopped")
        engine = self.app_module._cascade_engine
        self.assertEqual(engine.campaigns[cid].state, "STOPPED")

    async def test_campaign_survives_engine_restart_via_snapshot(self):
        async with self._client() as client:
            started = await client.post(
                "/api/cascade/campaigns",
                json={
                    "symbol": "BTCUSDT",
                    "capital_usd": 2000,
                    "mother_high": 105,
                    "mother_low": 99,
                    "mother_timestamp": _RECENT_TS,
                },
                headers=self._csrf_headers,
            )
            cid = started.json()["campaign"]["campaign_id"]
            # Simulate app restart: drop the engine singleton, then hit status.
            self.app_module._cascade_engine.stop()
            self.app_module._cascade_engine = None
            status = await client.get("/api/cascade/status")
        campaigns = status.json()["campaigns"]
        self.assertEqual(len(campaigns), 1)
        self.assertEqual(campaigns[0]["campaign_id"], cid)


class _FakeFill:
    def __init__(self, order_id):
        self.order_id = order_id


class _FakeRound:
    def __init__(self, fills):
        self.fills = fills


class _FakeCampaign:
    def __init__(self, campaign_id, seq, all_fills=None, rounds=None):
        self.campaign_id = campaign_id
        self.seq = seq
        self.all_fills = all_fills or []
        self.rounds = rounds or []


class _FakeEngine:
    def __init__(self, campaigns=None, closed=None):
        self.campaigns = campaigns or {}
        self.closed_campaigns = closed or []


class CascadeJournalLinkTests(unittest.TestCase):
    """The journal row's 'how we took the trade' chart depends on tying a paired
    round back to its campaign by shared exchange order id."""

    def setUp(self):
        self.app_module = import_module("app")
        self._orig_engine = getattr(self.app_module, "_cascade_engine", None)
        self.addCleanup(setattr, self.app_module, "_cascade_engine", self._orig_engine)

    def test_active_campaign_fill_links_by_order_id(self):
        self.app_module._cascade_engine = _FakeEngine(
            campaigns={"abc": _FakeCampaign("abc", 53, all_fills=[_FakeFill("3139163")])}
        )
        trades = [{"coin": "SOLUSDT", "buy_order_ids": ["3139163"], "source": "binance"}]
        self.app_module._link_trades_to_campaigns(trades)
        self.assertEqual(trades[0]["campaign_id"], "abc")
        self.assertEqual(trades[0]["campaign_seq"], 53)
        self.assertNotIn("buy_order_ids", trades[0], "internal key must be stripped")

    def test_closed_round_fill_links_from_rounds_snapshot(self):
        # A closed round moves its buys out of all_fills into rounds[].fills.
        closed = [
            {
                "campaign_id": "old",
                "seq": 10,
                "all_fills": [],
                "rounds": [{"fills": [{"order_id": "555"}]}],
            }
        ]
        self.app_module._cascade_engine = _FakeEngine(closed=closed)
        trades = [{"coin": "SOLUSDT", "buy_order_ids": ["555"], "source": "binance"}]
        self.app_module._link_trades_to_campaigns(trades)
        self.assertEqual(trades[0]["campaign_id"], "old")
        self.assertEqual(trades[0]["campaign_seq"], 10)

    def test_unmatched_and_paper_trades_get_no_campaign(self):
        self.app_module._cascade_engine = _FakeEngine(
            campaigns={"abc": _FakeCampaign("abc", 1, all_fills=[_FakeFill("PAPER")])}
        )
        trades = [
            {"coin": "ETHUSDT", "buy_order_ids": ["PAPER"], "source": "binance"},  # paper sentinel
            {"coin": "ETHUSDT", "buy_order_ids": ["999"], "source": "binance"},  # no such campaign
            {"coin": "ETHUSDT", "source": "sheet"},  # hand-typed row
        ]
        self.app_module._link_trades_to_campaigns(trades)
        for t in trades:
            self.assertNotIn("campaign_id", t)
            self.assertNotIn("buy_order_ids", t)

    def test_no_engine_is_harmless(self):
        self.app_module._cascade_engine = None
        trades = [{"coin": "SOLUSDT", "buy_order_ids": ["3139163"], "source": "binance"}]
        self.app_module._link_trades_to_campaigns(trades)
        self.assertNotIn("campaign_id", trades[0])
        self.assertNotIn("buy_order_ids", trades[0])


if __name__ == "__main__":
    unittest.main()
