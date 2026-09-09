"""A stopped campaign still holding coin must be reachable from Open Trades.

09-Sep-2026. Four Cascade-Auto positions sat as "stopped · manual exit" and
there was NO way to act on them:

  · the Auto page renders Open Trades with actions:false, so the Action
    column was not drawn at all; and
  · the Market Sell control that does exist lives on a campaign CARD, and
    those cards are only drawn for WORKING ladders — a stopped one has none.

Worse, the endpoint behind that control resolved the campaign in the LIVE
engine only, and these belong to the sandbox — so even a reachable button
would have answered "not found".
"""

import os
import re
import tempfile
import unittest
from contextlib import asynccontextmanager
from importlib import import_module

import httpx

_HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class StrandedRowShowsItsExitTests(unittest.TestCase):
    """The renderer's own source, since a stranded row is what it keys on."""

    @classmethod
    def setUpClass(cls):
        with open(os.path.join(_HERE, "static", "cryptoforge-app.js"), encoding="utf-8") as handle:
            cls.js = handle.read()

    def test_the_column_is_drawn_when_any_row_is_stranded(self):
        self.assertIn("var showActions = actions || open.some(_cfTradeIsStranded);", self.js)
        self.assertNotIn("+ (actions ? '<th>Action</th>' : '')", self.js)

    def test_a_stranded_row_is_ended_holding_and_unsold(self):
        body = re.search(r"function _cfTradeIsStranded\(c\) \{(.*?)\n\}", self.js, re.S).group(1)
        self.assertIn("filled_base_qty", body, "it must require a position")
        self.assertIn("tp_order_id", body, "a resting sell means it is not stranded")
        self.assertIn("_cfCascadeCampaignHasEnded", body)

    def test_a_healthy_row_on_a_read_only_page_still_gets_no_button(self):
        """actions:false pages must not sprout a market sell on every row."""
        self.assertIn("actions || _cfTradeIsStranded(c) ? _cfCascadeTradeAction(c) : ''", self.js)


class LiquidateFindsTheRightEngineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.app_module = import_module("app")
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._orig_db = self.app_module._STATE_DB_FILE
        self.addCleanup(lambda: setattr(self.app_module, "_STATE_DB_FILE", self._orig_db))
        self.app_module._STATE_DB_FILE = os.path.join(self._tmp.name, "state.db")
        self.app_module._rate_limits.clear()
        self.transport = httpx.ASGITransport(app=self.app_module.app)
        self.sold = []
        self.persisted = []

        outer = self

        class FakeEngine:
            def __init__(self, name, ids):
                self.name = name
                self.campaigns = {cid: object() for cid in ids}

            async def liquidate_campaign(self, campaign_id):
                outer.sold.append((self.name, campaign_id))
                return {"quantity": 1.0, "price": 100.0}

        self.live = FakeEngine("live", ["live-1"])
        self.auto = FakeEngine("auto", ["auto-9"])
        self.vrule = FakeEngine("vrule", ["vr-3"])

        for attr, value in (
            ("_get_cascade_engine", lambda: self.live),
            ("_get_auto_fib_engine", lambda: self.auto),
            ("_get_vrule_engine", lambda: self.vrule),
            ("_persist_cascade_runtime_snapshot", lambda eng: self.persisted.append("cascade")),
            ("_save_auto_fib", lambda: self.persisted.append("auto")),
            ("_save_vrule", lambda: self.persisted.append("vrule")),
        ):
            original = getattr(self.app_module, attr)
            setattr(self.app_module, attr, value)
            self.addCleanup(lambda a=attr, o=original: setattr(self.app_module, a, o))

    @asynccontextmanager
    async def _client(self):
        async with httpx.AsyncClient(transport=self.transport, base_url="http://testserver.local") as client:
            await client.post("/api/auth/login", json={"password": self.app_module.AUTH_PIN})
            self.headers = {
                "X-CSRF-Token": client.cookies.get("cryptoforge_csrf") or "",
                "X-Requested-With": "XMLHttpRequest",
            }
            yield client

    async def test_a_sandbox_campaign_is_sold_by_its_own_engine(self):
        async with self._client() as client:
            response = await client.post("/api/cascade/campaigns/auto-9/liquidate", headers=self.headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.sold, [("auto", "auto-9")], "the live engine was asked for a sandbox campaign")
        self.assertEqual(self.persisted, ["auto"], "the sandbox runtime was not saved after the sale")

    async def test_a_live_campaign_still_goes_to_the_live_engine(self):
        async with self._client() as client:
            await client.post("/api/cascade/campaigns/live-1/liquidate", headers=self.headers)
        self.assertEqual(self.sold, [("live", "live-1")])
        self.assertEqual(self.persisted, ["cascade"])

    async def test_a_vrule_campaign_goes_to_the_vrule_engine(self):
        async with self._client() as client:
            await client.post("/api/cascade/campaigns/vr-3/liquidate", headers=self.headers)
        self.assertEqual(self.sold, [("vrule", "vr-3")])
        self.assertEqual(self.persisted, ["vrule"])

    async def test_an_engine_that_will_not_start_does_not_hide_the_others(self):
        def boom():
            raise RuntimeError("broker unreachable")

        self.app_module._get_cascade_engine = boom
        async with self._client() as client:
            response = await client.post("/api/cascade/campaigns/auto-9/liquidate", headers=self.headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.sold, [("auto", "auto-9")])


if __name__ == "__main__":
    unittest.main()
