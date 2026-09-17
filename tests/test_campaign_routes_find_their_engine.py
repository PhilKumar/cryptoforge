"""Every per-campaign button must reach the engine that owns the campaign.

17-Sep-2026, Phil, trying to clear Cascade-Auto campaign #359:

    "Campaign 4b98416e2f not found — Not able to stop or delete"

The three strategies keep SEPARATE CascadeEngine instances. 09-Sep fixed this
for Market Sell alone (`_engine_holding_campaign`), and that helper's own
docstring says "every campaign route looked only in the live one" — but Stop,
Delete, Mode, MC kind, Recalculate, Restructure, Chart and Events were never
moved onto it. So a Cascade-Auto or V-Rule campaign was drawn with buttons that
all answered 404.
"""

import os
import tempfile
import unittest
from contextlib import asynccontextmanager
from importlib import import_module

import httpx
from state_quiesce import quiesce_state_writers


class _Campaign:
    def __init__(self, state="TRENDLINE_ACTIVE"):
        self.state = state
        self.event_log = [{"message": "hello"}]


class CampaignRoutesFindTheirEngineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.app_module = import_module("app")
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._orig_db = self.app_module._STATE_DB_FILE
        self.addCleanup(lambda: setattr(self.app_module, "_STATE_DB_FILE", self._orig_db))
        self.app_module._STATE_DB_FILE = os.path.join(self._tmp.name, "state.db")
        self.addCleanup(quiesce_state_writers, self.app_module._STATE_DB_FILE)
        self.app_module._rate_limits.clear()
        self.transport = httpx.ASGITransport(app=self.app_module.app)
        self.calls = []
        self.persisted = []

        outer = self

        class FakeEngine:
            def __init__(self, name, ids):
                self.name = name
                self.campaigns = {cid: _Campaign() for cid in ids}

            async def stop_campaign(self, campaign_id, cancel_orders=True):
                outer.calls.append((self.name, "stop", campaign_id))
                self.campaigns[campaign_id].state = "STOPPED"
                return {"status": "ok"}

            def delete_campaign(self, campaign_id):
                outer.calls.append((self.name, "delete", campaign_id))
                self.campaigns.pop(campaign_id, None)
                return {"status": "ok"}

            async def set_mode(self, campaign_id, mode):
                outer.calls.append((self.name, "mode", campaign_id))
                return {"status": "ok"}

            def set_mc_kind(self, campaign_id, kind):
                outer.calls.append((self.name, "mc_kind", campaign_id))
                return {"status": "ok"}

        self.live = FakeEngine("live", ["live-1"])
        self.auto = FakeEngine("auto", ["auto-359"])
        self.vrule = FakeEngine("vrule", ["vr-3"])

        for attr, value in (
            ("_get_cascade_engine", lambda: self.live),
            ("_get_auto_fib_engine", lambda: self.auto),
            ("_get_vrule_engine", lambda: self.vrule),
            ("_persist_cascade_runtime_snapshot", lambda eng: self.persisted.append("cascade")),
            ("_cascade_persist_closed_list", lambda eng: self.persisted.append("live-closed")),
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

    async def test_stop_reaches_a_cascade_auto_campaign(self):
        """THE BUG: #359 answered 404 on Stop."""
        async with self._client() as client:
            r = await client.post("/api/cascade/campaigns/auto-359/stop", json={}, headers=self.headers)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(self.calls, [("auto", "stop", "auto-359")])
        self.assertEqual(self.persisted, ["auto"])

    async def test_delete_reaches_a_cascade_auto_campaign(self):
        """THE BUG: #359 answered 404 on Delete."""
        async with self._client() as client:
            r = await client.delete("/api/cascade/campaigns/auto-359", headers=self.headers)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(self.calls, [("auto", "stop", "auto-359"), ("auto", "delete", "auto-359")])

    async def test_deleting_a_sandbox_campaign_never_writes_the_live_closed_history(self):
        """The closed-campaign bucket is the live engine's alone."""
        async with self._client() as client:
            await client.delete("/api/cascade/campaigns/auto-359", headers=self.headers)
        self.assertNotIn("live-closed", self.persisted, "a sandbox campaign was published as live history")
        self.assertEqual(self.persisted, ["auto"])

    async def test_deleting_a_live_campaign_still_saves_the_live_closed_history(self):
        async with self._client() as client:
            await client.delete("/api/cascade/campaigns/live-1", headers=self.headers)
        self.assertEqual(self.calls[-1], ("live", "delete", "live-1"))
        self.assertEqual(self.persisted, ["live-closed", "cascade"])

    async def test_stop_reaches_a_vrule_campaign(self):
        async with self._client() as client:
            r = await client.post("/api/cascade/campaigns/vr-3/stop", json={}, headers=self.headers)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(self.calls, [("vrule", "stop", "vr-3")])

    async def test_mode_and_mc_kind_reach_the_owning_engine(self):
        async with self._client() as client:
            await client.post("/api/cascade/campaigns/auto-359/mode", json={"mode": "paper"}, headers=self.headers)
            await client.post("/api/cascade/campaigns/vr-3/mc-kind", json={"mc_kind": "minor"}, headers=self.headers)
        self.assertEqual(self.calls, [("auto", "mode", "auto-359"), ("vrule", "mc_kind", "vr-3")])

    async def test_events_are_read_from_the_owning_engine(self):
        async with self._client() as client:
            r = await client.get("/api/cascade/campaigns/auto-359/events", headers=self.headers)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["events"], [{"message": "hello"}])

    async def test_a_live_campaign_still_goes_to_the_live_engine(self):
        async with self._client() as client:
            await client.post("/api/cascade/campaigns/live-1/stop", json={}, headers=self.headers)
        self.assertEqual(self.calls, [("live", "stop", "live-1")])
        self.assertEqual(self.persisted, ["cascade"])

    async def test_an_unknown_campaign_is_still_a_404(self):
        async with self._client() as client:
            r = await client.delete("/api/cascade/campaigns/nope", headers=self.headers)
        self.assertEqual(r.status_code, 404)


if __name__ == "__main__":
    unittest.main()
