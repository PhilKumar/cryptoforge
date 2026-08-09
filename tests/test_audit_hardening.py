import os
import stat
import tempfile
import unittest
from importlib import import_module
from types import SimpleNamespace
from unittest.mock import patch

import httpx

import alerter
from state_store import SQLiteJSONStore


class StatePermissionTests(unittest.TestCase):
    def test_state_database_is_owner_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "cryptoforge_state.db")
            SQLiteJSONStore(db_path)
            self.assertEqual(stat.S_IMODE(os.stat(db_path).st_mode), 0o600)


class RuntimeSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.app_module = import_module("app")

    async def test_live_start_rejects_missing_risk_acknowledgement_before_broker_access(self):
        payload = self.app_module.StrategyPayload(symbol="BTCUSDT")
        with self.assertRaises(self.app_module.HTTPException) as raised:
            await self.app_module.live_start(payload)
        self.assertEqual(raised.exception.status_code, 400)

    async def test_browser_websocket_is_revalidated_after_accept(self):
        class FakeWebSocket:
            cookies = {"cryptoforge_session": "session-token"}
            headers = {"user-agent": "AuditTest/1.0"}
            client = SimpleNamespace(host="127.0.0.1")

            def __init__(self):
                self.accepted = False
                self.closed = None

            async def accept(self):
                self.accepted = True

            async def close(self, code, reason):
                self.closed = (code, reason)

        ws = FakeWebSocket()
        with patch.object(self.app_module, "_validate_session", side_effect=[True, False]):
            await self.app_module.websocket_endpoint(ws)
        self.assertTrue(ws.accepted)
        self.assertEqual(ws.closed, (4001, "Session expired"))
        self.assertNotIn(ws, self.app_module.ws_clients)

    def test_cascade_campaign_blocks_state_restore(self):
        self.assertTrue(self.app_module._runtime_has_activity({"cascade_active_campaigns": 1}))

    async def test_emergency_stop_reports_a_failed_cascade_stop_as_partial(self):
        campaign = SimpleNamespace(campaign_id="campaign-1")

        class FailingCascade:
            active_campaigns = [campaign]
            _running = False

            async def stop_campaign(self, *_args, **_kwargs):
                return {"status": "error", "error": "broker did not confirm cancellation"}

            def stop(self):
                return None

        original = self.app_module._cascade_engine
        self.app_module._cascade_engine = FailingCascade()
        try:
            result = await self.app_module.emergency_stop(SimpleNamespace())
        finally:
            self.app_module._cascade_engine = original
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["failed"], 1)
        self.assertEqual(result["stopped"], 0)

    async def test_public_health_does_not_disclose_trading_or_state_details(self):
        transport = httpx.ASGITransport(app=self.app_module.app, client=("203.0.113.10", 443))
        async with httpx.AsyncClient(transport=transport, base_url="https://crypto.example") as client:
            response = await client.get("/api/health", headers={"X-Forwarded-For": "203.0.113.10"})
        self.assertEqual(response.status_code, 200)
        self.assertNotIn("runtime", response.json())
        self.assertNotIn("state_store", response.json())
        self.assertNotIn("broker", response.json())

    async def test_detailed_readiness_requires_auth_when_proxied(self):
        transport = httpx.ASGITransport(app=self.app_module.app, client=("127.0.0.1", 443))
        async with httpx.AsyncClient(transport=transport, base_url="https://crypto.example") as client:
            response = await client.get("/api/ready", headers={"X-Forwarded-For": "203.0.113.10"})
        self.assertEqual(response.status_code, 401)


class AlertConfigurationTests(unittest.TestCase):
    def test_alert_destinations_reload_after_environment_change(self):
        try:
            with patch.dict(
                os.environ,
                {
                    "TELEGRAM_BOT_TOKEN": "test-token",
                    "TELEGRAM_CHAT_ID": "test-chat",
                    "DISCORD_WEBHOOK_URL": "",
                },
                clear=False,
            ):
                alerter.reload_from_env()
                self.assertTrue(alerter._TELEGRAM_OK)
                self.assertFalse(alerter._DISCORD_OK)
        finally:
            alerter.reload_from_env()
