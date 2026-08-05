"""Route tests for the /api/cascade endpoints."""

import asyncio
import json
import os
import tempfile
import unittest
from contextlib import asynccontextmanager, suppress
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

    async def test_chart_serves_the_frozen_record_when_the_campaign_is_gone(self):
        """The permanent trade record: once a campaign has rotated out of memory,
        the chart still opens from its frozen snapshot — no live campaign needed,
        so every trade keeps its picture forever."""
        self.app_module._persist_chart_snapshot(
            "gone-123",
            {
                "status": "ok",
                "campaign_id": "gone-123",
                "state": "COMPLETED",
                "candles": [{"t": 1, "o": 1.0, "h": 2.0, "l": 1.0, "c": 1.5}],
                "legs": [],
                "trendlines": [],
            },
        )
        async with self._client() as client:
            resp = await client.get("/api/cascade/campaigns/gone-123/chart")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertTrue(data.get("snapshot"), "must be flagged as a frozen record")
        self.assertEqual(data.get("campaign_id"), "gone-123")
        self.assertEqual(len(data.get("candles") or []), 1)

    async def test_a_stale_snapshot_is_still_served_when_the_campaign_is_gone(self):
        """An outdated record beats no record. The campaign has rotated out of
        memory, so there is nothing left to rebuild it from."""
        self.app_module._persist_chart_snapshot(
            "stale-1",
            {
                "status": "ok",
                "campaign_id": "stale-1",
                "state": "COMPLETED",
                "candles": [{"t": 1, "o": 1.0, "h": 2.0, "l": 1.0, "c": 1.5}],
                "legs": [],
                "trendlines": [],
            },
        )
        # Force it to look like an older payload version.
        store = self.app_module._get_state_store()
        raw = store.get(self.app_module._BUCKET_CASCADE_CHART_SNAP, "stale-1", default={})
        raw["snapshot_version"] = 1
        store.put(self.app_module._BUCKET_CASCADE_CHART_SNAP, "stale-1", raw)

        async with self._client() as client:
            resp = await client.get("/api/cascade/campaigns/stale-1/chart")
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.json().get("snapshot"))

    async def test_a_current_snapshot_is_served_verbatim(self):
        """A record at the current version must NOT be re-rendered — that is the
        whole point of freezing it."""
        self.app_module._persist_chart_snapshot(
            "fresh-1",
            {
                "status": "ok",
                "campaign_id": "fresh-1",
                "state": "COMPLETED",
                "candles": [{"t": 1, "o": 1.0, "h": 2.0, "l": 1.0, "c": 1.5}],
                "avg_entry_price": 42.0,
                "legs": [],
                "trendlines": [],
            },
        )
        async with self._client() as client:
            resp = await client.get("/api/cascade/campaigns/fresh-1/chart")
        data = resp.json()
        self.assertEqual(data.get("avg_entry_price"), 42.0)
        self.assertEqual(data.get("snapshot_version"), self.app_module._CHART_SNAPSHOT_VERSION)

    async def test_chart_missing_and_unfrozen_is_404(self):
        async with self._client() as client:
            resp = await client.get("/api/cascade/campaigns/never-existed/chart")
        self.assertEqual(resp.status_code, 404)


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


class FeedKeySetRouteTests(unittest.IsolatedAsyncioTestCase):
    """The key-set endpoint. Public by necessity: an executor has to fetch it
    before it has proved anything about itself."""

    async def asyncSetUp(self):
        self.app_module = import_module("app")
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._orig_file = self.app_module._FEED_KEYSET_FILE
        self.app_module._FEED_KEYSET_FILE = os.path.join(self._tmp.name, "feed_keyset.json")
        self.addCleanup(lambda: setattr(self.app_module, "_FEED_KEYSET_FILE", self._orig_file))
        self.transport = httpx.ASGITransport(app=self.app_module.app)

    def _install(self, *, issued_at=None):
        import json as _json

        from engine.cascade_feed import ROOT_KID, FeedSigner, build_key_set, sign_key_set

        root = FeedSigner.generate(ROOT_KID)
        feed = FeedSigner.generate("cf-feed-2026a")
        now = int(issued_at if issued_at is not None else __import__("time").time())
        document = build_key_set(
            [{"kid": feed.kid, "public": feed.public_key_b64(), "not_before": now, "not_after": now + 90 * 86400}],
            issued_at=now,
        )
        with open(self.app_module._FEED_KEYSET_FILE, "w", encoding="utf-8") as handle:
            _json.dump(sign_key_set(document, root), handle)
        return root, feed

    async def test_it_is_reachable_without_a_session(self):
        self._install()
        async with httpx.AsyncClient(transport=self.transport, base_url="http://testserver.local") as client:
            response = await client.get("/api/cascade/feed/keys")
        self.assertEqual(response.status_code, 200)
        self.assertIn("sig", response.json())
        self.assertIn("max-age=300", response.headers.get("cache-control", ""))

    async def test_the_bytes_are_served_verbatim_so_the_signature_still_checks(self):
        """Re-serializing the document on the way out would break every signature."""
        from engine.cascade_feed import verify_key_set

        root, _ = self._install()
        async with httpx.AsyncClient(transport=self.transport, base_url="http://testserver.local") as client:
            response = await client.get("/api/cascade/feed/keys")
        verify_key_set(response.json(), root.public_key_b64())

    async def test_no_key_set_installed_is_a_404_not_a_crash(self):
        async with httpx.AsyncClient(transport=self.transport, base_url="http://testserver.local") as client:
            response = await client.get("/api/cascade/feed/keys")
        self.assertEqual(response.status_code, 404)

    async def test_an_unreadable_key_set_is_a_404_not_a_500(self):
        with open(self.app_module._FEED_KEYSET_FILE, "w", encoding="utf-8") as handle:
            handle.write("{ this is not json")
        async with httpx.AsyncClient(transport=self.transport, base_url="http://testserver.local") as client:
            response = await client.get("/api/cascade/feed/keys")
        self.assertEqual(response.status_code, 404)

    async def test_health_warns_before_the_key_set_expires(self):
        import time as _time

        self._install(issued_at=int(_time.time()) - 27 * 86400)
        async with httpx.AsyncClient(transport=self.transport, base_url="http://testserver.local") as client:
            body = (await client.get("/api/health")).json()
        self.assertIn("day(s)", body["feed_keyset"]["warning"])

    async def test_health_stays_quiet_when_no_feed_is_installed(self):
        async with httpx.AsyncClient(transport=self.transport, base_url="http://testserver.local") as client:
            body = (await client.get("/api/health")).json()
        self.assertNotIn("feed_keyset", body)


class FeedPublisherWiringTests(unittest.TestCase):
    """That the feed is OFF by default, and that when switched on it actually
    fires. Easy to build all of this and have the wiring silently never run."""

    def setUp(self):
        self.app_module = import_module("app")
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._saved = {
            "keyset": self.app_module._FEED_KEYSET_FILE,
            "state_dir": self.app_module._STATE_DIR,
            "state_db": self.app_module._STATE_DB_FILE,
            "publisher": self.app_module._cascade_feed_publisher,
            "checked": self.app_module._cascade_feed_publisher_checked,
        }
        self.addCleanup(self._restore)
        self.app_module._STATE_DIR = self._tmp.name
        self.app_module._FEED_KEYSET_FILE = os.path.join(self._tmp.name, "feed_keyset.json")
        self.app_module._STATE_DB_FILE = os.path.join(self._tmp.name, "state.db")
        self._reset_publisher()

    def _restore(self):
        self.app_module._FEED_KEYSET_FILE = self._saved["keyset"]
        self.app_module._STATE_DIR = self._saved["state_dir"]
        self.app_module._STATE_DB_FILE = self._saved["state_db"]
        self.app_module._cascade_feed_publisher = self._saved["publisher"]
        self.app_module._cascade_feed_publisher_checked = self._saved["checked"]

    def _reset_publisher(self):
        self.app_module._cascade_feed_publisher = None
        self.app_module._cascade_feed_publisher_checked = False

    def _install_feed(self):
        import json as _json
        import time as _time

        from cryptography.hazmat.primitives import serialization

        from engine.cascade_feed import ROOT_KID, FeedSigner, build_key_set, sign_key_set

        root = FeedSigner.generate(ROOT_KID)
        feed = FeedSigner.generate("cf-feed-2026a")
        now = int(_time.time())
        document = build_key_set(
            [{"kid": feed.kid, "public": feed.public_key_b64(), "not_before": now, "not_after": now + 90 * 86400}],
            issued_at=now,
        )
        with open(self.app_module._FEED_KEYSET_FILE, "w", encoding="utf-8") as handle:
            _json.dump(sign_key_set(document, root), handle)
        pem = feed._key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        with open(os.path.join(self._tmp.name, f"feed_key_{feed.kid}.pem"), "wb") as handle:
            handle.write(pem)
        return feed

    @staticmethod
    def _live_status():
        return {
            "campaigns": [
                {
                    "campaign_id": "casc_SOLUSDT_1",
                    "symbol": "SOLUSDT",
                    "mode": "live",
                    "created_at": "2026-08-03 19:47:00",
                    "mother_high": 178.42,
                    "mother_low": 174.10,
                    "mother_timestamp": 1785400800,
                    "state": "TRENDLINE_ACTIVE",
                    "timeframe": "5m",
                    "trendlines": [],
                    "legs": [],
                    "capital_usd": 2000.0,
                }
            ],
            "closed_campaigns": [],
        }

    def test_the_feed_is_off_unless_the_flag_is_set(self):
        self._install_feed()
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("CRYPTOFORGE_FEED_ENABLED", None)
            self.assertIsNone(self.app_module._get_cascade_feed_publisher())

    def test_enabled_without_a_key_set_publishes_nothing_and_does_not_crash(self):
        with patch.dict(os.environ, {"CRYPTOFORGE_FEED_ENABLED": "true"}):
            self.assertIsNone(self.app_module._get_cascade_feed_publisher())

    def test_enabled_with_a_key_set_but_no_private_key_publishes_nothing(self):
        import json as _json
        import time as _time

        from engine.cascade_feed import ROOT_KID, FeedSigner, build_key_set, sign_key_set

        root = FeedSigner.generate(ROOT_KID)
        now = int(_time.time())
        document = build_key_set(
            [{"kid": "cf-feed-absent", "public": "x", "not_before": now, "not_after": now + 86400}], issued_at=now
        )
        with open(self.app_module._FEED_KEYSET_FILE, "w", encoding="utf-8") as handle:
            _json.dump(sign_key_set(document, root), handle)
        with patch.dict(os.environ, {"CRYPTOFORGE_FEED_ENABLED": "true"}):
            self.assertIsNone(self.app_module._get_cascade_feed_publisher())

    def test_switched_on_it_actually_publishes_signed_geometry(self):
        from engine.cascade_feed import FeedLog, verify_frame

        feed = self._install_feed()
        with patch.dict(os.environ, {"CRYPTOFORGE_FEED_ENABLED": "true"}):
            self.assertIsNotNone(self.app_module._get_cascade_feed_publisher())
            self.app_module._broadcast_cascade_update(self._live_status())

        frames = FeedLog(self.app_module._get_state_store()).since("SOLUSDT", 0)
        types = [verify_frame(f, {feed.kid: feed.public_key_b64()})["type"] for f in frames]
        self.assertEqual(types, ["campaign.opened", "campaign.state"])

    def test_a_paper_campaign_is_not_published_even_with_the_feed_on(self):
        from engine.cascade_feed import FeedLog

        self._install_feed()
        status = self._live_status()
        status["campaigns"][0]["mode"] = "paper"
        with patch.dict(os.environ, {"CRYPTOFORGE_FEED_ENABLED": "true"}):
            self.app_module._broadcast_cascade_update(status)
        self.assertEqual(FeedLog(self.app_module._get_state_store()).since("SOLUSDT", 0), [])

    def test_a_broken_feed_never_breaks_the_broadcast(self):
        """The engine's snapshot persistence must survive a feed that explodes."""
        self._install_feed()
        with patch.dict(os.environ, {"CRYPTOFORGE_FEED_ENABLED": "true"}):
            self.app_module._get_cascade_feed_publisher()
            with patch.object(self.app_module._cascade_feed_publisher, "publish", side_effect=RuntimeError("boom")):
                self.app_module._broadcast_cascade_update(self._live_status())  # must not raise


class _ASGIWebSocket:
    """A minimal WebSocket client that speaks ASGI directly.

    starlette's TestClient cannot be used here: it passes `app=` to httpx.Client,
    which httpx removed, so every websocket_connect raises TypeError. Three tests
    written against it PASSED for the wrong reason — they asserted that connecting
    raised, and the TypeError obliged. Driving the protocol ourselves is a dozen
    lines and tests the actual route.
    """

    def __init__(self, app, path):
        self._app = app
        self._path = path
        self._to_app = asyncio.Queue()
        self._from_app = asyncio.Queue()
        self._task = None
        self.closed = None

    async def __aenter__(self):
        scope = {
            "type": "websocket",
            "asgi": {"version": "3.0"},
            "path": self._path,
            "raw_path": self._path.encode(),
            "query_string": b"",
            "headers": [(b"host", b"testserver.local")],
            "client": ("127.0.0.1", 1234),
            "server": ("testserver.local", 80),
            "scheme": "ws",
            "subprotocols": [],
        }
        await self._to_app.put({"type": "websocket.connect"})
        self._task = asyncio.ensure_future(self._app(scope, self._to_app.get, self._from_app.put))
        first = await self._recv_raw()
        if first["type"] == "websocket.close":
            self.closed = first
        return self

    async def __aexit__(self, *exc):
        await self._to_app.put({"type": "websocket.disconnect", "code": 1000})
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await self._task

    async def _recv_raw(self):
        return await asyncio.wait_for(self._from_app.get(), timeout=5)

    async def send_json(self, payload):
        await self._to_app.put({"type": "websocket.receive", "text": json.dumps(payload)})

    async def receive_json(self):
        message = await self._recv_raw()
        if message["type"] == "websocket.close":
            self.closed = message
            raise AssertionError(f"socket closed: {message.get('reason')}")
        return json.loads(message["text"])

    async def expect_close(self, *, max_drain=50):
        """Drain to the close. A displaced socket may still have a snapshot in
        flight ahead of it, and the close is what we are asserting about."""
        if self.closed:
            return self.closed
        for _ in range(max_drain):
            message = await self._recv_raw()
            if message["type"] == "websocket.close":
                self.closed = message
                return message
        raise AssertionError("never saw a close")


class FeedStreamTests(unittest.IsolatedAsyncioTestCase):
    """The transport, driven over a real ASGI WebSocket."""

    async def asyncSetUp(self):
        self.app_module = import_module("app")
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._saved = {
            "_FEED_KEYSET_FILE": self.app_module._FEED_KEYSET_FILE,
            "_STATE_DIR": self.app_module._STATE_DIR,
            "_STATE_DB_FILE": self.app_module._STATE_DB_FILE,
            "_cascade_feed_publisher": self.app_module._cascade_feed_publisher,
            "_cascade_feed_publisher_checked": self.app_module._cascade_feed_publisher_checked,
            "_cascade_engine": getattr(self.app_module, "_cascade_engine", None),
        }
        self.addCleanup(self._restore)
        self.app_module._STATE_DIR = self._tmp.name
        self.app_module._FEED_KEYSET_FILE = os.path.join(self._tmp.name, "feed_keyset.json")
        self.app_module._STATE_DB_FILE = os.path.join(self._tmp.name, "state.db")
        self.app_module._cascade_feed_publisher = None
        self.app_module._cascade_feed_publisher_checked = False
        self.app_module._feed_streams.clear()
        self.app_module._feed_nonces.clear()

    def _restore(self):
        for attr, value in self._saved.items():
            setattr(self.app_module, attr, value)
        self.app_module._feed_streams.clear()

    def _install_feed(self):
        import json as _json
        import time as _time

        from cryptography.hazmat.primitives import serialization

        from engine.cascade_feed import ROOT_KID, FeedSigner, build_key_set, sign_key_set

        root = FeedSigner.generate(ROOT_KID)
        feed = FeedSigner.generate("cf-feed-2026a")
        now = int(_time.time())
        document = build_key_set(
            [{"kid": feed.kid, "public": feed.public_key_b64(), "not_before": now, "not_after": now + 90 * 86400}],
            issued_at=now,
        )
        with open(self.app_module._FEED_KEYSET_FILE, "w", encoding="utf-8") as handle:
            _json.dump(sign_key_set(document, root), handle)
        with open(os.path.join(self._tmp.name, f"feed_key_{feed.kid}.pem"), "wb") as handle:
            handle.write(
                feed._key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )
            )
        return feed

    def _register_buyer(self, buyer_id="buyer-7"):
        from engine.cascade_feed import FeedSigner, FeedSubscribers

        buyer = FeedSigner.generate(buyer_id)
        FeedSubscribers(self.app_module._get_state_store()).add(buyer_id, buyer.public_key_b64())
        return buyer

    def _stub_engine(self, campaigns):
        class Stub:
            def get_status(self_inner):
                return {"campaigns": campaigns, "closed_campaigns": []}

            def stop(self_inner):
                pass

        self.app_module._cascade_engine = Stub()

    @staticmethod
    def _campaign(mode="live"):
        return {
            "campaign_id": "casc_SOLUSDT_1",
            "symbol": "SOLUSDT",
            "mode": mode,
            "created_at": "2026-08-03 19:47:00",
            "mother_high": 178.42,
            "mother_low": 174.10,
            "mother_timestamp": 1785400800,
            "state": "TRENDLINE_ACTIVE",
            "timeframe": "5m",
            "trendlines": [],
            "legs": [],
            "capital_usd": 2000.0,
        }

    def _socket(self):
        return _ASGIWebSocket(self.app_module.app, "/ws/cascade-feed")

    async def test_a_registered_executor_gets_welcome_then_a_snapshot(self):
        from engine.cascade_feed import sign_handshake, verify_frame

        feed = self._install_feed()
        with patch.dict(os.environ, {"CRYPTOFORGE_FEED_ENABLED": "true"}):
            buyer = self._register_buyer()
            self._stub_engine([self._campaign()])
            async with self._socket() as ws:
                await ws.send_json(sign_handshake("buyer-7", buyer, nonce="n1"))
                welcome = await ws.receive_json()
                self.assertEqual(welcome["type"], "welcome")
                self.assertIsNone(welcome["clock_warning"])
                snapshot = await ws.receive_json()
                self.assertEqual(snapshot["type"], "snapshot")
                message = verify_frame(snapshot["frame"], {feed.kid: feed.public_key_b64()})
                self.assertEqual(message["type"], "campaign.opened")
                self.assertNotIn("capital_usd", message["payload"])
                state = await ws.receive_json()
                self.assertEqual(state["type"], "snapshot")
                self.assertEqual(
                    verify_frame(state["frame"], {feed.kid: feed.public_key_b64()})["type"], "campaign.state"
                )
                self.assertEqual((await ws.receive_json())["type"], "snapshot.end")

    async def test_an_unregistered_machine_is_closed_out(self):
        from engine.cascade_feed import FeedSigner, sign_handshake

        self._install_feed()
        with patch.dict(os.environ, {"CRYPTOFORGE_FEED_ENABLED": "true"}):
            self._stub_engine([])
            stranger = FeedSigner.generate("buyer-99")
            async with self._socket() as ws:
                await ws.send_json(sign_handshake("buyer-99", stranger, nonce="n1"))
                closed = await ws.expect_close()
        self.assertEqual(closed["code"], 4003)
        self.assertIn("not registered", closed["reason"])

    async def test_a_forged_signature_is_closed_out(self):
        from engine.cascade_feed import FeedSigner, sign_handshake

        self._install_feed()
        with patch.dict(os.environ, {"CRYPTOFORGE_FEED_ENABLED": "true"}):
            self._register_buyer()
            self._stub_engine([])
            impostor = FeedSigner.generate("buyer-7")
            async with self._socket() as ws:
                await ws.send_json(sign_handshake("buyer-7", impostor, nonce="n1"))
                closed = await ws.expect_close()
        self.assertEqual(closed["code"], 4003)

    async def test_a_lapsed_subscription_cannot_open_a_stream(self):
        from engine.cascade_feed import FeedSubscribers, sign_handshake

        self._install_feed()
        with patch.dict(os.environ, {"CRYPTOFORGE_FEED_ENABLED": "true"}):
            buyer = self._register_buyer()
            FeedSubscribers(self.app_module._get_state_store()).set_status("buyer-7", "lapsed")
            self._stub_engine([])
            async with self._socket() as ws:
                await ws.send_json(sign_handshake("buyer-7", buyer, nonce="n1"))
                closed = await ws.expect_close()
        self.assertIn("lapsed", closed["reason"])

    async def test_the_stream_is_shut_when_the_feed_is_off(self):
        async with self._socket() as ws:
            closed = await ws.expect_close()
        self.assertEqual(closed["code"], 4004)

    async def test_a_skewed_clock_is_said_out_loud_at_connect(self):
        """It would otherwise join nothing at all and look perfectly healthy."""
        import time as _time

        from engine.cascade_feed import sign_handshake

        self._install_feed()
        with patch.dict(os.environ, {"CRYPTOFORGE_FEED_ENABLED": "true"}):
            buyer = self._register_buyer()
            self._stub_engine([])
            async with self._socket() as ws:
                await ws.send_json(sign_handshake("buyer-7", buyer, nonce="n1", timestamp=_time.time() + 95))
                welcome = await ws.receive_json()
                self.assertIn("skipped as too old", welcome["clock_warning"])

    async def test_a_paper_campaign_is_absent_from_the_snapshot(self):
        from engine.cascade_feed import sign_handshake

        self._install_feed()
        with patch.dict(os.environ, {"CRYPTOFORGE_FEED_ENABLED": "true"}):
            buyer = self._register_buyer()
            self._stub_engine([self._campaign(mode="paper")])
            async with self._socket() as ws:
                await ws.send_json(sign_handshake("buyer-7", buyer, nonce="n1"))
                self.assertEqual((await ws.receive_json())["type"], "welcome")
                self.assertEqual((await ws.receive_json())["type"], "snapshot.end")

    async def test_a_second_connection_displaces_the_first(self):
        """Sharing a key cannot be prevented — it can be made useless and visible."""
        from engine.cascade_feed import sign_handshake

        self._install_feed()
        with patch.dict(os.environ, {"CRYPTOFORGE_FEED_ENABLED": "true"}):
            buyer = self._register_buyer()
            self._stub_engine([])
            async with self._socket() as first:
                await first.send_json(sign_handshake("buyer-7", buyer, nonce="n1"))
                self.assertEqual((await first.receive_json())["type"], "welcome")
                async with self._socket() as second:
                    await second.send_json(sign_handshake("buyer-7", buyer, nonce="n2"))
                    self.assertEqual((await second.receive_json())["type"], "welcome")
                    closed = await first.expect_close()
        self.assertEqual(closed["code"], 4009)


class FeedSubscriberRouteTests(unittest.IsolatedAsyncioTestCase):
    """The signup step: a printed public key becomes an entitlement here.

    Session-authed like every other /api route — these decide who may receive
    the paid feed, which is exactly the kind of thing the PIN exists for.
    """

    async def asyncSetUp(self):
        self.app_module = import_module("app")
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._orig_db = self.app_module._STATE_DB_FILE
        self.app_module._STATE_DB_FILE = os.path.join(self._tmp.name, "state.db")
        self.addCleanup(lambda: setattr(self.app_module, "_STATE_DB_FILE", self._orig_db))
        self.app_module._rate_limits.clear()
        self.app_module._feed_streams.clear()
        self.transport = httpx.ASGITransport(app=self.app_module.app)

    @asynccontextmanager
    async def _client(self):
        async with httpx.AsyncClient(transport=self.transport, base_url="http://testserver.local") as client:
            await client.post("/api/auth/login", json={"password": self.app_module.AUTH_PIN})
            self._headers = {
                "X-CSRF-Token": client.cookies.get("cryptoforge_csrf") or "",
                "X-Requested-With": "XMLHttpRequest",
            }
            yield client

    @staticmethod
    def _key():
        from engine.cascade_feed import FeedSigner

        return FeedSigner.generate("buyer-x").public_key_b64()

    async def test_registration_needs_a_session(self):
        async with httpx.AsyncClient(transport=self.transport, base_url="http://testserver.local") as client:
            response = await client.post(
                "/api/cascade/feed/subscribers", json={"buyer_id": "b", "public_key": self._key()}
            )
        self.assertEqual(response.status_code, 401)

    async def test_rekeying_keeps_the_entitlement_it_found(self):
        """A buyer's laptop dies and they register again. That must not quietly
        hand them an entitlement with no end date, which is what writing a whole
        fresh record did."""
        async with self._client() as client:
            await client.post(
                "/api/cascade/feed/subscribers",
                json={
                    "buyer_id": "buyer-anita",
                    "public_key": self._key(),
                    "label": "Anita \u2014 quarterly",
                    "expires_at": 1790000000,
                },
                headers=self._headers,
            )
            response = await client.post(
                "/api/cascade/feed/subscribers",
                json={"buyer_id": "buyer-anita", "public_key": self._key(), "replace": True},
                headers=self._headers,
            )
            self.assertEqual(response.status_code, 200)
            record = response.json()["subscriber"]
        self.assertEqual(record["expires_at"], 1790000000, "the paid-up date must survive a re-key")
        self.assertEqual(record["label"], "Anita \u2014 quarterly", "an omitted label means leave it alone")

    async def test_a_revoked_buyer_cannot_re_register_their_way_back_in(self):
        """The ban is the strongest thing here and only Phil may lift it, so the
        routine act of swapping a key must not be a way around it."""
        async with self._client() as client:
            await client.post(
                "/api/cascade/feed/subscribers",
                json={"buyer_id": "buyer-banned", "public_key": self._key()},
                headers=self._headers,
            )
            await client.post(
                "/api/cascade/feed/subscribers/buyer-banned/status",
                json={"status": "revoked"},
                headers=self._headers,
            )
            response = await client.post(
                "/api/cascade/feed/subscribers",
                json={"buyer_id": "buyer-banned", "public_key": self._key(), "replace": True},
                headers=self._headers,
            )
            self.assertEqual(response.status_code, 409)
            listed = (await client.get("/api/cascade/feed/subscribers")).json()["subscribers"]
        self.assertEqual(listed[0]["status"], "revoked", "still banned")

    async def test_a_new_label_on_a_rekey_replaces_the_old_one(self):
        async with self._client() as client:
            await client.post(
                "/api/cascade/feed/subscribers",
                json={"buyer_id": "buyer-r", "public_key": self._key(), "label": "old"},
                headers=self._headers,
            )
            response = await client.post(
                "/api/cascade/feed/subscribers",
                json={"buyer_id": "buyer-r", "public_key": self._key(), "label": "new", "replace": True},
                headers=self._headers,
            )
        self.assertEqual(response.json()["subscriber"]["label"], "new")

    async def test_a_key_registers_and_lists(self):
        async with self._client() as client:
            response = await client.post(
                "/api/cascade/feed/subscribers",
                json={"buyer_id": "buyer-7", "public_key": self._key(), "label": "Phil's first buyer"},
                headers=self._headers,
            )
            self.assertEqual(response.status_code, 200)
            listed = (await client.get("/api/cascade/feed/subscribers")).json()["subscribers"]
        self.assertEqual(listed[0]["buyer_id"], "buyer-7")
        self.assertEqual(listed[0]["status"], "active")
        self.assertFalse(listed[0]["connected"])
        # No subscription yet, so nothing to count down.
        self.assertIsNone(listed[0]["days_left"])

    async def test_junk_that_is_not_an_ed25519_key_is_refused(self):
        """A registered key that cannot verify is a locked-out buyer later."""
        async with self._client() as client:
            for bad in ("not-base64!!", "aGVsbG8=", ""):
                response = await client.post(
                    "/api/cascade/feed/subscribers",
                    json={"buyer_id": "buyer-7", "public_key": bad},
                    headers=self._headers,
                )
                self.assertEqual(response.status_code, 400, bad)

    async def test_rekeying_an_existing_buyer_needs_an_explicit_replace(self):
        """A typo'd buyer_id must not silently displace a paying customer."""
        async with self._client() as client:
            await client.post(
                "/api/cascade/feed/subscribers",
                json={"buyer_id": "buyer-7", "public_key": self._key()},
                headers=self._headers,
            )
            collision = await client.post(
                "/api/cascade/feed/subscribers",
                json={"buyer_id": "buyer-7", "public_key": self._key()},
                headers=self._headers,
            )
            self.assertEqual(collision.status_code, 409)
            replaced = await client.post(
                "/api/cascade/feed/subscribers",
                json={"buyer_id": "buyer-7", "public_key": self._key(), "replace": True},
                headers=self._headers,
            )
            self.assertEqual(replaced.status_code, 200)

    async def test_lapsing_a_buyer_flows_to_the_entitlement_check(self):
        """The heartbeat check reads the same record this route writes."""
        from engine.cascade_feed import FeedSubscribers

        async with self._client() as client:
            await client.post(
                "/api/cascade/feed/subscribers",
                json={"buyer_id": "buyer-7", "public_key": self._key()},
                headers=self._headers,
            )
            response = await client.post(
                "/api/cascade/feed/subscribers/buyer-7/status",
                json={"status": "lapsed"},
                headers=self._headers,
            )
            self.assertEqual(response.status_code, 200)
        ok, reason = FeedSubscribers(self.app_module._get_state_store()).entitled("buyer-7")
        self.assertFalse(ok)
        self.assertIn("lapsed", reason)

    async def test_an_unknown_buyer_or_status_is_named(self):
        async with self._client() as client:
            missing = await client.post(
                "/api/cascade/feed/subscribers/nobody/status", json={"status": "lapsed"}, headers=self._headers
            )
            self.assertEqual(missing.status_code, 404)
            bad = await client.post(
                "/api/cascade/feed/subscribers/nobody/status", json={"status": "banished"}, headers=self._headers
            )
            self.assertEqual(bad.status_code, 400)


class RazorpayWebhookRouteTests(unittest.IsolatedAsyncioTestCase):
    """The webhook, end to end, against the real subscriber record.

    Public because Razorpay has no session — its HMAC over the raw body IS the
    authentication, which is why the first test is that an unsigned call gets
    nowhere.
    """

    async def asyncSetUp(self):
        self.app_module = import_module("app")
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._orig_db = self.app_module._STATE_DB_FILE
        self.app_module._STATE_DB_FILE = os.path.join(self._tmp.name, "state.db")
        self.addCleanup(lambda: setattr(self.app_module, "_STATE_DB_FILE", self._orig_db))
        self.app_module._rate_limits.clear()
        self.transport = httpx.ASGITransport(app=self.app_module.app)
        self.subscription = {
            "id": "sub_123",
            "status": "active",
            "current_end": 1785900000,
            "notes": {"buyer_id": "buyer-7"},
        }
        self._register("buyer-7")

    def _register(self, buyer_id, status="active"):
        from engine.cascade_feed import FeedSigner, FeedSubscribers

        subs = FeedSubscribers(self.app_module._get_state_store())
        subs.add(buyer_id, FeedSigner.generate(buyer_id).public_key_b64())
        if status != "active":
            subs.set_status(buyer_id, status)

    def _record(self, buyer_id="buyer-7"):
        from engine.cascade_feed import FeedSubscribers

        return FeedSubscribers(self.app_module._get_state_store()).get(buyer_id)

    async def _post(self, *, body=None, sign_with="whsec_test", headers=None):
        import hashlib
        import hmac as _hmac

        raw = (
            body
            if body is not None
            else json.dumps(
                {
                    "event": "subscription.charged",
                    "payload": {"subscription": {"entity": {"id": "sub_123", "status": "active"}}},
                }
            ).encode()
        )
        signature = _hmac.new(sign_with.encode(), raw, hashlib.sha256).hexdigest() if sign_with else "bad"
        sent = {"X-Razorpay-Signature": signature, "Content-Type": "application/json", **(headers or {})}
        with patch.dict(
            os.environ,
            {
                "RAZORPAY_WEBHOOK_SECRET": "whsec_test",
                "RAZORPAY_KEY_ID": "id",
                "RAZORPAY_KEY_SECRET": "secret",
            },
        ):
            with patch.object(
                self.app_module,
                "_razorpay_client",
                lambda: _StubRazorpay(self.subscription),
            ):
                async with httpx.AsyncClient(transport=self.transport, base_url="http://testserver.local") as client:
                    return await client.post("/api/billing/razorpay/webhook", content=raw, headers=sent)

    async def test_an_unsigned_call_is_refused(self):
        response = await self._post(sign_with=None)
        self.assertEqual(response.status_code, 401)

    async def test_a_signature_from_the_wrong_secret_is_refused(self):
        response = await self._post(sign_with="whsec_attacker")
        self.assertEqual(response.status_code, 401)

    async def test_a_charge_extends_the_buyers_expiry(self):
        response = await self._post()
        self.assertEqual(response.status_code, 200)
        record = self._record()
        self.assertEqual(record["status"], "active")
        self.assertEqual(record["expires_at"], 1785900000 + 3 * 86400)
        self.assertEqual(record["razorpay_subscription_id"], "sub_123")

    async def test_a_redelivery_is_a_200_no_op(self):
        """Never 4xx a duplicate — that just makes Razorpay redeliver harder."""
        headers = {"X-Razorpay-Event-Id": "evt_1"}
        first = await self._post(headers=headers)
        self.assertEqual(first.status_code, 200)
        self.subscription["current_end"] = 9999999999  # would move it, if acted on
        second = await self._post(headers=headers)
        self.assertEqual(second.status_code, 200)
        self.assertTrue(second.json()["duplicate"])
        self.assertEqual(self._record()["expires_at"], 1785900000 + 3 * 86400)

    async def test_a_halted_subscription_lapses_the_buyer(self):
        self.subscription["status"] = "halted"
        await self._post()
        self.assertEqual(self._record()["status"], "lapsed")

    async def test_the_authoritative_fetch_decides_not_the_event(self):
        """A signed event claiming "charged" while Razorpay says halted must
        lapse them — otherwise a replayed old delivery revives a dead mandate."""
        self.subscription["status"] = "halted"
        raw = json.dumps(
            {
                "event": "subscription.charged",
                "payload": {"subscription": {"entity": {"id": "sub_123", "status": "active"}}},
            }
        ).encode()
        await self._post(body=raw)
        self.assertEqual(self._record()["status"], "lapsed")

    async def test_a_revoked_buyer_is_not_reactivated_by_a_real_payment(self):
        self._register("buyer-7", status="revoked")
        await self._post()
        self.assertEqual(self._record()["status"], "revoked")

    async def test_an_unknown_buyer_alerts_and_still_answers_200(self):
        """A dashboard typo needs a human, not a retry."""
        self.subscription["notes"] = {"buyer_id": "buyer-typo"}
        response = await self._post()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["error"], "unknown buyer_id")
        inbox = self.app_module._notify_load()
        self.assertTrue(any("unknown buyer" in str(item.get("title", "")).lower() for item in inbox))

    async def test_a_non_subscription_event_is_skipped(self):
        raw = json.dumps({"event": "payment.captured", "payload": {}}).encode()
        response = await self._post(body=raw)
        self.assertEqual(response.status_code, 200)
        self.assertIn("skipped", response.json())

    async def test_a_failed_fetch_defers_so_razorpay_can_redeliver(self):
        """NOT remembered as processed: the fetch is what decides, so an event
        we could not resolve must stay eligible for redelivery."""
        import hashlib
        import hmac as _hmac

        raw = json.dumps(
            {"event": "subscription.charged", "payload": {"subscription": {"entity": {"id": "sub_123"}}}}
        ).encode()
        sig = _hmac.new(b"whsec_test", raw, hashlib.sha256).hexdigest()
        headers = {"X-Razorpay-Signature": sig, "X-Razorpay-Event-Id": "evt_flaky"}
        with patch.dict(os.environ, {"RAZORPAY_WEBHOOK_SECRET": "whsec_test"}):
            with patch.object(self.app_module, "_razorpay_client", lambda: _BrokenRazorpay()):
                async with httpx.AsyncClient(transport=self.transport, base_url="http://testserver.local") as client:
                    response = await client.post("/api/billing/razorpay/webhook", content=raw, headers=headers)
        self.assertEqual(response.json()["status"], "deferred")
        # The retry lands, because the failure was never recorded as handled.
        retry = await self._post(headers={"X-Razorpay-Event-Id": "evt_flaky"})
        self.assertEqual(retry.status_code, 200)
        self.assertEqual(self._record()["status"], "active")


class _StubRazorpay:
    def __init__(self, subscription):
        self._subscription = subscription

    def fetch_subscription(self, subscription_id):
        return dict(self._subscription)


class _BrokenRazorpay:
    def fetch_subscription(self, subscription_id):
        from engine.billing import BillingRefused

        raise BillingRefused("Razorpay refused the fetch (500).", status_code=502)
