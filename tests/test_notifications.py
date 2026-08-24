"""The trade-alert inbox: what raises an alert, and what makes it go quiet.

The point of these records is that they outlive the tab. A fill that happened
while nobody was looking must still be on screen at the next page load, and it
must survive the engine re-logging the same line after a restart without
turning into two cards to dismiss.
"""

import os
import tempfile
import unittest
from contextlib import asynccontextmanager
from importlib import import_module

import httpx


class NotificationInboxTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.app_module = import_module("app")
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._orig_state_db = self.app_module._STATE_DB_FILE
        self.addCleanup(self._restore)

        self.app_module._STATE_DB_FILE = os.path.join(self._tmp.name, "cryptoforge_state.db")
        self.app_module._rate_limits.clear()
        self.transport = httpx.ASGITransport(app=self.app_module.app)

    def _restore(self):
        self.app_module._STATE_DB_FILE = self._orig_state_db

    @asynccontextmanager
    async def _client(self):
        async with httpx.AsyncClient(transport=self.transport, base_url="http://testserver.local") as client:
            await client.post("/api/auth/login", json={"password": self.app_module.AUTH_PIN})
            self._csrf_headers = {
                "X-CSRF-Token": client.cookies.get("cryptoforge_csrf") or "",
                "X-Requested-With": "XMLHttpRequest",
            }
            yield client

    async def test_push_is_stored_unseen_and_survives_a_reload(self):
        app = self.app_module
        app._notify_push("cascade_fill", "BTCUSDT — Entry filled", "Bought $200 at 61,000")
        async with self._client() as client:
            first = await client.get("/api/notifications")
            self.assertEqual(first.status_code, 200)
            self.assertEqual(first.json()["unseen"], 1)
            # A second load is the reload: the alert is still waiting.
            second = await client.get("/api/notifications")
        payload = second.json()
        self.assertEqual(payload["unseen"], 1)
        self.assertEqual(payload["items"][0]["title"], "BTCUSDT — Entry filled")
        self.assertFalse(payload["items"][0]["seen"])

    async def test_same_event_twice_queues_one_card(self):
        app = self.app_module
        first = app._notify_push("cascade_fill", "Entry filled", "Bought $200 at 61,000", dedupe_key="fill-1")
        repeat = app._notify_push("cascade_fill", "Entry filled", "Bought $200 at 61,000", dedupe_key="fill-1")
        self.assertIsNotNone(first)
        self.assertIsNone(repeat)
        self.assertEqual(len(app._notify_load()), 1)

    async def test_ack_clears_only_what_was_acknowledged(self):
        app = self.app_module
        one = app._notify_push("trade_entry", "Live entry", "BTCUSDT", dedupe_key="a")
        app._notify_push("trade_exit", "Live exit", "BTCUSDT", dedupe_key="b")
        async with self._client() as client:
            acked = await client.post("/api/notifications/ack", json={"ids": [one["id"]]}, headers=self._csrf_headers)
            self.assertEqual(acked.status_code, 200)
            self.assertEqual(acked.json(), {"acknowledged": 1, "unseen": 1})

            remaining = await client.get("/api/notifications")
            self.assertEqual([row["title"] for row in remaining.json()["items"]], ["Live exit"])

            cleared = await client.post("/api/notifications/ack", json={"all": True}, headers=self._csrf_headers)
            self.assertEqual(cleared.json(), {"acknowledged": 1, "unseen": 0})

            empty = await client.get("/api/notifications")
        self.assertEqual(empty.json()["items"], [])
        # Acknowledged is not deleted — the history is still readable.
        self.assertEqual(empty.json()["total"], 2)

    async def test_cascade_event_levels_that_raise_an_alert(self):
        app = self.app_module
        app._cascade_persist_event(
            {"level": "stop", "message": "Campaign stopped by hand", "symbol": "BTCUSDT", "campaign_id": "c1"}
        )
        app._cascade_persist_event(
            {"level": "error", "message": "Broker rejected the order", "symbol": "BTCUSDT", "campaign_id": "c1"}
        )
        # Geometry chatter is not worth a card that has to be dismissed by hand.
        app._cascade_persist_event(
            {"level": "trendline", "message": "New trendline drawn", "symbol": "BTCUSDT", "campaign_id": "c1"}
        )
        titles = [row["title"] for row in app._notify_load()]
        # An event with no strategy on it came from the live book, which is
        # what "Cascade-Hybrid" names (2026-08-24: three engines, one chat).
        self.assertEqual(
            titles,
            ["Cascade-Hybrid · BTCUSDT — Campaign stopped", "Cascade-Hybrid · BTCUSDT — Cascade error"],
        )

    async def test_money_moving_events_are_announced_once(self):
        """A fill and a target already raise a richer engine alert of their own.

        Mapping them here too put two cards on screen for one entry — same
        numbers, same second, different wording.
        """
        app = self.app_module
        app._cascade_persist_event(
            {"level": "fill", "message": "Bought $200 at 61,000", "symbol": "BTCUSDT", "campaign_id": "c1"}
        )
        app._cascade_persist_event(
            {"level": "round", "message": "Round 1 closed at TP", "symbol": "BTCUSDT", "campaign_id": "c1"}
        )
        self.assertEqual(app._notify_load(), [])
        # The engine's own alert is what the operator sees, and it carries the
        # campaign number and LIVE/PAPER that the event log line cannot.
        app._cascade_alert("Cascade ENTRY filled", "SOLUSDT #72 (LIVE) — MAJOR MC", level="success")
        rows = app._notify_load()
        self.assertEqual([row["title"] for row in rows], ["Cascade ENTRY filled"])
        self.assertEqual(rows[0]["level"], "success")

    async def test_replayed_cascade_event_does_not_queue_twice(self):
        app = self.app_module
        event = {"level": "stop", "message": "Campaign stopped", "symbol": "BTCUSDT", "campaign_id": "c1"}
        app._cascade_persist_event(dict(event, timestamp="2026-07-31 10:00:00"))
        # Recovery replays the candle and re-logs the line at a new second.
        app._cascade_persist_event(dict(event, timestamp="2026-07-31 10:04:00"))
        self.assertEqual(len(app._notify_load()), 1)

    async def test_scalp_entry_and_exit_raise_alerts(self):
        app = self.app_module
        app._scalp_persist_event({"level": "entry", "msg": "Bought 0.01 BTC at 61,000", "ts": "t1"})
        app._scalp_persist_event({"level": "info", "msg": "Watching BTCUSDT", "ts": "t2"})
        app._scalp_persist_event({"level": "exit", "msg": "Sold 0.01 BTC at 61,900", "ts": "t3"})
        self.assertEqual([row["kind"] for row in app._notify_load()], ["scalp_entry", "scalp_exit"])

    async def test_live_entry_and_exit_raise_alerts(self):
        app = self.app_module
        app._alert_state.pop("run-1", None)
        self.addCleanup(app._alert_state.pop, "run-1", None)
        app._check_trade_alerts(
            "run-1",
            "Live",
            {
                "type": "entry",
                "open_positions": 1,
                "closed_trades": 0,
                "open_trades": [{"symbol": "BTCUSDT", "side": "LONG", "entry_price": 61000}],
            },
        )
        app._check_trade_alerts(
            "run-1",
            "Live",
            {
                "type": "exit",
                "open_positions": 0,
                "closed_trades": 1,
                "recent_trades": [{"id": 1, "symbol": "BTCUSDT", "pnl": 42.5, "exit_reason": "target"}],
                "total_pnl": 42.5,
            },
        )
        rows = app._notify_load()
        self.assertEqual([row["kind"] for row in rows], ["trade_entry", "trade_exit"])
        self.assertEqual(rows[1]["level"], "success")

    async def test_inbox_is_capped(self):
        app = self.app_module
        for i in range(app._NOTIFY_MAX + 10):
            app._notify_push("trade_entry", f"Alert {i}", "", dedupe_key=f"k{i}")
        rows = app._notify_load()
        self.assertEqual(len(rows), app._NOTIFY_MAX)
        self.assertEqual(rows[-1]["title"], f"Alert {app._NOTIFY_MAX + 9}")


if __name__ == "__main__":
    unittest.main()
