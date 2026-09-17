"""17-Sep-2026, three things from one screenshot of 18 stopped V-Rule positions.

  "I want to market sell all of this... I am not having a select all"
  "I am getting wrong telegram alerts that it is cascade trade"
  "This is bombarding my server"

1. POST /api/cascade/liquidate-many sells many stranded positions at once,
   each through its own engine's liquidate_campaign.
2. The automatic strategies (Cascade-Auto, V-Rule) no longer alert about their
   own bookkeeping — starts, restarts, retirements, closes with nothing bought.
   Every alert in the hour before the report was one of those.
3. Routine Scalp saves are queued off the event loop (7 of 40 py-spy samples
   of the main thread were in that SQLite write).
"""

import os
import re
import tempfile
import unittest
from contextlib import asynccontextmanager
from importlib import import_module

import httpx
from state_quiesce import quiesce_state_writers

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class _Campaign:
    def __init__(self, symbol, seq, mode="paper"):
        self.symbol, self.seq, self.mode = symbol, seq, mode


class SellAllRouteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.app = import_module("app")
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        orig_db = self.app._STATE_DB_FILE
        self.addCleanup(lambda: setattr(self.app, "_STATE_DB_FILE", orig_db))
        self.app._STATE_DB_FILE = os.path.join(self._tmp.name, "state.db")
        self.addCleanup(quiesce_state_writers, self.app._STATE_DB_FILE)
        self.app._rate_limits.clear()
        self.transport = httpx.ASGITransport(app=self.app.app)
        self.sold, self.persisted, self.alerts = [], [], []
        outer = self

        class Engine:
            def __init__(self, name, campaigns, refuse=()):
                self.name = name
                self.campaigns = campaigns
                self.refuse = set(refuse)

            async def liquidate_campaign(self, cid):
                if cid in self.refuse:
                    return {"error": "This campaign is not holding anything"}
                outer.sold.append((self.name, cid))
                return {"quantity": 1.0, "price": 100.0}

        self.live = Engine("live", {"L1": _Campaign("BTCUSDT", 1, "live")})
        self.auto = Engine("auto", {"A1": _Campaign("ETHUSDT", 2)})
        self.vrule = Engine("vrule", {"V1": _Campaign("SOLUSDT", 3), "V2": _Campaign("SOLUSDT", 4)}, refuse={"V2"})
        for attr, value in (
            ("_get_cascade_engine", lambda: self.live),
            ("_get_auto_fib_engine", lambda: self.auto),
            ("_get_vrule_engine", lambda: self.vrule),
            ("_persist_cascade_runtime_snapshot", lambda eng: self.persisted.append("cascade")),
            ("_save_auto_fib", lambda: self.persisted.append("auto")),
            ("_save_vrule", lambda: self.persisted.append("vrule")),
        ):
            original = getattr(self.app, attr)
            setattr(self.app, attr, value)
            self.addCleanup(lambda a=attr, o=original: setattr(self.app, a, o))
        orig_alert = self.app.alerter.alert
        self.app.alerter.alert = lambda title, body, level="error": self.alerts.append(title)
        self.addCleanup(lambda: setattr(self.app.alerter, "alert", orig_alert))

    @asynccontextmanager
    async def _client(self):
        async with httpx.AsyncClient(transport=self.transport, base_url="http://testserver.local") as client:
            await client.post("/api/auth/login", json={"password": self.app.AUTH_PIN})
            self.headers = {
                "X-CSRF-Token": client.cookies.get("cryptoforge_csrf") or "",
                "X-Requested-With": "XMLHttpRequest",
            }
            yield client

    async def _sell(self, ids):
        async with self._client() as client:
            return await client.post("/api/cascade/liquidate-many", json={"campaign_ids": ids}, headers=self.headers)

    async def test_every_position_is_sold_by_its_own_engine(self):
        r = await self._sell(["A1", "V1", "L1"])
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(self.sold, [("auto", "A1"), ("vrule", "V1"), ("live", "L1")])
        self.assertEqual(r.json()["sold"], 3)
        self.assertEqual(sorted(self.persisted), ["auto", "cascade", "vrule"], "each engine saved once")

    async def test_one_refusal_does_not_stop_the_rest(self):
        r = await self._sell(["V2", "V1", "missing"])
        body = r.json()
        self.assertEqual(body["sold"], 1)
        self.assertEqual(body["failed"], 2)
        by_id = {row["campaign_id"]: row for row in body["results"]}
        self.assertIn("not holding", by_id["V2"]["error"])
        self.assertIn("not found", by_id["missing"]["error"])
        self.assertTrue(by_id["V1"]["ok"])

    async def test_paper_sales_send_no_telegram_live_sales_send_one(self):
        await self._sell(["A1", "V1"])
        self.assertEqual(self.alerts, [], "paper sales are bookkeeping, not a phone alert")
        self.app._rate_limits.clear()
        await self._sell(["L1"])
        self.assertEqual(len(self.alerts), 1)
        self.assertIn("1 position(s) sold", self.alerts[0])

    async def test_bad_requests_are_refused(self):
        async with self._client() as client:
            for body in (
                {},
                {"campaign_ids": []},
                {"campaign_ids": "A1"},
                {"campaign_ids": [f"x{i}" for i in range(51)]},
            ):
                self.app._rate_limits.clear()
                r = await client.post("/api/cascade/liquidate-many", json=body, headers=self.headers)
                self.assertEqual(r.status_code, 400, body)
        self.assertEqual(self.sold, [])

    async def test_duplicate_ids_sell_once(self):
        await self._sell(["A1", "A1", " A1 "])
        self.assertEqual(self.sold, [("auto", "A1")])


class StrategyAlertTests(unittest.TestCase):
    def setUp(self):
        self.app = import_module("app")
        self.pushed, self.telegram = [], []
        for attr, value in (
            ("_notify_push", lambda *a, **k: self.pushed.append(a[1] if len(a) > 1 else k.get("title"))),
        ):
            original = getattr(self.app, attr)
            setattr(self.app, attr, value)
            self.addCleanup(lambda a=attr, o=original: setattr(self.app, a, o))
        orig = self.app.alerter.alert
        self.app.alerter.alert = lambda title, body, level="error": self.telegram.append(title)
        self.addCleanup(lambda: setattr(self.app.alerter, "alert", orig))

    def test_bookkeeping_alerts_stay_off_the_phone(self):
        for what in ("Auto-restarted", "Minor MC retired at the break", "Escalated timeframe", "Restart chain stopped"):
            self.app._strategy_alert(f"Cascade-Auto · PAXGUSDT #408 — {what}", "body", "warn")
        self.assertEqual(self.telegram, [])
        self.assertEqual(self.pushed, [])

    def test_money_and_failures_still_reach_the_phone(self):
        for what in (
            "ENTRY filled",
            "TARGET hit",
            "Order FAILED",
            "Market sell FAILED",
            "Position missing on the exchange",
        ):
            self.app._strategy_alert(f"V-Rule · SOLUSDT #3 — {what}", "body", "warn")
        self.assertEqual(len(self.telegram), 5)

    def test_engine_wide_alarms_still_reach_the_phone(self):
        self.app._strategy_alert("Cascade engine STALLED", "body", "error")
        self.assertEqual(self.telegram, ["Cascade engine STALLED"])

    def test_strategy_start_and_stop_lines_are_not_alerts(self):
        for level in ("start", "stop", "info", "order"):
            self.app._strategy_event({"mode": "live", "level": level, "message": "m", "symbol": "BTCUSDT"})
        self.assertEqual(self.pushed, [])
        self.app._strategy_event({"mode": "live", "level": "error", "message": "Fib rejected", "symbol": "BTCUSDT"})
        self.assertEqual(len(self.pushed), 1)

    def test_a_close_with_nothing_bought_is_not_announced(self):
        untraded = {
            "campaign_id": "c1",
            "mode": "live",
            "close_reason": "mother_broken",
            "symbol": "BTCUSDT",
            "seq": 407,
            "strategy": "auto-cascade-fib",
            "filled_base_qty": 0,
            "realized_pnl": 0,
            "rounds": [],
            "all_fills": [],
        }
        self.app._strategy_closed(untraded)
        self.assertEqual(self.telegram, [])
        traded = dict(untraded, campaign_id="c2", rounds=[{"pnl": 1.2}], realized_pnl=1.2, close_reason="tp_filled")
        self.app._strategy_closed(traded)
        self.assertEqual(len(self.telegram), 1)

    def test_both_strategy_engines_use_the_quiet_filter(self):
        src = open(os.path.join(ROOT, "app.py"), encoding="utf-8").read()
        self.assertEqual(src.count("on_alert=_strategy_alert"), 2)
        self.assertEqual(src.count("on_event=_strategy_event"), 2)

    def test_the_hand_driven_cascade_keeps_every_alert(self):
        src = open(os.path.join(ROOT, "app.py"), encoding="utf-8").read()
        self.assertIn("on_event=_cascade_persist_event", src)


class ScalpSaveIsOffTheLoopTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_routine_update_is_queued_not_written(self):
        app = import_module("app")
        queued, written = [], []
        orig_q, orig_s = app._queue_runtime_snapshot, app._save_scalp_runtime
        app._queue_runtime_snapshot = lambda bucket, snap, key="current": queued.append(bucket)
        app._save_scalp_runtime = lambda state: written.append(state)
        try:
            await app._broadcast_scalp_update({"open_trades": [], "pending_entries": []})
        finally:
            app._queue_runtime_snapshot, app._save_scalp_runtime = orig_q, orig_s
        self.assertEqual(queued, [app._BUCKET_SCALP_RUNTIME])
        self.assertEqual(written, [], "the SQLite write must not happen on the event loop")

    def test_shutdown_writes_the_last_word(self):
        src = open(os.path.join(ROOT, "app.py"), encoding="utf-8").read()
        block = src[src.index("async def _shutdown_runtime_engines") :]
        self.assertLess(block.index('"scalp engine"'), block.index('"persist scalp runtime"'))


class SellAllButtonTests(unittest.TestCase):
    def setUp(self):
        self.js = open(os.path.join(ROOT, "static", "cryptoforge-app.js"), encoding="utf-8").read()

    def test_the_table_offers_sell_all_for_two_or_more_stopped_rows(self):
        body = re.search(r"function cfRenderCascadeTrades\(.*?\n\}\n", self.js, re.S).group(0)
        self.assertIn("stranded.length >= 2", body)
        self.assertIn("cfCascadeLiquidateAll(", body)
        self.assertIn("read-only-hide", body, "a viewer must not see a sell control")

    def test_sell_all_is_one_request_after_a_confirmation(self):
        fn = re.search(r"async function cfCascadeLiquidateAll\(.*?\n\}\n", self.js, re.S).group(0)
        self.assertIn("cfConfirm(", fn)
        self.assertLess(fn.index("cfConfirm("), fn.index("cfApiFetch("))
        self.assertEqual(fn.count("cfApiFetch("), 1)
        self.assertIn("/api/cascade/liquidate-many", fn)
        self.assertIn("LIVE", fn, "the confirmation must say when real coin is being sold")


if __name__ == "__main__":
    unittest.main()
