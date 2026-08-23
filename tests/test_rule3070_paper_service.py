import json
import os
import tempfile
import unittest
from contextlib import asynccontextmanager
from importlib import import_module
from unittest.mock import patch

import httpx

from engine import rule3070_paper


class Rule3070InstrumentTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.out_patch = patch.object(rule3070_paper, "OUT", self.tmp.name)
        self.out_patch.start()

    def tearDown(self):
        self.out_patch.stop()
        self.tmp.cleanup()

    def test_symbols_marked_running_reads_the_intent_off_disk(self):
        """What boot resumes: books left RUNNING, never ones deliberately stopped."""
        for name, payload in (
            ("paper_state.json", {"start_ts": 1, "running": True}),  # BTC, running
            ("paper_state_ETHUSDT.json", {"start_ts": 1, "running": False}),  # stopped on purpose
            ("paper_state_SOLUSDT.json", {"start_ts": 1}),  # pre-dates the flag
            ("paper_state_XRPUSDT.json", {"start_ts": 1, "running": True}),
        ):
            with open(os.path.join(self.tmp.name, name), "w") as fh:
                json.dump(payload, fh)
        # A corrupt file must be skipped, not crash the whole boot scan.
        with open(os.path.join(self.tmp.name, "paper_state_DOGEUSDT.json"), "w") as fh:
            fh.write("{ not json")
        self.assertEqual(rule3070_paper.symbols_marked_running(), ["BTCUSDT", "XRPUSDT"])

    def test_no_state_files_means_nothing_to_resume(self):
        self.assertEqual(rule3070_paper.symbols_marked_running(), [])

    def test_stop_records_the_decision_so_boot_does_not_undo_it(self):
        service = rule3070_paper.Rule3070PaperService()
        service._state = {"start_ts": 1, "running": True}
        service._write_state()
        self.assertEqual(rule3070_paper.symbols_marked_running(), ["BTCUSDT"])
        service.stop()
        self.assertEqual(rule3070_paper.symbols_marked_running(), [])

    def test_a_shutdown_does_not_look_like_a_decision_to_stop(self):
        """The restart bug: every paper book came back stopped.

        The app's shutdown called stop() like a person would, so it wrote
        running=False for books that were merrily running — erasing the flag
        symbols_marked_running() reads at boot. On 2026-08-23 the forced deploy
        stamped False onto BTCUSDT, ETHUSDT and SOLUSDT inside one minute and
        the V-Rule page read "PAPER Stopped" with nobody having stopped it.
        """
        service = rule3070_paper.Rule3070PaperService()
        service._state = {"start_ts": 1, "running": True}
        service._write_state()
        service.stop(persist=False)  # what shutdown does now
        self.assertEqual(
            rule3070_paper.symbols_marked_running(),
            ["BTCUSDT"],
            "a process going down must not read as Phil switching the book off",
        )
        self.assertFalse(service.status().get("running"), "the thread is still stopped in memory")

    def test_six_supported_instruments_are_exposed(self):
        self.assertEqual(
            rule3070_paper.SUPPORTED_SYMBOLS,
            ("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "PAXGUSDT"),
        )
        with self.assertRaises(ValueError):
            rule3070_paper.Rule3070PaperService("BNBUSDT")

    def test_each_instrument_uses_an_isolated_book_and_btc_keeps_legacy_paths(self):
        service = rule3070_paper.Rule3070PaperService()
        self.assertEqual(service.state_path, os.path.join(self.tmp.name, "paper_state.json"))
        self.assertEqual(service.journal_path, os.path.join(self.tmp.name, "paper_journal.jsonl"))

        btc_state = {"start_ts": 101, "seen": ["btc-fill"]}
        with open(service.state_path, "w") as fh:
            json.dump(btc_state, fh)

        eth = service.select_symbol("ethusdt")
        self.assertEqual(eth["symbol"], "ETHUSDT")
        self.assertEqual(service.state_path, os.path.join(self.tmp.name, "paper_state_ETHUSDT.json"))
        self.assertEqual(service.journal_path, os.path.join(self.tmp.name, "paper_journal_ETHUSDT.jsonl"))
        self.assertFalse(os.path.exists(service.state_path))

        with open(service.journal_path, "w") as fh:
            fh.write(json.dumps({"kind": "TARGET", "net": 4.25}) + "\n")
        service.select_symbol("BTCUSDT")
        service.select_symbol("ETHUSDT")
        self.assertEqual(service.status()["closed"], {"count": 1, "net": 4.25})

        with open(os.path.join(self.tmp.name, "paper_state.json")) as fh:
            self.assertEqual(json.load(fh), btc_state)

    def test_instruments_have_independent_writer_locks(self):
        btc = rule3070_paper.Rule3070PaperService("BTCUSDT")
        eth = rule3070_paper.Rule3070PaperService("ETHUSDT")

        self.assertEqual(btc.lock_path, os.path.join(self.tmp.name, "paper.lock"))
        self.assertEqual(eth.lock_path, os.path.join(self.tmp.name, "paper_ETHUSDT.lock"))
        self.assertNotEqual(btc.lock_path, eth.lock_path)

    def test_unstarted_preview_uses_the_same_30_day_boundary_as_a_new_paper_clock(self):
        service = rule3070_paper.Rule3070PaperService("ETHUSDT")
        now = 1_786_502_400

        with patch.object(rule3070_paper.time, "time", return_value=now):
            self.assertEqual(service._history_start(), now - rule3070_paper.WARMUP_DAYS * 86400)
            self.assertEqual(service._replay_start_ts(), now)

        service._state = {"start_ts": now - 3600, "history_start_ts": now - 31 * 86400}
        self.assertEqual(service._history_start(), now - 31 * 86400)
        self.assertEqual(service._replay_start_ts(), now - 3600)

    def test_two_instrument_writers_can_run_together(self):
        btc = rule3070_paper.Rule3070PaperService("BTCUSDT")
        eth = rule3070_paper.Rule3070PaperService("ETHUSDT")

        with patch.object(rule3070_paper.Rule3070PaperService, "_loop", lambda service: service._stop.wait()):
            try:
                self.assertTrue(btc.start()["running"])
                self.assertTrue(eth.start()["running"])
                self.assertTrue(os.path.exists(btc.lock_path))
                self.assertTrue(os.path.exists(eth.lock_path))
            finally:
                btc.stop()
                eth.stop()

    def test_reset_archives_only_the_selected_instrument(self):
        service = rule3070_paper.Rule3070PaperService("ETHUSDT")
        btc_path = os.path.join(self.tmp.name, "paper_state.json")
        with open(btc_path, "w") as fh:
            json.dump({"start_ts": 11}, fh)
        with open(service.state_path, "w") as fh:
            json.dump({"start_ts": 22}, fh)

        result = service.reset()

        self.assertTrue(result["reset"])
        self.assertTrue(os.path.exists(btc_path))
        self.assertFalse(os.path.exists(service.state_path))
        self.assertTrue(os.path.exists(service.state_path + "." + result["archived_as"]))


class Rule3070BootResumeTests(unittest.IsolatedAsyncioTestCase):
    """Boot must outlast the outgoing instance, not race it.

    A blue-green deploy starts this process, drains the old one for 30s, and
    only then stops it — so the paper writer lock is still held by a LIVE pid
    when boot reaches the resume. On 2026-08-23 at 09:18:56 all three books
    failed with "Another paper writer is running (pid 1242585)", 31 seconds
    before that pid let go, and nothing retried: the books stayed marked
    running while nothing ran.
    """

    async def test_it_retries_until_the_old_instance_lets_go(self):
        app_module = import_module("app")
        attempts = {"n": 0}

        class Svc:
            def start(self_inner):
                attempts["n"] += 1
                if attempts["n"] < 3:
                    raise RuntimeError("Another paper writer is running (pid 1242585) — stop it first")
                return {"running": True}

        with (
            patch.object(app_module, "_RULE3070_RESUME_DELAY_SEC", 0),
            patch.object(app_module, "_get_rule3070_service", lambda symbol: Svc()),
            patch("engine.rule3070_paper.symbols_marked_running", lambda: ["BTCUSDT"]),
        ):
            await app_module._resume_rule3070_on_boot()
        self.assertEqual(attempts["n"], 3, "it gave up before the handover finished")

    async def test_it_gives_up_rather_than_retrying_for_ever(self):
        app_module = import_module("app")
        attempts = {"n": 0}

        class Dead:
            def start(self_inner):
                attempts["n"] += 1
                raise RuntimeError("Another paper writer is running (pid 999) — stop it first")

        with (
            patch.object(app_module, "_RULE3070_RESUME_DELAY_SEC", 0),
            patch.object(app_module, "_RULE3070_RESUME_ATTEMPTS", 4),
            patch.object(app_module, "_get_rule3070_service", lambda symbol: Dead()),
            patch("engine.rule3070_paper.symbols_marked_running", lambda: ["BTCUSDT"]),
        ):
            await app_module._resume_rule3070_on_boot()
        self.assertEqual(attempts["n"], 4, "boot must not retry for ever")

    async def test_one_stuck_book_does_not_hold_up_the_others(self):
        app_module = import_module("app")
        started = []

        class Svc:
            def __init__(self_inner, symbol):
                self_inner.symbol = symbol

            def start(self_inner):
                if self_inner.symbol == "ETHUSDT":
                    raise RuntimeError("Another paper writer is running (pid 999) — stop it first")
                started.append(self_inner.symbol)
                return {"running": True}

        with (
            patch.object(app_module, "_RULE3070_RESUME_DELAY_SEC", 0),
            patch.object(app_module, "_RULE3070_RESUME_ATTEMPTS", 2),
            patch.object(app_module, "_get_rule3070_service", Svc),
            patch("engine.rule3070_paper.symbols_marked_running", lambda: ["BTCUSDT", "ETHUSDT", "SOLUSDT"]),
        ):
            await app_module._resume_rule3070_on_boot()
        self.assertEqual(started, ["BTCUSDT", "SOLUSDT"], "the healthy books must come back once each")


class Rule3070RouteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.app_module = import_module("app")
        self.tmp = tempfile.TemporaryDirectory()
        self.original_db = self.app_module._STATE_DB_FILE
        self.original_services = getattr(self.app_module, "_rule3070_services", None)
        self.original_totp_secret = self.app_module.TOTP_SECRET
        self.app_module._STATE_DB_FILE = os.path.join(self.tmp.name, "state.db")
        self.app_module.TOTP_SECRET = ""
        self.app_module._rule3070_services = {}
        self.app_module._rate_limits.clear()
        self.out_patch = patch.object(rule3070_paper, "OUT", self.tmp.name)
        self.loop_patch = patch.object(
            rule3070_paper.Rule3070PaperService,
            "_loop",
            lambda service: service._stop.wait(),
        )
        self.out_patch.start()
        self.loop_patch.start()
        self.transport = httpx.ASGITransport(app=self.app_module.app)

    async def asyncTearDown(self):
        for service in self.app_module._rule3070_services.values():
            service.stop()
        self.app_module._STATE_DB_FILE = self.original_db
        self.app_module.TOTP_SECRET = self.original_totp_secret
        if self.original_services is None:
            self.app_module.__dict__.pop("_rule3070_services", None)
        else:
            self.app_module._rule3070_services = self.original_services
        self.loop_patch.stop()
        self.out_patch.stop()
        self.tmp.cleanup()

    @asynccontextmanager
    async def client(self):
        async with httpx.AsyncClient(transport=self.transport, base_url="http://testserver.local") as client:
            login = await client.post("/api/auth/login", json={"password": self.app_module.AUTH_PIN})
            self.assertEqual(login.status_code, 200)
            headers = {
                "X-CSRF-Token": client.cookies.get("cryptoforge_csrf") or "",
                "X-Requested-With": "XMLHttpRequest",
            }
            yield client, headers

    async def test_switching_and_starting_another_coin_keeps_btc_running(self):
        async with self.client() as (client, headers):
            btc = await client.post(
                "/api/rule3070/start",
                json={"symbol": "BTCUSDT"},
                headers=headers,
            )
            eth = await client.post(
                "/api/rule3070/start",
                json={"symbol": "ETHUSDT"},
                headers=headers,
            )

            self.assertEqual(btc.status_code, 200)
            self.assertEqual(eth.status_code, 200)
            self.assertEqual(eth.json()["running_symbols"], ["BTCUSDT", "ETHUSDT"])

            stopped_eth = await client.post(
                "/api/rule3070/stop",
                json={"symbol": "ETHUSDT"},
                headers=headers,
            )
            btc_status = await client.get("/api/rule3070/status?symbol=BTCUSDT")

            self.assertEqual(stopped_eth.status_code, 200)
            self.assertFalse(stopped_eth.json()["running"])
            self.assertEqual(stopped_eth.json()["running_symbols"], ["BTCUSDT"])
            self.assertTrue(btc_status.json()["running"])
            self.assertEqual(btc_status.json()["running_symbols"], ["BTCUSDT"])


if __name__ == "__main__":
    unittest.main()
