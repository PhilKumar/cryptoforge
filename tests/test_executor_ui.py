"""The page the buyer keeps open.

Two properties carry this file. The boundary: the server binds loopback and
refuses anything that is not loopback, because a page showing positions and
exposure must not leak onto the LAN, and the boundary IS the interface — there
is no auth because there is no remote access to authenticate.

And the ordering: the armed exposure leads the page for the same reason it
leads report.py — it is the number the buyer is actually relying on.
"""

import json
import re
import threading
import unittest
import urllib.request
from unittest import mock

from executor import ui
from executor.feed_client import FeedClient
from executor.orders import Fill
from executor.power import WINDOWS
from executor.runtime import ExecutorRuntime, RuntimeConfig
from executor.ui import PAGE, UIServer, UIState, campaigns_view
from tests.test_executor_runtime import NOW, FakeExchange, FakeMarket


class UIStateTests(unittest.TestCase):
    def setUp(self):
        self.state = UIState()

    def test_the_snapshot_leads_with_what_report_leads_with(self):
        self.state.set_status({"armed_exposure_usd": 7.25, "opening_new": True})
        snapshot = self.state.snapshot()
        self.assertIn("$7.25", snapshot["lines"][0])

    def test_events_keep_only_the_recent_past(self):
        """A strip, not the audit trail — that is the exchange's history."""
        for index in range(150):
            self.state.add_event(f"event {index}")
        events = self.state.snapshot()["events"]
        self.assertEqual(len(events), 100)
        self.assertEqual(events[0]["line"], "event 149")

    def test_the_windows_asymmetry_reaches_the_page(self):
        state = UIState(power=WINDOWS)
        state.set_status({"armed_exposure_usd": 7.25, "opening_new": True})
        self.assertIn("2 seconds", state.snapshot()["advice"])

    def test_the_disclosure_is_always_present(self):
        self.assertIn("may still", self.state.snapshot()["disclosure"])

    def test_concurrent_writers_do_not_corrupt_a_snapshot(self):
        def hammer():
            for index in range(300):
                self.state.add_event(f"x{index}")
                self.state.set_status({"armed_exposure_usd": index, "opening_new": True})

        threads = [threading.Thread(target=hammer) for _ in range(4)]
        for thread in threads:
            thread.start()
        for _ in range(200):
            self.state.snapshot()
        for thread in threads:
            thread.join()
        self.assertIsInstance(self.state.snapshot()["status"], dict)


class CampaignViewTests(unittest.TestCase):
    """The rows come from the runtime's own book, not a parallel bookkeeping."""

    def _runtime(self):
        client = FeedClient(public_keys={}, keyset_fetched_at=NOW, now_fn=lambda: NOW)
        return ExecutorRuntime(
            client=client,
            adapter=FakeExchange(),
            market=FakeMarket(),
            config=RuntimeConfig(capital_usd=5000.0),
            now_fn=lambda: NOW,
        )

    def test_a_position_shows_its_average_and_target_state(self):
        from executor.orders import CampaignOrders

        runtime = self._runtime()
        orders = runtime.book.track(
            CampaignOrders(campaign_id="c1", symbol="SOLUSDT", mother_high=178.42, exchange="binance")
        )
        orders.on_entry_filled(Fill(price=162.0, quantity=0.05, timestamp=1))
        from executor.feed_client import FollowedCampaign

        runtime._client.campaigns["c1"] = FollowedCampaign(
            campaign_id="c1",
            symbol="SOLUSDT",
            exchange="binance",
            created_at=int(NOW),
            mother_high=178.42,
            mother_low=174.10,
            mother_timestamp=1,
            timeframe="5m",
            state="TRENDLINE_ACTIVE",
            model_version=21,
            joined=True,
        )
        rows = campaigns_view(runtime)
        self.assertEqual(rows[0]["position_qty"], 0.05)
        self.assertAlmostEqual(rows[0]["avg_entry"], 162.0)
        self.assertFalse(rows[0]["exit_resting"])

    def test_a_skipped_campaign_appears_with_its_reason(self):
        from executor.feed_client import FollowedCampaign

        runtime = self._runtime()
        runtime._client.campaigns["old"] = FollowedCampaign(
            campaign_id="old",
            symbol="SOLUSDT",
            exchange="binance",
            created_at=int(NOW) - 900,
            mother_high=178.42,
            mother_low=174.10,
            mother_timestamp=1,
            timeframe="5m",
            state="TRENDLINE_ACTIVE",
            model_version=21,
            joined=False,
            skip_reason="Started 900s ago — past the join window.",
        )
        rows = campaigns_view(runtime)
        self.assertEqual(rows[0]["state"], "skipped")
        self.assertIn("join window", rows[0]["skip_reason"])

    def test_an_old_skip_never_reaches_the_activity_log(self):
        """One connect emits one campaign event per campaign the server has
        ever run. Logging the old ones buried everything else."""
        from executor.ui import worth_logging

        self.assertFalse(worth_logging("campaign", {"joined": False, "skipped_as_old": True}))
        self.assertFalse(worth_logging("campaign", {"joined": False, "skipped_unsubscribed": True}))
        self.assertTrue(worth_logging("campaign", {"joined": True}))
        self.assertTrue(worth_logging("campaign", {"joined": False, "reason": "model v22"}))
        self.assertTrue(worth_logging("halt", {"campaign_id": "c1"}))
        self.assertFalse(worth_logging("leg", {"campaign_id": "c1"}))

    def test_an_old_skip_is_marked_for_folding(self):
        from executor.feed_client import FollowedCampaign

        runtime = self._runtime()
        runtime._client.campaigns["old"] = FollowedCampaign(
            campaign_id="old",
            symbol="SOLUSDT",
            exchange="binance",
            created_at=int(NOW) - 900,
            mother_high=178.42,
            mother_low=174.10,
            mother_timestamp=1,
            timeframe="5m",
            state="TRENDLINE_ACTIVE",
            model_version=21,
            joined=False,
            skip_reason="Started 900s ago — past the join window.",
            skipped_as_old=True,
        )
        rows = campaigns_view(runtime)
        self.assertTrue(rows[0]["skipped_as_old"])


class UIServerTests(unittest.TestCase):
    def setUp(self):
        self.state = UIState()
        self.state.set_status({"armed_exposure_usd": 7.25, "opening_new": True})
        self.server = UIServer(self.state, port=0)  # port 0: the OS picks a free one
        problem = self.server.start()
        self.assertIsNone(problem)
        self.port = self.server._server.server_address[1]
        self.addCleanup(self.server.stop)

    def _get(self, path):
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=5) as response:
            return response.status, response.read()

    def test_it_binds_loopback_only(self):
        """The boundary IS the interface. A LAN address must not even be bound."""
        self.assertEqual(self.server._server.server_address[0], "127.0.0.1")

    def test_the_page_is_served(self):
        status, body = self._get("/")
        self.assertEqual(status, 200)
        self.assertIn(b"by CryptoForge", body)  # the child carries the parent's name

    def test_the_state_is_served_as_json(self):
        status, body = self._get("/api/state")
        self.assertEqual(status, 200)
        snapshot = json.loads(body)
        self.assertEqual(snapshot["status"]["armed_exposure_usd"], 7.25)
        self.assertIn("$7.25", snapshot["lines"][0])

    def test_anything_else_is_a_404(self):
        try:
            status, _ = self._get("/etc/passwd")
        except urllib.error.HTTPError as exc:
            status = exc.code
        self.assertEqual(status, 404)

    def test_a_busy_port_is_a_note_not_a_crash(self):
        second = UIServer(UIState(), port=self.port)
        problem = second.start()
        self.assertIn("busy", problem)
        self.assertIn("runs fine without it", problem)

    def test_the_page_is_self_contained(self):
        """No CDN, no external fetch: nothing to trust but the executor itself."""
        for marker in ("http://", "https://", "@import"):
            self.assertNotIn(marker, PAGE.replace("http://127.0.0.1", ""))

    def test_nothing_the_page_loads_comes_from_off_the_machine(self):
        """The guide arrives by iframe and the faces by stylesheet, so `src=`
        and `href=` are no longer disqualifying — but every one must be
        same-origin. A font CDN reads identically to a local path to anything
        cruder than this, and a font CDN is exactly what the parent uses."""
        for attr in ("src", "href"):
            for ref in re.findall(attr + r'\s*=\s*"([^"]*)"', PAGE):
                self.assertTrue(
                    ref.startswith("/") or ref.startswith("data:") or ref.startswith("#"),
                    f"off-machine {attr}: {ref}",
                )

    def test_the_guide_is_served_as_a_whole_document(self):
        status, body = self._get("/guide.html")
        self.assertEqual(status, 200)
        # A body fragment in an iframe renders in quirks mode, which the
        # guide's layout does not survive.
        self.assertTrue(body.lstrip().lower().startswith(b"<!doctype html>"))
        self.assertIn(b"Twenty minutes", body)

    def test_the_guide_tab_points_at_the_route_that_serves_it(self):
        self.assertIn('data-page="guide"', PAGE)
        self.assertIn('src="/guide.html"', PAGE)

    def test_a_missing_guide_is_a_note_not_a_broken_page(self):
        """The guide going astray must not look like the executor breaking."""
        with mock.patch("executor.ui.GUIDE_FILE", "/nonexistent/guide.html"):
            body = ui.guide_document()
        self.assertIn(b"ask us for another copy", body)
        self.assertTrue(body.lstrip().lower().startswith(b"<!doctype html>"))

    def test_every_font_set_is_served_and_embedded(self):
        """Each preset must actually ship its faces. A preset whose stylesheet
        were missing would silently fall back to the system stack and all six
        would look the same — the exact failure the parent's CSS warns about."""
        for name in ui.FONT_SETS:
            status, body = self._get(f"/assets/fonts/{name}.css")
            self.assertEqual(status, 200, name)
            self.assertIn(b"@font-face", body, name)
            self.assertIn(b"url(data:font/woff2;base64,", body, name)
            self.assertNotIn(b"http", body, name)

    def test_the_head_links_the_core_faces_and_the_presets_are_reachable(self):
        self.assertIn('href="/assets/fonts/core.css"', PAGE)
        self.assertIn('"/assets/fonts/" + font + ".css"', PAGE)

    def test_an_unknown_font_set_is_a_404(self):
        for path in ("/assets/fonts/nope.css", "/assets/fonts/../../ui.py", "/assets/fonts/"):
            try:
                status, _ = self._get(path)
            except urllib.error.HTTPError as exc:
                status = exc.code
            self.assertEqual(status, 404, path)

    def test_a_traversing_name_never_reaches_the_filesystem(self):
        """Membership of FONT_SETS is the whole check, so there is no path to
        sanitise — assert that directly rather than trusting the URL parse."""
        for name in ("../guide", "/etc/passwd", "core/../../ui", ""):
            self.assertIsNone(ui.font_css(name), name)

    def test_the_appearance_controls_offer_the_terminals_six_and_six(self):
        for tint in ("gold", "arctic", "magenta", "citrus", "graphite", "bronze"):
            self.assertIn(f'"{tint}"', PAGE)
        for font in ui.FONT_SETS:
            if font != "core":
                self.assertIn(f'"{font}"', PAGE)
        # Same storage keys as the terminal, so the two products agree.
        self.assertIn('"cf-appearance"', PAGE)
        self.assertIn('"cf-theme"', PAGE)

    def test_the_theme_is_applied_before_the_body_paints(self):
        """After first paint it is a white flash on a page that is meant to be
        ink, which is why the parent does it in the head too."""
        self.assertLess(PAGE.index("window.cfTheme()"), PAGE.index("<body>"))

    def test_the_brand_mark_is_the_terminals_own(self):
        for part in ("brand-column col-a", "brand-column col-b", "brand-column col-c", "brand-spark"):
            self.assertIn(part, PAGE)

    def test_the_chart_reads_its_colours_from_the_theme(self):
        """Canvas is painted, not styled: a hardcoded palette is the one place
        a tint or light mode would visibly fail to reach."""
        chart = PAGE[PAGE.index("function drawChart()") : PAGE.index("async function openChart")]
        self.assertIn("getComputedStyle(document.documentElement)", chart)
        for hardcoded in ('"#22d3ee"', '"#2dd4bf"', '"#fb7185"', '"#f59e0b"', '"#fbbf24"', '"#040814"'):
            self.assertNotIn(hardcoded, chart)

    def test_the_guide_itself_fetches_nothing(self):
        """It ships to strangers' machines and is served from loopback: a
        remote font or script would be both a leak and a dependency."""
        with open(ui.GUIDE_FILE, encoding="utf-8") as handle:
            guide = handle.read()
        for pattern in (r'src\s*=\s*"https?:', r'href\s*=\s*"https?:', "@import"):
            self.assertEqual(re.findall(pattern, guide), [], f"the guide reaches out: {pattern}")


if __name__ == "__main__":
    unittest.main()


class ActionEndpointTests(unittest.TestCase):
    """The buttons cancel and place real orders, so the gates are the test.

    Any web page can blind-POST to localhost — a form needs no permission. The
    custom header forces a CORS preflight we never answer, and the Host check
    is DNS-rebinding defence: a hostile page can point its own domain at
    127.0.0.1 and fetch it same-origin, but the browser faithfully reports the
    Host it asked for.
    """

    def setUp(self):
        self.state = UIState()
        self.calls = []
        self.server = UIServer(
            self.state, port=0, actions={"pause": lambda: (self.calls.append("pause"), "Paused.")[1]}
        )
        self.assertIsNone(self.server.start())
        self.port = self.server._server.server_address[1]
        self.addCleanup(self.server.stop)

    def _post(self, path="/api/action", body=b'{"action":"pause"}', headers=None):
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}",
            data=body,
            method="POST",
            headers={"Content-Type": "application/json", **(headers or {})},
        )
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as exc:
            return exc.code, b""

    def test_a_post_without_the_custom_header_is_refused(self):
        status, _ = self._post()
        self.assertEqual(status, 403)
        self.assertEqual(self.calls, [])

    def test_a_post_with_the_header_runs_the_action(self):
        status, body = self._post(headers={"X-Cascade-UI": "1"})
        self.assertEqual(status, 200)
        self.assertEqual(self.calls, ["pause"])
        self.assertIn(b"Paused.", body)

    def test_a_rebinding_host_is_refused_even_from_loopback(self):
        status, _ = self._post(headers={"X-Cascade-UI": "1", "Host": "evil.example.com"})
        self.assertEqual(status, 403)
        self.assertEqual(self.calls, [])

    def test_an_unknown_action_is_a_404_not_a_crash(self):
        status, _ = self._post(body=b'{"action":"transfer_everything"}', headers={"X-Cascade-UI": "1"})
        self.assertEqual(status, 404)

    def test_the_action_lands_on_the_event_strip(self):
        self._post(headers={"X-Cascade-UI": "1"})
        self.assertEqual(self.state.snapshot()["events"][0]["line"], "Paused.")

    def test_a_saved_subscription_reaches_the_page_not_just_the_machine(self):
        """The runtime carries its own config, built at startup. Updating only
        that left the page showing boot values while the machine followed
        something else — the change looked ignored when it had been applied."""
        import os
        import tempfile

        from executor.config import ExecutorConfig

        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        config = ExecutorConfig(
            server_url="http://localhost",
            buyer_id="b",
            root_public_key="k",
            state_dir=directory.name,
            source_path=os.path.join(directory.name, "config.json"),
        )

        class StubRuntime:
            def set_subscription(self, **kwargs):
                return "Now following."

            def venue_change_blockers(self):
                return []

        class StubExecutor:
            def __init__(self):
                self.config = config
                self.runtime = StubRuntime()
                self.identity = mock.Mock(public_key_b64=lambda: "pk")
                self.transport = mock.Mock()

            def _on_status(self, kind, detail):
                pass

            def _market_for_ui(self):
                return None

        executor = StubExecutor()
        server = ui.wire(executor, port=0)
        self.addCleanup(server.stop)
        state = executor._ui_state
        port = server._server.server_address[1]
        request = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/action",
            data=b'{"action":"set_subscription","payload":{"timeframes":"15m","signal_exchanges":"coindcx"}}',
            method="POST",
            headers={"Content-Type": "application/json", "X-Cascade-UI": "1"},
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            response.read()

        identity = state.snapshot()["identity"]
        self.assertEqual(identity["timeframes"], ["15m"])
        self.assertEqual(identity["following"], "15m · drawn on coindcx · all coins")
        self.assertEqual(config.timeframes, ["15m"])

    def test_a_settings_action_receives_what_to_set(self):
        """Switches take no argument and settings do; one signature carries
        both, so a handler that wants nothing keeps saying so."""
        got = {}
        server = UIServer(
            self.state,
            port=0,
            actions={
                "set_it": lambda payload: (got.update(payload), "Set.")[1],
                "plain": lambda: "Switched.",
            },
        )
        self.assertIsNone(server.start())
        self.addCleanup(server.stop)
        port = server._server.server_address[1]

        def post(body):
            request = urllib.request.Request(
                f"http://127.0.0.1:{port}/api/action",
                data=body,
                method="POST",
                headers={"Content-Type": "application/json", "X-Cascade-UI": "1"},
            )
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.read()

        self.assertIn(b"Set.", post(b'{"action":"set_it","payload":{"timeframes":["15m"]}}'))
        self.assertEqual(got, {"timeframes": ["15m"]})
        self.assertIn(b"Switched.", post(b'{"action":"plain"}'))


class BuyerSwitchTests(unittest.TestCase):
    """Pause, confirm, stand down — each read by the tick, none touching exits."""

    def _runtime(self):
        client = FeedClient(public_keys={}, keyset_fetched_at=NOW, now_fn=lambda: NOW)
        return ExecutorRuntime(
            client=client,
            adapter=FakeExchange(),
            market=FakeMarket(),
            config=RuntimeConfig(capital_usd=5000.0),
            now_fn=lambda: NOW,
        )

    def test_pause_blocks_opening_and_says_so(self):
        runtime = self._runtime()
        runtime.pause_opening()
        status = runtime.status()
        self.assertFalse(status["opening_new"])
        self.assertIn("Paused by you", status["posture_reason"])
        runtime.resume_opening()
        self.assertTrue(runtime.status()["opening_new"])

    def test_a_long_gap_holds_until_the_buyer_confirms(self):
        """The gate used to be a message with no door: the report said "until
        you have looked" and there was nothing anywhere to press."""
        runtime = self._runtime()
        report = runtime.on_wake({"shutdown_at": NOW - 8 * 3600, "slept_armed": False})
        self.assertTrue(report["requires_confirmation"])
        self.assertFalse(runtime.status()["opening_new"])
        runtime.confirm_wake()
        self.assertTrue(runtime.status()["opening_new"])

    def test_stand_down_runs_the_sleep_invariants_on_the_next_tick(self):
        from executor.orders import CampaignOrders, Fill

        runtime = self._runtime()
        orders = runtime.book.track(
            CampaignOrders(campaign_id="c1", symbol="SOLUSDT", mother_high=178.42, exchange="binance")
        )
        orders.on_entry_filled(Fill(price=162.0, quantity=0.05, timestamp=1))
        runtime.request_stand_down()
        report = runtime.tick()
        self.assertTrue(runtime.opening_paused)
        self.assertTrue(any("Stood down" in note for note in report.notes))
        # The held position got its exit placed, not abandoned.
        self.assertTrue(orders.exit_resting)

    def test_a_closed_round_lands_in_the_ledger_net_of_venue_fees(self):
        from executor import model
        from executor.orders import CampaignOrders, Fill

        runtime = self._runtime()
        orders = runtime.book.track(
            CampaignOrders(campaign_id="c1", symbol="SOLUSDT", mother_high=178.42, exchange="coindcx")
        )
        orders.on_entry_filled(Fill(price=100.0, quantity=1.0, timestamp=1))
        orders.on_exit_filled(105.0, ts=99)
        row = runtime.rounds_view()[0]
        self.assertEqual(row["closed_ts"], 99)
        fees = (100.0 + 105.0) * model.EXCHANGE_FEE_PCT["coindcx"] / 100.0
        self.assertAlmostEqual(row["net_est_usd"], 5.0 - fees, places=4)
        self.assertAlmostEqual(runtime.status()["rounds_net_est_usd"], round(5.0 - fees, 2), places=2)


class ChartViewTests(unittest.TestCase):
    """The chart draws THEIR candles under OUR geometry.

    Which is exactly the split the whole product rests on, so the test that
    matters is what the payload does not contain: nothing of ours that this
    machine could not have derived itself.
    """

    def setUp(self):
        from executor.feed_client import FollowedCampaign, FollowedLeg
        from executor.orders import CampaignOrders, Candle, Fill

        client = FeedClient(public_keys={}, keyset_fetched_at=NOW, now_fn=lambda: NOW)
        self.runtime = ExecutorRuntime(
            client=client,
            adapter=FakeExchange(),
            market=FakeMarket(),
            config=RuntimeConfig(capital_usd=5000.0),
            now_fn=lambda: NOW,
        )
        followed = FollowedCampaign(
            campaign_id="c1",
            symbol="SOLUSDT",
            exchange="binance",
            created_at=int(NOW),
            mother_high=178.42,
            mother_low=174.10,
            mother_timestamp=1785400800,
            timeframe="5m",
            state="TRENDLINE_ACTIVE",
            model_version=21,
            joined=True,
        )
        followed.legs[4] = FollowedLeg(
            leg_id=4,
            trendline_id=3,
            low=172.88,
            touch_high=176.40,
            fib_high=176.40,
            fib_low=172.88,
            allocation_anchor=178.42,
            allocation_pct_gross=3.1,
        )
        followed.trendlines[3] = {
            "trendline_id": 3,
            "anchor1_price": 178.42,
            "anchor1_timestamp": 1785400800,
            "anchor2_price": 177.06,
            "anchor2_timestamp": 1785403500,
        }
        followed.standing_trendline_id = 3
        client.campaigns["c1"] = followed
        orders = self.runtime.book.track(
            CampaignOrders(campaign_id="c1", symbol="SOLUSDT", mother_high=178.42, exchange="binance")
        )
        orders.on_entry_filled(Fill(price=162.0, quantity=0.05, timestamp=1785404400))
        orders.intents(163.0)  # places the exit, so target is set
        self.market = FakeMarket(candles=[Candle(1785404000 + i * 300, 163, 164, 162, 163.5) for i in range(20)])

    def test_it_carries_their_candles_and_our_geometry(self):
        from executor.ui import chart_view

        chart = chart_view(self.runtime, self.market, "c1")
        self.assertTrue(chart["candles"])
        self.assertEqual(chart["mother_high"], 178.42)
        self.assertIsNotNone(chart["trendline"])
        self.assertEqual(sorted(f["level"] for f in chart["fib_levels"]), [2, 4, 8])

    def test_the_money_marks_are_the_buyers_own(self):
        from executor.ui import chart_view

        chart = chart_view(self.runtime, self.market, "c1")
        self.assertAlmostEqual(chart["avg_entry"], 162.0)
        self.assertEqual(len(chart["fills"]), 1)
        self.assertIsNotNone(chart["target"])

    def test_it_carries_nothing_of_ours(self):
        """Our capital, our fills, our target were never on this machine — and
        the payload must not invent a place for them."""
        from executor.ui import chart_view

        chart = chart_view(self.runtime, self.market, "c1")
        for banned in ("capital_usd", "pool_usd", "our_fills", "allocation_pct", "mode"):
            self.assertNotIn(banned, chart)

    def test_a_finalized_leg_still_draws_its_rungs(self):
        """The chart has to show the ladder the buyer's money is waiting on.

        This asserted the opposite, matching the same wrong reading of
        `finalized` that stopped the executor placing anything: a leg is born
        finalized in the engine and trades from that moment, so hiding its
        levels left the buyer staring at a chart with no ladder while their
        orders sat at exactly those prices.
        """
        from executor.ui import chart_view

        self.runtime._client.campaigns["c1"].legs[4].finalized = True
        levels = chart_view(self.runtime, self.market, "c1")["fib_levels"]
        self.assertEqual(sorted(row["level"] for row in levels), [2, 4, 8])

    def test_an_unknown_campaign_is_none_not_a_crash(self):
        from executor.ui import chart_view

        self.assertIsNone(chart_view(self.runtime, self.market, "nope"))

    def test_a_broken_candle_feed_still_returns_the_geometry(self):
        """A rate-limited exchange must not blank the chart's levels too."""
        from executor.ui import chart_view

        class Broken:
            def closed_candles_since(self, *a, **k):
                raise RuntimeError("429")

        chart = chart_view(self.runtime, Broken(), "c1")
        self.assertEqual(chart["candles"], [])
        self.assertEqual(chart["mother_high"], 178.42)


class ChartEndpointTests(unittest.TestCase):
    def setUp(self):
        self.state = UIState()
        self.server = UIServer(
            self.state,
            port=0,
            chart_fn=lambda cid: {"campaign_id": cid, "candles": []} if cid == "c1" else None,
        )
        self.assertIsNone(self.server.start())
        self.port = self.server._server.server_address[1]
        self.addCleanup(self.server.stop)

    def _get(self, path):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=5) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as exc:
            return exc.code, b""

    def test_a_known_campaign_returns_its_chart(self):
        status, body = self._get("/api/chart?cid=c1")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["campaign_id"], "c1")

    def test_an_unknown_campaign_is_a_404(self):
        self.assertEqual(self._get("/api/chart?cid=nope")[0], 404)
