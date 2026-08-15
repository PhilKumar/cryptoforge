"""
Tests for the login gate: who gets rate limited, how hard, and whether the
second factor actually holds.

The thing these are really guarding is that a 6-digit PIN is the only secret
between a stranger and an app that places real orders.
"""

import base64
import os
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from httpx import ASGITransport, AsyncClient

import app as app_module


def _request(headers=None, peer="127.0.0.1"):
    """Enough of a Request for _client_ip: headers plus a client peer."""
    return SimpleNamespace(
        headers={k.lower(): v for k, v in (headers or {}).items()},
        client=SimpleNamespace(host=peer) if peer else None,
    )


class ClientIpTests(unittest.TestCase):
    """nginx APPENDS to X-Forwarded-For, so entry zero belongs to the caller."""

    def test_prefers_x_real_ip(self):
        req = _request({"X-Real-IP": "203.0.113.9", "X-Forwarded-For": "1.2.3.4, 203.0.113.9"})
        self.assertEqual(app_module._client_ip(req), "203.0.113.9")

    def test_spoofed_forwarded_for_does_not_win(self):
        # The attack: send your own X-Forwarded-For so every guess looks like a
        # different visitor and the lockout never accumulates. nginx turns that
        # header into "<spoofed>, <real peer>", so the real peer is last.
        req = _request({"X-Forwarded-For": "9.9.9.9, 198.51.100.7"})
        self.assertEqual(app_module._client_ip(req), "198.51.100.7")

    def test_every_spoof_collapses_to_one_bucket(self):
        seen = {app_module._client_ip(_request({"X-Forwarded-For": f"10.0.0.{n}, 198.51.100.7"})) for n in range(50)}
        self.assertEqual(seen, {"198.51.100.7"}, "50 forged headers must not create 50 rate-limit buckets")

    def test_falls_back_to_peer_without_headers(self):
        self.assertEqual(app_module._client_ip(_request(peer="192.0.2.5")), "192.0.2.5")

    def test_no_request_is_not_an_exception(self):
        self.assertEqual(app_module._client_ip(None), "unknown")


class LockoutEscalationTests(unittest.TestCase):
    def setUp(self):
        app_module._login_state.clear()
        # Force the in-memory path; a live Redis would make these order-dependent.
        patcher = patch.object(app_module, "_get_redis", return_value=None)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(app_module._login_state.clear)

    def test_ladder_climbs_then_plateaus(self):
        self.assertEqual(app_module._login_lockout_sec(4), 0, "under the threshold nothing locks")
        self.assertEqual(app_module._login_lockout_sec(5), 300)
        self.assertEqual(app_module._login_lockout_sec(6), 900)
        self.assertEqual(app_module._login_lockout_sec(7), 3600)
        self.assertEqual(app_module._login_lockout_sec(8), 21600)
        self.assertEqual(app_module._login_lockout_sec(9), 86400)
        self.assertEqual(app_module._login_lockout_sec(500), 86400, "the ladder plateaus, it does not crash")

    def test_first_four_failures_do_not_lock(self):
        for _ in range(4):
            app_module._record_failed_login("198.51.100.7")
            app_module._check_login_rate("198.51.100.7")  # must not raise

    def test_fifth_failure_locks_out(self):
        for _ in range(5):
            app_module._record_failed_login("198.51.100.7")
        with self.assertRaises(app_module.HTTPException) as caught:
            app_module._check_login_rate("198.51.100.7")
        self.assertEqual(caught.exception.status_code, 429)
        self.assertIn("5 minutes", caught.exception.detail)

    def test_lockout_lengthens_with_each_further_failure(self):
        ip = "198.51.100.7"
        for _ in range(5):
            app_module._record_failed_login(ip)
        first = app_module._login_state[ip]["until"]
        app_module._record_failed_login(ip)
        second = app_module._login_state[ip]["until"]
        app_module._record_failed_login(ip)
        third = app_module._login_state[ip]["until"]
        self.assertLess(first, second)
        self.assertLess(second, third)

    def test_a_days_guessing_budget_is_small(self):
        """The point of the ladder, stated as the number it is meant to move."""
        ip = "198.51.100.7"
        guesses, clock = 0, 0.0
        for _ in range(200):
            with patch.object(app_module.time, "time", return_value=clock):
                if app_module._login_lock_remaining(ip) > 0:
                    break
                app_module._record_failed_login(ip)
            guesses += 1
            clock += 1.0
        # Then walk a full day, taking every guess the lockout allows.
        deadline = clock + 86400
        while clock < deadline:
            with patch.object(app_module.time, "time", return_value=clock):
                remaining = app_module._login_lock_remaining(ip)
                if remaining > 0:
                    clock += remaining
                    continue
                app_module._record_failed_login(ip)
            guesses += 1
            clock += 1.0
        self.assertLess(guesses, 20, f"a full day should not buy many guesses, got {guesses}")

    def test_success_clears_the_escalation(self):
        for _ in range(5):
            app_module._record_failed_login("198.51.100.7")
        app_module._clear_login_attempts("198.51.100.7")
        app_module._check_login_rate("198.51.100.7")  # must not raise

    def test_lockouts_are_per_ip(self):
        for _ in range(6):
            app_module._record_failed_login("198.51.100.7")
        app_module._check_login_rate("203.0.113.4")  # a different caller is unaffected

    def test_humanize_reads_like_a_sentence(self):
        self.assertEqual(app_module._humanize_seconds(1), "1 second")
        self.assertEqual(app_module._humanize_seconds(300), "5 minutes")
        self.assertEqual(app_module._humanize_seconds(3600), "1 hour")
        self.assertEqual(app_module._humanize_seconds(86400), "24 hours")


class TotpTests(unittest.TestCase):
    wants_totp = True  # see tests/conftest.py — these patch TOTP_SECRET themselves
    # RFC 6238 Appendix B publishes expected codes for the ASCII secret
    # "12345678901234567890". If our implementation disagrees with these, it
    # disagrees with every authenticator app on the planet.
    RFC_SECRET = base64.b32encode(b"12345678901234567890").decode().rstrip("=")
    RFC_VECTORS = [(59, "287082"), (1111111109, "081804"), (1111111111, "050471"), (1234567890, "005924")]

    def setUp(self):
        app_module._totp_used_counters.clear()
        self.addCleanup(app_module._totp_used_counters.clear)

    def test_matches_rfc6238_published_vectors(self):
        with patch.object(app_module, "TOTP_SECRET", self.RFC_SECRET):
            for unix_time, expected in self.RFC_VECTORS:
                counter = unix_time // app_module._TOTP_STEP_SEC
                self.assertEqual(app_module._totp_code_at(counter), expected, f"at t={unix_time}")

    def test_disabled_by_default(self):
        with patch.object(app_module, "TOTP_SECRET", ""):
            self.assertFalse(app_module._totp_enabled())

    def test_accepts_the_current_code(self):
        with patch.object(app_module, "TOTP_SECRET", self.RFC_SECRET):
            now = int(time.time() // app_module._TOTP_STEP_SEC)
            self.assertEqual(app_module._verify_totp(app_module._totp_code_at(now)), now)

    def test_tolerates_one_step_of_clock_drift(self):
        with patch.object(app_module, "TOTP_SECRET", self.RFC_SECRET):
            now = int(time.time() // app_module._TOTP_STEP_SEC)
            self.assertEqual(app_module._verify_totp(app_module._totp_code_at(now - 1)), now - 1)
            self.assertEqual(app_module._verify_totp(app_module._totp_code_at(now + 1)), now + 1)

    def test_rejects_two_steps_of_drift(self):
        with patch.object(app_module, "TOTP_SECRET", self.RFC_SECRET):
            now = int(time.time() // app_module._TOTP_STEP_SEC)
            self.assertIsNone(app_module._verify_totp(app_module._totp_code_at(now - 2)))

    def test_a_spent_code_cannot_be_replayed(self):
        # A code is valid for 30 seconds. Without this, someone who reads it
        # over your shoulder can sign in behind you inside that window.
        with patch.object(app_module, "TOTP_SECRET", self.RFC_SECRET):
            now = int(time.time() // app_module._TOTP_STEP_SEC)
            code = app_module._totp_code_at(now)
            counter = app_module._verify_totp(code)
            self.assertIsNotNone(counter)
            app_module._consume_totp(counter)  # what a SUCCESSFUL login does
            self.assertIsNone(app_module._verify_totp(code), "a spent code must not work twice")

    def test_checking_a_code_does_not_spend_it(self):
        """The bug this guards is a self-lockout, not a break-in.

        Checking and spending used to be one call, so a wrong PIN burned the
        code still on your phone. The obvious retry — same code, PIN typed
        correctly this time — came back "wrong PIN or code" with no hint that
        you now had to wait for the next 30-second window. A few rounds of that
        and the escalating lockout has you out for hours, from one typo.
        """
        with patch.object(app_module, "TOTP_SECRET", self.RFC_SECRET):
            now = int(time.time() // app_module._TOTP_STEP_SEC)
            code = app_module._totp_code_at(now)
            for attempt in range(5):
                self.assertEqual(
                    app_module._verify_totp(code),
                    now,
                    f"attempt {attempt + 1}: a failed login must leave the code usable",
                )

    def test_rejects_malformed_input(self):
        with patch.object(app_module, "TOTP_SECRET", self.RFC_SECRET):
            for bad in ("", "12345", "1234567", "abcdef", None, "   "):
                self.assertIsNone(app_module._verify_totp(bad), repr(bad))

    def test_bad_secret_fails_closed(self):
        with patch.object(app_module, "TOTP_SECRET", "not-valid-base32!!"):
            self.assertIsNone(app_module._verify_totp("123456"))

    def test_setup_tool_agrees_with_the_server(self):
        """tools/totp_setup.py --verify must not disagree with the login path."""
        import tools.totp_setup as setup_tool

        counter = int(time.time() // app_module._TOTP_STEP_SEC)
        with patch.object(app_module, "TOTP_SECRET", self.RFC_SECRET):
            self.assertEqual(setup_tool.code_at(self.RFC_SECRET, counter), app_module._totp_code_at(counter))


if __name__ == "__main__":
    unittest.main()


class LoginRouteTwoFactorTests(unittest.IsolatedAsyncioTestCase):
    """The login route end to end, because the unit tests above passed while
    the real sequence still locked you out."""

    wants_totp = True  # see tests/conftest.py — opt out of the auto-disable
    RFC_SECRET = TotpTests.RFC_SECRET
    PIN = "424242"

    async def asyncSetUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addAsyncCleanup(self._tmp.cleanup)
        for name, value in (
            ("AUTH_PIN", self.PIN),
            ("TOTP_SECRET", self.RFC_SECRET),
            ("_STATE_DB_FILE", os.path.join(self._tmp.name, "t.db")),
        ):
            patcher = patch.object(app_module, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        patcher = patch.object(app_module, "_get_redis", return_value=None)
        patcher.start()
        self.addCleanup(patcher.stop)
        app_module._login_state.clear()
        app_module._totp_used_counters.clear()
        self.addCleanup(app_module._login_state.clear)
        self.addCleanup(app_module._totp_used_counters.clear)

        self.client = AsyncClient(transport=ASGITransport(app=app_module.app), base_url="http://testserver")
        self.addAsyncCleanup(self.client.aclose)

    def _code(self, offset=0):
        return app_module._totp_code_at(int(time.time() // app_module._TOTP_STEP_SEC) + offset)

    async def _login(self, pin, code):
        return await self.client.post("/api/auth/login", json={"password": pin, "totp": code})

    async def test_status_advertises_the_second_factor(self):
        res = await self.client.get("/api/auth/status")
        self.assertTrue(res.json()["totp_required"])

    async def test_pin_alone_is_not_enough(self):
        self.assertEqual((await self._login(self.PIN, "")).status_code, 401)

    async def test_correct_pin_and_code_gets_in(self):
        res = await self._login(self.PIN, self._code())
        self.assertEqual(res.status_code, 200)
        self.assertIn("cryptoforge_session", res.cookies)

    async def test_a_pin_typo_does_not_burn_the_code(self):
        """The regression. Mistype the PIN, then retype it correctly with the
        same code still on screen — that must work."""
        code = self._code()
        self.assertEqual((await self._login("424241", code)).status_code, 401)
        res = await self._login(self.PIN, code)
        self.assertEqual(res.status_code, 200, "the retry with the same on-screen code must succeed")

    async def test_several_pin_typos_still_leave_the_code_usable(self):
        code = self._code()
        for _ in range(4):  # stop short of the 5-failure lockout
            self.assertEqual((await self._login("000000", code)).status_code, 401)
        self.assertEqual((await self._login(self.PIN, code)).status_code, 200)

    async def test_a_code_already_used_to_log_in_is_refused(self):
        code = self._code()
        self.assertEqual((await self._login(self.PIN, code)).status_code, 200)
        self.assertEqual((await self._login(self.PIN, code)).status_code, 401, "no replay")

    async def test_wrong_code_with_right_pin_is_refused(self):
        self.assertEqual((await self._login(self.PIN, "000000")).status_code, 401)

    async def test_the_error_does_not_say_which_factor_failed(self):
        """Naming the wrong half lets an attacker crack them one at a time."""

        def detail(res):
            # error_handlers.py reshapes 4xx bodies into {success, error:{...}}.
            body = res.json()
            return (body.get("error") or {}).get("detail", body.get("detail"))

        bad_pin = detail(await self._login("000000", self._code()))
        bad_code = detail(await self._login(self.PIN, "000000"))
        self.assertEqual(bad_pin, bad_code)
        self.assertEqual(bad_pin, "Invalid PIN or code")

    async def test_repeated_failures_still_lock_out(self):
        for _ in range(5):
            await self._login("000000", "000000")
        res = await self._login(self.PIN, self._code())
        self.assertEqual(res.status_code, 429, "the lockout must still bite")


class ViewerDoorTests(unittest.IsolatedAsyncioTestCase):
    """The read-only door. Phil, 2026-08-17: "Portfolio numbers can be visible
    here... Only nothing can be started or stopped or no admin activities."

    A second PIN opens a session whose role is `viewer`; the middleware then
    refuses every write by METHOD and a short list of admin reads by PATH.
    """

    wants_totp = True  # the door must hold with the second factor ON
    RFC_SECRET = TotpTests.RFC_SECRET
    PIN = "424242"
    VIEWER = "777777"

    async def asyncSetUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addAsyncCleanup(self._tmp.cleanup)
        for name, value in (
            ("AUTH_PIN", self.PIN),
            ("VIEWER_PIN", self.VIEWER),
            ("TOTP_SECRET", self.RFC_SECRET),
            ("_STATE_DB_FILE", os.path.join(self._tmp.name, "t.db")),
        ):
            patcher = patch.object(app_module, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        patcher = patch.object(app_module, "_get_redis", return_value=None)
        patcher.start()
        self.addCleanup(patcher.stop)
        app_module._login_state.clear()
        app_module._totp_used_counters.clear()
        app_module._rate_limits.clear()
        self.addCleanup(app_module._login_state.clear)
        self.addCleanup(app_module._totp_used_counters.clear)
        self.client = AsyncClient(transport=ASGITransport(app=app_module.app), base_url="http://testserver")
        self.addAsyncCleanup(self.client.aclose)

    def _code(self):
        return app_module._totp_code_at(int(time.time() // app_module._TOTP_STEP_SEC))

    async def _login(self, pin, code=""):
        return await self.client.post("/api/auth/login", json={"password": pin, "totp": code})

    def _write_headers(self):
        return {
            "X-CSRF-Token": self.client.cookies.get("cryptoforge_csrf") or "",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "http://testserver",
        }

    async def _login_as_viewer(self):
        res = await self._login(self.VIEWER)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json().get("role"), "viewer")
        return res

    # ── the door itself ──────────────────────────────────────────────

    async def test_status_advertises_the_door_only_when_a_viewer_pin_exists(self):
        self.assertTrue((await self.client.get("/api/auth/status")).json()["viewer_login_enabled"])
        with patch.object(app_module, "VIEWER_PIN", ""):
            self.assertFalse((await self.client.get("/api/auth/status")).json()["viewer_login_enabled"])

    async def test_viewer_pin_opens_a_viewer_session_without_a_code(self):
        await self._login_as_viewer()
        status = (await self.client.get("/api/auth/status")).json()
        self.assertTrue(status["authenticated"])
        self.assertEqual(status["role"], "viewer")
        self.assertTrue(status["read_only"])

    async def test_unlock_pin_without_its_code_still_gets_nothing(self):
        """The viewer door is a different PIN, not a way past the authenticator."""
        self.assertEqual((await self._login(self.PIN, "")).status_code, 401)

    async def test_unlock_pin_with_code_is_still_admin(self):
        res = await self._login(self.PIN, self._code())
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json().get("role"), "admin")
        status = (await self.client.get("/api/auth/status")).json()
        self.assertEqual(status["role"], "admin")
        self.assertFalse(status["read_only"])

    async def test_a_viewer_pin_equal_to_the_unlock_pin_closes_the_door(self):
        """If .env is hand-edited to make them match, the unlock PIN must not
        quietly become a second unlock PIN that skips the second factor."""
        with patch.object(app_module, "VIEWER_PIN", self.PIN):
            self.assertFalse(app_module._viewer_login_enabled())
            self.assertEqual((await self._login(self.PIN, "")).status_code, 401)

    async def test_wrong_viewer_pin_counts_toward_the_lockout(self):
        for _ in range(5):
            self.assertEqual((await self._login("777776")).status_code, 401)
        self.assertEqual((await self._login(self.VIEWER)).status_code, 429)

    # ── what a viewer may and may not do ─────────────────────────────

    async def test_viewer_reads_pass(self):
        await self._login_as_viewer()
        for path in ("/api/strategies", "/api/runs", "/api/notifications", "/api/broker/settings", "/api/live/status"):
            res = await self.client.get(path)
            self.assertNotIn(res.status_code, (401, 403), f"{path} must be readable by a viewer, got {res.status_code}")

    async def test_viewer_writes_are_refused_by_method(self):
        await self._login_as_viewer()
        headers = self._write_headers()
        for method, path in (
            ("POST", "/api/live/start"),
            ("POST", "/api/paper/start"),
            ("POST", "/api/emergency-stop"),
            ("POST", "/api/orders/place"),
            ("POST", "/api/backtest"),
            ("POST", "/api/notifications/ack"),
            ("PUT", "/api/admin/config"),
            ("DELETE", "/api/cache"),
        ):
            res = await self.client.request(method, path, headers=headers, json={})
            self.assertEqual(res.status_code, 403, f"{method} {path} -> {res.status_code}")
            body = res.json()
            self.assertEqual(body.get("code"), "viewer_read_only", body)

    async def test_viewer_admin_reads_are_refused_by_path(self):
        await self._login_as_viewer()
        for path in (
            "/api/admin/config",
            "/api/admin/health",
            "/api/ops/state/backup",
            "/api/ops/state/summary",
            "/api/audit/production-readiness",
            "/api/cascade/feed/subscribers",
        ):
            res = await self.client.get(path)
            self.assertEqual(res.status_code, 403, f"GET {path} -> {res.status_code}")
            self.assertEqual(res.json().get("code"), "viewer_read_only")

    async def test_viewer_may_log_out(self):
        await self._login_as_viewer()
        res = await self.client.post("/api/auth/logout", headers=self._write_headers())
        self.assertEqual(res.status_code, 200)
        self.assertFalse((await self.client.get("/api/auth/status")).json()["authenticated"])

    async def test_viewer_may_ask_the_broker_check(self):
        """A read wearing a POST verb — the top bar's connected light."""
        await self._login_as_viewer()
        res = await self.client.post("/api/broker/check", headers=self._write_headers())
        self.assertNotEqual(res.status_code, 403)

    async def test_admin_is_untouched_by_the_gate(self):
        await self._login(self.PIN, self._code())
        res = await self.client.get("/api/admin/health")
        self.assertEqual(res.status_code, 200)

    async def test_the_shell_arrives_marked_read_only_for_a_viewer(self):
        await self._login_as_viewer()
        html = (await self.client.get("/app")).text
        self.assertIn('class="read-only-account"', html[:400], "the class must be on <html>, before first paint")

    async def test_the_shell_is_not_marked_for_admin(self):
        await self._login(self.PIN, self._code())
        html = (await self.client.get("/app")).text
        self.assertNotIn("read-only-account", html[:400])

    # ── the session store ────────────────────────────────────────────

    def test_legacy_sessions_without_a_role_are_admin(self):
        record = app_module._normalize_session_record({"expires_at": (app_module._session_now()).isoformat()})
        self.assertEqual(record["role"], "admin")

    def test_revoking_viewer_sessions_keeps_admin_ones(self):
        admin_tok = app_module._create_session(role="admin")
        viewer_tok = app_module._create_session(role="viewer")
        self.assertEqual(app_module._revoke_sessions_with_role("viewer"), 1)
        self.assertTrue(app_module._validate_session(admin_tok))
        self.assertFalse(app_module._validate_session(viewer_tok))

    # ── the admin console ────────────────────────────────────────────

    def test_viewer_pin_must_be_six_digits(self):
        for bad in ("12345", "1234567", "abcdef", "12 456"):
            with self.assertRaises(Exception):
                app_module._normalize_admin_env_update("CRYPTOFORGE_VIEWER_PIN", bad)
        self.assertEqual(app_module._normalize_admin_env_update("CRYPTOFORGE_VIEWER_PIN", "123456"), "123456")
        self.assertEqual(app_module._normalize_admin_env_update("CRYPTOFORGE_VIEWER_PIN", ""), "", "blank clears it")

    async def test_console_refuses_a_viewer_pin_equal_to_the_unlock_pin(self):
        await self._login(self.PIN, self._code())
        res = await self.client.put(
            "/api/admin/config",
            headers=self._write_headers(),
            json={"values": {"CRYPTOFORGE_VIEWER_PIN": self.PIN}},
        )
        self.assertEqual(res.status_code, 400, res.text)
        self.assertIn("differ", res.text)

    async def test_console_saves_a_new_viewer_pin_and_signs_old_viewers_out(self):
        env_path = os.path.join(self._tmp.name, ".env")
        with open(env_path, "w") as fh:
            fh.write(f"CRYPTOFORGE_PIN={self.PIN}\nCRYPTOFORGE_VIEWER_PIN={self.VIEWER}\n")
        self.addCleanup(lambda: os.environ.pop("CRYPTOFORGE_VIEWER_PIN", None))
        with patch.object(app_module, "_ENV_PATH", env_path):
            old_viewer = app_module._create_session(role="viewer")
            await self._login(self.PIN, self._code())
            res = await self.client.put(
                "/api/admin/config",
                headers=self._write_headers(),
                json={"values": {"CRYPTOFORGE_VIEWER_PIN": "888888"}},
            )
            self.assertEqual(res.status_code, 200, res.text)
            self.assertEqual(app_module.VIEWER_PIN, "888888")
            self.assertFalse(app_module._validate_session(old_viewer), "the old viewer PIN's session must be gone")
            self.assertEqual((await self._login(self.VIEWER)).status_code, 401)
            self.assertEqual((await self._login("888888")).status_code, 200)
