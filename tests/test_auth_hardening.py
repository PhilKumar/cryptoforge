"""
Tests for the login gate: who gets rate limited, how hard, and whether the
second factor actually holds.

Since 2026-08-17 the desk opens on ACCOUNTS — username + password, an
authenticator per account, a passkey per device (accounts.py) — rather than
one shared PIN. CRYPTOFORGE_PIN survives as the first admin's password on an
empty install; CRYPTOFORGE_TOTP_SECRET seeds that account's authenticator.
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
    """The authenticator maths, now in accounts.py on pyotp."""

    wants_totp = True
    # RFC 6238 Appendix B publishes expected codes for the ASCII secret
    # "12345678901234567890". If our implementation disagrees with these, it
    # disagrees with every authenticator app on the planet.
    RFC_SECRET = base64.b32encode(b"12345678901234567890").decode().rstrip("=")
    RFC_VECTORS = [(59, "287082"), (1111111109, "081804"), (1111111111, "050471"), (1234567890, "005924")]

    def setUp(self):
        import accounts

        self.accounts = accounts
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        patcher = patch.object(app_module, "_STATE_DB_FILE", os.path.join(self._tmp.name, "t.db"))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.store = app_module._account_store()
        self.user = self.store.create_user(
            "phil", accounts.hash_password("Passw0rd!"), role="admin", mfa_totp_secret=self.RFC_SECRET
        )

    def _code(self, offset=0, now=None):
        import pyotp

        t = (time.time() if now is None else now) + offset * 30
        return pyotp.TOTP(self.RFC_SECRET).at(int(t))

    def test_matches_rfc6238_published_vectors(self):
        for unix_time, expected in self.RFC_VECTORS:
            self.assertEqual(
                self.accounts.matching_totp_counter(self.RFC_SECRET, expected, now=unix_time),
                unix_time // 30,
                f"at t={unix_time}",
            )

    def test_accepts_the_current_code(self):
        self.assertTrue(self.store.verify_user_totp(self.user, self._code()))

    def test_tolerates_one_step_of_clock_drift(self):
        now = time.time()
        self.assertIsNotNone(self.accounts.matching_totp_counter(self.RFC_SECRET, self._code(-1, now), now=now))
        self.assertIsNotNone(self.accounts.matching_totp_counter(self.RFC_SECRET, self._code(+1, now), now=now))

    def test_rejects_two_steps_of_drift(self):
        now = time.time()
        self.assertIsNone(self.accounts.matching_totp_counter(self.RFC_SECRET, self._code(-2, now), now=now))

    def test_a_spent_code_cannot_be_replayed(self):
        # A code is valid for 30 seconds. Without this, someone who reads it
        # over your shoulder can sign in behind you inside that window.
        code = self._code()
        self.assertTrue(self.store.verify_user_totp(self.user, code))
        user = self.store.get_user(self.user["id"])
        self.assertFalse(self.store.verify_user_totp(user, code), "a spent code must not work twice")

    def test_checking_a_code_does_not_spend_it(self):
        """matching_totp_counter only LOOKS; only a successful login claims the
        counter. A wrong password must leave the code on the phone usable."""
        code = self._code()
        for attempt in range(5):
            self.assertIsNotNone(
                self.accounts.matching_totp_counter(self.RFC_SECRET, code),
                f"attempt {attempt + 1}: a failed login must leave the code usable",
            )

    def test_rejects_malformed_input(self):
        for bad in ("", "12345", "1234567", "abcdef", None, "   "):
            self.assertIsNone(self.accounts.matching_totp_counter(self.RFC_SECRET, bad), repr(bad))

    def test_bad_secret_fails_closed(self):
        self.assertIsNone(self.accounts.matching_totp_counter("not-valid-base32!!", "123456"))

    def test_setup_tool_agrees_with_the_server(self):
        """tools/totp_setup.py --verify must not disagree with the login path."""
        import tools.totp_setup as setup_tool

        counter = int(time.time() // 30)
        code = setup_tool.code_at(self.RFC_SECRET, counter)
        self.assertEqual(self.accounts.matching_totp_counter(self.RFC_SECRET, code), counter)

    def test_an_account_without_an_authenticator_never_verifies(self):
        plain = self.store.create_user("kavi", self.accounts.hash_password("lookonly1"), role="viewer")
        self.assertFalse(self.store.verify_user_totp(plain, self._code()))


class _AccountsHarness(unittest.IsolatedAsyncioTestCase):
    """A fresh state DB, the seeded admin, and an HTTP client."""

    wants_totp = True  # opt out of conftest's blanking; each class sets TOTP_SECRET itself
    RFC_SECRET = TotpTests.RFC_SECRET
    PIN = "424242"
    SEED_TOTP = ""  # the seeded admin's authenticator secret, "" for none

    async def asyncSetUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addAsyncCleanup(self._tmp.cleanup)
        for name, value in (
            ("AUTH_PIN", self.PIN),
            ("ADMIN_BOOTSTRAP_PASSWORD", ""),
            ("TOTP_SECRET", self.SEED_TOTP),
            ("_STATE_DB_FILE", os.path.join(self._tmp.name, "t.db")),
        ):
            patcher = patch.object(app_module, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        patcher = patch.object(app_module, "_get_redis", return_value=None)
        patcher.start()
        self.addCleanup(patcher.stop)
        app_module._login_state.clear()
        self.addCleanup(app_module._login_state.clear)
        self.client = AsyncClient(transport=ASGITransport(app=app_module.app), base_url="http://testserver")
        self.addAsyncCleanup(self.client.aclose)

    def _code(self, secret=None):
        import pyotp

        return pyotp.TOTP(secret or self.RFC_SECRET).now()

    async def _login(self, username="admin", password=None, code=""):
        body = {"username": username, "password": self.PIN if password is None else password}
        if code:
            body["totp"] = code
        return await self.client.post("/api/auth/login", json=body)

    def _write_headers(self):
        return {
            "X-CSRF-Token": self.client.cookies.get("cryptoforge_csrf") or "",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "http://testserver",
        }

    @staticmethod
    def _detail(res):
        # error_handlers.py reshapes 4xx bodies into {success, error:{...}}.
        body = res.json()
        return (body.get("error") or {}).get("detail", body.get("detail"))


class LoginRouteTests(_AccountsHarness):
    """The login route end to end, with the seeded admin's authenticator ON."""

    SEED_TOTP = TotpTests.RFC_SECRET

    async def test_status_says_nothing_before_a_session(self):
        res = await self.client.get("/api/auth/status")
        self.assertEqual(res.json(), {"authenticated": False})

    async def test_first_login_seeds_the_admin_from_the_pin_and_the_totp_secret(self):
        self.assertEqual(app_module._account_store().count_users(), 0)
        res = await self._login()
        self.assertEqual(res.status_code, 428, "the seeded admin has an authenticator, so a code is asked for")
        self.assertEqual(res.json().get("code"), "mfa_required")
        admin = app_module._account_store().first_admin()
        self.assertEqual(admin["username"], "admin")
        self.assertTrue(admin["mfa_enabled"])

    async def test_password_alone_is_not_enough(self):
        self.assertEqual((await self._login()).status_code, 428)
        self.assertFalse((await self.client.get("/api/auth/status")).json()["authenticated"])

    async def test_correct_password_and_code_gets_in(self):
        res = await self._login(code=self._code())
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["role"], "admin")
        self.assertIn("cryptoforge_session", res.cookies)
        status = (await self.client.get("/api/auth/status")).json()
        self.assertEqual(status["username"], "admin")
        self.assertTrue(status["mfa_enabled"])

    async def test_legacy_login_without_a_username_is_the_first_admin(self):
        """Every older test and the last PIN-era client send only a password."""
        res = await self.client.post("/api/auth/login", json={"password": self.PIN, "totp": self._code()})
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["username"], "admin")

    async def test_a_password_typo_does_not_burn_the_code(self):
        """Mistype the password, then retype it correctly with the same code
        still on screen — that must work."""
        code = self._code()
        self.assertEqual((await self._login(password="424241", code=code)).status_code, 401)
        self.assertEqual((await self._login(code=code)).status_code, 200)

    async def test_a_code_already_used_to_log_in_is_refused(self):
        code = self._code()
        self.assertEqual((await self._login(code=code)).status_code, 200)
        self.assertEqual((await self._login(code=code)).status_code, 401, "no replay")

    async def test_wrong_code_with_right_password_is_refused(self):
        self.assertEqual((await self._login(code="000000")).status_code, 401)

    async def test_the_error_does_not_say_which_factor_failed(self):
        bad_pw = self._detail(await self._login(password="000000", code=self._code()))
        bad_code = self._detail(await self._login(code="000000"))
        self.assertNotIn("password", bad_code.lower().replace("credentials", ""))
        self.assertEqual(bad_pw, "Invalid username or password")
        self.assertEqual(bad_code, "Invalid credentials or authenticator code")

    async def test_unknown_user_and_wrong_password_read_the_same(self):
        ghost = self._detail(await self._login(username="nobody", password="whatever"))
        wrong = self._detail(await self._login(password="wrong"))
        self.assertEqual(ghost, wrong)

    async def test_repeated_failures_still_lock_out(self):
        for _ in range(5):
            await self._login(password="000000")
        res = await self._login(code=self._code())
        self.assertEqual(res.status_code, 429, "the lockout must still bite")

    async def test_the_shell_is_not_marked_read_only_for_admin(self):
        await self._login(code=self._code())
        html = (await self.client.get("/app")).text
        self.assertNotIn("read-only-account", html[:400])


class AccountsAndRolesTests(_AccountsHarness):
    """Admin creates accounts; each role gets what it should. Seeded admin
    has NO authenticator here, so the flows are short."""

    async def _login_admin(self):
        res = await self._login()
        self.assertEqual(res.status_code, 200, res.text)

    async def _create(self, username, password, role):
        return await self.client.post(
            "/api/admin/users",
            headers=self._write_headers(),
            json={"username": username, "password": password, "role": role},
        )

    async def test_admin_creates_a_viewer_who_signs_in_read_only(self):
        await self._login_admin()
        res = await self._create("kavi", "lookonly1", "viewer")
        self.assertEqual(res.status_code, 200, res.text)
        viewer = AsyncClient(transport=ASGITransport(app=app_module.app), base_url="http://testserver")
        self.addAsyncCleanup(viewer.aclose)
        res = await viewer.post("/api/auth/login", json={"username": "kavi", "password": "lookonly1"})
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["role"], "viewer")
        status = (await viewer.get("/api/auth/status")).json()
        self.assertTrue(status["read_only"])
        html = (await viewer.get("/app")).text
        self.assertIn('class="read-only-account"', html[:400], "the class must be on <html>, before first paint")

    async def test_viewer_reads_pass_writes_and_admin_reads_are_refused(self):
        await self._login_admin()
        self.assertEqual((await self._create("kavi", "lookonly1", "viewer")).status_code, 200)
        v = AsyncClient(transport=ASGITransport(app=app_module.app), base_url="http://testserver")
        self.addAsyncCleanup(v.aclose)
        await v.post("/api/auth/login", json={"username": "kavi", "password": "lookonly1"})
        for path in (
            "/api/strategies",
            "/api/runs",
            "/api/notifications",
            "/api/broker/settings",
            "/api/live/status",
            "/api/user/profile",
        ):
            res = await v.get(path)
            self.assertNotIn(res.status_code, (401, 403), f"{path} must be readable by a viewer, got {res.status_code}")
        headers = {
            "X-CSRF-Token": v.cookies.get("cryptoforge_csrf") or "",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "http://testserver",
        }
        for method, path in (
            ("POST", "/api/live/start"),
            ("POST", "/api/paper/start"),
            ("POST", "/api/emergency-stop"),
            ("POST", "/api/orders/place"),
            ("POST", "/api/backtest"),
            ("POST", "/api/notifications/ack"),
            ("PUT", "/api/admin/config"),
            ("POST", "/api/admin/users"),
            ("DELETE", "/api/cache"),
        ):
            res = await v.request(method, path, headers=headers, json={})
            self.assertEqual(res.status_code, 403, f"{method} {path} -> {res.status_code}")
            self.assertEqual(res.json().get("code"), "viewer_read_only", res.text)
        for path in (
            "/api/admin/config",
            "/api/admin/users",
            "/api/ops/state/backup",
            "/api/audit/production-readiness",
            "/api/cascade/feed/subscribers",
        ):
            res = await v.get(path)
            self.assertEqual(res.status_code, 403, f"GET {path} -> {res.status_code}")
        # Their own session and their own security are theirs.
        res = await v.put(
            "/api/user/password", headers=headers, json={"current_password": "lookonly1", "new_password": "lookonly2"}
        )
        self.assertEqual(res.status_code, 200, res.text)
        self.assertEqual((await v.post("/api/broker/check", headers=headers)).status_code, 200)
        self.assertEqual((await v.post("/api/auth/logout", headers=headers)).status_code, 200)
        self.assertFalse((await v.get("/api/auth/status")).json()["authenticated"])

    async def test_a_user_role_trades_but_never_sees_the_admin_console(self):
        await self._login_admin()
        self.assertEqual((await self._create("trader", "trade1234", "user")).status_code, 200)
        u = AsyncClient(transport=ASGITransport(app=app_module.app), base_url="http://testserver")
        self.addAsyncCleanup(u.aclose)
        await u.post("/api/auth/login", json={"username": "trader", "password": "trade1234"})
        self.assertEqual((await u.get("/api/admin/users")).status_code, 403)
        self.assertEqual((await u.get("/api/admin/config")).status_code, 403)
        headers = {
            "X-CSRF-Token": u.cookies.get("cryptoforge_csrf") or "",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "http://testserver",
        }
        # A trading write is not refused by ROLE (it may fail for other reasons — no engine, bad body — never 403).
        res = await u.post("/api/paper/stop", headers=headers, json={})
        self.assertNotEqual(res.status_code, 403)
        html = (await u.get("/app")).text
        self.assertNotIn("read-only-account", html[:400])

    async def test_console_rules(self):
        await self._login_admin()
        self.assertEqual((await self._create("x", "trade1234", "viewer")).status_code, 400, "username too short")
        self.assertEqual((await self._create("kavi", "short1", "viewer")).status_code, 400, "password policy")
        self.assertEqual((await self._create("kavi", "onlyletters", "viewer")).status_code, 400, "needs a digit")
        self.assertEqual((await self._create("kavi", "lookonly1", "god")).status_code, 400, "unknown role")
        self.assertEqual((await self._create("kavi", "lookonly1", "viewer")).status_code, 200)
        self.assertEqual(
            (await self._create("Kavi", "lookonly1", "viewer")).status_code, 409, "case-insensitive uniqueness"
        )
        me = (await self.client.get("/api/auth/status")).json()["user_id"]
        headers = self._write_headers()
        self.assertEqual(
            (await self.client.put(f"/api/admin/users/{me}/toggle", headers=headers, json={})).status_code, 400
        )
        self.assertEqual((await self.client.delete(f"/api/admin/users/{me}", headers=headers)).status_code, 400)
        self.assertEqual(
            (
                await self.client.put(f"/api/admin/users/{me}/role", headers=headers, json={"role": "viewer"})
            ).status_code,
            400,
        )

    async def test_disable_ends_the_sessions_and_delete_removes_the_account(self):
        await self._login_admin()
        created = (await self._create("kavi", "lookonly1", "viewer")).json()["user"]
        v = AsyncClient(transport=ASGITransport(app=app_module.app), base_url="http://testserver")
        self.addAsyncCleanup(v.aclose)
        await v.post("/api/auth/login", json={"username": "kavi", "password": "lookonly1"})
        self.assertTrue((await v.get("/api/auth/status")).json()["authenticated"])
        headers = self._write_headers()
        res = await self.client.put(f"/api/admin/users/{created['id']}/toggle", headers=headers, json={})
        self.assertEqual(res.status_code, 200)
        self.assertFalse(res.json()["is_active"])
        self.assertFalse(
            (await v.get("/api/auth/status")).json()["authenticated"], "a disabled account's session ends at once"
        )
        res = await v.post("/api/auth/login", json={"username": "kavi", "password": "lookonly1"})
        self.assertEqual(res.status_code, 403)
        self.assertEqual(
            (await self.client.delete(f"/api/admin/users/{created['id']}", headers=headers)).status_code, 200
        )
        names = [u["username"] for u in (await self.client.get("/api/admin/users")).json()["users"]]
        self.assertNotIn("kavi", names)

    async def test_admin_password_reset_signs_the_account_out_everywhere(self):
        await self._login_admin()
        created = (await self._create("kavi", "lookonly1", "viewer")).json()["user"]
        v = AsyncClient(transport=ASGITransport(app=app_module.app), base_url="http://testserver")
        self.addAsyncCleanup(v.aclose)
        await v.post("/api/auth/login", json={"username": "kavi", "password": "lookonly1"})
        res = await self.client.put(
            f"/api/admin/users/{created['id']}/password", headers=self._write_headers(), json={"password": "newpass99"}
        )
        self.assertEqual(res.status_code, 200)
        self.assertFalse((await v.get("/api/auth/status")).json()["authenticated"])
        self.assertEqual(
            (await v.post("/api/auth/login", json={"username": "kavi", "password": "lookonly1"})).status_code, 401
        )
        self.assertEqual(
            (await v.post("/api/auth/login", json={"username": "kavi", "password": "newpass99"})).status_code, 200
        )

    async def test_the_last_active_admin_cannot_be_disabled_demoted_or_deleted(self):
        await self._login_admin()
        self.assertEqual((await self._create("second", "trade1234", "admin")).status_code, 200)
        me = (await self.client.get("/api/auth/status")).json()["user_id"]
        other = [u for u in (await self.client.get("/api/admin/users")).json()["users"] if u["id"] != me][0]
        headers = self._write_headers()
        # Two admins: the other one may be demoted.
        self.assertEqual(
            (
                await self.client.put(f"/api/admin/users/{other['id']}/role", headers=headers, json={"role": "viewer"})
            ).status_code,
            200,
        )
        # Now I am the last admin — the other (a viewer now) cannot make me anything else, and I refuse to touch myself.
        self.assertEqual(
            (await self.client.put(f"/api/admin/users/{me}/role", headers=headers, json={"role": "user"})).status_code,
            400,
        )


class MfaEnrolmentTests(_AccountsHarness):
    """Setting up, replacing and removing an authenticator from Account Settings."""

    async def _login_admin(self):
        self.assertEqual((await self._login()).status_code, 200)

    async def test_enrol_then_login_needs_the_code_and_other_sessions_end(self):
        await self._login_admin()
        other = AsyncClient(transport=ASGITransport(app=app_module.app), base_url="http://testserver")
        self.addAsyncCleanup(other.aclose)
        self.assertEqual(
            (await other.post("/api/auth/login", json={"username": "admin", "password": self.PIN})).status_code, 200
        )
        headers = self._write_headers()
        res = await self.client.post("/api/auth/mfa/enroll/start", headers=headers, json={"password": self.PIN})
        self.assertEqual(res.status_code, 200, res.text)
        body = res.json()
        self.assertEqual(body["status"], "pending")
        self.assertTrue(body["otpauth_uri"].startswith("otpauth://totp/CryptoForge:admin?"))
        self.assertTrue(body["qr_data_uri"].startswith("data:image/svg+xml;base64,"))
        # A wrong first code does not enable it.
        res = await self.client.post(
            "/api/auth/mfa/enroll/verify", headers=headers, json={"password": self.PIN, "totp": "000000"}
        )
        self.assertEqual(res.status_code, 401)
        self.assertFalse((await self.client.get("/api/auth/status")).json()["mfa_enabled"])
        res = await self.client.post(
            "/api/auth/mfa/enroll/verify",
            headers=headers,
            json={"password": self.PIN, "totp": self._code(body["secret"])},
        )
        self.assertEqual(res.status_code, 200, res.text)
        # This browser was rotated onto a fresh session; the other one is gone.
        self.assertTrue((await self.client.get("/api/auth/status")).json()["mfa_enabled"])
        self.assertFalse((await other.get("/api/auth/status")).json()["authenticated"])
        # And signing in now needs the code.
        fresh = AsyncClient(transport=ASGITransport(app=app_module.app), base_url="http://testserver")
        self.addAsyncCleanup(fresh.aclose)
        self.assertEqual(
            (await fresh.post("/api/auth/login", json={"username": "admin", "password": self.PIN})).status_code, 428
        )
        # The enrolment just spent this window's code — the replay guard would
        # (rightly) refuse it — so sign in with the next window's, which the
        # ±1-step drift allowance accepts.
        import pyotp

        ahead = pyotp.TOTP(body["secret"]).at(int(time.time()) + 30)
        self.assertEqual(
            (
                await fresh.post("/api/auth/login", json={"username": "admin", "password": self.PIN, "totp": ahead})
            ).status_code,
            200,
        )

    async def test_start_needs_the_current_password(self):
        await self._login_admin()
        res = await self.client.post(
            "/api/auth/mfa/enroll/start", headers=self._write_headers(), json={"password": "nope"}
        )
        self.assertEqual(res.status_code, 401)

    async def test_disable_needs_password_and_a_fresh_code_and_signs_out(self):
        await self._login_admin()
        headers = self._write_headers()
        body = (
            await self.client.post("/api/auth/mfa/enroll/start", headers=headers, json={"password": self.PIN})
        ).json()
        code = self._code(body["secret"])
        self.assertEqual(
            (
                await self.client.post(
                    "/api/auth/mfa/enroll/verify", headers=headers, json={"password": self.PIN, "totp": code}
                )
            ).status_code,
            200,
        )
        headers = self._write_headers()  # rotated session, fresh CSRF cookie
        # The enrolment code was just spent; wait for the next window would be slow, so use the pending-secret trick:
        # verify_user_totp accepts ±1 step, and claim rejects <= last counter, so a code one step AHEAD is fresh.
        import pyotp

        ahead = pyotp.TOTP(body["secret"]).at(int(time.time()) + 30)
        res = await self.client.request(
            "DELETE", "/api/auth/mfa", headers=headers, json={"password": self.PIN, "totp": "000000"}
        )
        self.assertEqual(res.status_code, 401)
        res = await self.client.request(
            "DELETE", "/api/auth/mfa", headers=headers, json={"password": self.PIN, "totp": ahead}
        )
        self.assertEqual(res.status_code, 200, res.text)
        self.assertFalse((await self.client.get("/api/auth/status")).json()["authenticated"])
        self.assertEqual(
            (await self.client.post("/api/auth/login", json={"username": "admin", "password": self.PIN})).status_code,
            200,
        )


class SessionStoreTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        for name, value in (
            ("AUTH_PIN", "424242"),
            ("ADMIN_BOOTSTRAP_PASSWORD", ""),
            ("TOTP_SECRET", ""),
            ("_STATE_DB_FILE", os.path.join(self._tmp.name, "t.db")),
        ):
            patcher = patch.object(app_module, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_legacy_sessions_without_a_user_belong_to_the_first_admin(self):
        record = app_module._normalize_session_record({"expires_at": (app_module._session_now()).isoformat()})
        self.assertEqual(record["role"], "admin")
        self.assertIsNone(record["user_id"])
        user = app_module._session_user_for_record(record)
        self.assertEqual(
            user["username"], "admin", "an empty install seeds the admin so the old session survives the upgrade"
        )

    def test_revoking_one_account_keeps_the_others_and_the_kept_token(self):
        store = app_module._account_store()
        a = store.create_user("a", "x", role="admin")
        b = store.create_user("b", "x", role="viewer")
        tok_a = app_module._create_session(role="admin", user_id=a["id"])
        tok_b1 = app_module._create_session(role="viewer", user_id=b["id"])
        tok_b2 = app_module._create_session(role="viewer", user_id=b["id"])
        self.assertEqual(app_module._revoke_sessions_for_user(b["id"], keep_token=tok_b2), 1)
        self.assertTrue(app_module._validate_session(tok_a))
        self.assertFalse(app_module._validate_session(tok_b1))
        self.assertTrue(app_module._validate_session(tok_b2))

    def test_a_session_dies_with_its_account(self):
        store = app_module._account_store()
        b = store.create_user("b", "x", role="viewer")
        tok = app_module._create_session(role="viewer", user_id=b["id"])
        self.assertTrue(app_module._validate_session(tok))
        store.update_user(b["id"], is_active=False)
        self.assertFalse(app_module._validate_session(tok))


class PasskeyRouteTests(_AccountsHarness):
    """The ceremony's plumbing — options, challenge lifecycle, refusals. The
    signature maths itself is exercised with a real ECDSA key below."""

    async def _login_admin(self):
        self.assertEqual((await self._login()).status_code, 200)

    async def test_register_options_carry_a_challenge_and_the_host_as_rp(self):
        await self._login_admin()
        res = await self.client.post("/api/auth/passkeys/register/options", headers=self._write_headers(), json={})
        self.assertEqual(res.status_code, 200, res.text)
        body = res.json()
        self.assertEqual(body["options"]["rp"]["id"], "testserver")
        self.assertEqual(body["options"]["user"]["name"], "admin")
        self.assertTrue(body["challenge_id"])
        self.assertTrue(body["options"]["challenge"])
        self.assertEqual(body["options"]["authenticatorSelection"]["userVerification"], "required")

    async def test_login_options_need_no_session_and_reveal_no_accounts(self):
        res = await self.client.post("/api/auth/passkeys/login/options", json={})
        self.assertEqual(res.status_code, 200, res.text)
        self.assertEqual(res.json()["options"]["allowCredentials"], [])

    async def test_a_challenge_is_single_use_and_purpose_bound(self):
        await self._login_admin()
        headers = self._write_headers()
        body = (await self.client.post("/api/auth/passkeys/register/options", headers=headers, json={})).json()
        # Wrong purpose: a login challenge presented as a registration.
        login = (await self.client.post("/api/auth/passkeys/login/options", json={})).json()
        res = await self.client.post(
            "/api/auth/passkeys/register/verify",
            headers=headers,
            json={"challenge_id": login["challenge_id"], "credential": {}},
        )
        self.assertEqual(res.status_code, 400)
        # Right purpose, garbage credential: refused, and the challenge is now spent.
        res = await self.client.post(
            "/api/auth/passkeys/register/verify",
            headers=headers,
            json={"challenge_id": body["challenge_id"], "credential": {}},
        )
        self.assertEqual(res.status_code, 400)
        res = await self.client.post(
            "/api/auth/passkeys/register/verify",
            headers=headers,
            json={"challenge_id": body["challenge_id"], "credential": {}},
        )
        self.assertEqual(res.status_code, 400)
        self.assertIn("expired", self._detail(res).lower())

    async def test_an_unknown_passkey_cannot_sign_in(self):
        login = (await self.client.post("/api/auth/passkeys/login/options", json={})).json()
        res = await self.client.post(
            "/api/auth/passkeys/login/verify",
            json={"challenge_id": login["challenge_id"], "credential": {"id": "nope"}},
        )
        self.assertEqual(res.status_code, 401)

    async def test_a_real_key_registers_and_signs_in(self):
        """A full ceremony with a P-256 key generated here, the way a phone would."""
        import hashlib
        import json as _json

        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import ec

        import webauthn_auth as wa

        await self._login_admin()
        headers = self._write_headers()
        opts = (await self.client.post("/api/auth/passkeys/register/options", headers=headers, json={})).json()
        rp_id = opts["options"]["rp"]["id"]
        origin = "http://testserver"
        priv = ec.generate_private_key(ec.SECP256R1())
        nums = priv.public_key().public_numbers()
        cose = {1: 2, 3: -7, -1: 1, -2: nums.x.to_bytes(32, "big"), -3: nums.y.to_bytes(32, "big")}
        cred_id = b"cred-0001"
        rp_hash = hashlib.sha256(rp_id.encode()).digest()
        # authData: rpIdHash(32) flags(1) signCount(4) aaguid(16) credIdLen(2) credId cosePubKey
        flags = wa.FLAG_USER_PRESENT | wa.FLAG_USER_VERIFIED | wa.FLAG_ATTESTED_CREDENTIAL_DATA
        auth_data = (
            rp_hash
            + bytes([flags])
            + (0).to_bytes(4, "big")
            + b"\x00" * 16
            + len(cred_id).to_bytes(2, "big")
            + cred_id
            + wa._cose_to_bytes(cose)
        )
        att_obj = b"\xa1" + b"\x68authData" + wa._cbor_write_head(2, len(auth_data)) + auth_data
        client_data = _json.dumps(
            {"type": "webauthn.create", "challenge": opts["options"]["challenge"], "origin": origin}
        ).encode()
        res = await self.client.post(
            "/api/auth/passkeys/register/verify",
            headers=headers,
            json={
                "challenge_id": opts["challenge_id"],
                "label": "Test phone",
                "credential": {
                    "id": wa.b64url_encode(cred_id),
                    "type": "public-key",
                    "response": {
                        "clientDataJSON": wa.b64url_encode(client_data),
                        "attestationObject": wa.b64url_encode(att_obj),
                    },
                },
            },
        )
        self.assertEqual(res.status_code, 200, res.text)
        listed = (await self.client.get("/api/auth/passkeys")).json()["passkeys"]
        self.assertEqual([p["label"] for p in listed], ["Test phone"])

        # Sign in on a fresh client with only the key.
        fresh = AsyncClient(transport=ASGITransport(app=app_module.app), base_url="http://testserver")
        self.addAsyncCleanup(fresh.aclose)
        login = (await fresh.post("/api/auth/passkeys/login/options", json={})).json()
        client_data = _json.dumps(
            {"type": "webauthn.get", "challenge": login["options"]["challenge"], "origin": origin}
        ).encode()
        auth_data = rp_hash + bytes([wa.FLAG_USER_PRESENT | wa.FLAG_USER_VERIFIED]) + (1).to_bytes(4, "big")
        signature = priv.sign(auth_data + hashlib.sha256(client_data).digest(), ec.ECDSA(hashes.SHA256()))
        res = await fresh.post(
            "/api/auth/passkeys/login/verify",
            json={
                "challenge_id": login["challenge_id"],
                "credential": {
                    "id": wa.b64url_encode(cred_id),
                    "type": "public-key",
                    "response": {
                        "clientDataJSON": wa.b64url_encode(client_data),
                        "authenticatorData": wa.b64url_encode(auth_data),
                        "signature": wa.b64url_encode(signature),
                    },
                },
            },
        )
        self.assertEqual(res.status_code, 200, res.text)
        self.assertEqual(res.json()["username"], "admin")
        self.assertTrue((await fresh.get("/api/auth/status")).json()["authenticated"])

        # Removing the device ends its way in.
        cid = listed[0]["credential_id"]
        self.assertEqual((await self.client.delete(f"/api/auth/passkeys/{cid}", headers=headers)).status_code, 200)
        self.assertEqual((await self.client.get("/api/auth/passkeys")).json()["passkeys"], [])


if __name__ == "__main__":
    unittest.main()
