"""
Tests for the login gate: who gets rate limited, how hard, and whether the
second factor actually holds.

The thing these are really guarding is that a 6-digit PIN is the only secret
between a stranger and an app that places real orders.
"""

import base64
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

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
            self.assertTrue(app_module._verify_totp(app_module._totp_code_at(now)))

    def test_tolerates_one_step_of_clock_drift(self):
        with patch.object(app_module, "TOTP_SECRET", self.RFC_SECRET):
            now = int(time.time() // app_module._TOTP_STEP_SEC)
            self.assertTrue(app_module._verify_totp(app_module._totp_code_at(now - 1)))
            app_module._totp_used_counters.clear()
            self.assertTrue(app_module._verify_totp(app_module._totp_code_at(now + 1)))

    def test_rejects_two_steps_of_drift(self):
        with patch.object(app_module, "TOTP_SECRET", self.RFC_SECRET):
            now = int(time.time() // app_module._TOTP_STEP_SEC)
            self.assertFalse(app_module._verify_totp(app_module._totp_code_at(now - 2)))

    def test_a_code_cannot_be_replayed(self):
        # A code is valid for 30 seconds. Without this, someone who reads it
        # over your shoulder can sign in behind you inside that window.
        with patch.object(app_module, "TOTP_SECRET", self.RFC_SECRET):
            now = int(time.time() // app_module._TOTP_STEP_SEC)
            code = app_module._totp_code_at(now)
            self.assertTrue(app_module._verify_totp(code))
            self.assertFalse(app_module._verify_totp(code), "the same code must not work twice")

    def test_rejects_malformed_input(self):
        with patch.object(app_module, "TOTP_SECRET", self.RFC_SECRET):
            for bad in ("", "12345", "1234567", "abcdef", None, "   "):
                self.assertFalse(app_module._verify_totp(bad), repr(bad))

    def test_bad_secret_fails_closed(self):
        with patch.object(app_module, "TOTP_SECRET", "not-valid-base32!!"):
            self.assertFalse(app_module._verify_totp("123456"))

    def test_setup_tool_agrees_with_the_server(self):
        """tools/totp_setup.py --verify must not disagree with the login path."""
        import tools.totp_setup as setup_tool

        counter = int(time.time() // app_module._TOTP_STEP_SEC)
        with patch.object(app_module, "TOTP_SECRET", self.RFC_SECRET):
            self.assertEqual(setup_tool.code_at(self.RFC_SECRET, counter), app_module._totp_code_at(counter))


if __name__ == "__main__":
    unittest.main()
