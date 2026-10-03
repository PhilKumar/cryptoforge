"""What a buyer's machine sends home, and what it must never do while sending.

This is the only thing in the executor that tells anyone anything about a
buyer's account, so the tests that matter are the ones about restraint: it
carries no credentials, it cannot stop a tick, and a desk that is down does not
turn into a retry storm on a machine whose job is filling orders.

The server's half of the same wire is tested in tests/test_buyer_reports.py;
`test_the_server_accepts_what_the_executor_signs` is the one that proves the
two halves still agree, since they sign by hand on opposite sides of a package
boundary (the executor imports nothing from engine/, by design).
"""

import unittest

from executor.reporting import APP_VERSION, MAX_FILLS, MAX_POSITIONS, ReportSender, build_report, sign_payload


class _Config:
    exchange = "binance"
    quote_asset = "USDT"
    api_key = "SHOULD-NEVER-TRAVEL"  # noqa: S105 - the point of the leak test
    api_secret = "ALSO-NEVER"  # noqa: S105
    server_url = "https://crypto.philforge.in"


class _Adapter:
    def __init__(self, balance=1480.22, boom=None):
        self._balance = balance
        self._boom = boom

    def free_balance(self, asset):
        if self._boom:
            raise RuntimeError(self._boom)
        return self._balance


class _Runtime:
    def __init__(self, rounds=None):
        self._rounds = rounds or []

    def rounds_view(self, limit=50):
        return self._rounds[:limit]


def _portfolio(**over):
    data = {
        "holdings": [
            {
                "campaign_id": "c-1",
                "symbol": "SOLUSDT",
                "quantity": 3.5,
                "avg_entry": 148.2,
                "last_price": 144.6,
                "unrealised_usd": -12.6,
            }
        ],
        "free_quote": 1480.22,
        "invested_usd": 518.7,
        "unrealised_usd": -12.6,
        "realised_usd": 41.2,
    }
    data.update(over)
    return data


def _build(**over):
    kwargs = {
        "runtime": _Runtime(),
        "adapter": _Adapter(),
        "config": _Config(),
        "status": {"following": 2},
        "portfolio": _portfolio(),
        "feed_state": "connected",
        "started_at": 1000.0,
        "now": 8200.0,
    }
    kwargs.update(over)
    return build_report(**kwargs)


class BuildTests(unittest.TestCase):
    def test_it_reports_the_money_and_the_health(self):
        report = _build()
        self.assertTrue(report["running"])
        self.assertTrue(report["exchange_ok"])
        self.assertEqual(report["uptime_sec"], 7200)
        self.assertEqual(report["campaigns_following"], 2)
        self.assertEqual(report["app_version"], APP_VERSION)
        self.assertAlmostEqual(report["balance_usd"], 1480.22)
        self.assertAlmostEqual(report["committed_usd"], 518.7)
        self.assertEqual(report["positions"][0]["symbol"], "SOLUSDT")
        self.assertAlmostEqual(report["positions"][0]["mark"], 144.6)

    def test_no_credential_ever_appears_in_a_report(self):
        """The load-bearing one. A report says what the account is DOING,
        never how to touch it."""
        text = repr(_build())
        self.assertNotIn("SHOULD-NEVER-TRAVEL", text)
        self.assertNotIn("ALSO-NEVER", text)
        for banned in ("api_key", "api_secret", "secret", "key_path", "password"):
            self.assertNotIn(banned, text)

    def test_an_unreachable_exchange_is_reported_not_hidden(self):
        report = _build(adapter=_Adapter(boom="connection refused"))
        self.assertFalse(report["exchange_ok"])
        self.assertIn("connection refused", report["last_error"])

    def test_closed_rounds_become_the_recent_fills(self):
        rounds = [
            {"symbol": "ETHUSDT", "quantity": 0.4, "exit_price": 2150.5, "closed_ts": 1790000000},
            {"symbol": "SOLUSDT", "quantity": 2.0, "exit_price": 151.0, "closed_ts": 1789990000},
        ]
        report = _build(runtime=_Runtime(rounds))
        self.assertEqual(report["recent_fills"][0]["symbol"], "ETHUSDT")
        self.assertEqual(report["recent_fills"][0]["price"], 2150.5)

    def test_a_huge_book_is_capped_before_it_is_sent(self):
        many = [dict(_portfolio()["holdings"][0], campaign_id=f"c-{i}") for i in range(200)]
        rounds = [{"symbol": "X", "quantity": 1, "exit_price": 1, "closed_ts": i} for i in range(200)]
        report = _build(portfolio=_portfolio(holdings=many), runtime=_Runtime(rounds))
        self.assertLessEqual(len(report["positions"]), MAX_POSITIONS)
        self.assertLessEqual(len(report["recent_fills"]), MAX_FILLS)

    def test_a_missing_price_does_not_poison_the_numbers(self):
        holdings = [dict(_portfolio()["holdings"][0], last_price=None, unrealised_usd=None)]
        report = _build(portfolio=_portfolio(holdings=holdings, free_quote=None))
        self.assertIsNone(report["positions"][0]["mark"])
        self.assertIsNone(report["balance_usd"])


class _Identity:
    def __init__(self):
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

        self.buyer_id = "buyer-7"
        self.signing_key = Ed25519PrivateKey.generate()

    def public_key_b64(self):
        import base64

        from cryptography.hazmat.primitives import serialization

        return base64.b64encode(
            self.signing_key.public_key().public_bytes(
                encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
            )
        ).decode("ascii")


class SenderTests(unittest.TestCase):
    def setUp(self):
        self.identity = _Identity()
        self.sent = []

    def _sender(self, code=200, boom=None, every=60):
        def post(url, payload):
            if boom:
                raise RuntimeError(boom)
            self.sent.append((url, payload))
            return code

        return ReportSender(
            base_url="https://crypto.philforge.in",
            identity=self.identity,
            post=post,
            every_sec=every,
        )

    def test_it_sends_at_most_once_a_minute(self):
        sender = self._sender()
        self.assertTrue(sender.maybe_send(lambda: {"running": True}, now=1000))
        self.assertFalse(sender.maybe_send(lambda: {"running": True}, now=1030))
        self.assertTrue(sender.maybe_send(lambda: {"running": True}, now=1061))
        self.assertEqual(len(self.sent), 2)

    def test_a_desk_that_is_down_is_not_retried_every_tick(self):
        """A machine whose job is filling orders must not spend its time on
        HTTP because the desk is unreachable."""
        sender = self._sender(boom="no route to host")
        self.assertFalse(sender.maybe_send(lambda: {"running": True}, now=1000))
        self.assertFalse(sender.maybe_send(lambda: {"running": True}, now=1010))
        self.assertIn("not sent", sender.last_result)

    def test_a_refusal_is_recorded_and_swallowed(self):
        sender = self._sender(code=400)
        self.assertFalse(sender.send({"running": True}, now=1000))
        self.assertEqual(sender.last_result, "HTTP 400")

    def test_a_report_that_cannot_be_built_does_not_raise(self):
        sender = self._sender()

        def explode():
            raise ValueError("book is mid-write")

        self.assertFalse(sender.maybe_send(explode, now=1000))
        self.assertIn("not built", sender.last_result)
        self.assertEqual(self.sent, [])

    def test_it_posts_to_the_report_route(self):
        self._sender().send({"running": True}, now=1000)
        self.assertEqual(self.sent[0][0], "https://crypto.philforge.in/api/cascade/feed/report")

    def test_the_server_accepts_what_the_executor_signs(self):
        """Both halves sign by hand, on opposite sides of a package boundary.
        If one ever changes its serialization, this is what notices."""
        from engine.buyer_reports import verify_report
        from engine.cascade_feed import FeedSubscribers

        class Store:
            rows = {}

            def get(self, bucket, key, default=None):
                return self.rows.get((bucket, key), default)

            def put(self, bucket, key, payload):
                self.rows[(bucket, key)] = payload

            def get_mapping(self, bucket):
                return {k: v for (b, k), v in self.rows.items() if b == bucket}

            def delete(self, bucket, key):
                self.rows.pop((bucket, key), None)

        subs = FeedSubscribers(Store())
        subs.add("buyer-7", self.identity.public_key_b64())
        payload = sign_payload(self.identity, _build(), nonce="n1")
        out = verify_report(payload, subs)
        self.assertEqual(out["buyer_id"], "buyer-7")
        self.assertAlmostEqual(out["report"]["balance_usd"], 1480.22)

    def test_tampering_with_the_numbers_after_signing_is_caught(self):
        from engine.buyer_reports import ReportRefused, verify_report
        from engine.cascade_feed import FeedSubscribers

        class Store(dict):
            def get(self, bucket, key, default=None):
                return dict.get(self, (bucket, key), default)

            def put(self, bucket, key, payload):
                self[(bucket, key)] = payload

            def get_mapping(self, bucket):
                return {k: v for (b, k), v in self.items() if b == bucket}

            def delete(self, bucket, key):
                self.pop((bucket, key), None)

        subs = FeedSubscribers(Store())
        subs.add("buyer-7", self.identity.public_key_b64())
        payload = sign_payload(self.identity, _build(), nonce="n1")
        payload["report"]["balance_usd"] = 999999
        with self.assertRaises(ReportRefused):
            verify_report(payload, subs)


if __name__ == "__main__":
    unittest.main()
