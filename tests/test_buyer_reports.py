"""What a buyer's executor may tell the desk, and what we refuse to believe.

Phil, 03-Oct-2026, picking what the operator terminal may know: "Health +
money". So a report carries positions and P&L — which makes the verification
and the shaping load-bearing rather than decorative. The two tests that matter
are `test_a_report_signed_for_other_numbers_is_refused` (a signature lifted
onto a different body) and `test_a_hostile_report_cannot_fill_the_database`.
"""

import unittest

from engine.buyer_reports import (
    MAX_FILLS,
    MAX_POSITIONS,
    REPORT_FRESH_SEC,
    BuyerReports,
    ReportRefused,
    shape_report,
    sign_report,
    verify_report,
)
from engine.cascade_feed import FeedSigner, FeedSubscribers


class FakeStore:
    def __init__(self):
        self.rows = {}

    def get(self, bucket, key, default=None):
        return self.rows.get((bucket, str(key)), default)

    def put(self, bucket, key, payload):
        self.rows[(bucket, str(key))] = payload

    def delete(self, bucket, key):
        self.rows.pop((bucket, str(key)), None)

    def get_mapping(self, bucket):
        return {key: value for (row_bucket, key), value in self.rows.items() if row_bucket == bucket}


def _report(**overrides):
    body = {
        "app_version": "1.4.0",
        "exchange": "binance",
        "running": True,
        "uptime_sec": 7200,
        "exchange_ok": True,
        "feed_state": "connected",
        "campaigns_following": 2,
        "symbols": ["SOLUSDT", "ETHUSDT"],
        "balance_usd": 1480.22,
        "committed_usd": 519.78,
        "unrealized_usd": -12.40,
        "positions": [{"symbol": "SOLUSDT", "qty": 3.5, "avg_entry": 148.2, "mark": 144.6, "unrealized_usd": -12.6}],
        "recent_fills": [{"symbol": "SOLUSDT", "side": "BUY", "qty": 1.5, "price": 148.2, "at": 1785770000}],
    }
    body.update(overrides)
    return body


class ReportVerificationTests(unittest.TestCase):
    def setUp(self):
        self.store = FakeStore()
        self.now = 1785770000.0
        self.subs = FeedSubscribers(self.store, now_fn=lambda: self.now)
        self.key = FeedSigner.generate("buyer-7")
        self.subs.add("buyer-7", self.key.public_key_b64(), label="Anita")

    def _signed(self, **over):
        payload = sign_report("buyer-7", self.key, _report(), nonce="n1", timestamp=self.now)
        payload.update(over)
        return payload

    def test_a_signed_report_is_accepted_and_shaped(self):
        out = verify_report(self._signed(), self.subs, now=self.now)
        self.assertEqual(out["buyer_id"], "buyer-7")
        self.assertEqual(out["report"]["positions"][0]["symbol"], "SOLUSDT")
        self.assertAlmostEqual(out["report"]["balance_usd"], 1480.22)

    def test_a_report_signed_for_other_numbers_is_refused(self):
        """A valid signature must not be liftable onto a different body — the
        whole point of money being in here."""
        payload = self._signed()
        payload["report"] = _report(balance_usd=9_999_999)
        with self.assertRaises(ReportRefused):
            verify_report(payload, self.subs, now=self.now)

    def test_an_unregistered_machine_is_refused(self):
        stranger = FeedSigner.generate("buyer-nobody")
        payload = sign_report("buyer-nobody", stranger, _report(), nonce="n2", timestamp=self.now)
        with self.assertRaises(ReportRefused):
            verify_report(payload, self.subs, now=self.now)

    def test_someone_elses_key_cannot_report_as_this_buyer(self):
        other = FeedSigner.generate("buyer-7")  # right id, wrong key
        payload = sign_report("buyer-7", other, _report(), nonce="n3", timestamp=self.now)
        with self.assertRaises(ReportRefused):
            verify_report(payload, self.subs, now=self.now)

    def test_a_replayed_report_is_refused(self):
        seen = {}
        payload = self._signed()
        verify_report(payload, self.subs, now=self.now, seen_nonces=seen)
        with self.assertRaises(ReportRefused):
            verify_report(payload, self.subs, now=self.now, seen_nonces=seen)

    def test_a_report_from_a_wrong_clock_is_refused(self):
        payload = sign_report("buyer-7", self.key, _report(), nonce="n4", timestamp=self.now + 3600)
        with self.assertRaises(ReportRefused):
            verify_report(payload, self.subs, now=self.now)

    def test_a_lapsed_buyer_may_still_report(self):
        """The moment Phil most wants to see a screen is when it just stopped."""
        self.subs.set_status("buyer-7", "lapsed")
        out = verify_report(self._signed(), self.subs, now=self.now)
        self.assertEqual(out["buyer_id"], "buyer-7")

    def test_a_body_that_is_not_a_report_is_refused(self):
        payload = self._signed()
        payload["report"] = "all good"
        with self.assertRaises(ReportRefused):
            verify_report(payload, self.subs, now=self.now)


class ShapingTests(unittest.TestCase):
    def test_a_hostile_report_cannot_fill_the_database(self):
        shaped = shape_report(
            {
                "app_version": "v" * 500,
                "last_error": "e" * 10_000,
                "positions": [{"symbol": "X" * 99, "qty": 1}] * 500,
                "recent_fills": [{"symbol": "Y", "qty": 1}] * 500,
                "symbols": ["S"] * 500,
            }
        )
        self.assertLessEqual(len(shaped["app_version"]), 40)
        self.assertLessEqual(len(shaped["last_error"]), 200)
        self.assertLessEqual(len(shaped["positions"]), MAX_POSITIONS)
        self.assertLessEqual(len(shaped["recent_fills"]), MAX_FILLS)
        self.assertLessEqual(len(shaped["symbols"]), 20)
        self.assertLessEqual(len(shaped["positions"][0]["symbol"]), 20)

    def test_unknown_fields_are_not_stored(self):
        shaped = shape_report({"balance_usd": 10, "exchange_api_key": "SECRET", "note": {"deep": "junk"}})
        self.assertNotIn("exchange_api_key", shaped)
        self.assertNotIn("note", shaped)

    def test_nonsense_numbers_become_none_rather_than_poison(self):
        shaped = shape_report({"balance_usd": "lots", "unrealized_usd": float("inf"), "uptime_sec": -5})
        self.assertIsNone(shaped["balance_usd"])
        self.assertIsNone(shaped["unrealized_usd"])
        self.assertEqual(shaped["uptime_sec"], 0)

    def test_a_report_of_the_wrong_type_entirely_still_shapes(self):
        shaped = shape_report(["not", "a", "dict"])
        self.assertEqual(shaped["positions"], [])
        self.assertFalse(shaped["running"])


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.store = FakeStore()
        self.now = [1785770000.0]
        self.reports = BuyerReports(self.store, now_fn=lambda: self.now[0])

    def test_only_the_latest_report_is_kept(self):
        self.reports.put("buyer-7", shape_report(_report(balance_usd=1)))
        self.reports.put("buyer-7", shape_report(_report(balance_usd=2)))
        self.assertEqual(len(self.reports.mapping()), 1)
        self.assertEqual(self.reports.get("buyer-7")["report"]["balance_usd"], 2)

    def test_freshness_is_measured_not_assumed(self):
        self.reports.put("buyer-7", shape_report(_report()))
        self.assertTrue(self.reports.fresh("buyer-7"))
        self.now[0] += REPORT_FRESH_SEC + 1
        self.assertFalse(self.reports.fresh("buyer-7"))

    def test_a_buyer_who_never_reported_is_not_fresh(self):
        self.assertFalse(self.reports.fresh("buyer-nobody"))

    def test_forgetting_a_buyer_forgets_their_numbers(self):
        self.reports.put("buyer-7", shape_report(_report()))
        self.reports.remove("buyer-7")
        self.assertIsNone(self.reports.get("buyer-7"))


if __name__ == "__main__":
    unittest.main()
