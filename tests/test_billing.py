"""Money becoming an entitlement, and silence taking it away.

Billing writes the same record `entitled()` reads, so a bug here is a buyer
who paid and cannot trade, or one who stopped paying and still can. The tests
below are mostly about the second kind, because the first kind complains.

Two rules carry the design and each is checked from both directions:

- **Billing may only write active/lapsed.** `revoked` is Phil's manual ban, and
  a webhook — forged, replayed, or merely a genuine payment from someone he
  banned — must never undo it.
- **Webhooks extend life; silence ends it.** `expires_at` moves only when money
  arrived, so no failure of ours can keep a non-paying buyer alive.
"""

import hashlib
import hmac
import json
import unittest

from engine.billing import (
    GRACE_DAYS,
    BillingRefused,
    ProcessedEvents,
    RazorpayClient,
    apply_decision,
    buyer_id_of,
    decide,
    event_id,
    parse_event,
    subscription_id_of,
    verify_signature,
)

SECRET = "whsec_test"
NOW = 1785770000.0


def _body(**overrides) -> bytes:
    event = {
        "event": "subscription.charged",
        "payload": {"subscription": {"entity": {"id": "sub_123", "status": "active"}}},
    }
    event.update(overrides)
    return json.dumps(event).encode("utf-8")


def _sign(raw: bytes, secret: str = SECRET) -> str:
    return hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).hexdigest()


def _subscription(status="active", current_end=NOW + 30 * 86400, buyer="buyer-7"):
    return {
        "id": "sub_123",
        "status": status,
        "current_end": int(current_end) if current_end else None,
        "notes": {"buyer_id": buyer} if buyer else {},
    }


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
        return {k: v for (b, k), v in self.rows.items() if b == bucket}


class SignatureTests(unittest.TestCase):
    """Over the exact bytes received — never a re-serialization."""

    def test_a_correct_signature_passes(self):
        raw = _body()
        verify_signature(raw, _sign(raw), SECRET)

    def test_a_wrong_secret_is_refused(self):
        raw = _body()
        with self.assertRaises(BillingRefused) as caught:
            verify_signature(raw, _sign(raw, "wrong"), SECRET)
        self.assertEqual(caught.exception.status_code, 401)

    def test_a_re_serialized_body_does_not_verify(self):
        """The reason we sign bytes: re-encoding changes them.

        If the route ever normalised the payload before checking, an attacker's
        differently-spaced JSON would be reshaped into ours and verify against
        a signature that was never over our bytes.
        """
        raw = _body()
        reserialized = json.dumps(json.loads(raw), indent=2).encode("utf-8")
        self.assertNotEqual(raw, reserialized)
        with self.assertRaises(BillingRefused):
            verify_signature(reserialized, _sign(raw), SECRET)

    def test_an_unconfigured_server_refuses_rather_than_accepting_everything(self):
        """An empty secret must never mean "no check"."""
        raw = _body()
        with self.assertRaises(BillingRefused) as caught:
            verify_signature(raw, _sign(raw), "")
        self.assertEqual(caught.exception.status_code, 503)

    def test_junk_that_is_not_json_is_refused(self):
        with self.assertRaises(BillingRefused):
            parse_event(b"not json")


class EventShapeTests(unittest.TestCase):
    def test_the_delivery_id_comes_from_razorpays_header(self):
        self.assertEqual(event_id({}, {"x-razorpay-event-id": "evt_9"}), "evt_9")

    def test_an_event_without_a_header_still_deduplicates(self):
        event = json.loads(_body())
        self.assertEqual(event_id(event, {}), event_id(event, {}))
        self.assertNotEqual(event_id(event, {}), event_id({"other": 1}, {}))

    def test_the_subscription_id_is_read_from_the_payload(self):
        self.assertEqual(subscription_id_of(json.loads(_body())), "sub_123")

    def test_an_event_about_nothing_has_no_subscription(self):
        self.assertIsNone(subscription_id_of({"event": "payment.captured"}))

    def test_the_buyer_is_read_from_the_authoritative_subscription(self):
        """Not from the event — a forged event must not be able to name someone
        else's buyer_id and move their entitlement."""
        self.assertEqual(buyer_id_of(_subscription(buyer="buyer-7")), "buyer-7")
        self.assertIsNone(buyer_id_of(_subscription(buyer=None)))


class DecisionTests(unittest.TestCase):
    """What each Razorpay state means, including the ones that mean nothing."""

    def test_active_extends_to_current_end_plus_grace(self):
        status, expires, _ = decide(_subscription("active", current_end=NOW))
        self.assertEqual(status, "active")
        self.assertEqual(expires, int(NOW) + GRACE_DAYS * 86400)

    def test_halted_lapses(self):
        self.assertEqual(decide(_subscription("halted"))[0], "lapsed")

    def test_paused_lapses(self):
        self.assertEqual(decide(_subscription("paused"))[0], "lapsed")

    def test_pending_writes_nothing_because_grace_already_covers_it(self):
        self.assertIsNone(decide(_subscription("pending"))[0])

    def test_cancelled_writes_nothing(self):
        """They paid through current_end. Cutting them at the cancel click
        would be taking back service they bought; expiry ends it to the day."""
        status, expires, why = decide(_subscription("cancelled"))
        self.assertIsNone(status)
        self.assertIsNone(expires)
        self.assertIn("already paid through", why)

    def test_active_without_a_period_end_refuses_to_invent_one(self):
        """A guessed expiry is worse than no write — no write lets the
        previous, correct one stand."""
        self.assertIsNone(decide(_subscription("active", current_end=None))[0])

    def test_an_unrecognised_status_writes_nothing(self):
        self.assertIsNone(decide(_subscription("teleported"))[0])


class WriteLimitTests(unittest.TestCase):
    """What billing is allowed to do to a record."""

    def test_a_payment_extends_an_active_buyer(self):
        record = {"buyer_id": "b", "status": "active", "expires_at": 1}
        updated = apply_decision(record, "active", 999)
        self.assertEqual(updated["expires_at"], 999)
        self.assertIn("last_charge_at", updated)

    def test_a_revoked_buyer_is_never_reactivated_by_a_payment(self):
        """Money does not un-ban anyone. If Phil wants them back he flips it."""
        record = {"buyer_id": "b", "status": "revoked", "expires_at": 1}
        updated = apply_decision(record, "active", 999)
        self.assertEqual(updated["status"], "revoked")
        self.assertEqual(updated["expires_at"], 1)

    def test_a_revoked_buyer_is_not_touched_by_a_lapse_either(self):
        record = {"buyer_id": "b", "status": "revoked"}
        self.assertEqual(apply_decision(record, "lapsed", None)["status"], "revoked")

    def test_billing_may_not_write_revoked_itself(self):
        """The status stays Phil's, so no future caller can widen billing's remit."""
        with self.assertRaises(BillingRefused):
            apply_decision({"buyer_id": "b", "status": "active"}, "revoked", None)

    def test_writing_nothing_leaves_the_record_exactly_as_it_was(self):
        record = {"buyer_id": "b", "status": "active", "expires_at": 5}
        self.assertEqual(apply_decision(record, None, None), record)

    def test_a_lapse_does_not_move_the_expiry(self):
        """Only money moves expires_at. A lapse stops them now, via status."""
        record = {"buyer_id": "b", "status": "active", "expires_at": 5}
        updated = apply_decision(record, "lapsed", None)
        self.assertEqual(updated["status"], "lapsed")
        self.assertEqual(updated["expires_at"], 5)


class IdempotencyTests(unittest.TestCase):
    def setUp(self):
        self.clock = [NOW]
        self.events = ProcessedEvents(FakeStore(), now_fn=lambda: self.clock[0])

    def test_a_delivery_is_remembered(self):
        self.assertFalse(self.events.seen("evt_1"))
        self.events.remember("evt_1")
        self.assertTrue(self.events.seen("evt_1"))

    def test_old_deliveries_are_pruned_and_recent_ones_kept(self):
        self.events.remember("old")
        self.clock[0] += 8 * 86400
        self.events.remember("new")
        self.assertEqual(self.events.prune(), 1)
        self.assertFalse(self.events.seen("old"))
        self.assertTrue(self.events.seen("new"))


class FetchTests(unittest.TestCase):
    """The event is a doorbell; this is the truth."""

    def test_it_fetches_the_subscription_with_basic_auth(self):
        calls = []

        def http(url, auth):
            calls.append((url, auth))
            return 200, _subscription()

        client = RazorpayClient("rzp_id", "rzp_secret", http=http)
        self.assertEqual(client.fetch_subscription("sub_123")["id"], "sub_123")
        self.assertTrue(calls[0][0].endswith("/subscriptions/sub_123"))
        self.assertEqual(calls[0][1], ("rzp_id", "rzp_secret"))

    def test_an_unconfigured_client_refuses(self):
        with self.assertRaises(BillingRefused) as caught:
            RazorpayClient("", "", http=lambda *a: (200, {})).fetch_subscription("sub_123")
        self.assertEqual(caught.exception.status_code, 503)

    def test_a_razorpay_error_is_a_refusal_not_a_decision(self):
        client = RazorpayClient("id", "secret", http=lambda *a: (500, {"error": "boom"}))
        with self.assertRaises(BillingRefused):
            client.fetch_subscription("sub_123")


if __name__ == "__main__":
    unittest.main()
