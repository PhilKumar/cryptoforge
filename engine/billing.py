"""
engine/billing.py — money becomes an entitlement, silence takes it away.

The runnable half of `CASCADE_BILLING.md`. Deliberately network-free: it
verifies signatures, decides what a subscription state means, and computes the
record to write. The HTTP route fetches and persists; every rule below is
reachable from a dict.

**Billing's only job is keeping the subscriber record truthful.** The record,
the 30-second entitlement check and the lapse-gates-new-only rule already
exist. This adds no second entitlement system — it writes the same
`cascade_feed_subscribers` record `entitled()` reads.

Two constraints are load-bearing and enforced in code, not just prose:

1. **Billing may only ever write `active` or `lapsed`.** `revoked` is Phil's
   manual status for bans and compromised keys, so a webhook — forged, replayed
   or merely confused — can never undo one.
2. **Webhooks extend life; silence ends it.** `expires_at` moves only when
   money arrived. Our outage stops a paying buyer at period end (bounded and
   audible); it can never keep a non-paying one alive, because nothing is
   written. The dangerous failure is structurally impossible.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
from typing import Callable, List, Optional, Tuple

_log = logging.getLogger("cryptoforge.billing")

BILLING_EVENT_BUCKET = "billing_events"

# Absorbed into expires_at rather than modelled separately, so the executor
# needs no new concept — it already honours expiry. Covers UPI Autopay's
# real-world flakiness: mandates fail transiently and Razorpay retries on its
# own schedule.
GRACE_DAYS = 3

# How long processed event ids are kept for idempotency. Razorpay's redelivery
# window is hours; a week is generous and costs kilobytes.
EVENT_RETENTION_DAYS = 7

# The only statuses billing may write. See the module docstring.
BILLING_WRITABLE = frozenset({"active", "lapsed"})


class BillingRefused(Exception):
    """The request is not something we will act on. Carries an HTTP status."""

    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.status_code = status_code


def verify_signature(raw_body: bytes, signature: str, secret: str) -> None:
    """
    HMAC-SHA256 over the EXACT bytes received.

    Never over a re-serialization — the same discipline as the feed frames and
    the CoinDCX client, for the same reason: any encoding difference between
    what was signed and what is checked is an auth failure with no useful
    message, and here it would be an auth SUCCESS for an attacker if we
    normalised their payload into ours.
    """
    if not secret:
        raise BillingRefused("Billing is not configured on this server.", status_code=503)
    expected = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, str(signature or "")):
        raise BillingRefused("Signature did not verify.", status_code=401)


def parse_event(raw_body: bytes) -> dict:
    try:
        event = json.loads(raw_body or b"{}")
    except (ValueError, TypeError) as exc:
        raise BillingRefused(f"Body is not JSON: {exc}") from exc
    if not isinstance(event, dict):
        raise BillingRefused("Body is not a JSON object.")
    return event


def event_id(event: dict, headers: Optional[dict] = None) -> str:
    """
    Razorpay's own delivery id, from the header it sends it in.

    Falls back to a hash of the payload: an event with no id at all must still
    be deduplicated, and two identical bodies are the same delivery for our
    purposes — we recompute state from source either way, so a false match
    costs a no-op and a false miss costs a redundant fetch.
    """
    for key in ("x-razorpay-event-id", "X-Razorpay-Event-Id"):
        found = (headers or {}).get(key)
        if found:
            return str(found)
    payload = json.dumps(event, sort_keys=True, separators=(",", ":"), default=str)
    return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def subscription_id_of(event: dict) -> Optional[str]:
    """The subscription this event is about, or None if it is about nothing."""
    entity = ((event.get("payload") or {}).get("subscription") or {}).get("entity") or {}
    return entity.get("id") or None


# ── what a subscription state means ──────────────────────────────────


def decide(subscription: dict, *, now: Optional[float] = None) -> Tuple[Optional[str], Optional[int], str]:
    """
    (status_to_write, expires_at, why) for an AUTHORITATIVE subscription.

    `status_to_write` of None means write nothing — which is a real answer, not
    a failure. `cancelled` is the case worth stating: the buyer has paid
    through `current_end`, so cutting them at the cancel click would be taking
    back service they bought. Expiry ends it to the day instead.
    """
    state = str(subscription.get("status") or "").strip().lower()
    current_end = subscription.get("current_end")
    stamp = time.time() if now is None else now

    if state == "active":
        if not current_end:
            # Active with no period end is not something we can date. Refuse to
            # invent one: an entitlement with a guessed expiry is worse than a
            # missing write, which merely lets the previous one stand.
            return None, None, "active but no current_end — nothing to extend from"
        expires = int(current_end) + GRACE_DAYS * 86400
        return "active", expires, f"paid through {int(current_end)}, +{GRACE_DAYS}d grace"

    if state in ("halted", "paused"):
        return "lapsed", None, f"subscription is {state}"

    if state in ("pending", "created", "authenticated"):
        return None, None, f"{state} — Razorpay is mid-retry; grace already covers it"

    if state in ("cancelled", "canceled", "completed", "expired"):
        return None, None, f"{state} — already paid through current_end; expiry ends it on time"

    _log.warning("[BILLING] unrecognised subscription status %r — writing nothing", state)
    return None, None, f"unrecognised status {state!r}"


def apply_decision(record: dict, status: Optional[str], expires_at: Optional[int]) -> dict:
    """
    Fold a decision into a subscriber record, refusing to exceed billing's remit.

    Two refusals live here rather than at the caller, because this is the one
    place every billing write passes through:

    - `revoked` is never overwritten. A ban survives any webhook, including a
      genuine payment: money does not un-ban anyone, and if Phil wants them
      back he flips it himself.
    - Only `active`/`lapsed` may be written at all, so a future caller cannot
      hand this a status that quietly widens what billing can do.
    """
    updated = dict(record)
    if status is None:
        return updated
    if status not in BILLING_WRITABLE:
        raise BillingRefused(f"Billing may not write status {status!r}.")
    if str(record.get("status")) == "revoked":
        _log.warning("[BILLING] %s is revoked — leaving it alone", record.get("buyer_id"))
        return updated
    updated["status"] = status
    if expires_at is not None:
        updated["expires_at"] = int(expires_at)
        updated["last_charge_at"] = int(time.time())
    return updated


# ── idempotency ──────────────────────────────────────────────────────


class ProcessedEvents:
    """
    Which deliveries we have already acted on.

    A repeat is a 200 no-op, never a 4xx — an error code just makes Razorpay
    redeliver harder, and the whole point is that a duplicate is harmless.
    """

    def __init__(
        self,
        store,
        *,
        bucket: str = BILLING_EVENT_BUCKET,
        now_fn: Callable[[], float] = time.time,
        retention_days: int = EVENT_RETENTION_DAYS,
    ):
        self._store = store
        self._bucket = bucket
        self._now = now_fn
        self._retention = retention_days * 86400

    def seen(self, event_key: str) -> bool:
        return self._store.get(self._bucket, event_key, default=None) is not None

    def remember(self, event_key: str, detail: Optional[dict] = None) -> None:
        self._store.put(
            self._bucket,
            event_key,
            {"at": int(self._now()), **(detail or {})},
        )

    def prune(self) -> int:
        cutoff = self._now() - self._retention
        dropped = 0
        for key, row in self._store.get_mapping(self._bucket).items():
            if isinstance(row, dict) and float(row.get("at") or 0) < cutoff:
                self._store.delete(self._bucket, key)
                dropped += 1
        return dropped


# ── the fetch that decides ───────────────────────────────────────────


class RazorpayClient:
    """
    One call: fetch a subscription. The event is a doorbell; this is the truth.

    `http` is injected so the whole billing path is testable without a network
    and without a Razorpay account — the same reason the exchange adapters take
    one.
    """

    BASE_URL = "https://api.razorpay.com/v1"

    def __init__(self, key_id: str, key_secret: str, *, http: Optional[Callable] = None, base_url: str = BASE_URL):
        self._key_id = key_id
        self._key_secret = key_secret
        self._http = http or _requests_get
        self._base = base_url.rstrip("/")

    def fetch_subscription(self, subscription_id: str) -> dict:
        if not (self._key_id and self._key_secret):
            raise BillingRefused("Razorpay API credentials are not configured.", status_code=503)
        status, body = self._http(f"{self._base}/subscriptions/{subscription_id}", (self._key_id, self._key_secret))
        if status >= 400:
            raise BillingRefused(f"Razorpay refused the fetch ({status}).", status_code=502)
        if not isinstance(body, dict):
            raise BillingRefused("Razorpay returned something that is not a subscription.", status_code=502)
        return body


def _requests_get(url: str, auth: Tuple[str, str]):
    import requests

    response = requests.get(url, auth=auth, timeout=15)
    try:
        return response.status_code, response.json()
    except ValueError:
        return response.status_code, {"error": response.text[:200]}


def buyer_id_of(subscription: dict) -> Optional[str]:
    """
    The buyer this subscription belongs to, from `notes.buyer_id`.

    Read from the AUTHORITATIVE subscription rather than the event, so a forged
    event cannot name someone else's buyer_id and move their entitlement.
    """
    notes = subscription.get("notes") or {}
    found = notes.get("buyer_id") or notes.get("buyerId")
    return str(found).strip() if found else None


def summarize(subscription: dict, decision: Tuple[Optional[str], Optional[int], str]) -> dict:
    """A line for the log and the admin list. No card data — there is none."""
    status, expires_at, why = decision
    return {
        "subscription_id": subscription.get("id"),
        "razorpay_status": subscription.get("status"),
        "wrote": status or "(nothing)",
        "expires_at": expires_at,
        "why": why,
    }


def known_buyer_ids(subscribers) -> List[str]:
    return [row.get("buyer_id") for row in subscribers.list()]
