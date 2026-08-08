"""
engine/cascade_feed.py — the wire between our geometry and a buyer's executor.

Nothing in the live trading path imports this yet. It is the emitter half of
`CASCADE_SIGNAL_FORMAT.md`: it turns campaign geometry into signed, versioned,
append-only messages, and it refuses to let anything else out.

One rule governs the whole module:

    A field may be published only if it is derivable from public candle data.

Capital, balances, orders, fills and positions never cross the wire. That is
not a privacy nicety — it is what keeps us from holding anyone's credentials,
and it is also what makes the format correct, because a follower's account
state genuinely differs from ours and publishing ours would be publishing a lie.

The rule is enforced twice, deliberately:

1. **Every payload is built by explicit construction.** No builder in this file
   copies a dict from the engine. If you want a field on the wire you have to
   type its name here, which means the default for a new engine field is "not
   published" rather than "published because nobody noticed".
2. **`_assert_publishable` walks the finished payload** against a denylist of
   account-specific names, at any depth, and raises. That is the tripwire for
   the edit six months from now that reaches for `leg.to_dict()` because it is
   shorter. Belt, and separately, braces.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

FEED_VERSION = 1

# Bucket in the SQLite JSON store. Separate from `cascade_events`, which is our
# own operational log and carries dollar figures — this one is the buyer-facing
# stream and carries none.
FEED_BUCKET = "cascade_feed"

# Server-side retention, in days. Not the executor's cursor validity, which is
# 24h and lives in the executor: one is a storage policy, the other is a trust
# policy, and CASCADE_SIGNAL_FORMAT.md explains why they are different numbers.
# Retention is generous because it is nearly free — heartbeats are excluded, so
# a busy day is a few hundred rows.
RETENTION_DAYS = 7

# Message types that may be appended to the durable log. Heartbeats are absent
# on purpose: they are liveness, not history. Retaining 2,880 a day per symbol
# would be ~99% of the volume and none of the value, and a heartbeat missing
# from a replay means nothing.
LOGGED_TYPES = frozenset(
    {
        "campaign.opened",
        "campaign.state",
        "campaign.closed",
        "trendline.set",
        "leg.opened",
        "leg.finalized",
    }
)


class FeedLeak(Exception):
    """A payload tried to publish something account-specific. Never catch this."""


# Account-specific field names, taken one by one from Campaign, Leg,
# PendingOrder, Fill and Round. Matched at any depth. A name here can never
# reach a buyer, whatever a future builder does.
#
# Note what is NOT here: `tick_size` and `min_notional_usd` are public exchange
# filters, and `allocation_pct_gross` is the pre-netting figure the executor
# needs. `allocation_pct` — the netted one, which depends on which OTHER
# campaigns we happen to be running — is denied, and the two names differing is
# a small piece of luck worth preserving.
NEVER_PUBLISH = frozenset(
    {
        # capital
        "capital_usd",
        "capital_unit_per_pct",
        "pool_usd",
        "pool_total_usd",
        "carry_in_usd",
        "carry_forward_usd",
        # deployment and netting — depend on the follower's own siblings
        "allocation_pct",
        "netted_pct",
        "funded_bands",
        "funded_floor_price",
        "collected",
        "pending_usd",
        "cumulative_used_pct",
        "reuse_below",
        # orders
        "pending_orders",
        "pending_order_id",
        "pending_rev",
        "pending_filled_qty",
        "pending_limit_price",
        "pending_stop_price",
        "pending_last_red",
        "pending_stop_ts",
        "pending_line",
        "order_id",
        "client_order_id",
        "usd_notional",
        "quantity",
        "filled_qty",
        "fill_price",
        "fill_timestamp",
        "stop_price",
        "limit_price",
        "stop_ts",
        "moved_usd",
        "moved_to_level",
        "own_usd",
        "received",
        "entry_style",
        "working_price",
        # fills, rounds, position
        "all_fills",
        "rounds",
        "avg_entry",
        "avg_entry_price",
        "filled_base_qty",
        "residual_base_qty",
        "exchange_qty",
        "position_checked_at",
        "position_missing_notice",
        "realized_pnl",
        "invested_usd",
        "pnl",
        "pnl_gross",
        "fees_usd",
        "fees_estimated",
        # The venue's rate is public, but OUR rate is not THEIRS: a buyer's
        # actual commission depends on their VIP tier and whether they pay in
        # BNB. Publishing ours as if it were theirs would price their target
        # off our account, which is the exact lie the one rule exists to stop.
        # The executor reads its own rate from its own venue.
        "fee_pct_per_side",
        "exit_price",
        # take-profit — derived from their fills, not ours
        "tp_price",
        "tp_order_id",
        "tp_order_price",
        "tp_rev",
        "tp_filled",
        "tp_min_notional_notice",
        # ours alone
        "mode",
        "event_log",
    }
)


def _assert_publishable(payload: Any, _path: str = "payload") -> None:
    """Walk a finished payload and refuse anything account-specific."""
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key in NEVER_PUBLISH:
                raise FeedLeak(f"{_path}.{key} is account-specific and must never be published")
            _assert_publishable(value, f"{_path}.{key}")
    elif isinstance(payload, (list, tuple)):
        for index, item in enumerate(payload):
            _assert_publishable(item, f"{_path}[{index}]")


# ── signing ──────────────────────────────────────────────────────────


def _frame_bytes(envelope: dict) -> bytes:
    """
    The exact bytes that get signed AND the exact bytes that get transmitted.

    We sign the serialized string rather than "the canonical JSON of the
    object", and the executor verifies the string it received without ever
    re-serializing. The obvious design is the wrong one here: canonical-JSON
    signing needs both sides to agree on float formatting, and `178.42`,
    `178.420` and `1.7842e2` are the same number with different bytes in
    different languages. A disagreement there is a signature failure on a
    perfectly valid message, which in this system means an executor halting a
    live campaign for no reason. Signing the transmitted bytes deletes the
    entire class of problem.
    """
    return json.dumps(
        envelope,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


class FeedSigner:
    """
    Signs with the operational feed key. The root key that authorises this one
    lives off-server and is not this class's business — see the key-set design
    in CASCADE_SIGNAL_FORMAT.md.
    """

    def __init__(self, kid: str, private_key: Ed25519PrivateKey):
        if not kid:
            raise ValueError("a signing key needs a kid — revocation names it")
        self.kid = kid
        self._key = private_key

    @classmethod
    def generate(cls, kid: str) -> "FeedSigner":
        return cls(kid, Ed25519PrivateKey.generate())

    @classmethod
    def from_pem(cls, kid: str, pem: bytes, password: Optional[bytes] = None) -> "FeedSigner":
        key = serialization.load_pem_private_key(pem, password=password)
        if not isinstance(key, Ed25519PrivateKey):
            raise ValueError("feed keys are ed25519")
        return cls(kid, key)

    def public_key_b64(self) -> str:
        raw = self._key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
        return _b64(raw)

    def frame(self, envelope: dict) -> dict:
        """Wrap an envelope in its signed transport frame."""
        msg = _frame_bytes(envelope).decode("utf-8")
        sig = self._key.sign(msg.encode("utf-8"))
        return {"msg": msg, "sig": f"ed25519:{self.kid}:{_b64(sig)}"}


def verify_frame(frame: dict, public_keys: Dict[str, str]) -> dict:
    """
    Verify a frame against a kid → base64-public-key map and return the parsed
    envelope. This is the executor's half, kept here so both sides are read
    together and tested against each other.

    Raises InvalidSignature on anything that does not check out — including an
    unknown kid, which is how a revoked key stops working.
    """
    msg = frame.get("msg")
    sig = frame.get("sig") or ""
    if not isinstance(msg, str) or not isinstance(sig, str):
        raise InvalidSignature("frame is not a signed message")
    parts = sig.split(":", 2)
    if len(parts) != 3 or parts[0] != "ed25519":
        raise InvalidSignature("unrecognised signature format")
    _, kid, encoded = parts
    pub_b64 = public_keys.get(kid)
    if not pub_b64:
        raise InvalidSignature(f"unknown or revoked key id {kid!r}")
    public = Ed25519PublicKey.from_public_bytes(_unb64(pub_b64))
    public.verify(_unb64(encoded), msg.encode("utf-8"))
    return json.loads(msg)


# ── the key set ──────────────────────────────────────────────────────
#
# The root key signs this document; the document says which feed keys are
# valid. Both halves of the trust chain are written here so they are read
# together — the executor's verification and our minting must not drift apart.

KEYSET_VERSION = 1

# How long a signed key set stays valid. This is NOT the executor's cache TTL
# (24h, which is how often it must re-fetch). Two different jobs again.
#
# The document needs an expiry at all because of one specific attack: an
# attacker who can intercept the key-set endpoint serves LAST MONTH's key set,
# the one that still lists a compromised kid as valid. Without an expiry that
# replay works forever and revocation is decorative. With it, a revocation
# becomes permanent once the old document lapses.
#
# 30 days is the trade. Shorter makes revocation stick faster and turns the
# offline re-signing ceremony into a chore; longer widens the replay window.
# The cost of the choice is a liveness risk — a key set nobody re-signs
# eventually stops every buyer — so the server warns while there is still time
# to act. See KEYSET_WARN_DAYS.
KEYSET_VALID_DAYS = 30
KEYSET_WARN_DAYS = 7


def build_key_set(
    keys: List[dict],
    *,
    revoked: Optional[List[str]] = None,
    issued_at: Optional[int] = None,
    valid_days: int = KEYSET_VALID_DAYS,
) -> dict:
    """The inner document. Signed by the ROOT key, never by a feed key."""
    issued = int(issued_at if issued_at is not None else time.time())
    return {
        "v": KEYSET_VERSION,
        "issued_at": issued,
        "expires_at": issued + int(valid_days * 86400),
        "keys": [
            {
                "kid": key["kid"],
                "alg": "ed25519",
                "public": key["public"],
                "not_before": int(key.get("not_before", issued)),
                "not_after": int(key["not_after"]),
            }
            for key in keys
        ],
        "revoked": list(revoked or []),
    }


ROOT_KID = "root"


def sign_key_set(document: dict, root: FeedSigner) -> dict:
    """
    Wrap a key set in a root-signed frame — the same shape as a message frame,
    on purpose, so there is one verification path to get right rather than two.
    """
    if root.kid != ROOT_KID:
        raise ValueError(f"a key set is signed by the root key, not by {root.kid!r}")
    return root.frame(document)


def verify_key_set(frame: dict, root_public_b64: str, *, now: Optional[float] = None) -> dict:
    """
    The executor's side. Verify against the root public key compiled into the
    build, then refuse an expired document.

    Fail-closed on expiry is the point: an executor holding a lapsed key set
    must stop opening campaigns rather than keep trusting whatever it last saw.
    """
    verified = verify_frame(frame, {ROOT_KID: root_public_b64})
    stamp = time.time() if now is None else now
    if int(verified.get("expires_at") or 0) <= stamp:
        raise InvalidSignature("key set has expired — refetch before opening anything new")
    return verified


def active_public_keys(document: dict, *, now: Optional[float] = None) -> Dict[str, str]:
    """
    kid → public key, for keys that are in window and not revoked. Feed
    everything through this rather than reading `document["keys"]` directly —
    revocation only works if the revoked list is honoured at the one place the
    map is built.
    """
    stamp = time.time() if now is None else now
    revoked = set(document.get("revoked") or [])
    active = {}
    for key in document.get("keys") or []:
        kid = key.get("kid")
        if not kid or kid in revoked:
            continue
        if not (int(key.get("not_before") or 0) <= stamp < int(key.get("not_after") or 0)):
            continue
        active[kid] = key.get("public")
    return active


def key_set_expiry_warning(document: dict, *, now: Optional[float] = None) -> Optional[str]:
    """Something for the server to surface while there is still time to act."""
    stamp = time.time() if now is None else now
    remaining = int(document.get("expires_at") or 0) - stamp
    if remaining <= 0:
        return "The feed key set has EXPIRED. Every executor has stopped opening campaigns."
    if remaining <= KEYSET_WARN_DAYS * 86400:
        return f"The feed key set expires in {int(remaining // 86400)} day(s) — re-sign it with the root key."
    return None


def _b64(raw: bytes) -> str:
    import base64

    return base64.b64encode(raw).decode("ascii")


def _unb64(text: str) -> bytes:
    import base64

    return base64.b64decode(text.encode("ascii"))


# ── envelopes ────────────────────────────────────────────────────────


def build_envelope(
    *,
    msg_type: str,
    symbol: str,
    campaign_id: str,
    payload: dict,
    seq: int,
    model_version: int,
    emitted_at: Optional[int] = None,
) -> dict:
    _assert_publishable(payload)
    return {
        "v": FEED_VERSION,
        "model_version": int(model_version),
        "seq": int(seq),
        "emitted_at": int(emitted_at if emitted_at is not None else time.time()),
        "type": msg_type,
        "symbol": symbol,
        "campaign_id": campaign_id,
        "payload": payload,
    }


# ── payload builders ─────────────────────────────────────────────────
#
# Each of these reads an engine dict and writes a payload field by field. None
# of them copies. That is the whole design: adding a field to Campaign should
# never publish it by accident.


def campaign_opened_payload(campaign: dict, *, advisory: Optional[dict] = None, default_exchange: str = "") -> dict:
    """
    The only message that may start a follower campaign.

    `advisory` is a convenience copy of public exchange filters. The executor
    must re-fetch these from its own exchangeInfo and prefer its own values —
    filters change, and an order rejected on a stale tick size is its problem
    to prevent, not ours to cause.

    **`exchange` names whose candles this geometry was drawn from**, and it is
    published even though it sounds account-ish, because it is not: it names a
    public data source, which is exactly as public as the candles themselves.
    Binance SOLUSDT and CoinDCX SOLUSDT are not the same series. Without it
    `symbol` silently implies "yours", and an executor cross-checking our
    levels against its own candles would find small mismatches with nothing to
    explain them. A follower trading a different venue from the one the fib was
    drawn on should be told, not left to infer it.

    The engine stores "" for "the venue this engine was started with", so the
    caller passes the resolved default — a bare "" would tell a buyer nothing.
    """
    payload = {
        "campaign_id": campaign.get("campaign_id"),
        "symbol": campaign.get("symbol"),
        "exchange": str(campaign.get("exchange") or default_exchange or "").lower(),
        "created_at": epoch_from_ist(campaign.get("created_at")),
        "mother_high": campaign.get("mother_high"),
        "mother_low": campaign.get("mother_low"),
        "mother_timestamp": campaign.get("mother_timestamp"),
        "mc_kind": campaign.get("mc_kind"),
        "left_mother_range": bool(campaign.get("left_mother_range")),
        "timeframe": campaign.get("timeframe"),
        "start_timeframe": campaign.get("start_timeframe"),
        "escalates": bool(campaign.get("escalates")),
        "state": campaign.get("state"),
        "parent_campaign_id": campaign.get("parent_campaign_id"),
        "generation": campaign.get("generation"),
        "barren_chain": campaign.get("barren_chain"),
        "min_fib_range_pct": campaign.get("min_fib_range_pct"),
        "median_bar_pct": campaign.get("median_bar_pct"),
        "advisory": {
            "tick_size": (advisory or {}).get("tick_size", campaign.get("tick_size")),
            "min_notional_usd": (advisory or {}).get("min_notional_usd", campaign.get("min_notional_usd")),
        },
    }
    _assert_publishable(payload)
    return payload


def trendline_set_payload(trendline: dict, *, supersedes: Optional[int] = None) -> dict:
    """
    `supersedes` means the old line was still STANDING and this one replaces
    it, and the executor asserts the standing-line rule on the pair: a
    replacement may never sit below the line it replaces. This engine never
    replaces a standing line — a new id is only drawn after a close above
    spends the old one, and that successor legitimately sits lower — so it
    always publishes None. The field and the guard stay for any feed that
    does replace in place.
    """
    payload = {
        "trendline_id": trendline.get("trendline_id"),
        "anchor1_price": trendline.get("anchor1_price"),
        "anchor1_timestamp": trendline.get("anchor1_timestamp"),
        "anchor2_price": trendline.get("anchor2_price"),
        "anchor2_timestamp": trendline.get("anchor2_timestamp"),
        "bears_fib": bool(trendline.get("bears_fib", True)),
        "supersedes": supersedes,
    }
    _assert_publishable(payload)
    return payload


def leg_opened_payload(leg: dict, *, allocation_anchor: float) -> dict:
    """
    The load-bearing message: this is what earns money the right to deploy.

    `allocation_anchor` is the previous leg's low, or the mother high for the
    first leg. We publish the anchor and the GROSS percent — the executor nets
    locally against its own band ledger, because netting depends on which of
    our symbols it happens to be running and its siblings are not ours.

    `derived` is a checksum, not an instruction. The executor computes the same
    levels itself and compares; a mismatch means the two sides disagree about
    the model, which is a halt, not a trade.
    """
    fib = leg.get("fib") or {}
    high_anchor = fib.get("high_anchor")
    low_anchor = fib.get("low_anchor")
    payload = {
        "leg_id": leg.get("leg_id"),
        "trendline_id": leg.get("trendline_id"),
        "low": leg.get("low"),
        "touch_high": leg.get("touch_high"),
        "touch_timestamp": leg.get("touch_timestamp"),
        "created_via_break": bool(leg.get("created_via_break")),
        "escalated": bool(leg.get("escalated")),
        "fib": (
            None
            if high_anchor is None or low_anchor is None
            else {"high_anchor": high_anchor, "low_anchor": low_anchor}
        ),
        "leg_pct_from_mother": leg.get("leg_pct_from_mother"),
        "allocation_anchor": allocation_anchor,
        "allocation_pct_gross": gross_allocation_pct(allocation_anchor, leg.get("low")),
        "derived": _derived_levels(high_anchor, low_anchor),
    }
    _assert_publishable(payload)
    return payload


def leg_finalized_payload(leg_id: int) -> dict:
    return {"leg_id": int(leg_id)}


def campaign_state_payload(campaign: dict) -> dict:
    """
    The break candles go out as bare timestamps, not as the candle dicts the
    engine holds. The executor has the candles — it is reading the same public
    market — so all it needs is which bar we adjudicated on. Shipping our copy
    of the OHLC would invite it to trade our snapshot instead of its own feed,
    and the two can differ by a tick.
    """
    payload = {
        "state": campaign.get("state"),
        "mother_break_candle": _candle_ts(campaign.get("mother_break_candle")),
        "mother_break_top_candle": _candle_ts(campaign.get("mother_break_top_candle")),
        "mother_break_wait_remaining": campaign.get("mother_break_wait_remaining"),
        "timeframe": campaign.get("timeframe"),
    }
    _assert_publishable(payload)
    return payload


def _candle_ts(candle: Any) -> Optional[int]:
    if isinstance(candle, dict):
        stamp = candle.get("timestamp")
        return int(stamp) if stamp is not None else None
    if isinstance(candle, (int, float)):
        return int(candle)
    return None


def campaign_closed_payload(campaign: dict) -> dict:
    """
    The executor stops drawing new structure — but must not blindly flatten.
    What it holds and what we hold are different positions, and unwinding is
    its own decision against its own fills.
    """
    payload = {
        "state": campaign.get("state"),
        "reason": campaign.get("close_reason"),
        "closed_at": epoch_from_ist(campaign.get("closed_at")),
    }
    _assert_publishable(payload)
    return payload


_IST = timezone(timedelta(hours=5, minutes=30))


def epoch_from_ist(value: Any) -> Optional[int]:
    """
    Engine timestamps are `_ist_now_str()` — "2026-08-03 19:47:00", IST, with
    nothing in the string that says so. That is right for our own UI and wrong
    for a wire, and it matters more than it looks: `created_at` is what the
    join-at-start rule measures against `max_join_age_sec`, so a follower whose
    machine is not in IST would read every campaign as 5½ hours old and join
    nothing, silently, forever.

    Everything on this wire is epoch seconds. Rendering back to IST — or to
    whatever the buyer's clock says — is the executor's job.
    """
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return int(value)
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        naive = datetime.strptime(value.strip(), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    return int(naive.replace(tzinfo=_IST).timestamp())


def gross_allocation_pct(anchor: Optional[float], low: Optional[float]) -> Optional[float]:
    """`(anchor - low) / anchor * 100`, before any netting."""
    if anchor in (None, 0) or low is None:
        return None
    return (float(anchor) - float(low)) / float(anchor) * 100.0


def _derived_levels(high_anchor: Optional[float], low_anchor: Optional[float]) -> Optional[dict]:
    if high_anchor is None or low_anchor is None:
        return None
    span = float(high_anchor) - float(low_anchor)
    return {f"level_{level}": float(high_anchor) - level * span for level in (2, 4, 8)}


# ── the durable log ──────────────────────────────────────────────────


# ── who may listen ───────────────────────────────────────────────────

SUBSCRIBER_BUCKET = "cascade_feed_subscribers"

# How far a handshake's clock may be from ours. Wide enough for an ordinary
# unsynchronised laptop, narrow enough that a captured handshake is not
# replayable for long. Nonces close the rest of that window.
HANDSHAKE_MAX_SKEW_SEC = 120

# The skew that makes join-at-start silently useless. max_join_age_sec is 300,
# so a machine more than a few minutes fast reads every campaign as already
# too old and joins nothing — with no error, ever. Tell them at connect.
JOIN_RISK_SKEW_SEC = 60


class NotEntitled(Exception):
    """The connection is refused. The message is meant to be read by a human."""


class FeedSubscribers:
    """
    The register of who may receive the feed.

    We store a buyer's PUBLIC key and nothing else that matters. The private
    half is generated on their machine at install and never sent, so there is
    no secret here that can leak from our side — the same principle that keeps
    us from holding their exchange credentials.
    """

    def __init__(self, store, *, bucket: str = SUBSCRIBER_BUCKET, now_fn: Callable[[], float] = time.time):
        self._store = store
        self._bucket = bucket
        self._now = now_fn

    def add(self, buyer_id: str, public_key_b64: str, *, label: str = "", expires_at: Optional[int] = None) -> dict:
        record = {
            "buyer_id": buyer_id,
            "public_key": public_key_b64,
            "label": label,
            "status": "active",
            "created_at": int(self._now()),
            "expires_at": int(expires_at) if expires_at else None,
        }
        self._store.put(self._bucket, buyer_id, record)
        return record

    def rekey(
        self, buyer_id: str, public_key_b64: str, *, label: Optional[str] = None, expires_at: Optional[int] = None
    ) -> dict:
        """Swap the key a registered buyer verifies with, and change nothing else.

        `add()` writes a whole fresh record, which is right for a new buyer and
        wrong for an existing one: replacing a key that way reset the status to
        active and the expiry to None. So re-keying a REVOKED buyer silently
        un-banned them, and re-keying anyone handed them an entitlement with no
        end date. A buyer whose laptop died and who registered again is exactly
        the person that would happen to.

        Status is never touched here — reactivating is its own deliberate act.
        Label and expiry are carried forward unless new ones are given.
        """
        record = self.get(buyer_id)
        if not record:
            raise KeyError(buyer_id)
        record["public_key"] = public_key_b64
        if label is not None:
            record["label"] = label
        if expires_at is not None:
            record["expires_at"] = int(expires_at)
        self._store.put(self._bucket, buyer_id, record)
        return record

    def get(self, buyer_id: str) -> Optional[dict]:
        record = self._store.get(self._bucket, buyer_id, default=None)
        return record if isinstance(record, dict) else None

    def list(self) -> List[dict]:
        rows = [row for row in self._store.get_mapping(self._bucket).values() if isinstance(row, dict)]
        rows.sort(key=lambda row: row.get("created_at") or 0, reverse=True)
        return rows

    def remove(self, buyer_id: str) -> dict:
        """Forget a buyer entirely, so their id and key can be registered fresh.

        Distinct from revoking, which is the tool for cutting off someone who
        should stay on the record. This is for a row that should never have
        existed — a test registration, a typo'd id — and it is what makes
        re-registering the same machine from scratch possible: `add()` on an
        existing id re-keys it and keeps the old status and expiry.
        """
        record = self.get(buyer_id)
        if not record:
            raise KeyError(buyer_id)
        self._store.delete(self._bucket, buyer_id)
        return record

    def set_status(self, buyer_id: str, status: str) -> dict:
        record = self.get(buyer_id)
        if not record:
            raise KeyError(buyer_id)
        record["status"] = status
        self._store.put(self._bucket, buyer_id, record)
        return record

    def entitled(self, buyer_id: str, *, now: Optional[float] = None) -> tuple[bool, str]:
        """(ok, reason). The reason is shown to the buyer, so it has to be plain."""
        record = self.get(buyer_id)
        stamp = self._now() if now is None else now
        if not record:
            return False, "This machine is not registered."
        if record.get("status") != "active":
            return False, f"This subscription is {record.get('status')}."
        expires = record.get("expires_at")
        if expires and int(expires) <= stamp:
            return False, "This subscription has expired."
        return True, ""


def verify_subscriber_handshake(
    handshake: dict,
    subscribers: FeedSubscribers,
    *,
    now: Optional[float] = None,
    seen_nonces: Optional[Dict[str, float]] = None,
) -> dict:
    """
    Check that a connecting executor is who it says and is paid up.

    Returns `{"subscriber":..., "clock_skew_sec":..., "join_risk": bool}`.

    The skew is measured and returned rather than merely tolerated, because of
    a failure this design would otherwise have: `max_join_age_sec` is judged
    against the executor's own clock, so a machine a few minutes fast decides
    every campaign is already too old and joins nothing at all — silently,
    indefinitely, with a stream that looks perfectly healthy. A buyer would
    have no way to tell that from a quiet market.
    """
    stamp = time.time() if now is None else now
    buyer_id = str(handshake.get("buyer_id") or "")
    nonce = str(handshake.get("nonce") or "")
    if not buyer_id or not nonce:
        raise NotEntitled("Handshake is missing its buyer id or nonce.")

    try:
        timestamp = float(handshake.get("timestamp"))
    except (TypeError, ValueError):
        raise NotEntitled("Handshake timestamp is not a number.")
    skew = timestamp - stamp
    if abs(skew) > HANDSHAKE_MAX_SKEW_SEC:
        raise NotEntitled(
            f"This machine's clock is {abs(skew):.0f}s "
            f"{'ahead of' if skew > 0 else 'behind'} the server. Sync it and reconnect."
        )

    if seen_nonces is not None:
        for old, when in list(seen_nonces.items()):
            if stamp - when > HANDSHAKE_MAX_SKEW_SEC * 2:
                seen_nonces.pop(old, None)
        if nonce in seen_nonces:
            raise NotEntitled("This handshake has already been used.")

    record = subscribers.get(buyer_id)
    if not record:
        raise NotEntitled("This machine is not registered.")

    signed = {"buyer_id": buyer_id, "nonce": nonce, "timestamp": handshake.get("timestamp")}
    frame = {"msg": _frame_bytes(signed).decode("utf-8"), "sig": handshake.get("sig") or ""}
    try:
        verify_frame(frame, {buyer_id: record.get("public_key")})
    except Exception:
        raise NotEntitled("Handshake signature did not verify.")

    ok, reason = subscribers.entitled(buyer_id, now=stamp)
    if not ok:
        raise NotEntitled(reason)

    if seen_nonces is not None:
        seen_nonces[nonce] = stamp
    return {"subscriber": record, "clock_skew_sec": skew, "join_risk": abs(skew) > JOIN_RISK_SKEW_SEC}


def sign_handshake(buyer_id: str, signer: FeedSigner, *, nonce: str, timestamp: Optional[float] = None) -> dict:
    """
    The executor's half of the handshake, written here so the two sides are
    read and tested together rather than reimplemented apart.

    `signer` must carry the buyer's own key, whose kid IS the buyer id — the
    signature and the claimed identity cannot then disagree.
    """
    if signer.kid != buyer_id:
        raise ValueError("a handshake is signed by the buyer's own key")
    stamp = time.time() if timestamp is None else timestamp
    signed = {"buyer_id": buyer_id, "nonce": nonce, "timestamp": stamp}
    return {**signed, "sig": signer.frame(signed)["sig"]}


def build_snapshot(
    campaigns: List[dict],
    signer: FeedSigner,
    *,
    model_version: int,
    head_by_symbol: Dict[str, int],
    publish_modes: Iterable[str] = ("live",),
    default_exchange: str = "",
    symbol_gate: Optional[Callable[[str], bool]] = None,
    announced_ids: Optional[set] = None,
) -> List[dict]:
    """
    Current geometry for everything running, as signed frames — so a cold
    executor does not have to replay history it would ignore anyway.

    `symbol_gate` is the publish catalogue: campaigns of an unchecked symbol
    are left out UNLESS this process already announced them (`announced_ids`),
    in which case they keep being served to their natural end — a buyer may be
    holding a position laddered from one.

    These carry the symbol's current head as their `seq`. They are a rendering
    of the present, not entries in the log, so they consume no seq of their
    own; the executor sets its cursor from them and picks up the stream from
    exactly there.
    """
    modes = {str(mode).lower() for mode in publish_modes}
    gate = symbol_gate or (lambda _symbol: True)
    announced = announced_ids or set()
    frames = []

    def emit(msg_type, symbol, campaign_id, payload):
        frames.append(
            signer.frame(
                build_envelope(
                    msg_type=msg_type,
                    symbol=symbol,
                    campaign_id=campaign_id,
                    payload=payload,
                    seq=int(head_by_symbol.get(symbol, 0)),
                    model_version=model_version,
                )
            )
        )

    for campaign in campaigns or []:
        if str(campaign.get("mode") or "").lower() not in modes:
            continue
        if campaign.get("campaign_id") not in announced and not gate(str(campaign.get("symbol") or "")):
            continue
        symbol = campaign.get("symbol") or ""
        campaign_id = campaign.get("campaign_id") or ""
        emit(
            "campaign.opened",
            symbol,
            campaign_id,
            campaign_opened_payload(campaign, default_exchange=default_exchange),
        )
        for trendline in campaign.get("trendlines") or []:
            emit("trendline.set", symbol, campaign_id, trendline_set_payload(trendline))
        legs = campaign.get("legs") or []
        for index, leg in enumerate(legs):
            anchor = legs[index - 1]["low"] if index > 0 and legs[index - 1].get("low") else campaign.get("mother_high")
            emit("leg.opened", symbol, campaign_id, leg_opened_payload(leg, allocation_anchor=anchor))
            if leg.get("finalized"):
                emit("leg.finalized", symbol, campaign_id, leg_finalized_payload(leg.get("leg_id")))
        emit("campaign.state", symbol, campaign_id, campaign_state_payload(campaign))
    return frames


class CascadeFeedPublisher:
    """
    Turns engine status snapshots into feed messages by diffing what changed.

    This is deliberately NOT six emit calls sprinkled through the geometry
    code. The engine trades real money, and every line added inside it is a
    line that can raise in a tick loop. `_emit_update` already hands out the
    full status on every geometry change, from all sixteen places that matter,
    so a differ hooked to that callback sees everything the instrumented
    version would — and it cannot miss a transition somebody adds next month,
    because it never had to know the transitions in the first place.

    Nothing here raises. A feed that breaks must never stop the engine.

    **Messages are idempotent by identity.** A repeated `campaign.opened` for a
    campaign the executor already knows carries no new information and must be
    a no-op on its side. That is required anyway — a subscribe snapshot and the
    event stream overlap — and it is what lets us restart without persisting
    publisher state: a fresh process re-announces what is running, and every
    executor already knows to ignore it.
    """

    def __init__(
        self,
        log: "FeedLog",
        signer: FeedSigner,
        *,
        model_version: int,
        publish_modes: Iterable[str] = ("live",),
        default_exchange: str = "",
        on_error: Optional[Callable[[str, Exception], None]] = None,
        now_fn: Callable[[], float] = time.time,
        symbol_gate: Optional[Callable[[str], bool]] = None,
    ):
        self._log = log
        self._signer = signer
        self._model_version = int(model_version)
        self._default_exchange = default_exchange
        # Paper campaigns are real geometry but they are not money we stand
        # behind. A buyer following one is being sold a rehearsal.
        self._modes = {str(mode).lower() for mode in publish_modes}
        self._on_error = on_error
        self._now = now_fn
        # Which symbols may be ANNOUNCED. Checked only for campaigns not yet
        # seen: one already announced keeps publishing to its natural end,
        # because buyers may be holding positions laddered from it, and a feed
        # that goes silent under a held position is the failure mode every
        # executor treats as an emergency. The catalogue decision therefore
        # bites at the next campaign, never mid-flight.
        self._symbol_gate = symbol_gate or (lambda _symbol: True)
        self._seen: Dict[str, dict] = {}
        self._last_prune = 0.0

    def publish(self, status: dict) -> List[dict]:
        """Emit everything that changed since the last call. Returns the frames."""
        frames: List[dict] = []
        try:
            campaigns = [c for c in (status or {}).get("campaigns") or [] if self._eligible(c)]
            for campaign in campaigns:
                frames.extend(self._publish_campaign(campaign))
            self._publish_closures(status, {c.get("campaign_id") for c in campaigns})
            self._maybe_prune()
        except Exception as exc:  # never let the feed stop the engine
            self._fail("publish", exc)
        return frames

    def _eligible(self, campaign: dict) -> bool:
        if not campaign.get("campaign_id") or str(campaign.get("mode") or "").lower() not in self._modes:
            return False
        if campaign.get("campaign_id") in self._seen:
            return True  # announced already — it finishes what it started
        return bool(self._symbol_gate(str(campaign.get("symbol") or "")))

    def announced_campaign_ids(self) -> set:
        """Campaigns this process has already published. The snapshot uses this
        to keep serving an in-flight campaign whose symbol was unchecked after
        it was announced — same kindness, same reason."""
        return set(self._seen)

    def _publish_campaign(self, campaign: dict) -> List[dict]:
        campaign_id = campaign["campaign_id"]
        symbol = campaign.get("symbol") or ""
        seen = self._seen.get(campaign_id)
        frames = []

        if seen is None:
            seen = {"state": None, "trendlines": {}, "legs": {}, "closed": False}
            self._seen[campaign_id] = seen
            frames.append(
                self._emit(
                    "campaign.opened",
                    symbol,
                    campaign_id,
                    campaign_opened_payload(campaign, default_exchange=self._default_exchange),
                )
            )

        for trendline in campaign.get("trendlines") or []:
            tl_id = trendline.get("trendline_id")
            fingerprint = (
                trendline.get("anchor1_price"),
                trendline.get("anchor1_timestamp"),
                trendline.get("anchor2_price"),
                trendline.get("anchor2_timestamp"),
                bool(trendline.get("bears_fib", True)),
            )
            if seen["trendlines"].get(tl_id) == fingerprint:
                continue
            # Never chained to the previous id: in this engine a new line only
            # exists because a close above SPENT the old one, and the successor
            # fans lower from the same mother high as the fall deepens. Calling
            # that a replacement told the executor's standing-line guard to
            # halt every normal multi-line campaign.
            seen["trendlines"][tl_id] = fingerprint
            frames.append(self._emit("trendline.set", symbol, campaign_id, trendline_set_payload(trendline)))

        legs = campaign.get("legs") or []
        for index, leg in enumerate(legs):
            leg_id = leg.get("leg_id")
            known = seen["legs"].get(leg_id)
            if known is None:
                seen["legs"][leg_id] = {"finalized": False}
                frames.append(
                    self._emit(
                        "leg.opened",
                        symbol,
                        campaign_id,
                        leg_opened_payload(leg, allocation_anchor=self._allocation_anchor(campaign, legs, index)),
                    )
                )
                known = seen["legs"][leg_id]
            if leg.get("finalized") and not known["finalized"]:
                known["finalized"] = True
                frames.append(self._emit("leg.finalized", symbol, campaign_id, leg_finalized_payload(leg_id)))

        state = campaign.get("state")
        if state != seen["state"]:
            seen["state"] = state
            frames.append(self._emit("campaign.state", symbol, campaign_id, campaign_state_payload(campaign)))

        return [frame for frame in frames if frame]

    @staticmethod
    def _allocation_anchor(campaign: dict, legs: List[dict], index: int) -> Optional[float]:
        """
        Mirrors build_fib_ladder_and_pool: the previous leg's low, or the
        mother high for the first — and note the truthiness check on the prior
        low, which the engine has and which a plain `index > 0` would miss.
        """
        if index > 0 and legs[index - 1].get("low"):
            return legs[index - 1]["low"]
        return campaign.get("mother_high")

    def _publish_closures(self, status: dict, live_ids: set) -> None:
        for campaign in (status or {}).get("closed_campaigns") or []:
            campaign_id = campaign.get("campaign_id")
            seen = self._seen.get(campaign_id)
            # Only announce the close of something we announced the open of.
            # A campaign that ended before the feed was switched on is not news
            # to anyone downstream.
            if not seen or seen["closed"] or campaign_id in live_ids:
                continue
            seen["closed"] = True
            self._emit(
                "campaign.closed",
                campaign.get("symbol") or "",
                campaign_id,
                campaign_closed_payload(campaign),
            )

    def _emit(self, msg_type: str, symbol: str, campaign_id: str, payload: dict) -> Optional[dict]:
        try:
            return self._log.append(
                envelope_fields={
                    "msg_type": msg_type,
                    "symbol": symbol,
                    "campaign_id": campaign_id,
                    "payload": payload,
                    "model_version": self._model_version,
                },
                signer=self._signer,
            )
        except FeedLeak:
            # Not swallowed like the rest: this one means we nearly published
            # somebody's position. Let it out.
            raise
        except Exception as exc:
            self._fail(f"{msg_type} for {campaign_id}", exc)
            return None

    def _maybe_prune(self) -> None:
        now = self._now()
        if now - self._last_prune < 3600:
            return
        self._last_prune = now
        try:
            self._log.prune()
        except Exception as exc:
            self._fail("prune", exc)

    def _fail(self, what: str, exc: Exception) -> None:
        if self._on_error:
            try:
                self._on_error(what, exc)
            except Exception:
                pass


class FeedLog:
    """
    Append-only, per-symbol monotonic `seq`, backed by the same SQLite JSON
    store as the rest of the app's durable state.

    Each stored row keeps routing metadata (symbol, seq, type) alongside the
    signed frame. That metadata is a server-side index only and never goes over
    the wire — the executor must trust nothing outside `frame["msg"]`, which is
    the part the signature covers.
    """

    def __init__(
        self,
        store,
        *,
        bucket: str = FEED_BUCKET,
        retention_days: int = RETENTION_DAYS,
        now_fn: Callable[[], float] = time.time,
    ):
        self._store = store
        self._bucket = bucket
        self._retention_sec = int(retention_days * 86400)
        self._now = now_fn
        self._head_cache: Dict[str, int] = {}

    # doc_key layout: "<SYMBOL>|<seq padded to 12>" for events, "head|<SYMBOL>"
    # for the watermark. Symbols are uppercase alphanumeric, so they can never
    # collide with the literal "head".
    @staticmethod
    def _event_key(symbol: str, seq: int) -> str:
        return f"{symbol}|{seq:012d}"

    @staticmethod
    def _head_key(symbol: str) -> str:
        return f"head|{symbol}"

    def head(self, symbol: str) -> int:
        """
        The highest seq issued for a symbol.

        Taken as the max of the durable watermark and the rows actually
        present. The watermark alone can lag a restored or hand-edited store;
        the rows alone would be worse — once retention prunes a quiet symbol's
        last event, a fresh scan restarts at 1 and reissues seq numbers an
        executor has already seen. The watermark is never pruned, which is what
        makes it the safe half of the pair.

        Scanned once per symbol per process, then cached: appends are rare
        (a few hundred a day across every symbol) but the scan parses the whole
        bucket, and doing that on every append would be silly.
        """
        cached = self._head_cache.get(symbol)
        if cached is not None:
            return cached
        stored = self._store.get(self._bucket, self._head_key(symbol), default=0)
        watermark = int(stored) if isinstance(stored, (int, float)) else 0
        scanned = 0
        for key in self._keys():
            row_symbol, _, row_seq = key.partition("|")
            if row_symbol == symbol and row_seq.isdigit():
                scanned = max(scanned, int(row_seq))
        resolved = max(watermark, scanned)
        self._head_cache[symbol] = resolved
        return resolved

    def append(self, *, envelope_fields: dict, signer: FeedSigner) -> dict:
        """
        Assign the next seq, sign, and store. `envelope_fields` is everything
        `build_envelope` needs except `seq`.

        The watermark is written BEFORE the event, which looks backwards and
        isn't. A crash between the two writes then leaves a seq that was
        allocated and never used — a hole, which the executor already handles
        by re-snapshotting. Writing the event first would leave the watermark
        behind the rows, and the next append would reuse a seq an executor had
        already accepted under different contents. A hole is a re-snapshot; a
        reused seq is silent corruption.
        """
        symbol = envelope_fields["symbol"]
        msg_type = envelope_fields["msg_type"]
        if msg_type not in LOGGED_TYPES:
            raise ValueError(f"{msg_type} is not a logged message type")
        seq = self.head(symbol) + 1
        self._store.put(self._bucket, self._head_key(symbol), seq)
        self._head_cache[symbol] = seq
        envelope = build_envelope(seq=seq, **envelope_fields)
        frame = signer.frame(envelope)
        self._store.put(
            self._bucket,
            self._event_key(symbol, seq),
            {
                "symbol": symbol,
                "seq": seq,
                "type": msg_type,
                "emitted_at": envelope["emitted_at"],
                "frame": frame,
            },
        )
        return frame

    def since(self, symbol: str, cursor: int) -> List[dict]:
        """Frames after `cursor`, in seq order. The executor's replay call."""
        rows = []
        for key in sorted(self._keys()):
            row_symbol, _, row_seq = key.partition("|")
            if row_symbol != symbol or not row_seq.isdigit() or int(row_seq) <= cursor:
                continue
            row = self._store.get(self._bucket, key, default=None)
            if isinstance(row, dict) and row.get("frame"):
                rows.append(row["frame"])
        return rows

    def heartbeat(self, *, symbol: str, signer: FeedSigner, running_campaigns: int, model_version: int) -> dict:
        """
        Liveness, and quietly also a gap detector.

        The heartbeat carries the current head rather than a seq of its own, so
        an executor whose cursor has fallen behind on a SILENT symbol finds out
        within 30 seconds instead of whenever the next real event happens to
        arrive — which on a quiet symbol could be hours. Heartbeats are never
        stored, so they consume no seq and leave no hole in a replay.
        """
        envelope = build_envelope(
            msg_type="heartbeat",
            symbol=symbol,
            campaign_id="",
            payload={"running_campaigns": int(running_campaigns)},
            seq=self.head(symbol),
            model_version=model_version,
        )
        return signer.frame(envelope)

    def symbols(self) -> List[str]:
        """Every symbol the log holds events for.

        Not the same as "symbols with a live campaign", and the difference is a
        bug: the last thing a campaign ever emits is `campaign.closed`, by
        which point the engine has already dropped it. Streaming only live
        symbols means that message — the one telling an executor to stop
        drawing new structure — is written and never delivered.
        """
        found = set()
        for key in self._keys():
            symbol, _, tail = key.partition("|")
            if tail.isdigit() and symbol:
                found.add(symbol)
        return sorted(found)

    def prune(self) -> int:
        """Drop events past retention. Watermarks are never pruned."""
        cutoff = self._now() - self._retention_sec
        dropped = 0
        for key in self._keys():
            if key.startswith("head|"):
                continue
            row = self._store.get(self._bucket, key, default=None)
            if not isinstance(row, dict):
                continue
            if float(row.get("emitted_at") or 0) < cutoff:
                self._store.delete(self._bucket, key)
                dropped += 1
        return dropped

    def _keys(self) -> Iterable[str]:
        return list(self._store.get_mapping(self._bucket).keys())
