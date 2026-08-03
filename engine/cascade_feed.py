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


def campaign_opened_payload(campaign: dict, *, advisory: Optional[dict] = None) -> dict:
    """
    The only message that may start a follower campaign.

    `advisory` is a convenience copy of public exchange filters. The executor
    must re-fetch these from its own exchangeInfo and prefer its own values —
    filters change, and an order rejected on a stale tick size is its problem
    to prevent, not ours to cause.
    """
    payload = {
        "campaign_id": campaign.get("campaign_id"),
        "symbol": campaign.get("symbol"),
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
    `supersedes` carries the standing-line rule. The executor asserts it rather
    than trusting it: a new line sitting below the one it replaces is a feed
    bug, and the right response is to halt that campaign, not to follow it.
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
