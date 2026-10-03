"""engine/buyer_reports.py — what a buyer's executor tells the desk about itself.

This is the OTHER direction from `engine/cascade_feed.py`. That module carries
our geometry out to buyers and forbids account data on the wire, because the
numbers would be ours and a follower's account genuinely differs. This module
carries a buyer's own account state IN, so Phil can see who is stuck, who is
holding what, and who never filled — the operator terminal, decided 03-Oct-2026.

Four things make that safe to build:

1. **The buyer signs it.** A report is verified against the public key already
   on the subscriber record, with a nonce and a clock-skew bound, exactly like
   the connect handshake. An unsigned or replayed report is not stored.
2. **We shape what we keep.** `shape_report` rebuilds the record field by
   field, with caps. A buyer's executor cannot write arbitrary keys, unbounded
   lists or megabytes of text into the desk's database — by accident or not.
3. **Only the latest report is kept** per buyer, so this never becomes a
   growing archive of someone's trading history.
4. **It is not a control channel.** Nothing here can make an executor do
   anything. The feed is still the only thing that flows outward, and it still
   carries no account data.

A lapsed or revoked buyer may still report. That is deliberate: their executor
keeps managing the positions it already holds, and the moment Phil most wants
to see a buyer's screen is the moment their subscription has just stopped.
"""

from __future__ import annotations

import time
from typing import Callable, Dict, List, Optional

from engine.cascade_feed import _frame_bytes, verify_frame

REPORT_BUCKET = "feed_buyer_reports"

# The executor build buyers should be on. Nothing on a buyer's machine checks
# for updates by itself, so this is how one finds out it is behind: the desk's
# answer to its own minute-ly report carries this, and the executor shows the
# buyer a line. It must equal executor/reporting.py's APP_VERSION — the two
# live apart because the executor imports nothing from engine/, and a test
# holds them together.
CURRENT_EXECUTOR_VERSION = "1.0"


def is_outdated(version: str) -> bool:
    """Compared piecewise as numbers, so 1.10 is newer than 1.9 — the trap in
    comparing version strings, and the one that would quietly tell every
    up-to-date buyer they are behind."""

    def parts(text: str):
        out = []
        for chunk in str(text or "").split("."):
            digits = "".join(c for c in chunk if c.isdigit())
            out.append(int(digits) if digits else 0)
        return out

    if not str(version or "").strip():
        return False  # never reported is its own state, not an old build
    mine, current = parts(version), parts(CURRENT_EXECUTOR_VERSION)
    mine += [0] * (len(current) - len(mine))
    current += [0] * (len(mine) - len(current))
    return mine < current


# A report older than this is stale: shown as "last seen", never as truth.
REPORT_FRESH_SEC = 180

# The executor reports on this cadence. Slow enough to be free, fast enough
# that a terminal refreshed by hand is never more than a minute behind.
REPORT_EVERY_SEC = 60

# A report arriving with a clock this far out is refused, for the same reason
# the handshake refuses one: a wrong clock makes every other timestamp a lie.
MAX_SKEW_SEC = 300

MAX_POSITIONS = 40
MAX_FILLS = 25
MAX_TEXT = 200


class ReportRefused(Exception):
    """A report that will not be stored, with a reason fit to read in a log."""


def _text(value, limit: int = MAX_TEXT) -> str:
    return str(value if value is not None else "")[:limit]


def _number(value) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number in (float("inf"), float("-inf")):  # NaN / inf
        return None
    return number


def shape_report(raw: dict) -> dict:
    """Rebuild a report from a buyer's payload — only known fields, all capped.

    Written as explicit construction rather than a filtered copy, for the same
    reason the feed builders are: the default for a field nobody has thought
    about must be "not stored".
    """
    raw = raw if isinstance(raw, dict) else {}

    def rows(key: str, limit: int, shaper: Callable[[dict], dict]) -> List[dict]:
        value = raw.get(key)
        if not isinstance(value, list):
            return []
        return [shaper(item) for item in value[:limit] if isinstance(item, dict)]

    return {
        # ── who and what ──
        "app_version": _text(raw.get("app_version"), 40),
        "exchange": _text(raw.get("exchange"), 40),
        "mode": _text(raw.get("mode"), 20),
        # ── is it well ──
        "running": bool(raw.get("running")),
        "uptime_sec": max(0, int(_number(raw.get("uptime_sec")) or 0)),
        "clock_skew_sec": _number(raw.get("clock_skew_sec")),
        "exchange_ok": bool(raw.get("exchange_ok")),
        "feed_state": _text(raw.get("feed_state"), 40),
        "last_error": _text(raw.get("last_error")),
        # ── what it is doing ──
        "campaigns_following": max(0, int(_number(raw.get("campaigns_following")) or 0)),
        "symbols": [_text(s, 20) for s in (raw.get("symbols") or [])[:20] if s],
        # ── the money (Phil, 03-Oct-2026: health AND money) ──
        "balance_usd": _number(raw.get("balance_usd")),
        "committed_usd": _number(raw.get("committed_usd")),
        "unrealized_usd": _number(raw.get("unrealized_usd")),
        "realized_usd": _number(raw.get("realized_usd")),
        "positions": rows(
            "positions",
            MAX_POSITIONS,
            lambda p: {
                "symbol": _text(p.get("symbol"), 20),
                "qty": _number(p.get("qty")),
                "avg_entry": _number(p.get("avg_entry")),
                "mark": _number(p.get("mark")),
                "unrealized_usd": _number(p.get("unrealized_usd")),
                "campaign_id": _text(p.get("campaign_id"), 40),
            },
        ),
        "recent_fills": rows(
            "recent_fills",
            MAX_FILLS,
            lambda f: {
                "symbol": _text(f.get("symbol"), 20),
                "side": _text(f.get("side"), 8),
                "qty": _number(f.get("qty")),
                "price": _number(f.get("price")),
                "at": _number(f.get("at")),
            },
        ),
    }


def sign_report(buyer_id: str, signer, report: dict, *, nonce: str, timestamp: Optional[float] = None) -> dict:
    """The executor's half, kept beside the verifier so both are read together."""
    if signer.kid != buyer_id:
        raise ValueError("a report is signed by the buyer's own key")
    stamp = time.time() if timestamp is None else timestamp
    signed = {"buyer_id": buyer_id, "nonce": nonce, "timestamp": stamp, "report": report}
    return {**signed, "sig": signer.frame(signed)["sig"]}


def verify_report(
    payload: dict,
    subscribers,
    *,
    now: Optional[float] = None,
    seen_nonces: Optional[Dict[str, float]] = None,
) -> dict:
    """Check a report really came from the buyer it claims, recently, once.

    Returns `{"buyer_id", "report", "clock_skew_sec"}`. Entitlement is NOT
    required — see the module docstring.
    """
    stamp = time.time() if now is None else now
    payload = payload if isinstance(payload, dict) else {}
    buyer_id = str(payload.get("buyer_id") or "")
    nonce = str(payload.get("nonce") or "")
    if not buyer_id or not nonce:
        raise ReportRefused("report is missing its buyer id or nonce")

    timestamp = _number(payload.get("timestamp"))
    if timestamp is None:
        raise ReportRefused("report timestamp is not a number")
    skew = timestamp - stamp
    if abs(skew) > MAX_SKEW_SEC:
        raise ReportRefused(f"report clock is {abs(skew):.0f}s out")

    if seen_nonces is not None:
        for old, when in list(seen_nonces.items()):
            if stamp - when > MAX_SKEW_SEC * 2:
                seen_nonces.pop(old, None)
        if nonce in seen_nonces:
            raise ReportRefused("this report has already been sent")

    record = subscribers.get(buyer_id)
    if not record:
        raise ReportRefused("this machine is not registered")

    report = payload.get("report")
    if not isinstance(report, dict):
        raise ReportRefused("report body is missing")

    # Signed over exactly what was sent, including the report body — otherwise
    # a valid signature could be lifted onto a different set of numbers.
    signed = {
        "buyer_id": buyer_id,
        "nonce": nonce,
        "timestamp": payload.get("timestamp"),
        "report": report,
    }
    frame = {"msg": _frame_bytes(signed).decode("utf-8"), "sig": payload.get("sig") or ""}
    try:
        verify_frame(frame, {buyer_id: record.get("public_key")})
    except Exception:
        raise ReportRefused("report signature did not verify")

    if seen_nonces is not None:
        seen_nonces[nonce] = stamp
    return {"buyer_id": buyer_id, "report": shape_report(report), "clock_skew_sec": skew}


class BuyerReports:
    """The latest report from each buyer. One row per buyer, replaced in place."""

    def __init__(self, store, *, bucket: str = REPORT_BUCKET, now_fn: Callable[[], float] = time.time):
        self._store = store
        self._bucket = bucket
        self._now = now_fn

    def put(self, buyer_id: str, report: dict, *, clock_skew_sec: Optional[float] = None) -> dict:
        record = {
            "buyer_id": buyer_id,
            "received_at": int(self._now()),
            "clock_skew_sec": clock_skew_sec,
            "report": report,
        }
        self._store.put(self._bucket, buyer_id, record)
        return record

    def get(self, buyer_id: str) -> Optional[dict]:
        record = self._store.get(self._bucket, buyer_id, default=None)
        return record if isinstance(record, dict) else None

    def mapping(self) -> Dict[str, dict]:
        rows = self._store.get_mapping(self._bucket)
        return {key: row for key, row in rows.items() if isinstance(row, dict)}

    def remove(self, buyer_id: str) -> None:
        self._store.delete(self._bucket, buyer_id)

    def fresh(self, buyer_id: str, *, now: Optional[float] = None) -> bool:
        record = self.get(buyer_id)
        if not record:
            return False
        stamp = self._now() if now is None else now
        return (stamp - int(record.get("received_at") or 0)) <= REPORT_FRESH_SEC
