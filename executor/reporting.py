"""executor/reporting.py — this machine's status, sent back to the desk.

Phil runs an operator terminal now (03-Oct-2026) and it needs to know more
than "the socket is open": whether a buyer's executor is running, whether the
exchange answers it, what it is holding and what that is worth.

Three promises this keeps, because this is the only thing on a buyer's machine
that ever sends anything about their account anywhere:

  **It is built explicitly.** Every field below is typed out. Nothing copies a
  runtime object wholesale, so a new internal field is never published by
  accident — the same rule the feed builders follow in the other direction.

  **It carries no credentials, ever.** Not the API key, not the secret, not the
  key file's path. A report tells the desk what the account IS DOING, never how
  to touch it.

  **It cannot stop trading.** Every failure path here is swallowed and logged.
  The desk refusing, rejecting or being unreachable must never delay a fill;
  sending is the lowest-priority thing this program does.
"""

from __future__ import annotations

import base64
import json
import logging
import time
from typing import Optional

_log = logging.getLogger("executor.reporting")

# Matches engine/buyer_reports.REPORT_EVERY_SEC. A minute is slow enough to
# cost nothing and fast enough that Phil's screen is never a minute behind.
REPORT_EVERY_SEC = 60

# Bumped when what this file sends changes shape, so the terminal can tell a
# buyer running an old build from one that is simply quiet.
APP_VERSION = "1.0"

MAX_POSITIONS = 40
MAX_FILLS = 25


def _older(mine: str, theirs: str) -> bool:
    """Piecewise and numeric, so 1.10 is newer than 1.9 rather than older.
    Mirrors engine/buyer_reports.is_outdated; a test holds the two together."""

    def parts(text: str):
        out = []
        for chunk in str(text or "").split("."):
            digits = "".join(c for c in chunk if c.isdigit())
            out.append(int(digits) if digits else 0)
        return out

    a, b = parts(mine), parts(theirs)
    a += [0] * (len(b) - len(a))
    b += [0] * (len(a) - len(b))
    return a < b


def _round(value, places: int = 6) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number in (float("inf"), float("-inf")):
        return None
    return round(number, places)


def build_report(
    *,
    runtime,
    adapter,
    config,
    status: dict,
    portfolio: dict,
    feed_state: str = "",
    started_at: Optional[float] = None,
    last_error: str = "",
    now: Optional[float] = None,
) -> dict:
    """What this machine would say about itself, right now."""
    stamp = time.time() if now is None else now
    holdings = list(portfolio.get("holdings") or [])[:MAX_POSITIONS]
    positions = [
        {
            "symbol": h.get("symbol") or "",
            "qty": _round(h.get("quantity")),
            "avg_entry": _round(h.get("avg_entry")),
            "mark": _round(h.get("last_price")),
            "unrealized_usd": _round(h.get("unrealised_usd"), 2),
            "campaign_id": h.get("campaign_id") or "",
        }
        for h in holdings
    ]
    # Closed rounds, not raw fills: a round is the unit a buyer and Phil both
    # think in — bought a ladder, sold it, kept this much.
    fills = [
        {
            "symbol": row.get("symbol") or "",
            "side": "SELL",
            "qty": _round(row.get("quantity")),
            "price": _round(row.get("exit_price")),
            "at": _round(row.get("closed_ts"), 0),
        }
        for row in (runtime.rounds_view(limit=MAX_FILLS) or [])[:MAX_FILLS]
    ]

    exchange_ok = True
    try:
        adapter.free_balance(config.quote_asset)
    except Exception as exc:  # the venue, not us — report it rather than hide it
        exchange_ok = False
        last_error = last_error or f"exchange unreachable: {exc}"[:200]

    return {
        "app_version": APP_VERSION,
        "exchange": str(getattr(config, "exchange", "") or ""),
        "mode": "live",
        "running": True,
        "uptime_sec": int(stamp - started_at) if started_at else 0,
        "exchange_ok": exchange_ok,
        "feed_state": feed_state or "",
        "last_error": str(last_error or "")[:200],
        "campaigns_following": int(status.get("following") or 0),
        "symbols": sorted({h["symbol"] for h in positions if h["symbol"]})[:20],
        "balance_usd": _round(portfolio.get("free_quote"), 2),
        "committed_usd": _round(portfolio.get("invested_usd"), 2),
        "unrealized_usd": _round(portfolio.get("unrealised_usd"), 2),
        "realized_usd": _round(portfolio.get("realised_usd"), 2),
        "positions": positions,
        "recent_fills": fills,
    }


def sign_payload(identity, report: dict, *, nonce: str, now: Optional[float] = None) -> dict:
    """Sign over the body itself, so the numbers and the signature cannot part."""
    signed = {
        "buyer_id": identity.buyer_id,
        "nonce": nonce,
        "timestamp": time.time() if now is None else now,
        "report": report,
    }
    msg = json.dumps(signed, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    sig = base64.b64encode(identity.signing_key.sign(msg.encode("utf-8"))).decode("ascii")
    return {**signed, "sig": f"ed25519:{identity.buyer_id}:{sig}"}


class ReportSender:
    """Sends at most one report a minute, and never raises at the caller."""

    def __init__(self, *, base_url: str, identity, post=None, every_sec: int = REPORT_EVERY_SEC):
        self._url = base_url.rstrip("/") + "/api/cascade/feed/report"
        self._identity = identity
        self._every = max(5, int(every_sec))
        self._post = post or self._httpx_post
        self._last_sent = 0.0
        self.last_result = ""
        # What the desk says the current build is. Learned from the answer to
        # this machine's own report — nothing here polls for updates, and no
        # version check can ever be a reason trading stops.
        self.current_version = ""

    @staticmethod
    def _httpx_post(url: str, payload: dict):
        import httpx

        response = httpx.post(url, json=payload, timeout=10)
        try:
            body = response.json()
        except Exception:
            body = {}
        return response.status_code, body

    def update_available(self) -> bool:
        """True only when the desk named a build and it is newer than this one.
        Unknown is never 'out of date': a desk that has not answered must not
        put a warning on a buyer's screen."""
        if not self.current_version:
            return False
        return _older(APP_VERSION, self.current_version)

    def due(self, now: Optional[float] = None) -> bool:
        stamp = time.time() if now is None else now
        return (stamp - self._last_sent) >= self._every

    def send(self, report: dict, *, now: Optional[float] = None, nonce: Optional[str] = None) -> bool:
        stamp = time.time() if now is None else now
        payload = sign_payload(
            self._identity,
            report,
            nonce=nonce or f"{int(stamp * 1000)}-{id(report) & 0xFFFF:04x}",
            now=stamp,
        )
        # Marked sent BEFORE the attempt: a desk that is down must not be
        # retried every tick, and a missing report is a far smaller problem
        # than a machine spending its time on HTTP instead of fills.
        self._last_sent = stamp
        try:
            answer = self._post(self._url, payload)
        except Exception as exc:
            self.last_result = f"not sent: {exc}"[:200]
            _log.debug("status report not sent: %s", exc)
            return False
        # A post may answer with a code alone or with the body beside it; an
        # older stub returning just the code must keep working.
        code, body = answer if isinstance(answer, tuple) else (answer, {})
        if isinstance(body, dict) and body.get("current_version"):
            self.current_version = str(body["current_version"])[:20]
        self.last_result = f"HTTP {code}"
        if code != 200:
            _log.debug("status report refused: HTTP %s", code)
        return code == 200

    def maybe_send(self, build, *, now: Optional[float] = None, on_sent=None) -> bool:
        """Build and send only when due. `build` is a callable so a report is
        never assembled on the ticks that would throw it away.

        `on_sent(report, at, result)` is handed the exact message that went, so
        the buyer's own dashboard can show them what we were told."""
        if not self.due(now):
            return False
        stamp = time.time() if now is None else now
        try:
            report = build()
        except Exception as exc:
            self.last_result = f"not built: {exc}"[:200]
            _log.debug("status report not built: %s", exc)
            self._last_sent = stamp
            return False
        sent = self.send(report, now=now)
        if on_sent is not None:
            try:
                on_sent(report, stamp, self.last_result)
            except Exception:  # a dashboard must never break the sender
                _log.debug("report callback failed", exc_info=True)
        return sent
