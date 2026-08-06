"""
executor/feed_client.py — the buyer's end of the wire.

Connects to `/ws/cascade-feed`, proves who it is, verifies every frame against
a root-signed key set, and keeps a picture of the geometry we publish. It
places no orders and knows nothing about the buyer's capital: this is the part
that decides what is TRUE, and `executor/model.py` is the part that decides
what to do about it.

Three postures, and the difference between them matters more than it looks:

- **following** — verified, entitled, current. Opens new structure.
- **stale** — two missed heartbeats, or a key set past its cache TTL. Keeps
  managing what is already held; opens nothing new.
- **halted** — a campaign whose published geometry contradicts itself. Stops
  on THAT campaign only, and says why.

The middle one carries the design. A feed that goes quiet must never look like
a calm market, and it must never cause a position to be abandoned either — so
"stale" is a state that trades less, not a state that stops caring.

Like `model.py`, this must not import `engine.cascade`.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, Optional

from cryptography.exceptions import InvalidSignature

from executor import model

# Two missed beats. Silence has to be distinguishable from "nothing happened",
# and one missed beat is a hiccup on a domestic connection.
STALE_AFTER_SEC = 90
# How long a cached key set is trusted before it must be refetched. This is the
# number that makes a revocation land on every executor in the field within a
# day, including ones that were switched off when it was published.
KEYSET_CACHE_TTL_SEC = 24 * 3600
# Join-at-start. A campaign older than this when we hear about it is one we
# missed the beginning of, and the ladder only makes sense from its mother.
MAX_JOIN_AGE_SEC = 300
# Refuse to open anything new if entitlement has not been confirmed this long.
# Otherwise "pull the network cable" is a way to keep trading forever.
ENTITLEMENT_GRACE_SEC = 24 * 3600


class FeedHalt(Exception):
    """This campaign's published geometry contradicts itself. Do not trade it."""


@dataclass
class FollowedLeg:
    leg_id: int
    trendline_id: int
    low: float
    touch_high: float
    fib_high: float
    fib_low: float
    allocation_anchor: float
    allocation_pct_gross: float
    finalized: bool = False

    def level_prices(self) -> Dict[int, float]:
        return {lvl: model.level_price(self.fib_high, self.fib_low, lvl) for lvl in model.CASCADE_LEVELS}


@dataclass
class FollowedCampaign:
    campaign_id: str
    symbol: str
    exchange: str
    created_at: int
    mother_high: float
    mother_low: float
    mother_timestamp: int
    timeframe: str
    state: str
    model_version: int
    min_notional_usd: float = 5.0
    tick_size: float = 0.01
    # Published on campaign.opened. The buy-stop raise allowance is measured in
    # this instrument's OWN bars, so a fabricated stand-in would be a different
    # filter on every market — which is the exact bug the bar-scaling fixed.
    median_bar_pct: float = 0.0
    joined: bool = False
    skip_reason: str = ""
    halted: str = ""
    legs: Dict[int, FollowedLeg] = field(default_factory=dict)
    trendlines: Dict[int, dict] = field(default_factory=dict)
    standing_trendline_id: Optional[int] = None

    @property
    def active(self) -> bool:
        return self.joined and not self.halted and self.state not in {"COMPLETED", "MOTHER_BROKEN", "STOPPED"}


class FeedClient:
    """
    Consumes verified frames and maintains the followed picture.

    Deliberately transport-free: `handle_frame` takes a frame, and whatever
    delivers it — a live socket, a replay, a test — is somebody else's problem.
    That is also what makes the join and halt rules testable without a network.
    """

    def __init__(
        self,
        *,
        public_keys: Dict[str, str],
        keyset_fetched_at: float,
        now_fn: Callable[[], float] = time.time,
        max_join_age_sec: int = MAX_JOIN_AGE_SEC,
        on_event: Optional[Callable[[str, dict], None]] = None,
        resumed_campaign_ids: Iterable[str] = (),
    ):
        self._keys = dict(public_keys)
        self._keyset_fetched_at = float(keyset_fetched_at)
        self._now = now_fn
        self._max_join_age = int(max_join_age_sec)
        self._on_event = on_event
        # Campaigns this machine had already joined before it stopped. The join
        # window asks "did we see this start?", and for these the answer is yes
        # — the process restarting does not un-see it. Without this a buyer who
        # rebooted mid-campaign kept the coin they held and stopped laddering
        # into it, turning a three-step entry into a one-step one silently.
        self._resumed = {str(cid) for cid in resumed_campaign_ids if cid}
        self.campaigns: Dict[str, FollowedCampaign] = {}
        self.cursors: Dict[str, int] = {}
        self.last_heartbeat_at: float = 0.0
        self.last_entitled_at: float = float(keyset_fetched_at)
        self.needs_resnapshot: bool = False

    # ── posture ──────────────────────────────────────────────────────

    @property
    def keyset_expired(self) -> bool:
        return self._now() - self._keyset_fetched_at > KEYSET_CACHE_TTL_SEC

    @property
    def stale(self) -> bool:
        if not self.last_heartbeat_at:
            return False
        return self._now() - self.last_heartbeat_at > STALE_AFTER_SEC

    @property
    def may_open_new(self) -> tuple[bool, str]:
        """
        Whether new structure may be opened. Never gates EXIT management —
        an executor that cannot open is still responsible for what it holds.
        """
        if self.keyset_expired:
            return False, "The signing key set is out of date. Reconnect to refresh it."
        if self._now() - self.last_entitled_at > ENTITLEMENT_GRACE_SEC:
            return False, "Subscription has not been confirmed in 24 hours."
        if self.stale:
            return False, "Signal feed is stale — managing open positions only."
        return True, ""

    # ── ingestion ────────────────────────────────────────────────────

    def handle_frame(self, frame: dict) -> Optional[dict]:
        """
        Verify, route, and return the parsed envelope (None if ignored).

        An unverifiable frame raises rather than being skipped. Silently
        dropping one would leave the picture quietly incomplete, which on this
        wire means trading a ladder that is missing a rung.
        """
        envelope = self._verify(frame)
        msg_type = envelope.get("type")
        symbol = envelope.get("symbol") or ""

        if msg_type == "heartbeat":
            self.last_heartbeat_at = self._now()
            self.last_entitled_at = self._now()
            head = int(envelope.get("seq") or 0)
            if head > self.cursors.get(symbol, 0):
                # A gap found on a silent symbol. Without the head riding the
                # heartbeat this would stay hidden until the next real event,
                # which on a quiet symbol could be hours.
                self.needs_resnapshot = True
            return envelope

        seq = int(envelope.get("seq") or 0)
        known = self.cursors.get(symbol)
        if known is not None and seq > known + 1 and msg_type in model_logged_types():
            # A gap re-snapshots rather than guessing. Filling the hole would
            # usually work, and "usually" is not a standard for placing orders.
            self.needs_resnapshot = True
        self.cursors[symbol] = max(seq, known or 0)

        handler = {
            "campaign.opened": self._on_campaign_opened,
            "campaign.state": self._on_campaign_state,
            "campaign.closed": self._on_campaign_closed,
            "trendline.set": self._on_trendline_set,
            "leg.opened": self._on_leg_opened,
            "leg.finalized": self._on_leg_finalized,
        }.get(msg_type)
        if not handler:
            return None
        try:
            handler(envelope, envelope.get("payload") or {})
        except FeedHalt as halt:
            campaign = self.campaigns.get(envelope.get("campaign_id") or "")
            if campaign:
                campaign.halted = str(halt)
            self._emit("halt", {"campaign_id": envelope.get("campaign_id"), "reason": str(halt)})
        return envelope

    def _verify(self, frame: dict) -> dict:
        try:
            return verify_frame_local(frame, self._keys)
        except InvalidSignature:
            self._emit("bad_signature", {"frame": frame})
            raise

    # ── handlers ─────────────────────────────────────────────────────

    def _on_campaign_opened(self, envelope: dict, payload: dict) -> None:
        campaign_id = payload.get("campaign_id") or envelope.get("campaign_id")
        if campaign_id in self.campaigns:
            # Idempotent by identity: a restart on their side re-announces what
            # is running, and a subscribe snapshot overlaps the event stream.
            return

        campaign = FollowedCampaign(
            campaign_id=campaign_id,
            symbol=payload.get("symbol") or envelope.get("symbol") or "",
            exchange=str(payload.get("exchange") or ""),
            created_at=int(payload.get("created_at") or 0),
            mother_high=float(payload.get("mother_high") or 0.0),
            mother_low=float(payload.get("mother_low") or 0.0),
            mother_timestamp=int(payload.get("mother_timestamp") or 0),
            timeframe=str(payload.get("timeframe") or ""),
            state=str(payload.get("state") or ""),
            model_version=int(envelope.get("model_version") or 0),
            min_notional_usd=float((payload.get("advisory") or {}).get("min_notional_usd") or 5.0),
            tick_size=float((payload.get("advisory") or {}).get("tick_size") or 0.01),
            median_bar_pct=float(payload.get("median_bar_pct") or 0.0),
        )
        self.campaigns[campaign_id] = campaign

        if campaign.model_version != model.MODEL_VERSION:
            # Not a halt and not a crash: the geometry may be drawn under rules
            # we would read differently, so we decline the campaign and say so.
            campaign.skip_reason = (
                f"Drawn under model v{campaign.model_version}; this executor understands v{model.MODEL_VERSION}."
            )
        else:
            age = self._now() - campaign.created_at
            # Resuming is not joining late: we were in this one before the
            # restart, so the ladder we would rebuild is the ladder we already
            # had. The age check exists to stop us picking up a fall we never
            # saw the top of, which is a different situation entirely.
            if age > self._max_join_age and campaign_id not in self._resumed:
                campaign.skip_reason = (
                    f"Started {int(age)}s ago — past the {self._max_join_age}s join window. "
                    "A ladder only makes sense from its mother."
                )
            else:
                may_open, reason = self.may_open_new
                campaign.skip_reason = "" if may_open else reason
                campaign.joined = may_open

        self._emit("campaign", {"campaign_id": campaign_id, "joined": campaign.joined, "reason": campaign.skip_reason})

    def _on_trendline_set(self, envelope: dict, payload: dict) -> None:
        campaign = self.campaigns.get(envelope.get("campaign_id") or "")
        if not campaign:
            return
        trendline_id = payload.get("trendline_id")
        supersedes = payload.get("supersedes")
        if supersedes is not None and supersedes in campaign.trendlines:
            old = campaign.trendlines[supersedes]
            # A new line may never sit BELOW the one it replaces. Asserted
            # rather than trusted: if it does, that is a feed bug, and
            # following it would draw fibs off a line we know is wrong.
            if _lower_at_both_anchors(payload, old):
                raise FeedHalt(f"trendline {trendline_id} sits below the line it supersedes ({supersedes})")
        campaign.trendlines[trendline_id] = dict(payload)
        campaign.standing_trendline_id = trendline_id

    def _on_leg_opened(self, envelope: dict, payload: dict) -> None:
        campaign = self.campaigns.get(envelope.get("campaign_id") or "")
        if not campaign:
            return
        fib = payload.get("fib") or {}
        high_anchor, low_anchor = fib.get("high_anchor"), fib.get("low_anchor")
        if high_anchor is None or low_anchor is None:
            return

        if not model.verify_derived_levels(payload.get("derived"), high_anchor, low_anchor):
            raise FeedHalt(f"leg {payload.get('leg_id')}: published levels do not match the fib ladder")
        if not model.verify_allocation(
            payload.get("allocation_pct_gross"), payload.get("allocation_anchor"), payload.get("low")
        ):
            raise FeedHalt(f"leg {payload.get('leg_id')}: published allocation does not match its own anchor and low")

        leg_id = int(payload.get("leg_id"))
        campaign.legs[leg_id] = FollowedLeg(
            leg_id=leg_id,
            trendline_id=int(payload.get("trendline_id") or 0),
            low=float(payload.get("low")),
            touch_high=float(payload.get("touch_high")),
            fib_high=float(high_anchor),
            fib_low=float(low_anchor),
            allocation_anchor=float(payload.get("allocation_anchor") or campaign.mother_high),
            allocation_pct_gross=float(payload.get("allocation_pct_gross") or 0.0),
        )
        self._emit("leg", {"campaign_id": campaign.campaign_id, "leg_id": leg_id})

    def _on_leg_finalized(self, envelope: dict, payload: dict) -> None:
        campaign = self.campaigns.get(envelope.get("campaign_id") or "")
        leg = campaign.legs.get(int(payload.get("leg_id") or 0)) if campaign else None
        if leg:
            leg.finalized = True

    def _on_campaign_state(self, envelope: dict, payload: dict) -> None:
        campaign = self.campaigns.get(envelope.get("campaign_id") or "")
        if campaign:
            campaign.state = str(payload.get("state") or campaign.state)

    def _on_campaign_closed(self, envelope: dict, payload: dict) -> None:
        campaign = self.campaigns.get(envelope.get("campaign_id") or "")
        if not campaign:
            return
        campaign.state = str(payload.get("state") or "STOPPED")
        # Stop drawing new structure — but do NOT flatten. What they hold and
        # what we hold are different positions, and unwinding is the buyer's
        # decision against their own fills.
        self._emit(
            "closed",
            {"campaign_id": campaign.campaign_id, "reason": payload.get("reason"), "flatten": False},
        )

    # ── what the buyer's ladder should look like ─────────────────────

    def plan(self, campaign_id: str, *, capital_usd: float, funded_bands=()) -> Optional[dict]:
        """
        The buyer's own ladder for a followed campaign: their netting, their
        pool, their rungs. Returns None for a campaign not being followed.

        Nothing here is placed. This is the picture an order layer would act
        on, and keeping it a pure function is what makes the money maths
        testable without an exchange.
        """
        campaign = self.campaigns.get(campaign_id)
        if not campaign or not campaign.active:
            return None
        allowed, tier, warning = model.capital_gate(capital_usd)
        if not allowed:
            return {"campaign_id": campaign_id, "refused": warning, "legs": []}

        legs = []
        for leg in sorted(campaign.legs.values(), key=lambda item: item.leg_id):
            # `finalized` does NOT mean spent, and skipping these legs meant a
            # buyer following a live campaign placed nothing at all.
            #
            # In the engine a leg is created with `finalized = True` the moment
            # its swing is complete, and its fib and rungs are built on the very
            # next lines — "the previous fib keeps every rung it has"
            # (engine/cascade.py `_open_leg`). So a finalized leg is the NORMAL
            # shape of a tradeable one: it means the anchors are locked, not
            # that the money is gone. Verified against live BTCUSDT #147, whose
            # only leg was finalized with three PENDING rungs holding real money
            # while the executor showed an empty ladder.
            net_pct = model.net_allocation_pct(
                leg.allocation_pct_gross,
                allocation_anchor=leg.allocation_anchor,
                leg_low=leg.low,
                mother_high=campaign.mother_high,
                funded_bands=funded_bands,
            )
            pool = model.leg_pool_usd(net_pct, capital_usd)
            legs.append(
                {
                    "leg_id": leg.leg_id,
                    "allocation_pct_gross": leg.allocation_pct_gross,
                    "allocation_pct_net": net_pct,
                    "pool_usd": pool,
                    "fidelity": model.fidelity(pool, campaign.min_notional_usd),
                    "rungs": [
                        {
                            "level": level,
                            "price": leg.level_prices()[level],
                            "usd": usd,
                            "entry_style": model.entry_style(level),
                        }
                        for level, usd in sorted(model.rung_split(pool).items())
                    ],
                }
            )
        return {
            "campaign_id": campaign_id,
            "symbol": campaign.symbol,
            "exchange": campaign.exchange,
            "capital_tier": tier,
            "capital_warning": warning,
            "legs": legs,
        }

    def target_price(self, campaign_id: str, avg_entry: float) -> Optional[float]:
        """The buyer's target, off the buyer's OWN average entry and own venue."""
        campaign = self.campaigns.get(campaign_id)
        if not campaign or avg_entry <= 0:
            return None
        return model.take_profit_price(avg_entry, campaign.mother_high, exchange=campaign.exchange)

    def _emit(self, kind: str, detail: dict) -> None:
        if self._on_event:
            try:
                self._on_event(kind, detail)
            except Exception:
                pass


def model_logged_types() -> frozenset:
    return frozenset(
        {"campaign.opened", "campaign.state", "campaign.closed", "trendline.set", "leg.opened", "leg.finalized"}
    )


def _lower_at_both_anchors(new: dict, old: dict) -> bool:
    """Is the new line below the old one where they can be compared?"""
    return float(new.get("anchor1_price") or 0) <= float(old.get("anchor1_price") or 0) and float(
        new.get("anchor2_price") or 0
    ) < float(old.get("anchor2_price") or 0)


# ── verification, kept local ─────────────────────────────────────────
#
# The executor ships without engine/cascade_feed.py, so the reading half of the
# wire contract is duplicated here. It is small, and it is the half where a
# subtle difference from the writer would be catastrophic, so it is duplicated
# on purpose rather than shared through an import that would not exist on a
# buyer's machine. tests/test_executor_feed_client.py checks the two agree.


def verify_frame_local(frame: dict, public_keys: Dict[str, str]) -> dict:
    """Verify the transmitted bytes and return the parsed envelope."""
    import base64

    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

    msg = frame.get("msg")
    sig = frame.get("sig") or ""
    if not isinstance(msg, str) or not isinstance(sig, str):
        raise InvalidSignature("frame is not a signed message")
    parts = sig.split(":", 2)
    if len(parts) != 3 or parts[0] != "ed25519":
        raise InvalidSignature("unrecognised signature format")
    _, kid, encoded = parts
    pub = public_keys.get(kid)
    if not pub:
        raise InvalidSignature(f"unknown or revoked key id {kid!r}")
    Ed25519PublicKey.from_public_bytes(base64.b64decode(pub.encode("ascii"))).verify(
        base64.b64decode(encoded.encode("ascii")), msg.encode("utf-8")
    )
    return json.loads(msg)


def new_nonce() -> str:
    return uuid.uuid4().hex


def active_keys_from_keyset(document: dict, *, now: Optional[float] = None) -> Dict[str, str]:
    """kid → public key, honouring windows and the revoked list."""
    stamp = time.time() if now is None else now
    revoked = set(document.get("revoked") or [])
    keys = {}
    for key in document.get("keys") or []:
        kid = key.get("kid")
        if not kid or kid in revoked:
            continue
        if not (int(key.get("not_before") or 0) <= stamp < int(key.get("not_after") or 0)):
            continue
        keys[kid] = key.get("public")
    return keys


__all__ = [
    "FeedClient",
    "FeedHalt",
    "FollowedCampaign",
    "FollowedLeg",
    "active_keys_from_keyset",
    "new_nonce",
    "verify_frame_local",
    "KEYSET_CACHE_TTL_SEC",
    "MAX_JOIN_AGE_SEC",
    "STALE_AFTER_SEC",
]
