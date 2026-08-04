"""
executor/recovery.py — the sleeping laptop.

How a buyer's executor survives sleep, shutdown, crash and network loss without
leaving money unmanaged. The companion prose is CASCADE_EXECUTOR_RECOVERY.md;
this is the part of it that runs.

Three properties bound the damage before any code here:

1. It is spot, not futures. An unmanaged position cannot be wiped out, only
   held longer than intended.
2. A stopped executor places no new orders — the pot only accumulates in the
   tick loop. Sleeping cannot deploy more capital.
3. There is at most one accumulated entry order at a time.

Together: **maximum unmanaged exposure equals the notional of the currently
resting entry order, and that number is known before the machine sleeps.**

Like the rest of `executor/`, this must not import `engine.cascade`.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import List, Optional

from executor.orders import OrderBook, OrderIntent

# The wake ladder's thresholds. These measure MONEY — how far the buyer's own
# positions may have drifted before a human should look — and are deliberately
# separate from the feed's data thresholds (2min replay / 24h cursor validity),
# which measure how long a cursor still means anything.
NORMAL_GAP_SEC = 120
CONFIRM_GAP_SEC = 6 * 3600

# Per-order budget for the cancels at sleep time. Deliberately short: the lid
# is closing and wifi is already tearing down. Do not retry into a closing
# window — record the failure instead, which is worth more than preventing it.
SLEEP_CANCEL_BUDGET_SEC = 2.0


@dataclass
class ShutdownRecord:
    """
    What the executor knew when it stopped. Written durably either way — an
    executor that knows it slept armed behaves very differently from one that
    assumes it slept clean.
    """

    shutdown_at: float
    reason: str = "clean"  # clean | sleep | crash
    slept_armed: bool = False
    armed_exposure_usd: float = 0.0
    resting_entry_ids: List[str] = field(default_factory=list)
    unprotected_campaigns: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "shutdown_at": self.shutdown_at,
            "reason": self.reason,
            "slept_armed": self.slept_armed,
            "armed_exposure_usd": self.armed_exposure_usd,
            "resting_entry_ids": list(self.resting_entry_ids),
            "unprotected_campaigns": list(self.unprotected_campaigns),
        }

    @classmethod
    def from_dict(cls, data: Optional[dict]) -> Optional["ShutdownRecord"]:
        if not data:
            return None
        return cls(
            shutdown_at=float(data.get("shutdown_at") or 0.0),
            reason=str(data.get("reason") or "clean"),
            slept_armed=bool(data.get("slept_armed")),
            armed_exposure_usd=float(data.get("armed_exposure_usd") or 0.0),
            resting_entry_ids=list(data.get("resting_entry_ids") or []),
            unprotected_campaigns=list(data.get("unprotected_campaigns") or []),
        )


# ── going away ───────────────────────────────────────────────────────


@dataclass
class SleepPlan:
    intents: List[tuple]  # (campaign_id, OrderIntent)
    record: ShutdownRecord

    @property
    def message(self) -> str:
        """What the buyer is told at sleep time, before they close the lid."""
        if self.record.slept_armed:
            return (
                f"Could not cancel every entry before sleeping. "
                f"${self.record.armed_exposure_usd:,.2f} can still fill while this machine is away, "
                f"and its position will wait here until it comes back."
            )
        return "Buy orders cancelled, sell orders left in place. Nothing can be bought while away."


def plan_for_sleep(book: OrderBook, *, now: Optional[float] = None, reason: str = "sleep") -> SleepPlan:
    """
    The twin invariants, as a list of things to do before the machine stops.

    1. Never sleep with an entry order resting — cancel them.
    2. Never sleep holding coin without an exit resting — place one.

    The asymmetry runs in opposite directions and is the whole point. An entry
    that fills unwatched creates a position with no target against it; the cost
    of cancelling wrongly is a missed entry, re-placed on the next tick after
    wake. An exit is the reverse: it can only close a position at a price
    already chosen, so a 3am rally through target wakes the buyer flat and
    profitable, and cancelling it would turn that into a miss.
    """
    stamp = time.time() if now is None else now
    intents = book.sleep_intents()
    return SleepPlan(
        intents=intents,
        record=ShutdownRecord(
            shutdown_at=stamp,
            reason=reason,
            armed_exposure_usd=book.armed_exposure_usd(),
            resting_entry_ids=[
                intent.client_order_id for _, intent in intents if intent.kind == "entry" and intent.action == "cancel"
            ],
            unprotected_campaigns=book.unprotected(),
        ),
    )


def record_sleep_outcome(record: ShutdownRecord, *, cancelled_ids: List[str]) -> ShutdownRecord:
    """
    Mark whether the cancels actually landed.

    The common real case is the lid closing while wifi tears down, so the
    cancel times out. Recording that is worth more than preventing it: on wake
    the flag drives an urgent reconcile ahead of everything else, and an
    executor that knows it slept armed does not assume its picture is current.
    """
    missed = [order_id for order_id in record.resting_entry_ids if order_id not in set(cancelled_ids)]
    record.slept_armed = bool(missed)
    record.resting_entry_ids = missed
    if not missed:
        record.armed_exposure_usd = 0.0
    return record


# ── coming back ──────────────────────────────────────────────────────


@dataclass(frozen=True)
class RecoveryStep:
    name: str
    why: str


# The order is the design. Protecting a held position comes BEFORE any geometry
# work, because an unprotected position is more urgent than a correct chart —
# and re-placing entries comes last, because re-placing against stale levels is
# worse than not re-placing at all.
ASK_EXCHANGE = RecoveryStep(
    "ask_exchange",
    "Open orders, fills since shutdown, balances. Local state is a hypothesis; the exchange is the fact.",
)
INGEST_FILLS = RecoveryStep(
    "ingest_fills",
    "Take fills with the exchange's own timestamps, not the moment we noticed them.",
)
PROTECT_POSITION = RecoveryStep(
    "protect_position",
    "Coin held with no exit resting gets one placed NOW, before any geometry work.",
)
REPLAY_GEOMETRY = RecoveryStep(
    "replay_geometry",
    "Replay missed candles, then the feed events missed in the gap.",
)
TP_CATCHUP = RecoveryStep(
    "tp_catchup",
    "If price is already past target, exit at market rather than waiting for a retest that may not come.",
)
REPLACE_ENTRIES = RecoveryStep(
    "replace_entries",
    "Re-place entries cancelled at shutdown — only now, because stale levels are worse than none.",
)
RECONCILE_FLOOR = RecoveryStep(
    "reconcile_floor",
    "Rebuild reuse_below from the buyer's OWN round history, never from the feed.",
)


@dataclass
class RecoveryPlan:
    gap_sec: float
    band: str  # normal | full | confirm
    steps: List[RecoveryStep]
    requires_confirmation: bool
    note: str = ""

    @property
    def step_names(self) -> List[str]:
        return [step.name for step in self.steps]


def classify_gap(gap_sec: float, *, slept_armed: bool = False) -> str:
    """
    Which band of the wake ladder this gap falls into.

    A gap flagged `slept_armed` skips straight to full recovery regardless of
    length: the machine went away with an order live, so however short the gap,
    its picture of what it holds cannot be trusted.
    """
    if slept_armed:
        return "confirm" if gap_sec > CONFIRM_GAP_SEC else "full"
    if gap_sec < NORMAL_GAP_SEC:
        return "normal"
    if gap_sec <= CONFIRM_GAP_SEC:
        return "full"
    return "confirm"


def plan_recovery(gap_sec: float, *, slept_armed: bool = False) -> RecoveryPlan:
    """
    What to do on wake, in order.

    Note what confirmation does and does not gate. Past six hours the executor
    does not auto-resume TRADING — it presents the divergence and waits. But it
    still asks the exchange, still ingests fills, and still protects a held
    position. Making someone click a button before their coin gets a target
    back would be enforcing a policy with their money, and the policy exists to
    protect them.
    """
    band = classify_gap(gap_sec, slept_armed=slept_armed)
    if band == "normal":
        return RecoveryPlan(
            gap_sec=gap_sec,
            band=band,
            steps=[ASK_EXCHANGE, INGEST_FILLS, PROTECT_POSITION],
            requires_confirmation=False,
            note="Short gap — the ordinary reconcile.",
        )

    steps = [ASK_EXCHANGE, INGEST_FILLS, PROTECT_POSITION, REPLAY_GEOMETRY, TP_CATCHUP]
    if band == "full":
        steps += [REPLACE_ENTRIES, RECONCILE_FLOOR]
        note = "Full recovery."
        if slept_armed:
            note = "Slept with an entry live — full recovery regardless of how short the gap was."
        return RecoveryPlan(gap_sec=gap_sec, band=band, steps=steps, requires_confirmation=False, note=note)

    return RecoveryPlan(
        gap_sec=gap_sec,
        band=band,
        steps=steps,
        requires_confirmation=True,
        note=(
            f"Away for {gap_sec / 3600:.1f} hours. Positions are protected and the picture is rebuilt, "
            "but no new entries go out until you have seen what changed."
        ),
    )


def tp_catchup_intent(
    campaign_id: str, *, target: float, market_price: float, quantity: float
) -> Optional[OrderIntent]:
    """
    Price ran past the target while the machine was away.

    Waiting for a retest that may not come is how a round that already won
    turns into one that is still open. Take it at market — the target was the
    decision, and the market being better than it does not change that.
    """
    if quantity <= 0 or target <= 0 or market_price < target:
        return None
    return OrderIntent(
        action="place",
        kind="exit",
        client_order_id=f"cfx-{campaign_id}-catchup",
        side="sell",
        order_type="market",
        quantity=quantity,
        reason=f"price {market_price:,.4f} is already past the {target:,.4f} target",
    )


def wake_report(plan: RecoveryPlan, record: Optional[ShutdownRecord]) -> str:
    """One paragraph a buyer can act on, not a log they have to decode."""
    away = f"{plan.gap_sec / 3600:.1f}h" if plan.gap_sec >= 3600 else f"{plan.gap_sec / 60:.0f}m"
    lines = [f"Away for {away}."]
    if record and record.slept_armed:
        lines.append(
            f"It went away with ${record.armed_exposure_usd:,.2f} of buy orders live, "
            f"so those may have filled while nothing was watching."
        )
    if record and record.unprotected_campaigns:
        lines.append(f"Holding without a sell order: {', '.join(record.unprotected_campaigns)} — placing now.")
    if plan.requires_confirmation:
        lines.append(plan.note)
    return " ".join(lines)
