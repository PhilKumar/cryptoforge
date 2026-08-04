"""
executor/model.py — everything the buyer's executor works out for itself.

The feed carries geometry. This file carries money. Nothing here comes off the
wire: the netting, the pool, the rung split, the take-profit and the capital
gate are all computed from the buyer's own capital and their own band ledger,
because their siblings are not our siblings and their fills are not our fills.

**This module must not import from `engine.cascade`.** It ships to buyers'
machines; the geometry engine does not. The constants below are therefore
copies, and copies drift — so `tests/test_executor_model.py` asserts each one
against the engine's value. That test lives in our repo and fails here, which
is the only place a drift can be caught before it reaches somebody's money.

The numbers are part of the `model_version` contract. If one of them changes,
`MODEL_VERSION` changes with it, and an executor that does not recognise the
version opens nothing.
"""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Tuple

Band = Tuple[float, float]

# ── the model contract (pinned against the engine by test) ───────────

MODEL_VERSION = 21
CASCADE_LEVELS = (2, 4, 8)
LEVEL_ALLOCATION = {2: 0.20, 4: 0.30, 8: 0.50}
# Shallow levels go in as buy stops above a falling market — resting a limit
# there buys a knife. Level 8 is the one worth owning at the line itself.
STOP_ENTRY_LEVELS = (2, 4)
TP_FIB_LEVEL = 0.25
TP_MUST_CLEAR_FEES = True
TP_MIN_NET_PCT = 0.05
FEE_PCT_PER_SIDE = 0.1
# An order sized exactly at the exchange minimum is one tick of adverse quote
# movement from rejection, so every rung carries 10% more.
RUNG_BUFFER_PCT = 0.10

# ── capital gating ───────────────────────────────────────────────────

# Below this, refuse to open. Not a soft warning: a ladder that can only afford
# its deepest rung is not this strategy, it is a single limit order wearing its
# name.
CAPITAL_HARD_FLOOR_USD = 1000.0
# Between the floor and this, campaigns run but coarsened — fewer, deeper
# entries than the feed describes. Said out loud at campaign start.
CAPITAL_FULL_FIDELITY_USD = 3000.0


# ── the band ledger ──────────────────────────────────────────────────


def merge_bands(bands: Iterable[Band]) -> List[Band]:
    """Sort and coalesce into a disjoint ascending list; touching bands merge."""
    clean = sorted((float(low), float(high)) for low, high in bands if float(high) > float(low) > 0)
    merged: List[Band] = []
    for low, high in clean:
        if merged and low <= merged[-1][1]:
            if high > merged[-1][1]:
                merged[-1] = (merged[-1][0], high)
        else:
            merged.append((low, high))
    return merged


def subtract_bands(span: Band, taken: Iterable[Band]) -> List[Band]:
    """
    The parts of `span` no band in `taken` covers, ascending.

    This is what makes a ledger different from a floor: ground taken in the
    MIDDLE of a fall leaves free ground both above and below it, and both come
    back.
    """
    low, high = float(span[0]), float(span[1])
    if high <= low:
        return []
    free: List[Band] = []
    cursor = low
    for t_low, t_high in merge_bands(taken):
        if t_high <= cursor:
            continue
        if t_low >= high:
            break
        if t_low > cursor:
            free.append((cursor, min(t_low, high)))
        cursor = max(cursor, t_high)
        if cursor >= high:
            break
    if cursor < high:
        free.append((cursor, high))
    return free


def free_span_of(span: Band, taken: Iterable[Band]) -> float:
    """How much of `span`, in price, is unfunded ground."""
    return sum(high - low for low, high in subtract_bands(span, taken))


# ── what the executor derives ────────────────────────────────────────


def net_allocation_pct(
    gross_pct: Optional[float],
    *,
    allocation_anchor: Optional[float],
    leg_low: Optional[float],
    mother_high: Optional[float],
    funded_bands: Iterable[Band],
) -> float:
    """
    The published gross, less the stretch this buyer's own siblings already
    funded.

    The feed cannot do this for them. Cross-campaign netting charges a leg only
    for price no sibling has paid for, and a follower running three of our six
    symbols has different siblings than we do. Their netting is theirs.

    Nets the PERCENT of the fall, never the capital — capital is a rate, and
    cutting it would shrink every rung until the pot could not clear the
    exchange minimum at all.
    """
    gross = float(gross_pct or 0.0)
    bands = list(funded_bands or [])
    if gross <= 0 or not bands or not allocation_anchor or leg_low is None:
        return max(gross, 0.0)
    span = (float(leg_low), min(float(allocation_anchor), float(mother_high or allocation_anchor)))
    if span[1] <= span[0]:
        return max(gross, 0.0)
    free_ratio = free_span_of(span, bands) / (span[1] - span[0])
    return max(gross * max(min(free_ratio, 1.0), 0.0), 0.0)


def leg_pool_usd(allocation_pct: float, capital_usd: float) -> float:
    """The one multiplication where the buyer's capital enters the model."""
    return max(float(allocation_pct), 0.0) * (float(capital_usd) / 100.0)


def level_price(high_anchor: float, low_anchor: float, level: float) -> float:
    """`high - level * (high - low)`. Must match the feed's `derived` checksum."""
    return float(high_anchor) - float(level) * (float(high_anchor) - float(low_anchor))


def rung_split(pool_usd: float) -> Dict[int, float]:
    """20/30/50 across levels 2/4/8."""
    return {level: max(float(pool_usd), 0.0) * share for level, share in LEVEL_ALLOCATION.items()}


def entry_style(level: int) -> str:
    return "stop" if level in STOP_ENTRY_LEVELS else "limit"


def min_rung_usd(min_notional_usd: float) -> float:
    """What one placeable order costs, cushion included."""
    return float(min_notional_usd) * (1.0 + RUNG_BUFFER_PCT)


def tp_breakeven_price(avg_entry: float) -> float:
    """Where a round merely pays both commissions and returns the cost basis."""
    rate = FEE_PCT_PER_SIDE / 100.0
    if rate <= 0:
        return float(avg_entry)
    return float(avg_entry) * (1.0 + rate) / (1.0 - rate)


def take_profit_price(avg_entry: float, mother_high: float) -> float:
    """
    Fib 0.25 off the buyer's OWN average entry — not ours. Their fills are
    theirs, so their target is theirs.

    The fee floor is dormant on real geometry (measured falls run 2.8-4.6%,
    well past the 0.80% crossing point). It exists to stop the pathological
    shallow round from setting a target that does not clear its own
    commission, not to move the target.
    """
    geometric = float(avg_entry) + TP_FIB_LEVEL * (float(mother_high) - float(avg_entry))
    if not TP_MUST_CLEAR_FEES:
        return geometric
    floor = tp_breakeven_price(avg_entry) * (1.0 + TP_MIN_NET_PCT / 100.0)
    return max(geometric, floor)


def fidelity(pool_usd: float, min_notional_usd: float) -> str:
    """
    Whether this leg's ladder can be laid as published.

    "full" — every rung clears the exchange minimum on its own.
    "coarse" — the shallow rungs cannot, so their money accumulates into
        deeper ones: fewer, deeper entries than the feed describes. Not broken,
        but not the same trade, and the buyer should be told.
    "none" — the whole pool cannot place a single order.

    Note this is per LEG, not per campaign. Pool scales with the leg's own
    depth, so a deep leg is faithful at capital where a shallow one is not —
    which is why a single "$3,000 is enough" number would be a lie in both
    directions.
    """
    one_rung = min_rung_usd(min_notional_usd)
    if float(pool_usd) < one_rung:
        return "none"
    smallest = min(rung_split(pool_usd).values())
    return "full" if smallest >= one_rung else "coarse"


def capital_gate(capital_usd: float) -> Tuple[bool, str, Optional[str]]:
    """
    (may_open, tier, warning). Decided entirely here; the feed carries no
    capital and never sees this.
    """
    capital = float(capital_usd or 0.0)
    if capital < CAPITAL_HARD_FLOOR_USD:
        return (
            False,
            "below_floor",
            f"${capital:,.0f} is under the ${CAPITAL_HARD_FLOOR_USD:,.0f} minimum. "
            "A ladder that can only afford its deepest rung is not this strategy.",
        )
    if capital < CAPITAL_FULL_FIDELITY_USD:
        return (
            True,
            "coarsened",
            f"At ${capital:,.0f} the shallow rungs on smaller legs will not clear the "
            "exchange minimum, so those campaigns run with fewer, deeper entries than published.",
        )
    return True, "full", None


def verify_derived_levels(published: Optional[dict], high_anchor: float, low_anchor: float, *, tol=1e-6) -> bool:
    """
    Recompute the feed's `derived` checksum and compare.

    A mismatch means the two sides disagree about the model. That is a halt on
    that campaign, not a trade — the published levels are where money goes, and
    "close enough" is not a standard to place orders against.
    """
    if not published:
        return True
    for level in CASCADE_LEVELS:
        ours = level_price(high_anchor, low_anchor, level)
        theirs = published.get(f"level_{level}")
        if theirs is None or abs(float(theirs) - ours) > tol:
            return False
    return True


def verify_allocation(
    published_gross: Optional[float], anchor: Optional[float], low: Optional[float], *, tol=1e-6
) -> bool:
    """Same idea for the funding percent: recompute, don't trust."""
    if published_gross is None or not anchor or low is None:
        return published_gross is None
    ours = (float(anchor) - float(low)) / float(anchor) * 100.0
    return abs(float(published_gross) - ours) <= tol
