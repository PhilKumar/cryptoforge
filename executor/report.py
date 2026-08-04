"""
executor/report.py — what the buyer actually reads.

The recovery design says the risk is disclosed, not enforced: every timeframe
stays available on a laptop, and the app's job is to make the cost of the
choice visible AT THE MOMENT IT IS MADE rather than in documentation nobody
opens.

So this file is deliberately about wording. The numbers all come from
elsewhere; what is decided here is which of them a buyer sees without asking,
and in what order.

The line that matters most is the armed exposure — "if this machine stops now,
at most $X can fill unwatched". It is knowable, because there is at most one
entry order of known size; it changes as the ladder moves; and it turns an
abstract worry into a figure someone can actually judge.

Like the rest of `executor/`, this must not import `engine.cascade`.
"""

from __future__ import annotations

from typing import List, Optional

from executor.power import PlatformPower, suspend_advice

# "5m" does not tell a buyer anything about how much babysitting it needs. The
# label does.
ATTENTION = {
    "5m": ("high attention", "Needs the machine awake nearly all the time."),
    "15m": ("some attention", "Copes with short breaks."),
    "1h": ("hands off", "Fine to leave alone overnight."),
    "4h": ("hands off", "Fine to leave alone overnight."),
    "1d": ("hands off", "Fine to leave alone overnight."),
}


def attention_label(timeframe: str) -> tuple:
    return ATTENTION.get(str(timeframe or "").lower(), ("some attention", "Copes with short breaks."))


def needs_acknowledgement(timeframe: str) -> bool:
    """
    Whether starting this campaign should stop and ask.

    Once per campaign, not a recurring nag: a buyer who has accepted the cost
    of a high-attention timeframe does not need telling again, and a warning
    that fires every time is a warning nobody reads.
    """
    return attention_label(timeframe)[0] == "high attention"


def campaign_start_notice(timeframe: str, *, fidelity: str, capital_warning: Optional[str] = None) -> List[str]:
    """What to show at the moment a campaign is about to start."""
    label, detail = attention_label(timeframe)
    lines = [f"{timeframe} — {label}. {detail}"]
    if fidelity == "coarse":
        lines.append(
            "At your capital the shallow rungs on this leg cannot clear the exchange minimum, "
            "so it will run with fewer, deeper entries than the signal describes."
        )
    elif fidelity == "none":
        lines.append("This leg's share is too small to place a single order, so it will be skipped.")
    if capital_warning:
        lines.append(capital_warning)
    return lines


def running_status(status: dict, *, power: Optional[PlatformPower] = None) -> List[str]:
    """
    The always-visible lines, most useful first.

    Exposure leads because it is the number a buyer is actually relying on.
    Everything after it explains why the executor is or is not doing anything.
    """
    exposure = float(status.get("armed_exposure_usd") or 0.0)
    lines = [
        f"If this machine stops now, at most ${exposure:,.2f} can fill unwatched."
        if exposure > 0
        else "Nothing can fill while this machine is away — no buy orders are resting."
    ]

    unprotected = status.get("unprotected") or []
    if unprotected:
        # The one genuinely bad configuration, and entirely preventable.
        lines.append(f"Holding coin with no sell order against it: {', '.join(unprotected)}. Placing one now.")

    if not status.get("opening_new"):
        lines.append(status.get("posture_reason") or "Not opening anything new.")

    halted = status.get("halted") or []
    if halted:
        lines.append(
            f"Stopped following {', '.join(halted)} — the published geometry contradicted itself. "
            "Existing positions there are still managed."
        )

    skipped = status.get("skipped") or {}
    for campaign_id, reason in skipped.items():
        lines.append(f"Not following {campaign_id}: {reason}")

    if power:
        advice = suspend_advice(power, armed_exposure_usd=exposure)
        if advice:
            lines.append(advice)
    return lines


def sleep_notice(message: str) -> str:
    """Shown as the machine goes away. Already plain; passed through as-is."""
    return message


def irreducible_risk() -> str:
    """
    The honest version, short, for whatever a buyer agrees to.

    Power loss with an entry resting will occasionally produce an unmanaged
    fill. Bounded to one order of known size, with no liquidation possible and
    a target placed within seconds of wake — but not eliminated, and saying so
    is the difference between a disclosure and a claim.
    """
    return (
        "This software trades from your machine. If your machine stops, a resting order may still "
        "fill, and that position waits for your machine to come back. It cannot be liquidated — this "
        "is spot, not leverage — and the most that can fill unwatched is the one order shown above."
    )
