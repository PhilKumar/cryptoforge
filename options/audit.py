"""options/audit.py — does this archive actually contain what you think?

Run this before any backtest, on any source, every time. It is deliberately
source-agnostic: point it at a Dhan pull or at the existing Upstox data and it
answers the same four questions.

    1. How complete is a trading day?   (bars per contract-session vs 375)
    2. Where does coverage begin?       (completeness by month)
    3. Does it hold near the money?     (completeness by |strike offset|)
    4. What was never delivered?        (200-with-no-candles, from the ledger)

Question 3 is the one that decides whether a result means anything. Deep-OTM
strikes being sparse is honest — nothing traded. ATM being sparse is a hole in
the vendor's archive, and ATM is where the P&L lives, so a book built over that
hole is measuring the gaps rather than the method.

Question 2 is how you catch a depth claim that is not true. If a source
advertises five years and completeness collapses to zero before some date, the
usable history is the part after that date, whatever the marketing says.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Optional

import pandas as pd

# NSE cash-session minutes: 09:15 to 15:30 inclusive of the opening minute.
BARS_PER_SESSION_1M = 375

# A session below this fraction of expected bars is treated as a hole rather
# than a quiet day. Chosen loose on purpose: real illiquidity does thin a
# series, and the aim is to catch archives that are missing, not thin.
SESSION_COMPLETE_THRESHOLD = 0.60


@dataclass
class AuditVerdict:
    usable_from: Optional[str]
    total_bars: int
    sessions: int
    complete_sessions: int
    atm_completeness: float
    silent_empty_requests: int
    notes: list

    def ok(self) -> bool:
        return (
            self.sessions > 0
            and self.atm_completeness >= SESSION_COMPLETE_THRESHOLD
            and self.silent_empty_requests == 0
        )


def _expected_bars(interval: str) -> int:
    per = {"1": 375, "5": 75, "15": 25, "25": 15, "60": 7}
    return per.get(str(interval), BARS_PER_SESSION_1M)


def session_completeness(bars: pd.DataFrame) -> pd.DataFrame:
    """Bars actually present per contract-session, against what a full session
    would hold. One row per (series, date)."""
    if bars.empty:
        return pd.DataFrame()
    df = bars.copy()
    df["ts"] = pd.to_datetime(df["ts"])
    df["session"] = df["ts"].dt.date
    grouped = (
        df.groupby(
            ["underlying", "expiry_flag", "expiry_code", "strike_offset", "option_type", "interval", "session"],
            dropna=False,
        )
        .size()
        .reset_index(name="bars")
    )
    grouped["expected"] = grouped["interval"].map(_expected_bars)
    grouped["completeness"] = grouped["bars"] / grouped["expected"]
    return grouped


def by_month(sessions: pd.DataFrame) -> pd.DataFrame:
    """Where does the archive actually start? Completeness per calendar month."""
    if sessions.empty:
        return pd.DataFrame()
    s = sessions.copy()
    s["month"] = pd.to_datetime(s["session"]).dt.to_period("M").astype(str)
    return (
        s.groupby("month")
        .agg(
            sessions=("bars", "size"),
            bars=("bars", "sum"),
            mean_completeness=("completeness", "mean"),
        )
        .reset_index()
        .sort_values("month")
    )


def by_moneyness(sessions: pd.DataFrame) -> pd.DataFrame:
    """The decisive cut. Sparse far from ATM is the market; sparse AT the
    money is the vendor."""
    if sessions.empty:
        return pd.DataFrame()
    s = sessions.copy()
    s["abs_offset"] = s["strike_offset"].abs()
    return (
        s.groupby("abs_offset")
        .agg(
            sessions=("bars", "size"),
            mean_completeness=("completeness", "mean"),
            empty_sessions=("bars", lambda x: int((x == 0).sum())),
        )
        .reset_index()
        .sort_values("abs_offset")
    )


def silent_empties(coverage: pd.DataFrame) -> pd.DataFrame:
    """Requests the source answered successfully, with nothing in them."""
    if coverage.empty:
        return pd.DataFrame()
    return coverage[coverage["status"] == "no_data"]


def audit(bars: pd.DataFrame, coverage: pd.DataFrame) -> AuditVerdict:
    sessions = session_completeness(bars)
    notes = []

    if sessions.empty:
        return AuditVerdict(None, 0, 0, 0, 0.0, len(silent_empties(coverage)), ["no bars in store — nothing to audit"])

    months = by_month(sessions)
    good_months = months[months["mean_completeness"] >= SESSION_COMPLETE_THRESHOLD]
    usable_from = good_months["month"].min() if not good_months.empty else None

    if usable_from and usable_from > months["month"].min():
        notes.append(
            f"coverage only becomes usable at {usable_from}, though rows exist "
            f"from {months['month'].min()} — treat earlier data as absent, not quiet"
        )

    money = by_moneyness(sessions)
    atm_rows = money[money["abs_offset"] <= 1]
    atm_completeness = float(atm_rows["mean_completeness"].mean()) if not atm_rows.empty else 0.0
    if atm_completeness < SESSION_COMPLETE_THRESHOLD:
        notes.append(
            f"near-ATM completeness is {atm_completeness:.0%} — this is a vendor "
            f"hole, not illiquidity, and it sits exactly where the P&L is"
        )

    empties = silent_empties(coverage)
    if len(empties):
        notes.append(
            f"{len(empties)} requests returned success with zero candles — "
            f"these are absences, and must not be read as no-trade sessions"
        )

    complete = int((sessions["completeness"] >= SESSION_COMPLETE_THRESHOLD).sum())
    return AuditVerdict(
        usable_from=usable_from,
        total_bars=int(sessions["bars"].sum()),
        sessions=len(sessions),
        complete_sessions=complete,
        atm_completeness=atm_completeness,
        silent_empty_requests=len(empties),
        notes=notes,
    )


def format_report(bars: pd.DataFrame, coverage: pd.DataFrame) -> str:
    sessions = session_completeness(bars)
    v = audit(bars, coverage)
    out = ["=" * 66, "OPTIONS DATA COVERAGE AUDIT", "=" * 66]
    out.append(
        f"bars={v.total_bars:,}  contract-sessions={v.sessions:,}  "
        f"complete={v.complete_sessions:,} "
        f"({(v.complete_sessions / v.sessions if v.sessions else 0):.0%})"
    )
    out.append(f"usable history begins: {v.usable_from or 'NEVER — no month passes threshold'}")
    out.append(f"near-ATM completeness: {v.atm_completeness:.0%}")
    out.append(f"silent empty responses: {v.silent_empty_requests}")

    if not sessions.empty:
        out += ["", "-- completeness by month " + "-" * 40]
        out.append(by_month(sessions).to_string(index=False))
        out += ["", "-- completeness by distance from ATM " + "-" * 28]
        out.append(by_moneyness(sessions).to_string(index=False))

    if v.notes:
        out += ["", "-- findings " + "-" * 52]
        out += [f"  * {n}" for n in v.notes]
    out += ["", f"VERDICT: {'USABLE' if v.ok() else 'NOT SAFE TO BACKTEST ON'}", "=" * 66]
    return "\n".join(out)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Audit an options archive before trusting it.")
    ap.add_argument("store_root", help="path passed to OptionStore")
    args = ap.parse_args(argv)
    from options.store import OptionStore

    store = OptionStore(args.store_root)
    print(format_report(store.load_bars(), store.load_coverage()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
