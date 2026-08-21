"""NIFTY's weekly expiry dates and lot sizes, 2021 onward, checked against the tape.

PhilForge's ``data/nse_contract_rules.json`` is ground truth but only reaches
back to October 2024, because that is where its Upstox prices start. The Dhan
archive reaches to January 2021, so the eras before that file begins are carried
here -- and then the whole calendar is verified against the option tape rather
than trusted: on a real expiry day the nearest-expiry option is worth its
intrinsic value at the close and nothing more.
"""

from __future__ import annotations

from datetime import date, timedelta

# expiry_weekday: Monday=0 ... Friday=4.
# Sources: PhilForge data/nse_contract_rules.json for 2024-10-03 onward
# (derived from real listed chains); NSE/Zerodha bulletin 291849 for the 2021
# lot cut; the 2024-04-26 step is the one settled in proj_nifty_lot_ground_truth.
ERAS = [
    # Opens a year before the archive does, so the week containing its first
    # session (2021-01-01 is a Friday) still resolves an era.
    dict(
        effective_from=date(2020, 1, 1),
        expiry_weekday=3,
        lot_size=75,
        note="Thursday weekly expiry, lot 75 (weeklies stayed 75 through July 2021)",
    ),
    dict(
        effective_from=date(2021, 8, 1),
        expiry_weekday=3,
        lot_size=50,
        note="NIFTY weekly lot cut 75 -> 50 from August 2021 (bulletin 291849)",
    ),
    dict(
        effective_from=date(2024, 4, 26),
        expiry_weekday=3,
        lot_size=25,
        note="lot 25; verified from Upstox chains 2024-10-03..2024-12-26",
    ),
    dict(
        effective_from=date(2025, 1, 2),
        expiry_weekday=3,
        lot_size=75,
        note="lot raised to 75; verified 2025-01-02..2025-08-28",
    ),
    dict(
        effective_from=date(2025, 9, 1),
        expiry_weekday=1,
        lot_size=75,
        note="weekly expiry moved to Tuesday (first Tuesday settle 2025-09-02)",
    ),
    dict(
        effective_from=date(2026, 1, 6),
        expiry_weekday=1,
        lot_size=65,
        note="lot cut to 65; verified from 2026-01-06 onward",
    ),
]

STRIKE_STEP = 50.0


def era_for(day: date) -> dict:
    applicable = [e for e in ERAS if e["effective_from"] <= day]
    if not applicable:
        raise ValueError(f"no contract era defined on or before {day}")
    return applicable[-1]


def lot_size(expiry: date) -> int:
    """Keyed by the contract's expiry, which is how a lot revision actually
    binds -- it applies to the contracts listed under it, not to the day you
    happened to trade."""
    return era_for(expiry)["lot_size"]


def weekly_expiries(sessions: list) -> list:
    """One expiry per week: the era's weekday, walked back to the last trading
    session if the exchange was shut that day."""
    trading = set(sessions)
    out: list = []
    if not sessions:
        return out
    week = sessions[0] - timedelta(days=sessions[0].weekday())
    last = sessions[-1]
    while week <= last:
        target = week + timedelta(days=era_for(week)["expiry_weekday"])
        settle = None
        for back in range(0, 6):
            cand = target - timedelta(days=back)
            if cand in trading:
                settle = cand
                break
        if settle is not None and (not out or settle > out[-1]):
            out.append(settle)
        week += timedelta(days=7)
    return out


def verify_against_tape(expiries: list, store: str, underlying: str = "NIFTY", months: int = 0) -> dict:
    """A real expiry prices at intrinsic by the close; a non-expiry day does not.

    Returns the two distributions so the calendar can be judged rather than
    assumed. Reads the nearest-expiry store only.
    """
    import glob
    import os

    import pandas as pd

    expiry_set = set(expiries)
    on_expiry, off_expiry = [], []
    paths = sorted(glob.glob(os.path.join(store, f"{underlying}_*.parquet")))
    if months:
        step = max(1, len(paths) // months)
        paths = paths[::step][:months]
    for path in paths:
        df = pd.read_parquet(path, columns=["ts", "strike", "side", "close", "spot"])
        df = df[df["ts"].dt.time >= pd.Timestamp("15:20").time()]
        df = df[(df["spot"] > 5000) & (df["spot"] < 100000)]
        if not len(df):
            continue
        # the strike nearest that day's closing spot, both sides
        df["day"] = df["ts"].dt.date
        df["dist"] = (df["strike"] - df["spot"]).abs()
        atm = df[df["dist"] <= STRIKE_STEP]
        if not len(atm):
            continue
        intrinsic = (atm["strike"] - atm["spot"]).where(atm["side"] == "PE", atm["spot"] - atm["strike"]).clip(lower=0)
        atm = atm.assign(time_value=(atm["close"] - intrinsic).clip(lower=0))
        by_day = atm.groupby("day")["time_value"].median()
        for day, tv in by_day.items():
            (on_expiry if day in expiry_set else off_expiry).append(float(tv))
    import statistics

    def summarise(xs):
        if not xs:
            return {"n": 0}
        xs = sorted(xs)
        return {"n": len(xs), "median": round(statistics.median(xs), 2), "p90": round(xs[int(0.9 * (len(xs) - 1))], 2)}

    return {"expiry_days": summarise(on_expiry), "other_days": summarise(off_expiry)}


if __name__ == "__main__":
    import os
    import sys

    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from tools.nifty_index_from_dhan import DEFAULT_STORE, load_minutes, sessions

    m = load_minutes()
    days = sessions(m)
    exp = weekly_expiries(days)
    print(f"{len(days):,} trading sessions, {len(exp):,} weekly expiries {exp[0]} .. {exp[-1]}")
    shifted = [e for e in exp if e.weekday() != era_for(e)["expiry_weekday"]]
    print(f"{len(shifted)} expiries shifted off the era weekday by a holiday, e.g. {shifted[:6]}")
    print("time value of the ATM contract at the close, nearest-expiry store:")
    print(" ", verify_against_tape(exp, DEFAULT_STORE, months=12))
