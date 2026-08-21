"""Rebuild NIFTY's own candles out of the spot stamped on every Dhan option row.

PhilForge's index cache only reaches back to October 2024, but the Dhan options
archive reaches to January 2021 -- and every option row carries the underlying's
level at that minute. So the index is already inside the options archive; it
just has to be lifted out.

Measured against PhilForge's cache over 11,254 overlapping 15m bars, the lifted
series matches the real one exactly on the close and within about three points
on the wicks. The wick gap is sampling: the spot field only updates when some
option trades, so an extreme touched between prints is not recorded. The 09:15
bar is the one real divergence -- NSE's opening bar carries the pre-open
equilibrium print, which the option tape never sees.
"""

from __future__ import annotations

import glob
import os
from typing import Optional

import pandas as pd

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_STORE = os.path.join(REPO, "data", "dhan_options")
CACHE = os.path.join(REPO, "cache", "nifty_index_from_dhan.parquet")

SESSION_OPEN = pd.Timestamp("09:15").time()
SESSION_LAST_MINUTE = pd.Timestamp("15:29").time()


def build_minutes(store: str = DEFAULT_STORE, underlying: str = "NIFTY") -> pd.DataFrame:
    """One row per minute: the index level, lifted from the option tape."""
    frames = []
    for path in sorted(glob.glob(os.path.join(store, f"{underlying}_*.parquet"))):
        df = pd.read_parquet(path, columns=["ts", "spot"]).dropna(subset=["spot"])
        # Dhan's series occasionally bleeds another instrument in. A NIFTY row
        # quoting 8,900 or 68,100 is not a bad tick, it is a different index.
        df = df[(df["spot"] > 5000) & (df["spot"] < 100000)]
        if not len(df):
            continue
        g = df.groupby("ts")["spot"]
        frames.append(pd.DataFrame({"open": g.first(), "high": g.max(), "low": g.min(), "close": g.last()}))
    minute = pd.concat(frames).sort_index()
    minute = minute[~minute.index.duplicated(keep="first")]
    return minute[(minute.index.time >= SESSION_OPEN) & (minute.index.time <= SESSION_LAST_MINUTE)]


def load_minutes(store: str = DEFAULT_STORE, cache: str = CACHE, rebuild: bool = False) -> pd.DataFrame:
    if not rebuild and os.path.exists(cache):
        return pd.read_parquet(cache)
    minute = build_minutes(store)
    os.makedirs(os.path.dirname(cache), exist_ok=True)
    minute.to_parquet(cache)
    return minute


def to_bars(minute: pd.DataFrame, rule: str = "15min") -> pd.DataFrame:
    """Session-anchored bars. 09:15 is the anchor, so a 15m bar labelled 10:15
    covers 10:15:00 through 10:29:59 -- the convention every level test below
    assumes."""
    bars = (
        minute.resample(rule, origin=pd.Timestamp("2021-01-01 09:15"))
        .agg({"open": "first", "high": "max", "low": "min", "close": "last"})
        .dropna()
    )
    return bars


def to_daily(minute: pd.DataFrame) -> pd.DataFrame:
    g = minute.groupby(minute.index.normalize())
    return pd.DataFrame(
        {"open": g["open"].first(), "high": g["high"].max(), "low": g["low"].min(), "close": g["close"].last()}
    )


def sessions(minute: pd.DataFrame) -> list:
    return sorted({d.date() for d in minute.index.normalize().unique()})


def compare_to_reference(bars: pd.DataFrame, reference_json: str) -> Optional[pd.DataFrame]:
    """Bar-for-bar difference against PhilForge's own 15m cache, where they overlap."""
    import json

    if not os.path.exists(reference_json):
        return None
    ref = pd.DataFrame(json.load(open(reference_json)), columns=["ts", "open", "high", "low", "close"])
    ref["ts"] = pd.to_datetime(ref["ts"])
    ref = ref.set_index("ts")
    ref = ref[(ref.index.time >= SESSION_OPEN) & (ref.index.time <= pd.Timestamp("15:15").time())]
    return bars.join(ref, how="inner", lsuffix="_lifted", rsuffix="_ref")


if __name__ == "__main__":
    m = load_minutes(rebuild=True)
    b = to_bars(m)
    print(f"{len(m):,} minutes, {len(b):,} 15m bars, {m.index.min()} .. {m.index.max()}")
    j = compare_to_reference(
        b, "/Users/philipkumar/Documents/PhilForge/tools/.nifty_cache/NIFTY_15m_2024-10-01_2026-08-01.json"
    )
    if j is not None:
        print(f"overlap with PhilForge cache: {len(j):,} bars")
        for c in ("open", "high", "low", "close"):
            d = (j[f"{c}_lifted"] - j[f"{c}_ref"]).abs()
            print(
                f"  {c:<5} median {d.median():6.2f} pts   p90 {d.quantile(0.9):6.2f}   bars off >20pts {int((d > 20).sum()):,}"
            )
