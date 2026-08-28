"""A second, independently written simulator for the final configuration.

Same spec, different author-shape: no shared harness code -- prices come
straight off the parquet files, store selection reimplemented, state machine
written event-style. If this and the harness disagree anywhere, one of them
has a bug.
"""

import json
import sys

sys.path.insert(0, "/Users/philipkumar/Documents/CryptoForge")
sys.path.insert(0, "/Users/philipkumar/Documents/CryptoForge/tools")
from bisect import bisect_left
from datetime import time as dtime

import pandas as pd
from supertrend_options_backtest import load_real_minutes, supertrend_dir, to_bars  # data + verified signal only

from options.charges import round_trip_charges
from tools.nifty_expiry_calendar import lot_size, weekly_expiries

ROOT = "/Users/philipkumar/Documents/CryptoForge/data"
SLIP = 0.0015
TRAIL, ARM, ROLL_PTS = 80.0, 100.0, 300
SQ = dtime(15, 20)
EA = dtime(9, 20)

minute = load_real_minutes()
bars = to_bars(minute, 60)
dirs = supertrend_dir(bars, 10, 1.5)
sessions = sorted({x.date() for x in minute.index.normalize().unique()})
weeklies = weekly_expiries(sessions)
monthlies = sorted(
    {
        max(w for w in weeklies if w.month == mth and w.year == yr)
        for yr in range(2021, 2027)
        for mth in range(1, 13)
        if any(w.month == mth and w.year == yr for w in weeklies)
    }
)

months = {}


def month(store, key):
    if (store, key) not in months:
        try:
            df = pd.read_parquet(f"{ROOT}/{store}/NIFTY_{key}.parquet", columns=["ts", "strike", "side", "open"])
            months[(store, key)] = {
                (t, int(s), sd): float(o) for t, s, sd, o in zip(df["ts"], df["strike"], df["side"], df["open"])
            }
        except Exception:
            months[(store, key)] = {}
    return months[(store, key)]


def nth(days, day, n):
    i = bisect_left(days, day) + n - 1
    return days[i] if 0 <= i < len(days) else None


def price(ts, strike, expiry):
    day = ts.date()
    store = (
        "dhan_options"
        if expiry == nth(weeklies, day, 1)
        else "dhan_options_e2"
        if expiry == nth(weeklies, day, 2)
        else "dhan_options_m1"
        if expiry == nth(monthlies, day, 1)
        else "dhan_options_m2"
        if expiry == nth(monthlies, day, 2)
        else None
    )
    if store is None:
        return None
    return month(store, ts.strftime("%Y-%m")).get((ts.to_pydatetime(), strike, "CE"))


midx = minute.index


def next_min(after):
    i = midx.searchsorted(after, side="right")
    return midx[i] if i < len(midx) else None


trades = []
pos = None
stamps = bars.index
H, L, C = bars["high"].values, bars["low"].values, bars["close"].values


def close_pos(pos, when, reason):
    px = price(when, pos["strike"], pos["expiry"])
    spot = float(minute["open"].loc[when]) if when in midx else None
    if px is None:
        px = max(0.0, (spot or 0) - pos["strike"])
        if px <= 0:
            return None
    buy = pos["prem"] * (1 + SLIP)
    sell = px * (1 - SLIP)
    q = pos["lot"]
    ch = round_trip_charges(trade_date=pos["ts"].date(), buy_premium=buy, sell_premium=sell, quantity=q).total
    return {
        "entry": str(pos["ts"]),
        "exit": str(when),
        "strike": pos["strike"],
        "net": (sell - buy) * q - ch,
        "reason": reason,
    }


for k in range(1, len(stamps)):
    ts = stamps[k]
    day = ts.date()
    bar_end = ts + pd.Timedelta(minutes=60)
    if pos is not None:
        exp = pos["expiry"]
        fired = None
        if day >= exp and (day > exp or ts.time() >= SQ):
            when = midx[midx.searchsorted(pd.Timestamp(f"{exp} 15:20"), side="right") - 1]
            r = close_pos(pos, when, "EXPIRY")
            if r:
                trades.append(r)
            pos = None
        elif pos["mfe"] > max(TRAIL, ARM) and (L[k] - pos["spot"]) <= pos["mfe"] - TRAIL:
            fired = "TRAIL"
        elif abs(C[k] - pos["strike"]) >= ROLL_PTS:
            fired = "ROLL"
        elif dirs[k] != 1:
            fired = "FLIP"
        if pos is not None and fired:
            when = next_min(bar_end - pd.Timedelta(minutes=1))
            if (
                when is not None
                and (fired != "ROLL" or when.date() <= exp)
                and (fired != "TRAIL" or when.date() <= exp)
            ):
                r = close_pos(pos, when, fired)
                if r:
                    trades.append(r)
                pos = None
        if pos is not None:
            pos["mfe"] = max(pos["mfe"], H[k] - pos["spot"])
    if pos is None and dirs[k] == 1:
        fill = next_min(bar_end - pd.Timedelta(minutes=1))
        if fill is None or fill.date() != day:
            continue
        if not (EA <= fill.time() < SQ):
            continue
        expiry = nth(weeklies, day, 2)
        if expiry is None:
            continue
        spot = float(minute["open"].loc[fill])
        strike = int(round(spot / 50) * 50)
        prem = price(fill, strike, expiry)
        if prem is None:
            continue
        pos = {
            "ts": fill,
            "spot": spot,
            "strike": strike,
            "expiry": expiry,
            "prem": prem,
            "lot": lot_size(expiry),
            "mfe": 0.0,
        }

if pos is not None:
    r = close_pos(pos, midx[-1], "END")
    if r:
        trades.append(r)

net = sum(t["net"] for t in trades)
print(f"REPLAYER: {len(trades)} trades, net {net:,.0f}")
ref = json.load(open("/Users/philipkumar/Documents/CryptoForge/tools/st_final_CE.json"))["results"][0]
print(f"HARNESS : {ref['n']} trades, net {ref['net']:,.0f}")
rt = {(t["entry"], t["exit"]): round(t["net"], 2) for t in trades}
ht = {(t["entry"], t["exit"]): round(t["net"], 2) for t in ref["trade_rows"]}
same = sum(1 for k in ht if k in rt and abs(rt[k] - ht[k]) < 1)
print(f"trade-for-trade identical: {same}/{len(ht)}")
only_h = [k for k in ht if k not in rt][:5]
only_r = [k for k in rt if k not in ht][:5]
if only_h:
    print("  in harness only:", only_h)
if only_r:
    print("  in replayer only:", only_r)
