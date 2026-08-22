"""Weekly CPR + EMA20 on NIFTY 15m, expressed in bi-weekly PE options.

The rule, as given:

    Weekly CPR drawn on the 15m chart. While the market is below the CPR, wait
    for it to come back up and tag the band, then buy a PE. EMA20 is the
    regime filter -- no PE while price is above it. Take profit trails down the
    half-pivot ladder S0.5, S1, S1.5, S2 ...: whichever level the market has
    reached becomes the exit, and a 15m candle closing back above that level
    ends the trade. A 15m close above the top of the CPR band kills the trade
    before any target is reached.

"Bi-weekly" is read as the second-nearest weekly expiry -- eight to fourteen
days of runway at entry -- which is what ``data/dhan_options_e2`` holds.

Two honesty rules run through this file:

  * A contract is priced only out of the store that actually holds it, minute by
    minute, and never from a neighbouring minute. A miss is counted, not filled.
  * Dhan sells about twelve strikes either side of the money. A PE held while
    NIFTY falls more than ~600 points walks off the edge of the archive -- and
    that is the winning trade, so its absence would flatter nothing and cost
    everything. Those exits are floored at intrinsic value, which an in-the-money
    put is worth at minimum, and reported separately from the priced book.
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Optional

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from options.charges import round_trip_charges  # noqa: E402
from options.dhan_listed import DhanListedSource  # noqa: E402
from tools.nifty_expiry_calendar import STRIKE_STEP, lot_size, weekly_expiries  # noqa: E402
from tools.nifty_index_from_dhan import load_minutes, sessions, to_bars, to_daily  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STORES = {
    "e1": os.path.join(REPO, "data", "dhan_options"),
    "e2": os.path.join(REPO, "data", "dhan_options_e2"),
    "m1": os.path.join(REPO, "data", "dhan_options_m1"),
    "m2": os.path.join(REPO, "data", "dhan_options_m2"),
}
# A contract still open on its own expiry day is squared off here, not carried
# into the settlement print.
SQUARE_OFF = time(15, 15)  # default; --square-off overrides per run
SESSION_CLOSE = time(15, 30)


# ---------------------------------------------------------------- levels ----
# Every rung a trade can act on, ordered high to low. A PE entered on a
# rejection at one rung takes the rungs beneath it as its targets and the rung
# directly above it as its stop, so the whole rule is one walk down this list.
RUNG_ORDER = [
    "R5",
    "R4.5",
    "R4",
    "R3.5",
    "R3",
    "R2.5",
    "R2",
    "R1.5",
    "R1",
    "R0.5",
    "TC",
    "BC",
    "S0.5",
    "S1",
    "S1.5",
    "S2",
    "S2.5",
    "S3",
    "S3.5",
    "S4",
    "S4.5",
    "S5",
]


def _finer(rungs: dict) -> dict:
    """Halve every gap between neighbouring R or S rungs, so the trail ratchets
    twice as often. TC and BC are left alone -- the CPR band is not a pivot
    step and interpolating across it would invent a level nobody draws."""
    items = list(rungs.items())
    out: dict = {}
    for i, (label, price) in enumerate(items):
        out[label] = price
        if i + 1 >= len(items):
            continue
        nxt, nxt_price = items[i + 1]
        if label[0] != nxt[0] or label[0] not in "RS":
            continue
        a, b = float(label[1:]), float(nxt[1:])
        out[f"{label[0]}{(a + b) / 2:g}"] = (price + nxt_price) / 2.0
    return out


@dataclass(frozen=True)
class Levels:
    """One week's floor pivots, as an ordered ladder of rungs."""

    pivot: float
    rungs: dict  # label -> price, ordered high to low

    @property
    def bc(self) -> float:
        return self.rungs["BC"]

    @property
    def tc(self) -> float:
        return self.rungs["TC"]

    def below(self, label: str) -> list:
        items = list(self.rungs.items())
        i = [k for k, _ in items].index(label)
        return items[i + 1 :]

    def rungs_above(self, label: str) -> list:
        items = list(self.rungs.items())
        i = [k for k, _ in items].index(label)
        return items[:i]

    def above(self, label: str) -> Optional[tuple]:
        items = list(self.rungs.items())
        i = [k for k, _ in items].index(label)
        return items[i - 1] if i > 0 else None


def weekly_levels(daily: pd.DataFrame) -> dict:
    """Each week's CPR and pivots, computed from the week before it.

    TradingView's Traditional pivots, which is what the Indian CPR scripts draw.
    The half levels are midpoints of neighbouring pivots -- S0.5 sits between
    the pivot and S1, R0.5 between the pivot and R1, and so on outward.
    """
    wk = (
        daily.resample("W-MON", label="left", closed="left")
        .agg({"high": "max", "low": "min", "close": "last"})
        .dropna()
    )
    out: dict = {}
    for i in range(1, len(wk)):
        hi, lo, cl = wk["high"].iloc[i - 1], wk["low"].iloc[i - 1], wk["close"].iloc[i - 1]
        p = (hi + lo + cl) / 3.0
        bc = (hi + lo) / 2.0
        tc = 2 * p - bc
        if tc < bc:
            tc, bc = bc, tc
        sup = [
            2 * p - hi,  # S1
            p - (hi - lo),  # S2
            2 * p - (2 * hi - lo),  # S3
            3 * p - (3 * hi - lo),  # S4
            4 * p - (4 * hi - lo),  # S5
        ]
        res = [
            2 * p - lo,  # R1
            p + (hi - lo),  # R2
            2 * p + (hi - 2 * lo),  # R3
            3 * p + (hi - 3 * lo),  # R4
            4 * p + (hi - 4 * lo),  # R5
        ]
        priced = {"TC": tc, "BC": bc, "R0.5": (p + res[0]) / 2.0, "S0.5": (p + sup[0]) / 2.0}
        for n in range(5):
            priced[f"S{n + 1}"], priced[f"R{n + 1}"] = sup[n], res[n]
            if n + 1 < 5:
                priced[f"S{n + 1}.5"] = (sup[n] + sup[n + 1]) / 2.0
                priced[f"R{n + 1}.5"] = (res[n] + res[n + 1]) / 2.0
        out[wk.index[i].date()] = Levels(pivot=p, rungs={k: priced[k] for k in RUNG_ORDER})
    return out


def _levels_from(hi: float, lo: float, cl: float) -> "Levels":
    p = (hi + lo + cl) / 3.0
    bc = (hi + lo) / 2.0
    tc = 2 * p - bc
    if tc < bc:
        tc, bc = bc, tc
    sup = [2 * p - hi, p - (hi - lo), 2 * p - (2 * hi - lo), 3 * p - (3 * hi - lo), 4 * p - (4 * hi - lo)]
    res = [2 * p - lo, p + (hi - lo), 2 * p + (hi - 2 * lo), 3 * p + (hi - 3 * lo), 4 * p + (hi - 4 * lo)]
    priced = {"TC": tc, "BC": bc, "R0.5": (p + res[0]) / 2.0, "S0.5": (p + sup[0]) / 2.0}
    for n in range(5):
        priced[f"S{n + 1}"], priced[f"R{n + 1}"] = sup[n], res[n]
        if n + 1 < 5:
            priced[f"S{n + 1}.5"] = (sup[n] + sup[n + 1]) / 2.0
            priced[f"R{n + 1}.5"] = (res[n] + res[n + 1]) / 2.0
    return Levels(pivot=p, rungs={k: priced[k] for k in RUNG_ORDER})


def _levels_at(hi, lo, cl, step: float) -> "Levels":
    lv = _levels_from(hi, lo, cl)
    return lv if step >= 0.5 else Levels(pivot=lv.pivot, rungs=_finer(lv.rungs))


def daily_levels(daily: pd.DataFrame, step: float = 0.5) -> dict:
    """Each session's pivots, from the session before it. Keyed by session date."""
    out: dict = {}
    for i in range(1, len(daily)):
        prev = daily.iloc[i - 1]
        out[daily.index[i].date()] = _levels_at(prev["high"], prev["low"], prev["close"], step)
    return out


SESSION_START = time(9, 15)


BROKER_CACHE = "/Users/philipkumar/Documents/PhilForge/tools/.nifty_cache"


def broker_bars(bar_minutes: int = 5, root: str = BROKER_CACHE, underlying: str = "NIFTY") -> pd.DataFrame:
    """NIFTY candles as the broker recorded them, for an independent second opinion."""
    import glob
    import json

    frames = []
    for path in sorted(glob.glob(os.path.join(root, f"{underlying}_5m_*.json"))):
        rows = json.load(open(path))
        # Some of these files carry a sixth column (volume); take the OHLC and drop the rest.
        df = pd.DataFrame([r[:5] for r in rows], columns=["ts", "open", "high", "low", "close"])
        df["ts"] = pd.to_datetime(df["ts"])
        frames.append(df.set_index("ts"))
    if not frames:
        raise SystemExit(f"no broker candles under {root}")
    bars = pd.concat(frames).sort_index()
    bars = bars[~bars.index.duplicated(keep="first")]
    bars = bars[(bars.index.time >= time(9, 15)) & (bars.index.time <= time(15, 25))]
    if bar_minutes != 5:
        bars = (
            bars.resample(f"{bar_minutes}min", origin=pd.Timestamp("2021-01-01 09:15"))
            .agg({"open": "first", "high": "max", "low": "min", "close": "last"})
            .dropna()
        )
    return bars


def broker_daily(bars: pd.DataFrame) -> pd.DataFrame:
    g = bars.groupby(bars.index.normalize())
    return pd.DataFrame(
        {"open": g["open"].first(), "high": g["high"].max(), "low": g["low"].min(), "close": g["close"].last()}
    )


def block_levels(minute: pd.DataFrame, hours: float = 2.0, step: float = 0.5) -> dict:
    """Pivots drawn from the previous intraday block of `hours`, keyed by the
    start of the block they govern.

    Blocks are measured from 09:15, so a 2-hour CPR runs 09:15, 11:15, 13:15 and
    a short 15:15 stub. The first block of a day is governed by the last block of
    the day before -- the same way a daily CPR is governed by yesterday.
    """
    idx = minute.index
    day_open = pd.to_datetime(idx.normalize()) + pd.Timedelta(hours=SESSION_START.hour, minutes=SESSION_START.minute)
    span = pd.Timedelta(hours=hours)
    starts = day_open + ((idx - day_open) // span) * span
    grouped = minute.groupby(starts)
    agg = pd.DataFrame(
        {"high": grouped["high"].max(), "low": grouped["low"].min(), "close": grouped["close"].last()}
    ).sort_index()
    out: dict = {}
    for i in range(1, len(agg)):
        prev = agg.iloc[i - 1]
        out[agg.index[i]] = _levels_at(prev["high"], prev["low"], prev["close"], step)
    return out


def block_start(ts, hours: float = 2.0):
    day_open = ts.normalize() + pd.Timedelta(hours=SESSION_START.hour, minutes=SESSION_START.minute)
    return day_open + ((ts - day_open) // pd.Timedelta(hours=hours)) * pd.Timedelta(hours=hours)


def levels_by_day(daily: pd.DataFrame, basis: str, step: float = 0.5) -> dict:
    """One Levels per session date, whichever basis draws them."""
    if basis == "daily":
        return daily_levels(daily, step)
    weekly = weekly_levels(daily)
    if step < 0.5:
        weekly = {k: Levels(pivot=v.pivot, rungs=_finer(v.rungs)) for k, v in weekly.items()}
    return {
        d.date(): weekly[d.date() - timedelta(days=d.weekday())]
        for d in daily.index
        if (d.date() - timedelta(days=d.weekday())) in weekly
    }


def levels_on(week_starts: list, table: dict, day: date) -> Optional[Levels]:
    monday = day - timedelta(days=day.weekday())
    return table.get(monday)


# ------------------------------------------------------------ supertrend ----
def supertrend(bars: pd.DataFrame, period: int = 10, multiplier: float = 1.7) -> pd.DataFrame:
    """Supertrend, with Wilder's ATR -- the line and which side of it we are on.

    In a downtrend the line is the upper band, and it only ever ratchets down.
    So the line goes flat exactly when price stops making new lows, which is what
    makes "the second touch of the flat line" a double top against it.
    """
    h, low_, c = bars["high"], bars["low"], bars["close"]
    prev = c.shift(1)
    tr = pd.concat([h - low_, (h - prev).abs(), (low_ - prev).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    hl2 = (h + low_) / 2.0
    upper_basic, lower_basic = hl2 + multiplier * atr, hl2 - multiplier * atr

    ub = upper_basic.to_numpy(copy=True)
    lb = lower_basic.to_numpy(copy=True)
    close = c.to_numpy()
    line = np.full(len(bars), np.nan)
    down = np.zeros(len(bars), dtype=bool)  # True while the line sits above price

    started = False
    for i in range(len(bars)):
        if np.isnan(ub[i]) or np.isnan(lb[i]):
            continue
        if not started:
            line[i], down[i], started = ub[i], True, True
            continue
        if close[i - 1] <= ub[i - 1]:
            ub[i] = min(ub[i], ub[i - 1])
        if close[i - 1] >= lb[i - 1]:
            lb[i] = max(lb[i], lb[i - 1])
        if down[i - 1]:
            down[i] = close[i] <= ub[i]
        else:
            down[i] = close[i] < lb[i]
        line[i] = ub[i] if down[i] else lb[i]
    return pd.DataFrame({"line": line, "down": down}, index=bars.index)


# ----------------------------------------------------------------- trade ----
@dataclass
class Trade:
    entry_ts: datetime
    entry_spot: float
    entry_rung: str
    entry_bar_high: float
    entry_ref: float
    entry_bar_low: float
    entry_st: float
    entry_bar_close: float
    strike: int
    expiry: date
    lots: int
    lot: int
    entry_premium: float
    bc: float
    tc: float
    exit_ts: Optional[datetime] = None
    exit_spot: float = 0.0
    exit_premium: float = 0.0
    exit_reason: str = ""
    deepest: str = ""
    priced_exit: bool = True
    unpriceable: bool = False
    best: Optional[float] = None
    mfe: float = 0.0
    charges: float = 0.0
    notes: list = field(default_factory=list)

    @property
    def qty(self) -> int:
        return self.lots * self.lot

    @property
    def gross(self) -> float:
        return (self.exit_premium - self.entry_premium) * self.qty

    @property
    def net(self) -> float:
        return self.gross - self.charges

    @property
    def held_days(self) -> int:
        return (self.exit_ts.date() - self.entry_ts.date()).days if self.exit_ts else 0


@dataclass
class Contract:
    expiry: date
    strike: int
    option_type: str


# ------------------------------------------------------------------- run ----
class Backtest:
    def __init__(self, args):
        self.args = args
        minute = load_minutes()
        self.minute = minute
        wanted = [r.strip() for r in str(args.entry_rungs).split(",") if r.strip()]
        unknown = [r for r in wanted if r not in RUNG_ORDER]
        if unknown:
            raise SystemExit(f"unknown rung(s) {unknown}; pick from {RUNG_ORDER}")
        # Highest first, so a bar that reached through two of them is credited to
        # the one it was actually turned back from.
        order = (
            RUNG_ORDER
            if str(getattr(args, "side", "PE")).upper() == "PE" or not getattr(args, "mirror", False)
            else list(reversed(RUNG_ORDER))
        )
        self.entry_rungs = [r for r in order if r in wanted]
        self.bar_minutes = int(args.bar_minutes)
        if self.bar_minutes < 5 and not getattr(args, "allow_thin_bars", False):
            raise SystemExit(
                "This index is lifted from the `spot` stamped on option rows, and that field "
                "carries ONE value per minute: 98.9% of 1-minute bars have high == low. A rule "
                "that needs a wick inside the candle cannot be tested below 5 minutes on this "
                "data -- it will quietly find almost no signals and look like a verdict. "
                "5m bars are only 0.1% zero-range. Use --bar-minutes 5, or fetch real 1-minute "
                "index candles from the broker and point this at them. --allow-thin-bars "
                "overrides, for diagnostics only."
            )
        src = getattr(args, "index_source", "dhan-spot")
        if src == "broker":
            # The signals come from the broker's own candles and only the fills
            # come from the option tape. If a rule survives both index sources it
            # is not an artefact of how this index was reconstructed.
            self.bars = broker_bars(self.bar_minutes)
            self.daily = broker_daily(self.bars)
        else:
            self.bars = to_bars(minute, f"{self.bar_minutes}min")
        # An entry needs a minute after the signal bar to fill in, so the last
        # bar of a session cannot open a trade.
        # An entry opened after the square-off bell is closed on the next bar,
        # which is not a trade, it is a round trip's worth of charges.
        self.entry_cutoff = (
            datetime.combine(date(2000, 1, 1), SESSION_CLOSE) - timedelta(minutes=self.bar_minutes)
        ).time()
        sq0 = str(getattr(args, "square_off", "15:15")).split(":")
        if getattr(args, "intraday", False):
            self.entry_cutoff = min(self.entry_cutoff, time(int(sq0[0]), int(sq0[1])))
        self.daily = to_daily(minute)
        self.session_days = sessions(minute)
        self.weeklies = weekly_expiries(self.session_days)
        step = getattr(args, "ladder_step", 0.5)
        self.block_hours = float(getattr(args, "block_hours", 2.0))
        if args.pivots == "block":
            self.levels = block_levels(minute, self.block_hours, step)
        else:
            self.levels = levels_by_day(self.daily, args.pivots, step)
        self.minute_index = minute.index
        self.spot_at = minute["open"]

        index_close = {d.strftime("%Y-%m-%d"): float(c) for d, c in self.daily["close"].items()}
        self.source = DhanListedSource(self.weeklies, STORES, "NIFTY", nearest_within=0)
        for store in self.source.stores.values():
            if not hasattr(store, "dropped"):
                raise RuntimeError(
                    "options/dhan_listed.py here does not filter rows by the index's own level. "
                    "Dhan's expiryCode=2 series bleeds other underlyings into the NIFTY files, and "
                    "without that filter this book is priced partly off another instrument. "
                    "The filter is in the working tree of branch claude/dhan-options-backfill but "
                    "is not committed; commit it before trusting any number from here."
                )
            # PhilForge's index cache starts 2024-10; ours reaches back to 2021, so
            # the rows before that date get judged too instead of waved through.
            store.levels = index_close

        if args.entry_mode == "supertrend":
            st = supertrend(self.bars, args.st_period, args.st_multiplier)
            self.bars["st"], self.bars["st_down"] = st["line"], st["down"]
        span = max(2, args.ema)
        self.bars["ema"] = self.bars["close"].ewm(span=span, adjust=False).mean()
        self.side = getattr(args, "side", "PE").upper()
        # `side` is only which contract gets bought. `dirn` is which way the
        # levels are read, and it changes ONLY when --mirror is asked for.
        # Swapping the instrument and rewriting the exits are two different
        # experiments and must not be run as one.
        self.dirn = 1 if (self.side == "CE" and getattr(args, "mirror", False)) else -1
        self.breakout_levels = [
            x.strip().upper() for x in str(getattr(args, "breakout_levels", "TC,PDH")).split(",") if x.strip()
        ]
        prev = self.daily.shift(1)
        self.prev_high = {d.date(): float(v) for d, v in prev["high"].items() if v == v}
        self.prev_low = {d.date(): float(v) for d, v in prev["low"].items() if v == v}
        self._entry_ref = 0.0
        self._taken_today: dict = {}
        sq = str(getattr(args, "square_off", "15:15")).split(":")
        self.square_off = time(int(sq[0]), int(sq[1]))
        self._flat_level, self._touches = None, 0
        self.skipped_no_entry_price = 0
        self.skipped_too_dear = 0
        self.unpriced_exits = 0
        self.dropped_unpriceable = 0

    # -- plumbing ---------------------------------------------------------
    def next_minute(self, after: datetime, limit_sessions: int = 3) -> Optional[datetime]:
        i = self.minute_index.searchsorted(after, side="right")
        if i >= len(self.minute_index):
            return None
        cand = self.minute_index[i]
        if (cand.date() - after.date()).days > limit_sessions + 4:
            return None
        return cand

    def breakout_entry(self, lv: "Levels", bar, prev_close: float, day) -> Optional[str]:
        """The bar that takes price through EVERY named level at once.

        `--breakout-levels TC,PDH` means above the top of the CPR *and* above
        yesterday's high, whichever is higher. Only the crossing bar counts: the
        bar before it must not already have been through, or one breakout would
        be bought again on every bar that stays above it.
        """
        marks = []
        for name in self.breakout_levels:
            if name == "PDH":
                marks.append(self.prev_high.get(day))
            elif name == "PDL":
                marks.append(self.prev_low.get(day))
            else:
                marks.append(lv.rungs.get(name))
        if any(m is None for m in marks):
            return None
        line = max(marks) if self.dirn > 0 else min(marks)
        if not (self.dirn * (float(bar["close"]) - line) > 0 and self.dirn * (prev_close - line) <= 0):
            return None
        self._entry_ref = line
        return "BRK"

    def candle_share_entry(self, lv: "Levels", bar) -> Optional[str]:
        """A level the candle straddles, accepted rather than rejected.

        The test is how much of the candle's range sits on the trade's side of
        the level: more than the threshold and the level has been taken, which
        is a buy. Rungs are tried from the trade's own side outward, so a call
        is credited to the lowest level inside the candle -- the one it has
        cleared by the most.
        """
        high, low = float(bar["high"]), float(bar["low"])
        span = high - low
        if span <= 0:
            return None
        for label in self.entry_rungs:
            level = lv.rungs[label]
            if not (low <= level <= high):
                continue
            share = (high - level) / span if self.dirn > 0 else (level - low) / span
            if share > self.args.share_threshold:
                return label
        return None

    def supertrend_touch(self, k: int, bar) -> Optional[str]:
        """The nth touch of a flat supertrend, while the line is above price.

        The counter resets whenever the line moves, so the touches counted are
        always touches of the *same* flat level -- which is what the chart shows.
        """
        if not bool(bar.get("st_down")) or pd.isna(bar.get("st")):
            self._flat_level, self._touches = None, 0
            return None
        level = float(bar["st"])
        if self._flat_level is None or abs(level - self._flat_level) > 0.01:
            self._flat_level, self._touches = level, 0
            return None  # the bar that moved the line is not a touch of the new one
        if bar["high"] < level:
            return None
        self._touches += 1
        return "ST" if self._touches == self.args.st_touch else None

    def stop_level(self, trade: "Trade", lv: "Levels", stop_rung: Optional[str]) -> Optional[float]:
        level = self._raw_stop(trade, lv, stop_rung)
        if level is None:
            return None
        # A stop closer than this is not a stop, it is noise. Without a floor a
        # supertrend entry can land two points under a pivot and be stopped by a
        # tick -- which is what made the first supertrend run look like a losing
        # rule when it was really a losing stop.
        floor_ = trade.entry_spot - self.dirn * self.args.min_stop_points
        level = min(level, floor_) if self.dirn > 0 else max(level, floor_)
        cap = getattr(self.args, "max_stop_points", 0.0)
        if cap:
            # Without this, a breakout entry that fills far past the line it broke
            # keeps the LINE as its stop -- 413 points away at worst, which is not
            # a stop, it is the whole premium. min_stop widens, this caps.
            ceiling = trade.entry_spot - self.dirn * cap
            level = max(level, ceiling) if self.dirn > 0 else min(level, ceiling)
        return level

    def _raw_stop(self, trade: "Trade", lv: "Levels", stop_rung: Optional[str]) -> Optional[float]:
        """What a close has to clear to end the trade.

        `rung` is the geometry: the pivot directly above the one bought at.
        `entry-high` is the price action: the top of the candle that was rejected,
        which is a much tighter line -- it sits a few points above the rung
        entered on, where the rung above is a median 39 points away.
        """
        if self.args.stop == "st-line":
            return trade.entry_st or trade.entry_bar_high
        if self.args.stop == "entry-high":
            # The far side of the entry candle, whichever side that is: a put is
            # wrong above its high, a call is wrong below its low.
            return trade.entry_bar_high if self.dirn < 0 else trade.entry_bar_low
        if trade.entry_rung == "BRK":
            # A breakout is wrong when price closes back through the line it broke.
            return trade.entry_ref
        if trade.entry_rung == "ST":
            if self.args.stop != "rung":
                # The double top's own high: the trade is wrong when price closes
                # back through the level it was turned away from twice.
                return trade.entry_bar_high
            beyond = [pr for pr in lv.rungs.values() if self.dirn * (pr - trade.entry_spot) < 0]
            if not beyond:
                return trade.entry_bar_high if self.dirn < 0 else trade.entry_bar_low
            here = trade.entry_spot
            nearest = min(beyond) if self.dirn < 0 else max(beyond)
            return here + self.args.stop_fraction * (nearest - here)
        if self.args.stop == "entry-close":
            return trade.entry_bar_close
        if not stop_rung:
            return None if trade.entry_rung != "ST" else trade.entry_bar_high
        here, above = lv.rungs[trade.entry_rung], lv.rungs[stop_rung]
        # 1.0 puts the stop on the rung above; 0.5 puts it halfway there, which
        # on a half-step ladder is the quarter rung.
        return here + self.args.stop_fraction * (above - here)

    @property
    def bar_span(self) -> timedelta:
        """One minute short of the bar, so `next_minute` lands on the first
        minute after it closes rather than skipping a bar."""
        return timedelta(minutes=self.bar_minutes - 1)

    def last_minute_before(self, when: datetime) -> Optional[datetime]:
        i = self.minute_index.searchsorted(when, side="right") - 1
        if i < 0:
            return None
        cand = self.minute_index[i]
        return cand if cand.date() == when.date() else None

    def premium(self, when: datetime, contract: Contract) -> Optional[float]:
        return self.source.lookup(when, contract)

    def second_weekly(self, day: date) -> Optional[date]:
        from bisect import bisect_left

        i = bisect_left(self.weeklies, day) + 1
        return self.weeklies[i] if 0 <= i < len(self.weeklies) else None

    # -- the rule ---------------------------------------------------------
    def run(self) -> list:
        bars = self.bars
        trades: list = []
        open_trade: Optional[Trade] = None
        live_levels: Optional[Levels] = None
        stop_rung: Optional[str] = None
        deepest_i = -1
        active_level: Optional[tuple] = None

        stamps = bars.index
        for k in range(1, len(bars)):
            ts = stamps[k]
            bar = bars.iloc[k]
            day = ts.date()
            lv = (
                self.levels.get(block_start(ts, self.block_hours))
                if self.args.pivots == "block"
                else self.levels.get(day)
            )
            if lv is None:
                continue

            if open_trade is not None:
                use = live_levels if self.args.levels == "frozen" else lv
                exit_now, reason = None, ""

                if ts.time() >= self.square_off and (self.args.intraday or day == open_trade.expiry):
                    exit_now = self.last_minute_before(
                        ts.replace(hour=self.square_off.hour, minute=self.square_off.minute)
                    )
                    reason = "SQUARE_OFF" if self.args.intraday else "EXPIRY"
                elif (
                    self.stop_level(open_trade, use, stop_rung) is not None
                    and self.dirn * (bar["close"] - self.stop_level(open_trade, use, stop_rung)) < 0
                ):
                    exit_now = self.next_minute(ts + self.bar_span)
                    reason = f"STOP_ABOVE_{stop_rung}" if self.args.stop == "rung" else "STOP_ABOVE_ENTRY_CANDLE"
                elif (
                    self.args.trail_points
                    and open_trade.best is not None
                    and (self.dirn * (bar["close"] - (open_trade.best - self.dirn * self.args.trail_points)) < 0)
                ):
                    exit_now = self.next_minute(ts + self.bar_span)
                    reason = "TRAIL_POINTS"
                elif active_level is not None and self.dirn * (bar["close"] - active_level[1]) < 0:
                    exit_now, reason = self.next_minute(ts + self.bar_span), f"TRAIL_{active_level[0]}"

                if exit_now is not None:
                    self.close_trade(open_trade, exit_now, reason)
                    if not open_trade.unpriceable:
                        trades.append(open_trade)
                    open_trade, live_levels, active_level, deepest_i = None, None, None, -1
                    stop_rung = None
                elif reason:
                    open_trade.notes.append(f"{reason} at {ts} but no minute to fill in")
                else:
                    # A rung a few points from entry is not a target, it is a
                    # scratch: it arms the trail at once and the first tick back
                    # ends the trade. --min-trail-points skips those.
                    near = self.args.min_trail_points
                    if open_trade.entry_rung not in use.rungs:
                        ladder = [
                            (lb, pr) for lb, pr in use.rungs.items() if self.dirn * (pr - open_trade.entry_spot) > 0
                        ]
                    elif self.dirn < 0:
                        ladder = use.below(open_trade.entry_rung)
                    else:
                        ladder = use.rungs_above(open_trade.entry_rung)
                    if near:
                        ladder = [x for x in ladder if abs(x[1] - open_trade.entry_spot) >= near]
                    if self.dirn > 0:
                        # RUNG_ORDER runs high to low, so a call's targets come
                        # out farthest-first; the trail must walk the nearest one
                        # first or it never engages at all.
                        ladder = list(reversed(ladder))
                    for i in range(deepest_i + 1, len(ladder)):
                        reached = bar["high"] >= ladder[i][1] if self.dirn > 0 else bar["low"] <= ladder[i][1]
                        if reached:
                            deepest_i = i
                            open_trade.deepest = ladder[i][0]
                        else:
                            break
                    behind = deepest_i - self.args.trail_lag
                    active_level = ladder[behind] if behind >= 0 else None
                    far = float(bar["high"]) if self.dirn > 0 else float(bar["low"])
                    if open_trade.best is None or self.dirn * (far - open_trade.best) > 0:
                        open_trade.best = far
                    open_trade.mfe = max(open_trade.mfe, self.dirn * (far - open_trade.entry_spot))
                    continue

            if open_trade is not None:
                continue
            if self.args.max_trades_per_day and self._taken_today.get(day, 0) >= self.args.max_trades_per_day:
                continue
            if ts.time() >= self.entry_cutoff:
                continue  # nothing left of the session to fill an entry in

            prev_close = bars["close"].iloc[k - 1]
            if self.args.ema > 0 and self.dirn * (bar["close"] - bar["ema"]) <= 0:
                continue  # EMA20 vetoes a trade taken against the regime

            if self.args.entry_mode == "breakout":
                rung = self.breakout_entry(lv, bar, prev_close, day)
                if rung is None:
                    continue
            elif self.args.entry_mode == "candle-share":
                rung = self.candle_share_entry(lv, bar)
                if rung is None:
                    continue
            elif self.args.entry_mode == "supertrend":
                rung = self.supertrend_touch(k, bar)
                if rung is None:
                    continue
            else:
                # The highest rung the bar was rejected from: it must have been under
                # the rung already, reached up and touched it, and closed back below.
                rung = None
                for label in self.entry_rungs:
                    price = lv.rungs[label]
                    # It must already be on the trade's side of the rung, must
                    # reach across and touch it, and must close back on the
                    # trade's side. Written through `dirn` so a put reads it as
                    # a rejection from below and a call as a bounce from above.
                    reach = bar["high"] if self.dirn < 0 else bar["low"]
                    if (
                        self.dirn * (prev_close - price) > 0
                        and self.dirn * (reach - price) <= 0
                        and self.dirn * (bar["close"] - price) > 0
                    ):
                        rung = label
                        break
                if rung is None:
                    continue

            fill_ts = self.next_minute(ts + self.bar_span)
            if fill_ts is None or fill_ts.date() != day:
                continue
            expiry = self.second_weekly(day)
            if expiry is None:
                continue
            spot = float(self.spot_at.loc[fill_ts])
            strike = int(round(spot / STRIKE_STEP) * STRIKE_STEP) + self.args.strike_offset * int(STRIKE_STEP)
            contract = Contract(expiry=expiry, strike=strike, option_type=self.side)
            prem = self.premium(fill_ts, contract)
            if prem is not None and self.args.max_premium:
                # A trade you could not fund is a trade you did not take. Skipping
                # it is what sets the account size, so it belongs in the rule.
                if prem * self.args.lots * lot_size(expiry) > self.args.max_premium:
                    self.skipped_too_dear += 1
                    continue
            if prem is None:
                self.skipped_no_entry_price += 1
                continue

            open_trade = Trade(
                entry_ts=fill_ts,
                entry_spot=spot,
                entry_rung=rung,
                entry_bar_high=float(bar["high"]),
                entry_ref=float(self._entry_ref),
                entry_bar_low=float(bar["low"]),
                entry_st=float(bar["st"]) if "st" in bar.index and not pd.isna(bar["st"]) else 0.0,
                entry_bar_close=float(bar["close"]),
                strike=strike,
                expiry=expiry,
                lots=self.args.lots,
                lot=lot_size(expiry),
                entry_premium=prem,
                bc=lv.bc,
                tc=lv.tc,
            )
            self._taken_today[day] = self._taken_today.get(day, 0) + 1
            live_levels, active_level, deepest_i = lv, None, -1
            # A supertrend entry is not on a rung, so there is no rung above it;
            # stop_level() reads the nearest one off the entry price instead.
            if rung not in lv.rungs:
                stop_rung = None
            elif self.dirn < 0:
                nxt = lv.above(rung)
                stop_rung = nxt[0] if nxt else None
            else:
                nxt = lv.below(rung)
                stop_rung = nxt[0][0] if nxt else None

        if open_trade is not None:
            last = self.minute_index[-1]
            self.close_trade(open_trade, last, "OPEN_AT_END")
            trades.append(open_trade)
        return trades

    def close_trade(self, t: Trade, when: datetime, reason: str) -> None:
        contract = Contract(expiry=t.expiry, strike=t.strike, option_type=self.side)
        price = self.premium(when, contract)
        spot = float(self.spot_at.loc[when]) if when in self.spot_at.index else t.entry_spot
        if price is None:
            # Deep in the money, off the edge of Dhan's band. An in-the-money put
            # is worth its intrinsic value at minimum, so that is the floor -- and
            # it is flagged, because a floor is not a price.
            price = max(0.0, (spot - t.strike) if self.dirn > 0 else (t.strike - spot))
            t.priced_exit = False
            self.unpriced_exits += 1
            if price <= 0:
                # Out of the money with no quote. Intrinsic is zero, but the option
                # is NOT worth zero -- booking it as one invents a total loss out of
                # a missing tick. The trade is dropped and counted instead.
                t.unpriceable = True
                self.dropped_unpriceable += 1
        t.exit_ts, t.exit_spot, t.exit_premium, t.exit_reason = when, spot, price, reason
        slip = self.args.slippage_pct / 100.0
        buy = t.entry_premium * (1 + slip)
        sell = t.exit_premium * (1 - slip)
        t.entry_premium, t.exit_premium = buy, sell
        t.charges = round_trip_charges(
            trade_date=t.entry_ts.date(), buy_premium=buy, sell_premium=sell, quantity=t.qty
        ).total


# ---------------------------------------------------------------- report ----
def report(trades: list, bt: Backtest, args) -> str:
    import statistics

    if not trades:
        return "no trades"
    out = []
    net = [t.net for t in trades]
    wins = [t for t in trades if t.net > 0]
    total = sum(net)
    priced = [t for t in trades if t.priced_exit]
    floored = [t for t in trades if not t.priced_exit]

    eq, peak, dd = 0.0, 0.0, 0.0
    for t in trades:
        eq += t.net
        peak = max(peak, eq)
        dd = min(dd, eq - peak)

    out.append(f"trades              {len(trades):,}   {trades[0].entry_ts.date()} .. {trades[-1].entry_ts.date()}")
    out.append(f"net                 Rs {total:,.0f}")
    out.append(f"win rate            {len(wins) / len(trades):.1%}  ({len(wins)} of {len(trades)})")
    gp = sum(t.net for t in wins)
    gl = -sum(t.net for t in trades if t.net <= 0)
    out.append(f"profit factor       {gp / gl if gl else float('inf'):.2f}   (+Rs {gp:,.0f} / -Rs {gl:,.0f})")
    out.append(f"average trade       Rs {statistics.mean(net):,.0f}   median Rs {statistics.median(net):,.0f}")
    out.append(f"best / worst        Rs {max(net):,.0f} / Rs {min(net):,.0f}")
    out.append(f"max drawdown        Rs {dd:,.0f}")
    out.append(f"peak capital        Rs {max(t.entry_premium * t.qty for t in trades):,.0f}  (one lot's premium)")
    out.append(f"median hold         {statistics.median([t.held_days for t in trades])} days")
    out.append("")
    out.append(f"priced exits        {len(priced):,} of {len(trades):,}")
    if floored:
        out.append(
            f"floored at intrinsic {len(floored):,} exits  (Rs {sum(t.net for t in floored):,.0f} of the net) "
            f"-- these are the deep winners Dhan's ATM band does not carry"
        )
    out.append(f"entries skipped     {bt.skipped_no_entry_price:,} (no price at the entry minute)")
    if bt.dropped_unpriceable:
        out.append(
            f"trades dropped      {bt.dropped_unpriceable:,} (exit out of the money with no quote; "
            f"valuing them at zero would invent a total loss)"
        )
    out.append("")
    reasons: dict = {}
    for t in trades:
        r = t.exit_reason.split("_")[0] if t.exit_reason.startswith(("TRAIL", "STOP")) else t.exit_reason
        reasons.setdefault(r, []).append(t.net)
    out.append("by exit:")
    for r, xs in sorted(reasons.items(), key=lambda kv: -sum(kv[1])):
        out.append(f"  {r:<16} {len(xs):>4} trades   Rs {sum(xs):>12,.0f}   avg Rs {statistics.mean(xs):>9,.0f}")
    out.append("")
    out.append("by year:")
    years: dict = {}
    for t in trades:
        years.setdefault(t.entry_ts.year, []).append(t.net)
    for y in sorted(years):
        xs = years[y]
        w = sum(1 for x in xs if x > 0)
        out.append(f"  {y}   {len(xs):>3} trades   Rs {sum(xs):>12,.0f}   win {w / len(xs):>5.0%}")
    out.append("")
    out.append("deepest rung reached:")
    rungs: dict = {}
    for t in trades:
        rungs.setdefault(t.deepest or "-none-", []).append(t.net)
    order = ["-none-"] + RUNG_ORDER
    for r in order:
        if r in rungs:
            xs = rungs[r]
            out.append(f"  {r:<8} {len(xs):>4} trades   Rs {sum(xs):>12,.0f}")
    out.append("")
    out.append("archive: " + bt.source.report())
    return "\n".join(out)


def main() -> None:
    """The defaults ARE the rule that survived; every flag below is a road not taken."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)

    # --- the surviving rule -------------------------------------------------
    ap.add_argument(
        "--index-source",
        choices=["dhan-spot", "broker"],
        default="dhan-spot",
        help="which NIFTY candles drive the signal; fills always come from the option tape",
    )
    ap.add_argument("--max-trades-per-day", type=int, default=0, help="cap entries per session; 0 is no cap")
    ap.add_argument("--square-off", default="15:15", help="intraday exit time, HH:MM")
    ap.add_argument("--breakout-levels", default="TC,PDH", help="levels a breakout must clear together")
    ap.add_argument("--allow-thin-bars", action="store_true", help="permit sub-5-minute bars this data cannot support")
    ap.add_argument(
        "--block-hours", type=float, default=2.0, help="length of an intraday CPR block when --pivots block"
    )
    ap.add_argument(
        "--pivots",
        choices=["weekly", "daily", "block"],
        default="daily",
        help="draw the CPR from last week, yesterday, or the previous intraday block",
    )
    ap.add_argument(
        "--intraday",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="square off at 15:15; nothing is carried overnight",
    )
    ap.add_argument("--bar-minutes", type=int, default=5, help="signal timeframe in minutes")
    ap.add_argument("--entry-rungs", default="R2,R1", help="rungs a rejection may be bought at, comma separated")
    ap.add_argument("--ema", type=int, default=20, help="0 turns the EMA regime filter off")
    ap.add_argument(
        "--strike-offset", type=int, default=2, help="strikes from ATM; +2 is 100 points in the money for a put"
    )
    ap.add_argument(
        "--stop-fraction",
        type=float,
        default=0.5,
        help="how far from the entry rung to the rung above the stop sits; 0.5 is the quarter rung",
    )
    ap.add_argument(
        "--max-premium", type=float, default=25000.0, help="skip an entry costing more than this; 0 means no cap"
    )
    ap.add_argument("--lots", type=int, default=1)

    # --- roads not taken, each one measured and worse -----------------------
    ap.add_argument(
        "--side", choices=["PE", "CE"], default="PE", help="which contract the signal buys; exits are untouched"
    )
    ap.add_argument(
        "--mirror", action="store_true", help="additionally flip every target, stop and trail to the other side"
    )
    ap.add_argument(
        "--share-threshold",
        type=float,
        default=0.5,
        help="how much of the candle must sit on the trade's side of the level",
    )
    ap.add_argument(
        "--entry-mode",
        choices=["rung", "supertrend", "candle-share", "breakout"],
        default="rung",
        help="buy the rejection at a pivot rung, or the nth touch of a flat supertrend",
    )
    ap.add_argument("--st-period", type=int, default=10)
    ap.add_argument("--st-multiplier", type=float, default=1.7)
    ap.add_argument("--st-touch", type=int, default=2, help="which touch of the flat line to buy")
    ap.add_argument(
        "--ladder-step",
        type=float,
        default=0.5,
        help="0.25 halves every gap between R or S rungs, so the trail ratchets twice as often",
    )
    ap.add_argument("--stop", choices=["rung", "entry-high", "entry-close", "st-line"], default="rung")
    ap.add_argument(
        "--min-trail-points",
        type=float,
        default=0.0,
        help="ignore trail rungs nearer than this to entry; they scratch the trade",
    )
    ap.add_argument(
        "--trail-points",
        type=float,
        default=0.0,
        help="trail by a fixed number of points from the best price instead of by rungs",
    )
    ap.add_argument(
        "--max-stop-points", type=float, default=0.0, help="cap the stop distance from entry; 0 is uncapped"
    )
    ap.add_argument(
        "--min-stop-points", type=float, default=0.0, help="a floor under the stop distance, in index points"
    )
    ap.add_argument("--trail-lag", type=int, default=0, help="rungs the exit sits behind the deepest reached")
    ap.add_argument(
        "--levels",
        choices=["frozen", "rolling"],
        default="frozen",
        help="frozen keeps the entry period's ladder for the life of the trade",
    )

    # --- measurement --------------------------------------------------------
    ap.add_argument("--slippage-pct", type=float, default=0.0, help="adverse percent of premium, each leg")
    ap.add_argument("--from", dest="start", default=None)
    ap.add_argument("--to", dest="end", default=None)
    ap.add_argument("--csv", default=None)
    ap.add_argument(
        "--rule",
        choices=["pe-rejection", "ce-breakout"],
        default=None,
        help="a named, fixed rule; overrides the individual flags it owns",
    )
    args = ap.parse_args()

    # The two rules that survived, pinned. Anything not named here keeps its flag default.
    PRESETS = {
        "pe-rejection": dict(
            side="PE",
            mirror=False,
            entry_mode="rung",
            entry_rungs="R2,R1",
            strike_offset=2,
            stop_fraction=0.5,
            min_stop_points=0.0,
            max_trades_per_day=0,
            trail_lag=0,
        ),
        "ce-breakout": dict(
            side="CE",
            mirror=True,
            entry_mode="breakout",
            breakout_levels="TC,PDH",
            strike_offset=0,
            min_stop_points=60.0,
            max_stop_points=60.0,
            max_trades_per_day=1,
            trail_lag=0,
        ),
    }
    if args.rule:
        for k, v in PRESETS[args.rule].items():
            setattr(args, k, v)

    bt = Backtest(args)
    trades = bt.run()
    if args.start:
        trades = [t for t in trades if str(t.entry_ts.date()) >= args.start]
    if args.end:
        trades = [t for t in trades if str(t.entry_ts.date()) <= args.end]
    print(report(trades, bt, args))
    if args.csv:
        pd.DataFrame(
            [
                {
                    "entry_ts": t.entry_ts,
                    "exit_ts": t.exit_ts,
                    "strike": t.strike,
                    "expiry": t.expiry,
                    "lot": t.lot,
                    "entry_spot": round(t.entry_spot, 2),
                    "exit_spot": round(t.exit_spot, 2),
                    "entry_premium": round(t.entry_premium, 2),
                    "exit_premium": round(t.exit_premium, 2),
                    "deepest": t.deepest,
                    "exit_reason": t.exit_reason,
                    "priced_exit": t.priced_exit,
                    "charges": round(t.charges, 2),
                    "net": round(t.net, 2),
                    "mfe_points": round(t.mfe, 1),
                }
                for t in trades
            ]
        ).to_csv(args.csv, index=False)
        print(f"\nwrote {args.csv}")


if __name__ == "__main__":
    main()
