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
SQUARE_OFF = time(15, 15)
SESSION_CLOSE = time(15, 30)


# ---------------------------------------------------------------- levels ----
@dataclass(frozen=True)
class Levels:
    """One week's floor pivots, plus the half steps the target ladder walks."""

    pivot: float
    bc: float
    tc: float
    supports: dict  # label -> price, ordered shallow to deep

    @property
    def ladder(self) -> list:
        return list(self.supports.items())


def weekly_levels(daily: pd.DataFrame) -> dict:
    """Each week's CPR and pivots, computed from the week before it.

    TradingView's Traditional pivots, which is what the Indian CPR scripts draw.
    The half levels are midpoints of neighbouring pivots -- S0.5 sits between
    the pivot and S1, S1.5 between S1 and S2, and so on.
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
        s = [
            2 * p - hi,  # S1
            p - (hi - lo),  # S2
            2 * p - (2 * hi - lo),  # S3
            3 * p - (3 * hi - lo),  # S4
            4 * p - (4 * hi - lo),  # S5
        ]
        rungs = {"S0.5": (p + s[0]) / 2.0}
        for n in range(5):
            rungs[f"S{n + 1}"] = s[n]
            if n + 1 < 5:
                rungs[f"S{n + 1}.5"] = (s[n] + s[n + 1]) / 2.0
        out[wk.index[i].date()] = Levels(pivot=p, bc=bc, tc=tc, supports=rungs)
    return out


def levels_on(week_starts: list, table: dict, day: date) -> Optional[Levels]:
    monday = day - timedelta(days=day.weekday())
    return table.get(monday)


# ----------------------------------------------------------------- trade ----
@dataclass
class Trade:
    entry_ts: datetime
    entry_spot: float
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
        self.bar_minutes = int(args.bar_minutes)
        self.bars = to_bars(minute, f"{self.bar_minutes}min")
        # An entry needs a minute after the signal bar to fill in, so the last
        # bar of a session cannot open a trade.
        self.entry_cutoff = (
            datetime.combine(date(2000, 1, 1), SESSION_CLOSE) - timedelta(minutes=self.bar_minutes)
        ).time()
        self.daily = to_daily(minute)
        self.session_days = sessions(minute)
        self.weeklies = weekly_expiries(self.session_days)
        self.levels = weekly_levels(self.daily)
        self.minute_index = minute.index
        self.spot_at = minute["open"]

        levels_by_day = {d.strftime("%Y-%m-%d"): float(c) for d, c in self.daily["close"].items()}
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
            store.levels = levels_by_day

        span = max(2, args.ema)
        self.bars["ema"] = self.bars["close"].ewm(span=span, adjust=False).mean()
        self.skipped_no_entry_price = 0
        self.unpriced_exits = 0

    # -- plumbing ---------------------------------------------------------
    def next_minute(self, after: datetime, limit_sessions: int = 3) -> Optional[datetime]:
        i = self.minute_index.searchsorted(after, side="right")
        if i >= len(self.minute_index):
            return None
        cand = self.minute_index[i]
        if (cand.date() - after.date()).days > limit_sessions + 4:
            return None
        return cand

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
        deepest_i = -1
        active_level: Optional[tuple] = None

        stamps = bars.index
        for k in range(1, len(bars)):
            ts = stamps[k]
            bar = bars.iloc[k]
            day = ts.date()
            lv = levels_on(None, self.levels, day)
            if lv is None:
                continue

            if open_trade is not None:
                use = live_levels if self.args.levels == "frozen" else lv
                exit_now, reason = None, ""

                if day == open_trade.expiry and ts.time() >= SQUARE_OFF:
                    exit_now, reason = self.last_minute_before(ts.replace(hour=15, minute=15)), "EXPIRY"
                elif bar["close"] > use.tc:
                    exit_now, reason = self.next_minute(ts + self.bar_span), "STOP_ABOVE_CPR"
                elif active_level is not None and bar["close"] > active_level[1]:
                    exit_now, reason = self.next_minute(ts + self.bar_span), f"TRAIL_{active_level[0]}"

                if exit_now is not None:
                    self.close_trade(open_trade, exit_now, reason)
                    trades.append(open_trade)
                    open_trade, live_levels, active_level, deepest_i = None, None, None, -1
                elif reason:
                    open_trade.notes.append(f"{reason} at {ts} but no minute to fill in")
                else:
                    ladder = use.ladder
                    for i in range(deepest_i + 1, len(ladder)):
                        if bar["low"] <= ladder[i][1]:
                            deepest_i = i
                            open_trade.deepest = ladder[i][0]
                        else:
                            break
                    behind = deepest_i - self.args.trail_lag
                    active_level = ladder[behind] if behind >= 0 else None
                    open_trade.mfe = max(open_trade.mfe, open_trade.entry_spot - float(bar["low"]))
                    continue

            if open_trade is not None:
                continue
            if ts.time() >= self.entry_cutoff:
                continue  # nothing left of the session to fill an entry in

            prev_close = bars["close"].iloc[k - 1]
            if not (prev_close < lv.bc):  # the market must already be below the CPR
                continue
            if not (bar["high"] >= lv.bc):  # and it must come back up and tag the band
                continue
            if not (bar["close"] < lv.bc):  # and be rejected by it
                continue
            if self.args.ema > 0 and not (bar["close"] < bar["ema"]):
                continue  # EMA20 vetoes a PE in an up regime

            fill_ts = self.next_minute(ts + self.bar_span)
            if fill_ts is None or fill_ts.date() != day:
                continue
            expiry = self.second_weekly(day)
            if expiry is None:
                continue
            spot = float(self.spot_at.loc[fill_ts])
            strike = int(round(spot / STRIKE_STEP) * STRIKE_STEP) + self.args.strike_offset * int(STRIKE_STEP)
            contract = Contract(expiry=expiry, strike=strike, option_type="PE")
            prem = self.premium(fill_ts, contract)
            if prem is None:
                self.skipped_no_entry_price += 1
                continue

            open_trade = Trade(
                entry_ts=fill_ts,
                entry_spot=spot,
                strike=strike,
                expiry=expiry,
                lots=self.args.lots,
                lot=lot_size(expiry),
                entry_premium=prem,
                bc=lv.bc,
                tc=lv.tc,
            )
            live_levels, active_level, deepest_i = lv, None, -1

        if open_trade is not None:
            last = self.minute_index[-1]
            self.close_trade(open_trade, last, "OPEN_AT_END")
            trades.append(open_trade)
        return trades

    def close_trade(self, t: Trade, when: datetime, reason: str) -> None:
        contract = Contract(expiry=t.expiry, strike=t.strike, option_type="PE")
        price = self.premium(when, contract)
        spot = float(self.spot_at.loc[when]) if when in self.spot_at.index else t.entry_spot
        if price is None:
            # Deep in the money, off the edge of Dhan's band. An in-the-money put
            # is worth its intrinsic value at minimum, so that is the floor -- and
            # it is flagged, because a floor is not a price.
            price = max(0.0, t.strike - spot)
            t.priced_exit = False
            self.unpriced_exits += 1
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
    out.append("")
    reasons: dict = {}
    for t in trades:
        r = t.exit_reason.split("_")[0] if t.exit_reason.startswith("TRAIL") else t.exit_reason
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
    order = ["-none-", "S0.5", "S1", "S1.5", "S2", "S2.5", "S3", "S3.5", "S4", "S4.5", "S5"]
    for r in order:
        if r in rungs:
            xs = rungs[r]
            out.append(f"  {r:<8} {len(xs):>4} trades   Rs {sum(xs):>12,.0f}")
    out.append("")
    out.append("archive: " + bt.source.report())
    return "\n".join(out)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bar-minutes", type=int, default=15, help="signal timeframe in minutes; 15 or 5")
    ap.add_argument("--ema", type=int, default=20, help="0 turns the EMA regime filter off entirely")
    ap.add_argument("--lots", type=int, default=1)
    ap.add_argument("--strike-offset", type=int, default=0, help="strikes from ATM; -2 is 100 points out of the money")
    ap.add_argument("--slippage-pct", type=float, default=0.0, help="adverse percent of premium, each leg")
    ap.add_argument(
        "--levels",
        choices=["frozen", "rolling"],
        default="frozen",
        help="frozen keeps the entry week's ladder for the life of the trade",
    )
    ap.add_argument(
        "--trail-lag",
        type=int,
        default=0,
        help="rungs the exit sits behind the deepest reached; 0 exits on a close back above "
        "the rung just touched, 1 keeps one rung of room",
    )
    ap.add_argument("--from", dest="start", default=None)
    ap.add_argument("--to", dest="end", default=None)
    ap.add_argument("--csv", default=None)
    args = ap.parse_args()

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
