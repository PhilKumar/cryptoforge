"""EMA20 on NIFTY, expressed in weekly options, priced on two archives.

The rule, as given:

    EMA20 on the NIFTY chart. When a candle closes ABOVE the EMA and the EMA is
    rising steeply -- "at 30 degrees" -- buy a call, and hold it until a candle
    closes back below the EMA. Mirror it for puts: a close below a steeply
    falling EMA buys a put, held until a candle closes back above.

Entry and exit are the same line read twice, so this is a state machine: long
while the close is on one side of the EMA, flat or reversed when it crosses. The
only thing that can turn a crossing into a *trade* is the slope filter.


WHAT "30 DEGREES" HAD TO BECOME
-------------------------------
An angle on a chart is not a property of the market. Stretch the window and the
same EMA reads 15 degrees or 60. To measure the rule at all, the slope has to be
divided by something the market itself supplies, and the honest divisor is one
bar's worth of movement:

    tan(angle) = (EMA points gained per bar) / ATR(14)

Read that as a chart drawn so one bar of WIDTH is one ATR of HEIGHT. It is
scale-free, and -- measured over 522,205 minutes of NIFTY, 2021-2026 -- it is
very nearly timeframe-free too, which is the test a good normalisation has to
pass:

    | 5m | 15m | 30m | 1h |  |slope|/ATR percentiles
    | median 0.146 | 0.147 | 0.146 | 0.147 |   ->  8.4 degrees
    | p90    0.357 | 0.344 | 0.343 | 0.340 |   -> 19.0 degrees
    | p99    0.573 | 0.553 | 0.512 | 0.516 |   -> 28.5 degrees
    | max          |       | 0.970 | 0.844 |   -> 41.0 degrees

So 30 degrees is not off the scale -- it is roughly the steepest one reading in
150. That makes it a real filter and a very selective one, and the reason
``--sweep`` exists is that nobody should take my word for which angle Phil drew.
The sweep prints the whole curve and lets the number be chosen with the book in
view rather than the picture.

``--slope-basis points`` is the escape hatch: it ignores ATR and thresholds the
raw EMA gain per bar, for when the question is "how many points" and not "how
steep".


TWO ARCHIVES, TWO FAILURE MODES
-------------------------------
The same signals are priced twice, because neither archive can be trusted alone:

  dhan    2021-01 -> 2026-08. Deep. But keyed by MONEYNESS, about twelve strikes
          either side of the money, so a contract stops being quoted exactly when
          the trade is winning. Exits off the edge are floored at intrinsic value
          and reported separately -- a floor is not a price.

  upstox  2024-10 -> 2026-08 only. Real strikes, real contracts, quoted for their
          whole life. But it holds only the contracts PhilForge once fetched -- a
          median of 33 NIFTY strikes per expiry -- so a lookup can miss because
          the archive never held that strike, which says nothing about the market
          and is never filled in.

A rule that pays on one and not the other has not been measured. Run both.
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Optional

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from options.charges import round_trip_charges  # noqa: E402
from options.dhan_listed import DhanListedSource  # noqa: E402
from options.upstox_archive import DEFAULT_ROOT as UPSTOX_ROOT  # noqa: E402
from options.upstox_archive import UpstoxArchiveSource  # noqa: E402
from tools.nifty_expiry_calendar import STRIKE_STEP, lot_size, weekly_expiries  # noqa: E402
from tools.nifty_index_from_dhan import load_minutes, sessions, to_bars, to_daily  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STORES = {
    "e1": os.path.join(REPO, "data", "dhan_options"),
    "e2": os.path.join(REPO, "data", "dhan_options_e2"),
    "m1": os.path.join(REPO, "data", "dhan_options_m1"),
    "m2": os.path.join(REPO, "data", "dhan_options_m2"),
}
SESSION_CLOSE = time(15, 30)


# ------------------------------------------------------------------ math ----
def true_range(bars: pd.DataFrame) -> pd.Series:
    pc = bars["close"].shift(1)
    return pd.concat([bars["high"] - bars["low"], (bars["high"] - pc).abs(), (bars["low"] - pc).abs()], axis=1).max(
        axis=1
    )


def slope_angle(bars: pd.DataFrame, ema: pd.Series, lookback: int, basis: str, unit: float, atr_period: int):
    """Signed degrees. Positive is a rising EMA.

    ``atr`` basis divides by one bar's average range, which is the scale-free
    reading described in the module docstring. A zero-range window -- the option
    tape does produce them -- would divide by nothing and report a vertical EMA,
    so those bars are given no angle at all rather than an infinite one.
    """
    gain = (ema - ema.shift(lookback)) / float(lookback)
    if basis == "atr":
        div = true_range(bars).rolling(atr_period).mean()
        div = div.where(div > 0)
    else:
        div = pd.Series(float(unit), index=bars.index)
    return pd.Series([math.degrees(math.atan(x)) if x == x else float("nan") for x in (gain / div)], index=bars.index)


def rsi(bars: pd.DataFrame, period: int = 14) -> pd.Series:
    """Wilder's RSI, smoothed the way Wilder smoothed it (alpha = 1/period), which
    is what every charting package draws. A simple rolling mean here would read a
    few points different at exactly the thresholds a rule keys on."""
    delta = bars["close"].diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean()
    out = 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)
    # No losing bars in the window is RSI 100, not a division by zero.
    return out.where(avg_loss > 0, 100.0).where(avg_gain > 0, other=out.where(avg_gain > 0, 0.0))


# ----------------------------------------------------------------- trade ----
@dataclass
class Trade:
    side: str
    entry_ts: datetime
    entry_spot: float
    entry_angle: float
    entry_ema: float
    entry_bar_close: float
    strike: int
    expiry: date
    lots: int
    lot: int
    entry_premium: float
    exit_ts: Optional[datetime] = None
    exit_spot: float = 0.0
    exit_premium: float = 0.0
    exit_reason: str = ""
    priced_exit: bool = True
    unpriceable: bool = False
    exit_offset: int = 0
    mfe: float = 0.0
    bars_held: int = 0
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


class HybridSource:
    """Dhan first, Upstox second, and a count of who answered.

    The two archives fail in opposite directions -- Dhan stops quoting a strike
    that leaves the ATM band, Upstox only ever held the strikes PhilForge once
    fetched -- so asking both covers more ground than either alone. It is not a
    cure: on NIFTY the Upstox archive carries a median of 33 strikes per expiry
    and as few as one, so a contract Dhan lost to a large move is often missing
    from Upstox too. The counters exist so a run can say how much was really
    rescued instead of implying it was all of it.
    """

    def __init__(self, dhan, upstox):
        self.dhan, self.upstox = dhan, upstox
        self.by_dhan = 0
        self.by_upstox = 0
        self.missed = 0

    def lookup(self, when, contract):
        px = self.dhan.lookup(when, contract)
        if px is not None:
            self.by_dhan += 1
            return px
        px = self.upstox.lookup(when, contract)
        if px is not None:
            self.by_upstox += 1
            return px
        self.missed += 1
        return None

    def lookup_forward(self, when, contract, minutes=15):
        px = self.lookup(when, contract)
        if px is not None:
            return px, 0
        return self.upstox.lookup_forward(when, contract, minutes)

    def report(self):
        total = self.by_dhan + self.by_upstox + self.missed
        if not total:
            return "no lookups"
        return (
            f"{self.by_dhan + self.by_upstox:,}/{total:,} served "
            f"({self.by_dhan:,} by Dhan, {self.by_upstox:,} rescued from Upstox, {self.missed:,} by neither)"
        )


# ------------------------------------------------------------------- run ----
class Backtest:
    def __init__(self, args):
        self.args = args
        minute = load_minutes()
        self.minute = minute
        self.bar_minutes = int(args.bar_minutes)
        if self.bar_minutes < 5 and not getattr(args, "allow_thin_bars", False):
            raise SystemExit(
                "This index is lifted from the `spot` stamped on option rows, and that field carries "
                "ONE value per minute: 98.9% of 1-minute bars have high == low. 5m bars are only 0.1% "
                "zero-range. Use --bar-minutes 5 or higher, or point this at real 1-minute index "
                "candles. --allow-thin-bars overrides, for diagnostics only."
            )
        self.bars = to_bars(minute, f"{self.bar_minutes}min")
        self.daily = to_daily(minute)
        self.session_days = sessions(minute)
        self.weeklies = weekly_expiries(self.session_days)
        self.minute_index = minute.index
        self.spot_at = minute["open"]

        ema = self.bars["close"].ewm(span=max(2, int(args.ema)), adjust=False).mean()
        self.bars["ema"] = ema
        self.bars["rsi"] = rsi(self.bars, max(2, int(args.rsi_period)))
        self.bars["angle"] = slope_angle(
            self.bars, ema, max(1, int(args.slope_bars)), args.slope_basis, args.slope_unit, int(args.atr_period)
        )

        self.pricing = args.pricing
        if self.pricing == "hybrid":
            index_close = {d.strftime("%Y-%m-%d"): float(c) for d, c in self.daily["close"].items()}
            dh = DhanListedSource(self.weeklies, STORES, "NIFTY", nearest_within=0)
            for store in dh.stores.values():
                store.levels = index_close
            self.source = HybridSource(dh, UpstoxArchiveSource(args.upstox_root, "NIFTY"))
            self.priceable_expiries = None
        elif self.pricing == "upstox":
            self.source = UpstoxArchiveSource(args.upstox_root, "NIFTY")
            if not self.source.expiries():
                raise SystemExit(f"no Upstox archive under {args.upstox_root}")
            # The archive only holds the expiries PhilForge once fetched. An
            # expiry it never held is not tradeable here, and pretending
            # otherwise would price the trade on the wrong contract.
            self.priceable_expiries = set(self.source.expiries())
        else:
            index_close = {d.strftime("%Y-%m-%d"): float(c) for d, c in self.daily["close"].items()}
            self.source = DhanListedSource(self.weeklies, STORES, "NIFTY", nearest_within=0)
            for store in self.source.stores.values():
                if not hasattr(store, "dropped"):
                    raise RuntimeError(
                        "options/dhan_listed.py here does not filter rows by the index's own level. "
                        "Dhan's expiryCode=2 series bleeds other underlyings into the NIFTY files. "
                        "Commit that filter before trusting any number from here."
                    )
                store.levels = index_close
            self.priceable_expiries = None

        self.sides = ["CE", "PE"] if args.side == "both" else [args.side]
        sq = str(args.square_off).split(":")
        self.square_off = time(int(sq[0]), int(sq[1]))
        self.entry_cutoff = (
            datetime.combine(date(2000, 1, 1), SESSION_CLOSE) - timedelta(minutes=self.bar_minutes)
        ).time()
        if args.intraday:
            self.entry_cutoff = min(self.entry_cutoff, self.square_off)

        ea = str(getattr(args, "entry_after", "09:16")).split(":")
        self.entry_after = time(int(ea[0]), int(ea[1]))
        self.skipped_too_early = 0
        self.skipped_too_cheap = 0
        self.skipped_no_entry_price = 0
        self.skipped_too_dear = 0
        self.skipped_no_expiry = 0
        self.unpriced_exits = 0
        self.dropped_unpriceable = 0
        self.signals = {"CE": 0, "PE": 0}
        self.vetoed_by_angle = {"CE": 0, "PE": 0}
        self.vetoed_by_rsi = {"CE": 0, "PE": 0}

    # -- plumbing ---------------------------------------------------------
    @property
    def bar_span(self) -> timedelta:
        return timedelta(minutes=self.bar_minutes - 1)

    def next_minute(self, after: datetime, limit_sessions: int = 3) -> Optional[datetime]:
        i = self.minute_index.searchsorted(after, side="right")
        if i >= len(self.minute_index):
            return None
        cand = self.minute_index[i]
        if (cand.date() - after.date()).days > limit_sessions + 4:
            return None
        return cand

    def last_minute_before(self, when: datetime) -> Optional[datetime]:
        i = self.minute_index.searchsorted(when, side="right") - 1
        if i < 0:
            return None
        cand = self.minute_index[i]
        return cand if cand.date() == when.date() else None

    def nth_weekly(self, day: date, n: int) -> Optional[date]:
        from bisect import bisect_left

        i = bisect_left(self.weeklies, day) + n - 1
        return self.weeklies[i] if 0 <= i < len(self.weeklies) else None

    def expiry_for(self, day: date) -> Optional[date]:
        e = self.nth_weekly(day, int(self.args.expiry))
        if e is None:
            return None
        if self.priceable_expiries is not None and e not in self.priceable_expiries:
            return None
        return e

    def premium(self, when: datetime, contract: Contract) -> tuple:
        if self.pricing == "upstox" and self.args.exit_search_minutes:
            return self.source.lookup_forward(when, contract, int(self.args.exit_search_minutes))
        return self.source.lookup(when, contract), 0

    # -- the rule ---------------------------------------------------------
    def reset_counters(self) -> None:
        """Everything run() accumulates, zeroed in one place.

        A sweep reuses one Backtest across many thresholds. Resetting some
        counters and not others is worse than resetting none: the tallies that
        get missed keep summing across every pass, so a coverage or skip line
        added to the sweep later would quietly report angle 30's numbers as the
        total of every angle before it.
        """
        self.signals = {"CE": 0, "PE": 0}
        self.vetoed_by_angle = {"CE": 0, "PE": 0}
        self.vetoed_by_rsi = {"CE": 0, "PE": 0}
        self.skipped_too_early = 0
        self.skipped_too_cheap = 0
        self.skipped_no_entry_price = 0
        self.skipped_too_dear = 0
        self.skipped_no_expiry = 0
        self.unpriced_exits = 0
        self.dropped_unpriceable = 0
        src = self.source
        for attr, blank in (
            ("served", 0),
            ("served_exact", 0),
            ("served_nearby", 0),
            ("by_dhan", 0),
            ("by_upstox", 0),
            ("missed", 0),
        ):
            if hasattr(src, attr):
                setattr(src, attr, blank)
        if hasattr(src, "misses"):
            src.misses = dict.fromkeys(src.misses, 0)
        if hasattr(src, "missed_at"):
            src.missed_at = set()
        if hasattr(src, "nearby_offsets"):
            src.nearby_offsets = []

    def run(self) -> list:
        self.reset_counters()
        bars = self.bars
        trades: list = []
        open_trade: Optional[Trade] = None
        armed: Optional[tuple] = None  # (side, bars_since_cross), for --entry armed

        stamps = bars.index
        for k in range(1, len(bars)):
            ts, bar = stamps[k], bars.iloc[k]
            prev = bars.iloc[k - 1]
            day = ts.date()
            if self.args.arm_same_day and armed is not None and armed[2] != day:
                # A cross that armed yesterday firing on today's open is not this
                # rule, it is a gap trade wearing its clothes. Overnight arming is
                # where 70% of the profit was hiding.
                armed = None
            close, ema, angle = float(bar["close"]), float(bar["ema"]), float(bar["angle"])
            if ema != ema:
                continue

            # ---- exit: a candle that closes back through the line -------
            if open_trade is not None:
                open_trade.bars_held += 1
                want = 1 if open_trade.side == "CE" else -1
                exit_now, reason = None, ""
                # How far the index went for and against the trade INSIDE this bar.
                favour = float(bar["high"]) if want > 0 else float(bar["low"])
                against = float(bar["low"]) if want > 0 else float(bar["high"])
                if ts.time() >= self.square_off and (self.args.intraday or day == open_trade.expiry):
                    exit_now = self.last_minute_before(
                        ts.replace(hour=self.square_off.hour, minute=self.square_off.minute)
                    )
                    reason = "SQUARE_OFF" if self.args.intraday else "EXPIRY"
                elif self.args.stop_points and want * (against - open_trade.entry_spot) <= -self.args.stop_points:
                    # Checked BEFORE the target: if one bar contains both, assume
                    # the bad one came first. A backtest that assumes otherwise is
                    # paying itself for a coin flip.
                    exit_now, reason = self.next_minute(ts + self.bar_span), "STOP_POINTS"
                elif (
                    self.args.trail_points
                    and open_trade.mfe > self.args.trail_points
                    and want * (against - open_trade.entry_spot) <= open_trade.mfe - self.args.trail_points
                ):
                    # Give back this many points from the best level the INDEX
                    # reached, and the trade is over. `mfe` only counts bars that
                    # have closed, so the trail ratchets a bar behind the high --
                    # it cannot see an extreme and sell it in the same candle.
                    exit_now, reason = self.next_minute(ts + self.bar_span), "TRAIL_POINTS"
                elif self.args.target_points and want * (favour - open_trade.entry_spot) >= self.args.target_points:
                    # The bar REACHED the target; the fill is the next minute after
                    # it closes, not the touch itself. That is conservative -- the
                    # index can and does come back before the option is sold.
                    exit_now, reason = self.next_minute(ts + self.bar_span), "TARGET_POINTS"
                elif want * (close - ema) < 0:
                    exit_now = self.next_minute(ts + self.bar_span)
                    reason = "CLOSE_THROUGH_EMA"
                elif self.args.max_hold_days and (day - open_trade.entry_ts.date()).days >= self.args.max_hold_days:
                    exit_now, reason = self.next_minute(ts + self.bar_span), "MAX_HOLD"

                if exit_now is not None:
                    self.close_trade(open_trade, exit_now, reason)
                    if not open_trade.unpriceable:
                        trades.append(open_trade)
                    open_trade, armed = None, None
                elif reason:
                    open_trade.notes.append(f"{reason} at {ts} but no minute to fill in")
                    continue
                else:
                    far = float(bar["high"]) if open_trade.side == "CE" else float(bar["low"])
                    open_trade.mfe = max(open_trade.mfe, want * (far - open_trade.entry_spot))
                    continue

            # ---- entry: the cross, or the first qualifying bar after it --
            if open_trade is not None:
                continue
            side = None
            for cand in self.sides:
                want = 1 if cand == "CE" else -1
                on_side = want * (close - ema) > 0
                crossed = on_side and want * (float(prev["close"]) - float(prev["ema"])) <= 0
                if crossed:
                    self.signals[cand] += 1
                    armed = (cand, 0, day)
                if self.args.entry == "cross":
                    fires = crossed
                else:
                    # A cross that was not steep enough yet stays armed for a few
                    # bars: the trend is allowed to prove itself before the trade
                    # is refused. It disarms the moment price falls back through.
                    fires = (
                        armed is not None
                        and armed[0] == cand
                        and on_side
                        and (not self.args.arm_bars or armed[1] <= self.args.arm_bars)
                    )
                if not fires:
                    continue
                if angle != angle:
                    continue
                if self.args.rsi_min:
                    # Read as a mirror: a call wants RSI at or above the level, a
                    # put wants it at or below the reflection of that level, so
                    # --rsi-min 60 is "60+ for CE, 40- for PE".
                    r = float(bar["rsi"])
                    if r != r:
                        continue
                    if want > 0 and r < self.args.rsi_min:
                        self.vetoed_by_rsi[cand] += 1
                        continue
                    if want < 0 and r > 100.0 - self.args.rsi_min:
                        self.vetoed_by_rsi[cand] += 1
                        continue
                if want * angle < self.args.min_angle:
                    if crossed:
                        self.vetoed_by_angle[cand] += 1
                    continue
                side = cand
                break
            if armed is not None:
                sgn = 1 if armed[0] == "CE" else -1
                armed = (armed[0], armed[1] + 1, armed[2]) if sgn * (close - ema) > 0 else None
            if side is None:
                continue
            if ts.time() >= self.entry_cutoff:
                continue

            fill_ts = self.next_minute(ts + self.bar_span)
            if fill_ts is None or fill_ts.date() != day:
                continue
            if fill_ts.time() < self.entry_after:
                # The session's opening print is the one bar this reconstructed
                # index cannot be trusted on, and it is also where the gap lives.
                # A rule that quietly earns most of its money there is a gap rule
                # wearing a momentum rule's clothes.
                self.skipped_too_early += 1
                continue
            expiry = self.expiry_for(day)
            if expiry is None:
                self.skipped_no_expiry += 1
                continue
            if fill_ts.time() >= self.square_off and (self.args.intraday or fill_ts.date() == expiry):
                # The signal bar cleared the cutoff but the fill lands on the
                # square-off minute itself, so the trade would open and close in
                # the same minute: a round trip's charges and no position.
                #
                # It has to be THIS trade's expiry, not the nearest weekly. With
                # --expiry 2 the two are different days, and keying on the nearest
                # one threw away signals that would have been carried overnight
                # to a contract expiring a week later.
                continue
            spot = float(self.spot_at.loc[fill_ts])
            want = 1 if side == "CE" else -1
            # In the money is a LOWER strike for a call and a HIGHER one for a
            # put, so the offset has to be applied against the trade's
            # direction. Getting this backwards buys the cheap OTM wing and
            # calls it delta.
            strike = int(round(spot / STRIKE_STEP) * STRIKE_STEP) - want * self.args.strike_offset * int(STRIKE_STEP)
            contract = Contract(expiry=expiry, strike=strike, option_type=side)
            prem, _ = self.premium(fill_ts, contract)
            if prem is None:
                self.skipped_no_entry_price += 1
                continue
            if self.args.min_premium and prem < self.args.min_premium:
                self.skipped_too_cheap += 1
                continue
            if self.args.max_premium and prem * self.args.lots * lot_size(expiry) > self.args.max_premium:
                # A trade you could not fund is a trade you did not take.
                self.skipped_too_dear += 1
                continue

            open_trade = Trade(
                side=side,
                entry_ts=fill_ts,
                entry_spot=spot,
                entry_angle=angle,
                entry_ema=ema,
                entry_bar_close=close,
                strike=strike,
                expiry=expiry,
                lots=self.args.lots,
                lot=lot_size(expiry),
                entry_premium=prem,
            )
            armed = None

        if open_trade is not None:
            self.close_trade(open_trade, self.minute_index[-1], "OPEN_AT_END")
            if not open_trade.unpriceable:
                trades.append(open_trade)
        return trades

    def close_trade(self, t: Trade, when: datetime, reason: str) -> None:
        contract = Contract(expiry=t.expiry, strike=t.strike, option_type=t.side)
        price, offset = self.premium(when, contract)
        spot = float(self.spot_at.loc[when]) if when in self.spot_at.index else t.entry_spot
        if price is None:
            # Off the edge of the archive. An in-the-money option is worth its
            # intrinsic value at minimum, so that is the floor -- and it is
            # flagged, because a floor is not a price.
            price = max(0.0, (spot - t.strike) if t.side == "CE" else (t.strike - spot))
            t.priced_exit = False
            self.unpriced_exits += 1
            if price <= 0:
                # Out of the money with no quote. The option is NOT worth zero;
                # booking it as zero invents a total loss out of a missing tick.
                t.unpriceable = True
                self.dropped_unpriceable += 1
        t.exit_ts, t.exit_spot, t.exit_premium, t.exit_reason = when, spot, price, reason
        t.exit_offset = offset
        slip = self.args.slippage_pct / 100.0
        buy = t.entry_premium * (1 + slip)
        sell = t.exit_premium * (1 - slip)
        t.entry_premium, t.exit_premium = buy, sell
        t.charges = round_trip_charges(
            trade_date=t.entry_ts.date(), buy_premium=buy, sell_premium=sell, quantity=t.qty
        ).total


# ---------------------------------------------------------------- report ----
def summary(trades: list) -> dict:
    import statistics

    net = [t.net for t in trades]
    wins = [t for t in trades if t.net > 0]
    gp = sum(t.net for t in wins)
    gl = -sum(t.net for t in trades if t.net <= 0)
    eq, peak, dd = 0.0, 0.0, 0.0
    for t in trades:
        eq += t.net
        peak = max(peak, eq)
        dd = min(dd, eq - peak)
    return {
        "n": len(trades),
        "net": sum(net),
        "win": len(wins) / len(trades),
        "pf": gp / gl if gl else float("inf"),
        "avg": statistics.mean(net),
        "dd": dd,
        "capital": max(t.entry_premium * t.qty for t in trades),
        "priced": sum(1 for t in trades if t.priced_exit),
        "priced_net": sum(t.net for t in trades if t.priced_exit),
    }


def report(trades: list, bt: Backtest, args) -> str:
    import statistics

    out = []
    out.append(
        f"rule                {args.bar_minutes}m NIFTY, EMA{args.ema}, close through the line, "
        f"angle >= {args.min_angle:g} deg ({args.slope_basis} basis, {args.slope_bars}-bar slope)"
    )
    out.append(
        f"contract            {'ATM' if not args.strike_offset else f'ATM{args.strike_offset:+d} strikes ITM'}"
        f", weekly expiry #{args.expiry}, {args.lots} lot, "
        f"{'squared off ' + args.square_off if args.intraday else 'held overnight'}"
    )
    out.append(f"pricing             {args.pricing}")
    out.append("")
    crosses = bt.signals["CE"] + bt.signals["PE"]
    vetoed = bt.vetoed_by_angle["CE"] + bt.vetoed_by_angle["PE"]
    out.append(
        f"EMA crosses         {crosses:,}  (CE {bt.signals['CE']:,} / PE {bt.signals['PE']:,});  "
        f"{vetoed:,} refused by the angle filter"
    )
    if not trades:
        out.append("")
        out.append("no trades")
        out.append("archive: " + bt.source.report())
        return "\n".join(out)

    s = summary(trades)
    out.append(f"trades              {s['n']:,}   {trades[0].entry_ts.date()} .. {trades[-1].entry_ts.date()}")
    out.append(f"net                 Rs {s['net']:,.0f}")
    out.append(f"win rate            {s['win']:.1%}  ({sum(1 for t in trades if t.net > 0)} of {s['n']})")
    gp = sum(t.net for t in trades if t.net > 0)
    gl = -sum(t.net for t in trades if t.net <= 0)
    out.append(f"profit factor       {s['pf']:.2f}   (+Rs {gp:,.0f} / -Rs {gl:,.0f})")
    net = [t.net for t in trades]
    out.append(f"average trade       Rs {s['avg']:,.0f}   median Rs {statistics.median(net):,.0f}")
    out.append(f"best / worst        Rs {max(net):,.0f} / Rs {min(net):,.0f}")
    out.append(f"max drawdown        Rs {s['dd']:,.0f}")
    out.append(f"peak capital        Rs {s['capital']:,.0f}  (one entry's premium)")
    out.append(
        f"median hold         {statistics.median([t.bars_held for t in trades]):.0f} bars"
        f"   ({statistics.median([t.held_days for t in trades])} days)"
    )
    top3 = sorted(net, reverse=True)[:3]
    out.append(
        f"top 3 trades        Rs {sum(top3):,.0f}  ({sum(top3) / s['net']:.0%} of net; without them "
        f"Rs {s['net'] - sum(top3):,.0f})"
    )
    out.append("")
    floored = [t for t in trades if not t.priced_exit]
    out.append(f"priced exits        {s['priced']:,} of {s['n']:,}   Rs {s['priced_net']:,.0f} of the net")
    if floored:
        out.append(
            f"floored at intrinsic {len(floored):,} exits  (Rs {sum(t.net for t in floored):,.0f}) "
            f"-- off the edge of the archive's strike band, valued at intrinsic, which UNDERSTATES them"
        )
    if bt.dropped_unpriceable:
        out.append(
            f"trades dropped      {bt.dropped_unpriceable:,} (exit out of the money with no quote; "
            f"valuing them at zero would invent a total loss)"
        )
    nearby = sum(1 for t in trades if t.exit_offset)
    if nearby:
        out.append(f"exits from a nearby minute {nearby:,} (same contract, next print inside the session)")
    out.append(
        f"entries skipped     {bt.skipped_too_early:,} before {args.entry_after}"
        f" · {bt.skipped_too_cheap:,} under the Rs {args.min_premium:g} premium floor"
    )
    out.append(
        f"                    {bt.skipped_no_entry_price:,} no price"
        f" · {bt.skipped_too_dear:,} over the premium cap"
        f" · {bt.skipped_no_expiry:,} no expiry this archive holds"
    )
    out.append("")

    def table(title, keyfn, order=None):
        out.append(f"{title}:")
        groups: dict = {}
        for t in trades:
            groups.setdefault(keyfn(t), []).append(t)
        keys = order if order else sorted(groups, key=lambda k: -sum(x.net for x in groups[k]))
        for k in keys:
            if k not in groups:
                continue
            xs = groups[k]
            w = sum(1 for t in xs if t.net > 0)
            out.append(
                f"  {str(k):<18} {len(xs):>4} trades   Rs {sum(t.net for t in xs):>12,.0f}"
                f"   win {w / len(xs):>4.0%}   avg Rs {sum(t.net for t in xs) / len(xs):>9,.0f}"
            )
        out.append("")

    table("by side", lambda t: t.side, ["CE", "PE"])
    table("by exit", lambda t: t.exit_reason)
    table("by year", lambda t: t.entry_ts.year, sorted({t.entry_ts.year for t in trades}))
    buckets = [(0, 10), (10, 15), (15, 20), (20, 25), (25, 30), (30, 90)]
    labels = {b: f"{b[0]}-{b[1]} deg" for b in buckets}

    def bucket(t):
        a = abs(t.entry_angle)
        for b in buckets:
            if b[0] <= a < b[1]:
                return labels[b]
        return "?"

    table("by entry angle", bucket, [labels[b] for b in buckets])
    out.append("archive: " + bt.source.report())
    return "\n".join(out)


def sweep(bt: Backtest, args, angles: list) -> str:
    out = [
        f"{'angle':>7} {'trades':>7} {'net':>13} {'win':>6} {'PF':>6} {'maxDD':>11} "
        f"{'avg':>9} {'priced':>7} {'priced net':>13}"
    ]
    keep = args.min_angle
    for a in angles:
        args.min_angle = a
        bt.args.min_angle = a
        trades = bt.run()
        trades = window(trades, args)
        if not trades:
            out.append(f"{a:>6.0f}d {0:>7}")
            continue
        s = summary(trades)
        out.append(
            f"{a:>6.0f}d {s['n']:>7,} {s['net']:>13,.0f} {s['win']:>5.0%} {s['pf']:>6.2f} "
            f"{s['dd']:>11,.0f} {s['avg']:>9,.0f} {s['priced']:>7,} {s['priced_net']:>13,.0f}"
        )
    args.min_angle = keep
    bt.args.min_angle = keep
    return "\n".join(out)


def window(trades: list, args) -> list:
    if args.start:
        trades = [t for t in trades if str(t.entry_ts.date()) >= args.start]
    if args.end:
        trades = [t for t in trades if str(t.entry_ts.date()) <= args.end]
    return trades


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)

    # --- the rule -----------------------------------------------------------
    ap.add_argument("--bar-minutes", type=int, default=15, help="signal timeframe in minutes")
    ap.add_argument("--ema", type=int, default=20, help="EMA span, in bars")
    ap.add_argument("--min-angle", type=float, default=30.0, help="how steep the EMA must be, in degrees")
    ap.add_argument(
        "--slope-basis",
        choices=["atr", "points"],
        default="atr",
        help="atr: one bar of width = one ATR of height, scale-free. points: raw EMA gain per bar",
    )
    ap.add_argument(
        "--slope-unit", type=float, default=10.0, help="points per unit of height when --slope-basis points"
    )
    ap.add_argument("--slope-bars", type=int, default=3, help="bars the EMA's rise is measured over")
    ap.add_argument("--atr-period", type=int, default=14)
    ap.add_argument("--rsi-period", type=int, default=14)
    ap.add_argument(
        "--rsi-min",
        type=float,
        default=0.0,
        help="CE needs RSI at or above this, PE at or below its mirror (60 means 60+ for CE, 40- for PE); 0 is off",
    )
    ap.add_argument("--side", choices=["both", "CE", "PE"], default="both")
    ap.add_argument(
        "--entry",
        choices=["cross", "armed"],
        default="cross",
        help="cross: the angle must qualify on the crossing candle itself. "
        "armed: the trend may prove itself for a few bars after the cross",
    )
    ap.add_argument(
        "--arm-bars",
        type=int,
        default=0,
        help="bars a cross stays armed when --entry armed; 0 arms it for as long as price stays on that side",
    )
    ap.add_argument("--expiry", type=int, default=1, help="1 is the nearest weekly, 2 the one after it")
    ap.add_argument(
        "--strike-offset",
        type=int,
        default=0,
        help="strikes IN the money; 0 is ATM. 2 buys a call 100 points below spot, a put 100 above",
    )
    ap.add_argument("--lots", type=int, default=1)
    ap.add_argument(
        "--intraday",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="square off daily; the rule as stated holds until the EMA is crossed, so this is OFF",
    )
    ap.add_argument("--square-off", default="15:15", help="intraday / expiry-day exit time, HH:MM")
    ap.add_argument(
        "--entry-after",
        default="09:16",
        help="no fill before this time; raise it to refuse the session's opening bar",
    )
    ap.add_argument("--min-premium", type=float, default=0.0, help="skip an entry cheaper than this per unit")
    ap.add_argument(
        "--arm-same-day",
        action="store_true",
        help="a cross must arm and fire in the same session; kills overnight-armed gap entries",
    )
    ap.add_argument("--max-hold-days", type=int, default=0, help="0 is no cap")
    ap.add_argument(
        "--target-points",
        type=float,
        default=0.0,
        help="exit once the INDEX has moved this many points in the trade's favour; 0 is off",
    )
    ap.add_argument(
        "--stop-points",
        type=float,
        default=0.0,
        help="exit once the INDEX has moved this many points against the trade; 0 is off",
    )
    ap.add_argument(
        "--trail-points",
        type=float,
        default=0.0,
        help="exit once the INDEX gives back this many points from its best level in the trade; 0 is off",
    )
    ap.add_argument("--max-premium", type=float, default=0.0, help="skip an entry costing more; 0 means no cap")

    # --- data ---------------------------------------------------------------
    ap.add_argument("--pricing", choices=["dhan", "upstox", "hybrid"], default="dhan")
    ap.add_argument("--upstox-root", default=UPSTOX_ROOT)
    ap.add_argument(
        "--exit-search-minutes",
        type=int,
        default=15,
        help="upstox only: how far to look for the same contract's next print when a minute did not trade",
    )
    ap.add_argument("--allow-thin-bars", action="store_true")

    # --- measurement --------------------------------------------------------
    ap.add_argument("--slippage-pct", type=float, default=0.0, help="adverse percent of premium, each leg")
    ap.add_argument("--from", dest="start", default=None)
    ap.add_argument("--to", dest="end", default=None)
    ap.add_argument("--csv", default=None)
    ap.add_argument(
        "--sweep",
        default=None,
        help="comma-separated angles to compare, or 'auto' for 0,5,10,15,20,25,30,35",
    )
    args = ap.parse_args()

    bt = Backtest(args)
    if args.sweep:
        angles = (
            [0, 5, 10, 15, 20, 25, 30, 35]
            if args.sweep == "auto"
            else [float(x) for x in args.sweep.split(",") if x.strip()]
        )
        print(sweep(bt, args, angles))
        return

    trades = window(bt.run(), args)
    print(report(trades, bt, args))
    if args.csv:
        pd.DataFrame(
            [
                {
                    "side": t.side,
                    "entry_ts": t.entry_ts,
                    "exit_ts": t.exit_ts,
                    "strike": t.strike,
                    "expiry": t.expiry,
                    "lot": t.lot,
                    "entry_angle": round(t.entry_angle, 2),
                    "entry_spot": round(t.entry_spot, 2),
                    "exit_spot": round(t.exit_spot, 2),
                    "entry_premium": round(t.entry_premium, 2),
                    "exit_premium": round(t.exit_premium, 2),
                    "exit_reason": t.exit_reason,
                    "priced_exit": t.priced_exit,
                    "bars_held": t.bars_held,
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
