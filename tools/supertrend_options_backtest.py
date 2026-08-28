"""Supertrend on NIFTY, expressed in weekly options, priced on the Dhan archive.

Phil's ask (2026-08-26): the supertrend sweep -- ATR period 10, multiplier
1.0..4.0 in halves, timeframes 1m/3m/5m/15m/1H -- over the FIVE-YEAR Dhan
expired-options archive, 2021 -> 2026.

The strategy is the plain always-in supertrend, intraday:

  * long a CE while the supertrend is bullish, long a PE while bearish;
  * the flip is the only signal exit -- no stop, no target;
  * squared off 15:20 every day, re-entered next morning while the trend holds
    (state-based entry, same shape as the PhilForge/Upstox sweep running in
    parallel);
  * ATM strike at the fill minute, nearest weekly expiry, 1 lot at the
    date-correct lot size, real date-aware Indian charges + slippage.

Two deliberate departures from tools/ema_options_backtest.py, whose pricing
spine this reuses:

  * signals come from REAL NIFTY 1-minute candles (PhilForge's index cache,
    2021-01 -> 2026-08), not from the `spot` column lifted off option rows.
    That reconstructed index has one price per minute (98.9% of its 1m bars
    are zero-range), which would starve the ATR that supertrend is built on --
    and Phil wants 1m and 3m frames.
  * entries are state-based, not cross-based: with a daily square-off, a
    cross-only rule trades a multi-day trend once and then watches it.
"""

from __future__ import annotations

import argparse
import glob
import itertools
import json
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Optional

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from options.charges import round_trip_charges  # noqa: E402
from options.dhan_listed import DhanListedSource  # noqa: E402
from options.upstox_archive import DEFAULT_ROOT as UPSTOX_ROOT  # noqa: E402
from options.upstox_archive import UpstoxArchiveSource  # noqa: E402
from tools.ema_options_backtest import STORES, HybridSource  # noqa: E402
from tools.nifty_expiry_calendar import STRIKE_STEP, lot_size, weekly_expiries  # noqa: E402

INDEX_CACHE = "/Users/philipkumar/Documents/PhilForge/tools/.nifty_cache"


def load_real_minutes(root: str = INDEX_CACHE) -> pd.DataFrame:
    """Real NIFTY 1m candles, de-duplicated across overlapping files and
    session-filtered -- the cache carries stray bars past 19:00.

    Parsed once and kept as parquet: ten sweep shards each re-reading 590k
    rows of JSON turned the load into the bottleneck."""
    pq = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "nifty_1m_real.parquet")
    newest = max((os.path.getmtime(f) for f in glob.glob(os.path.join(root, "NIFTY_1m_*.json"))), default=0)
    if os.path.exists(pq) and os.path.getmtime(pq) >= newest:
        return pd.read_parquet(pq)
    rows: dict[str, list] = {}
    for path in sorted(glob.glob(os.path.join(root, "NIFTY_1m_*.json"))):
        for ts, o, h, low, c, *_ in json.load(open(path)):
            rows[str(ts)[:19]] = [float(o), float(h), float(low), float(c)]
    frame = pd.DataFrame.from_dict(rows, orient="index", columns=["open", "high", "low", "close"])
    frame.index = pd.to_datetime(frame.index)
    frame = frame.sort_index()
    frame = frame[(frame.index.time >= time(9, 15)) & (frame.index.time <= time(15, 29))]
    frame.to_parquet(pq)
    return frame


def to_bars(minute: pd.DataFrame, minutes: int) -> pd.DataFrame:
    return (
        minute.resample(f"{minutes}min", origin=pd.Timestamp("2021-01-01 09:15"))
        .agg({"open": "first", "high": "max", "low": "min", "close": "last"})
        .dropna()
    )


def wilder_rsi(close: pd.Series, period: int) -> pd.Series:
    """Wilder's RSI (alpha = 1/period) -- what every charting package draws."""
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean()
    out = 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)
    return out.where(avg_loss > 0, 100.0).where(avg_gain > 0, other=out.where(avg_gain > 0, 0.0))


def supertrend_dir(bars: pd.DataFrame, period: int, multiplier: float) -> np.ndarray:
    """+1/-1 per bar. Byte-for-byte the PhilForge engine's implementation
    (Wilder RMA ATR, TradingView-matching band carry)."""
    high = bars["high"].values.astype(float)
    low = bars["low"].values.astype(float)
    close = bars["close"].values.astype(float)
    n = len(close)
    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]
    tr = np.maximum(high - low, np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)))
    alpha = 1.0 / period
    atr = np.zeros(n)
    atr[0] = tr[0]
    for i in range(1, n):
        atr[i] = alpha * tr[i] + (1 - alpha) * atr[i - 1]
    hl2 = (high + low) / 2.0
    upper_raw = hl2 + multiplier * atr
    lower_raw = hl2 - multiplier * atr
    upper = upper_raw.copy()
    lower = lower_raw.copy()
    st = np.zeros(n)
    st_dir = np.zeros(n, dtype=int)
    st[0] = lower[0]
    st_dir[0] = 1
    for i in range(1, n):
        lower[i] = lower_raw[i] if (lower_raw[i] > lower[i - 1] or close[i - 1] < lower[i - 1]) else lower[i - 1]
        upper[i] = upper_raw[i] if (upper_raw[i] < upper[i - 1] or close[i - 1] > upper[i - 1]) else upper[i - 1]
        if st[i - 1] == upper[i - 1]:
            if close[i] > upper[i]:
                st[i], st_dir[i] = lower[i], 1
            else:
                st[i], st_dir[i] = upper[i], -1
        else:
            if close[i] < lower[i]:
                st[i], st_dir[i] = upper[i], -1
            else:
                st[i], st_dir[i] = lower[i], 1
    return st_dir


@dataclass
class Trade:
    side: str
    entry_ts: datetime
    entry_spot: float
    strike: int
    expiry: date
    lot: int
    entry_premium: float
    entry_bar: int = 0
    mfe: float = 0.0
    exit_ts: Optional[datetime] = None
    exit_premium: float = 0.0
    exit_reason: str = ""
    priced_exit: bool = True
    unpriceable: bool = False
    charges: float = 0.0
    gross_raw: float = 0.0  # (exit - entry) * qty before slippage and charges

    @property
    def net(self) -> float:
        return (self.exit_premium - self.entry_premium) * self.lot - self.charges


@dataclass
class Contract:
    expiry: date
    strike: int
    option_type: str


class Runner:
    def __init__(self, args):
        self.args = args
        self.minute = load_real_minutes(args.index_cache)
        if args.start:
            self.minute = self.minute.loc[args.start :]
        if args.end:
            self.minute = self.minute.loc[: args.end + " 23:59"]
        self.minute_index = self.minute.index
        self.spot_open = self.minute["open"]
        daily = self.minute.groupby(self.minute.index.normalize())["close"].last()
        self.index_close = {d.strftime("%Y-%m-%d"): float(c) for d, c in daily.items()}
        session_days = sorted({d.date() for d in self.minute.index.normalize().unique()})
        self.weeklies = weekly_expiries(session_days)
        self.source = self._build_source(args)
        sq = str(args.square_off).split(":")
        self.square_off = time(int(sq[0]), int(sq[1]))
        ea = str(args.entry_after).split(":")
        self.entry_after = time(int(ea[0]), int(ea[1]))

    def _build_source(self, args):
        if args.pricing == "upstox":
            src = UpstoxArchiveSource(args.upstox_root, "NIFTY")
            if not src.expiries():
                raise SystemExit(f"no Upstox archive under {args.upstox_root}")
            return src
        dh = DhanListedSource(self.weeklies, STORES, "NIFTY", nearest_within=0)
        for store in dh.stores.values():
            store.levels = self.index_close
        if args.pricing == "hybrid":
            return HybridSource(dh, UpstoxArchiveSource(args.upstox_root, "NIFTY"))
        return dh

    # -- plumbing ---------------------------------------------------------
    def next_minute(self, after: datetime) -> Optional[datetime]:
        i = self.minute_index.searchsorted(after, side="right")
        return self.minute_index[i] if i < len(self.minute_index) else None

    def minute_at_or_before(self, when: datetime) -> Optional[datetime]:
        i = self.minute_index.searchsorted(when, side="right") - 1
        if i < 0:
            return None
        cand = self.minute_index[i]
        return cand if cand.date() == when.date() else None

    def expiry_for(self, day: date) -> Optional[date]:
        """The nth weekly at or after ``day``.

        n=1 is the nearest, which on expiry day itself is a contract with hours
        to live -- 34% of trades landed on <=1 DTE and every one of those
        buckets lost money. n=2 buys the following week instead.
        """
        from bisect import bisect_left

        i = bisect_left(self.weeklies, day) + int(self.args.expiry) - 1
        return self.weeklies[i] if 0 <= i < len(self.weeklies) else None

    def premium(self, when: datetime, contract: Contract):
        px = self.source.lookup(when, contract)
        return px

    def close_trade(self, t: Trade, when: datetime, reason: str, counters: dict) -> None:
        px = self.premium(when, Contract(t.expiry, t.strike, t.side))
        spot_ts = self.minute_at_or_before(when)
        spot = float(self.spot_open.loc[spot_ts]) if spot_ts is not None else t.entry_spot
        if px is None:
            px = max(0.0, (spot - t.strike) if t.side == "CE" else (t.strike - spot))
            t.priced_exit = False
            counters["unpriced_exits"] += 1
            if px <= 0:
                t.unpriceable = True
                counters["dropped_unpriceable"] += 1
        slip = self.args.slippage_pct / 100.0
        t.exit_ts, t.exit_reason = when, reason
        t.gross_raw = (px - t.entry_premium) * t.lot
        t.entry_premium = t.entry_premium * (1 + slip)
        t.exit_premium = px * (1 - slip)
        t.charges = round_trip_charges(
            trade_date=t.entry_ts.date(),
            buy_premium=t.entry_premium,
            sell_premium=t.exit_premium,
            quantity=t.lot,
        ).total

    # -- one (timeframe, multiplier) run ----------------------------------
    def run(self, tf: int, mult: float, side: str = "both", hold: str = "intraday"):
        bars = to_bars(self.minute, tf)
        dirs = supertrend_dir(bars, self.args.atr_period, mult)
        rsi = wilder_rsi(bars["close"], self.args.rsi_period).values if self.args.rsi_min else None
        stamps = bars.index
        counters = {
            "skipped_no_entry_price": 0,
            "skipped_no_expiry": 0,
            "unpriced_exits": 0,
            "dropped_unpriceable": 0,
            "capped_by_tpd": 0,
            "vetoed_by_rsi": 0,
            "rolls": 0,
        }
        trades: list[Trade] = []
        open_trade: Optional[Trade] = None
        trades_today = 0
        last_day = None

        for k in range(1, len(stamps)):
            ts = stamps[k]
            day = ts.date()
            if day != last_day:
                trades_today = 0
                last_day = day
            bar_close_ts = ts + pd.Timedelta(minutes=tf)
            want = int(dirs[k])

            # ---- exits, decided on this closed bar ----------------------
            if open_trade is not None and hold == "flip":
                have = 1 if open_trade.side == "CE" else -1
                if day >= open_trade.expiry and (day > open_trade.expiry or ts.time() >= self.square_off):
                    # The contract dies on its expiry day whatever the trend is
                    # doing. Priced at the expiry day's own square-off minute;
                    # a coarse frame may only NOTICE on the next session's
                    # first bar. The trend, if it survives, re-enters on the
                    # next weekly -- that is the roll, paid honestly.
                    fill = self.minute_at_or_before(datetime.combine(open_trade.expiry, self.square_off))
                    if fill is not None and fill >= open_trade.entry_ts:
                        self.close_trade(open_trade, fill, "EXPIRY", counters)
                        if not open_trade.unpriceable:
                            trades.append(open_trade)
                        open_trade = None
                elif (
                    self.args.stop_points
                    and have
                    * (
                        (float(bars["low"].iloc[k]) if have > 0 else float(bars["high"].iloc[k]))
                        - open_trade.entry_spot
                    )
                    <= -self.args.stop_points
                ):
                    # Triggered on the bar's ADVERSE extreme, filled on the next
                    # minute after it closes. Checked before the trail so that a
                    # bar containing both is settled the unkind way.
                    fill = self.next_minute(bar_close_ts - pd.Timedelta(minutes=1))
                    if fill is not None and fill.date() <= open_trade.expiry:
                        self.close_trade(open_trade, fill, "STOP", counters)
                        if not open_trade.unpriceable:
                            trades.append(open_trade)
                        open_trade = None
                elif (
                    self.args.target_points
                    and have
                    * (
                        (float(bars["high"].iloc[k]) if have > 0 else float(bars["low"].iloc[k]))
                        - open_trade.entry_spot
                    )
                    >= self.args.target_points
                ):
                    # The bar REACHED the target; the fill is the next minute
                    # after it closes, not the touch. Conservative on purpose --
                    # the index can and does come back before the option sells.
                    # Checked AFTER the stop, so a bar holding both is settled
                    # against the trade.
                    fill = self.next_minute(bar_close_ts - pd.Timedelta(minutes=1))
                    if fill is not None and fill.date() <= open_trade.expiry:
                        self.close_trade(open_trade, fill, "TARGET", counters)
                        if not open_trade.unpriceable:
                            trades.append(open_trade)
                        open_trade = None
                elif (
                    self.args.trail_points
                    and open_trade.mfe > max(self.args.trail_points, self.args.trail_after_points)
                    and have
                    * (
                        (float(bars["low"].iloc[k]) if have > 0 else float(bars["high"].iloc[k]))
                        - open_trade.entry_spot
                    )
                    <= open_trade.mfe - self.args.trail_points
                ):
                    # ``mfe`` only counts bars that have CLOSED, so the trail
                    # ratchets a bar behind the extreme -- it can never see a
                    # high and sell it inside the same candle.
                    fill = self.next_minute(bar_close_ts - pd.Timedelta(minutes=1))
                    if fill is not None and fill.date() <= open_trade.expiry:
                        self.close_trade(open_trade, fill, "TRAIL", counters)
                        if not open_trade.unpriceable:
                            trades.append(open_trade)
                        open_trade = None
                elif (
                    self.args.roll_strikes
                    and abs(float(bars["close"].iloc[k]) - open_trade.strike) >= self.args.roll_strikes * STRIKE_STEP
                ):
                    # The position has drifted this far from its strike. Sell it
                    # and let the entry rule below buy a fresh at-the-money one
                    # on the same bar, because the trend has not changed.
                    #
                    # This is not a filter and not a target: the exposure is
                    # continuous. What it buys is MEASURABILITY -- rolled while
                    # still inside Dhan's ATM+/-10 band, both legs have a real
                    # quote, where a position left to run leaves the band and
                    # has to be floored at intrinsic. It is also what a desk
                    # does anyway, since a deep ITM weekly is where the spread
                    # goes to die. It costs a full round trip each time.
                    fill = self.next_minute(bar_close_ts - pd.Timedelta(minutes=1))
                    if fill is not None and fill.date() <= open_trade.expiry:
                        self.close_trade(open_trade, fill, "ROLL", counters)
                        if not open_trade.unpriceable:
                            trades.append(open_trade)
                        open_trade = None
                        counters["rolls"] += 1
                elif (
                    self.args.time_stop_bars
                    and (k - open_trade.entry_bar) >= self.args.time_stop_bars
                    and have * (float(bars["close"].iloc[k]) - open_trade.entry_spot) < self.args.time_stop_points
                ):
                    # The bars have passed and the index has not gone anywhere.
                    # A trend trade that is not trending is a theta subscription;
                    # this cancels it without touching the entry rule.
                    fill = self.next_minute(bar_close_ts - pd.Timedelta(minutes=1))
                    if fill is not None:
                        self.close_trade(open_trade, fill, "NO_PROGRESS", counters)
                        if not open_trade.unpriceable:
                            trades.append(open_trade)
                        open_trade = None
                elif want != have:
                    fill = self.next_minute(bar_close_ts - pd.Timedelta(minutes=1))
                    if fill is not None:
                        self.close_trade(open_trade, fill, "FLIP", counters)
                        if not open_trade.unpriceable:
                            trades.append(open_trade)
                        open_trade = None
            elif open_trade is not None:
                have = 1 if open_trade.side == "CE" else -1
                if ts.time() >= self.square_off or day != open_trade.entry_ts.date():
                    # A coarse frame prints no bar at/after 15:20, so the
                    # square-off is only NOTICED on the next day's first bar --
                    # but it is always PRICED at the entry day's own square-off
                    # minute. Anything else is an overnight hold in disguise.
                    fill = self.minute_at_or_before(datetime.combine(open_trade.entry_ts.date(), self.square_off))
                    if fill is not None and fill >= open_trade.entry_ts:
                        self.close_trade(open_trade, fill, "SQUARE_OFF", counters)
                        if not open_trade.unpriceable:
                            trades.append(open_trade)
                        open_trade = None
                elif want != have:
                    fill = self.next_minute(bar_close_ts - pd.Timedelta(minutes=1))
                    if fill is not None and fill.date() == day and fill.time() < self.square_off:
                        self.close_trade(open_trade, fill, "FLIP", counters)
                        if not open_trade.unpriceable:
                            trades.append(open_trade)
                        open_trade = None
                    # else: the flip lands past the square-off; the square-off
                    # branch above catches it on the next bar or day change.

            if open_trade is not None:
                have = 1 if open_trade.side == "CE" else -1
                far = float(bars["high"].iloc[k]) if have > 0 else float(bars["low"].iloc[k])
                open_trade.mfe = max(open_trade.mfe, have * (far - open_trade.entry_spot))

            # ---- entry: in-trend state on this closed bar ----------------
            if open_trade is not None or want == 0:
                continue
            if hold == "intraday" and self.args.max_trades_day and trades_today >= self.args.max_trades_day:
                counters["capped_by_tpd"] += 1
                continue
            entry_side = "CE" if want > 0 else "PE"
            if side != "both" and entry_side != side:
                continue
            if rsi is not None:
                # A mirror gate: a call wants momentum at or above the level, a
                # put at or below its reflection. The trend state persists, so a
                # refusal here is a delay, not a miss -- the entry retries every
                # bar until momentum confirms or the trend flips away.
                r = rsi[k]
                if r != r:
                    continue
                if want > 0 and r < self.args.rsi_min:
                    counters["vetoed_by_rsi"] += 1
                    continue
                if want < 0 and r > 100.0 - self.args.rsi_min:
                    counters["vetoed_by_rsi"] += 1
                    continue
            fill = self.next_minute(bar_close_ts - pd.Timedelta(minutes=1))
            if fill is None or fill.date() != day:
                continue
            if not (self.entry_after <= fill.time() < self.square_off):
                continue
            expiry = self.expiry_for(day)
            if expiry is None:
                counters["skipped_no_expiry"] += 1
                continue
            spot = float(self.spot_open.loc[fill])
            # In the money is a LOWER strike for a call and a HIGHER one for a
            # put, so the offset applies AGAINST the trade's direction. An ITM
            # option carries more delta and less theta -- it is the cheapest
            # way to hold a position through a market that goes nowhere.
            strike = int(
                round(spot / STRIKE_STEP) * STRIKE_STEP - int(want) * int(self.args.strike_offset) * int(STRIKE_STEP)
            )
            prem = self.premium(fill, Contract(expiry, strike, entry_side))
            if prem is None:
                counters["skipped_no_entry_price"] += 1
                continue
            open_trade = Trade(
                side=entry_side,
                entry_ts=fill,
                entry_spot=spot,
                strike=strike,
                expiry=expiry,
                lot=self.args.lots * lot_size(expiry),
                entry_premium=prem,
                entry_bar=k,
            )
            trades_today += 1

        if open_trade is not None:
            self.close_trade(open_trade, self.minute_index[-1], "OPEN_AT_END", counters)
            if not open_trade.unpriceable:
                trades.append(open_trade)
        return trades, counters


def stats(trades: list[Trade]) -> dict:
    if not trades:
        return {"n": 0, "net": 0.0, "gross": 0.0, "win": 0.0, "avg": 0.0, "dd": 0.0, "priced": 0, "priced_net": 0.0}
    net = [t.net for t in trades]
    eq = peak = dd = 0.0
    for x in net:
        eq += x
        peak = max(peak, eq)
        dd = max(dd, peak - eq)
    return {
        "n": len(trades),
        "net": sum(net),
        "gross": sum(t.gross_raw for t in trades),
        "win": sum(1 for x in net if x > 0) / len(net),
        "avg": sum(net) / len(net),
        "dd": dd,
        "priced": sum(1 for t in trades if t.priced_exit),
        "priced_net": sum(t.net for t in trades if t.priced_exit),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--timeframes", nargs="+", type=int, default=[1, 3, 5, 15, 60])
    ap.add_argument("--multipliers", nargs="+", type=float, default=[1, 1.5, 2, 2.5, 3, 3.5, 4])
    ap.add_argument("--atr-period", type=int, default=10)
    ap.add_argument("--rsi-period", type=int, default=14)
    ap.add_argument("--expiry", type=int, default=1, help="1 is the nearest weekly, 2 the one after it")
    ap.add_argument(
        "--stop-points",
        type=float,
        default=0.0,
        help="exit once the INDEX has gone this many points against the trade; 0 is off",
    )
    ap.add_argument(
        "--target-points",
        type=float,
        default=0.0,
        help="exit once the INDEX has moved this many points in the trade's favour; 0 is off",
    )
    ap.add_argument(
        "--trail-points",
        type=float,
        default=0.0,
        help="exit once the INDEX gives back this many points from its best level in the trade; 0 is off",
    )
    ap.add_argument(
        "--trail-after-points",
        type=float,
        default=0.0,
        help="the trail stays disarmed until the INDEX has run this far in favour; "
        "0 arms it as soon as the move exceeds the trail distance itself",
    )
    ap.add_argument(
        "--roll-strikes",
        type=int,
        default=0,
        help="roll to a fresh ATM contract once price sits this many strikes from the strike; 0 is off",
    )
    ap.add_argument(
        "--strike-offset",
        type=int,
        default=0,
        help="strikes IN the money; 0 is ATM. 2 buys a call 100 points below spot, a put 100 above",
    )
    ap.add_argument(
        "--time-stop-bars",
        type=int,
        default=0,
        help="close a position that has not moved favourably after this many bars; 0 is off",
    )
    ap.add_argument(
        "--time-stop-points",
        type=float,
        default=0.0,
        help="how far the index must have travelled by then to earn a reprieve",
    )
    ap.add_argument(
        "--rsi-min",
        type=float,
        default=0.0,
        help="CE needs RSI >= this, PE needs RSI <= (100 - this); 0 is off",
    )
    ap.add_argument("--lots", type=int, default=1)
    ap.add_argument("--square-off", default="15:20")
    ap.add_argument("--entry-after", default="09:20")
    ap.add_argument("--max-trades-day", type=int, default=30, help="0 is uncapped; 30 matches the PhilForge sweep")
    ap.add_argument("--side", choices=["both", "CE", "PE"], default="both")
    ap.add_argument(
        "--hold",
        choices=["intraday", "flip"],
        default="intraday",
        help="intraday: square off 15:20 daily. flip: hold to the opposite flip, forced out only at expiry (the roll)",
    )
    ap.add_argument("--slippage-pct", type=float, default=0.15, help="adverse percent of premium, each leg")
    ap.add_argument("--pricing", choices=["dhan", "upstox", "hybrid"], default="dhan")
    ap.add_argument("--upstox-root", default=UPSTOX_ROOT)
    ap.add_argument("--index-cache", default=INDEX_CACHE)
    ap.add_argument("--from", dest="start", default=None)
    ap.add_argument("--to", dest="end", default=None)
    ap.add_argument("--out", default="tools/supertrend_dhan_results.json")
    args = ap.parse_args()

    runner = Runner(args)
    print(
        f"[data] {len(runner.minute):,} real 1m index bars, "
        f"{runner.minute_index[0]} -> {runner.minute_index[-1]}; pricing={args.pricing}",
        flush=True,
    )

    results = []
    print(
        f"\n{'tf':>4}{'mult':>6}{'trades':>8}{'win%':>7}{'gross':>13}{'net':>14}{'avg':>9}{'maxDD':>12}"
        f"{'floored':>9}{'no-entry':>9}",
        flush=True,
    )
    for tf, mult in itertools.product(args.timeframes, args.multipliers):
        trades, counters = runner.run(tf, mult, side=args.side, hold=args.hold)
        s = stats(trades)
        print(
            f"{tf:>4}{mult:>6g}{s['n']:>8}{s['win'] * 100:>6.0f}%{s['gross']:>13,.0f}{s['net']:>14,.0f}{s['avg']:>9,.0f}"
            f"{s['dd']:>12,.0f}{counters['unpriced_exits']:>9}{counters['skipped_no_entry_price']:>9}",
            flush=True,
        )
        results.append(
            {
                "tf": tf,
                "mult": mult,
                **{k: (round(v, 2) if isinstance(v, float) else v) for k, v in s.items()},
                "counters": counters,
                "trade_rows": [
                    {
                        "side": t.side,
                        "entry": str(t.entry_ts),
                        "exit": str(t.exit_ts),
                        "strike": t.strike,
                        "expiry": str(t.expiry),
                        "lot": t.lot,
                        "entry_prem": round(t.entry_premium, 2),
                        "exit_prem": round(t.exit_premium, 2),
                        "reason": t.exit_reason,
                        "priced": t.priced_exit,
                        "charges": round(t.charges, 2),
                        "net": round(t.net, 2),
                    }
                    for t in trades
                ],
                "by_year": {
                    str(y): round(sum(t.net for t in trades if t.entry_ts.year == y), 2)
                    for y in sorted({t.entry_ts.year for t in trades})
                },
                "by_side": {sd: round(sum(t.net for t in trades if t.side == sd), 2) for sd in ("CE", "PE")},
            }
        )
        # Written to a sibling and renamed, so a serialisation failure leaves the
        # previous file intact instead of a half-written one. A truncated results
        # file cost a whole sweep once: json.dump died mid-array on a numpy int
        # and the shell had the traceback pointed at /dev/null.
        tmp_out = args.out + ".partial"
        with open(tmp_out, "w") as fh:
            json.dump(
                {
                    "from": str(runner.minute_index[0]),
                    "to": str(runner.minute_index[-1]),
                    "pricing": args.pricing,
                    "archive": runner.source.report(),
                    "side": args.side,
                    "hold": args.hold,
                    "results": results,
                },
                fh,
            )
        os.replace(tmp_out, args.out)
    print(f"\narchive: {runner.source.report()}", flush=True)
    print(f"wrote {args.out}", flush=True)


if __name__ == "__main__":
    main()
