"""
tools/cascade_backtest.py — replay the cascade engine over the days Phil traded
by hand, so a rule change is a number instead of an argument.

Each case is one of his marked-up TradingView charts: the symbol and the mother
candle high he picked. The harness finds that candle in real Binance 5m data,
runs the engine forward over it candle by candle with no hindsight, and reports
what the engine would have done — did it find structure at all, how many entries
it took, how deep the money went, whether the target paid.

    .venv/bin/python tools/cascade_backtest.py            # all cases
    .venv/bin/python tools/cascade_backtest.py --case sol # substring filter
    .venv/bin/python tools/cascade_backtest.py --refetch  # ignore the cache

Candles are cached under tools/.backtest_cache so repeat runs are offline and
fast; the numbers only move when the engine does.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine.cascade import Campaign, Candle, CascadeEngine  # noqa: E402

IST = timezone(timedelta(hours=5, minutes=30))
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".backtest_cache")
FIVE_MIN = 300


@dataclass
class Case:
    """One hand-drawn chart: what he anchored on, and roughly when."""

    label: str
    symbol: str
    mother_high: float
    day: str  # YYYY-MM-DD, the date the mother candle sits on (IST)
    run_hours: int = 30  # how far forward to replay


# Phil's charts. The mother high is read straight off each screenshot; the day
# bounds the search for it. Coinbase/Kraken charts are mapped to the Binance
# USDT pair, which is where the engine actually trades.
CASES: List[Case] = [
    Case("82nd/83rd BTC", "BTCUSDT", 64710.00, "2026-06-14"),
    Case("85th BTC 1m", "BTCUSDT", 65995.00, "2026-06-15"),
    Case("86th BTC", "BTCUSDT", 67288.00, "2026-06-15"),
    Case("95th BTC 1m", "BTCUSDT", 62982.01, "2026-06-24"),
    Case("87th BTC 1m", "BTCUSDT", 66992.00, "2026-06-16"),
    Case("88th/89th BTC", "BTCUSDT", 66200.00, "2026-06-17"),
    Case("90th BTC 15m", "BTCUSDT", 67292.15, "2026-06-15"),
    Case("SOL 1st live", "SOLUSDT", 83.98, "2026-07-04"),
    Case("BTC 3rd/4th live", "BTCUSDT", 63920.00, "2026-07-06"),
    Case("BTC 5th live", "BTCUSDT", 64700.00, "2026-07-07"),
    Case("SOL 7th live", "SOLUSDT", 83.74, "2026-07-07"),
    Case("BTC 9th/10th live", "BTCUSDT", 64692.83, "2026-07-10"),
    Case("PAXG 11th live", "PAXGUSDT", 4097.58, "2026-07-14"),
    Case("BTC 12th/13th live", "BTCUSDT", 65600.00, "2026-07-15"),
    Case("SOL 16th live", "SOLUSDT", 79.05, "2026-07-15"),
]


# ── candle fetching ───────────────────────────────────────────────


def _cache_path(symbol: str, day: str) -> str:
    return os.path.join(CACHE_DIR, f"{symbol}_{day}.json")


def fetch_candles(symbol: str, day: str, refetch: bool = False) -> List[tuple]:
    """5m candles spanning the day before through two days after, cached."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = _cache_path(symbol, day)
    if os.path.exists(path) and not refetch:
        with open(path, "r", encoding="utf-8") as handle:
            return [tuple(row) for row in json.load(handle)]

    start = datetime.fromisoformat(day).replace(tzinfo=IST) - timedelta(hours=18)
    rows: List[tuple] = []
    cursor = int(start.timestamp() * 1000)
    for _ in range(4):  # 4 x 1000 candles ~= 3.5 days
        resp = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": symbol, "interval": "5m", "startTime": cursor, "limit": 1000},
            timeout=30,
        )
        resp.raise_for_status()
        batch = resp.json()
        if not isinstance(batch, list) or not batch:
            break
        rows.extend((int(k[0]) // 1000, float(k[1]), float(k[2]), float(k[3]), float(k[4])) for k in batch)
        cursor = int(batch[-1][0]) + FIVE_MIN * 1000
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(rows, handle)
    return rows


def find_mother(rows: List[tuple], mother_high: float, day: str) -> Optional[int]:
    """
    The candle whose high is the level he marked, searched only near the day the
    chart is from. Without that bound the same price prints again days later and
    the replay silently runs on the wrong swing.
    """
    origin = datetime.fromisoformat(day).replace(tzinfo=IST)
    lo = (origin - timedelta(hours=8)).timestamp()
    hi = (origin + timedelta(hours=32)).timestamp()
    best = None
    for i, (ts, _, high, _, _) in enumerate(rows):
        if not lo <= ts <= hi:
            continue
        gap = abs(high - mother_high)
        if gap <= mother_high * 0.0002 and (best is None or gap < best[1]):
            best = (i, gap)
    return best[0] if best else None


# ── replay ────────────────────────────────────────────────────────


class _OfflineBroker:
    """No network, no orders. The engine only asks it for symbol metadata."""

    display_name = "Backtest"

    def __init__(self, symbol: str, tick: str, min_notional: str = "5.0"):
        self._symbol = symbol
        self._tick = tick
        self._min_notional = min_notional

    def _is_configured(self):
        return True

    def get_product_by_symbol(self, symbol):
        return {"symbol": symbol, "broker_symbol": symbol, "min_notional": self._min_notional, "tick_size": self._tick}


def _tick_for(mother_high: float) -> str:
    if mother_high >= 1000:
        return "0.01"
    if mother_high >= 10:
        return "0.001"
    return "0.0001"


@dataclass
class Result:
    label: str
    symbol: str
    ok: bool
    note: str = ""
    fibs: int = 0
    trendlines: int = 0
    entries: int = 0
    avg_entry: Optional[float] = None
    deployed: float = 0.0
    deepest_pct: float = 0.0  # how far below the mother high the deepest fill sat
    rounds: int = 0
    realised: float = 0.0
    state: str = ""
    first_fib_at: Optional[str] = None


def replay(case: Case, refetch: bool = False, capital: float = 2000.0) -> Result:
    rows = fetch_candles(case.symbol, case.day, refetch)
    if not rows:
        return Result(case.label, case.symbol, False, "no candles returned")
    mi = find_mother(rows, case.mother_high, case.day)
    if mi is None:
        return Result(case.label, case.symbol, False, f"mother high {case.mother_high:,.2f} not on {case.day}")

    mother = rows[mi]
    engine = CascadeEngine(_OfflineBroker(case.symbol, _tick_for(case.mother_high)))
    campaign = Campaign(
        campaign_id="bt",
        symbol=case.symbol,
        capital_usd=capital,
        mother_high=mother[2],
        mother_low=mother[3],
        mother_timestamp=mother[0],
        mode="paper",
        min_notional_usd=5.0,
        tick_size=float(_tick_for(case.mother_high)),
        last_processed_ts=mother[0],
        window_start_ts=mother[0],
    )
    engine.campaigns["bt"] = campaign

    history: List[Candle] = []
    limit = mi + 1 + (case.run_hours * 12)
    first_fib_at = None
    for ts, o, h, low, c in rows[mi + 1 : limit]:
        candle = Candle(ts, o, h, low, c)
        history.append(candle)
        engine._candles_5m["bt"] = list(history)
        engine._process_candle(campaign, candle)
        if first_fib_at is None and campaign.legs:
            first_fib_at = datetime.fromtimestamp(candle.timestamp, IST).strftime("%m-%d %H:%M")
        if campaign.state not in ("WAITING_FIRST_DEPTH", "TRENDLINE_ACTIVE"):
            break

    fills = campaign.all_fills
    deployed = sum(f.price * f.quantity for f in fills) + sum(
        r.avg_entry * r.qty for r in campaign.rounds if r.avg_entry and r.qty
    )
    deepest = min((f.price for f in fills), default=None)
    return Result(
        label=case.label,
        symbol=case.symbol,
        ok=True,
        fibs=len(campaign.legs),
        trendlines=len(campaign.trendlines),
        entries=len(fills) + sum(1 for _ in campaign.rounds),
        avg_entry=campaign.avg_entry_price,
        deployed=deployed,
        deepest_pct=((mother[2] - deepest) / mother[2] * 100) if deepest else 0.0,
        rounds=len(campaign.rounds),
        realised=campaign.realized_pnl_total,
        state=campaign.state,
        first_fib_at=first_fib_at,
    )


# ── report ────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", help="only run cases whose label contains this")
    parser.add_argument("--refetch", action="store_true", help="ignore the candle cache")
    parser.add_argument("--capital", type=float, default=2000.0)
    args = parser.parse_args()

    cases = [c for c in CASES if not args.case or args.case.lower() in c.label.lower()]
    if not cases:
        print("no cases matched")
        return 1

    results = []
    for case in cases:
        try:
            results.append(replay(case, args.refetch, args.capital))
        except Exception as exc:  # a broken case must not hide the rest
            results.append(Result(case.label, case.symbol, False, f"{type(exc).__name__}: {exc}"))

    head = f"{'case':<20} {'sym':<9} {'fibs':>4} {'TL':>3} {'buys':>5} {'deployed':>10} {'deepest':>8} {'rounds':>7} {'P&L':>8}  first fib"
    print(head)
    print("-" * len(head))
    drew = traded = 0
    for r in results:
        if not r.ok:
            print(
                f"{r.label:<20} {r.symbol:<9} {'--':>4}  {'--':>2} {'--':>5} {'':>10} {'':>8} {'':>7} {'':>8}  {r.note}"
            )
            continue
        drew += 1 if r.fibs else 0
        traded += 1 if r.entries else 0
        print(
            f"{r.label:<20} {r.symbol:<9} {r.fibs:>4} {r.trendlines:>3} {r.entries:>5} "
            f"${r.deployed:>9,.2f} {r.deepest_pct:>7.2f}% {r.rounds:>7} ${r.realised:>7,.2f}  "
            f"{r.first_fib_at or '—'}"
        )

    usable = [r for r in results if r.ok]
    print()
    print(f"  {len(usable)}/{len(results)} cases replayed")
    print(f"  {drew}/{len(usable)} drew structure      {traded}/{len(usable)} took an entry")
    if usable:
        print(
            f"  total deployed ${sum(r.deployed for r in usable):,.2f}   realised ${sum(r.realised for r in usable):,.2f}"
        )
        deep = [r.deepest_pct for r in usable if r.deepest_pct]
        if deep:
            print(f"  deepest fill below the mother high: avg {sum(deep) / len(deep):.2f}%, max {max(deep):.2f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
