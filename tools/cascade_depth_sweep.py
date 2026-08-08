"""
tools/cascade_depth_sweep.py — run the real cascade engine over two years of 5m
candles under different fib ladders, so "should the ladder go deeper" is a
number instead of an opinion.

The engine is NOT reimplemented here. This drives engine/cascade.py candle by
candle exactly as the live loop does — the same geometry, the same pot, the same
buy-stop walk-down, the same TP, the same auto-restart on a mother break, the
same 5m->15m->1h escalation — and only swaps the three module constants that
define the ladder: CASCADE_LEVELS, LEVEL_ALLOCATION, STOP_ENTRY_LEVELS.

    .venv/bin/python tools/cascade_depth_sweep.py                 # everything
    .venv/bin/python tools/cascade_depth_sweep.py --symbol SOLUSDT
    .venv/bin/python tools/cascade_depth_sweep.py --config current

What is faithful to live, and what is not
-----------------------------------------
Faithful: geometry (trendlines, fibs, the size gate scaled per instrument),
funding percentages, the running pot and its rung minimum, the buy-stop walk,
the new-low release rule, TP net of fees at 0.1%/side, mother breaks and
retests, auto-restart chains with the barren cap, and timeframe escalation.

Not faithful, and identical across every config so the comparison still holds:
  - Mother breaks are spotted on 5m, not 1m. The 1m watcher only makes a break
    land EARLIER, never makes a break that would not have happened.
  - Fills are candle-resolution: a buy stop fills when a candle's high reaches
    it, at the limit cap; a TP closes when a candle's high reaches it, and a
    candle that took the entry is never allowed to also hit the target.
  - The first campaign of a run is anchored on the first candle in the window,
    and a fresh one is anchored by hand whenever a chain dies out. Live, Phil
    picks those. The chain re-anchors itself within days either way.
  - Every newly born campaign re-measures the instrument's median 5m bar. Live,
    a successor inherits its parent's measurement; over two years that would
    have judged 2026 SOL by how loud SOL was in 2024.
"""

from __future__ import annotations

import argparse
import json
import logging
import multiprocessing as mp
import os
import statistics
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.fetch_5m_history import load as load_history  # noqa: E402

IST = timezone(timedelta(hours=5, minutes=30))
FIVE_MIN = 300
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".sweep_out")


# ── the ladders under test ────────────────────────────────────────
#
# A level is a multiple of the fib's own range measured down from its high, so
# L8 sits eight leg-ranges below the touch high. Depth and money are separate
# questions, so each is asked separately:
#
#   current      what runs today: 2/4/8 at 20/30/50%
#   current-prop 2/4/8 with the weights the new ladders use, to separate "the
#                weighting changed" from "the depth changed"
#   d3..d16      Phil's ladder, grown one rung at a time, so the run says which
#                rung stopped paying rather than only whether the whole thing does
#
# Weights are proportional to depth: a rung eight ranges down is asking to be
# right about a much bigger fall than one three ranges down, and the money
# follows the same shape the current ladder has (deeper rung, bigger share).


def proportional(levels: Tuple[int, ...]) -> Dict[int, float]:
    total = float(sum(levels))
    share = {level: round(level / total, 6) for level in levels}
    # Rounding crumbs go to the deepest rung rather than quietly vanishing.
    share[levels[-1]] = round(share[levels[-1]] + (1.0 - sum(share.values())), 6)
    return share


FULL = (3, 4, 6, 8, 12, 16)

CONFIGS: Dict[str, dict] = {
    "current": {"levels": (2, 4, 8), "alloc": {2: 0.20, 4: 0.30, 8: 0.50}, "label": "2/4/8 @ 20/30/50 (live)"},
    "current-prop": {"levels": (2, 4, 8), "alloc": proportional((2, 4, 8)), "label": "2/4/8 @ depth-weighted"},
    "d3-4": {"levels": (3, 4), "alloc": proportional((3, 4)), "label": "3/4"},
    "d3-6": {"levels": (3, 4, 6), "alloc": proportional((3, 4, 6)), "label": "3/4/6"},
    "d3-8": {"levels": (3, 4, 6, 8), "alloc": proportional((3, 4, 6, 8)), "label": "3/4/6/8"},
    "d3-12": {"levels": (3, 4, 6, 8, 12), "alloc": proportional((3, 4, 6, 8, 12)), "label": "3/4/6/8/12"},
    "d3-16": {"levels": FULL, "alloc": proportional(FULL), "label": "3/4/6/8/12/16 (asked for)"},
    # The same six rungs, funded differently. Depth-weighting puts a third of
    # every pool on L16; if the deep ladder only loses because of THAT, these
    # two say so, and the rungs are exonerated.
    "d3-16-eq": {
        "levels": FULL,
        "alloc": {level: round(1.0 / len(FULL), 6) for level in FULL},
        "label": "3/4/6/8/12/16 @ equal",
    },
    "d3-16-front": {
        "levels": FULL,
        "alloc": {level: round((1.0 / level) / sum(1.0 / x for x in FULL), 6) for level in FULL},
        "label": "3/4/6/8/12/16 @ shallow-first",
    },
    # The other reading of the question: each depth on its own, the whole pool
    # on one rung, so the run says which single depth actually gets traded.
    **{f"solo-{level}": {"levels": (level,), "alloc": {level: 1.0}, "label": f"L{level} alone"} for level in FULL},
}

# Real Binance spot filters for the two pairs.
SYMBOL_META = {
    "BTCUSDT": {"tick": 0.01, "min_notional": 5.0},
    "SOLUSDT": {"tick": 0.01, "min_notional": 5.0},
}


class _OfflineBroker:
    """No network, no orders. The engine only asks it for symbol metadata."""

    display_name = "Backtest"
    broker_name = "binance"

    def __init__(self, symbol: str, meta: dict):
        self._symbol = symbol
        self._meta = meta

    def _is_configured(self):
        return True

    def get_product_by_symbol(self, symbol):
        return {
            "symbol": symbol,
            "broker_symbol": symbol,
            "min_notional": str(self._meta["min_notional"]),
            "tick_size": str(self._meta["tick"]),
        }


# ── results ───────────────────────────────────────────────────────


@dataclass
class RunResult:
    symbol: str
    config: str
    label: str
    capital: float
    bars: int = 0
    campaigns: int = 0
    manual_starts: int = 0
    drew_structure: int = 0
    fibs: int = 0
    fills: int = 0
    rounds: int = 0
    wins: int = 0
    losses: int = 0
    net_pnl: float = 0.0
    gross_pnl: float = 0.0
    fees: float = 0.0
    best_round: float = 0.0
    worst_round: float = 0.0
    peak_deployed: float = 0.0
    avg_deployed: float = 0.0
    deployed_samples: int = 0
    time_in_position_pct: float = 0.0
    median_hold_hours: float = 0.0
    max_hold_hours: float = 0.0
    deepest_fill_pct: float = 0.0
    avg_deepest_pct: float = 0.0
    stranded_cost: float = 0.0  # cost basis of positions still open at the end
    stranded_value: float = 0.0  # what they were worth at the last close
    # Rungs the ladder funded but the market could never reach: a level deep
    # enough to price at or below zero, and one that price simply never touched
    # before the campaign ended.
    dead_rungs: int = 0
    dead_rung_usd: float = 0.0
    unreached_rungs: int = 0
    unreached_rung_usd: float = 0.0
    escalated: int = 0
    per_level_fills: Dict[str, int] = field(default_factory=dict)
    per_level_usd: Dict[str, float] = field(default_factory=dict)
    monthly: Dict[str, float] = field(default_factory=dict)
    seconds: float = 0.0

    @property
    def open_pnl(self) -> float:
        return self.stranded_value - self.stranded_cost

    @property
    def total_pnl(self) -> float:
        return self.net_pnl + self.open_pnl

    @property
    def return_pct(self) -> float:
        return self.total_pnl / self.capital * 100 if self.capital else 0.0


# ── the replay ────────────────────────────────────────────────────


class Replay:
    """Two years of 5m candles through the live state machine."""

    def __init__(self, symbol: str, rows: List[tuple], capital: float, cascade, escalate: bool = True):
        self.cascade = cascade
        self.symbol = symbol
        self.rows = rows
        self.capital = capital
        # Escalation is what lets one campaign live for eighteen months and
        # climb to weekly candles. Switching it off pins every campaign to 5m,
        # which is the only way to ask the ladder question on its own.
        self.escalate = escalate
        self.meta = SYMBOL_META.get(symbol, {"tick": 0.01, "min_notional": 5.0})
        self.engine = cascade.CascadeEngine(_OfflineBroker(symbol, self.meta))
        # Per-campaign aggregation state for its own timeframe.
        self.buckets: Dict[str, dict] = {}
        self.seen: set = set()
        self.result = RunResult(symbol=symbol, config="", label="", capital=capital)
        self._holds: List[float] = []
        self._deepest: List[float] = []
        self._in_position_bars = 0
        self._deployed_total = 0.0

    # -- campaign birth ------------------------------------------------

    def _median_bar_pct(self, index: int) -> float:
        """The instrument's median 5m bar over the trailing two days."""
        window = self.rows[max(0, index - 576) : index]
        pcts = sorted((r[2] - r[3]) / r[4] for r in window if r[4] > 0 and r[2] >= r[3])
        if len(pcts) < 30:
            return 0.0
        return pcts[len(pcts) // 2]

    def _seed_campaign(self, index: int) -> None:
        """Anchor a fresh campaign by hand, the way Phil starts one."""
        cascade = self.cascade
        ts, o, h, low, c = self.rows[index]
        median_bar = self._median_bar_pct(index)
        campaign = cascade.Campaign(
            campaign_id=f"seed{index}",
            seq=self.engine._next_seq(),
            symbol=self.symbol,
            capital_usd=self.capital,
            mother_high=h,
            mother_low=low,
            mother_timestamp=ts,
            mode="paper",
            timeframe=cascade.BASE_TIMEFRAME,
            start_timeframe=cascade.BASE_TIMEFRAME,
            escalates=True,
            mc_kind="major",
            exchange="",
            min_notional_usd=self.meta["min_notional"],
            min_fib_range_pct=cascade.min_fib_range_for(self.symbol, median_bar),
            median_bar_pct=median_bar,
            tick_size=self.meta["tick"],
            model_version=cascade.MODEL_VERSION,
            last_processed_ts=ts,
            mother_watch_last_5m_ts=ts,
            window_start_ts=ts,
        )
        self.engine.campaigns[campaign.campaign_id] = campaign
        self.engine._candles[campaign.campaign_id] = [
            cascade.Candle(ts, o, h, low, c, timeframe=cascade.BASE_TIMEFRAME)
        ]
        self.result.manual_starts += 1
        self._register(campaign, index)

    def _register(self, campaign, index: int) -> None:
        """First sight of a campaign: give it a bucket and a fresh measurement.

        A successor inherits its parent's median-bar measurement, which is right
        over a week and wrong over two years. Re-measuring here is the one place
        this harness deliberately differs from the live chain, and it applies to
        every config alike.
        """
        self.seen.add(campaign.campaign_id)
        self.result.campaigns += 1
        if not self.escalate:
            campaign.escalates = False
        median_bar = self._median_bar_pct(index)
        if median_bar > 0:
            campaign.median_bar_pct = median_bar
            campaign.min_fib_range_pct = self.cascade.min_fib_range_for(self.symbol, median_bar)
        self.buckets[campaign.campaign_id] = {"tf": campaign.timeframe_sec, "start": None, "bar": None}

    # -- candle delivery -----------------------------------------------

    def _emit_for(self, campaign, base) -> Optional[object]:
        """The campaign's own candle, aggregated up from 5m, or None mid-bucket."""
        cascade = self.cascade
        state = self.buckets[campaign.campaign_id]
        tf_sec = campaign.timeframe_sec
        if tf_sec != state["tf"]:  # just escalated — start clean on the next bucket
            state.update({"tf": tf_sec, "start": None, "bar": None})
        ts = base[0]
        if tf_sec == FIVE_MIN:
            return cascade.Candle(ts, base[1], base[2], base[3], base[4], timeframe=campaign.timeframe)
        bucket = ts - (ts % tf_sec)
        if state["start"] != bucket:
            # Only a bucket entered at its first 5m slot is a whole candle.
            state["start"] = bucket
            state["bar"] = [base[1], base[2], base[3], base[4]] if ts == bucket else None
        elif state["bar"] is not None:
            state["bar"][1] = max(state["bar"][1], base[2])
            state["bar"][2] = min(state["bar"][2], base[3])
            state["bar"][3] = base[4]
        if ts + FIVE_MIN != bucket + tf_sec or state["bar"] is None:
            return None
        o, h, low, c = state["bar"]
        state["bar"] = None
        return cascade.Candle(bucket, o, h, low, c, timeframe=campaign.timeframe)

    # -- accounting ----------------------------------------------------

    def _harvest(self, campaign, last_close: float) -> None:
        result = self.result
        result.fibs += sum(1 for leg in campaign.legs if leg.fib is not None)
        if any(leg.fib is not None for leg in campaign.legs):
            result.drew_structure += 1
        if campaign.has_escalated:
            result.escalated += 1
        deepest = None
        for rnd in campaign.rounds:
            result.rounds += 1
            result.net_pnl += rnd.pnl
            result.gross_pnl += rnd.pnl_gross
            result.fees += rnd.fees_usd
            result.wins += 1 if rnd.pnl > 0 else 0
            result.losses += 1 if rnd.pnl <= 0 else 0
            result.best_round = max(result.best_round, rnd.pnl)
            result.worst_round = min(result.worst_round, rnd.pnl)
            if rnd.opened_ts and rnd.closed_ts and rnd.closed_ts > rnd.opened_ts:
                self._holds.append((rnd.closed_ts - rnd.opened_ts) / 3600.0)
            month = datetime.fromtimestamp(rnd.closed_ts or 0, IST).strftime("%Y-%m")
            result.monthly[month] = round(result.monthly.get(month, 0.0) + rnd.pnl, 4)
            for fill in rnd.fills:
                result.fills += 1
                key = str(fill.get("level"))
                result.per_level_fills[key] = result.per_level_fills.get(key, 0) + 1
                result.per_level_usd[key] = round(result.per_level_usd.get(key, 0.0) + float(fill.get("usd") or 0), 2)
                price = float(fill.get("price") or 0)
                if price > 0:
                    deepest = price if deepest is None else min(deepest, price)
        # Anything still held when the campaign ended is a bag, not a profit.
        for fill in campaign.all_fills:
            result.fills += 1
            key = str(fill.level)
            result.per_level_fills[key] = result.per_level_fills.get(key, 0) + 1
            result.per_level_usd[key] = round(result.per_level_usd.get(key, 0.0) + fill.price * fill.quantity, 2)
            result.stranded_cost += fill.price * fill.quantity
            result.stranded_value += last_close * fill.quantity
            deepest = fill.price if deepest is None else min(deepest, fill.price)
        # Money the ladder set aside that the market never took. A rung priced
        # at or below zero is geometry that cannot exist; a rung merely never
        # reached is capital committed to a fall that did not come.
        for leg in campaign.legs:
            for order in leg.pending_orders.values():
                if order.status not in {"PENDING", "UNFUNDED"} or order.usd_notional <= 0:
                    continue
                if not order.price:
                    result.dead_rungs += 1
                    result.dead_rung_usd += order.usd_notional
                else:
                    result.unreached_rungs += 1
                    result.unreached_rung_usd += order.usd_notional
        if deepest and campaign.mother_high > 0:
            pct = (campaign.mother_high - deepest) / campaign.mother_high * 100
            self._deepest.append(pct)
            result.deepest_fill_pct = max(result.deepest_fill_pct, pct)

    # -- the loop ------------------------------------------------------

    def run(self, config: str, cfg: dict) -> RunResult:
        cascade = self.cascade
        result = self.result
        result.config, result.label = config, cfg["label"]
        started = time.time()
        rows = self.rows
        final_states = cascade.FINAL_STATES
        active_states = cascade.ACTIVE_STATES
        pending = cascade.MOTHER_BREAK_PENDING
        engine = self.engine

        self._seed_campaign(0)
        for index in range(1, len(rows)):
            base = rows[index]
            ts, _o, high, _low, close = base

            campaigns = list(engine.campaigns.values())
            for campaign in campaigns:
                if campaign.state in final_states:
                    continue
                if campaign.campaign_id not in self.seen:
                    self._register(campaign, index)  # born from a break this bar
                # A campaign never re-reads its own mother candle, but a
                # successor born from a settle bar a few candles back must not
                # sit out the bar it was created on either.
                if ts <= int(campaign.last_processed_ts or 0):
                    continue
                if campaign.state == pending:
                    # The settle window is counted in the venue's own 5m bars,
                    # whatever timeframe the frozen parent was stepping.
                    if ts > int(campaign.mother_break_last_5m_ts or 0):
                        campaign.mother_break_last_5m_ts = ts
                        engine._advance_mother_break_confirmation(
                            campaign,
                            cascade.Candle(ts, base[1], high, base[3], close, timeframe=cascade.BASE_TIMEFRAME),
                        )
                    continue
                # The break watcher runs on every bar, not on the campaign's own
                # slower candle — the live engine watches 1m for exactly this.
                if high > campaign.mother_high:
                    campaign.mother_watch_last_5m_ts = ts
                    engine._mother_broken(
                        campaign, cascade.Candle(ts, base[1], high, base[3], close, timeframe=cascade.BASE_TIMEFRAME)
                    )
                    continue
                candle = self._emit_for(campaign, base)
                if candle is None:
                    continue
                history = engine._candles.setdefault(campaign.campaign_id, [])
                history.append(candle)
                if len(history) > 20000:
                    del history[: len(history) - 20000]
                engine._process_candle(campaign, candle)
                campaign.last_processed_ts = candle.timestamp
                if campaign.state in active_states:
                    engine._maybe_escalate(campaign, candle)

            # Harvest and drop anything that ended, so neither the campaign dict
            # nor the per-candle loop grows with two years of history.
            alive = 0
            for cid, campaign in list(engine.campaigns.items()):
                if campaign.state in final_states:
                    self._harvest(campaign, close)
                    del engine.campaigns[cid]
                    engine._candles.pop(cid, None)
                    self.buckets.pop(cid, None)
                else:
                    alive += 1
            if engine.closed_campaigns:
                engine.closed_campaigns.clear()
            if not alive:
                self._seed_campaign(index)

            if index % 12 == 0:  # hourly sample of what capital is doing
                deployed = sum(c.spent_usd for c in engine.campaigns.values())
                result.deployed_samples += 1
                self._deployed_total += deployed
                result.peak_deployed = max(result.peak_deployed, deployed)
                if deployed > 0:
                    self._in_position_bars += 1

        last_close = rows[-1][4]
        for campaign in list(engine.campaigns.values()):
            self._harvest(campaign, last_close)

        result.bars = len(rows)
        result.avg_deployed = self._deployed_total / result.deployed_samples if result.deployed_samples else 0.0
        result.time_in_position_pct = (
            self._in_position_bars / result.deployed_samples * 100 if result.deployed_samples else 0.0
        )
        result.median_hold_hours = statistics.median(self._holds) if self._holds else 0.0
        result.max_hold_hours = max(self._holds) if self._holds else 0.0
        result.avg_deepest_pct = sum(self._deepest) / len(self._deepest) if self._deepest else 0.0
        result.seconds = time.time() - started
        return result


def run_one(args: tuple) -> dict:
    symbol, config, capital, months, escalate = args
    logging.getLogger("cryptoforge.cascade").setLevel(logging.CRITICAL)
    import engine.cascade as cascade

    cfg = CONFIGS[config]
    cascade.CASCADE_LEVELS = tuple(cfg["levels"])
    cascade.LEVEL_ALLOCATION = dict(cfg["alloc"])
    # Every rung but the deepest goes in as a buy stop above a falling market;
    # the deepest is the one worth owning at its own line, as a resting limit.
    cascade.STOP_ENTRY_LEVELS = tuple(cfg["levels"][:-1])

    rows = load_history(symbol, months)
    replay = Replay(symbol, rows, capital, cascade, escalate=escalate)
    result = replay.run(config, cfg)
    payload = {k: v for k, v in result.__dict__.items()}
    payload["open_pnl"] = result.open_pnl
    payload["total_pnl"] = result.total_pnl
    payload["return_pct"] = result.return_pct
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", action="append", help="repeatable; default BTCUSDT and SOLUSDT")
    parser.add_argument("--config", action="append", help="repeatable; default every config")
    parser.add_argument("--capital", type=float, default=2000.0, help="per campaign, USD")
    parser.add_argument("--months", type=int, default=24)
    parser.add_argument("--jobs", type=int, default=0, help="0 = one per core")
    parser.add_argument(
        "--no-escalate", action="store_true", help="pin every campaign to 5m instead of climbing the ladder"
    )
    parser.add_argument("--out", default="depth_sweep.json", help="filename under tools/.sweep_out")
    args = parser.parse_args()

    symbols = [s.upper() for s in (args.symbol or ["BTCUSDT", "SOLUSDT"])]
    configs = args.config or list(CONFIGS)
    for name in configs:
        if name not in CONFIGS:
            print(f"unknown config {name!r}; have: {', '.join(CONFIGS)}")
            return 1

    escalate = not args.no_escalate
    jobs = [(symbol, config, args.capital, args.months, escalate) for symbol in symbols for config in configs]
    workers = args.jobs or min(len(jobs), mp.cpu_count())
    print(
        f"{len(jobs)} runs on {workers} workers — {args.months} months, ${args.capital:,.0f} per campaign, "
        f"escalation {'on' if escalate else 'OFF (5m throughout)'}\n"
    )

    os.makedirs(OUT_DIR, exist_ok=True)
    results: List[dict] = []
    with mp.Pool(workers) as pool:
        for payload in pool.imap_unordered(run_one, jobs):
            results.append(payload)
            print(
                f"  done {payload['symbol']:<9} {payload['config']:<13} "
                f"net ${payload['total_pnl']:>10,.2f}  {payload['rounds']:>5} rounds  "
                f"({payload['seconds']:.0f}s)"
            )

    order = {name: i for i, name in enumerate(CONFIGS)}
    results.sort(key=lambda r: (r["symbol"], order.get(r["config"], 99)))
    path = os.path.join(OUT_DIR, args.out)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(results, handle, indent=2)
    print(f"\nwrote {path}")
    report(results)
    return 0


def report(results: List[dict]) -> None:
    for symbol in sorted({r["symbol"] for r in results}):
        rows = [r for r in results if r["symbol"] == symbol]
        print(f"\n{symbol}  —  {rows[0]['bars']:,} 5m bars, ${rows[0]['capital']:,.0f} per campaign")
        head = (
            f"  {'ladder':<26} {'net P&L':>10} {'ret%':>7} {'rounds':>7} {'win%':>6} "
            f"{'fills':>6} {'peak $':>9} {'in pos%':>8} {'open bag':>10}"
        )
        print(head)
        print("  " + "-" * (len(head) - 2))
        for r in rows:
            wins = r["wins"] / r["rounds"] * 100 if r["rounds"] else 0.0
            print(
                f"  {r['label']:<26} ${r['total_pnl']:>9,.2f} {r['return_pct']:>6.1f}% {r['rounds']:>7,} "
                f"{wins:>5.1f}% {r['fills']:>6,} ${r['peak_deployed']:>8,.0f} "
                f"{r['time_in_position_pct']:>7.1f}% ${r['open_pnl']:>9,.2f}"
            )


if __name__ == "__main__":
    raise SystemExit(main())
