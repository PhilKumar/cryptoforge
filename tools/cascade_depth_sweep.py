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

It also answers the other half of the question — not "how deep should the
ladder go" but "should the target let go" — with --trail (see _install_trail).

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

# ── minor mother candles ──────────────────────────────────────────
#
# The major chain anchors at a top and, when the market keeps falling, sits
# underwater with its target far overhead — SOL closed its last round in July
# 2025 and spent the next twelve months holding a bag. A MINOR MC is the answer
# Phil marks by hand: a lower high, inside a move already running, with a target
# close enough to actually be reached on the way down.
#
# Nothing here is new engine behaviour — mc_kind="minor" already exists and is
# always a 5m campaign. What the backtest has to invent is WHERE Phil would
# mark one, since live he picks them off the chart. The rule used:
#
#   a 5m swing high, confirmed by MINOR_SWING_BARS bars either side, that sits
#   at least MINOR_GAP_PCT below every live campaign's mother high and below
#   the current price, taken only while fewer than --minors minors are alive.
#
# Confirmation-by-hindsight is deliberate: the swing is only anchored once the
# bars after it have printed, so the campaign starts strictly in the past, the
# way Phil marks one after a high has clearly failed.

MINOR_SWING_BARS = 12  # an hour of 5m either side makes a high a swing high
MINOR_GAP_PCT = 0.05  # a new minor must sit 5% under every live mother


# ── capping the escalation ladder ─────────────────────────────────
#
# Today a campaign climbs 5m -> 15m -> 1h -> 4h -> 1d -> 1w and, once it is on
# a weekly candle anchored at a top, it effectively never dies. --cap-tf stops
# the climb at a chosen rung; when a campaign has outgrown even that rung it is
# RETIRED and a fresh 5m campaign is anchored in its place. What happens to coin
# it is still holding is the whole question, so it is a separate switch:
#
#   park  keep the old campaign alive for one purpose only — its 0.25 target
#         stays live and it sells if price ever gets there. It buys nothing
#         more. The fresh 5m campaign starts immediately alongside it.
#   sell  dump the position at the market price on the retiring bar, take
#         whatever loss that is, and start clean.
#   hold  walk away holding it: the position is never sold, and lands in the
#         stranded bag. The pessimistic floor.

RETIRE_MODES = ("park", "sell", "hold")
ESCALATION_RUNGS = ("5m", "15m", "1h", "4h", "1d", "1w")


# ── what makes a campaign climb ───────────────────────────────────
#
# Today escalation is a CLOCK: 200 bars since the mother candle and the campaign
# moves up a rung, so it is on 1H after two days and 1D after thirty-three,
# whether it booked twenty targets on 5m or none. The reason in the engine is a
# charting one — past 200 bars the mother slides off the left edge of its own
# chart — which ended up deciding trading behaviour.
#
#   bars       that clock, exactly as it runs today
#   structure  Phil's rule: stay on the rung while it is still working. Climb
#              only once the rung has BOOKED a profit and price has then broken
#              the low that was standing when that round closed. A rung that is
#              still paying is never abandoned; a rung whose floor has gone has
#              been outgrown by the move, which is the thing the timeframe is
#              supposed to track.
#
# `reuse_below` is already exactly that low — _close_round sets it to the
# lowest low in the campaign's history at the moment the round closes, and the
# engine uses it to decide when spent levels come back onto the ladder.

ESCALATE_MODES = ("bars", "structure")


# ── the trailing target ───────────────────────────────────────────


def _restore(cascade) -> None:
    """Undo the previous job's patches.

    A pool worker runs many jobs in one process, so both the trailing target
    (a patched method) and a capped ladder (a rebound module constant) survive
    into the NEXT job unless they are put back. Left unfixed this silently
    reports a trailed run as the fixed baseline, and crashes the second capped
    job because the ladder it indexes into has already been cut down.
    """
    global _PRISTINE_TP_CHECK
    if _PRISTINE_TP_CHECK is None:
        _PRISTINE_TP_CHECK = cascade.CascadeEngine._paper_tp_check
    global _PRISTINE_BARS
    if _PRISTINE_BARS is None:
        _PRISTINE_BARS = cascade.ESCALATION_BARS
    cascade.CascadeEngine._paper_tp_check = _PRISTINE_TP_CHECK
    cascade.ESCALATION_LADDER = ESCALATION_RUNGS
    cascade.ESCALATION_BARS = _PRISTINE_BARS


_PRISTINE_TP_CHECK = None
_PRISTINE_BARS = None


def _install_trail(cascade, giveback: float) -> None:
    """Replace the fixed 0.25 target with one that lets a winner run.

    Live, the target is a resting sell at `avg_entry + 0.25 x (mother_high -
    avg_entry)` and a round ends the moment it trades. Trailing keeps the same
    line but treats it as the ARMING price rather than the exit: once a candle
    trades through it, the round stays open and a stop follows the running high
    down by `giveback` x the target distance, never dropping below the target
    itself. So the worst a trailed round can do is exit exactly where the fixed
    one did — per round. Not per campaign: holding on keeps the levels the
    round bought off the ladder for longer and leaves the position exposed to a
    mother break, and only the two-year run can price that.

    giveback is measured in units of the target's own distance, not in percent,
    so it means the same thing on a $60,000 BTC leg and a $70 SOL one. At 0.5
    the stop starts rising only after price has run half a target beyond the
    target; at 1.0 it needs a full extra target first.

    Candle resolution is read pessimistically at both ends: the candle that
    arms the trail can never also be the one that gets stopped out of it, and
    the stop a candle can be taken at is always the one the PREVIOUS candle
    left behind, so an intrabar spike never books a better exit than it earned.
    """
    compute_tp_price = cascade.compute_tp_price

    def _trailing_tp_check(self, campaign, closed_candle) -> None:
        if campaign.filled_base_qty <= 0:
            campaign.trail_peak = None
            campaign.trail_stop = None
            return
        # A fill on this candle moves the average, so it moves the target the
        # trail was hung from. Drop the trail and let it re-arm off the new one.
        if any(fill.timestamp >= closed_candle.timestamp for fill in campaign.all_fills):
            campaign.trail_peak = None
            campaign.trail_stop = None
            return
        tp = compute_tp_price(campaign)
        distance = (tp or 0.0) - (campaign.avg_entry_price or 0.0)
        if not tp or distance <= 0:
            return
        stop = getattr(campaign, "trail_stop", None)
        if stop is not None:
            if closed_candle.low <= stop:
                self._close_round(campaign, stop)
                campaign.trail_peak = None
                campaign.trail_stop = None
                return
            peak = max(getattr(campaign, "trail_peak", None) or 0.0, closed_candle.high)
            campaign.trail_peak = peak
            campaign.trail_stop = max(tp, peak - giveback * distance)
            return
        if closed_candle.high < tp:
            return
        campaign.trail_peak = closed_candle.high
        campaign.trail_stop = max(tp, closed_candle.high - giveback * distance)

    cascade.CascadeEngine._paper_tp_check = _trailing_tp_check


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
    trail: float = 0.0  # 0 = the fixed target; otherwise the giveback multiple
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
    # The minor-MC half of the book, so "did the minors carry the fall" is
    # answerable without unpicking the totals.
    minors: int = 0  # how many were allowed to run at once
    minor_campaigns: int = 0
    minor_rounds: int = 0
    minor_pnl: float = 0.0
    minor_stranded_cost: float = 0.0
    minor_stranded_value: float = 0.0
    cap_tf: str = ""
    retire: str = ""
    retired: int = 0  # campaigns that outgrew the capped rung
    retired_holding: int = 0  # ...and were still holding coin when they did
    retired_sold_at_target: int = 0  # parked ones that later reached the target
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

    def __init__(
        self,
        symbol: str,
        rows: List[tuple],
        capital: float,
        cascade,
        escalate: bool = True,
        minors: int = 0,
        minor_gap: float = MINOR_GAP_PCT,
        cap_tf: str = "",
        retire: str = "park",
        escalate_on: str = "bars",
    ):
        self.cascade = cascade
        self.symbol = symbol
        self.rows = rows
        self.capital = capital
        # Escalation is what lets one campaign live for eighteen months and
        # climb to weekly candles. Switching it off pins every campaign to 5m,
        # which is the only way to ask the ladder question on its own.
        self.escalate = escalate
        # How many minor MCs may run alongside the major chain at once, and how
        # far below every live mother a new one has to sit. 0 is today.
        self.minors = minors
        self.minor_gap = minor_gap
        # The rung a campaign may not climb past, and what to do with the coin
        # it is holding when it outgrows even that.
        self.cap_tf = cap_tf
        self.retire = retire
        self.parked: Dict[str, object] = {}
        # Per-campaign, per-rung bookkeeping for the structure gate.
        self.escalate_on = escalate_on
        self.rungs: Dict[str, dict] = {}
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

    def _seed_campaign(self, index: int, mother: Optional[int] = None, kind: str = "major") -> None:
        """Anchor a fresh campaign by hand, the way Phil starts one.

        `mother` is the bar the campaign is anchored to, which for a minor MC is
        a confirmed swing high some bars back rather than the current one.
        """
        cascade = self.cascade
        mother_index = index if mother is None else mother
        ts, o, h, low, c = self.rows[mother_index]
        median_bar = self._median_bar_pct(index)
        campaign = cascade.Campaign(
            campaign_id=f"seed{index}" if kind == "major" else f"minor{index}",
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
            mc_kind=kind,
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
        # A minor is marked in hindsight, so the bars between its mother and
        # now are history the campaign has already lived through — feed them in
        # rather than starting it blind at the current price.
        if mother_index < index:
            for row in self.rows[mother_index + 1 : index]:
                self.engine._candles[campaign.campaign_id].append(
                    cascade.Candle(row[0], row[1], row[2], row[3], row[4], timeframe=cascade.BASE_TIMEFRAME)
                )
        self.result.manual_starts += 1
        if kind == "minor":
            self.result.minor_campaigns += 1
        self._register(campaign, index)

    def _minor_anchor(self, index: int) -> Optional[int]:
        """The bar a new minor MC should hang from, or None.

        A swing high confirmed MINOR_SWING_BARS bars each side, sitting under
        both the current price and every live mother by at least minor_gap.
        """
        pivot = index - MINOR_SWING_BARS
        if pivot <= MINOR_SWING_BARS:
            return None
        high = self.rows[pivot][2]
        window = self.rows[pivot - MINOR_SWING_BARS : index + 1]
        if any(row[2] > high for row in window):
            return None
        # It has to be a high price has since failed to hold, not the top of a
        # market that is currently making new ones.
        if self.rows[index][4] >= high * (1 - self.minor_gap):
            return None
        for campaign in self.engine.campaigns.values():
            if campaign.state in self.cascade.FINAL_STATES:
                continue
            if high > campaign.mother_high * (1 - self.minor_gap):
                return None
        return pivot

    def _may_climb(self, campaign, candle) -> bool:
        """Phil's gate: this rung booked a profit, and its floor has since gone.

        Returns True only on the bar the rung is judged to have been outgrown.
        Once armed it stays armed, because the engine still insists on a clean
        bucket boundary and may refuse the climb for a few bars.
        """
        state = self.rungs.get(campaign.campaign_id)
        if state is None or state["tf"] != campaign.timeframe:
            # New rung (or a new campaign): the count starts again here.
            state = {"tf": campaign.timeframe, "rounds": len(campaign.rounds), "floor": None}
            self.rungs[campaign.campaign_id] = state
        if state["floor"] is None:
            if len(campaign.rounds) <= state["rounds"]:
                return False  # nothing booked on this rung yet — it is still working
            # reuse_below is the low that was standing when the round closed.
            state["floor"] = campaign.reuse_below or campaign.rounds[-1].exit_price
        return candle.low < state["floor"]

    def _outgrew_cap(self, campaign, candle) -> bool:
        """True once a campaign sitting on the capped rung would have climbed.

        Same measure the engine escalates on — bars since the mother on the
        current rung — so retirement happens exactly where the next escalation
        would have, not at some new threshold invented here.
        """
        if campaign.timeframe != self.cap_tf:
            return False
        if campaign.pending_stop_price is not None:
            return False  # mid-arm; let the buy stop resolve first
        if self.escalate_on == "structure":
            # Retire on whatever would have promoted it, so the cap and the
            # climb are judged by the same rule.
            return self._may_climb(campaign, candle)
        seconds = campaign.timeframe_sec
        bars = (candle.timestamp + seconds - campaign.mother_timestamp) / seconds
        return bars > self.cascade.ESCALATION_BARS

    def _retire(self, campaign, price: float) -> None:
        """Take a campaign off the ladder and let a fresh 5m one replace it."""
        result = self.result
        result.retired += 1
        holding = campaign.filled_base_qty > 0
        if holding:
            result.retired_holding += 1
        if not holding:
            campaign.state = "COMPLETED"
            return
        if self.retire == "sell":
            # Whatever it is worth on this bar, loss included.
            self.engine._close_round(campaign, price)
            campaign.state = "COMPLETED"
            return
        if self.retire == "hold":
            campaign.state = "STOPPED"  # never sold; _harvest counts it stranded
            return
        # park: out of the engine's hands, but its target stays live.
        self.engine.campaigns.pop(campaign.campaign_id, None)
        self.buckets.pop(campaign.campaign_id, None)
        self.parked[campaign.campaign_id] = campaign

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
        minor = str(campaign.mc_kind or "major").lower() == "minor"
        result.fibs += sum(1 for leg in campaign.legs if leg.fib is not None)
        if any(leg.fib is not None for leg in campaign.legs):
            result.drew_structure += 1
        if campaign.has_escalated:
            result.escalated += 1
        deepest = None
        for rnd in campaign.rounds:
            result.rounds += 1
            result.net_pnl += rnd.pnl
            if minor:
                result.minor_rounds += 1
                result.minor_pnl += rnd.pnl
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
            if minor:
                result.minor_stranded_cost += fill.price * fill.quantity
                result.minor_stranded_value += last_close * fill.quantity
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
                    if self.escalate_on == "bars" or self._may_climb(campaign, candle):
                        engine._maybe_escalate(campaign, candle)
                    if self.cap_tf and self._outgrew_cap(campaign, candle):
                        self._retire(campaign, close)

            # A parked campaign has stopped laddering; the only thing left for
            # it to do is sell into its own target.
            for cid, campaign in list(self.parked.items()):
                bar = cascade.Candle(ts, base[1], high, base[3], close, timeframe=cascade.BASE_TIMEFRAME)
                # One candle deep, kept current: _close_round reads the last
                # candle for its timestamp, and a parked campaign that kept its
                # whole history would carry two years of bars per park.
                engine._candles[cid] = [bar]
                engine._paper_tp_check(campaign, bar)
                if campaign.filled_base_qty <= 0:
                    result.retired_sold_at_target += 1
                    self._harvest(campaign, close)
                    engine._candles.pop(cid, None)
                    del self.parked[cid]

            # Harvest and drop anything that ended, so neither the campaign dict
            # nor the per-candle loop grows with two years of history.
            majors = minors_alive = 0
            for cid, campaign in list(engine.campaigns.items()):
                if campaign.state in final_states:
                    self._harvest(campaign, close)
                    del engine.campaigns[cid]
                    engine._candles.pop(cid, None)
                    self.buckets.pop(cid, None)
                elif str(campaign.mc_kind or "major").lower() == "minor":
                    minors_alive += 1
                else:
                    majors += 1
            if engine.closed_campaigns:
                engine.closed_campaigns.clear()
            # The major chain is re-anchored on its own account: with minors
            # running, "some campaign is alive" would let the major line die
            # out and never come back.
            if not majors:
                self._seed_campaign(index)
            elif minors_alive < self.minors:
                anchor = self._minor_anchor(index)
                if anchor is not None:
                    self._seed_campaign(index, mother=anchor, kind="minor")

            if index % 12 == 0:  # hourly sample of what capital is doing
                # Parked campaigns are out of the engine's dict but their coin
                # is still bought and paid for — leaving them out would report
                # a capped ladder as using far less money than it does.
                deployed = sum(c.spent_usd for c in engine.campaigns.values()) + sum(
                    c.spent_usd for c in self.parked.values()
                )
                result.deployed_samples += 1
                self._deployed_total += deployed
                result.peak_deployed = max(result.peak_deployed, deployed)
                if deployed > 0:
                    self._in_position_bars += 1

        last_close = rows[-1][4]
        for campaign in list(engine.campaigns.values()):
            self._harvest(campaign, last_close)
        # A parked campaign whose target never came is a bag like any other.
        for campaign in list(self.parked.values()):
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
    symbol, config, capital, months, escalate, trail, minors, cap_tf, retire, escalate_on = args
    logging.getLogger("cryptoforge.cascade").setLevel(logging.CRITICAL)
    import engine.cascade as cascade

    cfg = CONFIGS[config]
    _restore(cascade)
    if trail > 0:
        _install_trail(cascade, trail)
    if cap_tf:
        # Shortening the ladder is all it takes: `can_escalate` and
        # `next_timeframe_up` both read it, so nothing climbs past the cap.
        cascade.ESCALATION_LADDER = ESCALATION_RUNGS[: ESCALATION_RUNGS.index(cap_tf) + 1]
    if escalate_on == "structure":
        # The gate is the Replay's, not the engine's: stand the clock down so
        # the only thing deciding a climb is whether the rung was outgrown.
        cascade.ESCALATION_BARS = 0
    cascade.CASCADE_LEVELS = tuple(cfg["levels"])
    cascade.LEVEL_ALLOCATION = dict(cfg["alloc"])
    # Every rung but the deepest goes in as a buy stop above a falling market;
    # the deepest is the one worth owning at its own line, as a resting limit.
    cascade.STOP_ENTRY_LEVELS = tuple(cfg["levels"][:-1])

    rows = load_history(symbol, months)
    replay = Replay(
        symbol,
        rows,
        capital,
        cascade,
        escalate=escalate,
        minors=minors,
        cap_tf=cap_tf,
        retire=retire,
        escalate_on=escalate_on,
    )
    replay.result.trail = trail
    replay.result.minors = minors
    replay.result.cap_tf = cap_tf
    replay.result.retire = retire if cap_tf else ""
    result = replay.run(config, cfg)
    suffix = ""
    if trail > 0:
        suffix += f" · trail {trail:g}"
    if minors:
        suffix += f" · +{minors} minor MC"
    if cap_tf:
        suffix += f" · cap {cap_tf}/{retire}"
    if escalate_on != "bars":
        suffix += " · climb on structure"
    if suffix:
        result.label = cfg["label"] + suffix
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
    parser.add_argument(
        "--trail",
        action="append",
        type=float,
        help="repeatable; trailing target, giveback as a multiple of the target distance "
        "(0 = today's fixed 0.25 target). e.g. --trail 0 --trail 0.5",
    )
    parser.add_argument(
        "--minors",
        action="append",
        type=int,
        help="repeatable; how many minor MCs may run alongside the major chain (0 = today)",
    )
    parser.add_argument(
        "--cap-tf",
        action="append",
        help="repeatable; highest rung a campaign may climb to before it retires and a "
        f"fresh 5m one replaces it (one of {', '.join(ESCALATION_RUNGS)}; omit for today's full ladder)",
    )
    parser.add_argument(
        "--retire",
        action="append",
        choices=RETIRE_MODES,
        help=f"repeatable; what a retiring campaign does with coin it still holds ({'/'.join(RETIRE_MODES)})",
    )
    parser.add_argument(
        "--escalate-on",
        action="append",
        choices=ESCALATE_MODES,
        help="repeatable; 'bars' is today's 200-bar clock, 'structure' climbs only once the rung "
        "booked a profit and price then broke the low standing at that close",
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
    trails = args.trail if args.trail is not None else [0.0]
    minors = args.minors if args.minors is not None else [0]
    caps = args.cap_tf if args.cap_tf is not None else [""]
    retires = args.retire if args.retire is not None else ["park"]
    for cap in caps:
        if cap and cap not in ESCALATION_RUNGS:
            print(f"unknown --cap-tf {cap!r}; have: {', '.join(ESCALATION_RUNGS)}")
            return 1
    climbs = args.escalate_on if args.escalate_on is not None else ["bars"]
    jobs = [
        (symbol, config, args.capital, args.months, escalate, trail, minor, cap, retire, climb)
        for symbol in symbols
        for config in configs
        for trail in trails
        for minor in minors
        for cap in caps
        for retire in (retires if cap else ["park"])
        for climb in climbs
    ]
    workers = args.jobs or min(len(jobs), mp.cpu_count())
    print(
        f"{len(jobs)} runs on {workers} workers — {args.months} months, ${args.capital:,.0f} per campaign, "
        f"escalation {'on' if escalate else 'OFF (5m throughout)'}, "
        f"targets {', '.join('fixed' if t <= 0 else f'trail {t:g}' for t in trails)}, "
        f"minor MCs {', '.join(str(m) for m in minors)}, "
        f"ladder cap {', '.join(c or 'none' for c in caps)}\n"
    )

    os.makedirs(OUT_DIR, exist_ok=True)
    results: List[dict] = []
    with mp.Pool(workers) as pool:
        for payload in pool.imap_unordered(run_one, jobs):
            results.append(payload)
            target = "fixed" if payload.get("trail", 0.0) <= 0 else f"trail {payload['trail']:g}"
            print(
                f"  done {payload['symbol']:<9} {payload['config']:<13} {target:<10} "
                f"net ${payload['total_pnl']:>10,.2f}  {payload['rounds']:>5} rounds  "
                f"({payload['seconds']:.0f}s)"
            )

    order = {name: i for i, name in enumerate(CONFIGS)}
    results.sort(
        key=lambda r: (
            r["symbol"],
            order.get(r["config"], 99),
            r.get("cap_tf", ""),
            r.get("retire", ""),
            r.get("minors", 0),
            r.get("trail", 0.0),
        )
    )
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
            f"  {'ladder':<44} {'realised':>10} {'net P&L':>10} {'rounds':>7} "
            f"{'$/round':>8} {'peak $':>9} {'open bag':>11} {'retired':>9} {'sold@tgt':>9}"
        )
        print(head)
        print("  " + "-" * (len(head) - 2))
        for r in rows:
            per_round = r["net_pnl"] / r["rounds"] if r["rounds"] else 0.0
            retired = f"{r.get('retired_holding', 0)}/{r.get('retired', 0)}"
            print(
                f"  {r['label']:<44} ${r['net_pnl']:>9,.2f} ${r['total_pnl']:>9,.2f} "
                f"{r['rounds']:>7,} ${per_round:>7,.2f} ${r['peak_deployed']:>8,.0f} "
                f"${r['open_pnl']:>10,.2f} {retired:>9} {r.get('retired_sold_at_target', 0):>9,}"
            )


if __name__ == "__main__":
    raise SystemExit(main())
