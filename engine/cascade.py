"""
engine/cascade.py — autonomous "cascade" campaign engine.

Model (as specified by the user against their TradingView drawings):
- A campaign anchors on a manually chosen MOTHER CANDLE.
- A DIP is the running low; a higher high after it is the RISE, which
  confirms the dip. Any red candle CLOSING BELOW the dip cuts the swing; a
  cut of a confirmed dip is what draws a trendline and a fib.
- The TRENDLINE runs from the mother high to a red candle's open, picked by
  find_valid_anchor2: the tightest descending line no earlier close has
  crossed. It is the same line you get dragging from the mother candle with
  TradingView's magnet on.
- FIB 0 is the highest high that reached that line — touch or break —
  between the dip and the cut. FIB 1 is the dip.
- BUY orders go on fib levels 2/4/8 with 20/30/50% of the leg's pool. The
  first fib funds off the fall from the mother high to its own level 1; each
  later fib funds off the remaining move from the previous fib's level 1 to
  its own.
- Levels 2 and 4 do NOT rest on their line. They go in as BUY STOPS whose
  trigger is the PREVIOUS red candle's close, so the order sits above a
  falling market and steps down with it, filling only when price U-turns
  back up through the last red body. Two reds under the line are needed
  before one is placed. Level 8 rests as a plain limit on its line.
  See STOP_ENTRY_LEVELS and _advance_stop_entries.
- Take profit is measured FROM the average entry back toward the mother
  high — avg_entry + 0.25 x (mother_high - avg_entry) — and only exists once
  an entry has filled.
- Binance min-notional handling: per-level USD below the minimum merges into
  the next SHALLOWER level (8->4->2), so a pool too small to fill the ladder
  buys where price actually trades instead of parking below it; if even the
  pooled amount is below the minimum it carries forward to the next leg.

There is no candle-count logic anywhere — only rises, touches and cuts.

A structure may sit anywhere relative to the mother candle — above its low or
below it. The only size test is MIN_FIB_RANGE_PCT, which throws out a few ticks
of chop whose fib levels would be noise rather than support.

A rise back to within MOTHER_RETEST_PCT of the mother high (once price has left
the mother candle's range) retires that mother candle and restarts on the rise:
a trendline drawn to a point that close comes out flat and can never be touched.

Campaigns default to paper mode (simulated fills at live prices). Live mode
uses a desired-state sync: the state machine only mutates local order intents
and _sync_live_orders diffs them against the exchange's open orders, placing,
cancelling, and ingesting fills idempotently (client ids cf-csc-{...}).

Stored campaigns keep the geometry the rules produced when they ran, so
MODEL_VERSION stamps them and recalculate_campaign() replays one from its
mother candle under the current rules.
"""

from __future__ import annotations

import asyncio
import fcntl
import logging
import os
import time
import uuid
from dataclasses import MISSING, dataclass, field
from dataclasses import fields as dataclass_fields
from datetime import datetime, timedelta, timezone
from decimal import ROUND_DOWN, Decimal, InvalidOperation
from typing import Callable, Dict, Iterable, List, Optional, Tuple

_log = logging.getLogger("cryptoforge.cascade")

# Every user-facing time in this app is IST. The event log, closed_at and
# created_at used a bare datetime.now(), which is the SERVER clock — UTC on the
# Lightsail box — so the log read 5.5 hours behind the chart, whose candle
# stamps are converted to IST. One helper, so the two never disagree again.
_IST = timezone(timedelta(hours=5, minutes=30))


def _ist_now_str() -> str:
    return datetime.now(_IST).strftime("%Y-%m-%d %H:%M:%S")


CASCADE_LEVELS = (2, 4, 8)
LEVEL_ALLOCATION = {2: 0.20, 4: 0.30, 8: 0.50}
BASE_TIMEFRAME = "5m"
ESCALATION_THRESHOLD_PCT = 1.0
TP_FIB_LEVEL = 0.25
# Levels 2 and 4 are shallow: resting a limit there buys a knife price is still
# falling through. They instead go in as BUY STOPS above a falling market, which
# only fill once the market turns back up. Level 8 is the level worth owning at
# the line itself, so it stays a plain resting limit.
STOP_ENTRY_LEVELS = (2, 4)
# Gap between the stop trigger and the limit cap, in exchange ticks. On BTCUSDT
# (tick 0.01) that is 0.05 — a stop at 66,067.78 caps at 66,067.83.
STOP_LIMIT_OFFSET_TICKS = 5
# How far the limit sits above the trigger, per symbol. The gap has to be wide
# enough that the order still fills when the turn is quick, and tight enough
# that it never pays much over the trigger. Phil set these by feel per market:
# SOL moves in bigger relative steps than BTC or PAXG, so it wants 2 cents flat
# rather than five ticks.
STOP_LIMIT_GAP_USD = {"SOLUSDT": 0.02}
# The most a buy stop may be raised above its trigger and still count as a
# legitimate LIVE cross. When price is at or just above a freshly-set trigger
# the stop is raised to sit just over the market — a real continuation up. But
# when a campaign starts LATE or off a candle read from the left, the replay
# collects a fall that already bottomed and bounced DAYS ago, and the current
# price is far above the trigger. Arming there buys over value on no new low —
# exactly what the new-low rule forbids ([[proj_cascade_new_low_rule]]). So a
# raise beyond this fraction is refused: the pot stays collected and unarmed,
# and the walk-down re-arms it only when a genuine new low prints below the
# trigger. A live cross lands within a tick or two (well under 0.1% on the
# moves seen); the late-start bug that motivated this was 1.39% above.
MAX_STOP_RAISE_PCT = 0.005
DEFAULT_TICK_SIZE = 0.01
# Fallback LOT_SIZE step when the exchange filter is unavailable. Shared by
# _floor_to_step and the TP quantity-drift tolerance so the granularity used
# to round a sell can never disagree with the granularity used to judge it.
DEFAULT_LOT_STEP = "0.00000001"
# A mother break rolls straight into a fresh campaign on the breaking candle.
# If price simply rips upward, every candle would break its predecessor, so a
# run of restarts that never draws a fib is capped rather than left unbounded.
MAX_BARREN_AUTO_RESTARTS = 10
# Watchdogs. The engine can look healthy on screen while quietly not stepping
# candles at all, and auto-restart can pile up more campaigns than a person can
# hold in their head — both are worth a push notification.
# A rise that gets within this much of the mother high is treated as a retest:
# the trendline it would produce is too flat to be worth drawing, so that candle
# becomes the new mother candle. 0.05% is ~$33 on BTC at 66,354. It has to stay
# well under 0.121%, which is how close a rise came on 2026-07-20 without the
# structure being spent — that day went on to draw a second fib.
# A fib needs a real swing behind it. Two bars of chop 15 points apart would
# put level 2 thirty points down — noise dressed as structure. The smallest fib
# verified against TradingView is 0.132% (2026-07-20 18:10), so 0.08% clears the
# junk with room to spare. A fib may sit anywhere relative to the mother candle,
# above or below its low; only the size matters.
#
# This is the CEILING, not the answer — see min_fib_range_for(). It was measured
# on BTC, and as a flat percentage of price it does not travel: 0.08% is 0.81x a
# median BTC 5m bar but 1.53x a PAXG one, so gold-backed pairs were being asked
# for a swing half again bigger than their own bars. PAXGUSDT on 2026-07-28 drew
# nothing for 24 candles while the fall was plain on the chart; the structure the
# engine found matched Phil's hand-drawn fib to the cent (0=4042.80, 1=4039.87)
# and was thrown away for being $0.30 short of this number.
MIN_FIB_RANGE_PCT = 0.0008
# A fib must be at least this many median 5m bars tall. One bar: a swing smaller
# than the instrument's own typical candle is chop by definition, whatever that
# is worth in percent. Measured once when the campaign starts and then held, so
# a threshold never drifts underneath a running campaign.
#
# Deliberately NOT derived from BTC's ratio (0.81). Setting it there put BTC's
# own scaled floor a hair UNDER the flat number, so the clamp below stopped
# binding and BTC quietly loosened by 2% — the one thing this change promised
# not to do. A test pins that.
FIB_RANGE_BAR_RATIO = 1.0
# ...but it may only ever LOOSEN. min() with the BTC number means every symbol
# noisier than BTC — ETH and SOL sit at 0.5x a bar — keeps exactly the behaviour
# it has today, and the verified BTC days are untouched. Only instruments quieter
# than BTC get relief, which is the actual bug.
#
# The hard floor stops an illiquid or halted market, whose bars measure near
# zero, from admitting a two-tick wobble as structure.
MIN_FIB_RANGE_FLOOR_PCT = 0.0002
# Explicit per-symbol overrides, for when a measurement has to be overruled by
# hand. Empty by design: the automatic rule is meant to handle a symbol that has
# never been traded here before, without anyone remembering to add a row.
MIN_FIB_RANGE_PCT_BY_SYMBOL: Dict[str, float] = {}


def repair_scaled_fib_range(min_fib_range_pct: float, median_bar_pct: float) -> Tuple[float, float]:
    """Undo the ×100-per-restart inflation of a restored fib-size gate.

    Every value min_fib_range_for can return is clamped into
    [MIN_FIB_RANGE_FLOOR_PCT, MIN_FIB_RANGE_PCT], so a stored number outside
    that window is not a threshold anyone chose — it is a display percent that
    was persisted and read back as a fraction, once per restart. Prod carried
    0.0008 recorded as 8,000,000: five round trips, and a gate demanding a fib
    8,000,000% tall, which no swing on any instrument can ever clear. Those
    campaigns stopped drawing fibs and trendlines entirely while still holding
    their funded band, so every sibling born under them netted to zero too.

    Dividing back by 100 until it lands in the window recovers the original
    exactly, because the corruption is exactly ×100 and the window is narrower
    than that step. `median_bar_pct` rode along in the same payload and took
    the same number of round trips, so it is corrected by the same count rather
    than guessed at from its own range.

    Returns the pair unchanged when the gate is already sane, which is the
    normal case and the case for anything saved after the write side was fixed.
    """
    gate = _coerce_float(min_fib_range_pct)
    bar = _coerce_float(median_bar_pct)
    if gate <= 0:
        return MIN_FIB_RANGE_PCT, bar
    steps = 0
    while gate > MIN_FIB_RANGE_PCT and steps < 12:
        gate /= 100.0
        steps += 1
    if not (MIN_FIB_RANGE_FLOOR_PCT <= gate <= MIN_FIB_RANGE_PCT):
        # Not a clean multiple of the corruption — refuse to invent a
        # threshold and fall back to the calibrated one, which is what a
        # campaign with no measurement gets anyway.
        return MIN_FIB_RANGE_PCT, (bar / (100.0**steps) if steps else bar)
    return gate, (bar / (100.0**steps) if steps else bar)


def min_fib_range_for(symbol: str, median_bar_pct: float) -> float:
    """The smallest swing that counts as structure on this instrument.

    `median_bar_pct` is the median 5m high-low range as a FRACTION of price.
    0.0 means it could not be measured, in which case the BTC-calibrated
    constant stands — a missing measurement must never loosen a real threshold.
    """
    override = MIN_FIB_RANGE_PCT_BY_SYMBOL.get(str(symbol or "").upper())
    if override and override > 0:
        return float(override)
    if median_bar_pct <= 0:
        return MIN_FIB_RANGE_PCT
    scaled = median_bar_pct * FIB_RANGE_BAR_RATIO
    return max(min(scaled, MIN_FIB_RANGE_PCT), MIN_FIB_RANGE_FLOOR_PCT)


MOTHER_RETEST_PCT = 0.0005
# Before a rise can count as a RETRACEMENT back to the mother high, price has to
# have gone somewhere first. Arming on "traded below the mother candle's low"
# was not enough: on a 1m chart the mother's own body is a few ticks tall, so
# the very next bar straddles it and the bar after that wicks back to the high —
# five of the replayed days died on their first or second candle that way,
# before any structure could form. A real departure is a fall of this much from
# the mother HIGH, which is an order of magnitude past the retest tolerance.
MOTHER_DEPART_PCT = 0.005
MAX_ACTIVE_BEFORE_ALERT = 10
STALL_ALERT_SEC = 15 * 60
# How many closed campaigns stay in memory. This was written as a bare 50 in
# _archive_campaign while _adopt_ended_campaigns, load_closed_campaigns and the
# persistence layer all used 100 — so every archive quietly threw away 50
# campaigns the rest of the system was trying to keep, and the panel lost
# history that was still in the database. One name, one number.
CLOSED_HISTORY_LIMIT = 100
MODEL_VERSION = 20  # bump when the fib/trendline rules change; older campaigns are flagged stale
# A cut must close below the frozen dip by at least this fraction of price.
# "Decisive break" (cascade_lib's own term): probes a few dollars under the
# dip are the fall resuming, not a completed swing being cut.
DECISIVE_BREAK_PCT = 0.0002
# Two consecutive structures whose touch highs (fib level 0) sit within this
# fraction of each other are the same shelf — the second one's ladder would
# overlap the first's and cancel orders that were about to fill. Calibrated
# against both verified days: keepers separate by 0.055% and 0.173%, the
# skipped one by 0.015%.
MIN_LEG_SEPARATION_PCT = 0.0003
# How many times the SAME trigger may be sent before we stop retrying. A stop
# walking down a real fall changes price every time and is never throttled;
# only a trigger that will not stay resting hits this.
_MAX_SAME_TRIGGER_PLACEMENTS = 3
# Every trendline starts at the same point — the mother high — so two of them
# are the same line whenever their second anchors are close. Drawn on the chart
# they overlap into one thick smear, which is not what gets drawn by hand: the
# charts show two or three clearly separated lines, never four near-parallel
# ones. A new line has to sit this far from each existing line, measured at the
# candle that created it, or it reuses the line already there.
MIN_TRENDLINE_SEPARATION_PCT = 0.0015
# How far a close may poke above a candidate trendline before it disqualifies
# the anchor. Nobody drags a line that respects every close to the cent, and
# testing that way froze SOLUSDT #10's anchor for a whole day over three
# ONE-CENT overshoots, so no fifth trendline could ever be drawn.
#
# Swept against every anchor Phil has confirmed. Only 0.04%-0.05% satisfies all
# of them at once, so this sits in the middle of that band:
#   0.00%  PAXG TL2 lands at 4063.83 @ 15:50, not his 4064.83 @ 16:10; SOL
#          never gets its fifth line
#   0.02%  fixes both, but BTC #36 fib 3's dip drifts 66,052.63 -> 66,098.71
#   0.06%  BTC holds, but PAXG TL2 slides to 4062.73 @ 16:15
# Widening this is not a free knob: it moves anchors he has drawn by hand.
ANCHOR_CLOSE_TOLERANCE_PCT = 0.00045
MIN_NOTIONAL_FLOOR_USD = 5.0  # Binance Spot MIN_NOTIONAL filter is ~$5 on USDT pairs
# Cushion over the exchange minimum on every rung. An order sized exactly at
# MIN_NOTIONAL is one tick of adverse quote movement from being rejected, so
# each rung carries 10% more: $5.50 against a $5 minimum.
RUNG_BUFFER_PCT = 0.10
# Order states whose money is gone or committed elsewhere — never re-rung.
SPENT_ORDER_STATES = frozenset({"FILLED", "CLOSED", "CANCELLED"})
FIVE_MIN_SEC = 300
FIFTEEN_MIN_SEC = 900
# Every timeframe the engine can step, in bar seconds. The names are exactly
# what the broker's kline map expects.
TIMEFRAME_SECONDS = {
    "1m": 60,  # replay/backtest only — not a campaign starting timeframe
    "5m": FIVE_MIN_SEC,
    "15m": FIFTEEN_MIN_SEC,
    "1h": 3600,
    "4h": 4 * 3600,
    "1d": 86400,
    "1w": 7 * 86400,
}
# The ladder a campaign climbs as it outgrows the screen (Phase 2 walks it).
# Every rung climbs to the next one — 5m to 15m, 15m to 1H, 1H to 4H — so where
# a campaign STARTS on the ladder only decides where it joins, not whether it
# moves. Capped at 4H on purpose: a campaign quietly becoming a weekly position
# is too big a change of character to happen by itself, which is also why 1D and
# 1W are not on the ladder at all.
ESCALATION_LADDER = ("5m", "15m", "1h", "4h")
# What may be picked when STARTING a campaign. The timeframe and the KIND are
# two separate questions, and the form asks both:
#   kind = minor       — a sub-mother marked inside a move that is already
#                        running. ALWAYS 5m, whatever chart it was spotted on —
#                        viewing 1D or 1H and marking a minor high does not make
#                        that high a 1D or 1H structure, so a mc_kind of "minor"
#                        overrides any timeframe sent with it.
#   kind = major       — a campaign anchored to its own candle, on whichever of
#                        these timeframes that candle belongs to. 5m included: a
#                        fresh campaign off a recent high is a major, and used to
#                        be mislabelled minor when the kind was inferred from
#                        the timeframe alone.
# Whether a campaign escalates is decided by the LADDER, not by which of those
# two it is: start at 5m, 15m or 1H and it climbs toward the 4H cap. Start at
# 4H, 1D or 1W and it is fixed — 4H because it is already the cap, 1D and 1W
# because they are off the ladder by design.
CAMPAIGN_START_TIMEFRAMES = ("5m", "15m", "1h", "4h", "1d", "1w")
# The rungs that still have somewhere to climb to. Starting on one of these is
# what makes a campaign an escalating one.
ESCALATING_START_TIMEFRAMES = ESCALATION_LADDER[:-1]  # 5m, 15m, 1h
# When a campaign has more bars than this behind it on its current rung, it has
# outgrown the screen and climbs to the next one. 200 matches the chart budget:
# the campaign chart draws its last ~200 buckets, so past this the mother candle
# starts sliding off the left edge of its own chart.
ESCALATION_BARS = 200
MC_KINDS = ("major", "minor")
MINOR_MC_TIMEFRAME = BASE_TIMEFRAME
# How far back a mother candle may be anchored, measured in bars of the
# campaign's own timeframe rather than in days. 5000 bars is 30 pages of the
# broker's 1000-bar klines with room to spare.
MAX_REPLAY_BARS = 5000
# Roll-ups offered on the campaign chart: the campaign's own timeframe plus the
# next two above it. View-only — the geometry is always computed on the
# campaign's engine timeframe, whatever the chart is being read at.
CHART_TIMEFRAME_LADDER = ("1m", "5m", "15m", "1h", "4h", "1d", "1w")


def timeframe_seconds(timeframe: str) -> int:
    return TIMEFRAME_SECONDS.get(str(timeframe or "").lower(), FIVE_MIN_SEC)


def chart_timeframes_for(timeframe: str) -> Dict[str, int]:
    """The chart roll-ups available to a campaign stepping `timeframe`.

    A campaign cannot be drawn at a timeframe FINER than the candles it steps —
    a 4H campaign has no 5m history to show — so the options start at its own
    timeframe and climb from there.
    """
    base = str(timeframe or "").lower()
    if base not in CHART_TIMEFRAME_LADDER:
        base = BASE_TIMEFRAME
    start = CHART_TIMEFRAME_LADDER.index(base)
    return {name: TIMEFRAME_SECONDS[name] for name in CHART_TIMEFRAME_LADDER[start : start + 3]}


def next_timeframe_up(timeframe: str) -> str:
    """The next rung above `timeframe` on the escalation ladder, capped at 4H."""
    base = str(timeframe or "").lower()
    if base not in ESCALATION_LADDER:
        return base or BASE_TIMEFRAME
    index = ESCALATION_LADDER.index(base)
    return ESCALATION_LADDER[min(index + 1, len(ESCALATION_LADDER) - 1)]


ACTIVE_STATES = {"WAITING_FIRST_DEPTH", "TRENDLINE_ACTIVE"}
# A mother break freezes a campaign before it becomes final: its unfilled
# entries are cancelled and no new structure is allowed, while two further
# closed 5m candles confirm the reset.  It remains a running campaign during
# that short window so its capital stays reserved and a live TP can be watched.
MOTHER_BREAK_PENDING = "MOTHER_BREAK_PENDING"
RUNNING_STATES = ACTIVE_STATES | {MOTHER_BREAK_PENDING}
FINAL_STATES = {"COMPLETED", "MOTHER_BROKEN", "STOPPED"}
# Endings that roll straight into a fresh campaign. A deliberate stop does not.
RESTART_REASONS = {"mother_broken", "mother_retested"}
# Candles kept to the right of a finished campaign's last action. ONE, so the
# sell arrow is not clipped by the frame — the record ends at the candle the
# target hit on, not minutes or hours of price that had nothing to do with the
# trade.
_CHART_TAIL_BUCKETS = 1
# How close two mother breaks must be to count as the same push. One 15-minute
# break window: a major and a minor broken by the same move freeze within a
# candle or two of each other, while genuinely separate breaks are minutes apart
# at the very least.
_SIMULTANEOUS_BREAK_SEC = 900
# How often ended-but-still-holding campaigns are re-checked against the
# exchange. They place no orders, so there is nothing to be quick about — this
# only has to notice a TP that filled after the campaign was stopped.
_ENDED_POSITION_CHECK_SEC = 120

# Does the per-symbol capital group CAP what a new campaign may take, or is it
# just a number on the screen?
#
# Off since 2026-07-28, at Phil's call. The cap reserved each campaign's full
# nominal capital_usd for its whole life, but capital is a RATE — capital/100
# goes out per 1% of fall — so a $2000 campaign 5% down has committed about
# $100, not $2000. Reserving twenty times what a campaign will realistically use
# meant the first one started swallowed the whole group and every later one was
# refused with "capital group is exhausted", which is the fund-allocation
# confusion that started all of this.
#
# What holds exposure down instead is the band ledger (CROSS_CAMPAIGN_NETTING
# below): campaigns overlapping in price fund the shared ground ONCE, so running
# several on a symbol does not multiply the money at risk the way the nominal
# sum implied.
#
# Turn this back to True to restore the cap — nothing else needs changing, the
# budgets are still stored, still summed and still displayed.
GROUP_CAP_ENFORCED = False

# Does a new campaign born inside ground another has already funded skip that
# ground?
#
# ON since 2026-07-28, backed by the per-symbol BAND LEDGER below.
#
# The first attempt at this shipped OFF, because it was a single FLOOR: the
# lowest price any running campaign had funded down to, with the new campaign
# funding only below it. Right when the new campaign starts under the other's
# mother high (a minor MC inside a major), wrong when it starts above. Phil's
# case: a 15m runs 95 -> 92 while a 4H starts at 100. The floor is 92, so the
# 4H would fund only below 92 — silently skipping 100 -> 95, which NO campaign
# has funded. About $100 of allocation at $2000 capital, gone. The taken ground
# is a BAND in the middle, and a single floor cannot express that.
#
# The ledger does. It records which STRETCHES of price are funded, so a new
# campaign funds whatever is free — above a sibling's band, below it, or both.
#
# Setting this to False restores the old behaviour exactly: every campaign funds
# its whole fall, and the bands are still recorded but no longer subtracted.
CROSS_CAMPAIGN_NETTING = True


class CascadeModelError(Exception):
    pass


def _coerce_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _floor_to_step(quantity: float, step_size) -> float:
    """Floor a base quantity exactly as the exchange will for LOT_SIZE."""
    try:
        qty = Decimal(str(quantity))
        step = Decimal(str(step_size or DEFAULT_LOT_STEP))
        if qty <= 0 or step <= 0:
            return 0.0
        return float((qty / step).to_integral_value(rounding=ROUND_DOWN) * step)
    except (InvalidOperation, ValueError, TypeError):
        return max(_coerce_float(quantity), 0.0)


# ── Price band ledger ───────────────────────────────────────────────
#
# A "band" is a stretch of price [low, high] that some campaign's capital has
# already been allocated across. Bands are always kept sorted, merged and
# disjoint, so a list of them answers one question cheaply: of this fall, how
# much is ground nobody has paid for yet?

Band = Tuple[float, float]


def merge_bands(bands: Iterable[Band]) -> List[Band]:
    """Sort and coalesce bands into a disjoint, ascending list.

    Touching bands merge — [90, 95] and [95, 100] describe one funded stretch
    from 90 to 100, not two, and treating them as two would leave a zero-width
    gap that later maths could trip over.
    """
    clean = sorted((float(low), float(high)) for low, high in bands if float(high) > float(low) > 0)
    merged: List[Band] = []
    for low, high in clean:
        if merged and low <= merged[-1][1]:
            if high > merged[-1][1]:
                merged[-1] = (merged[-1][0], high)
        else:
            merged.append((low, high))
    return merged


def subtract_bands(span: Band, taken: Iterable[Band]) -> List[Band]:
    """The parts of `span` no band in `taken` covers, ascending.

    This is what makes the ledger different from a floor: taken ground in the
    MIDDLE of a fall leaves free ground both above and below it, and both parts
    come back.
    """
    low, high = float(span[0]), float(span[1])
    if high <= low:
        return []
    free: List[Band] = []
    cursor = low
    for t_low, t_high in merge_bands(taken):
        if t_high <= cursor:
            continue
        if t_low >= high:
            break
        if t_low > cursor:
            free.append((cursor, min(t_low, high)))
        cursor = max(cursor, t_high)
        if cursor >= high:
            break
    if cursor < high:
        free.append((cursor, high))
    return free


def free_span_of(span: Band, taken: Iterable[Band]) -> float:
    """How much of `span`, in price, is unfunded ground."""
    return sum(high - low for low, high in subtract_bands(span, taken))


# ── Pure model ──────────────────────────────────────────────────────


@dataclass
class Candle:
    timestamp: int  # epoch seconds (candle open time)
    open: float
    high: float
    low: float
    close: float
    timeframe: str = BASE_TIMEFRAME

    @property
    def is_red(self) -> bool:
        return self.close < self.open


@dataclass
class Trendline:
    trendline_id: int
    anchor1_price: float  # ALWAYS mother candle high
    anchor1_timestamp: int
    anchor2_price: float  # valid red candle open before the depth low
    anchor2_timestamp: int
    bears_fib: bool = True  # False for a same-shelf structure: geometry only

    def to_dict(self) -> dict:
        return dict(self.__dict__)

    @classmethod
    def from_dict(cls, data: dict) -> "Trendline":
        return cls(
            trendline_id=int(data.get("trendline_id", 0)),
            anchor1_price=_coerce_float(data.get("anchor1_price")),
            anchor1_timestamp=int(data.get("anchor1_timestamp", 0)),
            anchor2_price=_coerce_float(data.get("anchor2_price")),
            anchor2_timestamp=int(data.get("anchor2_timestamp", 0)),
            bears_fib=bool(data.get("bears_fib", True)),
        )


def trendline_price(tl: Trendline, at_timestamp: int) -> float:
    x1, y1 = tl.anchor1_timestamp, tl.anchor1_price
    x2, y2 = tl.anchor2_timestamp, tl.anchor2_price
    if x2 == x1:
        return y1
    slope = (y2 - y1) / (x2 - x1)
    return y1 + slope * (at_timestamp - x1)


def find_valid_anchor2(anchor1_price, anchor1_ts, candles_between, epsilon=1e-9):
    """
    cascade_lib's anchor rule: search backward from the red candle closest to
    the depth toward the mother candle, and return the first candidate whose
    connecting line is not crossed by any earlier candle's CLOSE. That is the
    tightest descending line the price action allows — the same line you get by
    dragging from the mother candle with TradingView's magnet on.

    A close is allowed to poke ANCHOR_CLOSE_TOLERANCE_PCT above the line before
    it disqualifies the anchor. Testing to the cent was rejecting anchors that
    are obviously right on the chart: on SOLUSDT 07-22 the 06:20 red open at
    78.53 — the swing top before the 11:30 candle broke the previous low — was
    thrown out by three closes sitting ONE CENT over, 0.017% of price, all
    within twenty minutes of it. That left the anchor frozen at 07-21 19:30 for
    the rest of the campaign, so no fifth trendline could ever be drawn, even
    though the line it would give sits 0.639% off the fourth — four times the
    separation needed to count as a different line.
    """
    red_candidates = [c for c in candles_between if c.is_red]
    for candidate in reversed(red_candidates):
        if candidate.timestamp == anchor1_ts:
            continue
        slope = (candidate.open - anchor1_price) / (candidate.timestamp - anchor1_ts)
        violated = False
        for c in candles_between:
            if c.timestamp < candidate.timestamp:
                line_price_at_c = anchor1_price + slope * (c.timestamp - anchor1_ts)
                allowance = abs(line_price_at_c) * ANCHOR_CLOSE_TOLERANCE_PCT
                if c.close > line_price_at_c + allowance + epsilon:
                    violated = True
                    break
        if not violated:
            return candidate.open, candidate.timestamp
    return None, None


def leg_broken(candle: Candle, current_low: float) -> bool:
    """Decisive break: a red candle whose CLOSE is below the reference low."""
    return candle.is_red and candle.close < current_low


@dataclass
class FibLadder:
    high_anchor: float  # level 0 = the leg's touch_high
    low_anchor: float  # level 1 = the leg's low

    def level_price(self, level: float) -> float:
        return self.high_anchor - level * (self.high_anchor - self.low_anchor)

    def to_dict(self) -> dict:
        return dict(self.__dict__)

    @classmethod
    def from_dict(cls, data: dict) -> "FibLadder":
        return cls(
            high_anchor=_coerce_float(data.get("high_anchor")),
            low_anchor=_coerce_float(data.get("low_anchor")),
        )


@dataclass
class PendingOrder:
    level: int
    price: Optional[float]
    usd_notional: float
    quantity: float
    leg_id: int
    timeframe: str = BASE_TIMEFRAME
    status: str = "PENDING"  # UNFUNDED | PENDING | PLACED | FILLED | CLOSED | CANCELLED | MERGED
    rev: int = 0
    order_id: Optional[str] = None
    client_order_id: str = ""
    filled_qty: float = 0.0
    fill_price: Optional[float] = None
    fill_timestamp: Optional[int] = None
    entry_style: str = "limit"  # limit = rest at the fib line | stop = buy-stop above a falling market
    stop_price: Optional[float] = None  # trigger: the PREVIOUS red candle's close
    limit_price: Optional[float] = None  # cap once triggered, a few ticks over the stop
    stop_ts: Optional[int] = None  # candle whose close last moved the stop
    last_red_close: Optional[float] = None  # most recent red close under the line
    # When a level is too small to place, where its money actually went. Kept so
    # the ladder can say "$4.06 moved to F1 L4" instead of leaving a bare $0 and
    # a status word to decode.
    moved_usd: float = 0.0
    moved_to_level: Optional[int] = None  # None with moved_usd > 0 means the next fib
    # The other side of the same story: what this level's own share was before
    # anything arrived, and which levels topped it up. Kept so a $5.50 order can
    # show itself as "$2.04 own + $3.46 from L8" instead of one opaque figure.
    own_usd: float = 0.0
    received: List[list] = field(default_factory=list)  # [[from_level, usd], ...]

    @property
    def is_open(self) -> bool:
        return self.status in {"PENDING", "PLACED"}

    @property
    def armed(self) -> bool:
        """A stop order is only live once two red candles have printed below
        the fib line — the first supplies the trigger, the second confirms."""
        return self.entry_style != "stop" or self.stop_price is not None

    @property
    def working_price(self) -> Optional[float]:
        """
        The worst price this order can pay. A plain limit pays its fib line; a
        stop pays its limit cap, and is nowhere at all until it arms.
        """
        if self.entry_style == "stop":
            return self.limit_price
        return self.price

    def to_dict(self) -> dict:
        payload = dict(self.__dict__)
        payload["armed"] = self.armed
        payload["working_price"] = self.working_price
        return payload

    @classmethod
    def from_dict(cls, data: dict) -> "PendingOrder":
        order = cls(
            level=int(data.get("level", 0)),
            price=data.get("price"),
            usd_notional=_coerce_float(data.get("usd_notional")),
            quantity=_coerce_float(data.get("quantity")),
            leg_id=int(data.get("leg_id", 0)),
        )
        for key in (
            "timeframe",
            "status",
            "rev",
            "order_id",
            "client_order_id",
            "filled_qty",
            "fill_price",
            "fill_timestamp",
            "entry_style",
            "stop_price",
            "limit_price",
            "stop_ts",
            "last_red_close",
            "moved_usd",
            "moved_to_level",
            "own_usd",
            "received",
        ):
            if key in data:
                setattr(order, key, data[key])
        return order


@dataclass
class Fill:
    price: float
    quantity: float
    level: int
    leg_id: int
    timestamp: int
    order_id: Optional[str] = None

    def to_dict(self) -> dict:
        return dict(self.__dict__)

    @classmethod
    def from_dict(cls, data: dict) -> "Fill":
        return cls(
            price=_coerce_float(data.get("price")),
            quantity=_coerce_float(data.get("quantity")),
            level=int(data.get("level", 0)),
            leg_id=int(data.get("leg_id", 0)),
            timestamp=int(data.get("timestamp", 0)),
            order_id=data.get("order_id"),
        )


@dataclass
class Round:
    """
    One open-to-TP cycle inside a campaign. A TP fill closes the round and
    returns its principal to the campaign's available capital; the campaign
    itself only ends when the mother high is breached above.
    """

    round_id: int
    leg_id: int
    avg_entry: float
    quantity: float
    invested_usd: float
    exit_price: float
    pnl: float
    closed_at: str = ""
    # Closing a round flattens the position and clears campaign.all_fills, so
    # the individual buys that made up the average are gone the moment the TP
    # lands. Snapshot them here: an average entry alone cannot tell you when
    # each rung filled, what it cost, or which fib level it came from.
    fills: List[dict] = field(default_factory=list)
    opened_ts: int = 0
    closed_ts: int = 0

    def to_dict(self) -> dict:
        return dict(self.__dict__)

    @classmethod
    def from_dict(cls, data: dict) -> "Round":
        return cls(
            round_id=int(data.get("round_id", 0)),
            leg_id=int(data.get("leg_id", 0)),
            avg_entry=_coerce_float(data.get("avg_entry")),
            quantity=_coerce_float(data.get("quantity")),
            invested_usd=_coerce_float(data.get("invested_usd")),
            exit_price=_coerce_float(data.get("exit_price")),
            pnl=_coerce_float(data.get("pnl")),
            closed_at=data.get("closed_at") or "",
            fills=[dict(row) for row in (data.get("fills") or []) if isinstance(row, dict)],
            opened_ts=int(data.get("opened_ts") or 0),
            closed_ts=int(data.get("closed_ts") or 0),
        )


@dataclass
class Leg:
    leg_id: int
    trendline_id: int
    low: float  # deepest low since the previous leg finalized
    touch_high: float  # running-max swing high → fib level 0
    touch_timestamp: int
    created_via_break: bool = False
    fib: Optional[FibLadder] = None
    leg_pct_from_mother: Optional[float] = None  # total fall from the mother high
    allocation_pct: Optional[float] = None  # percent this leg funds (see build_fib_ladder_and_pool)
    netted_pct: float = 0.0  # percent of this leg's stretch a sibling had already funded
    pool_usd: Optional[float] = None  # this leg's own allocation
    carry_in_usd: float = 0.0  # legacy: kept so older snapshots still load
    pool_total_usd: float = 0.0  # this fib's own contribution to the shared pool
    escalated: bool = False
    finalized: bool = False  # swing complete (low broke again)
    pending_orders: Dict[int, PendingOrder] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "leg_id": self.leg_id,
            "trendline_id": self.trendline_id,
            "low": self.low,
            "touch_high": self.touch_high,
            "touch_timestamp": self.touch_timestamp,
            "created_via_break": self.created_via_break,
            "fib": self.fib.to_dict() if self.fib else None,
            "leg_pct_from_mother": self.leg_pct_from_mother,
            "allocation_pct": self.allocation_pct,
            "netted_pct": self.netted_pct,
            "pool_usd": self.pool_usd,
            "carry_in_usd": self.carry_in_usd,
            "pool_total_usd": self.pool_total_usd,
            "escalated": self.escalated,
            "finalized": self.finalized,
            "pending_orders": {str(level): order.to_dict() for level, order in self.pending_orders.items()},
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Leg":
        leg = cls(
            leg_id=int(data.get("leg_id", 0)),
            trendline_id=int(data.get("trendline_id", 0)),
            low=_coerce_float(data.get("low")),
            touch_high=_coerce_float(data.get("touch_high")),
            touch_timestamp=int(data.get("touch_timestamp", 0)),
            created_via_break=bool(data.get("created_via_break")),
        )
        if data.get("fib"):
            leg.fib = FibLadder.from_dict(data["fib"])
        leg.leg_pct_from_mother = data.get("leg_pct_from_mother")
        leg.allocation_pct = data.get("allocation_pct")
        leg.netted_pct = _coerce_float(data.get("netted_pct"))
        leg.pool_usd = data.get("pool_usd")
        leg.carry_in_usd = _coerce_float(data.get("carry_in_usd"))
        leg.pool_total_usd = _coerce_float(data.get("pool_total_usd"))
        leg.escalated = bool(data.get("escalated"))
        leg.finalized = bool(data.get("finalized"))
        for level, order in (data.get("pending_orders") or {}).items():
            leg.pending_orders[int(level)] = PendingOrder.from_dict(order)
        return leg


@dataclass
class Campaign:
    campaign_id: str
    symbol: str
    capital_usd: float
    mother_high: float
    mother_low: float
    mother_timestamp: int
    seq: int = 0  # human-facing number, assigned in start order
    mode: str = "paper"  # paper | live
    # The candle this campaign is stepped on. Everything — trendlines, fibs,
    # cuts, entries — is derived from candles of this size, so it is the one
    # setting that changes what the geometry means. Escalation (Phase 2) moves
    # it UP the ladder; nothing ever moves it back down.
    timeframe: str = BASE_TIMEFRAME
    # The rung it JOINED the ladder at, never changed afterwards. Without it a
    # campaign sitting on 1H cannot say whether it was started there or climbed
    # there, which are different things to read on a card.
    start_timeframe: str = BASE_TIMEFRAME
    # Whether this campaign climbs the ladder at all. True for campaigns started
    # on 5m, 15m or 1H — every rung with somewhere above it to go. A campaign
    # started on 4H (the cap), 1D or 1W keeps its timeframe for life.
    escalates: bool = True
    # "major" = this campaign's own anchor, the one whose timeframe you choose.
    # "minor" = a sub-mother marked inside a move that is already running, which
    # is always 5m no matter what chart it was spotted on.
    mc_kind: str = "major"
    # Superseded by funded_bands. Kept so campaigns saved before the band
    # ledger still load; nothing reads it any more.
    funded_floor_price: float = 0.0
    # Stretches of price other campaigns on this symbol had already funded at
    # the moment this one was born, as [low, high] pairs. Every fib this
    # campaign draws funds only the parts of its fall that fall OUTSIDE these,
    # so a percent of the fall is never paid for twice.
    #
    # Captured once, at birth, and then FIXED for life. A running campaign funds
    # its own fall without skipping: campaigns born later stake their claim
    # around it, not through it. Equally, a sibling ending does not retro-fund
    # ground this campaign has already passed — money is deployed as price
    # moves, and there is no going back to buy a dip that is over.
    #
    # Note this nets the PERCENT, never the capital: capital_usd is a rate
    # (capital/100 per 1% of fall), so cutting it would shrink every rung and
    # push the pot below Binance's minimum, leaving the campaign unable to buy
    # shallow dips at all. The rate stays whole; only the ground narrows.
    funded_bands: List[Band] = field(default_factory=list)
    min_notional_usd: float = MIN_NOTIONAL_FLOOR_USD
    # Smallest swing that counts as a fib on THIS instrument, measured from its
    # own bars when the campaign started. Held for life rather than recomputed:
    # a threshold that drifted would silently change which swings count while a
    # campaign is mid-fall. See min_fib_range_for().
    min_fib_range_pct: float = MIN_FIB_RANGE_PCT
    median_bar_pct: float = 0.0  # what it was measured from, for the record
    tick_size: float = DEFAULT_TICK_SIZE  # exchange price increment, for the stop/limit gap
    parent_campaign_id: Optional[str] = None  # set when a mother break auto-started this one
    generation: int = 1  # 1 = manually started; each auto-restart increments
    barren_chain: int = 0  # consecutive auto-restarts that ended without drawing a fib
    left_mother_range: bool = False  # price has traded below the mother low, arming the retest rule
    model_version: int = 0  # rules version the stored legs/trendlines were built with
    created_at: str = ""
    state: str = "WAITING_FIRST_DEPTH"
    # Saved rather than held only in memory: a process restart in the 10-minute
    # confirmation window must neither skip the wait nor invent a new mother.
    mother_break_candle: Optional[dict] = None
    # The highest candle seen across the whole 15-minute break window (the
    # breaking candle and the two that confirm it). This is what becomes the
    # successor's mother — not the breaking candle, which is merely the first of
    # the three and often not the one that reached furthest up.
    mother_break_top_candle: Optional[dict] = None
    mother_break_wait_remaining: int = 0
    mother_break_last_5m_ts: int = 0
    # Once structure has escalated, it may be stepping 15m/1H candles; the
    # original mother is still a 5m reference and must be watched at 5m.
    mother_watch_last_5m_ts: int = 0
    cumulative_used_pct: float = 0.0
    carry_forward_usd: float = 0.0  # legacy: kept so older snapshots still load
    # The running total. Price falling through a level moves that level's money
    # in here; once it clears one rung the whole lot becomes a single buy stop,
    # and the two-red-candle turn buys all of it at once.
    pending_usd: float = 0.0
    collected: List[list] = field(default_factory=list)  # [[leg_id, level, usd, price], ...]
    pending_line: Optional[float] = None  # the level price that completed the total
    pending_stop_price: Optional[float] = None
    pending_limit_price: Optional[float] = None
    pending_stop_ts: Optional[int] = None
    pending_last_red: Optional[float] = None
    pending_order_id: Optional[str] = None
    pending_rev: int = 0
    pending_filled_qty: float = 0.0
    # The low that was standing when the last round closed. Levels the round
    # bought are released back onto the ladder, but only once price drops under
    # this — otherwise the campaign would buy the same shelf straight back at
    # the same price it just sold.
    reuse_below: Optional[float] = None
    trendlines: List[Trendline] = field(default_factory=list)
    legs: List[Leg] = field(default_factory=list)
    active_trendline_id: Optional[int] = None
    all_fills: List[Fill] = field(default_factory=list)  # fills of the OPEN position only
    rounds: List[Round] = field(default_factory=list)  # closed open-to-TP cycles
    avg_entry_price: Optional[float] = None
    tp_price: Optional[float] = None  # active TP once fills exist; display estimate before
    tp_order_id: Optional[str] = None
    # The price the RESTING exchange order was actually placed/adopted at. Kept
    # separate from tp_price on purpose: tp_price is updated the instant a fill
    # changes the average (for the fill log line and display), which happens
    # BEFORE the exchange order is replaced. Comparing "is the order already at
    # the right price" against tp_price was comparing the desired price to
    # itself and always said yes — so a new fill moved the average, tp_price
    # jumped to the new target immediately, and the sync saw "already correct"
    # and never cancelled the stale order sitting at the OLD average's target.
    # This field only ever reflects what is ACTUALLY resting on the exchange.
    tp_order_price: Optional[float] = None
    # The exact lot-rounded TP amount was below Binance's minimum notional.
    # Store the last reported combination so the 10-second sync never floods
    # the event log with the same rejected sell while the dust is held.
    tp_min_notional_notice: Optional[str] = None
    tp_rev: int = 0
    tp_filled: bool = False
    filled_base_qty: float = 0.0
    # Base asset bought but not sellable yet. Binance floors a sell to the
    # symbol's LOT_SIZE step, so a round that bought 0.00011542 BTC could only
    # offer 0.00011 — stranding 0.00000542, which is 4.7% of a $7.60 position.
    # The remainder is carried into the next round's sell instead of being
    # abandoned, so it clears as soon as the total reaches another whole step.
    residual_base_qty: float = 0.0
    # What the exchange last said is actually sellable for THIS campaign, and
    # when it was asked. Only ever set for an ended campaign that still shows a
    # position on our books: those leave ACTIVE_STATES, so nothing syncs them
    # again, and a TP that fills after the stop was never heard about. The books
    # then claim coin that has been sold — and offer to sell it a second time.
    # None means never checked; 0.0 means the exchange says there is nothing.
    exchange_qty: Optional[float] = None
    position_checked_at: str = ""
    # The last reported "books say X, exchange says Y" pair. The sweep runs
    # every couple of minutes and this condition holds until a human settles it,
    # so without a marker the same warning is logged forever. Same idea as
    # tp_min_notional_notice.
    position_missing_notice: str = ""
    realized_pnl: Optional[float] = None
    mother_broken_above: bool = False
    # The structure window: candles since the last cut (the cut candle seeds
    # the next window). Everything else — dip, touch, fib anchors — is derived
    # from the candle history inside this window at evaluation time, so there
    # is no swing state to corrupt or restart.
    window_start_ts: int = 0
    # ── the geometry state machine (Phil's rule, adjudicated 2026-07-31) ──
    # The standing LOW: runs down while the market falls, LOCKS when a candle
    # closes back above the low candle's close, and once locked only a decisive
    # red CLOSE below it moves anything — wicks under a locked low are noise.
    geo_low: Optional[float] = None
    geo_low_ts: int = 0
    geo_low_close: Optional[float] = None
    geo_low_locked: bool = False
    # Whether a NEW trendline may be drawn. True at birth (the first line needs
    # no permission); False while a line stands; True again once a close breaks
    # above the standing line. "The previous trendline has to be the reference
    # till market doesn't break and closes above."
    geo_armed: bool = True
    # Ultimate low since the mother candle — a fib's level 1 is this value as it
    # stood at the fib's touch.
    geo_ult_low: Optional[float] = None
    # Touches waiting for their level-1 to break: {touch_high, touch_ts, fib1,
    # trendline_id}. A fib is DRAWN only when its own fib1 is decisively closed
    # below — the levels exist for the way down, never while the market rises.
    pending_fibs: List[dict] = field(default_factory=list)
    broken_above: bool = False  # active trendline has been closed above
    last_processed_ts: int = 0  # open ts of the last closed candle stepped
    closed_at: str = ""
    close_reason: str = ""
    event_log: List[dict] = field(default_factory=list)

    @property
    def capital_unit_per_pct(self) -> float:
        return self.capital_usd / 100.0

    @property
    def timeframe_sec(self) -> int:
        return timeframe_seconds(self.timeframe)

    @property
    def can_escalate(self) -> bool:
        """A ladder-born campaign that has not already reached the 4H cap."""
        return bool(self.escalates) and self.timeframe != ESCALATION_LADDER[-1]

    @property
    def has_escalated(self) -> bool:
        """It has climbed at least one rung since it was started."""
        return self.timeframe != self.start_timeframe

    @property
    def active_trendline(self) -> Optional[Trendline]:
        for tl in self.trendlines:
            if tl.trendline_id == self.active_trendline_id:
                return tl
        return None

    @property
    def current_leg(self) -> Optional[Leg]:
        return self.legs[-1] if self.legs else None

    @property
    def allocated_down_to(self) -> float:
        """The lowest price this campaign has already allocated money down to.

        Legs are opened progressively deeper, and each one funds from the
        previous leg's level 1 down to its own, so the last leg's low is the
        bottom of everything paid for so far. 0.0 while no fib has been drawn —
        nothing has been funded, so nothing is claimed.
        """
        return min((leg.low for leg in self.legs if leg.low > 0), default=0.0)

    @property
    def claimed_bands(self) -> List[Band]:
        """The stretches of price THIS campaign's capital has actually funded.

        Its fibs run contiguously from the mother high down to the deepest leg
        low, minus whatever was already taken when it was born — that ground
        belongs to the campaign that paid for it, and when that one ends the
        ground goes free rather than passing to this one.

        Empty until a fib is drawn: an armed campaign that has not allocated a
        cent has no claim, and must not block a sibling from funding the fall.
        """
        floor = self.allocated_down_to
        if floor <= 0 or self.mother_high <= floor:
            return []
        return subtract_bands((floor, self.mother_high), self.funded_bands)

    @property
    def spent_usd(self) -> float:
        """Capital currently locked in the OPEN position. A closed round returns
        its principal here, which is what frees it up for the next fib."""
        return sum(f.price * f.quantity for f in self.all_fills)

    @property
    def realized_pnl_total(self) -> float:
        return sum(r.pnl for r in self.rounds)

    def leg_open_usd(self, leg_id: int) -> float:
        """Notional from this leg that is still held (not yet closed at TP)."""
        return sum(f.price * f.quantity for f in self.all_fills if f.leg_id == leg_id)

    @property
    def total_allocation_usd(self) -> float:
        """Everything the fall so far has earned the right to deploy.

        Each fib contributes its own slice of new ground — the depth it added
        below the previous one — so this grows as the market falls and is the
        single pool the whole price-ordered ladder is split from.
        """
        return sum(max(_coerce_float(leg.pool_usd), 0.0) for leg in self.legs)

    @property
    def open_legs(self) -> List[Leg]:
        """Every fib that still has an unfilled order on it.

        A new fib does not retire the one before it. The market falling past
        fib 2's level 1 does not delete fib 1's level 2 — that order is still
        sitting above, and price coming back up through it is exactly the buy
        the ladder was drawn to take. All of them rest at once and their
        amounts stack.
        """
        return [leg for leg in self.legs if any(o.is_open for o in leg.pending_orders.values())]

    @property
    def resting_usd(self) -> float:
        return sum(o.usd_notional for leg in self.legs for o in leg.pending_orders.values() if o.is_open)

    def leg_resting_usd(self, leg_id: int) -> float:
        for leg in self.legs:
            if leg.leg_id == leg_id:
                return sum(o.usd_notional for o in leg.pending_orders.values() if o.is_open)
        return 0.0

    def to_dict(self) -> dict:
        return {
            "campaign_id": self.campaign_id,
            "seq": self.seq,
            "symbol": self.symbol,
            "capital_usd": self.capital_usd,
            "mother_high": self.mother_high,
            "mother_low": self.mother_low,
            "mother_timestamp": self.mother_timestamp,
            "mode": self.mode,
            "timeframe": self.timeframe,
            "start_timeframe": self.start_timeframe,
            "escalates": self.escalates,
            "mc_kind": self.mc_kind,
            "funded_floor_price": self.funded_floor_price,
            "funded_bands": [[low, high] for low, high in self.funded_bands],
            "min_notional_usd": self.min_notional_usd,
            "min_fib_range_pct": self.min_fib_range_pct,
            "median_bar_pct": self.median_bar_pct,
            "tick_size": self.tick_size,
            "parent_campaign_id": self.parent_campaign_id,
            "generation": self.generation,
            "barren_chain": self.barren_chain,
            "left_mother_range": self.left_mother_range,
            "model_version": self.model_version,
            "created_at": self.created_at,
            "state": self.state,
            "mother_break_candle": self.mother_break_candle,
            "mother_break_top_candle": self.mother_break_top_candle,
            "mother_break_wait_remaining": self.mother_break_wait_remaining,
            "mother_break_last_5m_ts": self.mother_break_last_5m_ts,
            "mother_watch_last_5m_ts": self.mother_watch_last_5m_ts,
            "cumulative_used_pct": self.cumulative_used_pct,
            "carry_forward_usd": self.carry_forward_usd,
            "pending_rev": self.pending_rev,
            "pending_filled_qty": self.pending_filled_qty,
            "reuse_below": self.reuse_below,
            "pending_order_id": self.pending_order_id,
            "pending_last_red": self.pending_last_red,
            "pending_stop_ts": self.pending_stop_ts,
            "pending_limit_price": self.pending_limit_price,
            "pending_stop_price": self.pending_stop_price,
            "pending_line": self.pending_line,
            "collected": self.collected,
            "pending_usd": self.pending_usd,
            "trendlines": [tl.to_dict() for tl in self.trendlines],
            "legs": [leg.to_dict() for leg in self.legs],
            "active_trendline_id": self.active_trendline_id,
            "all_fills": [f.to_dict() for f in self.all_fills],
            "rounds": [r.to_dict() for r in self.rounds],
            "avg_entry_price": self.avg_entry_price,
            "tp_price": self.tp_price,
            "tp_order_id": self.tp_order_id,
            "tp_order_price": self.tp_order_price,
            "tp_min_notional_notice": self.tp_min_notional_notice,
            "tp_rev": self.tp_rev,
            "tp_filled": self.tp_filled,
            "filled_base_qty": self.filled_base_qty,
            "residual_base_qty": self.residual_base_qty,
            "exchange_qty": self.exchange_qty,
            "position_checked_at": self.position_checked_at,
            "position_missing_notice": self.position_missing_notice,
            "realized_pnl": self.realized_pnl,
            "mother_broken_above": self.mother_broken_above,
            "window_start_ts": self.window_start_ts,
            # Geometry machine state. Persisted because a live engine restart
            # replays only the candles it missed — a campaign restored without
            # its locked low or pending touches would draw different lines than
            # the one that went down.
            "geo_low": self.geo_low,
            "geo_low_ts": self.geo_low_ts,
            "geo_low_close": self.geo_low_close,
            "geo_low_locked": self.geo_low_locked,
            "geo_armed": self.geo_armed,
            "geo_ult_low": self.geo_ult_low,
            "pending_fibs": [dict(p) for p in self.pending_fibs],
            "broken_above": self.broken_above,
            "last_processed_ts": self.last_processed_ts,
            "closed_at": self.closed_at,
            "close_reason": self.close_reason,
            "event_log": list(self.event_log[-200:]),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Campaign":
        campaign = cls(
            campaign_id=str(data.get("campaign_id") or uuid.uuid4().hex[:10]),
            seq=int(data.get("seq") or 0),
            symbol=str(data.get("symbol") or "BTCUSDT"),
            capital_usd=_coerce_float(data.get("capital_usd"), 2000.0),
            mother_high=_coerce_float(data.get("mother_high")),
            mother_low=_coerce_float(data.get("mother_low")),
            mother_timestamp=int(data.get("mother_timestamp", 0)),
        )
        for key in (
            "mode",
            "timeframe",
            "start_timeframe",
            "escalates",
            "mc_kind",
            "funded_floor_price",
            "min_notional_usd",
            "min_fib_range_pct",
            "median_bar_pct",
            "tick_size",
            "parent_campaign_id",
            "generation",
            "barren_chain",
            "left_mother_range",
            "model_version",
            "created_at",
            "state",
            "mother_break_candle",
            "mother_break_top_candle",
            "mother_break_wait_remaining",
            "mother_break_last_5m_ts",
            "mother_watch_last_5m_ts",
            "cumulative_used_pct",
            "carry_forward_usd",
            "pending_usd",
            "collected",
            "pending_line",
            "pending_stop_price",
            "pending_limit_price",
            "pending_stop_ts",
            "pending_last_red",
            "pending_order_id",
            "pending_rev",
            "pending_filled_qty",
            "reuse_below",
            "active_trendline_id",
            "avg_entry_price",
            "tp_price",
            "tp_order_id",
            "tp_order_price",
            "tp_min_notional_notice",
            "tp_rev",
            "tp_filled",
            "filled_base_qty",
            "residual_base_qty",
            "exchange_qty",
            "position_checked_at",
            "position_missing_notice",
            "realized_pnl",
            "mother_broken_above",
            "window_start_ts",
            "geo_low",
            "geo_low_ts",
            "geo_low_close",
            "geo_low_locked",
            "geo_armed",
            "geo_ult_low",
            "broken_above",
            "last_processed_ts",
            "closed_at",
            "close_reason",
        ):
            if key in data:
                setattr(campaign, key, data[key])
        # A snapshot written before the geometry machine existed has no armed
        # flag; loading its default (True) next to already-drawn trendlines
        # would let a second line onto an unbroken chart. If lines exist and
        # the snapshot is silent, assume the last line still stands.
        if "geo_armed" not in data and (data.get("trendlines") or []):
            campaign.geo_armed = False
        campaign.pending_fibs = [dict(p) for p in data.get("pending_fibs") or [] if isinstance(p, dict)]
        # Heal a gate inflated by the old persist path before anything reads it.
        # Campaigns restored from a snapshot written before that was fixed carry
        # a ×100-per-restart number that silently stops them ever drawing
        # structure again — see repair_scaled_fib_range.
        campaign.min_fib_range_pct, campaign.median_bar_pct = repair_scaled_fib_range(
            campaign.min_fib_range_pct, campaign.median_bar_pct
        )
        campaign.funded_bands = merge_bands(
            (_coerce_float(band[0]), _coerce_float(band[1]))
            for band in data.get("funded_bands") or []
            if isinstance(band, (list, tuple)) and len(band) >= 2
        )
        campaign.trendlines = [Trendline.from_dict(tl) for tl in data.get("trendlines") or []]
        campaign.legs = [Leg.from_dict(leg) for leg in data.get("legs") or []]
        campaign.all_fills = [Fill.from_dict(f) for f in data.get("all_fills") or []]
        campaign.rounds = [Round.from_dict(r) for r in data.get("rounds") or []]
        campaign.event_log = list(data.get("event_log") or [])
        return campaign


def _is_trigger_immediately_error(error) -> bool:
    """True for Binance -2010 'Stop price would trigger immediately'.

    A deterministic, market-relative rejection — the trigger is at or below the
    current price — not a fault to alert on. Matched on both the code and the
    text so a wording change on either side does not turn it back into noise.
    """
    text = str(error or "").lower()
    return "-2010" in text or "would trigger immediately" in text


def timeframe_for_level(campaign: Campaign, leg: Leg, level: int) -> str:
    """The label an order carries: the campaign's own timeframe, or one rung up
    for the deep levels of a leg that fell far enough to be marked escalated.

    This is display only — every order is worked on the campaign's timeframe.
    It used to be hardcoded 5m/15m, which read as a lie the moment a campaign
    could step anything other than 5m.
    """
    if level == 2:
        return campaign.timeframe
    return next_timeframe_up(campaign.timeframe) if leg.escalated else campaign.timeframe


def recompute_avg_entry_price(campaign: Campaign) -> Optional[float]:
    total_cost = sum(f.price * f.quantity for f in campaign.all_fills)
    total_qty = sum(f.quantity for f in campaign.all_fills)
    campaign.avg_entry_price = (total_cost / total_qty) if total_qty > 0 else None
    campaign.filled_base_qty = total_qty
    return campaign.avg_entry_price


def compute_tp_price(campaign: Campaign) -> Optional[float]:
    """
    TP is measured FROM the average entry back toward the mother high, taking
    TP_FIB_LEVEL (0.25) of that move:

        tp = avg_entry + 0.25 * (mother_high - avg_entry)

    Returns None until an entry actually fills — there is no target before
    there is a position, and it moves with the average as more levels fill.
    """
    anchor = campaign.avg_entry_price
    if not anchor or anchor <= 0:
        # No entry yet — there is no target to speak of. The TP only exists
        # once the position does, measured from the actual average entry.
        return None
    return anchor + TP_FIB_LEVEL * (campaign.mother_high - anchor)


def build_fib_ladder_and_pool(campaign: Campaign, leg: Leg) -> None:
    if leg.touch_high >= campaign.mother_high:
        raise CascadeModelError(
            f"leg {leg.leg_id}: touch_high {leg.touch_high} must stay below mother high {campaign.mother_high}"
        )
    if leg.touch_high <= leg.low:
        raise CascadeModelError(f"leg {leg.leg_id}: touch_high {leg.touch_high} must exceed leg low {leg.low}")
    leg.fib = FibLadder(high_anchor=leg.touch_high, low_anchor=leg.low)

    # Total fall from the mother high down to this fib's level 1, for display.
    leg.leg_pct_from_mother = (campaign.mother_high - leg.low) / campaign.mother_high * 100
    touch_pct_from_mother = (campaign.mother_high - leg.touch_high) / campaign.mother_high * 100

    # Funding percent: the first fib measures from the mother high down to its
    # level 1; every fib after that measures the remaining move from the PREVIOUS
    # fib's level 1 down to its own level 1, so each leg only funds new ground.
    prior_leg = campaign.legs[-2] if len(campaign.legs) >= 2 else None
    if prior_leg is not None and prior_leg.low:
        anchor, allocation_pct = prior_leg.low, (prior_leg.low - leg.low) / prior_leg.low * 100
    else:
        anchor, allocation_pct = campaign.mother_high, leg.leg_pct_from_mother
    allocation_pct = max(allocation_pct, 0.0)

    # Band ledger: this campaign was born with some of its fall already funded
    # by a sibling on the same symbol. Charge only for the parts of this leg's
    # stretch that nobody had paid for — which may be above the sibling's band,
    # below it, or both, and that is exactly what a single floor could not say.
    #
    # The percent is scaled by the free PRICE, keeping the same denominator the
    # full leg used, so free% + already-funded% always adds back to the whole
    # leg. Nets the percent of the fall, never the capital: see funded_bands.
    leg.netted_pct = 0.0
    if CROSS_CAMPAIGN_NETTING and campaign.funded_bands and allocation_pct > 0 and anchor > 0:
        span = (leg.low, min(anchor, campaign.mother_high))
        if span[1] > span[0]:
            free_ratio = free_span_of(span, campaign.funded_bands) / (span[1] - span[0])
            gross, allocation_pct = allocation_pct, allocation_pct * max(min(free_ratio, 1.0), 0.0)
            leg.netted_pct = max(gross - allocation_pct, 0.0)

    leg.allocation_pct = allocation_pct
    leg.pool_usd = allocation_pct * campaign.capital_unit_per_pct
    campaign.cumulative_used_pct += allocation_pct
    leg.escalated = touch_pct_from_mother > ESCALATION_THRESHOLD_PCT


def plan_leg_orders(campaign: Campaign, leg: Leg) -> None:
    """Give a new fib its (empty) rungs, then replan the whole ladder."""
    if leg.fib is None:
        raise CascadeModelError(f"leg {leg.leg_id}: fib ladder must be built before planning orders")
    leg.carry_in_usd = 0.0
    leg.pool_total_usd = max(_coerce_float(leg.pool_usd), 0.0)
    for level in CASCADE_LEVELS:
        if level in leg.pending_orders:
            continue
        price = max(leg.fib.level_price(level), 0.0)
        leg.pending_orders[level] = PendingOrder(
            level=level,
            price=price or None,
            usd_notional=0.0,
            quantity=0.0,
            leg_id=leg.leg_id,
            timeframe=timeframe_for_level(campaign, leg, level),
            status="UNFUNDED",
            entry_style="stop" if level in STOP_ENTRY_LEVELS else "limit",
            client_order_id=f"cf-csc-{campaign.campaign_id}-{leg.leg_id}-{level}-0",
        )
    replan_ladder(campaign)


def ladders_overlap(high_a: float, low_a: float, high_b: float, low_b: float) -> bool:
    """Do two fibs put rungs in the same stretch of price?

    Rungs are laid from the touch high downward at CASCADE_LEVELS multiples of
    the fib range, so each ladder spans [high - max*range, high - min*range].
    Two ladders overlap unless one finishes entirely above where the other
    starts.

    This is the question the same-shelf rule actually needs answered. Comparing
    touch highs alone treats "same high, far deeper low" as a duplicate, and it
    is not: on BTCUSDT 07-21 a structure at 0=66,739.89 / 1=66,052.63 was
    dropped for sitting 0.010% from fib 1's high, yet its shallowest rung
    (65,365) was below fib 1's deepest (65,997). Two ladders that share no
    price cannot split money between near-identical rungs, which is the only
    harm the rule exists to prevent.
    """
    deepest = max(CASCADE_LEVELS)
    shallowest = min(CASCADE_LEVELS)
    range_a = high_a - low_a
    range_b = high_b - low_b
    if range_a <= 0 or range_b <= 0:
        return True  # degenerate: fall back to treating them as the same shelf
    floor_a, ceiling_a = high_a - deepest * range_a, high_a - shallowest * range_a
    floor_b, ceiling_b = high_b - deepest * range_b, high_b - shallowest * range_b
    return ceiling_a >= floor_b and ceiling_b >= floor_a


def rung_size_usd(campaign: Campaign) -> float:
    """The standard amount on one rung.

    Binance rejects anything under its MIN_NOTIONAL, so an order sized exactly
    at the minimum is one tick of adverse quote movement away from being
    rejected. Every rung therefore carries a 10% cushion — $5.50 against a $5
    minimum — which is also the smallest amount worth the round trip.
    """
    floor = max(_coerce_float(campaign.min_notional_usd, MIN_NOTIONAL_FLOOR_USD), MIN_NOTIONAL_FLOOR_USD)
    return round(floor * (1.0 + RUNG_BUFFER_PCT), 2)


def replan_ladder(campaign: Campaign) -> None:
    """
    Give every level of every fib its own share of its own fib's pool: 20% at
    level 2, 30% at level 4, 50% at level 8. That is all this does.

    No level is ever short-changed for being too small, and no fib hands money
    to another. A level's share can be sixty cents and that is fine — it is not
    an order on its own. Orders come from the running total in
    CascadeEngine._collect_crossed_levels: as price falls through the ladder,
    each level it touches adds its money to a pot, and the moment the pot clears
    one rung it becomes a single buy stop covering the lot.

    That is what makes overlapping fibs work. Fib 2's level 8 at 77.08 and fib
    3's level 4 at 77.07 are the same price to the market; price crossing that
    point collects both, and $1.02 + $3.13 becomes one placeable order instead
    of two that could never be placed.
    """
    # Safety cap: allocation is a percentage of the fall, so it cannot normally
    # outrun capital, but a manual pool or a restored snapshot could. Scale the
    # whole ladder rather than starving one end of it.
    committed = campaign.spent_usd + campaign.pending_usd
    allocation = campaign.total_allocation_usd
    room = max(campaign.capital_usd - committed, 0.0)
    scale = min(1.0, room / allocation) if allocation > room > 0 else (0.0 if allocation > 0 and room <= 0 else 1.0)

    for leg in campaign.legs:
        if leg.fib is None:
            continue
        pool = max(_coerce_float(leg.pool_usd), 0.0) * scale
        leg.pool_total_usd = pool
        for level in CASCADE_LEVELS:
            order = leg.pending_orders.get(level)
            if order is None or order.status not in {"PENDING", "UNFUNDED"}:
                continue  # collected, bought or cancelled — its money has moved on
            amount = round(pool * LEVEL_ALLOCATION[level], 2)
            price = _coerce_float(order.price) or max(leg.fib.level_price(level), 0.0)
            order.usd_notional = amount
            order.quantity = amount / price if amount > 0 and price > 0 else 0.0
            order.own_usd = amount
            order.status = "PENDING" if amount > 0 else "UNFUNDED"


# ── Engine ──────────────────────────────────────────────────────────


class CascadeEngine:
    def __init__(
        self,
        broker,
        on_campaign_closed: Optional[Callable] = None,
        on_event: Optional[Callable] = None,
        on_update: Optional[Callable] = None,
        on_alert: Optional[Callable] = None,
    ):
        self.broker = broker
        self.on_campaign_closed = on_campaign_closed
        self.on_event = on_event
        self.on_update = on_update
        self.on_alert = on_alert
        self._alert_state: Dict[str, float] = {}  # de-dupe key -> last sent monotonic time
        self.campaigns: Dict[str, Campaign] = {}
        self.closed_campaigns: List[dict] = []
        self._running = False
        self._task: Optional[asyncio.Task] = None
        # Strong references to in-flight _schedule() work — see _schedule.
        self._pending_tasks: set = set()
        # Per-campaign candle history, in that campaign's own timeframe
        # (rebuilt on restart). Not all 5m any more — read campaign.timeframe.
        self._candles: Dict[str, List[Candle]] = {}
        self._price_cache: Dict[str, tuple] = {}
        self._last_sync_ts: Dict[str, float] = {}  # per campaign — a shared
        # timestamp meant two live campaigns starved each other of syncs
        # campaign_id -> ((stop, limit), attempts) — see the churn brake in
        # _place_pending_stop. In memory only: a restart is a fresh chance.
        self._place_attempts: Dict[str, tuple] = {}
        self._loop_interval_sec = 5.0
        # 30s left a fill un-hedged for up to half a minute before the TP
        # went up. Weight cost at 10s is trivial against Binance's budget.
        self._sync_interval_sec = 10.0
        self._last_candle_ts = 0.0  # monotonic time of the last processed candle, for stall detection
        self._stall_alerted = False
        # Capital group per instrument: one budget per symbol, typed in once.
        # A new campaign's capital = budget − what active siblings already
        # committed, snapshotted at creation and fixed after. No entry for a
        # symbol means no group — campaigns take their typed capital unchanged,
        # which is exactly the pre-group behaviour.
        self.capital_groups: Dict[str, float] = {}
        # Campaign ids whose pot is collected but HELD unarmed because the fall
        # it collected already bounced far above the trigger (a late/left start).
        # In memory only, for one-shot logging — the hold itself is recomputed
        # from live price every tick, so a restart simply re-evaluates it.
        self._stale_pot_held: set = set()
        # Ended campaigns are swept on their own slower clock — they have no
        # orders to place, only a sold position to notice.
        self._last_ended_check: float = 0.0
        # See _acquire_write_lock: only the holder places orders.
        self._lock_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".cascade-writer.lock"
        )
        self._lock_handle = None
        self._lock_warned = False

    # ── lifecycle ────────────────────────────────────────────────

    def start(self):
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._monitor_loop())
        _log.info("[CASCADE] engine started")

    # ── single writer ────────────────────────────────────────────
    #
    # Only ONE process may drive orders. The blue-green deploy runs the old and
    # new instances together while it drains, so on 2026-07-22 two engines — at
    # one point three — managed the same campaigns against the same Binance
    # account. Each saw the other's resting orders as unrecognised, cancelled
    # them, and placed its own. That is the whole "buy stop was cancelled on
    # the exchange; re-placing" loop, the five stacked TP sells, and a
    # duplicate SOL buy of $14.58 that no campaign ever knew it owned.
    #
    # An advisory lock on a file settles it without coordination: whoever holds
    # it trades, everyone else serves HTTP and waits. The kernel drops it when
    # the holder exits, so the incoming instance takes over the moment the old
    # one stops, with no handoff protocol and nothing to get wrong.

    def _acquire_write_lock(self) -> bool:
        """True if this process holds the right to place orders."""
        if self._lock_handle is not None:
            return True
        # Opening and locking fail for opposite reasons and must not share a
        # handler. A missing directory is an OSError too, and folding it in
        # here read as "another process is trading" — which would silently stop
        # this one placing orders, including the exit on an open position.
        try:
            handle = open(self._lock_path, "a+")
        except Exception as exc:
            _log.warning("[CASCADE] write lock unusable (%s); trading without it", exc)
            return True
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            # Held elsewhere. Expected during a deploy — not an error.
            handle.close()
            return False
        handle.seek(0)
        handle.truncate()
        handle.write(f"{os.getpid()}\n")
        handle.flush()
        self._lock_handle = handle
        return True

    def _release_write_lock(self) -> None:
        if self._lock_handle is None:
            return
        try:
            fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_UN)
            self._lock_handle.close()
        except Exception:
            pass
        self._lock_handle = None

    def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            self._task = None
        # Hand the lock over promptly so the incoming instance can start
        # trading rather than waiting for this process to be reaped.
        self._release_write_lock()

    async def shutdown(self):
        self.stop()
        self._emit_update()

    @property
    def active_campaigns(self) -> List[Campaign]:
        return [c for c in self.campaigns.values() if c.state in RUNNING_STATES]

    @property
    def live_campaigns(self) -> List[Campaign]:
        return [c for c in self.active_campaigns if c.mode == "live"]

    # ── events / updates ─────────────────────────────────────────

    def _log_event(self, campaign: Optional[Campaign], level: str, message: str):
        event = {
            "timestamp": _ist_now_str(),
            "level": level,
            "message": message,
            "campaign_id": campaign.campaign_id if campaign else None,
            "symbol": campaign.symbol if campaign else None,
        }
        if campaign is not None:
            campaign.event_log.append(event)
            if len(campaign.event_log) > 200:
                campaign.event_log = campaign.event_log[-200:]
        _log.info("[CASCADE] %s", message)
        if self.on_event:
            try:
                self.on_event(event)
            except Exception as exc:
                _log.warning("[CASCADE] on_event callback failed: %s", exc)

    def _check_watchdogs(self) -> None:
        """
        Two things that are invisible on screen: an engine that has quietly
        stopped stepping candles, and a campaign list that has grown past what
        one person can keep track of.
        """
        active = self.active_campaigns
        if len(active) > MAX_ACTIVE_BEFORE_ALERT:
            live = sum(1 for c in active if c.mode == "live")
            deployed = sum(c.spent_usd for c in active)
            self._alert(
                "Cascade campaign count high",
                f"{len(active)} campaigns are active ({live} live).\n"
                f"Capital committed right now: ${deployed:,.2f}\n\n"
                f"Auto-restart keeps opening a new one on every mother break.",
                level="warn",
                dedupe_sec=3600,
            )
        if not active or not self._last_candle_ts:
            return
        stalled_for = time.monotonic() - self._last_candle_ts
        # Silence is only suspicious relative to the FASTEST campaign running.
        # A 1D campaign legitimately processes nothing for a day, and firing the
        # 15-minute alarm at it would be crying wolf every loop tick.
        fastest = min(active, key=lambda c: c.timeframe_sec)
        stall_limit = max(STALL_ALERT_SEC, 3 * fastest.timeframe_sec)
        if stalled_for > stall_limit:
            self._alert(
                "Cascade engine STALLED",
                f"No candle has been processed for {stalled_for / 60:.0f} minutes "
                f"while {len(active)} campaign(s) are active "
                f"(fastest is {fastest.timeframe}).\n\n"
                f"Orders already on Binance still stand, but nothing is being "
                f"armed, stepped or filled. Check the server.",
                level="error",
                dedupe_sec=1800,
            )

    def _alert(self, title: str, body: str, level: str = "warn", dedupe_sec: float = 0.0, dedupe_key: str = "") -> None:
        """
        Push something worth waking up for. `dedupe_sec` suppresses a repeat of
        the same key within that window, so a condition that stays true (five
        campaigns open, the engine stalled) does not fire every loop tick.

        `dedupe_key` defaults to the title, which is right for engine-wide
        conditions. Pass a per-campaign key for anything a SECOND campaign could
        raise at the same time: keyed on title alone, the first campaign's alert
        silences every sibling's for the whole window, so you hear about one
        broken position and never learn about the other three.
        """
        if not self.on_alert:
            return
        if dedupe_sec > 0:
            now = time.monotonic()
            key = dedupe_key or title
            last = self._alert_state.get(key, 0.0)
            if now - last < dedupe_sec:
                return
            self._alert_state[key] = now
        try:
            self.on_alert(title, body, level)
        except Exception as exc:
            _log.warning("[CASCADE] alert hook failed: %s", exc)

    def _emit_update(self):
        if self.on_update:
            try:
                self.on_update(self.get_status())
            except Exception as exc:
                _log.warning("[CASCADE] on_update callback failed: %s", exc)

    # ── capital groups ───────────────────────────────────────────

    def set_capital_group(self, symbol: str, budget_usd) -> dict:
        """Set (or clear, with 0/blank) the one budget a symbol's campaigns share."""
        symbol = str(symbol or "").strip().upper()
        if not symbol:
            return {"error": "Symbol is required"}
        budget = _coerce_float(budget_usd)
        if budget <= 0:
            existed = self.capital_groups.pop(symbol, None) is not None
            self._emit_update()
            return {"status": "ok", "symbol": symbol, "removed": existed}
        committed = self.group_committed_usd(symbol)
        self.capital_groups[symbol] = budget
        self._emit_update()
        return {
            "status": "ok",
            "symbol": symbol,
            "budget_usd": budget,
            "committed_usd": round(committed, 2),
            "available_usd": round(budget - committed, 2),
        }

    def group_committed_usd(self, symbol: str) -> float:
        """What the symbol's ACTIVE campaigns hold of the group budget.

        A campaign commits its full capital_usd for its whole life — resting
        orders are promises against it even before they fill — and releases it
        only by ending. Ended campaigns are out of the sum, so their capital
        flows back to the group automatically.
        """
        return sum(c.capital_usd for c in self.campaigns.values() if c.symbol == symbol and c.state in ACTIVE_STATES)

    def capital_group_status(self) -> Dict[str, dict]:
        out = {}
        for symbol, budget in sorted(self.capital_groups.items()):
            committed = self.group_committed_usd(symbol)
            out[symbol] = {
                "budget_usd": budget,
                "committed_usd": round(committed, 2),
                "available_usd": round(budget - committed, 2),
            }
        return out

    def load_capital_groups(self, groups: dict) -> None:
        """Seed groups from the persisted snapshot on restart."""
        for symbol, value in (groups or {}).items():
            budget = _coerce_float(value.get("budget_usd") if isinstance(value, dict) else value)
            if budget > 0:
                self.capital_groups[str(symbol).strip().upper()] = budget

    def instrument_stacks(self) -> Dict[str, dict]:
        """Per-instrument roll-up: every symbol's running campaigns as one stack.

        This is the Phase 4 view — with concurrent campaigns per symbol, each on
        its own timeframe and sharing a capital group, the per-card view stops
        answering the question that matters: "what is my total exposure on this
        instrument?" One entry per symbol answers it.
        """
        stacks: Dict[str, dict] = {}

        def _blank() -> dict:
            return {
                "active_count": 0,
                "live_count": 0,
                "committed_usd": 0.0,
                "in_position_usd": 0.0,
                "resting_usd": 0.0,
                "pending_usd": 0.0,
                "realized_pnl_usd": 0.0,
                "rounds_closed": 0,
                "timeframes": [],
                "budget_usd": None,
                "available_usd": None,
            }

        for campaign in self.campaigns.values():
            if campaign.state not in ACTIVE_STATES:
                continue
            stack = stacks.setdefault(campaign.symbol, _blank())
            stack["active_count"] += 1
            if campaign.mode == "live":
                stack["live_count"] += 1
            stack["committed_usd"] += campaign.capital_usd
            stack["in_position_usd"] += campaign.spent_usd
            stack["resting_usd"] += campaign.resting_usd
            stack["pending_usd"] += campaign.pending_usd
            stack["realized_pnl_usd"] += campaign.realized_pnl_total
            stack["rounds_closed"] += len(campaign.rounds)
            if campaign.timeframe not in stack["timeframes"]:
                stack["timeframes"].append(campaign.timeframe)
        # A group with nothing running is still a stack — the budget is standing
        # capital waiting for its next campaign and should stay visible.
        for symbol, budget in self.capital_groups.items():
            stack = stacks.setdefault(symbol, _blank())
            stack["budget_usd"] = budget
            stack["available_usd"] = round(budget - stack["committed_usd"], 2)
        for stack in stacks.values():
            for key in ("committed_usd", "in_position_usd", "resting_usd", "pending_usd", "realized_pnl_usd"):
                stack[key] = round(stack[key], 2)
        return dict(sorted(stacks.items()))

    # ── public API ───────────────────────────────────────────────

    async def start_campaign(
        self,
        symbol: str,
        capital_usd: float,
        mother_high: float,
        mother_low: float,
        mother_timestamp: Optional[int] = None,
        mode: str = "paper",
        timeframe: str = BASE_TIMEFRAME,
        mc_kind: str = "",
    ) -> dict:
        symbol = str(symbol or "").strip().upper()
        mode = "live" if str(mode or "").strip().lower() == "live" else "paper"
        timeframe = str(timeframe or BASE_TIMEFRAME).strip().lower() or BASE_TIMEFRAME
        # The kind and the timeframe say the same thing, so the UI only asks
        # once: picking 5m IS picking "initiate / minor MC", and picking
        # 4H/1D/1W IS picking "older MC". The explicit parameter stays for
        # callers that want to be unambiguous — and asking for a minor MC still
        # forces 5m, whatever timeframe came with it, because a minor high is a
        # small structure no matter which chart it was spotted on.
        asked = str(mc_kind or "").strip().lower()
        if asked == "minor":
            mc_kind, timeframe = "minor", MINOR_MC_TIMEFRAME
        elif asked == "major":
            mc_kind = "major"
        else:
            mc_kind = "minor" if timeframe == MINOR_MC_TIMEFRAME else "major"
        capital_usd = _coerce_float(capital_usd)
        mother_high = _coerce_float(mother_high)
        mother_low = _coerce_float(mother_low)
        if not symbol:
            return {"error": "Symbol is required"}
        if timeframe not in CAMPAIGN_START_TIMEFRAMES:
            return {
                "error": (
                    f"Timeframe must be one of {', '.join(CAMPAIGN_START_TIMEFRAMES)} — "
                    "5m for a mother candle at the right-hand edge or a sub-mother, "
                    "4h/1d/1w for a higher-timeframe campaign started from an older one"
                )
            }
        tf_sec = timeframe_seconds(timeframe)
        if mother_high <= 0 or mother_low <= 0 or mother_high <= mother_low:
            return {"error": "Mother candle high must be greater than mother candle low (both > 0)"}
        min_notional = MIN_NOTIONAL_FLOOR_USD
        product = None
        try:
            product = await asyncio.to_thread(self.broker.get_product_by_symbol, symbol)
        except Exception as exc:
            _log.warning("[CASCADE] product lookup failed for %s: %s", symbol, exc)
        if product is None:
            return {"error": f"Symbol {symbol} not found on {getattr(self.broker, 'display_name', 'broker')}"}
        min_notional = max(_coerce_float(product.get("min_notional"), min_notional), MIN_NOTIONAL_FLOOR_USD)
        tick_size = _coerce_float(product.get("tick_size"), DEFAULT_TICK_SIZE) or DEFAULT_TICK_SIZE
        # Capital group: when the symbol has a budget, a new campaign may only
        # take what its active siblings have not already committed — measured
        # once, HERE, and fixed for the campaign's life. Typing less than the
        # remainder is allowed (holding some back for the next minor MC is a
        # reasonable thing to do); typing more is quietly capped to it.
        #
        # PARKED (2026-07-28) behind GROUP_CAP_ENFORCED — see the flag for why.
        group_budget = _coerce_float(self.capital_groups.get(symbol))
        group_note = ""
        if GROUP_CAP_ENFORCED and group_budget > 0:
            available = group_budget - self.group_committed_usd(symbol)
            if available < min_notional * 2:
                return {
                    "error": (
                        f"{symbol} capital group is exhausted: ${group_budget:g} budget, "
                        f"${group_budget - available:,.2f} committed by running campaigns, "
                        f"${max(available, 0):,.2f} left. Stop a sibling campaign or raise the budget."
                    )
                }
            if capital_usd <= 0 or capital_usd > available:
                capital_usd = available
            group_note = f" (capital group: ${capital_usd:,.2f} of ${available:,.2f} available)"
        elif group_budget > 0:
            # The budget is still tracked and still shown; it simply no longer
            # gates. Every campaign takes the capital it was given.
            group_note = (
                f" (capital group ${group_budget:g} is informational only — the cap is off, "
                f"so this campaign takes its full ${capital_usd:,.2f})"
            )
        if capital_usd < min_notional * 2:
            return {"error": f"Capital must be at least ${min_notional * 2:g}"}
        # Ground a running campaign on this symbol has already paid for. Its
        # capital is untouched — only the stretch of fall it funds narrows.
        # How loud this instrument is, in its own bars. The fib size filter is
        # scaled from this, so a quiet market like PAXG is not asked for a swing
        # bigger than it ever makes.
        median_bar = await self._measure_median_bar_pct(symbol)
        min_fib_range = min_fib_range_for(symbol, median_bar)

        funded_bands, funded_by = self._birth_bands_for(symbol, mother_high) if CROSS_CAMPAIGN_NETTING else ([], [])
        floor_note = ""
        if funded_bands:
            taken_pct = sum(high - low for low, high in funded_bands) / mother_high * 100
            floor_note = (
                " (starts across ground campaign"
                + ("s " if len(funded_by) > 1 else " ")
                + ", ".join(f"#{c.seq}" for c in sorted(funded_by, key=lambda c: c.seq))
                + " already funded — "
                + ", ".join(f"{low:,.2f}-{high:,.2f}" for low, high in funded_bands)
                + f", {taken_pct:.2f}% of the fall. It funds only what is free above and below those,"
                " so no percent of the fall is paid for twice)"
            )

        now_ts = int(time.time())
        if mother_timestamp:
            mother_ts = int(mother_timestamp)
        else:
            # No timestamp given: the mother candle is a past candle the user
            # read off the chart, so find the recent candle of this campaign's
            # timeframe whose high matches the entered mother high and anchor
            # there. Defaulting to "now" would make the engine wait for future
            # candles forever and ignore all the history that already formed
            # the trendlines.
            detected = await self._resolve_mother_timestamp(symbol, mother_high, timeframe)
            if detected is None:
                return {
                    "error": (
                        f"Could not find a recent {timeframe} candle matching that mother high. "
                        "Set the Mother Candle Time so the engine can replay from it, "
                        "or double-check the high value."
                    )
                }
            mother_ts = detected
        if mother_ts > now_ts:
            return {"error": "Mother candle timestamp cannot be in the future"}
        # How far back a mother may be anchored is really a limit on how many
        # candles the replay has to chew through, not on wall-clock age. 90 days
        # of 5m is already ~26k candles; the same bar budget on 1D is years, and
        # a "from the left" weekly mother is exactly the point of those
        # timeframes. So: 90 days, or MAX_REPLAY_BARS of this timeframe,
        # whichever reaches further.
        max_age_sec = max(90 * 86400, MAX_REPLAY_BARS * tf_sec)
        if mother_ts < now_ts - max_age_sec:
            return {
                "error": (
                    f"Mother candle is more than {max_age_sec // 86400} days old for a {timeframe} "
                    "campaign — pick a more recent mother candle or a higher timeframe"
                )
            }
        twin = self._active_duplicate(symbol, mother_ts, mother_high)
        if twin is not None:
            return {
                "error": (
                    f"Campaign #{twin.seq} is already running on {symbol} from this exact mother "
                    f"candle ({mother_high:,.2f} / {mother_low:,.2f}). Stop or delete it first, or "
                    f"pick a different mother candle."
                )
            }

        campaign = Campaign(
            campaign_id=uuid.uuid4().hex[:10],
            seq=self._next_seq(),
            symbol=symbol,
            capital_usd=capital_usd,
            mother_high=mother_high,
            mother_low=mother_low,
            mother_timestamp=mother_ts,
            mode=mode,
            timeframe=timeframe,
            start_timeframe=timeframe,
            # Every rung with somewhere above it climbs: 5m, 15m and 1H all
            # escalate toward the 4H cap. 4H/1D/1W stay put for life.
            escalates=timeframe in ESCALATING_START_TIMEFRAMES,
            mc_kind=mc_kind,
            funded_bands=funded_bands,
            min_notional_usd=min_notional,
            min_fib_range_pct=min_fib_range,
            median_bar_pct=median_bar,
            tick_size=tick_size,
            model_version=MODEL_VERSION,
            created_at=_ist_now_str(),
            last_processed_ts=mother_ts,
            mother_watch_last_5m_ts=mother_ts,
            window_start_ts=mother_ts,
        )
        self.campaigns[campaign.campaign_id] = campaign
        self._log_event(
            campaign,
            "start",
            f"Campaign {campaign.campaign_id} started ({mode.upper()}) — {symbol} {timeframe}, "
            f"{mc_kind} MC, capital ${capital_usd:g}, "
            f"mother high {mother_high:g} / low {mother_low:g}"
            + (" — minor MC, so 5m regardless of the chart it was marked on" if mc_kind == "minor" else "")
            + (f" — climbs to {ESCALATION_LADDER[-1]}" if campaign.escalates else " — fixed timeframe, no escalation")
            + (
                f" — smallest fib {min_fib_range * 100:.3f}% (median 5m bar {median_bar * 100:.3f}%)"
                if median_bar > 0
                else f" — smallest fib {min_fib_range * 100:.3f}% (bars not measurable, using the default)"
            )
            + group_note
            + floor_note,
        )
        self.start()
        self._emit_update()
        return {"status": "ok", "campaign": campaign.to_dict()}

    async def stop_campaign(self, campaign_id: str, cancel_orders: bool = True) -> dict:
        campaign = self.campaigns.get(campaign_id)
        if campaign is None:
            return {"error": f"Campaign {campaign_id} not found"}
        if campaign.state in FINAL_STATES:
            return {"error": f"Campaign {campaign_id} is already {campaign.state.lower()}"}
        holding = _coerce_float(campaign.filled_base_qty, 0.0) > 0
        if cancel_orders and campaign.mode == "live":
            # Stopping pulls the pending buys so no new entry fires, but a
            # position already held keeps its TP sell resting on the exchange
            # so it still exits at target — stopping the engine must never
            # strand coins with nothing to sell them.
            await self._cancel_all_live_orders(campaign, include_tp=not holding)
        campaign.state = "STOPPED"
        campaign.close_reason = "stopped"
        campaign.closed_at = _ist_now_str()
        # What is on the exchange, not the freshly-computed target — the two can
        # differ for a moment right after a fill, before the next sync replaces
        # the resting order to match.
        tp_for_log = _coerce_float(campaign.tp_order_price or campaign.tp_price, 0.0)
        if holding and campaign.mode == "live" and campaign.tp_order_id and tp_for_log > 0:
            self._log_event(
                campaign,
                "stop",
                f"Campaign {campaign_id} stopped — still holding, TP sell left "
                f"resting at {tp_for_log:,.2f} so it exits at target",
            )
        else:
            self._log_event(campaign, "stop", f"Campaign {campaign_id} stopped")
        self._archive_campaign(campaign)
        self._emit_update()
        return {"status": "ok", "campaign": campaign.to_dict()}

    async def reconcile_ended_positions(self) -> dict:
        """Ask the exchange about ended campaigns our books still show holding.

        A stopped campaign keeps its TP resting so it can still exit at target,
        but stopping takes it out of ACTIVE_STATES and nothing syncs it again.
        When that TP fills, the engine never hears: the campaign sits in Open
        Trades claiming a position that was sold, and offers a Market Sell for
        coin that is not there.

        Cheap and safe to call often — it only reads, and only for the handful
        of ended campaigns that still claim coin.
        """
        checked, settled = 0, 0
        for campaign in list(self.campaigns.values()):
            if campaign.state not in FINAL_STATES or campaign.mode != "live":
                continue
            if _coerce_float(campaign.filled_base_qty, 0.0) <= 0:
                continue
            checked += 1
            try:
                if await self._settle_ended_position(campaign):
                    settled += 1
            except Exception as exc:
                _log.warning("[CASCADE] ended-position check failed for %s: %s", campaign.campaign_id, exc)
        if settled:
            self._emit_update()
        return {"status": "ok", "checked": checked, "settled": settled}

    async def _settle_ended_position(self, campaign: Campaign) -> bool:
        """One ended campaign, reconciled against the exchange. True if it moved."""
        # Our own TP is the best evidence there is: if it filled, the exchange
        # tells us the quantity AND the price, so the round books properly with
        # real P&L instead of being guessed at.
        if campaign.tp_order_id:
            row = await self._safe_get_order(campaign, campaign.tp_order_id)
            status = str((row or {}).get("status") or "").upper()
            if status == "FILLED":
                offered = campaign.filled_base_qty + campaign.residual_base_qty
                executed = _coerce_float(row.get("executedQty"), offered)
                quote = _coerce_float(row.get("cummulativeQuoteQty"))
                exit_price = (
                    quote / executed
                    if executed > 0 and quote > 0
                    else _coerce_float(campaign.tp_order_price or campaign.tp_price, 0.0)
                )
                campaign.residual_base_qty = max(round(offered - executed, 12), 0.0)
                self._close_round(campaign, exit_price, sold_qty=executed)
                campaign.exchange_qty = 0.0
                campaign.position_checked_at = _ist_now_str()
                self._log_event(
                    campaign,
                    "fill",
                    f"Take-profit filled at {exit_price:,.2f} after the campaign had already ended — "
                    f"booked now. The position is closed and nothing is held.",
                )
                self._alert(
                    "Cascade TARGET hit (ended campaign)",
                    f"{campaign.symbol} #{campaign.seq} had been stopped, but its resting take-profit "
                    f"filled at {exit_price:,.2f}.\n\nBooked now — the position is closed.",
                    level="info",
                )
                self._archive_campaign(campaign)
                return True
            if status in {"CANCELED", "EXPIRED", "REJECTED"}:
                campaign.tp_order_id = None
                campaign.tp_order_price = None
            else:
                # Still resting (NEW / PARTIALLY_FILLED). The coin is accounted
                # for — it is locked inside that very order — and the campaign
                # will exit at target on its own. Nothing to check and nothing
                # to warn about.
                campaign.position_checked_at = _ist_now_str()
                campaign.exchange_qty = _coerce_float(campaign.filled_base_qty, 0.0)
                campaign.position_missing_notice = ""
                return False

        # No resting TP of ours to explain it. Is the coin actually there? Ask
        # what the account HOLDS, not what is free: another campaign's resting
        # sell locks its own coin, and free alone would call that gone. The
        # balance is shared across the symbol, so subtract what the others still
        # claim before deciding any of it is ours.
        owned = await self._owned_base_balance(campaign)
        campaign.position_checked_at = _ist_now_str()
        if owned is None:
            campaign.exchange_qty = None  # could not read — say nothing
            return False
        free = owned
        claimed_by_others = sum(
            _coerce_float(c.filled_base_qty, 0.0) + _coerce_float(c.residual_base_qty, 0.0)
            for c in self.campaigns.values()
            if c.campaign_id != campaign.campaign_id and c.symbol == campaign.symbol and c.filled_base_qty > 0
        )
        mine = max(free - claimed_by_others, 0.0)
        campaign.exchange_qty = round(mine, 12)
        ours = _coerce_float(campaign.filled_base_qty, 0.0)
        if mine >= ours * 0.99:
            campaign.position_missing_notice = ""
            return False  # the coin is there; nothing to do

        # Say it ONCE. The sweep runs every two minutes and this condition stays
        # true until someone settles it by hand, so logging on every pass buries
        # the event log in the same paragraph forever — which is how a log stops
        # being read at all. Re-armed only if the numbers actually change.
        notice = f"{mine:.12f}/{ours:.12f}"
        if campaign.position_missing_notice == notice:
            return False
        campaign.position_missing_notice = notice

        # It is gone, and no TP of ours accounts for it — sold by hand, by
        # another tool, or by a TP whose id we lost. Deliberately NOT booked as
        # a round: we do not know what it sold for, and inventing a price would
        # put a fabricated number in the P&L. Say so loudly and let it be
        # settled by hand.
        self._log_event(
            campaign,
            "warn",
            f"The exchange shows {mine:.8f} available but this campaign's books claim {ours:.8f}. "
            f"The coin is gone and no take-profit of ours accounts for it. Not booking a round — "
            f"the sale price is unknown. Market Sell is disabled; settle this one by hand.",
        )
        self._alert(
            "Cascade position missing on the exchange",
            f"{campaign.symbol} #{campaign.seq} (ended) claims {ours:.8f} but the exchange has "
            f"{mine:.8f} free.\n\nNo take-profit of ours explains it. Nothing was booked — the "
            f"sale price is unknown.",
            level="warn",
            dedupe_sec=3600,
            dedupe_key=f"position-missing:{campaign.campaign_id}",
        )
        return True

    async def liquidate_campaign(self, campaign_id: str) -> dict:
        """Sell a stopped campaign's remaining position at market, now.

        A stopped campaign keeps its TP resting so it can still exit at target,
        which is right when the target is reachable and wrong when it is not —
        the coin then sits in the account forever with a dead campaign's name on
        it. This is the manual way out: cancel the resting sell, market-sell what
        the exchange actually holds, and book the round so the P&L is recorded
        against the campaign rather than vanishing into the wallet.

        Deliberately restricted to ENDED campaigns. A running campaign must exit
        through its own target — selling underneath it would leave the ladder
        armed and buying back into a position it thinks it still holds.
        """
        campaign = self.campaigns.get(campaign_id)
        if campaign is None:
            return {"error": f"Campaign {campaign_id} not found"}
        if campaign.state not in FINAL_STATES:
            return {"error": "Only an ended campaign can be sold at market — stop it first"}
        desired_qty = _coerce_float(campaign.filled_base_qty, 0.0) + _coerce_float(campaign.residual_base_qty, 0.0)
        if desired_qty <= 0:
            return {"error": "This campaign is not holding anything"}
        if campaign.mode == "live":
            # Ask the exchange BEFORE selling. Our books can be stale by minutes
            # — a TP that filled after the stop is the normal case — and a sell
            # placed on a stale position either fails outright or, worse, sells
            # coin belonging to another campaign on the same symbol.
            if await self._settle_ended_position(campaign):
                return {
                    "error": (
                        "The exchange says this position is already gone — it has just been "
                        "reconciled, so there is nothing to sell. Refresh to see the updated row."
                    )
                }
            desired_qty = _coerce_float(campaign.filled_base_qty, 0.0) + _coerce_float(campaign.residual_base_qty, 0.0)
            if desired_qty <= 0:
                return {"error": "This campaign is not holding anything"}

        if campaign.mode != "live":
            # Paper: there is nothing on an exchange to cancel or sell, so book
            # it against the last price the engine saw.
            price_meta = self._price_cache.get(campaign.symbol)
            price = _coerce_float(price_meta[0] if price_meta else 0.0)
            if price <= 0:
                return {"error": "No price available to close the paper position against"}
            self._close_round(campaign, price, sold_qty=desired_qty)
            self._log_event(campaign, "stop", f"Paper position closed at market {price:,.2f}")
            self._archive_campaign(campaign)
            self._emit_update()
            return {"status": "ok", "mode": "paper", "price": price, "quantity": desired_qty}

        # Pull the resting TP FIRST. Its coin is locked while it rests, so a
        # market sell placed alongside it can only reach the unlocked remainder
        # — the same locked-balance trap the TP sizing had to learn.
        if campaign.tp_order_id:
            if not await self._safe_cancel(campaign, campaign.tp_order_id):
                return {"error": "Could not cancel the resting take-profit — not selling on top of it"}
            campaign.tp_order_id = None
            campaign.tp_order_price = None

        free_qty = await self._free_base_balance(campaign)
        sell_qty = desired_qty if free_qty is None else min(desired_qty, free_qty)
        try:
            product = await asyncio.to_thread(self.broker.get_product_by_symbol, campaign.symbol)
        except Exception as exc:
            _log.warning("[CASCADE] liquidate product lookup failed for %s: %s", campaign.symbol, exc)
            product = {}
        step = _coerce_float((product or {}).get("step_size"), 0.0) or float(DEFAULT_LOT_STEP)
        sell_qty = _floor_to_step(sell_qty, step)
        if sell_qty <= 0:
            return {
                "error": f"Nothing sellable — {desired_qty:.8f} rounds to zero at the "
                f"exchange lot step {step:g}. This is dust, not a position."
            }

        client_id = f"cf-csc-{campaign.campaign_id}-liq-{int(time.time())}"
        try:
            result = await asyncio.to_thread(
                lambda: self.broker.place_order(
                    campaign.symbol,
                    0.0,
                    "sell",
                    order_type="market_order",
                    client_order_id=client_id,
                    base_qty=sell_qty,
                )
            )
        except Exception as exc:
            result = {"error": str(exc)}
        error = (result or {}).get("error") if isinstance(result, dict) else "unknown error"
        if error:
            self._log_event(campaign, "error", f"Market sell failed: {error}")
            self._alert(
                "Cascade market sell FAILED",
                f"{campaign.symbol} #{campaign.seq} (LIVE)\n"
                f"Tried to sell {sell_qty:.8f} at market and Binance refused: {error}\n\n"
                f"The take-profit has already been cancelled — this position now has NO resting sell.",
                level="error",
            )
            return {"error": str(error)}

        executed = _coerce_float(result.get("executedQty"), sell_qty)
        quote = _coerce_float(result.get("cummulativeQuoteQty"))
        price_meta = self._price_cache.get(campaign.symbol)
        exit_price = (
            quote / executed if executed > 0 and quote > 0 else _coerce_float(price_meta[0] if price_meta else 0.0)
        )
        campaign.residual_base_qty = max(round(desired_qty - executed, 12), 0.0)
        self._close_round(campaign, exit_price, sold_qty=executed)
        self._log_event(
            campaign,
            "stop",
            f"Position sold at market: {executed:.8f} @ {exit_price:,.2f}"
            + (f" — {campaign.residual_base_qty:.8f} left as unsellable dust" if campaign.residual_base_qty else ""),
        )
        self._archive_campaign(campaign)
        self._emit_update()
        return {
            "status": "ok",
            "mode": "live",
            "price": exit_price,
            "quantity": executed,
            "residual": campaign.residual_base_qty,
        }

    async def set_mode(self, campaign_id: str, mode: str) -> dict:
        campaign = self.campaigns.get(campaign_id)
        if campaign is None:
            return {"error": f"Campaign {campaign_id} not found"}
        mode = str(mode or "").strip().lower()
        if mode != "live":
            return {"error": "Campaigns can only be flipped from paper to live"}
        if campaign.mode == "live":
            return {"status": "ok", "campaign": campaign.to_dict()}
        if campaign.all_fills:
            return {"error": "Campaign already has paper fills — start a fresh live campaign instead"}
        checker = getattr(self.broker, "_is_configured", None)
        if not (callable(checker) and checker()):
            return {"error": "Broker API keys are not configured — cannot go live"}
        campaign.mode = "live"
        self._log_event(campaign, "mode", f"Campaign {campaign_id} switched to LIVE")
        await self._sync_live_orders(campaign)
        self._emit_update()
        return {"status": "ok", "campaign": campaign.to_dict()}

    def _active_duplicate(self, symbol: str, mother_ts: int, mother_high: float) -> Optional[Campaign]:
        """
        A campaign is identified by its symbol and its mother candle. Two live
        ones on the same anchor would draw the same structure and place the same
        orders twice, doubling the position without doubling the intent — so
        starting a second is refused, whether it came from a double submit, a
        replayed request, or a restore.
        """
        for campaign in self.campaigns.values():
            if (
                campaign.state in ACTIVE_STATES
                and campaign.symbol == symbol
                and campaign.mother_timestamp == mother_ts
                and abs(campaign.mother_high - mother_high) < 1e-9
            ):
                return campaign
        return None

    def _next_seq(self) -> int:
        """Campaign numbers run in start order and are never reused, so a
        deleted campaign does not renumber the ones around it."""
        seen = [c.seq for c in self.campaigns.values()]
        seen += [int(row.get("seq") or 0) for row in self.closed_campaigns]
        return (max(seen) if seen else 0) + 1

    def delete_campaign(self, campaign_id: str) -> dict:
        """
        Remove a campaign from the live set. It is archived rather than
        discarded — a deleted campaign still happened, and its fills and rounds
        stay reviewable in history. Purging the record entirely is a separate,
        explicit action (purge_closed_campaign).
        """
        campaign = self.campaigns.pop(campaign_id, None)
        if campaign is None:
            return {"error": f"Campaign {campaign_id} not found"}
        self._candles.pop(campaign_id, None)
        self._last_sync_ts.pop(campaign_id, None)
        if not any(row.get("campaign_id") == campaign_id for row in self.closed_campaigns):
            if not campaign.close_reason:
                campaign.close_reason = "deleted"
                campaign.closed_at = _ist_now_str()
            self._archive_campaign(campaign)
        self._emit_update()
        return {"status": "ok"}

    def purge_closed_campaign(self, campaign_id: str) -> dict:
        """Drop a campaign from the closed history for good."""
        before = len(self.closed_campaigns)
        self.closed_campaigns = [row for row in self.closed_campaigns if row.get("campaign_id") != campaign_id]
        self._emit_update()
        return {"status": "ok", "removed": before != len(self.closed_campaigns)}

    def load_closed_campaigns(self, rows: List[dict]) -> None:
        """Seed history from the store on restart, newest last, without dupes."""
        merged = {}
        for row in list(rows or []) + list(self.closed_campaigns):
            key = row.get("campaign_id")
            if key:
                merged[key] = row
        self.closed_campaigns = sorted(merged.values(), key=lambda r: str(r.get("closed_at") or ""))[
            -CLOSED_HISTORY_LIMIT:
        ]

    def get_status(self) -> dict:
        campaigns = []
        for campaign in self.campaigns.values():
            payload = campaign.to_dict()
            payload["display_tp_price"] = compute_tp_price(campaign)
            payload["spent_usd"] = round(campaign.spent_usd, 2)
            payload["resting_usd"] = round(campaign.resting_usd, 2)
            payload["pending_usd"] = round(campaign.pending_usd, 2)
            payload["pending_line"] = campaign.pending_line
            payload["pending_stop_price"] = campaign.pending_stop_price
            payload["pending_limit_price"] = campaign.pending_limit_price
            payload["rung_usd"] = rung_size_usd(campaign)
            price_meta = self._price_cache.get(campaign.symbol)
            last_price = price_meta[0] if price_meta else None
            payload["last_price"] = last_price
            # How far price is down from the mother high right now, and how far
            # the deepest leg has been — the latter is what sizes the pools.
            payload["fall_pct_from_mother"] = (
                round((campaign.mother_high - last_price) / campaign.mother_high * 100, 4)
                if last_price and campaign.mother_high > 0
                else None
            )
            payload["allocated_pct"] = round(campaign.cumulative_used_pct, 4)
            # These two are FRACTIONS on the campaign and stay fractions here.
            # They used to be rewritten as `* 100` for display under the same
            # keys to_dict already uses — and this payload is what gets
            # persisted (app._snapshot_cascade_runtime), so from_dict read the
            # display number straight back as a fraction and every restart
            # multiplied the fib-size gate by another 100. Nothing rendered
            # them; the only thing the scaling ever did was corrupt the state.
            # Anything that wants percent multiplies at the point it prints.
            payload["min_fib_range_pct_display"] = round(campaign.min_fib_range_pct * 100, 4)
            payload["median_bar_pct_display"] = round(campaign.median_bar_pct * 100, 4)
            # Ground a sibling had already funded when this campaign was born,
            # so the strip can say why its pools are smaller than the raw fall
            # implies. Empty for a campaign that started on clear ground.
            payload["funded_bands"] = [[low, high] for low, high in campaign.funded_bands]
            payload["netted_pct"] = (
                round(
                    sum(high - low for low, high in campaign.funded_bands) / campaign.mother_high * 100,
                    4,
                )
                if campaign.funded_bands and campaign.mother_high > 0
                else 0.0
            )
            payload["rounds_closed"] = len(campaign.rounds)
            payload["realized_pnl_total"] = round(campaign.realized_pnl_total, 2)
            payload["carry_forward_usd"] = round(campaign.carry_forward_usd, 2)
            payload["stale_model"] = campaign.model_version != MODEL_VERSION
            payload["model_version"] = campaign.model_version
            campaigns.append(payload)
        return {
            "status": "ok",
            "running": self._running,
            "campaigns": campaigns,
            "closed_campaigns": list(self.closed_campaigns[-40:]),
            "active_count": len(self.active_campaigns),
            "live_count": len(self.live_campaigns),
            "capital_groups": self.capital_group_status(),
            "instruments": self.instrument_stacks(),
            "updated_at": _ist_now_str(),
        }

    async def _chart_candles(self, campaign: Campaign, max_candles: int) -> List[Candle]:
        """Closed candles from the mother candle forward, in the campaign's own
        timeframe — the same candles the engine stepped, so the chart and the
        geometry can never disagree."""
        tf_sec = campaign.timeframe_sec
        try:
            df = await self.broker.async_get_candles(campaign.symbol, resolution=campaign.timeframe)
        except Exception as exc:
            _log.warning("[CASCADE] chart candle fetch failed for %s: %s", campaign.symbol, exc)
            return []
        if df is None or getattr(df, "empty", True):
            return []
        now = int(time.time())
        rows = []
        for index, row in df.iterrows():
            ts = int(index.timestamp())
            # A manually selected mother may sit inside a higher-timeframe bar
            # (for example 19:35 inside Binance's 19:30–20:30 1H candle).
            # Filtering strictly from its timestamp discarded that very candle:
            # the chart could draw the mother-high line but never the mother
            # candle itself.  Keep the closed bar which CONTAINS the mother.
            if ts + tf_sec <= campaign.mother_timestamp or ts + tf_sec > now:
                continue
            rows.append(
                Candle(
                    timestamp=ts,
                    open=_coerce_float(row.get("open")),
                    high=_coerce_float(row.get("high")),
                    low=_coerce_float(row.get("low")),
                    close=_coerce_float(row.get("close")),
                    timeframe=campaign.timeframe,
                )
            )
        return rows[-max_candles:]

    # What survives a replay. Everything NOT named here is derived from the
    # candles and is rebuilt from its dataclass default, so a field added later
    # cannot be forgotten. It was a hand-written reset list that caused this:
    # `collected` and `pending_usd` were added to Campaign long after the list
    # was written, so every Recalc replayed the same levels on top of the
    # previous run's total. Six presses turned a $7.60 pot into $46.62, and
    # once the inflated total cleared the rung it armed a buy stop for money
    # no level had actually collected.
    _RECALC_KEEP = frozenset(
        {
            # identity and settings
            "campaign_id",
            "symbol",
            "capital_usd",
            "mother_high",
            "mother_low",
            "mother_timestamp",
            "seq",
            "mode",
            # Settings, not replay output. A recalc that reset these would
            # replay a 1D campaign as if it were 5m.
            "timeframe",
            "start_timeframe",
            "escalates",
            "mc_kind",
            # Birth settings, not replay output. The ledger a campaign was born
            # against is fixed for its life, and a recalc that cleared it would
            # replay every leg funding ground a sibling had already paid for.
            "funded_bands",
            "funded_floor_price",
            "min_notional_usd",
            # Measured at birth from the instrument's own bars, not replayed.
            "min_fib_range_pct",
            "median_bar_pct",
            "tick_size",
            "parent_campaign_id",
            "generation",
            "barren_chain",
            "created_at",
            "event_log",
            # Monotonic, NOT derived. Client ids are cf-csc-{id}-buy-{rev} and
            # Binance remembers every id it has seen, so rewinding these would
            # collide with orders this campaign already placed.
            "pending_rev",
            "tp_rev",
        }
    )

    def _reset_derived_state(self, campaign: Campaign) -> None:
        """Return every replay-derived field to its default before a replay."""
        for spec in dataclass_fields(Campaign):
            if spec.name in self._RECALC_KEEP:
                continue
            if spec.default is not MISSING:
                setattr(campaign, spec.name, spec.default)
            elif spec.default_factory is not MISSING:  # type: ignore[misc]
                setattr(campaign, spec.name, spec.default_factory())  # type: ignore[misc]
        campaign.window_start_ts = campaign.mother_timestamp
        campaign.model_version = MODEL_VERSION

    async def _cancel_campaign_orders(self, campaign: Campaign) -> int:
        """Cancel every order this campaign still has resting on the exchange."""
        cancelled = 0
        try:
            open_orders = await self._open_orders_by_id(campaign)
        except Exception as exc:
            _log.warning("[CASCADE] could not list open orders for %s: %s", campaign.campaign_id, exc)
            return 0
        for order_id, row in open_orders.items():
            client_id = str(row.get("clientOrderId") or "")
            if client_id.startswith(f"cf-csc-{campaign.campaign_id}-"):
                await self._safe_cancel(campaign, order_id)
                cancelled += 1
        if cancelled:
            self._log_event(
                campaign,
                "order",
                f"Cancelled {cancelled} resting order(s) before replaying the campaign.",
            )
        return cancelled

    async def recalculate_campaign(self, campaign_id: str) -> dict:
        """
        Rebuild a campaign's trendlines and fibs from scratch under the current
        rules, replaying every candle from the mother candle. Stored campaigns
        keep whatever geometry the rules produced when they ran, so a campaign
        created under older rules keeps stale fibs until this is run.

        Refused when real money is involved: a live campaign that has TRADED
        cannot have its ladder rewritten underneath it. That means holding coin
        (all_fills) or having closed a round (rounds) — the second half of that
        was missing and cost real history.

        A replay only regenerates what it can derive from candles, and fills are
        simulated for PAPER campaigns alone (_paper_fill_check). So a paper
        campaign's rounds are replay output and are correctly rebuilt, while a
        live campaign's rounds are what the exchange actually did: nothing
        regenerates them, and _reset_derived_state wiping them is permanent.
        A live campaign that closed a round is flat, so all_fills is empty and
        the old guard waved it straight through — SOLUSDT lost two closed rounds
        and the +$0.45 realised against them, and the Closed Rounds ledger has
        never shown a round belonging to a running campaign since.

        Refusing is the fix rather than preserving the rounds across the reset,
        because the rest of that campaign's traded state cannot be rebuilt
        either: the levels a closed round bought are marked CLOSED on their
        legs, the replay rebuilds every leg PENDING, and the ladder would go
        back and re-buy the shelf it just sold — the new-low rule relies on
        exactly those markings.
        """
        campaign = self.campaigns.get(campaign_id)
        if campaign is None:
            return {"error": f"Campaign {campaign_id} not found"}
        if campaign.mode == "live" and (campaign.all_fills or campaign.rounds):
            return {
                "error": (
                    "Live campaign that has traded cannot be recalculated — its fills and closed "
                    "rounds came from the exchange, not from the replay, so rebuilding would erase "
                    "them for good. Stop it and start a fresh one."
                )
            }

        candles = await self._chart_candles(campaign, max_candles=100000)
        if not candles:
            return {"error": "No candles available to replay"}

        # A live campaign can be carrying a resting buy stop for the pot the
        # replay is about to erase. Cancel it FIRST: if it were left working it
        # would still be an order for money the campaign no longer believes it
        # collected, and a fill would land on a position nothing is tracking.
        if campaign.mode == "live":
            await self._cancel_campaign_orders(campaign)

        self._reset_derived_state(campaign)
        self._candles[campaign_id] = []

        for candle in candles:
            if candle.timestamp <= campaign.mother_timestamp:
                continue  # the mother candle would trivially break its own high
            self._candles[campaign_id].append(candle)
            self._process_candle(campaign, candle)
            campaign.last_processed_ts = candle.timestamp
            if campaign.state in FINAL_STATES:
                break

        self._log_event(
            campaign,
            "recalc",
            f"Recalculated under model v{MODEL_VERSION}: replayed {len(candles)} candles -> "
            f"{len(campaign.trendlines)} trendline(s), {len(campaign.legs)} fib(s)",
        )
        self._emit_update()
        return {"status": "ok", "campaign": campaign.to_dict(), "candles_replayed": len(candles)}

    @staticmethod
    def _aggregate_candles(candles: List[Candle], bucket_sec: int, base_sec: int = FIVE_MIN_SEC) -> List[Candle]:
        """
        Roll the campaign's own candles up into larger view buckets. The engine
        only ever reasons in the campaign timeframe — this is purely so a 5m
        campaign can be read at 15m or 1H without the geometry (which is
        5m-derived) shifting underneath it.
        """
        if bucket_sec <= base_sec:
            return candles
        out: List[Candle] = []
        current: Optional[Candle] = None
        current_bucket = None
        for c in candles:
            bucket = (c.timestamp // bucket_sec) * bucket_sec
            if current is None or bucket != current_bucket:
                if current is not None:
                    out.append(current)
                current = Candle(timestamp=bucket, open=c.open, high=c.high, low=c.low, close=c.close)
                current_bucket = bucket
            else:
                current.high = max(current.high, c.high)
                current.low = min(current.low, c.low)
                current.close = c.close
        if current is not None:
            out.append(current)
        return out

    def _closed_campaign(self, campaign_id: str) -> Optional[Campaign]:
        """Rebuild an ended campaign from the closed history.

        Archiving drops a campaign out of self.campaigns, so anything that only
        looked there could not see it — which is why Closed Campaigns' Chart
        button did nothing. The geometry is all in the stored snapshot.
        """
        for row in reversed(self.closed_campaigns):
            if isinstance(row, dict) and str(row.get("campaign_id")) == str(campaign_id):
                try:
                    return Campaign.from_dict(row)
                except Exception as exc:  # a malformed old record must not 500
                    _log.warning("[CASCADE] closed campaign %s could not be read: %s", campaign_id, exc)
                    return None
        return None

    @staticmethod
    def _auto_chart_timeframe(campaign: Campaign, max_candles: int, end_ts: int = 0) -> str:
        """The smallest timeframe that still fits the WHOLE campaign on screen.

        The chart takes the last `max_candles` buckets, so on 5m a campaign
        older than about 25 hours pushed its own mother candle off the left
        edge — and the mother candle is the one thing every line on the chart
        is measured from. Rolling up to 15m and then 1H keeps the entire
        campaign, mother included, inside one screen.

        View only. The engine still computes every fib, trendline and entry
        from the campaign's own candles regardless of what is being displayed.
        """
        options = chart_timeframes_for(campaign.timeframe)
        # For a finished campaign the span ends where the TRADE ended, not at
        # the wall clock. Measuring to "now" made a closed trade's chart zoom
        # out a rung further every few hours — the same record, redrawn coarser
        # each time you opened it, until a 5m trade was being shown on 1H bars.
        span_end = int(end_ts) if end_ts else int(time.time())
        span_sec = max(span_end - int(campaign.mother_timestamp or 0), 0)
        budget = max(int(max_candles), 1)
        chosen = campaign.timeframe
        for name, bucket_sec in options.items():
            chosen = name
            if span_sec <= bucket_sec * budget:
                break
        return chosen

    @staticmethod
    def _campaign_end_ts(campaign: Campaign) -> int:
        """When this campaign stopped acting, or 0 while it is still running.

        Taken from the last thing it actually DID — a round's exit, or a fill
        still open when it ended — rather than the wall clock, so a campaign
        stopped by hand hours after its last trade still charts to the trade.
        """
        if campaign.state not in FINAL_STATES:
            return 0
        stamps = [int(r.closed_ts or 0) for r in campaign.rounds]
        stamps += [int(f.timestamp or 0) for f in campaign.all_fills]
        latest = max(stamps) if stamps else 0
        # A campaign that ended without ever filling still has a story — the
        # mother and the structure it drew. Fall back to the mother so the
        # window is anchored somewhere real rather than collapsing to nothing.
        return latest or int(campaign.mother_timestamp or 0)

    async def get_chart_data(self, campaign_id: str, max_candles: int = 200, timeframe: str = "auto") -> dict:
        """
        Candles plus the geometry the engine actually used — trendline anchors,
        each leg's fib anchors/levels, ladder order prices and fills — so the
        marked levels can be verified visually against a real chart.

        Ended campaigns work too — they are read back out of the closed history,
        which is the whole point of the Chart button on that table.
        """
        campaign = self.campaigns.get(campaign_id) or self._closed_campaign(campaign_id)
        if campaign is None:
            return {"error": f"Campaign {campaign_id} not found"}

        # Always pull a full window straight from the broker rather than the
        # engine's in-memory list: that list is only what this process has
        # stepped through, so after a restart it can hold a handful of candles
        # and the chart would render almost empty.
        options = chart_timeframes_for(campaign.timeframe)
        base_sec = campaign.timeframe_sec
        requested_input = str(timeframe).lower()
        requested = requested_input
        trade_end_ts = self._campaign_end_ts(campaign)
        auto_timeframe = self._auto_chart_timeframe(campaign, max_candles, end_ts=trade_end_ts)
        mother_forced_visible = False
        if requested == "auto" or requested not in options:
            requested = auto_timeframe
        # A mother candle is the anchor for every trendline and fib, so showing
        # a hand-picked smaller view that pushes it off-screen is misleading.
        # A manual selection may zoom OUT (to 15m/1H/4H), but never IN far
        # enough to hide the mother.  This changes presentation only; all
        # trading calculations retain the campaign's own timeframe.
        elif options[requested] < options[auto_timeframe]:
            requested = auto_timeframe
            mother_forced_visible = True
        bucket_sec = options.get(requested, base_sec)
        # Pull enough base candles that the rolled-up view still spans the window.
        raw_needed = max_candles * max(bucket_sec // base_sec, 1)
        history = await self._chart_candles(campaign, raw_needed)
        if not history:
            history = self._candles.get(campaign_id) or []

        view = self._aggregate_candles(history, bucket_sec, base_sec)
        # A finished trade is a record, not a live feed. Cut the view off at the
        # candle the campaign ended in (plus a short tail so the exit is not
        # jammed against the right edge), otherwise every later view redraws a
        # longer and longer chart of price doing things this trade had no part
        # in — and the entry/exit markers shrink into an unreadable corner.
        if trade_end_ts:
            cutoff = trade_end_ts + _CHART_TAIL_BUCKETS * bucket_sec
            trimmed = [c for c in view if c.timestamp <= cutoff]
            # Never trim to nothing: a campaign that ended before any candle was
            # stepped would otherwise render an empty chart.
            if trimmed:
                view = trimmed
        candles = [
            {
                "t": c.timestamp,
                "o": c.open,
                "h": c.high,
                "l": c.low,
                "c": c.close,
                # The displayed bar may be a roll-up; flag the bucket that
                # contains the actual mother timestamp so the client can make
                # it unmistakable instead of relying on a high-price line.
                "is_mother": c.timestamp <= campaign.mother_timestamp < c.timestamp + bucket_sec,
            }
            for c in view[-max_candles:]
        ]
        # Always include the mother candle itself as the left anchor of the view.
        mother = {
            "t": campaign.mother_timestamp,
            "high": campaign.mother_high,
            "low": campaign.mother_low,
        }
        trendlines = [
            {
                "id": tl.trendline_id,
                "a1": {"t": tl.anchor1_timestamp, "p": tl.anchor1_price},
                "a2": {"t": tl.anchor2_timestamp, "p": tl.anchor2_price},
                "active": tl.trendline_id == campaign.active_trendline_id,
                "bears_fib": tl.bears_fib,
            }
            for tl in campaign.trendlines
        ]
        legs = []
        for leg in campaign.legs:
            levels = {}
            if leg.fib:
                levels = {str(lv): leg.fib.level_price(lv) for lv in (0, 1, 2, 4, 8)}
            legs.append(
                {
                    "leg_id": leg.leg_id,
                    "trendline_id": leg.trendline_id,
                    "touch_high": leg.touch_high,
                    "touch_timestamp": leg.touch_timestamp,
                    "low": leg.low,
                    "finalized": leg.finalized,
                    "escalated": leg.escalated,
                    "pool_usd": leg.pool_usd,
                    "fall_pct_from_mother": leg.leg_pct_from_mother,
                    "allocation_pct": leg.allocation_pct,
                    "netted_pct": leg.netted_pct,
                    "levels": levels,
                    "orders": [
                        {
                            "level": order.level,
                            "price": order.price,
                            "usd_notional": order.usd_notional,
                            "status": order.status,
                            "fill_price": order.fill_price,
                            "own_usd": order.own_usd,
                            "received": order.received,
                            "moved_usd": order.moved_usd,
                            "moved_to_level": order.moved_to_level,
                        }
                        for order in sorted(leg.pending_orders.values(), key=lambda o: o.level)
                    ],
                }
            )
        price_meta = self._price_cache.get(campaign.symbol)
        return {
            "status": "ok",
            "campaign_id": campaign.campaign_id,
            "symbol": campaign.symbol,
            "state": campaign.state,
            "mode": campaign.mode,
            "mother": mother,
            "timeframe": requested,
            "timeframe_auto": requested_input == "auto",
            "mother_forced_visible": mother_forced_visible,
            # What this campaign may be drawn at: its own timeframe and the two
            # above it. A 4H campaign has no 5m history, so the chart must not
            # offer 5m for it.
            "timeframe_options": list(options.keys()),
            "campaign_timeframe": campaign.timeframe,
            "candles": candles,
            "trendlines": trendlines,
            "legs": legs,
            "fills": [f.to_dict() for f in campaign.all_fills],
            # Entries alone only tell half the trade. Each closed round carries
            # the buys that made it and the price it was sold at, so the record
            # can mark both ends of every cycle instead of a cloud of dots that
            # never resolves into an exit.
            "entries": [
                {
                    "t": int(fill.get("timestamp") or 0),
                    "price": _coerce_float(fill.get("price"), 0.0),
                    "round": r.round_id,
                }
                for r in campaign.rounds
                for fill in (r.fills or [])
            ]
            + [{"t": int(f.timestamp or 0), "price": f.price, "round": None} for f in campaign.all_fills],
            "exits": [
                {
                    "t": int(r.closed_ts or 0),
                    "price": r.exit_price,
                    "round": r.round_id,
                    "pnl": r.pnl,
                    "avg_entry": r.avg_entry,
                }
                for r in campaign.rounds
                if r.closed_ts and r.exit_price
            ],
            # On a FROZEN record these must describe the trade that happened,
            # not the campaign's live fields. A stopped campaign still carries a
            # live average and a target recomputed from it, so the frozen chart
            # was drawing today's numbers over a finished trade; and a campaign
            # that closed at TP has both cleared to None, so it drew no lines at
            # all. The last round is the truth in both cases.
            "avg_entry_price": (
                (campaign.rounds[-1].avg_entry if campaign.rounds else campaign.avg_entry_price)
                if trade_end_ts
                else campaign.avg_entry_price
            ),
            "tp_price": (
                (campaign.rounds[-1].exit_price if campaign.rounds else compute_tp_price(campaign))
                if trade_end_ts
                else compute_tp_price(campaign)
            ),
            "last_price": price_meta[0] if price_meta else None,
            # Set once the campaign is over: the client renders a frozen record
            # rather than a live chart, and stops offering the timeframe toggle.
            "trade_end_ts": trade_end_ts,
            "frozen": bool(trade_end_ts),
            "close_reason": campaign.close_reason or "",
        }

    def restore_campaigns(self, snapshots: List[dict]) -> int:
        restored = 0
        for snapshot in snapshots or []:
            try:
                campaign = Campaign.from_dict(snapshot)
            except Exception as exc:
                _log.warning("[CASCADE] failed to restore campaign: %s", exc)
                continue
            self.campaigns[campaign.campaign_id] = campaign
            restored += 1
        self._repair_inherited_mc_kind()
        self._backfill_closed_history()
        return restored

    def _repair_inherited_mc_kind(self) -> int:
        """Relabel successors that the old restart rule stamped as minors.

        Every auto-restart used to be born mc_kind="minor" purely because it
        comes back on 5m. A break or a retest restarts a campaign as soon as
        price climbs back to the mother, so within a round or two every chain
        on the page read MINOR MC — including campaigns alone on their symbol
        with no major anywhere near them. The kind is inherited now, but the
        campaigns already on disk still carry the old label, so they are walked
        back to the campaign Phil actually started and given its kind.

        Only a chain whose root is still on hand is touched. A parent pruned out
        of history says nothing about where the chain began, and guessing
        "major" there would strip a genuine minor of its stand-down.
        """
        kinds = {c.campaign_id: str(c.mc_kind or "major").lower() for c in self.campaigns.values()}
        parents = {c.campaign_id: str(c.parent_campaign_id or "") for c in self.campaigns.values()}
        for row in self.closed_campaigns:
            cid = str(row.get("campaign_id") or "")
            if cid and cid not in kinds:
                kinds[cid] = str(row.get("mc_kind") or "major").lower()
                parents[cid] = str(row.get("parent_campaign_id") or "")

        repaired = 0
        for campaign in self.campaigns.values():
            if campaign.mc_kind != "minor" or not campaign.parent_campaign_id:
                continue
            seen = {campaign.campaign_id}
            current = campaign.parent_campaign_id
            root = ""
            while current and current not in seen:
                if current not in kinds:
                    root = ""
                    break
                seen.add(current)
                root = current
                current = parents.get(current) or ""
            if kinds.get(root) != "major":
                continue
            campaign.mc_kind = "major"
            kinds[campaign.campaign_id] = "major"
            for row in self.closed_campaigns:
                if row.get("campaign_id") == campaign.campaign_id:
                    row["mc_kind"] = "major"
            self._log_event(
                campaign,
                "info",
                "Relabelled MAJOR MC. It was marked minor only because it restarted on 5m — "
                "it carries on the major move it descends from, and a minor is now only ever "
                "a sub-mother started by hand.",
            )
            repaired += 1
        if repaired:
            _log.info("[CASCADE] relabelled %s successor campaign(s) back to major", repaired)
        return repaired

    def _backfill_closed_history(self) -> int:
        """
        Adopt already-ended campaigns into the closed list.

        A campaign that ended while holding a position used to skip archiving
        entirely, so it stayed in the live set and never reached history. The
        campaign itself was persisted intact — rounds and all — so those can be
        recovered rather than lost. Runs on every restore and is idempotent.
        """
        known = {row.get("campaign_id") for row in self.closed_campaigns}
        adopted = 0
        for campaign in self.campaigns.values():
            if campaign.state not in FINAL_STATES or campaign.campaign_id in known:
                continue
            if not campaign.close_reason:
                campaign.close_reason = campaign.state.lower()
            self.closed_campaigns.append(campaign.to_dict())
            adopted += 1
        if adopted:
            self.closed_campaigns = self.closed_campaigns[-CLOSED_HISTORY_LIMIT:]
            _log.info("[CASCADE] adopted %s ended campaign(s) into closed history", adopted)
        return adopted

    async def reconcile(self, campaign_id: Optional[str] = None) -> dict:
        """Restart recovery: replay missed candles, then sync live orders."""
        targets = (
            [self.campaigns[campaign_id]]
            if campaign_id and campaign_id in self.campaigns
            else list(self.active_campaigns)
        )
        results = {}
        for campaign in targets:
            try:
                await self._candle_step(campaign)
                if campaign.mode == "live" and campaign.state in ACTIVE_STATES:
                    await self._sync_live_orders(campaign)
                results[campaign.campaign_id] = "ok"
            except Exception as exc:
                _log.warning("[CASCADE] reconcile failed for %s: %s", campaign.campaign_id, exc)
                results[campaign.campaign_id] = str(exc)
        self._emit_update()
        return {"status": "ok", "results": results}

    # ── monitor loop ─────────────────────────────────────────────

    async def _monitor_loop(self):
        while self._running:
            try:
                # Without the lock another process owns the orders. Sitting out
                # is the whole point: two engines reconciling one account cancel
                # each other's orders and duplicate fills.
                if not self._acquire_write_lock():
                    if not self._lock_warned:
                        self._lock_warned = True
                        _log.info(
                            "[CASCADE] another instance holds the write lock — "
                            "not placing orders until it exits (normal during a deploy)"
                        )
                    await asyncio.sleep(self._loop_interval_sec)
                    continue
                if self._lock_warned:
                    self._lock_warned = False
                    _log.info("[CASCADE] took the write lock — this instance now owns order placement")
                changed = False
                for campaign in list(self.active_campaigns):
                    try:
                        changed |= await self._campaign_tick(campaign)
                    except Exception as exc:
                        _log.warning("[CASCADE] tick failed for %s: %s", campaign.campaign_id, exc)
                if changed:
                    self._emit_update()
                # Ended campaigns are not in active_campaigns, so nothing above
                # ever looks at them again. Their resting TPs still fill though,
                # and until we ask, the books go on claiming coin that is sold.
                now = time.monotonic()
                if now - self._last_ended_check >= _ENDED_POSITION_CHECK_SEC:
                    self._last_ended_check = now
                    try:
                        await self.reconcile_ended_positions()
                    except Exception as exc:
                        _log.warning("[CASCADE] ended-position sweep failed: %s", exc)
                self._check_watchdogs()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                _log.warning("[CASCADE] monitor loop error: %s", exc)
            await asyncio.sleep(self._loop_interval_sec)

    async def _campaign_tick(self, campaign: Campaign) -> bool:
        changed = False
        # New closed candles drive the state machine.
        stepped = await self._candle_step(campaign)
        changed |= stepped
        # Keep the live price fresh for the UI (Last Price) and paper TP checks.
        had_price = campaign.symbol in self._price_cache
        price = await self._get_price(campaign.symbol)
        if not had_price and price:
            changed = True  # surface the first price so the status card fills in
        # Paper TP check against the live price.
        if campaign.mode == "paper" and campaign.state in ACTIVE_STATES and campaign.filled_base_qty > 0:
            tp = compute_tp_price(campaign)
            if price and tp and price >= tp:
                self._close_round(campaign, tp)
                changed = True
        # Live order sync (throttled).
        now = time.monotonic()
        last_sync = self._last_sync_ts.get(campaign.campaign_id, 0.0)
        # A candle step may have just built a new ladder — get those orders
        # resting on the exchange now rather than up to an interval later.
        due = stepped or (now - last_sync >= self._sync_interval_sec)
        if campaign.mode == "live" and due:
            self._last_sync_ts[campaign.campaign_id] = now
            changed |= await self._sync_live_orders(campaign)
        return changed

    # ── pricing / candles ────────────────────────────────────────

    async def _get_price(self, symbol: str, max_age: float = 4.0) -> float:
        cached = self._price_cache.get(symbol)
        if cached and time.monotonic() - cached[1] < max_age:
            return cached[0]
        try:
            ticker = await asyncio.to_thread(self.broker.get_ticker, symbol)
            price = _coerce_float(ticker.get("last_price") or ticker.get("mark_price"))
        except Exception as exc:
            _log.warning("[CASCADE] price fetch failed for %s: %s", symbol, exc)
            price = cached[0] if cached else 0.0
        self._price_cache[symbol] = (price, time.monotonic())
        return price

    async def _resolve_mother_timestamp(
        self, symbol: str, mother_high: float, timeframe: str = BASE_TIMEFRAME
    ) -> Optional[int]:
        """
        Find the open timestamp of the recent closed candle whose high most
        closely matches mother_high (within ~0.15%). Prefers the most recent
        candle on ties. Returns None if no close match is in the recent window
        (then the caller asks the user to supply the timestamp explicitly).
        """
        tf_sec = timeframe_seconds(timeframe)
        try:
            df = await self.broker.async_get_candles(symbol, resolution=timeframe)
        except Exception as exc:
            _log.warning("[CASCADE] mother candle lookup failed for %s: %s", symbol, exc)
            return None
        if df is None or df.empty or mother_high <= 0:
            return None
        now = int(time.time())
        tolerance = max(mother_high * 0.0015, 0.01)
        best_ts = None
        best_diff = None
        for index, row in df.iterrows():
            ts = int(index.timestamp())
            if ts + tf_sec > now:
                continue  # skip the still-forming candle
            diff = abs(_coerce_float(row.get("high")) - mother_high)
            if diff <= tolerance and (best_diff is None or diff <= best_diff):
                best_diff = diff
                best_ts = ts
        return best_ts

    async def _measure_median_bar_pct(self, symbol: str) -> float:
        """Median 5m high-low range on this symbol, as a fraction of price.

        How loud the instrument is in its own terms. Two days of bars is enough
        to be stable without reaching back into a different regime. Returns 0.0
        if it cannot be measured, which leaves the BTC-calibrated threshold
        standing — a failed fetch must never quietly loosen a real filter.
        """
        try:
            since = int(time.time()) - 2 * 86400
            candles = await self._fetch_closed_candles(symbol, since, BASE_TIMEFRAME)
        except Exception as exc:
            _log.warning("[CASCADE] bar measurement failed for %s: %s", symbol, exc)
            return 0.0
        pcts = sorted((c.high - c.low) / c.close for c in candles if c.close > 0 and c.high >= c.low)
        if len(pcts) < 30:
            return 0.0  # too little history to call it a measurement
        return pcts[len(pcts) // 2]

    async def _fetch_closed_candles(self, symbol: str, since_ts: int, timeframe: str = BASE_TIMEFRAME) -> List[Candle]:
        """Closed candles of `timeframe` with open ts > since_ts, paging as needed."""
        tf_sec = timeframe_seconds(timeframe)
        now = int(time.time())
        candles: List[Candle] = []
        cursor = since_ts
        for _ in range(30):  # safety cap: 30 pages of 1000 bars
            start = datetime.utcfromtimestamp(max(cursor - tf_sec, 0)).strftime("%Y-%m-%d")
            try:
                df = await self.broker.async_get_candles(symbol, resolution=timeframe, start=start)
            except Exception as exc:
                _log.warning("[CASCADE] candle fetch failed for %s: %s", symbol, exc)
                break
            if df is None or df.empty:
                break
            batch = []
            for index, row in df.iterrows():
                ts = int(index.timestamp())
                if ts <= cursor or ts + tf_sec > now:
                    continue
                batch.append(
                    Candle(
                        timestamp=ts,
                        open=_coerce_float(row.get("open")),
                        high=_coerce_float(row.get("high")),
                        low=_coerce_float(row.get("low")),
                        close=_coerce_float(row.get("close")),
                        timeframe=timeframe,
                    )
                )
            if not batch:
                break
            candles.extend(batch)
            cursor = batch[-1].timestamp
            if cursor + 2 * tf_sec > now:
                break
        return candles

    async def _candle_step(self, campaign: Campaign) -> bool:
        if campaign.state == MOTHER_BREAK_PENDING:
            return await self._mother_break_confirmation_step(campaign)
        if campaign.state not in ACTIVE_STATES:
            return False
        # The structure timeframe can climb, but a mother break is always
        # judged by closed 5m candles.  Without this watcher a campaign that
        # reached 1H would reset on an hour bar and lose the precise 5m candle
        # the user selected as the new mother.
        if (
            campaign.timeframe != BASE_TIMEFRAME
            and campaign.timeframe in ESCALATION_LADDER
            and campaign.escalates
            and campaign.start_timeframe == BASE_TIMEFRAME
            and await self._mother_break_watch_step(campaign)
        ):
            return True
        now = int(time.time())
        tf = campaign.timeframe
        if campaign.last_processed_ts and now < campaign.last_processed_ts + 2 * campaign.timeframe_sec:
            return False
        history = self._candles.setdefault(campaign.campaign_id, [])
        if not history and campaign.last_processed_ts > campaign.mother_timestamp:
            # Restored campaign: the structure window is derived from candle
            # history, so rebuild everything since the mother candle. Candles
            # already processed are backfilled without re-running the engine.
            prior = await self._fetch_closed_candles(campaign.symbol, campaign.mother_timestamp, tf)
            history.extend(c for c in prior if c.timestamp <= campaign.last_processed_ts)
        new_candles = await self._fetch_closed_candles(campaign.symbol, campaign.last_processed_ts, tf)
        if not new_candles:
            return False
        changed = False
        for candle in new_candles:
            history.append(candle)
            if len(history) > 20000:
                del history[: len(history) - 20000]
            self._process_candle(campaign, candle)
            campaign.last_processed_ts = candle.timestamp
            self._last_candle_ts = time.monotonic()  # proof of life for the stall watchdog
            changed = True
            if campaign.state not in ACTIVE_STATES:
                break
            if self._maybe_escalate(campaign, candle):
                # The rest of the batch is candles of the OLD timeframe. They
                # must not be stepped as if nothing happened — the next tick
                # fetches fresh candles at the new timeframe instead.
                break
        return changed

    async def _mother_break_watch_step(self, campaign: Campaign) -> bool:
        """Detect the first closed 5m wick above an escalated campaign's MC."""
        after_ts = max(int(campaign.mother_watch_last_5m_ts or 0), int(campaign.mother_timestamp or 0))
        candles = await self._fetch_closed_candles(campaign.symbol, after_ts, BASE_TIMEFRAME)
        for candle in candles:
            if candle.timestamp <= after_ts:
                continue
            campaign.mother_watch_last_5m_ts = candle.timestamp
            if candle.high > campaign.mother_high:
                self._mother_broken(campaign, candle)
                return True
        return bool(candles)

    async def _mother_break_confirmation_step(self, campaign: Campaign) -> bool:
        """Count the two 5m candles after a break while the parent is frozen.

        The breaking candle itself remains the replacement mother.  These
        follow-up bars are only a cooling-off window that prevents a rapid move
        above the old high from spawning a new campaign every five minutes.
        """
        if campaign.mother_break_wait_remaining <= 0:
            self._finish_mother_break(campaign)
            return True
        after_ts = int(campaign.mother_break_last_5m_ts or 0)
        if after_ts <= 0:
            return False
        candles = await self._fetch_closed_candles(campaign.symbol, after_ts, BASE_TIMEFRAME)
        changed = False
        for candle in candles:
            if candle.timestamp <= after_ts:
                continue
            campaign.mother_break_last_5m_ts = candle.timestamp
            self._advance_mother_break_confirmation(campaign, candle)
            changed = True
            if campaign.state != MOTHER_BREAK_PENDING:
                break
        return changed

    def _maybe_escalate(self, campaign: Campaign, candle: Candle) -> bool:
        """Climb one rung when the campaign has outgrown its current one.

        Forward-only, by design (Phil's calls, all three):
          - everything already built — trendlines, fibs, the pot, resting
            orders — is frozen untouched; only structure drawn AFTER the switch
            uses the new timeframe
          - a pot that is mid-arm (a buy stop armed and walking the fall)
            finishes on its OLD timeframe: the switch waits until the stop
            fills or disarms
          - the mother candle's high is never touched
        The switch itself only happens on a candle that closes exactly on a
        new-timeframe bucket boundary, so the aggregated history has no partial
        bucket and the next fetch starts clean at the next bucket.
        """
        if not campaign.can_escalate:
            return False
        if campaign.pending_stop_price is not None:
            return False  # mid-arm — finish the arm on the old timeframe first
        old_sec = campaign.timeframe_sec
        bars = (candle.timestamp + old_sec - campaign.mother_timestamp) / old_sec
        if bars <= ESCALATION_BARS:
            return False
        new_tf = next_timeframe_up(campaign.timeframe)
        new_sec = timeframe_seconds(new_tf)
        if (candle.timestamp + old_sec) % new_sec != 0:
            return False  # wait for the candle that completes a new-TF bucket
        old_tf = campaign.timeframe
        history = self._candles.get(campaign.campaign_id) or []
        self._candles[campaign.campaign_id] = self._aggregate_candles(history, new_sec, old_sec)
        campaign.timeframe = new_tf
        # The open of the last COMPLETE new-TF bucket, so the next fetch's
        # strict `> last_processed_ts` starts exactly at the next bucket —
        # no re-processed candles, no gap.
        campaign.last_processed_ts = candle.timestamp + old_sec - new_sec
        self._log_event(
            campaign,
            "escalate",
            f"Escalated {old_tf} -> {new_tf} after {bars:.0f} {old_tf} bars. Everything already "
            f"built is frozen — existing trendlines, fibs, the pot and resting orders are "
            f"untouched; only new structure is drawn on {new_tf} candles.",
        )
        self._alert(
            "Cascade escalated",
            f"{campaign.symbol} #{campaign.seq} outgrew {old_tf} ({bars:.0f} bars since the "
            f"mother candle) and now steps {new_tf} candles.\n\n"
            f"Nothing already built was changed. "
            + (
                f"Next rung: {next_timeframe_up(new_tf)}."
                if campaign.can_escalate
                else "This is the 4H cap — it stays here."
            ),
            level="info",
            dedupe_sec=60.0,
        )
        return True

    def _candles_between(self, campaign: Campaign, until_ts: int) -> List[Candle]:
        history = self._candles.get(campaign.campaign_id, [])
        return [c for c in history if campaign.mother_timestamp < c.timestamp < until_ts]

    # ── state machine ────────────────────────────────────────────
    #
    # 1. Track the dip (running low). A higher high after it is the RISE, which
    #    confirms the dip.
    # 2. Any red candle CLOSING BELOW the dip cuts the swing. If the dip had
    #    been confirmed by a rise, that cut draws the trendline and the fib:
    #       trendline = mother high -> highest high since the previous fib
    #       fib 0     = highest high that reached that line (touch OR break),
    #                   looking only at candles after both the dip and the anchor
    #       fib 1     = the dip
    #    An unconfirmed cut just restarts the swing.
    # 3. There is no candle-count logic anywhere — only rises and cuts.

    def _process_candle(self, campaign: Campaign, candle: Candle) -> None:
        if campaign.state == MOTHER_BREAK_PENDING:
            # A frozen campaign never marks another fib or entry.  These two
            # candles only complete the confirmation window for its successor.
            self._advance_mother_break_confirmation(campaign, candle)
            return
        # Strictly ABOVE. A candle that prints the mother's high exactly is a
        # double top — the ceiling held, and the cascade below it is still
        # valid. Treating equality as a break killed campaigns on their second
        # candle whenever the top was two bars wide, which is common.
        if candle.high > campaign.mother_high:
            self._mother_broken(campaign, candle)
            return
        # A RETRACEMENT that climbs back to just under the mother high leaves no
        # room for a trendline: the line from the mother high to that point comes
        # out nearly flat, and a flat line has no useful touch. Promote that
        # candle to be the new mother candle instead.
        #
        # Only once price has actually fallen away from the mother, though — the
        # bars right after a top are naturally still near it, and without this
        # every campaign would restart on its second candle. "Fallen away" is
        # measured from the mother HIGH, not its low: the low of a single 1m
        # candle is only a few ticks down, which the very next bar clears.
        if candle.low <= campaign.mother_high * (1 - MOTHER_DEPART_PCT):
            campaign.left_mother_range = True
        if campaign.left_mother_range and candle.high >= campaign.mother_high * (1 - MOTHER_RETEST_PCT):
            self._mother_retested(campaign, candle)
            return
        if not campaign.window_start_ts:
            campaign.window_start_ts = campaign.mother_timestamp
        # Every candle feeds the geometry machine: green candles lock lows and
        # break lines above, red candles cut lows and draw structure.
        self._advance_geometry(campaign, candle)
        if campaign.state in ACTIVE_STATES:
            # Fill against the trigger that was resting while this candle formed,
            # THEN let the candle walk it down. The trigger sits a body above the
            # last close, so a candle that wicks up through it really would have
            # been filled — advancing first would hide that.
            if campaign.mode == "paper":
                self._paper_fill_check(campaign, candle)
                self._paper_tp_check(campaign, candle)
            # A new low under the last closed round releases the levels that
            # round bought: fresh ground, so the ladder can work them again.
            self._release_closed_levels(campaign, candle)
            # Levels the candle reached hand their money to the running total,
            # then the total's stop walks down with the fall.
            self._collect_crossed_levels(campaign, candle)
            self._advance_stop_entries(campaign, candle)

    def _advance_geometry(self, campaign: Campaign, candle: Candle) -> None:
        """Phil's two-stage geometry, adjudicated from his charts 2026-07-31.

        TRENDLINES and FIBS are different objects with different triggers.

        The LOW runs down while the market falls and LOCKS when a candle closes
        back above the low candle's close. Once locked, wicks under it move
        nothing — only a red candle CLOSING decisively below it. That close
        draws the next TRENDLINE if one is armed, and starts the next low
        either way. A line stands until some close breaks ABOVE it; only that
        arms the next line: "the previous trendline has to be the reference
        till market doesn't break and closes above."

        A FIB lives on a line. Its touch is a candle testing the line from
        below after a low exists; fib 0 = the touch high, fib 1 = the ultimate
        low since the mother as it stood at the touch. The fib is DRAWN —
        ladder, orders, money — only when its own level 1 is decisively closed
        below: the levels exist for the way down, and nothing is drawn while
        the market rises.
        """
        # 1. Arming: a close above the standing line spends it.
        line = campaign.active_trendline
        if line is not None and not campaign.geo_armed:
            lv = trendline_price(line, candle.timestamp)
            if lv > 0 and candle.close > lv * (1 + ANCHOR_CLOSE_TOLERANCE_PCT):
                campaign.geo_armed = True
                campaign.broken_above = True
                self._log_event(
                    campaign,
                    "info",
                    f"Close {candle.close:g} broke above trendline {line.trendline_id} "
                    f"({lv:,.2f}). The line is spent — the next one is armed and waits "
                    f"for the low to break.",
                )

        # 2. Pending fibs whose level 1 this red close decisively breaks are
        #    drawn now — the fall has reached the ground their levels fund.
        if candle.close < candle.open:
            self._draw_due_fibs(campaign, candle)

        # 3. A decisive red close below the LOCKED low.
        if (
            campaign.geo_low_locked
            and campaign.geo_low is not None
            and candle.close < candle.open
            and candle.close < campaign.geo_low - campaign.geo_low * DECISIVE_BREAK_PCT
        ):
            if campaign.geo_armed:
                self._geo_draw_trendline(campaign, candle)
            # Armed or not, this fall owns the next low.
            campaign.geo_low = candle.low
            campaign.geo_low_ts = candle.timestamp
            campaign.geo_low_close = candle.close
            campaign.geo_low_locked = False
            campaign.geo_ult_low = candle.low if campaign.geo_ult_low is None else min(campaign.geo_ult_low, candle.low)
            return

        # 4. A live touch on the STANDING line files a pending fib. Touches on
        #    a broken line count for nothing — SOL's 78.37 "pause" fib came
        #    from exactly that — and a touch needs a low before it: the V is
        #    dip first, rise second. A high AT the mother is a double top, not
        #    a touch.
        if (
            line is not None
            and not campaign.geo_armed
            and campaign.geo_low_ts
            and candle.timestamp > campaign.geo_low_ts
            and candle.high < campaign.mother_high
        ):
            lv = trendline_price(line, candle.timestamp)
            if lv > 0 and candle.high >= lv and candle.close < lv:
                fib1 = candle.low if campaign.geo_ult_low is None else min(campaign.geo_ult_low, candle.low)
                self._file_pending_fib(campaign, candle.high, candle.timestamp, fib1, line.trendline_id)

        # 5. Low tracking: run down while falling, lock on the rise, ignore
        #    wicks once locked. A GREEN candle that sets the low locks it
        #    itself — a bar that spikes down and rallies back inside five
        #    minutes is the whole V in one candle, and waiting for a later
        #    close to confirm it missed the user-verified 64,790.01 dip whose
        #    candle closed 114 points off its own low.
        campaign.geo_ult_low = candle.low if campaign.geo_ult_low is None else min(campaign.geo_ult_low, candle.low)
        if campaign.geo_low is None or candle.low < campaign.geo_low:
            if not campaign.geo_low_locked:
                campaign.geo_low = candle.low
                campaign.geo_low_ts = candle.timestamp
                campaign.geo_low_close = candle.close
                # Only a structure-sized recovery is the whole V — a candle
                # green by a few ticks locking a few-tick wiggle handed TL1 to
                # noise on the steady-fall day.
                if (
                    candle.close > candle.open
                    and (candle.close - candle.low) >= candle.close * campaign.min_fib_range_pct
                ):
                    campaign.geo_low_locked = True
        elif (
            not campaign.geo_low_locked and campaign.geo_low_close is not None and candle.close > campaign.geo_low_close
        ):
            campaign.geo_low_locked = True

    def _file_pending_fib(
        self, campaign: Campaign, touch_high: float, touch_ts: int, fib1: float, trendline_id: int
    ) -> None:
        """Record a touch; the fib itself waits for its level 1 to break.

        The FIRST touch on a level holds as fib 0 — on the 07-20 second-day
        chart Phil paired 64,753.77, the first test of the line, with the
        64,599.89 dip, not the lower re-test that came after. Structures too
        small to ladder are dropped here, the same size gate the old engine
        applied at the cut.
        """
        if fib1 is None or touch_high <= fib1:
            return
        if (touch_high - fib1) < touch_high * campaign.min_fib_range_pct:
            return  # a few ticks of chop, not a swing — its levels would be noise
        for p in campaign.pending_fibs:
            if abs(p["fib1"] - fib1) <= fib1 * 1e-9:
                # Same structure grazing the line again: the TOP graze is fib 0
                # — "the next fib is only drawn if it breaks the previous low,
                # hence we can take the top it grazed the TL".
                if touch_high > p["touch_high"]:
                    p["touch_high"], p["touch_ts"], p["trendline_id"] = touch_high, int(touch_ts), trendline_id
                return
        # A pending whose level the market has now fallen BELOW (this touch
        # froze a deeper ultimate low, and no decisive close ever confirmed the
        # shallower level) was not where the swing turned — the deeper
        # structure supersedes it. The 07-20 00:40 wick pending gives way to
        # the 00:45 touch exactly like this.
        campaign.pending_fibs = [p for p in campaign.pending_fibs if p["fib1"] <= fib1 + fib1 * 1e-9]
        campaign.pending_fibs.append(
            {"touch_high": touch_high, "touch_ts": int(touch_ts), "fib1": fib1, "trendline_id": trendline_id}
        )

    def _draw_due_fibs(self, campaign: Campaign, candle: Candle) -> None:
        """Draw every pending fib whose level 1 this red close decisively broke."""
        due = [
            p
            for p in campaign.pending_fibs
            if candle.timestamp > p["touch_ts"] and candle.close < p["fib1"] - p["fib1"] * DECISIVE_BREAK_PCT
        ]
        for p in due:
            campaign.pending_fibs.remove(p)
            self._draw_fib(campaign, p)

    def _draw_fib(self, campaign: Campaign, pending: dict) -> None:
        """The cut has landed: the structure becomes a leg with a ladder.

        Two structures touched at essentially the same price are the same
        shelf; the second adds nothing but a cancellation of orders that were
        about to fill — unless the ladders do not actually overlap, in which
        case it is a different swing that starts from the same high (BTC #36's
        fib 3 hangs 0.010% from fib 2's touch with a far deeper ladder).
        """
        touch_high, fib1 = pending["touch_high"], pending["fib1"]
        for leg in campaign.legs:
            if not leg.touch_high or not leg.low:
                continue
            if not ladders_overlap(touch_high, fib1, leg.touch_high, leg.low):
                continue
            if abs(touch_high - leg.touch_high) / leg.touch_high >= MIN_LEG_SEPARATION_PCT:
                continue
            # Same shelf — but only a newcomer that is NOT deeper is a
            # duplicate. A structure from the same high whose level 1 sits
            # decisively below the incumbent's is the next swing down and
            # funds ground the incumbent never reaches (#36's fib 3, Phil:
            # "TL3 has to be drawn for the levels to fund below"). On the
            # 07-20 second day the user's 64,753.77/64,599.89 was nearly the
            # small 64,758.24/64,680's twin on top but 80 dollars deeper.
            if fib1 < leg.low - leg.low * DECISIVE_BREAK_PCT:
                continue
            self._log_event(
                campaign,
                "skip",
                f"Fib skipped: its touch {touch_high:,.2f} is on fib {leg.leg_id}'s shelf "
                f"({leg.touch_high:,.2f}), no deeper, and their ladders overlap — fib "
                f"{leg.leg_id}'s ladder stays resting.",
            )
            return
        legs_before = len(campaign.legs)
        self._draw_leg(campaign, touch_high, fib1, pending["touch_ts"], pending["trendline_id"])
        if len(campaign.legs) > legs_before:
            campaign.state = "TRENDLINE_ACTIVE"

    def _geo_draw_trendline(self, campaign: Campaign, candle: Candle) -> None:
        """The locked low broke and a line is armed: draw it if the chart allows.

        Anchor = the red candle with the highest OPEN strictly after the low's
        candle — the break candle itself is a candidate, since on 5m the swing
        top's red is often the breaker. The line from the mother high through
        that open must clear every close from the mother to now, on BOTH sides
        of the anchor; if anything closed across it, NO line is drawn — there
        is no second-best anchor. That is what keeps a chart at two or three
        clean lines instead of a fan, and why PAXG 07-31 has no third line.
        """
        history = self._candles.get(campaign.campaign_id, [])
        window = [c for c in history if campaign.mother_timestamp < c.timestamp <= candle.timestamp]
        cands = [c for c in window if c.timestamp > campaign.geo_low_ts and c.is_red and c.open < campaign.mother_high]
        if not cands:
            return
        anchor = max(cands, key=lambda c: c.open)
        ap, ats = anchor.open, anchor.timestamp
        if ats == campaign.mother_timestamp:
            return
        for c in window:
            if c.timestamp == ats:
                continue
            lv = campaign.mother_high + (ap - campaign.mother_high) * (
                (c.timestamp - campaign.mother_timestamp) / (ats - campaign.mother_timestamp)
            )
            if c.close > lv + abs(lv) * ANCHOR_CLOSE_TOLERANCE_PCT:
                self._log_event(
                    campaign,
                    "skip",
                    f"Low {campaign.geo_low:g} broke, but the line to {ap:g} would cut through "
                    f"the close {c.close:g} — no trendline. The market has not offered a clean "
                    f"line yet.",
                )
                return
        tl = Trendline(
            trendline_id=len(campaign.trendlines) + 1,
            anchor1_price=campaign.mother_high,
            anchor1_timestamp=campaign.mother_timestamp,
            anchor2_price=ap,
            anchor2_timestamp=int(ats),
        )
        campaign.trendlines.append(tl)
        campaign.active_trendline_id = tl.trendline_id
        campaign.geo_armed = False
        campaign.broken_above = False
        campaign.state = "TRENDLINE_ACTIVE"
        self._log_event(
            campaign,
            "trendline",
            f"Trendline {tl.trendline_id} drawn: mother high {campaign.mother_high:g} -> "
            f"red candle open {ap:g}, on the break of the low {campaign.geo_low:g}",
        )
        # The touch may already have happened — the anchor candle usually IS
        # the touch — so the new line's history is read back for it. The break
        # that drew the line can be the fib's own cut in the same breath.
        touch = self._geo_retro_touch(campaign, tl, candle, window)
        if touch is None:
            # Nothing genuine has tested the line: the anchor candle itself is
            # the only touch there is — the line passes through its open, and
            # its high is fib 0 (BTC 07-31's 07:00, where open and high are
            # the same price and no later candle ever reached the line).
            ult = None
            for c in window:
                if c.timestamp >= tl.anchor2_timestamp:
                    break
                ult = c.low if ult is None else min(ult, c.low)
            anchor_candle = next((c for c in window if c.timestamp == tl.anchor2_timestamp), None)
            if ult is not None and anchor_candle is not None and anchor_candle.high < campaign.mother_high:
                fib1 = min(ult, anchor_candle.low)
                if (anchor_candle.high - fib1) >= anchor_candle.high * campaign.min_fib_range_pct:
                    touch = (anchor_candle.high, int(anchor_candle.timestamp), fib1)
        if touch is not None:
            th, tts, fib1 = touch
            pending = {"touch_high": th, "touch_ts": tts, "fib1": fib1, "trendline_id": tl.trendline_id}
            if candle.timestamp > tts and candle.close < fib1 - fib1 * DECISIVE_BREAK_PCT:
                self._draw_fib(campaign, pending)
            else:
                self._file_pending_fib(campaign, th, tts, fib1, tl.trendline_id)

    def _geo_retro_touch(self, campaign: Campaign, tl: Trendline, cut_candle: Candle, window: List[Candle]):
        """The FIRST touch on a just-drawn line, read from the candles behind it.

        The cut candle is excluded — it is the cut, never its own touch (#36's
        20:30 breaker would otherwise displace the 20:25 touch Phil confirmed).
        A touch needs a low before it, so the first candle after the mother can
        never touch: the V is dip first, rise second.
        """
        best = None
        ult = None
        for c in window:
            if (
                ult is not None
                and c.timestamp != cut_candle.timestamp
                and c.high < campaign.mother_high
                # The anchor candle with no wick above its open never TESTED
                # the line — the line merely passes through its open. Only a
                # high poking above counts as the market trying the line.
                and not (c.timestamp == tl.anchor2_timestamp and c.high <= tl.anchor2_price)
            ):
                lv = trendline_price(tl, c.timestamp)
                if lv > 0 and c.high >= lv and c.close < lv:
                    fib1 = min(ult, c.low)
                    if (c.high - fib1) >= c.high * campaign.min_fib_range_pct:
                        if best is None or fib1 < best[2] - best[2] * 1e-9:
                            # a deeper low: the swing has grown — this level
                            # supersedes the shallower one entirely
                            best = (c.high, int(c.timestamp), fib1)
                        elif c.high > best[0]:
                            # same structure grazing again: the TOP graze is
                            # fib 0
                            best = (c.high, int(c.timestamp), best[2])
            ult = c.low if ult is None else min(ult, c.low)
        return best

    def _duplicate_trendline(self, campaign: Campaign, candidate: Trendline, at_ts: int) -> Optional[Trendline]:
        """The existing line this one would sit on top of, if there is one.

        Every trendline shares anchor1 — the mother high — so two of them can
        only differ by slope, and comparing where they land at the candle that
        created the new one is the same thing the eye does. Lines closer than
        MIN_TRENDLINE_SEPARATION_PCT there are one line drawn twice.
        """
        mine = trendline_price(candidate, at_ts)
        if mine <= 0:
            return None
        for tl in campaign.trendlines:
            theirs = trendline_price(tl, at_ts)
            if theirs > 0 and abs(mine - theirs) / theirs < MIN_TRENDLINE_SEPARATION_PCT:
                return tl
        return None

    def _draw_leg(
        self,
        campaign: Campaign,
        touch_high: float,
        swing_low: float,
        touch_ts: Optional[int],
        trendline_id: int,
    ) -> None:
        prior_leg = campaign.current_leg
        leg = Leg(
            leg_id=len(campaign.legs) + 1,
            trendline_id=trendline_id,
            low=swing_low,
            touch_high=touch_high,
            touch_timestamp=int(touch_ts or campaign.mother_timestamp),
        )
        leg.finalized = True
        campaign.legs.append(leg)
        # The previous fib keeps every rung it has. This one adds its own to the
        # pool and to the ladder, and the whole ladder is re-split by price.
        try:
            build_fib_ladder_and_pool(campaign, leg)
            plan_leg_orders(campaign, leg)
        except CascadeModelError as exc:
            campaign.legs.pop()
            self._log_event(campaign, "error", f"Fib rejected: {exc}")
            return

        funded = [
            order
            for lg in campaign.legs
            for order in lg.pending_orders.values()
            if order.status in {"PENDING", "PLACED"} and order.usd_notional > 0
        ]
        funded.sort(key=lambda o: -(o.price or 0.0))
        self._log_event(
            campaign,
            "leg",
            f"Fib {leg.leg_id} drawn on trendline {trendline_id}: 0={touch_high:g} 1={swing_low:g} "
            f"(adds {_coerce_float(leg.allocation_pct):.3f}% = ${_coerce_float(leg.pool_usd):,.2f} to the pool"
            f"{f', {leg.netted_pct:.3f}% netted off as already funded' if leg.netted_pct > 0 else ''}"
            f"{', escalated' if leg.escalated else ''}). Ladder re-split by price — "
            + (
                ", ".join(f"F{o.leg_id} L{o.level} ${o.usd_notional:g} @ {o.price:,.2f}" for o in funded)
                if funded
                else f"pool ${campaign.total_allocation_usd:,.2f} still under one rung, nothing placeable yet"
            ),
        )

    # ── fills / TP ───────────────────────────────────────────────

    def _paper_fill_check(self, campaign: Campaign, closed_candle: Candle) -> None:
        """A buy stop sits ABOVE the market and triggers on the way up. Fill at
        the limit cap: the pessimistic end of the band it can execute in."""
        if campaign.pending_stop_price is None or campaign.pending_usd <= 0:
            return
        # A candle that just set the stop is the fall continuing, not a turn —
        # it owns the trigger and cannot also take it.
        if campaign.pending_stop_ts is not None and closed_candle.timestamp <= campaign.pending_stop_ts:
            return
        if closed_candle.high >= campaign.pending_stop_price:
            self._fill_pending(
                campaign,
                campaign.pending_limit_price or campaign.pending_stop_price,
                closed_candle.timestamp,
            )

    def _paper_tp_check(self, campaign: Campaign, closed_candle: Candle) -> None:
        """Close a paper round when a candle trades through the target.

        The live loop tests the TP against the last traded price, which a
        candle replay does not have — so the replay could OPEN a position and
        never sell it. Recalc is a replay, which meant pressing it on a paper
        campaign erased every closed round and handed back an open position
        that should have been sold hours earlier: the SOL 07-21 campaign
        replayed to a position still open, when its round had in fact closed
        at 78.05 and 79 later candles had traded above the target.
        """
        if campaign.filled_base_qty <= 0:
            return
        tp = compute_tp_price(campaign)
        if not tp or closed_candle.high < tp:
            return
        # A candle that also took the entry cannot be assumed to have reached
        # the target afterwards — the order of ticks inside it is unknowable,
        # so the pessimistic reading is that the target comes later.
        if any(fill.timestamp >= closed_candle.timestamp for fill in campaign.all_fills):
            return
        self._close_round(campaign, tp)

    def _release_closed_levels(self, campaign: Campaign, candle: Candle) -> None:
        """Give back the levels a closed round bought, once price breaks under
        the low that was standing when it closed."""
        if campaign.reuse_below is None or candle.low >= campaign.reuse_below:
            return
        released = [
            (leg, level)
            for leg in campaign.legs
            for level, order in leg.pending_orders.items()
            if order.status == "CLOSED"
        ]
        campaign.reuse_below = None
        if not released:
            return
        for leg, level in released:
            order = leg.pending_orders[level]
            order.status = "PENDING"
            order.filled_qty = 0.0
            order.fill_price = None
            order.fill_timestamp = None
            order.order_id = None
            order.rev += 1
            order.client_order_id = f"cf-csc-{campaign.campaign_id}-{leg.leg_id}-{level}-{order.rev}"
        replan_ladder(campaign)
        self._log_event(
            campaign,
            "level",
            f"New low at {candle.low:,.2f} — {len(released)} level(s) the last round bought are back "
            f"on the ladder: " + ", ".join(f"F{leg.leg_id} L{level}" for leg, level in released),
        )

    def _collect_crossed_levels(self, campaign: Campaign, candle: Candle) -> None:
        """Price reaching a level adds that level's money to the running total.

        A level is not an order. It is a marker saying "this much belongs to
        this price", and the market touching it is what puts the money in play.
        Levels are collected shallowest first so the total builds in the order
        price actually meets them.
        """
        crossed = []
        for leg in campaign.legs:
            for level, order in leg.pending_orders.items():
                if order.status != "PENDING" or not order.price or order.usd_notional <= 0:
                    continue
                if candle.low <= order.price:
                    crossed.append((order.price, leg, level, order))
        if not crossed:
            return
        crossed.sort(key=lambda row: -row[0])
        rung = rung_size_usd(campaign)
        for price, leg, level, order in crossed:
            order.status = "COLLECTED"
            campaign.pending_usd = round(campaign.pending_usd + order.usd_notional, 2)
            campaign.collected.append([leg.leg_id, level, order.usd_notional, price])
            if campaign.pending_line is None and campaign.pending_usd + 1e-9 >= rung:
                # This is the level that made the total placeable. Two reds below
                # THIS line arm the stop.
                campaign.pending_line = price
                self._log_event(
                    campaign,
                    "order",
                    f"F{leg.leg_id} L{level} reached at {price:,.2f} — running total "
                    f"${campaign.pending_usd:,.2f} clears the ${rung:,.2f} minimum. Waiting for "
                    f"two red candles below {price:,.2f} to set the buy stop.",
                )
            else:
                self._log_event(
                    campaign,
                    "level",
                    f"F{leg.leg_id} L{level} reached at {price:,.2f} — +${order.usd_notional:,.2f}, "
                    f"running total ${campaign.pending_usd:,.2f}"
                    + ("" if campaign.pending_line else f" (needs ${rung:,.2f} to buy)"),
                )

    def _advance_stop_entries(self, campaign: Campaign, closed_candle: Candle) -> None:
        """
        The running total goes in as ONE working BUY STOP, and the trigger sits
        at the close of the previous red candle.

        That geometry is the whole idea. While the market keeps falling, each
        new red close drags the stop down with it and nothing fills; the order
        chases price down without ever buying into it. Only when the market
        U-turns and trades back up through that last red body does it trigger —
        the cheapest confirmed entry the fall offered.

        Two reds under the line are needed before anything is placed: the first
        breaks the line, the second confirms the fall and puts the market below
        the trigger (a buy stop has to sit above the market to be a stop at
        all). Greens are ignored entirely, and a red closing higher than the
        last one does not count, because price must keep falling.

        The fall crossing yet another level does not restart anything. That
        level's money simply joins the total the working order already covers.
        """
        if campaign.pending_line is None or campaign.pending_usd <= 0:
            return
        probe = closed_candle
        if probe.timestamp == campaign.pending_stop_ts:
            return
        if probe.close >= probe.open:
            return  # only red candles act, before arming and after
        if probe.close >= campaign.pending_line:
            return  # the line that completed the total has not broken yet
        if campaign.pending_last_red is None:
            campaign.pending_last_red = probe.close
            self._log_event(
                campaign,
                "order",
                f"Line {campaign.pending_line:,.2f} broken at {probe.close:,.2f} — waiting for a "
                f"second red candle to set the stop for ${campaign.pending_usd:,.2f}",
            )
            return
        if probe.close >= campaign.pending_last_red:
            return  # not lower than the previous red — price must keep falling
        # The trigger is the PREVIOUS red close, one body back, so it sits ABOVE
        # where the market just closed. That is what makes it a stop rather than
        # a limit: the fall walks it down and only a turn back up takes it.
        self._set_pending_stop(campaign, campaign.pending_last_red, probe)
        campaign.pending_last_red = probe.close

    def _set_pending_stop(self, campaign: Campaign, trigger: float, probe: Candle) -> None:
        tick = _coerce_float(campaign.tick_size, DEFAULT_TICK_SIZE) or DEFAULT_TICK_SIZE
        stop = round(trigger, 8)
        gap = STOP_LIMIT_GAP_USD.get(campaign.symbol.upper())
        limit = round(trigger + (gap if gap is not None else STOP_LIMIT_OFFSET_TICKS * tick), 8)
        first = campaign.pending_stop_price is None
        if campaign.pending_order_id:
            # The resting order is at the wrong trigger now; drop the id so the
            # exchange sweep cancels it and a fresh one goes out.
            campaign.pending_order_id = None
        campaign.pending_rev += 1
        campaign.pending_stop_price = stop
        campaign.pending_limit_price = limit
        campaign.pending_stop_ts = probe.timestamp
        # The trigger just moved — a new low re-armed the pot lower. Whatever
        # hold was in place is stale; let the next placement re-evaluate against
        # the fresh trigger (and re-log a hold if the market is still far above).
        self._stale_pot_held.discard(campaign.campaign_id)
        self._log_event(
            campaign,
            "order",
            (
                f"Buy stop set for ${campaign.pending_usd:,.2f}: trigger {stop:,.2f} / limit {limit:,.2f}"
                if first
                else f"Buy stop walked down to {stop:,.2f} / limit {limit:,.2f} for ${campaign.pending_usd:,.2f}"
            ),
        )

    def _fill_pending_part(self, campaign: Campaign, price: float, qty: float, timestamp: int) -> None:
        """A stop-limit can execute in pieces. Book the part that traded and
        leave the rest of the total working — the levels stay collected until
        the whole thing is bought."""
        if qty <= 0 or price <= 0:
            return
        spent = min(qty * price, campaign.pending_usd)
        campaign.all_fills.append(
            Fill(
                price=price,
                quantity=qty,
                level=int(campaign.collected[-1][1]) if campaign.collected else 0,
                leg_id=int(campaign.collected[-1][0]) if campaign.collected else 0,
                timestamp=timestamp,
                order_id=str(campaign.pending_order_id or ""),
            )
        )
        campaign.pending_usd = round(max(campaign.pending_usd - spent, 0.0), 2)
        recompute_avg_entry_price(campaign)
        campaign.tp_price = compute_tp_price(campaign)
        self._log_event(
            campaign,
            "fill",
            f"Partial buy: ${spent:,.2f} of the collected total at {price:,.2f} — "
            f"${campaign.pending_usd:,.2f} still working",
        )
        if campaign.pending_usd <= 0.01:
            self._settle_pending(campaign, price, timestamp)

    def _settle_pending(self, campaign: Campaign, price: float, timestamp: int) -> None:
        """Mark every collected level bought and clear the pot."""
        for leg_id, level, _usd, _price in campaign.collected:
            leg = next((lg for lg in campaign.legs if lg.leg_id == leg_id), None)
            order = leg.pending_orders.get(level) if leg else None
            if order is not None:
                order.status = "FILLED"
                order.fill_price = price
                order.fill_timestamp = timestamp
        campaign.pending_usd = 0.0
        campaign.collected = []
        campaign.pending_line = None
        campaign.pending_stop_price = None
        campaign.pending_limit_price = None
        campaign.pending_stop_ts = None
        campaign.pending_last_red = None
        campaign.pending_order_id = None
        campaign.pending_filled_qty = 0.0
        self._stale_pot_held.discard(campaign.campaign_id)

    def _fill_pending(self, campaign: Campaign, price: float, timestamp: int, order_id: str = "PAPER") -> None:
        """The turn came: buy everything the fall collected, in one order."""
        usd = campaign.pending_usd
        if usd <= 0 or price <= 0:
            return
        levels = list(campaign.collected)
        deepest = min((row[3] for row in levels), default=price)
        campaign.all_fills.append(
            Fill(
                price=price,
                quantity=usd / price,
                level=int(levels[-1][1]) if levels else 0,
                leg_id=int(levels[-1][0]) if levels else 0,
                timestamp=timestamp,
                order_id=order_id,
            )
        )
        self._settle_pending(campaign, price, timestamp)
        recompute_avg_entry_price(campaign)
        campaign.tp_price = compute_tp_price(campaign)
        self._log_event(
            campaign,
            "fill",
            f"Bought ${usd:,.2f} at {price:,.2f} on the turn — {len(levels)} level(s) collected down to "
            f"{deepest:,.2f} (avg {campaign.avg_entry_price:,.2f}, TP {campaign.tp_price:,.2f})",
        )
        # An entry is money leaving the account. Not deduped: every fill is a
        # distinct event and skipping one would hide a real position.
        self._alert(
            "Cascade ENTRY filled",
            f"{campaign.symbol} #{campaign.seq} ({campaign.mode.upper()}) — {campaign.mc_kind.upper()} MC\n"
            f"Bought ${usd:,.2f} at {price:,.2f}\n"
            f"{len(levels)} level(s) collected down to {deepest:,.2f}\n"
            f"Average entry: {campaign.avg_entry_price:,.2f}\n"
            f"Target: {campaign.tp_price:,.2f}",
            level="info",
        )

    def _record_fill(
        self,
        campaign: Campaign,
        leg: Leg,
        order: PendingOrder,
        price: float,
        timestamp: int,
        order_id: Optional[str] = None,
        quantity: Optional[float] = None,
    ) -> None:
        qty = quantity if quantity is not None else max(order.quantity - order.filled_qty, 0.0)
        if qty <= 0:
            return
        order.filled_qty += qty
        order.fill_price = price
        order.fill_timestamp = timestamp
        if order.filled_qty >= order.quantity - 1e-12:
            order.status = "FILLED"
        fill = Fill(
            price=price,
            quantity=qty,
            level=order.level,
            leg_id=leg.leg_id,
            timestamp=timestamp,
            order_id=order_id or order.order_id,
        )
        campaign.all_fills.append(fill)
        recompute_avg_entry_price(campaign)
        campaign.tp_price = compute_tp_price(campaign)
        self._log_event(
            campaign,
            "fill",
            f"Leg {leg.leg_id} L{order.level} filled: {qty:.8f} @ {price:,.2f} "
            f"(avg {campaign.avg_entry_price:,.2f}, TP {campaign.tp_price:,.2f})",
        )
        # That rung is spent. Re-split what is left over the rungs that remain,
        # so the next buy is planned from the money actually still available.
        replan_ladder(campaign)

    def _close_round(self, campaign: Campaign, exit_price: float, sold_qty: Optional[float] = None) -> None:
        """
        A TP fill closes the current open-to-TP round, not the campaign. The
        principal comes back into available capital and the position resets to
        flat; the cascade keeps running and the freed money is re-split across
        the rungs still waiting. Only a mother-high breach (or a manual stop)
        ends the campaign.
        """
        # What the exchange actually sold, when it told us. Booking the bought
        # quantity instead overstates the round: LOT_SIZE can leave part of it
        # unsold, and the P&L would claim coin that never left the account.
        qty = campaign.filled_base_qty if sold_qty is None else sold_qty
        avg = campaign.avg_entry_price or 0.0
        invested = sum(f.price * f.quantity for f in campaign.all_fills)
        leg = campaign.current_leg
        ordered_fills = sorted(campaign.all_fills, key=lambda f: (f.timestamp, f.level))
        fill_log = [
            {
                "timestamp": int(fill.timestamp or 0),
                "price": fill.price,
                "quantity": fill.quantity,
                "usd": round(fill.price * fill.quantity, 8),
                "level": fill.level,
                "leg_id": fill.leg_id,
                "order_id": fill.order_id,
            }
            for fill in ordered_fills
        ]
        # Candle time, not wall-clock: in a backtest the two are years apart,
        # and the UI reads every cascade timestamp as a candle in IST.
        seen = self._candles.get(campaign.campaign_id) or []
        closed_ts = int(seen[-1].timestamp) if seen else 0
        rnd = Round(
            round_id=len(campaign.rounds) + 1,
            leg_id=leg.leg_id if leg else 0,
            avg_entry=avg,
            quantity=qty,
            invested_usd=round(invested, 8),
            exit_price=exit_price,
            pnl=round((exit_price - avg) * qty, 8),
            closed_at=_ist_now_str(),
            fills=fill_log,
            opened_ts=int(ordered_fills[0].timestamp or 0) if ordered_fills else 0,
            closed_ts=closed_ts,
        )
        campaign.rounds.append(rnd)
        # The target landing is the event the whole campaign exists to produce.
        # Alerted here rather than at campaign close, because a round closing is
        # NOT a campaign closing — the cascade keeps running, and a TP that only
        # announced itself when the mother finally broke could be hours late or
        # never arrive at all.
        self._alert(
            "Cascade TARGET hit",
            f"{campaign.symbol} #{campaign.seq} ({campaign.mode.upper()}) — {campaign.mc_kind.upper()} MC\n"
            f"Round {rnd.round_id} closed at {exit_price:,.2f}\n"
            f"Average entry: {avg:,.2f}  ·  Qty: {qty:g}\n"
            f"P&L: {'+' if rnd.pnl >= 0 else ''}${rnd.pnl:,.2f}\n\n"
            f"Campaign realised so far: ${campaign.realized_pnl_total:,.2f}. "
            f"The campaign keeps running — only a mother break or a stop ends it.",
            level="info",
        )

        # Flatten the position: principal returns to available capital.
        campaign.all_fills = []
        campaign.filled_base_qty = 0.0
        campaign.avg_entry_price = None
        campaign.tp_price = None
        campaign.tp_order_id = None
        campaign.tp_order_price = None
        campaign.tp_min_notional_notice = None
        campaign.tp_rev += 1
        campaign.realized_pnl = round(campaign.realized_pnl_total, 8)

        # Filled entries are spent and gone; anything still resting stays live —
        # on every fib, not just the newest, since they all rest together.
        for lg in campaign.legs:
            for order in lg.pending_orders.values():
                if order.status == "FILLED":
                    order.status = "CLOSED"

        # The levels this round bought are spent for now, but their money came
        # back with the principal — so they are not gone, only parked. They come
        # back onto the ladder the moment price makes a new low under where the
        # round closed. Arming them immediately would buy the same shelf back at
        # the price it was just sold at.
        history = self._candles.get(campaign.campaign_id, [])
        campaign.reuse_below = min((c.low for c in history), default=exit_price)

        # The principal is back in the pool, so the rungs still waiting get a
        # bigger share of it.
        replan_ladder(campaign)

        self._log_event(
            campaign,
            "round",
            f"Round {rnd.round_id} closed at TP {exit_price:,.2f} — sold {qty:.8f} "
            f"(avg entry {avg:,.2f}), PnL ${rnd.pnl:,.2f}. ${invested:,.2f} principal "
            f"returned to the pool; campaign continues until the mother high breaks.",
        )

    def _mother_retested(self, campaign: Campaign, candle: Candle) -> None:
        """
        Price rose back to within MOTHER_RETEST_PCT of the mother high without
        breaking it. The old mother candle is spent — any line drawn from it to
        here would run almost horizontal — so this candle takes over as the
        mother and the cascade restarts on it.
        """
        gap = campaign.mother_high - candle.high
        campaign.state = "COMPLETED"
        campaign.close_reason = "mother_retested"
        campaign.closed_at = _ist_now_str()
        self._log_event(
            campaign,
            "warn",
            f"Rise to {candle.high:,.2f} came within {gap:,.2f} "
            f"({gap / campaign.mother_high * 100:.3f}%) of the mother high "
            f"{campaign.mother_high:,.2f} — too flat to draw a trendline. "
            f"Restarting on this candle.",
        )
        if campaign.mode == "live":
            self._schedule(self._cancel_all_live_orders(campaign, include_tp=False))
        elif campaign.filled_base_qty > 0:
            tp = compute_tp_price(campaign)
            if tp and candle.high >= tp:
                self._close_round(campaign, tp)
        self._archive_campaign(campaign)
        self._auto_restart(campaign, candle)

    def _mother_broken(self, campaign: Campaign, candle: Optional[Candle] = None) -> None:
        """Freeze at the first break, then restart after two more 5m closes.

        The successor's mother is the HIGHEST of the three candles in that
        15-minute window, taken whole — its own high and its own low. The
        breaking candle is only the first of the three, and a break usually
        keeps running: anchoring on it left the new mother below price that had
        already printed, so the successor started measuring its fall from a high
        the market had beaten minutes earlier. Still a real candle, never a
        synthetic 15m aggregate.
        """
        if candle is None:
            return
        snapshot = {
            "timestamp": candle.timestamp,
            "open": candle.open,
            "high": candle.high,
            "low": candle.low,
            "close": candle.close,
            "timeframe": candle.timeframe,
        }
        campaign.mother_broken_above = True
        campaign.state = MOTHER_BREAK_PENDING
        campaign.mother_break_candle = snapshot
        campaign.mother_break_top_candle = dict(snapshot)
        campaign.mother_break_wait_remaining = 2
        campaign.mother_break_last_5m_ts = candle.timestamp
        self._log_event(
            campaign,
            "warn",
            f"Mother candle high {campaign.mother_high:g} broken above — campaign frozen. "
            f"Two further 5m candles close before the restart; the highest of the three becomes the next "
            f"mother. Leading so far: high {candle.high:,.2f} / low {candle.low:,.2f}. "
            + (
                "Resting TP order left on the exchange to capture the exit."
                if campaign.mode == "live" and campaign.filled_base_qty > 0
                else ""
            ),
        )
        if campaign.mode == "live":
            self._schedule(self._cancel_all_live_orders(campaign, include_tp=False))

    def _advance_mother_break_confirmation(self, campaign: Campaign, candle: Candle) -> None:
        """Use a closed post-break 5m candle to advance the frozen reset."""
        source = campaign.mother_break_candle or {}
        source_ts = int(source.get("timestamp") or 0)
        if campaign.state != MOTHER_BREAK_PENDING or candle.timestamp <= source_ts:
            return
        # Every candle in the window is a candidate for the next mother, so the
        # comparison happens on the way through — by the time the countdown
        # reaches zero these candles are gone from the step and cannot be
        # revisited.
        top = campaign.mother_break_top_candle or dict(source)
        if candle.high > _coerce_float(top.get("high")):
            campaign.mother_break_top_candle = {
                "timestamp": candle.timestamp,
                "open": candle.open,
                "high": candle.high,
                "low": candle.low,
                "close": candle.close,
                "timeframe": candle.timeframe,
            }
        campaign.mother_break_wait_remaining = max(int(campaign.mother_break_wait_remaining) - 1, 0)
        leader = campaign.mother_break_top_candle or {}
        if campaign.mother_break_wait_remaining:
            self._log_event(
                campaign,
                "wait",
                f"Mother-break freeze confirmed by one closed 5m candle — one more closes before the restart. "
                f"Highest so far: {_coerce_float(leader.get('high')):,.2f} / "
                f"{_coerce_float(leader.get('low')):,.2f}.",
            )
            return
        self._finish_mother_break(campaign)

    def _finish_mother_break(self, campaign: Campaign) -> None:
        """Archive the frozen parent and start from the window's highest candle."""
        source = campaign.mother_break_top_candle or campaign.mother_break_candle or {}
        if not source:
            return
        break_candle = Candle(
            timestamp=int(source.get("timestamp") or 0),
            open=_coerce_float(source.get("open")),
            high=_coerce_float(source.get("high")),
            low=_coerce_float(source.get("low")),
            close=_coerce_float(source.get("close")),
            timeframe=str(source.get("timeframe") or BASE_TIMEFRAME),
        )
        if break_candle.timestamp <= 0 or break_candle.high <= break_candle.low:
            return
        campaign.state = "MOTHER_BROKEN"
        campaign.close_reason = "mother_broken"
        campaign.closed_at = _ist_now_str()
        campaign.mother_break_wait_remaining = 0
        self._log_event(
            campaign,
            "warn",
            f"Mother-break confirmation complete — parent ended. The successor's mother is the highest of the "
            f"three 5m candles in the break window: high {break_candle.high:,.2f} / low {break_candle.low:,.2f} "
            f"at {break_candle.timestamp}.",
        )
        if campaign.mode == "paper" and campaign.filled_base_qty > 0:
            # Paper has no resting exchange TP.  At a mother break the target
            # has necessarily been reached, so close it as the live TP would.
            tp = compute_tp_price(campaign)
            if tp:
                self._close_round(campaign, tp)
        self._archive_campaign(campaign)
        self._auto_restart(campaign, break_candle)

    def _schedule(self, coro) -> None:
        """
        Fire a coroutine from the synchronous state machine. That machine also
        runs during restore and replay, where no event loop exists — a bare
        ensure_future raises RuntimeError there and would abort the caller
        half-way through handling a mother break, leaving orders untouched and
        the successor campaign unstarted.
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            coro.close()
            _log.warning("[CASCADE] no running event loop; skipped a scheduled order task")
            return
        # Keep a strong reference until it finishes. The event loop only holds a
        # weak one, so a task nothing else refers to can be garbage-collected
        # part-way through. This is the path that cancels resting live orders on
        # a mother break — losing it leaves real buy stops working for a
        # campaign that has ended, which is the kind of failure that shows up as
        # an unexplained fill days later.
        task = asyncio.ensure_future(coro)
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

    def funded_bands_for(self, symbol: str, exclude_id: str = "") -> List[Band]:
        """Every stretch of price on this symbol that running capital already covers.

        The ledger, assembled on demand from the campaigns themselves rather
        than kept as a separate structure that could drift out of step with
        them. Each running campaign contributes the ground it has actually
        funded — see Campaign.claimed_bands.

        Only RUNNING campaigns count, which IS the release rule: a campaign that
        completes, breaks its mother or is stopped drops out of this sum, and
        its ground is free for the next one. Release happens on campaign end,
        not on a round closing — a campaign between rounds still holds its
        fibs, and will buy that ground again on the next leg down.
        """
        bands: List[Band] = []
        for campaign in self.campaigns.values():
            if campaign.symbol != symbol or campaign.campaign_id == exclude_id:
                continue
            if campaign.state not in RUNNING_STATES:
                continue
            bands.extend(campaign.claimed_bands)
        return merge_bands(bands)

    def _birth_bands_for(self, symbol: str, mother_high: float) -> tuple:
        """The ledger clipped to what a new campaign starting at `mother_high` sees.

        Returns (bands, owners). Ground at or above the new mother high is
        dropped — the campaign will never fall through it, so carrying it would
        only clutter the record it keeps for life.
        """
        taken = [(low, min(high, mother_high)) for low, high in self.funded_bands_for(symbol) if low < mother_high]
        bands = merge_bands(taken)
        owners = [
            campaign
            for campaign in self.campaigns.values()
            if campaign.symbol == symbol
            and campaign.state in RUNNING_STATES
            and any(low < mother_high for low, _high in campaign.claimed_bands)
        ]
        return bands, owners

    def _ancestor_ids(self, campaign: Campaign) -> set:
        """Every campaign this one descends from, walking parent links upward."""
        seen = set()
        current = campaign.parent_campaign_id
        while current and current not in seen:
            seen.add(current)
            parent = self.campaigns.get(current)
            current = parent.parent_campaign_id if parent else None
        return seen

    def _minor_yields_to_major(self, parent: Campaign) -> bool:
        """When a major and a minor break together, only the major restarts.

        A minor mother is a sub-structure marked *inside* the move the major is
        already trading, so the same push upward breaks both. Restarting both
        left two campaigns on one symbol re-anchored within minutes of each
        other, chasing the same high with two lots of capital — and the minor's
        successor is the redundant one, because the major's anchor is the
        structure that actually defines the move.

        Only a SIMULTANEOUS break counts, and simultaneity is judged on the two
        break candles' timestamps. Two things made that necessary. A minor whose
        mother breaks while the major is running quietly is an independent event
        and must restart normally. And a successor inherits its parent's kind,
        so matching on "some major once broke" would have let a long-archived
        ancestor block its own descendants forever.
        """
        if parent.mc_kind != "minor":
            return False
        own_break = int((parent.mother_break_candle or {}).get("timestamp") or 0)
        if not own_break:
            return False
        ancestors = self._ancestor_ids(parent)
        major = None
        for c in self.campaigns.values():
            if c.campaign_id == parent.campaign_id or c.campaign_id in ancestors:
                continue
            if c.symbol != parent.symbol or c.mc_kind != "major":
                continue
            if not (c.state == MOTHER_BREAK_PENDING or c.close_reason == "mother_broken"):
                continue
            their_break = int((c.mother_break_candle or {}).get("timestamp") or 0)
            if their_break and abs(their_break - own_break) <= _SIMULTANEOUS_BREAK_SEC:
                major = c
                break
        if major is None:
            return False
        self._log_event(
            parent,
            "warn",
            f"Minor MC broke at the same time as the major (#{major.seq}) on {parent.symbol}. "
            f"No successor started for the minor — the major re-anchors and carries the move, "
            f"so only one campaign runs on from here. Its capital returns to the group.",
        )
        self._alert(
            "Cascade minor MC retired at the break",
            f"{parent.symbol} #{parent.seq} (MINOR MC, {parent.mode.upper()}) broke together with "
            f"major campaign #{major.seq}.\n\n"
            f"Only the major restarts. The minor ends here and its ${parent.capital_usd:,.2f} goes "
            f"back to the {parent.symbol} group.",
            level="warn",
        )
        return True

    def _auto_restart(self, parent: Campaign, candle: Candle) -> Optional[Campaign]:
        """
        A break does not end the cascade, it moves it. The candle that broke
        above becomes the new mother candle — its own high and low — and a fresh
        campaign starts there with nothing carried over: no trendlines, no fibs,
        no orders, no fills. Everything is rebuilt from the new mother candle
        under the same rules.

        Manual start is untouched; this only covers the break case, so a
        campaign stopped or deleted on purpose stays stopped.
        """
        if parent.close_reason not in RESTART_REASONS:
            return None
        # A straight rip upward breaks a mother candle every bar. Chains that
        # never manage to draw a fib are cut off rather than multiplying forever.
        barren = 0 if parent.legs else parent.barren_chain + 1
        if barren > MAX_BARREN_AUTO_RESTARTS:
            self._log_event(
                parent,
                "warn",
                f"{barren - 1} auto-restarts in a row drew no fib — chain stopped. "
                f"Start a new campaign by hand when the move settles.",
            )
            return None
        if candle.high <= candle.low:
            return None
        if self._active_duplicate(parent.symbol, candle.timestamp, candle.high) is not None:
            return None  # this candle already anchors a running campaign
        if self._minor_yields_to_major(parent):
            return None

        # The child wants the parent's capital, but never more than the capital
        # group has left. The parent is already archived (its state is final),
        # so its own capital has flowed back and normally covers the child in
        # full — the clamp only bites when siblings grabbed budget in between.
        child_capital = parent.capital_usd
        group_budget = _coerce_float(self.capital_groups.get(parent.symbol))
        if GROUP_CAP_ENFORCED and group_budget > 0:
            available = group_budget - self.group_committed_usd(parent.symbol)
            child_capital = min(child_capital, available)
            if child_capital < parent.min_notional_usd * 2:
                self._log_event(
                    parent,
                    "warn",
                    f"Mother break would auto-restart, but the {parent.symbol} capital group has "
                    f"only ${max(available, 0):,.2f} left of ${group_budget:g}. No restart — stop "
                    f"a sibling campaign or raise the budget, then start one by hand.",
                )
                self._alert(
                    "Cascade restart blocked — capital group exhausted",
                    f"{parent.symbol} #{parent.seq} ended on a mother break, but the capital group "
                    f"has ${max(available, 0):,.2f} left of ${group_budget:g}, so no successor "
                    f"was started.",
                    level="warn",
                )
                return None

        child = Campaign(
            campaign_id=uuid.uuid4().hex[:10],
            seq=self._next_seq(),
            symbol=parent.symbol,
            capital_usd=child_capital,
            mother_high=candle.high,
            mother_low=candle.low,
            mother_timestamp=candle.timestamp,
            mode=parent.mode,
            # ALWAYS 5m, whatever the parent was running. Phil's rule: when the
            # major mother candle breaks above, the whole campaign stops and
            # restarts from that high on 5m. A 1D campaign does not spawn
            # another 1D campaign — the break is a fresh start at a new high.
            timeframe=BASE_TIMEFRAME,
            start_timeframe=BASE_TIMEFRAME,
            escalates=True,
            # The KIND is inherited, not derived from that 5m. Restarting on 5m
            # used to mean "minor", so every successor was labelled a minor —
            # and since a break or a retest restarts a campaign the moment price
            # climbs back (which is exactly what a closed round leaves behind),
            # every campaign on the page drifted to MINOR MC within a round or
            # two, alone on its symbol with no major anywhere near it. The kind
            # is a statement about structure: a major's successor re-anchors the
            # same move and carries it on, so it is still the major. Only a
            # genuine sub-mother marked inside someone else's move is a minor,
            # and that only ever comes from Phil starting one by hand.
            mc_kind=parent.mc_kind,
            # A successor is a newly-born MC like any other, so it takes the
            # band ledger as it stands now. The parent is already archived and
            # has released its ground; only still-running siblings show up.
            funded_bands=(self._birth_bands_for(parent.symbol, candle.high)[0] if CROSS_CAMPAIGN_NETTING else []),
            min_notional_usd=parent.min_notional_usd,
            # Same instrument, so the parent's measurement carries over rather
            # than a fresh fetch on the mother-break path.
            min_fib_range_pct=parent.min_fib_range_pct,
            median_bar_pct=parent.median_bar_pct,
            tick_size=parent.tick_size,
            parent_campaign_id=parent.campaign_id,
            generation=parent.generation + 1,
            barren_chain=barren,
            model_version=MODEL_VERSION,
            created_at=_ist_now_str(),
            last_processed_ts=candle.timestamp,
            mother_watch_last_5m_ts=candle.timestamp,
            window_start_ts=candle.timestamp,
        )
        self.campaigns[child.campaign_id] = child
        # The breaking candle is the mother, so history starts clean from it —
        # but only when the candle itself is 5m.  An escalated parent can now
        # detect its break through the dedicated 5m watcher, in which case this
        # is correctly seeded even though the parent happened to be stepping
        # 15m/1H.  A daily breaking candle still must not masquerade as 5m.
        seed_from_break = parent.timeframe == BASE_TIMEFRAME or (
            parent.escalates and parent.start_timeframe == BASE_TIMEFRAME and candle.timeframe == BASE_TIMEFRAME
        )
        self._candles[child.campaign_id] = [candle] if seed_from_break else []
        self._log_event(
            child,
            "start",
            f"Auto-started from the break of campaign #{parent.seq} — new mother candle "
            f"high {candle.high:,.2f} / low {candle.low:,.2f} ({child.mode.upper()}, "
            f"generation {child.generation}), restarting on {BASE_TIMEFRAME}"
            + (f" from a {parent.timeframe} break" if parent.timeframe != BASE_TIMEFRAME else "")
            + ". Nothing carried over.",
        )
        why = "broke above" if parent.close_reason == "mother_broken" else "was retested from below"
        self._alert(
            "Cascade auto-restarted",
            f"{child.symbol} — campaign #{parent.seq}'s mother candle {why}.\n\n"
            f"New campaign #{child.seq} ({child.mode.upper()}, generation {child.generation})\n"
            f"New mother candle: high {candle.high:,.2f} / low {candle.low:,.2f}\n"
            f"Capital: ${child.capital_usd:,.2f}\n\n"
            f"Nothing was carried over — it starts from scratch.",
            level="warn" if child.mode == "live" else "info",
        )
        return child

    def _archive_campaign(self, campaign: Campaign) -> None:
        payload = campaign.to_dict()
        # Replace, never duplicate. A campaign can be archived more than once —
        # stopped first, then sold at market later — and the second archive is
        # the truthful one. Appending blindly put the same campaign in the closed
        # table twice, the stale copy still claiming an open position.
        existing = next(
            (i for i, row in enumerate(self.closed_campaigns) if row.get("campaign_id") == campaign.campaign_id),
            None,
        )
        if existing is not None:
            self.closed_campaigns[existing] = payload
        else:
            self.closed_campaigns.append(payload)
        if len(self.closed_campaigns) > CLOSED_HISTORY_LIMIT:
            self.closed_campaigns = self.closed_campaigns[-CLOSED_HISTORY_LIMIT:]
        if self.on_campaign_closed:
            try:
                self.on_campaign_closed(payload)
            except Exception as exc:
                _log.warning("[CASCADE] on_campaign_closed callback failed: %s", exc)

    # ── live order sync ──────────────────────────────────────────

    async def _open_orders_by_id(self, campaign: Campaign) -> Dict[str, dict]:
        rows = await asyncio.to_thread(self.broker.get_orders, campaign.symbol, "open")
        result = {}
        for row in rows or []:
            if isinstance(row, dict) and row.get("orderId") is not None:
                result[str(row["orderId"])] = row
        return result

    async def _free_base_balance(self, campaign: Campaign) -> Optional[float]:
        """How much of the base asset is actually free to sell, per the exchange.

        A spot BUY pays its commission out of the coin received (unless fees are
        settled in BNB), so the account ends up holding slightly LESS than
        usd/price — the gross quantity the books recorded. Trying to sell the
        recorded amount is rejected -2010 "insufficient balance". The exchange
        balance is the only authority on what can really be sold, so the TP is
        capped to it. Returns None if the balance could not be read, in which
        case the caller falls back to the recorded quantity and retries.
        """
        try:
            product = await asyncio.to_thread(self.broker.get_product_by_symbol, campaign.symbol)
            wallet = await asyncio.to_thread(self.broker.get_wallet)
        except Exception as exc:
            _log.warning("[CASCADE] free-balance lookup failed for %s: %s", campaign.symbol, exc)
            return None
        base = str((product or {}).get("base_asset") or "").upper()
        if not base or not isinstance(wallet, list):
            return None
        for row in wallet:
            if str(row.get("asset_symbol") or "").upper() == base:
                return _coerce_float(row.get("free_balance"), 0.0)
        return 0.0

    async def _owned_base_balance(self, campaign: Campaign) -> Optional[float]:
        """How much of the base asset the account HOLDS — free plus locked.

        Different question from _free_base_balance, and the distinction matters.
        `free` answers "what can I sell this second", which is the right basis
        for sizing a sell. It is the wrong basis for "does this coin still
        exist", because a resting sell order LOCKS the coin it offers: the
        balance reads 0 free while the coin is sitting right there. Asking the
        free balance whether a position still exists says "gone" about every
        position that has a take-profit resting against it.
        """
        try:
            product = await asyncio.to_thread(self.broker.get_product_by_symbol, campaign.symbol)
            wallet = await asyncio.to_thread(self.broker.get_wallet)
        except Exception as exc:
            _log.warning("[CASCADE] owned-balance lookup failed for %s: %s", campaign.symbol, exc)
            return None
        base = str((product or {}).get("base_asset") or "").upper()
        if not base or not isinstance(wallet, list):
            return None
        for row in wallet:
            if str(row.get("asset_symbol") or "").upper() == base:
                return _coerce_float(row.get("free_balance"), 0.0) + _coerce_float(row.get("locked_balance"), 0.0)
        return 0.0

    async def _sync_live_orders(self, campaign: Campaign) -> bool:
        """
        Desired-state reconciliation for a live campaign:
        1. ingest fills/cancellations of tracked orders no longer open;
        2. place PENDING entry orders (idempotent via client order ids);
        3. keep a single TP limit sell resting at the current TP price.
        """
        if campaign.mode != "live":
            return False
        try:
            open_orders = await self._open_orders_by_id(campaign)
        except Exception as exc:
            _log.warning("[CASCADE] open-orders fetch failed for %s: %s", campaign.symbol, exc)
            return False
        changed = False

        # 1) Ingest the accumulated buy stop if the exchange has moved it on.
        if campaign.pending_order_id:
            row = open_orders.get(str(campaign.pending_order_id))
            if row is not None:
                # Still resting, but a stop-limit can fill in pieces. Take what
                # has executed and leave the rest working.
                executed = _coerce_float(row.get("executedQty"))
                quote = _coerce_float(row.get("cummulativeQuoteQty"))
                if executed > campaign.pending_filled_qty + 1e-12:
                    delta_qty = executed - campaign.pending_filled_qty
                    price = (
                        quote / executed
                        if executed > 0 and quote > 0
                        else (campaign.pending_limit_price or campaign.pending_stop_price or 0.0)
                    )
                    campaign.pending_filled_qty = executed
                    self._fill_pending_part(campaign, price, delta_qty, int(time.time()))
                    changed = True
            if row is None:
                status_row = await self._safe_get_order(campaign, campaign.pending_order_id)
                status = str(status_row.get("status") or "").upper()
                if status == "FILLED":
                    executed = _coerce_float(status_row.get("executedQty"))
                    quote = _coerce_float(status_row.get("cummulativeQuoteQty"))
                    price = (
                        quote / executed
                        if executed > 0 and quote > 0
                        else (campaign.pending_limit_price or campaign.pending_stop_price or 0.0)
                    )
                    self._fill_pending(campaign, price, int(time.time()), campaign.pending_order_id)
                    changed = True
                elif status in {"CANCELED", "EXPIRED", "REJECTED"}:
                    # Say WHICH. The three mean very different things — EXPIRED
                    # is a stop that triggered and could not fill inside its
                    # limit, CANCELED is something pulling it, REJECTED is the
                    # order never being accepted — and the old message collapsed
                    # all three into "cancelled", which made a re-place loop
                    # impossible to diagnose from the log alone.
                    self._log_event(
                        campaign,
                        "warn",
                        f"The buy stop came back {status} from the exchange "
                        f"(trigger {campaign.pending_stop_price}, limit {campaign.pending_limit_price}); re-placing",
                    )
                    # Only for real money, and deduped: a stop walking down a
                    # fall legitimately expires and re-places, and alerting on
                    # every one of those would train the eye to ignore the case
                    # that matters — something outside CryptoForge pulling our
                    # orders.
                    if campaign.mode == "live":
                        meaning = {
                            "EXPIRED": "the stop triggered but could not fill inside its limit price",
                            "CANCELED": "something cancelled it — the exchange, another client, or by hand",
                            "REJECTED": "the exchange never accepted it",
                        }.get(status, "the exchange returned it unfilled")
                        self._alert(
                            "Cascade entry CANCELLED",
                            f"{campaign.symbol} #{campaign.seq} (LIVE) — {campaign.mc_kind.upper()} MC\n"
                            f"The ${campaign.pending_usd:,.2f} buy stop came back {status}: {meaning}.\n"
                            f"Trigger {campaign.pending_stop_price} / limit {campaign.pending_limit_price}\n\n"
                            f"The money stays collected and a fresh order goes out on the next sync.",
                            level="warn",
                            dedupe_sec=900,
                            dedupe_key=f"entry-cancelled:{campaign.campaign_id}",
                        )
                    campaign.pending_order_id = None
                    changed = True

        # 2) One accumulated buy stop, repriced as the fall walks it down.
        if campaign.state in ACTIVE_STATES:
            known_ids = set()
            if campaign.pending_order_id:
                known_ids.add(str(campaign.pending_order_id))
            if campaign.tp_order_id:
                known_ids.add(str(campaign.tp_order_id))
            for order_id, row in open_orders.items():
                client_id = str(row.get("clientOrderId") or "")
                if client_id.startswith(f"cf-csc-{campaign.campaign_id}-") and order_id not in known_ids:
                    # TP ownership/recovery is deliberately handled by
                    # _sync_tp_order below.  Cancelling an untracked TP here
                    # first left a stale open-orders snapshot for that method
                    # to "adopt", producing the false Adopted/CANCELED loop
                    # seen after a service handover.  Entry cleanup must never
                    # take down a protective sell.
                    if "-tp-" in client_id:
                        continue
                    await self._safe_cancel(campaign, order_id)
                    changed = True
            # Isolated on purpose. This step raised for days and the tick's
            # try/except swallowed it, which ALSO meant step 3 below never ran
            # — so a live campaign could neither buy nor place a take-profit.
            # A failure to arm the entry must never take the exit down with it.
            try:
                if await self._place_pending_stop(campaign):
                    changed = True
            except Exception as exc:
                _log.warning("[CASCADE] buy stop placement failed for %s: %s", campaign.campaign_id, exc)
                self._alert(
                    "Cascade entry not placed",
                    f"{campaign.symbol} campaign #{campaign.seq} (LIVE)\n"
                    f"${campaign.pending_usd:,.2f} collected but the buy stop could not be placed.\n"
                    f"{type(exc).__name__}: {exc}",
                    level="error",
                    dedupe_sec=900,
                    dedupe_key=f"entry-not-placed:{campaign.campaign_id}",
                )

        # 3) TP management.
        changed |= await self._sync_tp_order(campaign, open_orders)
        return changed

    @staticmethod
    def _order_id_from(result: dict) -> str:
        """The exchange id out of a placement reply, or "" if it carried none.

        Storing str(...or "") straight onto the campaign was a live-money bug:
        an empty string is falsy, so the next sync believed nothing was resting,
        skipped the cancel, and placed ANOTHER order — while still logging
        "placed" each time. For a TP that means several sell orders for one
        position, all able to fill.
        """
        if not isinstance(result, dict):
            return ""
        return str(result.get("orderId") or result.get("id") or "")

    async def _adopt_order_by_client_id(self, campaign: Campaign, client_id: str) -> Optional[str]:
        """Find a resting order by the client id we gave it, and return its
        exchange id. Client ids are ours and unique, so a match is definitive."""
        try:
            open_orders = await self._open_orders_by_id(campaign)
        except Exception as exc:
            _log.warning("[CASCADE] could not list orders to adopt %s: %s", client_id, exc)
            return None
        for order_id, row in open_orders.items():
            if str(row.get("clientOrderId") or "") == client_id:
                return str(order_id)
        return None

    async def _place_pending_stop(self, campaign: Campaign) -> bool:
        """Rest the accumulated buy on the exchange. One order, whatever the
        fall has collected so far, at the trigger the last red set."""
        if campaign.pending_order_id or campaign.pending_usd <= 0:
            return False
        stop = campaign.pending_stop_price
        limit = campaign.pending_limit_price
        if not stop or not limit:
            return False  # not armed: two reds have not printed below the line yet
        # A buy stop has to sit ABOVE the market, or Binance rejects it -2010
        # "would trigger immediately". Read a FRESH price (not the 4s cache): on
        # a thin book like PAXG the price crosses the trigger inside those 4
        # seconds. One ticker call is cheap — placement is gated on arming.
        market = await self._get_price(campaign.symbol, max_age=1.0)
        # If price has ALREADY reached the trigger, a stop placed at the trigger
        # would be rejected -2010. The RIGHT answer is NOT a limit buy: a limit
        # fills on the way DOWN, so when the fall continues it buys into the
        # knife — which is the one thing the buy stop exists to prevent. (PAXG,
        # 2026-07-23: a stale trigger below a falling price took a limit that
        # filled at 4,108.69 while price carried on down to 4,103.61.)
        #
        # Instead RAISE the stop to just above the current price and keep it a
        # STOP. It then fills only if price continues UP through it — a real
        # turn — and if price falls it simply rests above and waits. The entry
        # is necessarily a touch higher than the original trigger, which is the
        # cost of price having already passed it; the invariant that matters is
        # preserved: the entry never fills on a downward move.
        tick = _coerce_float(campaign.tick_size, DEFAULT_TICK_SIZE) or DEFAULT_TICK_SIZE
        eff_stop, eff_limit = stop, limit
        raised = bool(market and market >= stop)
        if raised:
            # How far above the trigger is the market? A legitimate raise is a
            # live cross — price ticked just over a freshly-set trigger within
            # the price-cache window. A large gap means the fall this pot
            # collected already bottomed and bounced (a late or left-anchored
            # start replays an old fall in seconds), so arming just above the
            # current price would buy over value on NO new low. Hold instead:
            # keep the pot collected and let the walk-down re-arm it only when a
            # genuine new low prints below the trigger.
            raise_pct = (market - stop) / stop if stop > 0 else 0.0
            if raise_pct > MAX_STOP_RAISE_PCT:
                if campaign.campaign_id not in self._stale_pot_held:
                    self._stale_pot_held.add(campaign.campaign_id)
                    self._log_event(
                        campaign,
                        "order",
                        f"Buy HELD, not armed: price {market:,.2f} is {raise_pct * 100:.2f}% above the "
                        f"trigger {stop:,.2f}. The collected fall already bottomed and bounced, so arming "
                        f"here would buy over value with no new low. ${campaign.pending_usd:,.2f} stays "
                        f"collected — it will arm only when price makes a fresh low below {stop:,.2f}.",
                    )
                    self._alert(
                        "Cascade entry held — no new low",
                        f"{campaign.symbol} #{campaign.seq} ({campaign.mode.upper()})\n"
                        f"Price {market:,.2f} is {raise_pct * 100:.2f}% above the collected fall's trigger "
                        f"{stop:,.2f}.\n\nHolding ${campaign.pending_usd:,.2f} until price makes a new low — "
                        f"it will not buy over value. This is normal when a campaign is started late or from "
                        f"an older mother candle.",
                        level="warn",
                        dedupe_sec=1800,
                        dedupe_key=f"entry-held:{campaign.campaign_id}",
                    )
                return False
            gap = limit - stop
            eff_stop = round(market + tick, 8)
            eff_limit = round(eff_stop + (gap if gap > 0 else STOP_LIMIT_OFFSET_TICKS * tick), 8)
        # Churn brake. On 2026-07-22 a live campaign placed the same trigger,
        # had it come back cancelled, and re-placed — every 10-15s, indefinitely.
        # Whatever the underlying cause, an entry that will not stay resting must
        # not be retried forever: it burns rate limit and hides the real fault.
        # Keyed in memory only, and cleared whenever the trigger actually moves,
        # so a stop walking down a real fall is never throttled.
        attempts = self._place_attempts.get(campaign.campaign_id)
        if attempts and attempts[0] == (stop, limit):
            if attempts[1] >= _MAX_SAME_TRIGGER_PLACEMENTS:
                if attempts[1] == _MAX_SAME_TRIGGER_PLACEMENTS:
                    self._place_attempts[campaign.campaign_id] = (attempts[0], attempts[1] + 1)
                    self._log_event(
                        campaign,
                        "error",
                        f"Buy stop at {stop:,.2f} would not stay on the exchange after "
                        f"{_MAX_SAME_TRIGGER_PLACEMENTS} attempts — holding off until the trigger moves. "
                        f"${campaign.pending_usd:,.2f} stays collected and unarmed.",
                    )
                    self._alert(
                        "Cascade entry keeps being cancelled",
                        f"{campaign.symbol} campaign #{campaign.seq} (LIVE)\n"
                        f"${campaign.pending_usd:,.2f} buy stop at {stop:,.2f} will not rest.\n"
                        f"Stopped retrying to protect the rate limit.",
                        level="error",
                        dedupe_sec=1800,
                        dedupe_key=f"entry-churn:{campaign.campaign_id}",
                    )
                return False
            self._place_attempts[campaign.campaign_id] = (attempts[0], attempts[1] + 1)
        else:
            self._place_attempts[campaign.campaign_id] = ((stop, limit), 1)
        client_id = f"cf-csc-{campaign.campaign_id}-buy-{campaign.pending_rev}"
        try:
            # ALWAYS a stop — never a limit. A stop fills only on an upward
            # cross, so it can never buy into a fall. When raised, eff_stop sits
            # just above the current price; otherwise it is the original trigger.
            result = await asyncio.to_thread(
                lambda: self.broker.place_order(
                    campaign.symbol,
                    campaign.pending_usd,
                    "buy",
                    order_type="stop_limit",
                    limit_price=eff_limit,
                    stop_price=eff_stop,
                    client_order_id=client_id,
                )
            )
        except Exception as exc:
            result = {"error": str(exc)}
        if isinstance(result, dict) and not result.get("error"):
            order_id = self._order_id_from(result) or (await self._adopt_order_by_client_id(campaign, client_id) or "")
            if not order_id:
                self._log_event(
                    campaign,
                    "error",
                    "Buy was accepted but carried no order id and could not be found by client id; "
                    "not recording it, so the next sync will reconcile rather than place a second one.",
                )
                return False
            campaign.pending_order_id = order_id
            self._stale_pot_held.discard(campaign.campaign_id)
            if raised:
                self._log_event(
                    campaign,
                    "order",
                    f"Price {market:,.2f} had already passed the trigger {stop:,.2f} — re-armed the buy stop "
                    f"just above the market at {eff_stop:,.2f} / limit {eff_limit:,.2f} (${campaign.pending_usd:,.2f}) "
                    f"so it fills only on a continuation up, never on the fall.",
                )
            else:
                self._log_event(
                    campaign,
                    "order",
                    f"Buy stop placed: ${campaign.pending_usd:,.2f}, trigger {stop:,.2f} / limit {limit:,.2f}",
                )
            return True
        error = (result or {}).get("error") if isinstance(result, dict) else "unknown error"
        # -2010 "would trigger immediately" is not a failure to diagnose — it is
        # the deterministic answer that the market is at or above the trigger
        # "Duplicate order sent" means this exact client id already rests on the
        # exchange — the order we wanted IS there, we simply lost track of its
        # id (a restart, or a placement whose reply never arrived). Adopting it
        # is the correct resolution. Treating it as a failure left the id unset,
        # so the next sync tried the same client id and was refused again, for
        # as long as the campaign ran.
        #
        # Checked BEFORE the -2010 case: a duplicate must always be adopted, and
        # some clients return it carrying a -2010 code, so a phrase match on
        # "duplicate" has to win over the code match.
        if "duplicate" in str(error).lower():
            adopted = await self._adopt_order_by_client_id(campaign, client_id)
            if adopted:
                campaign.pending_order_id = adopted
                self._log_event(
                    campaign,
                    "order",
                    f"Buy stop for ${campaign.pending_usd:,.2f} was already resting on the exchange "
                    f"({client_id}); adopted it instead of placing a second one.",
                )
                return True
        # -2010 "would trigger immediately" is the same condition the fresh-price
        # guard above catches, lost to the race between our fetch and Binance's.
        # Treat it as a benign wait: roll back the churn attempt so it is not
        # counted toward the "won't rest" brake, and stay silent — no red error,
        # no alert. It rests on its own the next sync where price is below the
        # trigger.
        if _is_trigger_immediately_error(error):
            self._place_attempts.pop(campaign.campaign_id, None)
            return False
        self._log_event(campaign, "error", f"Failed to place the buy stop: {error}")
        self._alert(
            "Cascade order FAILED",
            f"{campaign.symbol} campaign #{campaign.seq} (LIVE)\n"
            f"${campaign.pending_usd:,.2f} buy stop at {stop:,.2f}\n"
            f"Binance said: {error}\n\n"
            f"The collected money is unarmed until this succeeds.",
            level="error",
            dedupe_sec=300,
            dedupe_key=f"order-failed:{campaign.campaign_id}",
        )
        return False

    async def _sync_tp_order(self, campaign: Campaign, open_orders: Dict[str, dict]) -> bool:
        changed = False
        if campaign.tp_order_id and str(campaign.tp_order_id) not in open_orders:
            status_row = await self._safe_get_order(campaign, campaign.tp_order_id)
            status = str(status_row.get("status") or "").upper()
            if status != "FILLED":
                # Dropping the id silently made a re-place loop unreadable: the
                # log showed "TP limit sell placed" over and over with nothing
                # explaining why the last one stopped counting.
                self._log_event(
                    campaign,
                    "warn",
                    f"TP order {campaign.tp_order_id} is no longer open (status {status or 'unknown'}); "
                    f"will place a fresh one.",
                )
            if status == "FILLED":
                offered = campaign.filled_base_qty + campaign.residual_base_qty
                executed = _coerce_float(status_row.get("executedQty"), offered)
                quote = _coerce_float(status_row.get("cummulativeQuoteQty"))
                exit_price = (
                    quote / executed
                    if executed > 0 and quote > 0
                    else (campaign.tp_order_price or campaign.tp_price or 0.0)
                )
                # Whatever LOT_SIZE would not let us offer stays ours; carry it
                # so the next round's sell clears it once the total reaches
                # another whole step, rather than stranding it forever.
                campaign.residual_base_qty = max(round(offered - executed, 12), 0.0)
                # Entry buys that never filled stay resting — the campaign is
                # still live and price can come back down to them.
                self._close_round(campaign, exit_price, sold_qty=executed)
                return True
            campaign.tp_order_id = None
            campaign.tp_order_price = None
            changed = True

        if campaign.state not in ACTIVE_STATES or campaign.filled_base_qty <= 0:
            return changed
        desired_tp = compute_tp_price(campaign)
        if not desired_tp:
            return changed
        # The exchange is the authority on what is resting, not our id. If any
        # TP of ours is already open, adopt it rather than sending a second
        # sell — a lost id (restart, or a reply that carried none) otherwise
        # stacks one sell order per sync against a single position, and every
        # one of them can fill.
        if not campaign.tp_order_id:
            mine = f"cf-csc-{campaign.campaign_id}-tp-"
            existing_row = next(
                (
                    (oid, row)
                    for oid, row in open_orders.items()
                    if str(row.get("clientOrderId") or "").startswith(mine)
                ),
                None,
            )
            if existing_row:
                existing, row = existing_row
                campaign.tp_order_id = str(existing)
                # Read the ACTUAL resting price off the exchange row rather than
                # assuming it matches today's desired target — an adopted order
                # was very likely placed against an older average.
                campaign.tp_order_price = _coerce_float(row.get("price")) or None
                self._log_event(
                    campaign,
                    "order",
                    f"Adopted the TP already resting on the exchange ({existing}) instead of placing another.",
                )
                changed = True
                # Recovery's first responsibility is to establish ownership,
                # never to disturb a sell that may already protect the coin.
                # The next normal sync will have the adopted order's full
                # exchange row (including its actual price) and can replace it
                # only if its target is genuinely stale.
                return changed
        # Exactly ONE TP may rest against a position. Ownership is settled by
        # now (adoption returns above), so any other TP of ours on the book is a
        # duplicate — from a cancel that failed, or a placement whose reply was
        # lost and got retried. The entry sweep deliberately skips -tp- ids, so
        # this is the only place that can clear one, and leaving it would let
        # the same coin be sold twice.
        if campaign.tp_order_id:
            mine = f"cf-csc-{campaign.campaign_id}-tp-"
            for other_id, row in list(open_orders.items()):
                if str(other_id) == str(campaign.tp_order_id):
                    continue
                if not str(row.get("clientOrderId") or "").startswith(mine):
                    continue
                self._log_event(
                    campaign,
                    "warn",
                    f"A duplicate TP ({other_id}) was resting against the same position; cancelling it and "
                    f"keeping {campaign.tp_order_id}.",
                )
                if await self._safe_cancel(campaign, other_id):
                    changed = True
        # Compare against tp_order_price — the price ACTUALLY resting on the
        # exchange — never against tp_price. tp_price is updated by the fill
        # handlers the instant a fill moves the average, before this sync has
        # had a chance to replace the order; comparing to it would compare the
        # desired price to itself and always report "already correct".
        current_price_ok = campaign.tp_order_price and abs((campaign.tp_order_price or 0.0) - desired_tp) < 1e-9
        # Price alone is NOT enough to call a resting TP correct. A TP can sit at
        # exactly the right averaged price while covering only part of the
        # position — which is precisely what the locked-balance bug produced:
        # BTCUSDT #16 rested at 64,098.61 (the correct average) for just
        # 0.00014 of a 0.00034865 holding, leaving ~$13 with no exit. Gating the
        # replacement on price alone meant the sync returned early every tick
        # and never noticed. The quantity has to be checked too, so the desired
        # sell size is computed BEFORE deciding there is nothing to do.
        #
        # Sell what the exchange says is actually there, not the gross the books
        # recorded: the buy's commission came out of the coin, so the recorded
        # quantity over-asks by the fee and Binance rejects the whole sell -2010.
        # Capping at the free balance (the broker floors it to LOT_SIZE) fixes
        # that; it is a no-op when fees are paid in BNB and the balance is full.
        desired_qty = campaign.filled_base_qty + campaign.residual_base_qty
        free_qty = await self._free_base_balance(campaign)
        # Our own resting TP LOCKS the coin it offers, so the exchange reports
        # that quantity as unavailable — `free` excludes `locked`. We are about
        # to cancel that very order, so its unfilled quantity is genuinely ours
        # to re-offer and has to be added back before capping.
        #
        # Without this the replacement sell only covers coin that arrived AFTER
        # the old TP was placed. That is the live "TP sells only the last buy"
        # bug: buy #1's 0.00020818 BTC sat locked in the resting TP, `free`
        # reported just buy #2's 0.00014048, and min(desired, free) silently
        # shrank the sell to the newest fill alone — leaving 60% of the position
        # with no target at all. The averaged PRICE was right the whole time,
        # which is exactly why it read as "averaging only the last buy".
        #
        # Only OUR order's quantity is added back, never the wallet's whole
        # `locked` figure: another campaign on this symbol may have its own sell
        # resting, and that coin is not ours to offer.
        if free_qty is not None and campaign.tp_order_id:
            resting = open_orders.get(str(campaign.tp_order_id)) or {}
            reclaimable = _coerce_float(resting.get("origQty")) - _coerce_float(resting.get("executedQty"))
            free_qty += max(reclaimable, 0.0)
        sell_qty = desired_qty if free_qty is None else min(desired_qty, free_qty)
        if sell_qty <= 0:
            self._log_event(
                campaign,
                "error",
                f"TP not placed — the books show {desired_qty:.8f} to sell but the exchange free "
                f"balance is {(free_qty or 0.0):.8f}. Holding the position; will retry next sync.",
            )
            return changed
        # Binance validates the lot-rounded base quantity, not the book's
        # nominal quantity. A partial/legacy fill can leave a $4.57 holding;
        # submitting that TP every sync only produces a noisy rejection and
        # never protects or exits anything. Keep it as campaign inventory so a
        # later fill can combine with it, and retain an existing TP while this
        # smaller replacement is impossible.
        try:
            product = await asyncio.to_thread(self.broker.get_product_by_symbol, campaign.symbol)
        except Exception as exc:
            _log.warning("[CASCADE] TP product lookup failed for %s: %s", campaign.symbol, exc)
            product = {}
        product = product or {}
        minimum = max(
            _coerce_float(product.get("min_notional"), campaign.min_notional_usd),
            _coerce_float(campaign.min_notional_usd, MIN_NOTIONAL_FLOOR_USD),
            MIN_NOTIONAL_FLOOR_USD,
        )
        step = _coerce_float(product.get("step_size"), 0.0) or float(DEFAULT_LOT_STEP)
        sell_qty = _floor_to_step(sell_qty, step)
        sell_notional = sell_qty * desired_tp
        if sell_qty <= 0 or sell_notional + 1e-10 < minimum:
            notice = f"{sell_qty:.12f}@{desired_tp:.12f}/{minimum:.12f}"
            if campaign.tp_min_notional_notice != notice:
                campaign.tp_min_notional_notice = notice
                self._log_event(
                    campaign,
                    "warn",
                    f"TP held — sellable amount is ${sell_notional:.5f}, below Binance's ${minimum:g} minimum. "
                    "No TP was sent; it will combine with a later campaign fill or needs manual resolution.",
                )
            return changed
        campaign.tp_min_notional_notice = None
        # NOW both halves can be judged. A resting TP is only correct if it is at
        # the right price AND offering the right amount. The tolerance is one
        # whole lot step, which also supplies the hysteresis that stops a
        # jittering free balance from cancelling and re-placing every tick.
        if campaign.tp_order_id and current_price_ok:
            resting = open_orders.get(str(campaign.tp_order_id)) or {}
            resting_qty = _coerce_float(resting.get("origQty")) - _coerce_float(resting.get("executedQty"))
            if resting_qty <= 0:
                # The row carried no usable quantity. A missing field is a data
                # gap, not evidence of a short order — cancelling a working sell
                # on that basis would churn live orders for nothing. Price
                # matches, so leave it and re-judge when the row is complete.
                return changed
            if abs(sell_qty - resting_qty) < step:
                return changed  # right price, right size — nothing to do
            self._log_event(
                campaign,
                "warn",
                f"TP at {desired_tp:,.2f} is the right price but covers {resting_qty:.8f} of "
                f"{sell_qty:.8f} — replacing it so the whole position has an exit.",
            )
        # Only cancel a stale TP after proving the replacement meets the
        # exchange rules. Keeping an older target is safer than leaving the
        # position with no target at all.
        if campaign.tp_order_id:
            # And only place the replacement once the old one is PROVEN gone.
            # A swallowed cancel failure used to be followed by a second sell
            # anyway — two orders against one position, both able to fill, and
            # the entry sweep skips -tp- ids so nothing would clear the orphan.
            # A cancel can also fail because it just filled, which the next
            # sync's fill branch books properly; either way, waiting is right.
            if not await self._safe_cancel(campaign, campaign.tp_order_id):
                self._log_event(
                    campaign,
                    "warn",
                    f"Stale TP {campaign.tp_order_id} could not be cancelled — keeping it and retrying "
                    f"next sync rather than resting a second sell against the same position.",
                )
                return changed
            campaign.tp_order_id = None
            campaign.tp_order_price = None
        campaign.tp_rev += 1
        tp_client_id = f"cf-csc-{campaign.campaign_id}-tp-{campaign.tp_rev}"
        try:
            result = await asyncio.to_thread(
                lambda: self.broker.place_order(
                    campaign.symbol,
                    0.0,
                    "sell",
                    order_type="limit_order",
                    limit_price=desired_tp,
                    client_order_id=tp_client_id,
                    base_qty=sell_qty,
                )
            )
        except Exception as exc:
            result = {"error": str(exc)}
        if isinstance(result, dict) and not result.get("error"):
            order_id = self._order_id_from(result)
            if not order_id:
                # Accepted but unidentifiable. Find it by our own client id
                # rather than storing "" and placing a second sell next sync.
                order_id = await self._adopt_order_by_client_id(campaign, tp_client_id) or ""
            if not order_id:
                self._log_event(
                    campaign,
                    "error",
                    "TP sell was accepted but carried no order id and could not be found by client id; "
                    "not recording it, so the next sync will reconcile rather than stack a second sell.",
                )
                return changed
            campaign.tp_order_id = order_id
            campaign.tp_price = desired_tp
            campaign.tp_order_price = desired_tp
            short = desired_qty - sell_qty
            self._log_event(
                campaign,
                "order",
                f"TP limit sell placed: {sell_qty:.8f} @ {desired_tp:,.2f}"
                + (f" (includes {campaign.residual_base_qty:.8f} carried)" if campaign.residual_base_qty else "")
                + (f" — capped to the exchange balance, {short:.8f} held back as fee dust" if short > 1e-9 else ""),
            )
            changed = True
        else:
            error = (result or {}).get("error") if isinstance(result, dict) else "unknown error"
            self._log_event(campaign, "error", f"Failed to place TP sell: {error}")
        return changed

    async def _recover_order_by_client_id(self, campaign: Campaign, order: PendingOrder) -> bool:
        """After an ambiguous placement failure, check whether the order actually rests."""
        try:
            open_orders = await self._open_orders_by_id(campaign)
        except Exception:
            return False
        for order_id, row in open_orders.items():
            if str(row.get("clientOrderId") or "") == order.client_order_id:
                order.order_id = order_id
                order.status = "PLACED"
                return True
        return False

    async def _safe_get_order(self, campaign: Campaign, order_id) -> dict:
        try:
            return await asyncio.to_thread(self.broker.get_order, campaign.symbol, order_id) or {}
        except Exception as exc:
            _log.warning("[CASCADE] get_order failed for %s: %s", order_id, exc)
            return {}

    async def _safe_cancel(self, campaign: Campaign, order_id) -> bool:
        """Cancel an order, reporting whether it actually went through.

        The return value matters for the TP: a silently-swallowed failure used
        to be followed by placing the replacement anyway, leaving TWO sells
        resting against one position. Callers that are merely tidying up can
        keep ignoring it.
        """
        try:
            result = await asyncio.to_thread(self.broker.cancel_order, order_id, campaign.symbol)
        except Exception as exc:
            _log.warning("[CASCADE] cancel failed for %s: %s", order_id, exc)
            return False
        if isinstance(result, dict) and result.get("error"):
            _log.warning("[CASCADE] cancel refused for %s: %s", order_id, result.get("error"))
            return False
        return True

    async def _cancel_all_live_orders(self, campaign: Campaign, include_tp: bool) -> None:
        """Pull every working order this campaign owns.

        The accumulated buy stop lives in campaign.pending_order_id, NOT in
        leg.pending_orders — those are collection markers and only ever reach
        "PLACED" through a legacy recovery path. Cancelling by marker status
        alone therefore missed the one order that was actually working, so
        stopping a live campaign left a live buy stop on the exchange. It could
        still fill after the campaign was archived, buying coin with nothing
        tracking it and no TP to sell it.

        include_tp is False when the campaign ends holding a position: the TP
        is deliberately left resting so the exit still happens.
        """
        if campaign.pending_order_id:
            await self._safe_cancel(campaign, campaign.pending_order_id)
            campaign.pending_order_id = None
        campaign.pending_stop_price = None
        campaign.pending_limit_price = None
        campaign.pending_stop_ts = None
        campaign.pending_last_red = None
        for leg in campaign.legs:
            for order in leg.pending_orders.values():
                if order.status == "PLACED" and order.order_id:
                    await self._safe_cancel(campaign, order.order_id)
                if order.is_open:
                    order.status = "CANCELLED"
        if include_tp and campaign.tp_order_id:
            await self._safe_cancel(campaign, campaign.tp_order_id)
            campaign.tp_order_id = None
            campaign.tp_order_price = None
        # Belt and braces: sweep anything of ours still open on the exchange.
        # Our own bookkeeping is exactly what failed above, so the exchange —
        # not the campaign object — gets the final say on what is still working.
        try:
            open_orders = await self._open_orders_by_id(campaign)
        except Exception as exc:
            _log.warning("[CASCADE] cancel sweep could not list orders: %s", exc)
            return
        for order_id, row in open_orders.items():
            client_id = str(row.get("clientOrderId") or "")
            if not client_id.startswith(f"cf-csc-{campaign.campaign_id}-"):
                continue
            if not include_tp and "-tp-" in client_id:
                continue  # the exit stays working on purpose
            await self._safe_cancel(campaign, order_id)
