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
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Dict, List, Optional

_log = logging.getLogger("cryptoforge.cascade")

CASCADE_LEVELS = (2, 4, 8)
LEVEL_ALLOCATION = {2: 0.20, 4: 0.30, 8: 0.50}
BASE_TIMEFRAME = "5m"
ESCALATED_TIMEFRAME = "15m"
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
DEFAULT_TICK_SIZE = 0.01
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
MIN_FIB_RANGE_PCT = 0.0008
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
MODEL_VERSION = 18  # bump when the fib/trendline rules change; older campaigns are flagged stale
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
# Every trendline starts at the same point — the mother high — so two of them
# are the same line whenever their second anchors are close. Drawn on the chart
# they overlap into one thick smear, which is not what gets drawn by hand: the
# charts show two or three clearly separated lines, never four near-parallel
# ones. A new line has to sit this far from each existing line, measured at the
# candle that created it, or it reuses the line already there.
MIN_TRENDLINE_SEPARATION_PCT = 0.0015
MIN_NOTIONAL_FLOOR_USD = 5.0  # Binance Spot MIN_NOTIONAL filter is ~$5 on USDT pairs
# Cushion over the exchange minimum on every rung. An order sized exactly at
# MIN_NOTIONAL is one tick of adverse quote movement from being rejected, so
# each rung carries 10% more: $5.50 against a $5 minimum.
RUNG_BUFFER_PCT = 0.10
# Order states whose money is gone or committed elsewhere — never re-rung.
SPENT_ORDER_STATES = frozenset({"FILLED", "CLOSED", "CANCELLED"})
FIVE_MIN_SEC = 300
FIFTEEN_MIN_SEC = 900
# View-only roll-ups for the campaign chart. The engine always steps in 5m;
# these just change how the same candles are drawn.
CHART_TIMEFRAMES = {"5m": FIVE_MIN_SEC, "15m": FIFTEEN_MIN_SEC, "1h": 3600}

ACTIVE_STATES = {"WAITING_FIRST_DEPTH", "TRENDLINE_ACTIVE"}
FINAL_STATES = {"COMPLETED", "MOTHER_BROKEN", "STOPPED"}
# Endings that roll straight into a fresh campaign. A deliberate stop does not.
RESTART_REASONS = {"mother_broken", "mother_retested"}


class CascadeModelError(Exception):
    pass


def _coerce_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


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
                if c.close > line_price_at_c + epsilon:
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
    min_notional_usd: float = MIN_NOTIONAL_FLOOR_USD
    tick_size: float = DEFAULT_TICK_SIZE  # exchange price increment, for the stop/limit gap
    parent_campaign_id: Optional[str] = None  # set when a mother break auto-started this one
    generation: int = 1  # 1 = manually started; each auto-restart increments
    barren_chain: int = 0  # consecutive auto-restarts that ended without drawing a fib
    left_mother_range: bool = False  # price has traded below the mother low, arming the retest rule
    model_version: int = 0  # rules version the stored legs/trendlines were built with
    created_at: str = ""
    state: str = "WAITING_FIRST_DEPTH"
    cumulative_used_pct: float = 0.0
    carry_forward_usd: float = 0.0  # legacy: kept so older snapshots still load
    trendlines: List[Trendline] = field(default_factory=list)
    legs: List[Leg] = field(default_factory=list)
    active_trendline_id: Optional[int] = None
    all_fills: List[Fill] = field(default_factory=list)  # fills of the OPEN position only
    rounds: List[Round] = field(default_factory=list)  # closed open-to-TP cycles
    avg_entry_price: Optional[float] = None
    tp_price: Optional[float] = None  # active TP once fills exist; display estimate before
    tp_order_id: Optional[str] = None
    tp_rev: int = 0
    tp_filled: bool = False
    filled_base_qty: float = 0.0
    realized_pnl: Optional[float] = None
    mother_broken_above: bool = False
    # The structure window: candles since the last cut (the cut candle seeds
    # the next window). Everything else — dip, touch, fib anchors — is derived
    # from the candle history inside this window at evaluation time, so there
    # is no swing state to corrupt or restart.
    window_start_ts: int = 0
    broken_above: bool = False  # active trendline has been closed above
    last_processed_ts: int = 0  # last processed closed 5m candle open ts
    closed_at: str = ""
    close_reason: str = ""
    event_log: List[dict] = field(default_factory=list)

    @property
    def capital_unit_per_pct(self) -> float:
        return self.capital_usd / 100.0

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
            "min_notional_usd": self.min_notional_usd,
            "tick_size": self.tick_size,
            "parent_campaign_id": self.parent_campaign_id,
            "generation": self.generation,
            "barren_chain": self.barren_chain,
            "left_mother_range": self.left_mother_range,
            "model_version": self.model_version,
            "created_at": self.created_at,
            "state": self.state,
            "cumulative_used_pct": self.cumulative_used_pct,
            "carry_forward_usd": self.carry_forward_usd,
            "trendlines": [tl.to_dict() for tl in self.trendlines],
            "legs": [leg.to_dict() for leg in self.legs],
            "active_trendline_id": self.active_trendline_id,
            "all_fills": [f.to_dict() for f in self.all_fills],
            "rounds": [r.to_dict() for r in self.rounds],
            "avg_entry_price": self.avg_entry_price,
            "tp_price": self.tp_price,
            "tp_order_id": self.tp_order_id,
            "tp_rev": self.tp_rev,
            "tp_filled": self.tp_filled,
            "filled_base_qty": self.filled_base_qty,
            "realized_pnl": self.realized_pnl,
            "mother_broken_above": self.mother_broken_above,
            "window_start_ts": self.window_start_ts,
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
            "min_notional_usd",
            "tick_size",
            "parent_campaign_id",
            "generation",
            "barren_chain",
            "left_mother_range",
            "model_version",
            "created_at",
            "state",
            "cumulative_used_pct",
            "carry_forward_usd",
            "active_trendline_id",
            "avg_entry_price",
            "tp_price",
            "tp_order_id",
            "tp_rev",
            "tp_filled",
            "filled_base_qty",
            "realized_pnl",
            "mother_broken_above",
            "window_start_ts",
            "broken_above",
            "last_processed_ts",
            "closed_at",
            "close_reason",
        ):
            if key in data:
                setattr(campaign, key, data[key])
        campaign.trendlines = [Trendline.from_dict(tl) for tl in data.get("trendlines") or []]
        campaign.legs = [Leg.from_dict(leg) for leg in data.get("legs") or []]
        campaign.all_fills = [Fill.from_dict(f) for f in data.get("all_fills") or []]
        campaign.rounds = [Round.from_dict(r) for r in data.get("rounds") or []]
        campaign.event_log = list(data.get("event_log") or [])
        return campaign


def timeframe_for_level(leg: Leg, level: int) -> str:
    if level == 2:
        return BASE_TIMEFRAME
    return ESCALATED_TIMEFRAME if leg.escalated else BASE_TIMEFRAME


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
    if prior_leg is None or not prior_leg.low:
        allocation_pct = leg.leg_pct_from_mother
    else:
        allocation_pct = (prior_leg.low - leg.low) / prior_leg.low * 100
    allocation_pct = max(allocation_pct, 0.0)

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
            timeframe=timeframe_for_level(leg, level),
            status="UNFUNDED",
            entry_style="stop" if level in STOP_ENTRY_LEVELS else "limit",
            client_order_id=f"cf-csc-{campaign.campaign_id}-{leg.leg_id}-{level}-0",
        )
    replan_ladder(campaign)


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
    Spread the campaign's money across ONE ladder built from every fib's levels,
    ordered by price rather than by which fib they came from.

    Fibs overlap. Fib 2's level 2 can easily sit between fib 1's level 4 and its
    level 8, and treating each fib as its own private pool ignores that: it
    piled everything a fib owned onto that fib's deepest rung, well past the
    minimum it needed, while rungs price would reach first sat empty.

    So instead:

    1. Every unfilled level of every fib becomes a rung on one ladder, ordered
       by price.
    2. Funding starts at the DEEPEST rung and works up. Each takes a standard
       rung, and when the money runs out the shallow rungs are the ones left
       empty. A rung that cannot be given a full one gets nothing, because a
       part-rung is not placeable.
    3. Anything left after every rung is covered is spread over them weighted by
       level — 20/30/50 — so the deep end carries the most there too.

    Deepest-first is the whole point of the strategy: the money is meant to buy
    the cheapest prices the fall offers, so a scarce pool belongs at the bottom
    of the ladder, not on the rung nearest the market.

    There is no carry-forward between fibs any more. There is one pool, and it
    is re-split from scratch whenever a fill spends part of it or a new fib adds
    rungs, so the money always sits where the current ladder wants it.
    """
    rung = rung_size_usd(campaign)
    budget = max(campaign.total_allocation_usd - campaign.spent_usd, 0.0)

    rungs: List[tuple] = []
    for leg in campaign.legs:
        if leg.fib is None:
            continue
        for level in CASCADE_LEVELS:
            order = leg.pending_orders.get(level)
            if order is None or order.status in SPENT_ORDER_STATES:
                continue
            price = _coerce_float(order.price) or max(leg.fib.level_price(level), 0.0)
            if price <= 0:
                continue
            rungs.append((price, leg, level, order))
    rungs.sort(key=lambda row: -row[0])

    amounts: Dict[int, float] = {}
    weights = [LEVEL_ALLOCATION[row[2]] for row in rungs]
    total_weight = sum(weights) or 1.0
    shares = [budget * w / total_weight for w in weights]

    if rungs and min(shares) + 1e-9 >= rung:
        # Plenty to go round: split it purely by weight so the 20/30/50 shape
        # holds exactly, with no rung needing a top-up.
        for row, share in zip(rungs, shares):
            amounts[id(row[3])] = share
    else:
        # Short. Start at the DEEPEST rung and work up, handing each a full rung
        # until the money runs out — the cheapest prices get covered and the
        # shallow rungs go empty. A part-rung is not placeable, so a rung either
        # gets the whole thing or nothing. Whatever survives that pass then
        # shares any surplus, weighted deeper again.
        funded: List[tuple] = []
        remaining = budget
        for row in reversed(rungs):
            if remaining + 1e-9 < rung:
                break
            funded.append(row)
            remaining -= rung
        for row in funded:
            amounts[id(row[3])] = rung
        if funded and remaining > 0.01:
            fw = [LEVEL_ALLOCATION[row[2]] for row in funded]
            fw_total = sum(fw) or 1.0
            for row, weight in zip(funded, fw):
                amounts[id(row[3])] += remaining * weight / fw_total

    for price, leg, level, order in rungs:
        amount = round(amounts.get(id(order), 0.0), 2)
        if abs(amount - _coerce_float(order.usd_notional)) < 0.01 and order.status != "UNFUNDED":
            continue
        if order.status == "PLACED" and order.order_id:
            # The resting order is the wrong size now. Drop the id so the
            # exchange sweep cancels it and a fresh one goes out at the new
            # amount under a new client id.
            order.order_id = None
            order.rev += 1
            order.client_order_id = f"cf-csc-{campaign.campaign_id}-{leg.leg_id}-{level}-{order.rev}"
        order.usd_notional = amount
        order.quantity = amount / price if amount > 0 and price > 0 else 0.0
        order.status = "PENDING" if amount > 0 else "UNFUNDED"
        order.own_usd = min(amount, rung)
        order.received = []
        order.moved_usd = 0.0
        order.moved_to_level = None


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
        self._candles_5m: Dict[str, List[Candle]] = {}  # per-campaign candle history (rebuilt on restart)
        self._price_cache: Dict[str, tuple] = {}
        self._last_sync_ts: Dict[str, float] = {}  # per campaign — a shared
        # timestamp meant two live campaigns starved each other of syncs
        self._loop_interval_sec = 5.0
        # 30s left a fill un-hedged for up to half a minute before the TP
        # went up. Weight cost at 10s is trivial against Binance's budget.
        self._sync_interval_sec = 10.0
        self._last_candle_ts = 0.0  # monotonic time of the last processed candle, for stall detection
        self._stall_alerted = False

    # ── lifecycle ────────────────────────────────────────────────

    def start(self):
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._monitor_loop())
        _log.info("[CASCADE] engine started")

    def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            self._task = None

    async def shutdown(self):
        self.stop()
        self._emit_update()

    @property
    def active_campaigns(self) -> List[Campaign]:
        return [c for c in self.campaigns.values() if c.state in ACTIVE_STATES]

    @property
    def live_campaigns(self) -> List[Campaign]:
        return [c for c in self.active_campaigns if c.mode == "live"]

    # ── events / updates ─────────────────────────────────────────

    def _log_event(self, campaign: Optional[Campaign], level: str, message: str):
        event = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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
        if stalled_for > STALL_ALERT_SEC:
            self._alert(
                "Cascade engine STALLED",
                f"No 5m candle has been processed for {stalled_for / 60:.0f} minutes "
                f"while {len(active)} campaign(s) are active.\n\n"
                f"Orders already on Binance still stand, but nothing is being "
                f"armed, stepped or filled. Check the server.",
                level="error",
                dedupe_sec=1800,
            )

    def _alert(self, title: str, body: str, level: str = "warn", dedupe_sec: float = 0.0) -> None:
        """
        Push something worth waking up for. `dedupe_sec` suppresses a repeat of
        the same title within that window, so a condition that stays true (five
        campaigns open, the engine stalled) does not fire every loop tick.
        """
        if not self.on_alert:
            return
        if dedupe_sec > 0:
            now = time.monotonic()
            last = self._alert_state.get(title, 0.0)
            if now - last < dedupe_sec:
                return
            self._alert_state[title] = now
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

    # ── public API ───────────────────────────────────────────────

    async def start_campaign(
        self,
        symbol: str,
        capital_usd: float,
        mother_high: float,
        mother_low: float,
        mother_timestamp: Optional[int] = None,
        mode: str = "paper",
    ) -> dict:
        symbol = str(symbol or "").strip().upper()
        mode = "live" if str(mode or "").strip().lower() == "live" else "paper"
        capital_usd = _coerce_float(capital_usd)
        mother_high = _coerce_float(mother_high)
        mother_low = _coerce_float(mother_low)
        if not symbol:
            return {"error": "Symbol is required"}
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
        if capital_usd < min_notional * 2:
            return {"error": f"Capital must be at least ${min_notional * 2:g}"}

        now_ts = int(time.time())
        if mother_timestamp:
            mother_ts = int(mother_timestamp)
        else:
            # No timestamp given: the mother candle is a past candle the user
            # read off the chart, so find the recent 5m candle whose high
            # matches the entered mother high and anchor there. Defaulting to
            # "now" would make the engine wait for future candles forever and
            # ignore all the history that already formed the trendlines.
            detected = await self._resolve_mother_timestamp(symbol, mother_high)
            if detected is None:
                return {
                    "error": (
                        "Could not find a recent 5m candle matching that mother high. "
                        "Set the Mother Candle Time so the engine can replay from it, "
                        "or double-check the high value."
                    )
                }
            mother_ts = detected
        if mother_ts > now_ts:
            return {"error": "Mother candle timestamp cannot be in the future"}
        if mother_ts < now_ts - 90 * 86400:
            return {"error": "Mother candle is more than 90 days old — pick a more recent mother candle"}
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
            min_notional_usd=min_notional,
            tick_size=tick_size,
            model_version=MODEL_VERSION,
            created_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            last_processed_ts=mother_ts,
            window_start_ts=mother_ts,
        )
        self.campaigns[campaign.campaign_id] = campaign
        self._log_event(
            campaign,
            "start",
            f"Campaign {campaign.campaign_id} started ({mode.upper()}) — {symbol}, capital ${capital_usd:g}, "
            f"mother high {mother_high:g} / low {mother_low:g}",
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
        if cancel_orders and campaign.mode == "live":
            await self._cancel_all_live_orders(campaign, include_tp=True)
        campaign.state = "STOPPED"
        campaign.close_reason = "stopped"
        campaign.closed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._log_event(campaign, "stop", f"Campaign {campaign_id} stopped")
        self._archive_campaign(campaign)
        self._emit_update()
        return {"status": "ok", "campaign": campaign.to_dict()}

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
        self._candles_5m.pop(campaign_id, None)
        self._last_sync_ts.pop(campaign_id, None)
        if not any(row.get("campaign_id") == campaign_id for row in self.closed_campaigns):
            if not campaign.close_reason:
                campaign.close_reason = "deleted"
                campaign.closed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
        self.closed_campaigns = sorted(merged.values(), key=lambda r: str(r.get("closed_at") or ""))[-100:]

    def get_status(self) -> dict:
        campaigns = []
        for campaign in self.campaigns.values():
            payload = campaign.to_dict()
            payload["display_tp_price"] = compute_tp_price(campaign)
            payload["spent_usd"] = round(campaign.spent_usd, 2)
            payload["resting_usd"] = round(campaign.resting_usd, 2)
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
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    async def _chart_candles(self, campaign: Campaign, max_candles: int) -> List[Candle]:
        """Closed 5m candles from the mother candle forward, for the chart."""
        try:
            df = await self.broker.async_get_candles(campaign.symbol, resolution=BASE_TIMEFRAME)
        except Exception as exc:
            _log.warning("[CASCADE] chart candle fetch failed for %s: %s", campaign.symbol, exc)
            return []
        if df is None or getattr(df, "empty", True):
            return []
        now = int(time.time())
        rows = []
        for index, row in df.iterrows():
            ts = int(index.timestamp())
            if ts < campaign.mother_timestamp or ts + FIVE_MIN_SEC > now:
                continue
            rows.append(
                Candle(
                    timestamp=ts,
                    open=_coerce_float(row.get("open")),
                    high=_coerce_float(row.get("high")),
                    low=_coerce_float(row.get("low")),
                    close=_coerce_float(row.get("close")),
                )
            )
        return rows[-max_candles:]

    async def recalculate_campaign(self, campaign_id: str) -> dict:
        """
        Rebuild a campaign's trendlines and fibs from scratch under the current
        rules, replaying every candle from the mother candle. Stored campaigns
        keep whatever geometry the rules produced when they ran, so a campaign
        created under older rules keeps stale fibs until this is run.

        Refused when real money is involved: a live campaign that has filled
        cannot have its ladder rewritten underneath its resting orders.
        """
        campaign = self.campaigns.get(campaign_id)
        if campaign is None:
            return {"error": f"Campaign {campaign_id} not found"}
        if campaign.mode == "live" and campaign.all_fills:
            return {"error": "Live campaign with fills cannot be recalculated — stop it and start a fresh one"}

        candles = await self._chart_candles(campaign, max_candles=100000)
        if not candles:
            return {"error": "No candles available to replay"}

        # Reset everything derived from the candles, keeping the campaign's
        # identity and settings.
        campaign.trendlines = []
        campaign.legs = []
        campaign.active_trendline_id = None
        campaign.all_fills = []
        campaign.rounds = []
        campaign.avg_entry_price = None
        campaign.filled_base_qty = 0.0
        campaign.tp_price = None
        campaign.tp_filled = False
        campaign.realized_pnl = None
        campaign.cumulative_used_pct = 0.0
        campaign.carry_forward_usd = 0.0
        campaign.mother_broken_above = False
        campaign.closed_at = ""
        campaign.close_reason = ""
        campaign.state = "WAITING_FIRST_DEPTH"
        campaign.window_start_ts = campaign.mother_timestamp
        campaign.model_version = MODEL_VERSION
        self._candles_5m[campaign_id] = []

        for candle in candles:
            if candle.timestamp <= campaign.mother_timestamp:
                continue  # the mother candle would trivially break its own high
            self._candles_5m[campaign_id].append(candle)
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
    def _aggregate_candles(candles: List[Candle], bucket_sec: int) -> List[Candle]:
        """
        Roll 5m candles up into larger view buckets. The engine only ever reasons
        in 5m — this is purely so the chart can be read at 15m or 1H without the
        geometry (which is 5m-derived) shifting underneath it.
        """
        if bucket_sec <= FIVE_MIN_SEC:
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

    async def get_chart_data(self, campaign_id: str, max_candles: int = 300, timeframe: str = "5m") -> dict:
        """
        Candles plus the geometry the engine actually used — trendline anchors,
        each leg's fib anchors/levels, ladder order prices and fills — so the
        marked levels can be verified visually against a real chart.
        """
        campaign = self.campaigns.get(campaign_id)
        if campaign is None:
            return {"error": f"Campaign {campaign_id} not found"}

        # Always pull a full window straight from the broker rather than the
        # engine's in-memory list: that list is only what this process has
        # stepped through, so after a restart it can hold a handful of candles
        # and the chart would render almost empty.
        bucket_sec = CHART_TIMEFRAMES.get(str(timeframe).lower(), FIVE_MIN_SEC)
        # Pull enough 5m candles that the rolled-up view still spans the window.
        raw_needed = max_candles * max(bucket_sec // FIVE_MIN_SEC, 1)
        history = await self._chart_candles(campaign, raw_needed)
        if not history:
            history = self._candles_5m.get(campaign_id) or []

        view = self._aggregate_candles(history, bucket_sec)
        candles = [{"t": c.timestamp, "o": c.open, "h": c.high, "l": c.low, "c": c.close} for c in view[-max_candles:]]
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
            "timeframe": str(timeframe).lower() if str(timeframe).lower() in CHART_TIMEFRAMES else "5m",
            "candles": candles,
            "trendlines": trendlines,
            "legs": legs,
            "fills": [f.to_dict() for f in campaign.all_fills],
            "avg_entry_price": campaign.avg_entry_price,
            "tp_price": compute_tp_price(campaign),
            "last_price": price_meta[0] if price_meta else None,
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
        self._backfill_closed_history()
        return restored

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
            self.closed_campaigns = self.closed_campaigns[-100:]
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
                changed = False
                for campaign in list(self.active_campaigns):
                    try:
                        changed |= await self._campaign_tick(campaign)
                    except Exception as exc:
                        _log.warning("[CASCADE] tick failed for %s: %s", campaign.campaign_id, exc)
                if changed:
                    self._emit_update()
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

    async def _get_price(self, symbol: str) -> float:
        cached = self._price_cache.get(symbol)
        if cached and time.monotonic() - cached[1] < 4.0:
            return cached[0]
        try:
            ticker = await asyncio.to_thread(self.broker.get_ticker, symbol)
            price = _coerce_float(ticker.get("last_price") or ticker.get("mark_price"))
        except Exception as exc:
            _log.warning("[CASCADE] price fetch failed for %s: %s", symbol, exc)
            price = cached[0] if cached else 0.0
        self._price_cache[symbol] = (price, time.monotonic())
        return price

    async def _resolve_mother_timestamp(self, symbol: str, mother_high: float) -> Optional[int]:
        """
        Find the open timestamp of the recent closed 5m candle whose high most
        closely matches mother_high (within ~0.15%). Prefers the most recent
        candle on ties. Returns None if no close match is in the recent window
        (then the caller asks the user to supply the timestamp explicitly).
        """
        try:
            df = await self.broker.async_get_candles(symbol, resolution="5m")
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
            if ts + FIVE_MIN_SEC > now:
                continue  # skip the still-forming candle
            diff = abs(_coerce_float(row.get("high")) - mother_high)
            if diff <= tolerance and (best_diff is None or diff <= best_diff):
                best_diff = diff
                best_ts = ts
        return best_ts

    async def _fetch_closed_5m(self, symbol: str, since_ts: int) -> List[Candle]:
        """Fetch closed 5m candles with open ts > since_ts, paging as needed."""
        now = int(time.time())
        candles: List[Candle] = []
        cursor = since_ts
        for _ in range(30):  # safety cap: 30 pages ≈ 100 days of 5m candles
            start = datetime.utcfromtimestamp(max(cursor - FIVE_MIN_SEC, 0)).strftime("%Y-%m-%d")
            try:
                df = await self.broker.async_get_candles(symbol, resolution="5m", start=start)
            except Exception as exc:
                _log.warning("[CASCADE] candle fetch failed for %s: %s", symbol, exc)
                break
            if df is None or df.empty:
                break
            batch = []
            for index, row in df.iterrows():
                ts = int(index.timestamp())
                if ts <= cursor or ts + FIVE_MIN_SEC > now:
                    continue
                batch.append(
                    Candle(
                        timestamp=ts,
                        open=_coerce_float(row.get("open")),
                        high=_coerce_float(row.get("high")),
                        low=_coerce_float(row.get("low")),
                        close=_coerce_float(row.get("close")),
                    )
                )
            if not batch:
                break
            candles.extend(batch)
            cursor = batch[-1].timestamp
            if cursor + 2 * FIVE_MIN_SEC > now:
                break
        return candles

    async def _candle_step(self, campaign: Campaign) -> bool:
        if campaign.state not in ACTIVE_STATES:
            return False
        now = int(time.time())
        if campaign.last_processed_ts and now < campaign.last_processed_ts + 2 * FIVE_MIN_SEC:
            return False
        history = self._candles_5m.setdefault(campaign.campaign_id, [])
        if not history and campaign.last_processed_ts > campaign.mother_timestamp:
            # Restored campaign: the structure window is derived from candle
            # history, so rebuild everything since the mother candle. Candles
            # already processed are backfilled without re-running the engine.
            prior = await self._fetch_closed_5m(campaign.symbol, campaign.mother_timestamp)
            history.extend(c for c in prior if c.timestamp <= campaign.last_processed_ts)
        new_candles = await self._fetch_closed_5m(campaign.symbol, campaign.last_processed_ts)
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
        return changed

    def _candles_between(self, campaign: Campaign, until_ts: int) -> List[Candle]:
        history = self._candles_5m.get(campaign.campaign_id, [])
        return [c for c in history if campaign.mother_timestamp < c.timestamp < until_ts]

    def _fifteen_minute_candle(self, campaign: Campaign, closed_5m: Candle) -> Optional[Candle]:
        """When closed_5m completes a 15m bucket, return the aggregated candle."""
        bucket_start = (closed_5m.timestamp // FIFTEEN_MIN_SEC) * FIFTEEN_MIN_SEC
        if closed_5m.timestamp != bucket_start + 2 * FIVE_MIN_SEC:
            return None
        history = self._candles_5m.get(campaign.campaign_id, [])
        members = [c for c in history if bucket_start <= c.timestamp < bucket_start + FIFTEEN_MIN_SEC]
        if len(members) < 3:
            return None
        return Candle(
            timestamp=bucket_start,
            open=members[0].open,
            high=max(c.high for c in members),
            low=min(c.low for c in members),
            close=members[-1].close,
            timeframe=ESCALATED_TIMEFRAME,
        )

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
        # Only a red candle can cut a dip; everything else just extends the
        # window. All structure — dip, touch, fib anchors — is derived from the
        # candle history at evaluation time, so nothing is ever discarded.
        if candle.close < candle.open:
            self._evaluate_cut(campaign, candle)
        if campaign.state in ACTIVE_STATES:
            # Fill against the trigger that was resting while this candle formed,
            # THEN let the candle walk it down. The trigger sits a body above the
            # last close, so a candle that wicks up through it really would have
            # been filled — advancing first would hide that.
            if campaign.mode == "paper":
                self._paper_fill_check(campaign, candle)
            self._advance_stop_entries(campaign, candle)

    def _evaluate_cut(self, campaign: Campaign, candle: Candle) -> None:
        """
        Try to finalize a structure on this red candle. The window is every
        candle since the last cut (the cut candle seeds the next window).

        1. Anchor: find_valid_anchor2 over mother -> this candle — the tightest
           descending line from the mother high no close has crossed (what the
           magnet tool gives when dragging from the mother candle).
        2. Crossings: candles in the window that TOUCH that line close-based —
           high >= line while the close stays below it. A crossing needs at
           least one earlier window candle, and the candle currently holding
           the running dip low cannot be its own rise.
        3. The dip freezes at the first crossing: fib 1 = the lowest low before
           it. Lows after the touch belong to the next structure.
        4. Cut: this candle's close DECISIVELY below the frozen dip (>= 0.02%
           of price). Indecisive probes a few dollars under the dip are the
           fall resuming, not a completed swing.
        5. fib 0 = the highest crossing high (running max over the up-swing,
           per cascade_lib's running_max_high).
        """
        history = self._candles_5m.get(campaign.campaign_id, [])
        window = [
            c
            for c in history
            if c.timestamp >= campaign.window_start_ts
            and c.timestamp > campaign.mother_timestamp
            and c.timestamp <= candle.timestamp
        ]
        if len(window) < 2:
            return

        between = [c for c in history if campaign.mother_timestamp < c.timestamp < candle.timestamp]
        anchor_price, anchor_ts = find_valid_anchor2(campaign.mother_high, campaign.mother_timestamp, between)
        if anchor_price is None or anchor_price >= campaign.mother_high:
            return
        tl = Trendline(
            trendline_id=len(campaign.trendlines) + 1,
            anchor1_price=campaign.mother_high,
            anchor1_timestamp=campaign.mother_timestamp,
            anchor2_price=anchor_price,
            anchor2_timestamp=int(anchor_ts),
        )

        frozen_dip = None
        run_min = None
        run_min_ts = None
        first_cross_ts = None
        touch_high = None
        touch_ts = None
        for c in window:
            line = trendline_price(tl, c.timestamp)
            crossed = c.high >= line and c.close < line and c.high < campaign.mother_high
            # A touch only counts if it comes AFTER the candle that made the
            # dip: one candle cannot be both the low of the swing and the rise
            # off it. Note this is about ORDER, not about whether the touching
            # candle also made a new low — in a steady fall almost every candle
            # that reaches a falling line is also printing a lower low, so
            # excluding those outright would mean no structure ever forms.
            if crossed and run_min_ts is not None and c.timestamp > run_min_ts:
                if first_cross_ts is None:
                    first_cross_ts = c.timestamp
                    frozen_dip = run_min
                if touch_high is None or c.high > touch_high:
                    touch_high = c.high
                    touch_ts = c.timestamp
            if first_cross_ts is None and (run_min is None or c.low < run_min):
                run_min = c.low
                run_min_ts = c.timestamp

        if first_cross_ts is None or frozen_dip is None or touch_high is None:
            return
        if (touch_high - frozen_dip) < touch_high * MIN_FIB_RANGE_PCT:
            return  # a few ticks of chop, not a swing — its levels would be noise
        if candle.close >= frozen_dip:
            return
        if (frozen_dip - candle.close) < candle.close * DECISIVE_BREAK_PCT:
            return  # not a decisive break of the dip
        if touch_high <= frozen_dip:
            return

        # Two structures touched at essentially the same price are the same
        # shelf. The second one adds nothing but a cancellation of orders that
        # were about to fill, so it is dropped ENTIRELY — no trendline, no fib.
        # The previous structure stays active with its ladder resting.
        # Checked against EVERY fib drawn so far, not just the last one. Price
        # wanders away from a shelf and comes back to it hours later, so the
        # duplicate is usually two or three fibs back: one campaign drew fib 1
        # and fib 3 with the identical touch high of 78.75 because fib 3 was
        # only ever compared against fib 2. That matters more now that every
        # fib keeps its ladder resting — a same-shelf fib puts a second set of
        # orders a few ticks from the first and splits the money between them.
        prior = None
        separation = 0.0
        for leg in campaign.legs:
            if not leg.touch_high:
                continue
            gap = abs(touch_high - leg.touch_high) / leg.touch_high
            if prior is None or gap < separation:
                prior, separation = leg, gap
        if prior is not None:
            if separation < MIN_LEG_SEPARATION_PCT:
                # Geometry only: the line is real and belongs on the chart, but
                # it carries no fib and places no orders, so the previous fib
                # keeps its resting ladder.
                #
                # Because nothing downstream depends on this anchor, it is found
                # the way the line gets drawn by hand — dragging from the mother
                # candle with the magnet on, which snaps to the open of the very
                # candle that made the break. Fib-bearing anchors must keep
                # excluding that candle: including it moves fib 0 on both
                # verified days.
                display = [c for c in history if campaign.mother_timestamp < c.timestamp <= candle.timestamp]
                d_price, d_ts = find_valid_anchor2(campaign.mother_high, campaign.mother_timestamp, display)
                if d_price is None or d_price >= campaign.mother_high:
                    d_price, d_ts = anchor_price, anchor_ts
                ghost = Trendline(
                    trendline_id=len(campaign.trendlines) + 1,
                    anchor1_price=campaign.mother_high,
                    anchor1_timestamp=campaign.mother_timestamp,
                    anchor2_price=d_price,
                    anchor2_timestamp=int(d_ts),
                    bears_fib=False,
                )
                if self._duplicate_trendline(campaign, ghost, candle.timestamp) is None:
                    campaign.trendlines.append(ghost)
                self._log_event(
                    campaign,
                    "skip",
                    f"Trendline {len(campaign.trendlines)} drawn (geometry only) to red "
                    f"candle open {d_price:,.2f}. Its touch {touch_high:,.2f} is just "
                    f"{separation * 100:.3f}% from fib {prior.leg_id}'s "
                    f"{prior.touch_high:,.2f} — same shelf, so no fib is drawn and fib "
                    f"{prior.leg_id}'s ladder stays resting.",
                )
                campaign.window_start_ts = candle.timestamp
                return

        # A line that runs where one already runs is not a new line. The fib it
        # carries is real and gets drawn; it just hangs off the existing line
        # rather than adding a near-identical twin to the chart.
        twin = self._duplicate_trendline(campaign, tl, candle.timestamp)
        added_trendline = twin is None
        if twin is not None:
            tl = twin
        else:
            campaign.trendlines.append(tl)
            self._log_event(
                campaign,
                "trendline",
                f"Trendline {tl.trendline_id} drawn: mother high {campaign.mother_high:g} -> "
                f"red candle open {anchor_price:g}",
            )
        campaign.active_trendline_id = tl.trendline_id
        campaign.state = "TRENDLINE_ACTIVE"
        legs_before = len(campaign.legs)
        self._draw_leg(campaign, touch_high, frozen_dip, touch_ts, tl.trendline_id)
        if len(campaign.legs) == legs_before:
            if added_trendline:
                campaign.trendlines.pop()
            campaign.active_trendline_id = campaign.trendlines[-1].trendline_id if campaign.trendlines else None
            return
        # This cut candle opens the next window; its low seeds the next dip.
        campaign.window_start_ts = candle.timestamp

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
            f"{', escalated' if leg.escalated else ''}). Ladder re-split by price — "
            + (
                ", ".join(f"F{o.leg_id} L{o.level} ${o.usd_notional:g} @ {o.price:,.2f}" for o in funded)
                if funded
                else f"pool ${campaign.total_allocation_usd:,.2f} still under one rung, nothing placeable yet"
            ),
        )

    # ── fills / TP ───────────────────────────────────────────────

    def _paper_fill_check(self, campaign: Campaign, closed_5m: Candle) -> None:
        candle_15m = self._fifteen_minute_candle(campaign, closed_5m)
        for leg in campaign.open_legs:
            for order in leg.pending_orders.values():
                price = order.working_price
                if not order.is_open or not price:
                    continue
                probe = closed_5m if order.timeframe == BASE_TIMEFRAME else candle_15m
                if probe is None:
                    continue
                # A candle that just set the stop is the fall continuing, not a
                # turn — it owns the trigger and cannot also take it.
                if order.stop_ts is not None and probe.timestamp <= order.stop_ts:
                    continue
                if order.entry_style == "stop":
                    # A buy stop sits ABOVE the market and triggers on the way up.
                    # Fill at the limit cap: the pessimistic end of the band the
                    # order can actually execute in.
                    if probe.high >= (order.stop_price or 0.0):
                        self._record_fill(campaign, leg, order, price, probe.timestamp, order_id="PAPER")
                elif probe.low <= price:
                    self._record_fill(campaign, leg, order, price, probe.timestamp, order_id="PAPER")

    def _advance_stop_entries(self, campaign: Campaign, closed_5m: Candle) -> None:
        """
        Levels 2 and 4 go in as ONE working BUY STOP, not resting limits, and
        the trigger sits at the close of the LOWEST red candle so far.

        That geometry is the whole idea. While the market keeps falling, each
        new red close drags the stop down with it and nothing fills; the order
        chases price down without ever buying into it. Only when the market
        U-turns and trades back up through that last red body does it trigger —
        and because the trigger is the lowest close, that is the cheapest
        confirmed entry the fall offered.

        Two reds under the line are needed before anything is placed: the first
        breaks the line, the second confirms the fall and puts the market below
        the trigger (a buy stop has to sit above the market to be a stop at
        all). Greens are ignored entirely, and a red closing higher than the
        last one does not count, because price must be lower than the previous
        candle.

        The ladder does NOT restart when the fall crosses a deeper level. L4's
        money folds into the order already working and the sequence carries on
        undisturbed — see _merge_stop_level.

        The probe is the carrier's own timeframe, so a leg escalated to 15m
        steps off 15m closes with no extra plumbing.

        Every open fib steps independently. Each keeps its own carrier and its
        own last-red memory, so fib 1's level 2 chases price down at the same
        time fib 3's does, and both are live when the turn comes.
        """
        for leg in campaign.open_legs:
            self._advance_leg_stops(campaign, leg, closed_5m)

    def _advance_leg_stops(self, campaign: Campaign, leg: Leg, closed_5m: Candle) -> None:
        stops = [order for _, order in sorted(leg.pending_orders.items()) if order.entry_style == "stop"]
        carrier = next((order for order in stops if order.is_open and order.price), None)
        if carrier is None:
            return
        probe = closed_5m if carrier.timeframe == BASE_TIMEFRAME else self._fifteen_minute_candle(campaign, closed_5m)
        if probe is None or probe.timestamp == carrier.stop_ts:
            return
        if probe.close >= probe.open:
            return  # only red candles act, before arming and after

        # Deeper levels the fall has now passed hand their money to the order
        # already working rather than opening a second front.
        for order in stops:
            if order is not carrier and order.is_open and order.price and probe.close < order.price:
                self._merge_stop_level(campaign, carrier, order)

        if probe.close >= carrier.price:
            return  # the carrier's own line has not broken yet
        if carrier.last_red_close is None:
            carrier.last_red_close = probe.close
            self._log_event(
                campaign,
                "order",
                f"L{carrier.level} fib line {carrier.price:,.2f} broken at {probe.close:,.2f} — "
                f"waiting for a second red {carrier.timeframe} candle to set the stop",
            )
            return
        if probe.close >= carrier.last_red_close:
            return  # not lower than the previous red — price must keep falling
        # The trigger is the PREVIOUS red close, one body back, so it sits ABOVE
        # where the market just closed. That is what makes it a stop rather than
        # a limit: the fall walks it down and only a turn back up takes it.
        self._reprice_stop(campaign, carrier, carrier.last_red_close, probe)
        carrier.last_red_close = probe.close

    def _merge_stop_level(self, campaign: Campaign, carrier: PendingOrder, donor: PendingOrder) -> None:
        """Fold a deeper stop level's allocation into the order already working.
        The cascade keeps running on one trigger instead of restarting."""
        amount = _coerce_float(donor.usd_notional)
        donor.usd_notional = 0.0
        donor.quantity = 0.0
        donor.status = "MERGED"
        if amount <= 0:
            return
        carrier.usd_notional = round(carrier.usd_notional + amount, 2)
        if carrier.limit_price:
            carrier.quantity = carrier.usd_notional / carrier.limit_price
        if carrier.status == "PLACED":
            self._release_for_replacement(campaign, carrier)
        self._log_event(
            campaign,
            "order",
            f"Fell through L{donor.level} ({donor.price:,.2f}) — its ${amount:,.2f} joins the working "
            f"L{carrier.level} buy stop, now ${carrier.usd_notional:,.2f}; sequence continues",
        )

    def _reprice_stop(self, campaign: Campaign, order: PendingOrder, trigger: float, probe: Candle) -> None:
        first = order.stop_price is None
        tick = _coerce_float(campaign.tick_size, DEFAULT_TICK_SIZE) or DEFAULT_TICK_SIZE
        order.stop_price = trigger
        order.limit_price = trigger + STOP_LIMIT_OFFSET_TICKS * tick
        order.stop_ts = probe.timestamp
        if order.limit_price > 0:
            order.quantity = order.usd_notional / order.limit_price
        if order.status == "PLACED":
            self._release_for_replacement(campaign, order)
        self._log_event(
            campaign,
            "order",
            f"L{order.level} buy stop {'armed' if first else 'stepped down'} at {trigger:,.2f} "
            f"(limit {order.limit_price:,.2f}) — the lowest red close so far; "
            f"buying only if price turns back up through it",
        )

    def _stop_is_placeable(self, campaign: Campaign, order: PendingOrder) -> bool:
        """
        A BUY stop has to sit above the market or Binance rejects it outright
        (-2010, "order would immediately trigger"). The trigger is the last red
        candle's close, so right after a close the market can still be sitting
        on it. Hold the order back until price is genuinely below the trigger —
        the next sync picks it up, and a market already above the trigger is one
        that has turned up without us, which the next red close re-arms.
        """
        stop = _coerce_float(order.stop_price)
        meta = self._price_cache.get(campaign.symbol)
        last = meta[0] if meta else None
        if stop <= 0 or not last:
            return True  # no live price to judge by — let the exchange decide
        if last < stop:
            return True
        self._log_event(
            campaign,
            "warn",
            f"L{order.level} buy stop {stop:,.2f} not placed — market is at {last:,.2f}, "
            f"at or above the trigger; waiting for price to drop back under it",
        )
        return False

    def _release_for_replacement(self, campaign: Campaign, order: PendingOrder) -> None:
        """Drop our claim on a resting order so _sync_live_orders cancels it
        (the id is no longer one of ours) and re-places it under a fresh id."""
        order.status = "PENDING"
        order.order_id = None
        order.rev += 1
        order.client_order_id = f"cf-csc-{campaign.campaign_id}-{order.leg_id}-{order.level}-{order.rev}"

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

    def _close_round(self, campaign: Campaign, exit_price: float) -> None:
        """
        A TP fill closes the current open-to-TP round, not the campaign. The
        principal comes back into available capital and the position resets to
        flat; the cascade keeps running and the freed money is re-split across
        the rungs still waiting. Only a mother-high breach (or a manual stop)
        ends the campaign.
        """
        qty = campaign.filled_base_qty
        avg = campaign.avg_entry_price or 0.0
        invested = sum(f.price * f.quantity for f in campaign.all_fills)
        leg = campaign.current_leg
        rnd = Round(
            round_id=len(campaign.rounds) + 1,
            leg_id=leg.leg_id if leg else 0,
            avg_entry=avg,
            quantity=qty,
            invested_usd=round(invested, 8),
            exit_price=exit_price,
            pnl=round((exit_price - avg) * qty, 8),
            closed_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        campaign.rounds.append(rnd)

        # Flatten the position: principal returns to available capital.
        campaign.all_fills = []
        campaign.filled_base_qty = 0.0
        campaign.avg_entry_price = None
        campaign.tp_price = None
        campaign.tp_order_id = None
        campaign.tp_rev += 1
        campaign.realized_pnl = round(campaign.realized_pnl_total, 8)

        # Filled entries are spent and gone; anything still resting stays live —
        # on every fib, not just the newest, since they all rest together.
        for lg in campaign.legs:
            for order in lg.pending_orders.values():
                if order.status == "FILLED":
                    order.status = "CLOSED"

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
        campaign.closed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
        campaign.mother_broken_above = True
        campaign.state = "MOTHER_BROKEN"
        campaign.close_reason = "mother_broken"
        campaign.closed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._log_event(
            campaign,
            "warn",
            f"Mother candle high {campaign.mother_high:g} broken above — campaign ended. "
            + (
                "Resting TP order left on the exchange to capture the exit."
                if campaign.mode == "live" and campaign.filled_base_qty > 0
                else ""
            ),
        )
        if campaign.mode == "live":
            self._schedule(self._cancel_all_live_orders(campaign, include_tp=False))
        elif campaign.filled_base_qty > 0:
            # Paper: price at/above mother high is at/above TP by construction.
            tp = compute_tp_price(campaign)
            if tp:
                # Closing the round must NOT skip archiving — it used to return
                # here, so any campaign that ended holding a position never
                # reached the closed list at all.
                self._close_round(campaign, tp)
        self._archive_campaign(campaign)
        if candle is not None:
            self._auto_restart(campaign, candle)

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
        asyncio.ensure_future(coro)

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

        child = Campaign(
            campaign_id=uuid.uuid4().hex[:10],
            seq=self._next_seq(),
            symbol=parent.symbol,
            capital_usd=parent.capital_usd,
            mother_high=candle.high,
            mother_low=candle.low,
            mother_timestamp=candle.timestamp,
            mode=parent.mode,
            min_notional_usd=parent.min_notional_usd,
            tick_size=parent.tick_size,
            parent_campaign_id=parent.campaign_id,
            generation=parent.generation + 1,
            barren_chain=barren,
            model_version=MODEL_VERSION,
            created_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            last_processed_ts=candle.timestamp,
            window_start_ts=candle.timestamp,
        )
        self.campaigns[child.campaign_id] = child
        # The breaking candle is the mother, so history starts clean from it.
        self._candles_5m[child.campaign_id] = [candle]
        self._log_event(
            child,
            "start",
            f"Auto-started from the break of campaign #{parent.seq} — new mother candle "
            f"high {candle.high:,.2f} / low {candle.low:,.2f} ({child.mode.upper()}, "
            f"generation {child.generation}). Nothing carried over.",
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
        self.closed_campaigns.append(payload)
        if len(self.closed_campaigns) > 50:
            self.closed_campaigns = self.closed_campaigns[-50:]
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

        # 1) Ingest state changes for tracked entry orders, on every fib that
        #    still has one — older ladders stay live alongside the newest.
        for leg in campaign.legs:
            for order in leg.pending_orders.values():
                if order.order_id is None:
                    continue
                row = open_orders.get(str(order.order_id))
                if row is not None:
                    executed = _coerce_float(row.get("executedQty"))
                    if executed > order.filled_qty + 1e-12:
                        delta_qty = executed - order.filled_qty
                        avg_price = _coerce_float(row.get("price"), order.working_price or order.price or 0.0)
                        self._record_fill(
                            campaign, leg, order, avg_price, int(time.time()), order.order_id, quantity=delta_qty
                        )
                        changed = True
                    continue
                status_row = await self._safe_get_order(campaign, order.order_id)
                status = str(status_row.get("status") or "").upper()
                if status == "FILLED":
                    executed = _coerce_float(status_row.get("executedQty"), order.quantity)
                    quote = _coerce_float(status_row.get("cummulativeQuoteQty"))
                    avg_price = (
                        quote / executed if executed > 0 and quote > 0 else (order.working_price or order.price or 0.0)
                    )
                    delta_qty = max(executed - order.filled_qty, 0.0)
                    if delta_qty > 0:
                        self._record_fill(
                            campaign, leg, order, avg_price, int(time.time()), order.order_id, quantity=delta_qty
                        )
                    order.status = "FILLED"
                    changed = True
                elif status in {"CANCELED", "EXPIRED", "REJECTED"}:
                    if order.status == "PLACED":
                        order.status = "PENDING"  # externally cancelled: re-place below
                        order.order_id = None
                        order.rev += 1
                        order.client_order_id = (
                            f"cf-csc-{campaign.campaign_id}-{order.leg_id}-{order.level}-{order.rev}"
                        )
                        self._log_event(
                            campaign, "warn", f"Entry order L{order.level} was cancelled on exchange; re-placing"
                        )
                        changed = True

        # 2) Cancel stale placed orders (repriced) and place PENDING orders.
        if campaign.state in ACTIVE_STATES:
            # Every fib's ids count as known. Without that, an older ladder's
            # perfectly good resting orders look like strays and get cancelled.
            known_ids = {str(o.order_id) for lg in campaign.legs for o in lg.pending_orders.values() if o.order_id}
            if campaign.tp_order_id:
                known_ids.add(str(campaign.tp_order_id))
            for order_id, row in open_orders.items():
                client_id = str(row.get("clientOrderId") or "")
                if client_id.startswith(f"cf-csc-{campaign.campaign_id}-") and order_id not in known_ids:
                    await self._safe_cancel(campaign, order_id)
                    changed = True
            for leg in campaign.legs:
                for order in leg.pending_orders.values():
                    if await self._place_entry_order(campaign, order):
                        changed = True

        # 3) TP management.
        changed |= await self._sync_tp_order(campaign, open_orders)
        return changed

    async def _place_entry_order(self, campaign: Campaign, order: PendingOrder) -> bool:
        """Rest one PENDING entry on the exchange. Returns True if state moved."""
        if order.status != "PENDING" or not order.price or order.usd_notional <= 0:
            return False
        price = order.working_price
        if not price:
            return False  # stop entry not armed yet — nothing to rest on the exchange
        is_stop = order.entry_style == "stop"
        if is_stop and not self._stop_is_placeable(campaign, order):
            return False
        try:
            result = await asyncio.to_thread(
                lambda o=order, p=price, st=is_stop: self.broker.place_order(
                    campaign.symbol,
                    o.usd_notional,
                    "buy",
                    order_type="stop_limit" if st else "limit_order",
                    limit_price=p,
                    stop_price=o.stop_price if st else None,
                    client_order_id=o.client_order_id,
                )
            )
        except Exception as exc:
            result = {"error": str(exc)}
        if isinstance(result, dict) and not result.get("error"):
            order.order_id = str(result.get("orderId") or result.get("id") or "")
            order.status = "PLACED"
            self._log_event(
                campaign,
                "order",
                (
                    f"Fib {order.leg_id} L{order.level} buy stop placed ${order.usd_notional:g} "
                    f"trigger {order.stop_price:,.2f} / limit {price:,.2f} (fib line {order.price:,.2f})"
                    if is_stop
                    else f"Fib {order.leg_id} L{order.level} limit buy placed ${order.usd_notional:g} @ {price:,.2f}"
                ),
            )
            return True
        error = (result or {}).get("error") if isinstance(result, dict) else "unknown error"
        if await self._recover_order_by_client_id(campaign, order):
            return True
        self._log_event(campaign, "error", f"Failed to place fib {order.leg_id} L{order.level} buy: {error}")
        self._alert(
            "Cascade order FAILED",
            f"{campaign.symbol} campaign #{campaign.seq} (LIVE)\n"
            f"Fib {order.leg_id} level {order.level}, ${order.usd_notional:,.2f} at {price:,.2f}\n"
            f"Binance said: {error}\n\n"
            f"The level is unarmed until this succeeds.",
            level="error",
            dedupe_sec=300,
        )
        return False

    async def _sync_tp_order(self, campaign: Campaign, open_orders: Dict[str, dict]) -> bool:
        changed = False
        if campaign.tp_order_id and str(campaign.tp_order_id) not in open_orders:
            status_row = await self._safe_get_order(campaign, campaign.tp_order_id)
            status = str(status_row.get("status") or "").upper()
            if status == "FILLED":
                executed = _coerce_float(status_row.get("executedQty"), campaign.filled_base_qty)
                quote = _coerce_float(status_row.get("cummulativeQuoteQty"))
                exit_price = quote / executed if executed > 0 and quote > 0 else (campaign.tp_price or 0.0)
                # Entry buys that never filled stay resting — the campaign is
                # still live and price can come back down to them.
                self._close_round(campaign, exit_price)
                return True
            campaign.tp_order_id = None
            changed = True

        if campaign.state not in ACTIVE_STATES or campaign.filled_base_qty <= 0:
            return changed
        desired_tp = compute_tp_price(campaign)
        if not desired_tp:
            return changed
        current_price_ok = campaign.tp_price and abs((campaign.tp_price or 0.0) - desired_tp) < 1e-9
        if campaign.tp_order_id and current_price_ok:
            return changed
        if campaign.tp_order_id:
            await self._safe_cancel(campaign, campaign.tp_order_id)
            campaign.tp_order_id = None
        campaign.tp_rev += 1
        try:
            result = await asyncio.to_thread(
                lambda: self.broker.place_order(
                    campaign.symbol,
                    0.0,
                    "sell",
                    order_type="limit_order",
                    limit_price=desired_tp,
                    client_order_id=f"cf-csc-{campaign.campaign_id}-tp-{campaign.tp_rev}",
                    base_qty=campaign.filled_base_qty,
                )
            )
        except Exception as exc:
            result = {"error": str(exc)}
        if isinstance(result, dict) and not result.get("error"):
            campaign.tp_order_id = str(result.get("orderId") or result.get("id") or "")
            campaign.tp_price = desired_tp
            self._log_event(
                campaign,
                "order",
                f"TP limit sell placed: {campaign.filled_base_qty:.8f} @ {desired_tp:,.2f}",
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

    async def _safe_cancel(self, campaign: Campaign, order_id) -> None:
        try:
            await asyncio.to_thread(self.broker.cancel_order, order_id, campaign.symbol)
        except Exception as exc:
            _log.warning("[CASCADE] cancel failed for %s: %s", order_id, exc)

    async def _cancel_all_live_orders(self, campaign: Campaign, include_tp: bool) -> None:
        for leg in campaign.legs:
            for order in leg.pending_orders.values():
                if order.status == "PLACED" and order.order_id:
                    await self._safe_cancel(campaign, order.order_id)
                if order.is_open:
                    order.status = "CANCELLED"
        if include_tp and campaign.tp_order_id:
            await self._safe_cancel(campaign, campaign.tp_order_id)
            campaign.tp_order_id = None
