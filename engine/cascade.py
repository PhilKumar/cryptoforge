"""
engine/cascade.py — autonomous "cascade" campaign engine.

Model (ported from the user's Automation_Trade cascade_lib v2):
- A campaign anchors on a manually chosen MOTHER CANDLE. Down-trendlines are
  drawn with anchor1 = always the mother high; anchor2 = a valid red candle
  open (find_valid_anchor2).
- A TOUCH of the active trendline (high crosses the line, close stays below,
  high < mother high) spins a LEG with a fib ladder anchored touch_high →
  running-min low. Resting LIMIT BUY orders are placed at fib levels 2/4/8
  with 20/30/50% of the leg's pool (incremental depth pct × capital/100).
- A BREAK (close above the line) arms pending_break; a later decisive
  low-break (red close below the last leg's low) creates a NEW trendline
  anchored back to the mother high, which needs its own future touch.
- Take profit: a resting LIMIT SELL for the whole filled position at
  mother_high − 0.25 × (mother_high − avg_entry), re-placed whenever a new
  fill moves the average.
- Binance min-notional handling: per-level USD below the minimum merges into
  the next deeper level (2→4→8); if the whole pool is below the minimum it
  carries forward to the next leg.

Campaigns default to paper mode (simulated fills at live prices). Live mode
uses a desired-state sync: the state machine only mutates local order intents
and _sync_live_orders diffs them against the exchange's open orders, placing,
cancelling, and ingesting fills idempotently (client ids cf-csc-{...}).
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
FIRST_DEPTH_MIN_PCT = 0.5  # first trendline needs at least this fall pct from the mother high
MIN_NOTIONAL_FLOOR_USD = 5.0  # Binance Spot MIN_NOTIONAL filter is ~$5 on USDT pairs
FIVE_MIN_SEC = 300
FIFTEEN_MIN_SEC = 900

ACTIVE_STATES = {"WAITING_FIRST_DEPTH", "TRENDLINE_ACTIVE"}
FINAL_STATES = {"COMPLETED", "MOTHER_BROKEN", "STOPPED"}


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
        )


def trendline_price(tl: Trendline, at_timestamp: int) -> float:
    x1, y1 = tl.anchor1_timestamp, tl.anchor1_price
    x2, y2 = tl.anchor2_timestamp, tl.anchor2_price
    if x2 == x1:
        return y1
    slope = (y2 - y1) / (x2 - x1)
    return y1 + slope * (at_timestamp - x1)


def classify_candle(mother_high: float, tl: Trendline, candle: Candle) -> str:
    """Returns 'BREAK', 'TOUCH', or 'NONE'."""
    line = trendline_price(tl, candle.timestamp)
    if candle.close > line:
        return "BREAK"
    if candle.high >= line and candle.close < line and candle.high < mother_high:
        return "TOUCH"
    return "NONE"


def leg_broken(candle: Candle, current_low: float) -> bool:
    """Decisive break: a red candle whose CLOSE is below the reference low."""
    return candle.is_red and candle.close < current_low


def find_valid_anchor2(anchor1_price, anchor1_ts, candles_between, epsilon=1e-9):
    """
    Search backward from the red candle closest to the depth toward anchor1,
    returning the first candidate whose connecting line is not crossed by any
    earlier candle's CLOSE.
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
    status: str = "PENDING"  # PENDING | PLACED | FILLED | CANCELLED | MERGED | CARRIED
    rev: int = 0
    order_id: Optional[str] = None
    client_order_id: str = ""
    filled_qty: float = 0.0
    fill_price: Optional[float] = None
    fill_timestamp: Optional[int] = None

    @property
    def is_open(self) -> bool:
        return self.status in {"PENDING", "PLACED"}

    def to_dict(self) -> dict:
        return dict(self.__dict__)

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
class Leg:
    leg_id: int
    trendline_id: int
    low: float  # deepest low since the previous leg finalized
    touch_high: float  # running-max swing high → fib level 0
    touch_timestamp: int
    created_via_break: bool = False
    fib: Optional[FibLadder] = None
    leg_pct_from_mother: Optional[float] = None
    pool_usd: Optional[float] = None
    escalated: bool = False
    finalized: bool = False  # swing complete (low broke again)
    pending_orders: Dict[int, PendingOrder] = field(default_factory=dict)
    carry_forward_qty: Dict[int, float] = field(default_factory=dict)

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
            "pool_usd": self.pool_usd,
            "escalated": self.escalated,
            "finalized": self.finalized,
            "pending_orders": {str(level): order.to_dict() for level, order in self.pending_orders.items()},
            "carry_forward_qty": {str(level): qty for level, qty in self.carry_forward_qty.items()},
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
        leg.pool_usd = data.get("pool_usd")
        leg.escalated = bool(data.get("escalated"))
        leg.finalized = bool(data.get("finalized"))
        for level, order in (data.get("pending_orders") or {}).items():
            leg.pending_orders[int(level)] = PendingOrder.from_dict(order)
        for level, qty in (data.get("carry_forward_qty") or {}).items():
            leg.carry_forward_qty[int(level)] = _coerce_float(qty)
        return leg


@dataclass
class Campaign:
    campaign_id: str
    symbol: str
    capital_usd: float
    mother_high: float
    mother_low: float
    mother_timestamp: int
    mode: str = "paper"  # paper | live
    min_notional_usd: float = MIN_NOTIONAL_FLOOR_USD
    created_at: str = ""
    state: str = "WAITING_FIRST_DEPTH"
    cumulative_used_pct: float = 0.0
    carry_forward_usd: float = 0.0
    trendlines: List[Trendline] = field(default_factory=list)
    legs: List[Leg] = field(default_factory=list)
    active_trendline_id: Optional[int] = None
    pending_break: bool = False
    all_fills: List[Fill] = field(default_factory=list)
    avg_entry_price: Optional[float] = None
    tp_price: Optional[float] = None  # active TP once fills exist; display estimate before
    tp_order_id: Optional[str] = None
    tp_rev: int = 0
    tp_filled: bool = False
    filled_base_qty: float = 0.0
    realized_pnl: Optional[float] = None
    mother_broken_above: bool = False
    swing_tracking: bool = False
    depth_low: Optional[float] = None
    depth_low_timestamp: Optional[int] = None
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
        return sum(f.price * f.quantity for f in self.all_fills)

    @property
    def resting_usd(self) -> float:
        leg = self.current_leg
        if not leg:
            return 0.0
        return sum(o.usd_notional for o in leg.pending_orders.values() if o.is_open)

    def to_dict(self) -> dict:
        return {
            "campaign_id": self.campaign_id,
            "symbol": self.symbol,
            "capital_usd": self.capital_usd,
            "mother_high": self.mother_high,
            "mother_low": self.mother_low,
            "mother_timestamp": self.mother_timestamp,
            "mode": self.mode,
            "min_notional_usd": self.min_notional_usd,
            "created_at": self.created_at,
            "state": self.state,
            "cumulative_used_pct": self.cumulative_used_pct,
            "carry_forward_usd": self.carry_forward_usd,
            "trendlines": [tl.to_dict() for tl in self.trendlines],
            "legs": [leg.to_dict() for leg in self.legs],
            "active_trendline_id": self.active_trendline_id,
            "pending_break": self.pending_break,
            "all_fills": [f.to_dict() for f in self.all_fills],
            "avg_entry_price": self.avg_entry_price,
            "tp_price": self.tp_price,
            "tp_order_id": self.tp_order_id,
            "tp_rev": self.tp_rev,
            "tp_filled": self.tp_filled,
            "filled_base_qty": self.filled_base_qty,
            "realized_pnl": self.realized_pnl,
            "mother_broken_above": self.mother_broken_above,
            "swing_tracking": self.swing_tracking,
            "depth_low": self.depth_low,
            "depth_low_timestamp": self.depth_low_timestamp,
            "last_processed_ts": self.last_processed_ts,
            "closed_at": self.closed_at,
            "close_reason": self.close_reason,
            "event_log": list(self.event_log[-200:]),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Campaign":
        campaign = cls(
            campaign_id=str(data.get("campaign_id") or uuid.uuid4().hex[:10]),
            symbol=str(data.get("symbol") or "BTCUSDT"),
            capital_usd=_coerce_float(data.get("capital_usd"), 2000.0),
            mother_high=_coerce_float(data.get("mother_high")),
            mother_low=_coerce_float(data.get("mother_low")),
            mother_timestamp=int(data.get("mother_timestamp", 0)),
        )
        for key in (
            "mode",
            "min_notional_usd",
            "created_at",
            "state",
            "cumulative_used_pct",
            "carry_forward_usd",
            "active_trendline_id",
            "pending_break",
            "avg_entry_price",
            "tp_price",
            "tp_order_id",
            "tp_rev",
            "tp_filled",
            "filled_base_qty",
            "realized_pnl",
            "mother_broken_above",
            "swing_tracking",
            "depth_low",
            "depth_low_timestamp",
            "last_processed_ts",
            "closed_at",
            "close_reason",
        ):
            if key in data:
                setattr(campaign, key, data[key])
        campaign.trendlines = [Trendline.from_dict(tl) for tl in data.get("trendlines") or []]
        campaign.legs = [Leg.from_dict(leg) for leg in data.get("legs") or []]
        campaign.all_fills = [Fill.from_dict(f) for f in data.get("all_fills") or []]
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
    Active TP once fills exist: fib 0.25 anchored mother high → average entry.
    Before any fill: display estimate anchored mother high → leg1 low.
    """
    if campaign.avg_entry_price and campaign.avg_entry_price > 0:
        return campaign.mother_high - TP_FIB_LEVEL * (campaign.mother_high - campaign.avg_entry_price)
    if campaign.legs:
        first = campaign.legs[0]
        return campaign.mother_high - TP_FIB_LEVEL * (campaign.mother_high - first.low)
    return None


def build_fib_ladder_and_pool(campaign: Campaign, leg: Leg) -> None:
    if leg.touch_high >= campaign.mother_high:
        raise CascadeModelError(
            f"leg {leg.leg_id}: touch_high {leg.touch_high} must stay below mother high {campaign.mother_high}"
        )
    if leg.touch_high <= leg.low:
        raise CascadeModelError(f"leg {leg.leg_id}: touch_high {leg.touch_high} must exceed leg low {leg.low}")
    leg.fib = FibLadder(high_anchor=leg.touch_high, low_anchor=leg.low)

    leg.leg_pct_from_mother = (campaign.mother_high - leg.low) / campaign.mother_high * 100
    touch_pct_from_mother = (campaign.mother_high - leg.touch_high) / campaign.mother_high * 100

    incremental_pct = max(leg.leg_pct_from_mother - campaign.cumulative_used_pct, 0.0)
    leg.pool_usd = incremental_pct * campaign.capital_unit_per_pct
    campaign.cumulative_used_pct = max(campaign.cumulative_used_pct, leg.leg_pct_from_mother)
    leg.escalated = touch_pct_from_mother > ESCALATION_THRESHOLD_PCT


def cancel_and_carry_forward(prior_leg: Leg, next_leg: Leg) -> None:
    """Mark prior leg's unfilled orders CARRIED; roll their qty into next_leg."""
    for level, order in prior_leg.pending_orders.items():
        if order.is_open:
            order.status = "CARRIED"
            next_leg.carry_forward_qty[level] = next_leg.carry_forward_qty.get(level, 0.0) + max(
                order.quantity - order.filled_qty, 0.0
            )


def plan_leg_orders(campaign: Campaign, leg: Leg) -> None:
    """
    Split the leg pool 20/30/50 across levels 2/4/8, then apply the Binance
    min-notional merge: sub-minimum amounts roll into the next deeper level;
    a whole pool below minimum carries forward to the next leg. Spend is
    capped so filled + resting notional never exceeds campaign capital.
    """
    if leg.fib is None:
        raise CascadeModelError(f"leg {leg.leg_id}: fib ladder must be built before planning orders")
    min_notional = max(_coerce_float(campaign.min_notional_usd, MIN_NOTIONAL_FLOOR_USD), MIN_NOTIONAL_FLOOR_USD)
    pool = max(_coerce_float(leg.pool_usd), 0.0) + max(_coerce_float(campaign.carry_forward_usd), 0.0)
    campaign.carry_forward_usd = 0.0

    usd = {level: pool * LEVEL_ALLOCATION[level] for level in CASCADE_LEVELS}
    # Value carried-forward quantity from the prior leg at the new level prices.
    for level in CASCADE_LEVELS:
        carried_qty = leg.carry_forward_qty.get(level, 0.0)
        if carried_qty > 0:
            usd[level] += carried_qty * max(leg.fib.level_price(level), 0.0)

    merged = set()
    carried = False
    if usd[2] < min_notional:
        usd[4] += usd[2]
        usd[2] = 0.0
        merged.add(2)
    if usd[4] < min_notional:
        usd[8] += usd[4]
        usd[4] = 0.0
        merged.add(4)
    if usd[8] < min_notional:
        campaign.carry_forward_usd = usd[8]
        usd[8] = 0.0
        carried = True

    # Capital cap: filled + this ladder must never exceed campaign capital.
    available = max(campaign.capital_usd - campaign.spent_usd, 0.0)
    total = sum(usd.values())
    if total > available:
        overshoot = total - available
        for level in reversed(CASCADE_LEVELS):  # trim deepest first
            if overshoot <= 0:
                break
            trim = min(usd[level], overshoot)
            usd[level] -= trim
            overshoot -= trim
        for level in CASCADE_LEVELS:
            if 0 < usd[level] < min_notional:
                usd[level] = 0.0

    leg.pending_orders = {}
    for level in CASCADE_LEVELS:
        price = max(leg.fib.level_price(level), 0.0)
        amount = usd[level]
        if amount <= 0 or price <= 0:
            status = "MERGED" if level in merged else ("CARRIED" if carried and level == 8 else "CANCELLED")
            leg.pending_orders[level] = PendingOrder(
                level=level,
                price=price or None,
                usd_notional=0.0,
                quantity=0.0,
                leg_id=leg.leg_id,
                timeframe=timeframe_for_level(leg, level),
                status=status,
            )
            continue
        leg.pending_orders[level] = PendingOrder(
            level=level,
            price=price,
            usd_notional=round(amount, 2),
            quantity=amount / price,
            leg_id=leg.leg_id,
            timeframe=timeframe_for_level(leg, level),
            status="PENDING",
            client_order_id=f"cf-csc-{campaign.campaign_id}-{leg.leg_id}-{level}-0",
        )


def reprice_leg_orders(campaign: Campaign, leg: Leg) -> bool:
    """
    Swing high rose: rebuild the fib and move still-open orders to the new
    (deeper) level prices. Filled/merged/carried orders are untouched.
    Returns True if any open order moved.
    """
    if leg.fib is None:
        return False
    leg.fib = FibLadder(high_anchor=leg.touch_high, low_anchor=leg.low)
    moved = False
    for level, order in leg.pending_orders.items():
        if not order.is_open:
            continue
        new_price = max(leg.fib.level_price(level), 0.0)
        if new_price <= 0 or order.price is None or abs(new_price - order.price) < 1e-12:
            continue
        order.price = new_price
        order.quantity = order.usd_notional / new_price if new_price > 0 else 0.0
        order.rev += 1
        order.client_order_id = f"cf-csc-{campaign.campaign_id}-{leg.leg_id}-{level}-{order.rev}"
        if order.status == "PLACED":
            order.status = "PENDING"  # sync will cancel the old order and place the new one
        moved = True
    return moved


# ── Engine ──────────────────────────────────────────────────────────


class CascadeEngine:
    def __init__(
        self,
        broker,
        on_campaign_closed: Optional[Callable] = None,
        on_event: Optional[Callable] = None,
        on_update: Optional[Callable] = None,
    ):
        self.broker = broker
        self.on_campaign_closed = on_campaign_closed
        self.on_event = on_event
        self.on_update = on_update
        self.campaigns: Dict[str, Campaign] = {}
        self.closed_campaigns: List[dict] = []
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._candles_5m: Dict[str, List[Candle]] = {}  # per-campaign candle history (rebuilt on restart)
        self._price_cache: Dict[str, tuple] = {}
        self._last_sync_ts = 0.0
        self._loop_interval_sec = 5.0
        self._sync_interval_sec = 30.0

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

        campaign = Campaign(
            campaign_id=uuid.uuid4().hex[:10],
            symbol=symbol,
            capital_usd=capital_usd,
            mother_high=mother_high,
            mother_low=mother_low,
            mother_timestamp=mother_ts,
            mode=mode,
            min_notional_usd=min_notional,
            created_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            last_processed_ts=mother_ts,
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

    def delete_campaign(self, campaign_id: str) -> dict:
        campaign = self.campaigns.pop(campaign_id, None)
        if campaign is None:
            return {"error": f"Campaign {campaign_id} not found"}
        self._candles_5m.pop(campaign_id, None)
        self._emit_update()
        return {"status": "ok"}

    def get_status(self) -> dict:
        campaigns = []
        for campaign in self.campaigns.values():
            payload = campaign.to_dict()
            payload["display_tp_price"] = compute_tp_price(campaign)
            payload["spent_usd"] = round(campaign.spent_usd, 2)
            payload["resting_usd"] = round(campaign.resting_usd, 2)
            price_meta = self._price_cache.get(campaign.symbol)
            payload["last_price"] = price_meta[0] if price_meta else None
            campaigns.append(payload)
        return {
            "status": "ok",
            "running": self._running,
            "campaigns": campaigns,
            "closed_campaigns": list(self.closed_campaigns[-20:]),
            "active_count": len(self.active_campaigns),
            "live_count": len(self.live_campaigns),
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    async def get_chart_data(self, campaign_id: str, max_candles: int = 300) -> dict:
        """
        Candles plus the geometry the engine actually used — trendline anchors,
        each leg's fib anchors/levels, ladder order prices and fills — so the
        marked levels can be verified visually against a real chart.
        """
        campaign = self.campaigns.get(campaign_id)
        if campaign is None:
            return {"error": f"Campaign {campaign_id} not found"}

        history = self._candles_5m.get(campaign_id) or []
        if not history:
            # Engine restarted (candle history is in-memory only) — refetch.
            history = await self._fetch_closed_5m(campaign.symbol, campaign.mother_timestamp)
            if history:
                self._candles_5m[campaign_id] = history

        candles = [
            {"t": c.timestamp, "o": c.open, "h": c.high, "l": c.low, "c": c.close} for c in history[-max_candles:]
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
                    "levels": levels,
                    "orders": [
                        {
                            "level": order.level,
                            "price": order.price,
                            "usd_notional": order.usd_notional,
                            "status": order.status,
                            "fill_price": order.fill_price,
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
        return restored

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
            except asyncio.CancelledError:
                return
            except Exception as exc:
                _log.warning("[CASCADE] monitor loop error: %s", exc)
            await asyncio.sleep(self._loop_interval_sec)

    async def _campaign_tick(self, campaign: Campaign) -> bool:
        changed = False
        # New closed candles drive the state machine.
        changed |= await self._candle_step(campaign)
        # Keep the live price fresh for the UI (Last Price) and paper TP checks.
        had_price = campaign.symbol in self._price_cache
        price = await self._get_price(campaign.symbol)
        if not had_price and price:
            changed = True  # surface the first price so the status card fills in
        # Paper TP check against the live price.
        if campaign.mode == "paper" and campaign.state in ACTIVE_STATES and campaign.filled_base_qty > 0:
            tp = compute_tp_price(campaign)
            if price and tp and price >= tp:
                self._complete_campaign(campaign, tp)
                changed = True
        # Live order sync (throttled).
        now = time.monotonic()
        if campaign.mode == "live" and now - self._last_sync_ts >= self._sync_interval_sec:
            self._last_sync_ts = now
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
        new_candles = await self._fetch_closed_5m(campaign.symbol, campaign.last_processed_ts)
        if not new_candles:
            return False
        history = self._candles_5m.setdefault(campaign.campaign_id, [])
        changed = False
        for candle in new_candles:
            history.append(candle)
            if len(history) > 20000:
                del history[: len(history) - 20000]
            self._process_candle(campaign, candle)
            campaign.last_processed_ts = candle.timestamp
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

    def _process_candle(self, campaign: Campaign, candle: Candle) -> None:
        # Mother-high breach ends the hunt in every state.
        if candle.high >= campaign.mother_high:
            self._mother_broken(campaign)
            return

        if campaign.state == "WAITING_FIRST_DEPTH":
            self._process_waiting_first_depth(campaign, candle)
        elif campaign.state == "TRENDLINE_ACTIVE":
            self._process_trendline_active(campaign, candle)

        # Paper fill checks run after ladder management so freshly repriced
        # orders are honored. Levels on 15m confirm only on closed 15m candles.
        if campaign.mode == "paper" and campaign.state in ACTIVE_STATES:
            self._paper_fill_check(campaign, candle)

    def _process_waiting_first_depth(self, campaign: Campaign, candle: Candle) -> None:
        if campaign.depth_low is None or candle.low < campaign.depth_low:
            campaign.depth_low = candle.low
            campaign.depth_low_timestamp = candle.timestamp

        depth_pct = (campaign.mother_high - (campaign.depth_low or campaign.mother_high)) / campaign.mother_high * 100
        if depth_pct < FIRST_DEPTH_MIN_PCT:
            return
        between = self._candles_between(campaign, campaign.depth_low_timestamp or candle.timestamp)
        anchor2_price, anchor2_ts = find_valid_anchor2(campaign.mother_high, campaign.mother_timestamp, between)
        if anchor2_price is None:
            return
        tl = Trendline(
            trendline_id=len(campaign.trendlines) + 1,
            anchor1_price=campaign.mother_high,
            anchor1_timestamp=campaign.mother_timestamp,
            anchor2_price=anchor2_price,
            anchor2_timestamp=anchor2_ts,
        )
        campaign.trendlines.append(tl)
        campaign.active_trendline_id = tl.trendline_id
        campaign.state = "TRENDLINE_ACTIVE"
        self._log_event(
            campaign,
            "trendline",
            f"Trendline {tl.trendline_id} drawn (mother high {campaign.mother_high:g} → "
            f"red open {anchor2_price:g}); depth {campaign.depth_low:g}",
        )

    def _process_trendline_active(self, campaign: Campaign, candle: Candle) -> None:
        tl = campaign.active_trendline
        if tl is None:
            return
        leg = campaign.current_leg

        if campaign.swing_tracking and leg is not None and not leg.finalized:
            # The up-swing continues (even through a break) until the low breaks.
            if candle.high > leg.touch_high:
                leg.touch_high = candle.high
                leg.touch_timestamp = candle.timestamp
                if reprice_leg_orders(campaign, leg):
                    self._log_event(
                        campaign,
                        "reprice",
                        f"Leg {leg.leg_id} swing high rose to {leg.touch_high:g}; ladder repriced",
                    )
            if leg_broken(candle, leg.low):
                leg.finalized = True
                campaign.swing_tracking = False
                campaign.depth_low = candle.low
                campaign.depth_low_timestamp = candle.timestamp
                self._log_event(
                    campaign,
                    "leg",
                    f"Leg {leg.leg_id} swing finalized (touch high {leg.touch_high:g}); new depth window open",
                )
                if campaign.pending_break:
                    self._create_trendline_from_break(campaign, candle)
                return
            classification = classify_candle(campaign.mother_high, tl, candle)
            if classification == "BREAK":
                if not campaign.pending_break:
                    campaign.pending_break = True
                    self._log_event(
                        campaign, "break", f"Trendline {tl.trendline_id} broken (close above line); awaiting low-break"
                    )
            return

        # Not swing-tracking: maintain the running depth low for the next leg.
        if campaign.depth_low is None or candle.low < campaign.depth_low:
            campaign.depth_low = candle.low
            campaign.depth_low_timestamp = candle.timestamp
            # Until the first trendline is touched, its anchor2 tracks the red
            # candle before the (still deepening) first depth.
            if not campaign.legs and tl.trendline_id == 1:
                between = self._candles_between(campaign, campaign.depth_low_timestamp)
                anchor2_price, anchor2_ts = find_valid_anchor2(campaign.mother_high, campaign.mother_timestamp, between)
                if anchor2_price is not None and anchor2_ts != tl.anchor2_timestamp:
                    tl.anchor2_price = anchor2_price
                    tl.anchor2_timestamp = anchor2_ts

        if campaign.pending_break and leg is not None and leg_broken(candle, leg.low):
            self._create_trendline_from_break(campaign, candle)
            return

        classification = classify_candle(campaign.mother_high, tl, candle)
        if classification == "BREAK":
            if not campaign.pending_break:
                campaign.pending_break = True
                self._log_event(
                    campaign, "break", f"Trendline {tl.trendline_id} broken (close above line); awaiting low-break"
                )
            return
        if classification != "TOUCH":
            return
        if campaign.depth_low is None or campaign.depth_low >= candle.high:
            return

        prior_leg = campaign.current_leg
        new_leg = Leg(
            leg_id=len(campaign.legs) + 1,
            trendline_id=tl.trendline_id,
            low=campaign.depth_low,
            touch_high=candle.high,
            touch_timestamp=candle.timestamp,
        )
        campaign.legs.append(new_leg)
        campaign.pending_break = False
        campaign.swing_tracking = True
        if prior_leg is not None:
            cancel_and_carry_forward(prior_leg, new_leg)
        try:
            build_fib_ladder_and_pool(campaign, new_leg)
            plan_leg_orders(campaign, new_leg)
        except CascadeModelError as exc:
            campaign.legs.pop()
            campaign.swing_tracking = False
            self._log_event(campaign, "error", f"Leg rejected: {exc}")
            return
        placed = [order for order in new_leg.pending_orders.values() if order.status == "PENDING"]
        self._log_event(
            campaign,
            "leg",
            f"Leg {new_leg.leg_id} touch at {new_leg.touch_high:g} (low {new_leg.low:g}, "
            f"pool ${new_leg.pool_usd:,.2f}{', escalated' if new_leg.escalated else ''}) — "
            + (
                "orders: " + ", ".join(f"L{o.level} ${o.usd_notional:g} @ {o.price:,.2f}" for o in placed)
                if placed
                else f"pool below minimum, ${campaign.carry_forward_usd:,.2f} carried forward"
            ),
        )

    def _create_trendline_from_break(self, campaign: Campaign, low_break_candle: Candle) -> None:
        between = self._candles_between(campaign, low_break_candle.timestamp)
        anchor2_price, anchor2_ts = find_valid_anchor2(campaign.mother_high, campaign.mother_timestamp, between)
        campaign.pending_break = False
        if anchor2_price is None:
            self._log_event(campaign, "warn", "Low-break after trendline break, but no valid anchor2 found yet")
            return
        tl = Trendline(
            trendline_id=len(campaign.trendlines) + 1,
            anchor1_price=campaign.mother_high,
            anchor1_timestamp=campaign.mother_timestamp,
            anchor2_price=anchor2_price,
            anchor2_timestamp=anchor2_ts,
        )
        campaign.trendlines.append(tl)
        campaign.active_trendline_id = tl.trendline_id
        campaign.depth_low = low_break_candle.low
        campaign.depth_low_timestamp = low_break_candle.timestamp
        self._log_event(
            campaign,
            "trendline",
            f"Trendline {tl.trendline_id} created after break + low-break (anchor2 {anchor2_price:g}); "
            "waiting for its own touch",
        )

    # ── fills / TP ───────────────────────────────────────────────

    def _paper_fill_check(self, campaign: Campaign, closed_5m: Candle) -> None:
        leg = campaign.current_leg
        if leg is None:
            return
        candle_15m = self._fifteen_minute_candle(campaign, closed_5m)
        for order in leg.pending_orders.values():
            if not order.is_open or not order.price:
                continue
            probe = closed_5m if order.timeframe == BASE_TIMEFRAME else candle_15m
            if probe is None:
                continue
            if probe.low <= order.price:
                self._record_fill(campaign, leg, order, order.price, probe.timestamp, order_id="PAPER")

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

    def _complete_campaign(self, campaign: Campaign, exit_price: float) -> None:
        qty = campaign.filled_base_qty
        avg = campaign.avg_entry_price or 0.0
        campaign.realized_pnl = round((exit_price - avg) * qty, 8)
        campaign.tp_filled = True
        campaign.state = "COMPLETED"
        campaign.close_reason = "tp_filled"
        campaign.closed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._log_event(
            campaign,
            "complete",
            f"TP filled at {exit_price:,.2f} — sold {qty:.8f} (avg entry {avg:,.2f}), "
            f"PnL ${campaign.realized_pnl:,.2f}",
        )
        self._archive_campaign(campaign)

    def _mother_broken(self, campaign: Campaign) -> None:
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
            asyncio.ensure_future(self._cancel_all_live_orders(campaign, include_tp=False))
        elif campaign.filled_base_qty > 0:
            # Paper: price at/above mother high is at/above TP by construction.
            tp = compute_tp_price(campaign)
            if tp:
                self._complete_campaign(campaign, tp)
                return
        self._archive_campaign(campaign)

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
        leg = campaign.current_leg

        # 1) Ingest state changes for tracked entry orders.
        if leg is not None:
            for order in leg.pending_orders.values():
                if order.order_id is None:
                    continue
                row = open_orders.get(str(order.order_id))
                if row is not None:
                    executed = _coerce_float(row.get("executedQty"))
                    if executed > order.filled_qty + 1e-12:
                        delta_qty = executed - order.filled_qty
                        avg_price = _coerce_float(row.get("price"), order.price or 0.0)
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
                    avg_price = quote / executed if executed > 0 and quote > 0 else (order.price or 0.0)
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
        if leg is not None and campaign.state in ACTIVE_STATES:
            known_ids = {str(o.order_id) for o in leg.pending_orders.values() if o.order_id}
            if campaign.tp_order_id:
                known_ids.add(str(campaign.tp_order_id))
            for order_id, row in open_orders.items():
                client_id = str(row.get("clientOrderId") or "")
                if client_id.startswith(f"cf-csc-{campaign.campaign_id}-") and order_id not in known_ids:
                    await self._safe_cancel(campaign, order_id)
                    changed = True
            for order in leg.pending_orders.values():
                if order.status != "PENDING" or not order.price or order.usd_notional <= 0:
                    continue
                try:
                    result = await asyncio.to_thread(
                        lambda o=order: self.broker.place_order(
                            campaign.symbol,
                            o.usd_notional,
                            "buy",
                            order_type="limit_order",
                            limit_price=o.price,
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
                        f"Placed L{order.level} limit buy ${order.usd_notional:g} @ {order.price:,.2f}",
                    )
                    changed = True
                else:
                    error = (result or {}).get("error") if isinstance(result, dict) else "unknown error"
                    recovered = await self._recover_order_by_client_id(campaign, order)
                    if recovered:
                        changed = True
                    else:
                        self._log_event(campaign, "error", f"Failed to place L{order.level} buy: {error}")

        # 3) TP management.
        changed |= await self._sync_tp_order(campaign, open_orders)
        return changed

    async def _sync_tp_order(self, campaign: Campaign, open_orders: Dict[str, dict]) -> bool:
        changed = False
        if campaign.tp_order_id and str(campaign.tp_order_id) not in open_orders:
            status_row = await self._safe_get_order(campaign, campaign.tp_order_id)
            status = str(status_row.get("status") or "").upper()
            if status == "FILLED":
                executed = _coerce_float(status_row.get("executedQty"), campaign.filled_base_qty)
                quote = _coerce_float(status_row.get("cummulativeQuoteQty"))
                exit_price = quote / executed if executed > 0 and quote > 0 else (campaign.tp_price or 0.0)
                await self._cancel_all_live_orders(campaign, include_tp=False)
                self._complete_campaign(campaign, exit_price)
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
        leg = campaign.current_leg
        if leg is not None:
            for order in leg.pending_orders.values():
                if order.status == "PLACED" and order.order_id:
                    await self._safe_cancel(campaign, order.order_id)
                if order.is_open:
                    order.status = "CANCELLED"
        if include_tp and campaign.tp_order_id:
            await self._safe_cancel(campaign, campaign.tp_order_id)
            campaign.tp_order_id = None
