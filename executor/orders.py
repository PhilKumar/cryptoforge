"""
executor/orders.py — what the buyer's machine actually places.

This is the only part of the executor with money on the other side of it, so
it is built as a decider, not a doer: it takes price, geometry and the buyer's
own fills, and returns a list of `OrderIntent`. Something thin turns those into
exchange calls. Nothing here has a network, which is what makes every rule
below testable without an account.

The shape of the strategy on this side:

- A level is not an order. It is a marker saying "this much money belongs to
  this price", and the market touching it puts that money in play. Crossed
  levels accumulate into ONE pot.
- The pot goes in as ONE working buy stop whose trigger is the previous red
  candle's close. While the market keeps falling each new red drags the stop
  down and nothing fills; only a turn back up through that last red body takes
  it. That is the cheapest confirmed entry the fall offered.
- Level 8 is the one worth owning at the line itself, so it rests as a plain
  limit rather than chasing.
- The exit is a fib 0.25 off the buyer's OWN average entry, replaced whenever
  that average moves.

Like the rest of `executor/`, this must not import `engine.cascade`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from executor import model

# ── the entry contract (pinned against the engine by test) ───────────

# How far above its trigger a buy stop may be re-armed. A quarter of the
# instrument's OWN median bar, not a flat percent: measured across markets, a
# flat 0.1% is 20 median bars on PAXG and 0.3 on ADA — a 65x spread in
# strictness, which is no filter at all on one and a straitjacket on the other.
MAX_STOP_RAISE_BAR_RATIO = 0.25
# Floored at a few ticks so a genuine tick-scale cross is never refused.
MAX_STOP_RAISE_FLOOR_TICKS = 3
# How far the limit cap sits above the trigger, in ticks, unless the symbol has
# its own gap. Wide enough to still fill on a quick turn, tight enough never to
# pay much over the trigger.
STOP_LIMIT_OFFSET_TICKS = 5
STOP_LIMIT_GAP_USD = {"SOLUSDT": 0.02}
DEFAULT_TICK_SIZE = 0.01


def _opt_float(value) -> Optional[float]:
    """None stays None. Every price on CampaignOrders means "not set" by absence,
    so coercing a missing one to 0.0 would read as a real level at zero."""
    return None if value is None else float(value)


def max_stop_raise_usd(trigger: float, median_bar_pct: float, tick_size: float) -> float:
    """
    The allowance, as an absolute distance rather than a percent so the caller
    compares against the trigger it actually holds.

    A `median_bar_pct` of 0 means it could not be measured, and then the tick
    floor is the whole allowance — the strict end, because a failed measurement
    must never widen a real filter.
    """
    tick = float(tick_size or DEFAULT_TICK_SIZE) or DEFAULT_TICK_SIZE
    floor = MAX_STOP_RAISE_FLOOR_TICKS * tick
    if trigger <= 0 or median_bar_pct <= 0:
        return floor
    return max(trigger * median_bar_pct * MAX_STOP_RAISE_BAR_RATIO, floor)


# ── intents ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class OrderIntent:
    """One thing to ask the exchange for. `client_order_id` makes it idempotent.

    A crash between deciding and placing is the ordinary case, not the exotic
    one — laptops close. Re-deriving the same id means recovery can ask the
    exchange "did this one land?" instead of guessing, and a duplicate place is
    rejected by the exchange rather than doubling the position.
    """

    action: str  # place | cancel
    kind: str  # entry | exit
    client_order_id: str
    side: str = "buy"
    order_type: str = "limit"  # limit | stop_limit
    price: Optional[float] = None
    stop_price: Optional[float] = None
    usd_notional: Optional[float] = None
    quantity: Optional[float] = None
    reason: str = ""


@dataclass
class Fill:
    price: float
    quantity: float
    timestamp: int
    client_order_id: str = ""


@dataclass
class Candle:
    timestamp: int
    open: float
    high: float
    low: float
    close: float

    @property
    def is_red(self) -> bool:
        return self.close < self.open


@dataclass
class CampaignOrders:
    """
    The buyer's order state for one campaign. One pot, one working entry, one
    exit — the same singular shape the engine has, which is what bounds the
    exposure of a machine that stops: at most one entry order of known size.
    """

    campaign_id: str
    symbol: str
    mother_high: float
    exchange: str = ""
    tick_size: float = DEFAULT_TICK_SIZE
    min_notional_usd: float = 5.0
    median_bar_pct: float = 0.0

    collected_levels: set = field(default_factory=set)
    pot_usd: float = 0.0
    pot_line: Optional[float] = None
    last_red: Optional[float] = None
    stop_price: Optional[float] = None
    limit_price: Optional[float] = None
    stop_ts: int = 0
    entry_rev: int = 0
    entry_resting: bool = False
    held_reason: str = ""

    fills: List[Fill] = field(default_factory=list)
    exit_resting: bool = False
    exit_price: Optional[float] = None
    exit_rev: int = 0

    # Their own floor, from their own round closes. Never from the feed: our
    # round and theirs close at different moments, and theirs may not close at
    # all, so our floor is not their floor.
    reuse_below: Optional[float] = None

    # What each finished round did, kept locally so the buyer's page can show
    # a history without asking the exchange. The P&L is NET of the venue's
    # headline commission and says "estimated" — their actual rate (VIP tier,
    # fee token) is theirs, and pretending to know it would be a false ledger.
    closed_rounds: List[dict] = field(default_factory=list)

    # ── position ─────────────────────────────────────────────────

    @property
    def base_qty(self) -> float:
        return round(sum(fill.quantity for fill in self.fills), 12)

    @property
    def avg_entry(self) -> Optional[float]:
        qty = self.base_qty
        if qty <= 0:
            return None
        return sum(fill.price * fill.quantity for fill in self.fills) / qty

    @property
    def rung_usd(self) -> float:
        return model.min_rung_usd(self.min_notional_usd)

    # ── the fall puts money in play ──────────────────────────────

    def collect(self, candle: Candle, rungs: List[dict]) -> List[str]:
        """
        Price reaching a level adds that level's money to the pot.

        Collected shallowest first, so the total builds in the order price
        actually met them — and the level that TIPS the pot over one placeable
        rung becomes the line whose break arms the stop.
        """
        if self.reuse_below is not None and candle.low >= self.reuse_below:
            # The new-low rule: after a round closes, nothing re-arms until
            # price prints below where it closed. Otherwise the next round
            # re-enters at or above the last exit, which is the one thing this
            # strategy must never do.
            return []

        crossed = [
            rung
            for rung in rungs
            if rung["usd"] > 0
            and rung["price"]
            and candle.low <= rung["price"]
            and (rung["leg_id"], rung["level"]) not in self.collected_levels
        ]
        crossed.sort(key=lambda rung: -rung["price"])
        notes = []
        for rung in crossed:
            self.collected_levels.add((rung["leg_id"], rung["level"]))
            self.pot_usd = round(self.pot_usd + float(rung["usd"]), 2)
            if self.pot_line is None and self.pot_usd + 1e-9 >= self.rung_usd:
                self.pot_line = float(rung["price"])
                notes.append(
                    f"F{rung['leg_id']} L{rung['level']} at {rung['price']:,.4f} — "
                    f"${self.pot_usd:,.2f} clears the ${self.rung_usd:,.2f} minimum; "
                    f"two reds below {rung['price']:,.4f} will set the stop."
                )
            else:
                notes.append(f"F{rung['leg_id']} L{rung['level']} at {rung['price']:,.4f} — +${rung['usd']:,.2f}")
        return notes

    # ── the turn takes it ────────────────────────────────────────

    def advance_stop(self, candle: Candle) -> Optional[str]:
        """
        Walk the buy stop down with the fall. Returns a note if it moved.

        Two reds under the line are needed before anything is placed: the first
        breaks the line, the second confirms the fall and puts the market below
        the trigger — a buy stop has to sit above the market to be a stop at
        all. Greens are ignored, and a red closing HIGHER than the last one
        does not count, because price must keep falling.
        """
        if self.pot_line is None or self.pot_usd <= 0:
            return None
        if candle.timestamp == self.stop_ts or not candle.is_red:
            return None
        if candle.close >= self.pot_line:
            return None
        if self.last_red is None:
            self.last_red = candle.close
            return f"Line {self.pot_line:,.4f} broken at {candle.close:,.4f} — waiting for a second red."
        if candle.close >= self.last_red:
            return None

        # The trigger is the PREVIOUS red close, one body back, so it sits
        # ABOVE where the market just closed. That is what makes it a stop.
        trigger = self.last_red
        gap = STOP_LIMIT_GAP_USD.get(self.symbol.upper())
        self.stop_price = round(trigger, 8)
        self.limit_price = round(trigger + (gap if gap is not None else STOP_LIMIT_OFFSET_TICKS * self.tick_size), 8)
        self.stop_ts = candle.timestamp
        self.entry_rev += 1
        self.entry_resting = False  # the resting order is at the wrong trigger now
        self.held_reason = ""
        self.last_red = candle.close
        return f"Buy stop at {self.stop_price:,.4f} / limit {self.limit_price:,.4f} for ${self.pot_usd:,.2f}"

    def entry_allowed(self, market_price: float) -> Tuple[bool, str]:
        """
        Whether the armed stop may actually be placed against this market.

        The case this refuses: a campaign that started late, or read its mother
        from the left, replays a fall that already bottomed and bounced days
        ago. The trigger is far below the market, and arming there buys over
        value on no new low. So the pot stays collected and unarmed, and only a
        genuine fresh low below the trigger re-arms it.
        """
        if self.stop_price is None or self.pot_usd <= 0:
            return False, "no armed stop"
        allowed = max_stop_raise_usd(self.stop_price, self.median_bar_pct, self.tick_size)
        raised = market_price - self.stop_price
        if raised > allowed:
            pct = (raised / self.stop_price * 100) if self.stop_price else 0.0
            return False, (
                f"Held, not armed: {market_price:,.4f} is {pct:.2f}% above the trigger {self.stop_price:,.4f} "
                f"({raised:,.4f} against an allowance of {allowed:,.4f}). The collected fall already bottomed "
                f"and bounced — arming here buys over value with no new low."
            )
        return True, ""

    # ── intents ──────────────────────────────────────────────────

    def entry_client_order_id(self) -> str:
        return f"cfx-{self.campaign_id}-e{self.entry_rev}"

    def exit_client_order_id(self) -> str:
        return f"cfx-{self.campaign_id}-x{self.exit_rev}"

    def intents(self, market_price: float) -> List[OrderIntent]:
        """Everything that should change on the exchange right now."""
        intents: List[OrderIntent] = []

        if self.stop_price is not None and self.pot_usd > 0 and not self.entry_resting:
            allowed, reason = self.entry_allowed(market_price)
            if allowed:
                intents.append(
                    OrderIntent(
                        action="place",
                        kind="entry",
                        client_order_id=self.entry_client_order_id(),
                        side="buy",
                        order_type="stop_limit",
                        price=self.limit_price,
                        stop_price=self.stop_price,
                        usd_notional=self.pot_usd,
                        reason="pot armed by two reds",
                    )
                )
            else:
                self.held_reason = reason

        intents.extend(self.exit_intents())
        return intents

    def exit_intents(self) -> List[OrderIntent]:
        """
        The exit, off the buyer's own average entry and their own venue's fee.

        Replaced whenever the average moves, because a target computed against
        an older average is the wrong price for the position now held.
        """
        qty = self.base_qty
        if qty <= 0:
            return []
        target = round(
            model.take_profit_price(self.avg_entry, self.mother_high, exchange=self.exchange),
            8,
        )
        if self.exit_resting and self.exit_price is not None and abs(self.exit_price - target) < 1e-9:
            return []
        intents = []
        if self.exit_resting:
            intents.append(
                OrderIntent(
                    action="cancel",
                    kind="exit",
                    client_order_id=self.exit_client_order_id(),
                    reason="average entry moved",
                )
            )
            self.exit_rev += 1
        self.exit_price = target
        self.exit_resting = True
        intents.append(
            OrderIntent(
                action="place",
                kind="exit",
                client_order_id=self.exit_client_order_id(),
                side="sell",
                order_type="limit",
                price=target,
                quantity=qty,
                reason="take profit",
            )
        )
        return intents

    # ── events from the exchange ─────────────────────────────────

    def on_entry_filled(self, fill: Fill) -> None:
        self.fills.append(fill)
        self.pot_usd = 0.0
        self.pot_line = None
        self.last_red = None
        self.stop_price = None
        self.limit_price = None
        self.entry_resting = False
        self.held_reason = ""

    def on_exit_filled(self, exit_price: float, *, ts: int = 0) -> None:
        """A round closed. Their floor moves to where THEIR round closed."""
        qty = self.base_qty
        entry = self.avg_entry
        if qty > 0 and entry:
            invested = entry * qty
            exited = float(exit_price) * qty
            fees = (invested + exited) * model.fee_pct_for(self.exchange) / 100.0
            self.closed_rounds.append(
                {
                    "opened_ts": min((fill.timestamp for fill in self.fills), default=0),
                    "closed_ts": int(ts),
                    "quantity": qty,
                    "avg_entry": entry,
                    "exit_price": float(exit_price),
                    "gross_usd": round(exited - invested, 4),
                    "fees_est_usd": round(fees, 4),
                    "net_est_usd": round(exited - invested - fees, 4),
                }
            )
        self.fills.clear()
        self.exit_resting = False
        self.exit_price = None
        self.exit_rev += 1
        self.reuse_below = float(exit_price)
        # A fresh round redraws from scratch: money already spent on this
        # stretch is spent, and the levels are collectable again only below the
        # new floor.
        self.collected_levels.clear()
        self.pot_usd = 0.0
        self.pot_line = None
        self.last_red = None
        self.stop_price = None

    # ── the campaign is over ─────────────────────────────────────

    def abandon_entry_intents(self, reason: str) -> List[OrderIntent]:
        """Take the resting buy off the exchange. Nothing about the exit.

        Deliberately separate from `sleep_intents`, which cancels for the
        opposite reason: sleeping means "not now", and the entry is re-placed on
        the next tick after wake. This means "not ever" — the ladder that priced
        that trigger is finished — so the cancel is paired with `abandon_entry`
        rather than with a re-place.
        """
        if not self.entry_resting:
            return []
        return [
            OrderIntent(
                action="cancel",
                kind="entry",
                client_order_id=self.entry_client_order_id(),
                reason=reason,
            )
        ]

    def abandon_entry(self) -> None:
        """Clear the whole entry side once the cancel has landed.

        The pot goes with the order. It is money set aside against rungs that
        will never be published again, and leaving it on the books would show a
        buyer capital pending on a campaign that cannot spend it — while the
        stop price beside it named a trigger that can no longer fire.

        The position and its target are untouched, and so is `closed_rounds`:
        this ends the buying, not the holding.
        """
        self.pot_usd = 0.0
        self.pot_line = None
        self.last_red = None
        self.stop_price = None
        self.limit_price = None
        self.entry_resting = False
        self.held_reason = ""

    # ── the twin sleep invariants ────────────────────────────────

    def sleep_intents(self) -> List[OrderIntent]:
        """
        What must happen before this machine stops.

        1. Never sleep with an entry order resting — cancel it. A fill with
           nothing watching creates a position with no target against it. The
           cost of being wrong is a missed entry, re-placed on the next tick
           after wake: opportunity only.
        2. Never sleep holding coin without an exit resting — place one. A
           resting take-profit is the one order that is SAFER left alone: if
           price rallies through target at 3am it takes the exit and the buyer
           wakes up flat and profitable. Cancelling it turns a good outcome
           into a missed one.

        The asymmetry is the point, and it runs in opposite directions for the
        two order types, which is why both are stated rather than assumed.
        """
        intents: List[OrderIntent] = []
        if self.entry_resting:
            intents.append(
                OrderIntent(
                    action="cancel",
                    kind="entry",
                    client_order_id=self.entry_client_order_id(),
                    reason="sleeping: an entry must not fill unwatched",
                )
            )
        if self.base_qty > 0 and not self.exit_resting:
            intents.extend(self.exit_intents())
        return intents

    def armed_exposure_usd(self) -> float:
        """
        "If this machine stops now, at most $X can fill unwatched."

        The single most useful number in the product: knowable, changing as the
        ladder moves, and it turns an abstract worry into a figure a buyer can
        actually judge.
        """
        return round(self.pot_usd if self.entry_resting else 0.0, 2)

    # ── surviving a restart ──────────────────────────────────────

    def worth_keeping(self) -> bool:
        """Whether a restart needs to remember this campaign at all.

        One that never collected, never filled and never closed a round is
        fully described by the feed and the venue, so writing it down would only
        grow the file for nothing.
        """
        return bool(
            self.pot_usd > 0
            or self.collected_levels
            or self.fills
            or self.closed_rounds
            or self.reuse_below is not None
            or self.entry_resting
            or self.exit_resting
        )

    def to_dict(self) -> dict:
        """Everything a fresh process cannot ask someone else for.

        The venue-derived numbers ride along — `tick_size`, `min_notional_usd`,
        and the feed's `median_bar_pct` — because a restored campaign is put
        straight into the book and never passes back through `sync()`, which is
        the only place that reads them. They are the same values `sync()` wrote
        at join time, so a restored campaign is identical to one that stayed in
        memory rather than a differently-configured copy of it.
        """
        return {
            "campaign_id": self.campaign_id,
            "symbol": self.symbol,
            "mother_high": self.mother_high,
            "exchange": self.exchange,
            "tick_size": self.tick_size,
            "min_notional_usd": self.min_notional_usd,
            "median_bar_pct": self.median_bar_pct,
            # Sorted by the string of the leg id, not the id itself: it comes
            # from the feed as JSON and a mixed batch of ints and strings would
            # raise on comparison, which is not a reason to lose the pot.
            "collected_levels": sorted(
                ([leg_id, level] for leg_id, level in self.collected_levels),
                key=lambda pair: (str(pair[0]), pair[1]),
            ),
            "pot_usd": self.pot_usd,
            "pot_line": self.pot_line,
            "last_red": self.last_red,
            "stop_price": self.stop_price,
            "limit_price": self.limit_price,
            "stop_ts": self.stop_ts,
            "entry_rev": self.entry_rev,
            "entry_resting": self.entry_resting,
            "held_reason": self.held_reason,
            "fills": [
                {
                    "price": fill.price,
                    "quantity": fill.quantity,
                    "timestamp": fill.timestamp,
                    "client_order_id": fill.client_order_id,
                }
                for fill in self.fills
            ],
            "exit_resting": self.exit_resting,
            "exit_price": self.exit_price,
            "exit_rev": self.exit_rev,
            "reuse_below": self.reuse_below,
            "closed_rounds": [dict(row) for row in self.closed_rounds],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "CampaignOrders":
        """Rebuild one campaign's book.

        `entry_resting` and `exit_resting` come back as written, including True.
        They are claims about the exchange, not facts, and restoring them true
        is what makes the next `poll_fills` ask about those order ids — which is
        how a fill that happened while the process was down gets noticed at all.
        Restoring them false would silently drop that fill and re-place over the
        top of a live order.
        """
        orders = cls(
            campaign_id=str(data["campaign_id"]),
            symbol=str(data["symbol"]),
            mother_high=float(data.get("mother_high") or 0.0),
            exchange=str(data.get("exchange") or ""),
            tick_size=float(data.get("tick_size") or DEFAULT_TICK_SIZE),
            min_notional_usd=float(data.get("min_notional_usd") or 5.0),
            median_bar_pct=float(data.get("median_bar_pct") or 0.0),
        )
        orders.collected_levels = {
            (pair[0], pair[1]) for pair in (data.get("collected_levels") or []) if len(pair) == 2
        }
        orders.pot_usd = float(data.get("pot_usd") or 0.0)
        orders.pot_line = _opt_float(data.get("pot_line"))
        orders.last_red = _opt_float(data.get("last_red"))
        orders.stop_price = _opt_float(data.get("stop_price"))
        orders.limit_price = _opt_float(data.get("limit_price"))
        orders.stop_ts = int(data.get("stop_ts") or 0)
        orders.entry_rev = int(data.get("entry_rev") or 0)
        orders.entry_resting = bool(data.get("entry_resting"))
        orders.held_reason = str(data.get("held_reason") or "")
        orders.fills = [
            Fill(
                price=float(row["price"]),
                quantity=float(row["quantity"]),
                timestamp=int(row.get("timestamp") or 0),
                client_order_id=str(row.get("client_order_id") or ""),
            )
            for row in (data.get("fills") or [])
        ]
        orders.exit_resting = bool(data.get("exit_resting"))
        orders.exit_price = _opt_float(data.get("exit_price"))
        orders.exit_rev = int(data.get("exit_rev") or 0)
        orders.reuse_below = _opt_float(data.get("reuse_below"))
        orders.closed_rounds = [dict(row) for row in (data.get("closed_rounds") or [])]
        return orders


class OrderBook:
    """Every campaign's orders, and the fleet-wide questions worth asking."""

    def __init__(self):
        self.campaigns: Dict[str, CampaignOrders] = {}

    def get(self, campaign_id: str) -> Optional[CampaignOrders]:
        return self.campaigns.get(campaign_id)

    def track(self, orders: CampaignOrders) -> CampaignOrders:
        self.campaigns[orders.campaign_id] = orders
        return orders

    def armed_exposure_usd(self) -> float:
        return round(sum(orders.armed_exposure_usd() for orders in self.campaigns.values()), 2)

    def sleep_intents(self) -> List[Tuple[str, OrderIntent]]:
        return [(orders.campaign_id, intent) for orders in self.campaigns.values() for intent in orders.sleep_intents()]

    def engaged(self) -> List[str]:
        """Campaigns holding coin or with an order resting on the exchange.

        The question a venue change has to ask. Positions live on the venue
        this machine is connected to; switching underneath them would leave
        real coin on the old exchange with nothing managing its exit, and
        resting orders there that nothing will ever cancel.
        """
        return [
            orders.campaign_id
            for orders in self.campaigns.values()
            if orders.base_qty > 0 or orders.entry_resting or orders.exit_resting
        ]

    def unprotected(self) -> List[str]:
        """Campaigns holding coin with no exit resting — the one bad state."""
        return [
            orders.campaign_id for orders in self.campaigns.values() if orders.base_qty > 0 and not orders.exit_resting
        ]
