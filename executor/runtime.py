"""
executor/runtime.py — the loop that makes the other five files a program.

feed_client knows what is true. model knows what it is worth. orders knows what
to place. exchange knows how to say it. recovery knows what to do after a gap.
This wires them together and owns the one thing none of them can: the decision
about WHEN, and the buyer's own capital.

Two behaviours here matter more than the plumbing, and both are integration
properties that no single module can express on its own:

**Staleness reduces trading; it never stops caring.** A feed that has gone
quiet, a key set past its cache, a lapsed subscription — each stops NEW
structure and leaves exit management running. A buyer whose subscription
expired still has coin on an exchange, and it still needs its target.

**A halted campaign is quarantined, not abandoned.** Published geometry that
contradicts itself stops that campaign opening anything more, while its
existing position keeps its exit and its siblings carry on untouched.

Like the rest of `executor/`, this must not import `engine.cascade`.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Protocol, Tuple

from executor import model
from executor.config import subscription_phrase
from executor.exchange import ExchangeAdapter, IntentExecutor
from executor.feed_client import FeedClient, FollowedCampaign
from executor.orders import CampaignOrders, Candle, Fill, OrderBook
from executor.recovery import (
    ShutdownRecord,
    plan_for_sleep,
    plan_recovery,
    record_sleep_outcome,
    tp_catchup_intent,
    wake_report,
)

_log = logging.getLogger("cascade.executor.runtime")

Band = Tuple[float, float]

# Bumped when the saved book's shape changes. A record from a different version
# is ignored rather than half-read: starting with an empty book costs the pot,
# which price can rebuild, while a misread one places real orders against
# numbers that mean something else.
BOOK_VERSION = 1

# Why a buy is cancelled when its campaign stops being published. Not the sleep
# reason: sleeping means "not now" and re-places on wake, this means "not ever".
EN_ROUTE_TO_NOWHERE = "the campaign that priced this trigger has ended"


class MarketData(Protocol):
    """The buyer's own candles, from their own venue — never ours.

    They trade their exchange's prices, so they must read their exchange's
    candles. Ours are what the geometry was DRAWN on, which is a different
    question and is why `exchange` rides on campaign.opened.
    """

    def last_price(self, symbol: str) -> float: ...

    def closed_candles_since(self, symbol: str, timeframe: str, since_ts: int) -> List[Candle]: ...


@dataclass
class RuntimeConfig:
    capital_usd: float
    quote_asset: str = "USDT"
    # Symbols the buyer wants to follow. Empty means all of them — which is a
    # real choice, not a default: fewer symbols means fewer siblings competing
    # for the same capital and a more faithful ladder on each.
    symbols: List[str] = field(default_factory=list)
    # The venue this machine TRADES on, which is not necessarily the venue the
    # geometry was drawn on. Every figure about the buyer's own money — the
    # fee floor under their target, the commission netted off their rounds —
    # is priced here, because this is where they actually pay.
    exchange: str = "binance"
    # What this subscription covers, for the status line. The filtering itself
    # happens in the feed client, at the join decision.
    subscription_line: str = ""


@dataclass
class TickReport:
    placed: int = 0
    cancelled: int = 0
    skipped: List[tuple] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)
    opened_blocked: List[str] = field(default_factory=list)


class ExecutorRuntime:
    def __init__(
        self,
        *,
        client: FeedClient,
        adapter: ExchangeAdapter,
        market: MarketData,
        config: RuntimeConfig,
        now_fn: Callable[[], float] = time.time,
    ):
        self._client = client
        self._adapter = adapter
        self._market = market
        self._config = config
        self._now = now_fn
        self.book = OrderBook()
        # Bands this buyer's own campaigns have already funded on a symbol.
        # Captured at each new campaign's birth and fixed for its life: a
        # sibling ending does not retro-fund ground a running campaign already
        # walked past, because money is deployed as price moves and there is no
        # going back to buy a dip that is over.
        self._birth_bands: Dict[str, List[Band]] = {}
        # Buyer-facing switches, all read by the tick. Bools written from the
        # UI's thread and read from the loop's — atomic in CPython, and the
        # worst race is one tick of lag on a machine ticking every 20 seconds.
        self.opening_paused: bool = False
        self.awaiting_confirmation: str = ""  # the wake message, kept until confirmed
        self._stand_down_requested: bool = False
        self.last_prices: Dict[str, float] = {}
        # The reason opening was last blocked, so the log can record the change
        # of posture instead of restating it once per campaign per tick.
        self._last_block_reason: str = ""
        # Book campaigns the feed has stopped publishing and that have already
        # been announced. Same rule as the posture line: say it when it becomes
        # true, not once every twenty seconds for as long as it stays true.
        self._unpublished_announced: set = set()

    # ── keeping the book in step with the feed ───────────────────

    def sync(self) -> List[str]:
        """Give every newly joined campaign an order book of its own."""
        notes = []
        # First, let the client reconsider campaigns it refused only for
        # arriving late. Done here rather than on the socket thread so a
        # campaign whose legs are still in flight has had time to land — see
        # FeedClient.adopt_latecomers. getattr: tests drive sync on stub
        # clients that never grew the method.
        adopt = getattr(self._client, "adopt_latecomers", None)
        if adopt is not None:
            adopt()
        for campaign_id, campaign in self._client.campaigns.items():
            if campaign_id in self.book.campaigns or not campaign.joined:
                continue
            if self._config.symbols and campaign.symbol not in self._config.symbols:
                continue
            allowed, _, warning = model.capital_gate(self._config.capital_usd)
            if not allowed:
                notes.append(f"{campaign_id}: {warning}")
                continue
            # Ask THIS venue about the symbol before taking the campaign on.
            # The geometry may be drawn on an exchange that lists coins ours
            # does not, and finding that out at order time means a campaign
            # that looks followed and can never place. Rules also come back
            # from the venue that will enforce them, not from the feed's
            # advisory copy of some other venue's filters.
            try:
                rules = self._adapter.symbol_rules(campaign.symbol)
            except Exception as exc:
                notes.append(f"{campaign_id}: {campaign.symbol} is not tradeable on {self._config.exchange} — {exc}")
                continue
            self._birth_bands[campaign_id] = self._own_funded_bands(campaign.symbol)
            self.book.track(
                CampaignOrders(
                    campaign_id=campaign_id,
                    symbol=campaign.symbol,
                    mother_high=campaign.mother_high,
                    # Where this buyer PAYS, not where the geometry was drawn:
                    # every fee figure below is about their own money.
                    exchange=self._config.exchange,
                    tick_size=rules.tick_size or campaign.tick_size,
                    min_notional_usd=rules.min_notional_usd or campaign.min_notional_usd,
                    median_bar_pct=campaign.median_bar_pct,
                )
            )
            if warning:
                notes.append(f"{campaign_id}: {warning}")
        return notes

    # ── surviving a restart ──────────────────────────────────────

    def book_snapshot(self) -> dict:
        """What must be on disk for a restart to carry on rather than begin.

        The birth bands ride with each campaign because they are fixed at its
        birth and cannot be recomputed later: `_own_funded_bands` reads the book
        as it stood the moment the campaign joined, and by the next start its
        siblings have moved on or ended. Rebuilding them from today's book would
        re-net a different set of bands and quietly resize every rung — the
        ladder would come back a different shape from the one that was half paid
        for.
        """
        return {
            "v": BOOK_VERSION,
            "campaigns": [
                {
                    **orders.to_dict(),
                    "birth_bands": [[low, high] for low, high in self._birth_bands.get(campaign_id, [])],
                }
                for campaign_id, orders in self.book.campaigns.items()
                if orders.worth_keeping()
            ],
        }

    def restore_book(self, saved: Optional[dict]) -> List[str]:
        """Put the last run's book back before anything else looks at it.

        Called before `on_wake`, deliberately. The protect pass asks every
        campaign whether it holds coin without an exit resting, and against the
        empty book a fresh process starts with, the answer is always no — so the
        one step that exists to put a target back on unprotected coin had
        nothing to iterate. Restoring first is what gives it something to
        protect.

        Restored campaigns go straight into the book, which means `sync()` skips
        them (it only builds campaigns it has not seen). That is the intent:
        their venue rules and birth bands are already settled, and re-deriving
        either would change a ladder that is part paid for.
        """
        if not saved:
            return []
        version = saved.get("v")
        if version != BOOK_VERSION:
            _log.warning("saved book is version %r, expected %r — starting empty", version, BOOK_VERSION)
            return ["Saved positions were written by a different version and could not be read — starting fresh."]

        pot = 0.0
        holding = 0
        for record in saved.get("campaigns") or []:
            try:
                orders = CampaignOrders.from_dict(record)
            except Exception as exc:
                # One unreadable campaign must not cost the others theirs.
                _log.warning("could not restore %s: %s", record.get("campaign_id"), exc)
                continue
            self.book.track(orders)
            self._birth_bands[orders.campaign_id] = [
                (float(low), float(high)) for low, high in (record.get("birth_bands") or [])
            ]
            pot += orders.pot_usd
            if orders.base_qty > 0:
                holding += 1

        if not self.book.campaigns:
            return []
        parts = [f"Picked up {len(self.book.campaigns)} campaign{'s' if len(self.book.campaigns) != 1 else ''}"]
        if pot > 0:
            parts.append(f"${pot:,.2f} already collected")
        if holding:
            parts.append(f"{holding} holding coin")
        return [f"{', '.join(parts)} from before the restart."]

    def _own_funded_bands(self, symbol: str) -> List[Band]:
        """
        The stretches this buyer's other campaigns on this symbol already pay
        for. Their siblings, not ours — which is the entire reason the feed
        publishes the gross percent and lets them net it locally.
        """
        bands: List[Band] = []
        for campaign_id, orders in self.book.campaigns.items():
            if orders.symbol != symbol:
                continue
            followed = self._client.campaigns.get(campaign_id)
            if not followed or not followed.legs:
                continue
            lowest = min(leg.low for leg in followed.legs.values())
            if followed.mother_high > lowest:
                bands.append((lowest, followed.mother_high))
        return bands

    # ── the tick ─────────────────────────────────────────────────

    def tick(self) -> TickReport:
        """
        One pass: collect what price reached, walk the stops, place what is due.

        Exits are always managed. Entries are gated — by the feed's posture, by
        the campaign's own halt, and by whether the buyer's capital can lay the
        ladder at all.
        """
        report = TickReport()
        self.sync()
        if self._stand_down_requested:
            self._stand_down_requested = False
            self.opening_paused = True
            stood = self.prepare_for_sleep(reason="buyer")
            report.notes.append(f"Stood down at your request. {stood['message']}")
        may_open, reason = self._client.may_open_new
        # The buyer's own gates come after the feed's, and both keep exits
        # running: pause is "stop opening", never "stop caring".
        if may_open and self.opening_paused:
            may_open, reason = False, "Paused by you — opening nothing new until you resume."
        elif may_open and self.awaiting_confirmation:
            may_open, reason = False, "Waiting for you to review what changed while this machine was away."

        prices: Dict[str, float] = {}

        for campaign_id, orders in list(self.book.campaigns.items()):
            followed = self._client.campaigns.get(campaign_id)
            # A campaign the feed no longer publishes used to be skipped whole,
            # exit and all. That is the one thing this loop must never do: the
            # coin was bought with the buyer's money and stays theirs after the
            # geometry is over, so it keeps its target. Only ENTRIES need the
            # feed — the ladder cannot be drawn without it — and `exit_intents`
            # needs nothing the book does not already hold.
            #
            # It is reachable because the book is restored from disk: a campaign
            # that ended on the server while this machine was down comes back
            # here and is not in the snapshot. Within a session the feed client
            # only ever adds campaigns, so this is a settled state rather than a
            # flicker to debounce.
            entries_allowed = followed is not None and may_open and followed.active
            if followed is None:
                report.notes.extend(self._unpublished_note(campaign_id, orders))
            elif not entries_allowed and (orders.pot_usd > 0 or orders.stop_price):
                # Only counted when the FEED is what is holding it back. An
                # unpublished campaign is blocked for its own reason and would
                # otherwise inflate the posture line's count.
                report.opened_blocked.append(campaign_id)

            if entries_allowed:
                try:
                    report.notes.extend(self._advance(campaign_id, orders, followed))
                except Exception as exc:
                    # Contained per campaign, like every other venue call in
                    # this class. Raising here escaped the loop, so one venue
                    # refusing one symbol's candles — a coin it does not list,
                    # an interval it does not serve, a bad minute on the wire —
                    # abandoned every campaign after it in the pass, INCLUDING
                    # the exit orders protecting coin already held.
                    #
                    # Entries stop for this campaign this tick, deliberately:
                    # without candles we cannot know which rungs were crossed,
                    # and guessing that is how money gets placed at the wrong
                    # price. Its exit still goes in below.
                    _log.warning("could not advance %s: %s", campaign_id, exc)
                    entries_allowed = False
                    report.notes.append(
                        f"{campaign_id}: could not read {orders.symbol} {followed.timeframe} candles — {exc}. "
                        "Opening nothing new here this tick; its exit is still managed."
                    )
            # A blocked posture is not a per-campaign event. It used to write
            # one line per campaign per tick, so three campaigns on a 20s tick
            # buried the fills under the same sentence 9 times a minute. The
            # change of posture is logged once, below the loop; the campaign
            # list and status line carry the standing state.

            # Exits run whatever the posture; entries only when allowed.
            # `intents()` covers both, so a blocked campaign asks for its exit
            # alone rather than nothing at all.
            if orders.symbol not in prices:
                try:
                    prices[orders.symbol] = float(self._market.last_price(orders.symbol))
                except Exception:
                    prices[orders.symbol] = self.last_prices.get(orders.symbol, 0.0)
            self.last_prices[orders.symbol] = prices[orders.symbol]
            if entries_allowed:
                intents = orders.intents(prices[orders.symbol])
            else:
                intents = orders.exit_intents()
                if followed is None:
                    # The ladder that priced this trigger is finished, so a buy
                    # resting on it is waiting to spend money on geometry that
                    # no longer exists. Cancelled ahead of the exit: it is the
                    # order that can still cost something.
                    intents = orders.abandon_entry_intents(EN_ROUTE_TO_NOWHERE) + intents
            if not intents:
                continue
            result = IntentExecutor(self._adapter, orders.symbol, quote_asset=self._config.quote_asset).apply(
                intents, our_resting_exit_qty=self._resting_exit_qty(orders)
            )
            # Cleared only once the exchange confirms the cancel. Clearing on
            # the attempt would drop the pot while the order was still live.
            if followed is None and orders.entry_client_order_id() in result.cancelled:
                spent = orders.pot_usd
                orders.abandon_entry()
                report.notes.append(
                    f"{orders.symbol}: cancelled the resting buy on the campaign that ended — ${spent:,.2f} released."
                )
            orders.entry_resting = bool(result.resting_entry) or orders.entry_resting
            report.placed += len(result.placed)
            report.cancelled += len(result.cancelled)
            report.skipped.extend(result.skipped)

        # One line when the posture changes, not one per campaign per tick.
        # Both directions are worth a line: a buyer needs to see that opening
        # stopped, and needs to see that it started again without watching for
        # the absence of a message.
        blocking = reason if not may_open else ""
        if blocking != self._last_block_reason:
            if blocking:
                held = len(report.opened_blocked)
                where = (
                    f" Opening nothing new on {held} campaign{'s' if held != 1 else ''}; their exits are still managed."
                    if held
                    else ""
                )
                report.notes.append(f"{blocking}{where}")
            else:
                report.notes.append("Opening has resumed.")
            self._last_block_reason = blocking
        return report

    def _unpublished_note(self, campaign_id: str, orders: CampaignOrders) -> List[str]:
        """Told once, when it is first noticed.

        A standing condition, and this loop runs every twenty seconds — restated
        each pass it would bury the fills the panel exists to show. Silent when
        there is nothing at stake: a campaign that ended holding nothing needs no
        announcement, only the ones still carrying the buyer's money.

        A resting buy does not count as something at stake here: it is cancelled
        this same pass and gets its own line, which says more than this one
        would.
        """
        if campaign_id in self._unpublished_announced:
            return []
        if orders.base_qty <= 0 and not orders.exit_resting:
            return []
        self._unpublished_announced.add(campaign_id)
        return [
            f"{orders.symbol}: this campaign is no longer published — it ended while this machine was away. "
            f"Nothing new opens here; the {orders.symbol} you hold keeps its target."
        ]

    def _advance(self, campaign_id: str, orders: CampaignOrders, followed: FollowedCampaign) -> List[str]:
        plan = self._client.plan(
            campaign_id,
            capital_usd=self._config.capital_usd,
            funded_bands=self._birth_bands.get(campaign_id, []),
        )
        if not plan or plan.get("refused"):
            return [f"{campaign_id}: {plan['refused']}"] if plan and plan.get("refused") else []

        rungs = [
            {"leg_id": leg["leg_id"], "level": rung["level"], "price": rung["price"], "usd": rung["usd"]}
            for leg in plan["legs"]
            for rung in leg["rungs"]
        ]
        notes = []
        for candle in self._market.closed_candles_since(orders.symbol, followed.timeframe, orders.stop_ts):
            notes.extend(orders.collect(candle, rungs))
            moved = orders.advance_stop(candle)
            if moved:
                notes.append(moved)
        return notes

    def _resting_exit_qty(self, orders: CampaignOrders) -> float:
        """Coin our own resting sell has locked. See exchange.sellable_qty."""
        return orders.base_qty if orders.exit_resting else 0.0

    # ── events from the exchange ─────────────────────────────────

    def poll_fills(self) -> List[str]:
        """
        Ask the exchange what happened to the orders we placed.

        Polling rather than a user-data stream on purpose, for now: a websocket
        that silently stops delivering fills leaves the executor believing it
        is flat while it is not, and that failure is invisible. A poll that
        stops throws. The cost is latency on a strategy whose exits are resting
        orders anyway — the exchange fills them whether or not we noticed.
        """
        noticed = []
        for campaign_id, orders in list(self.book.campaigns.items()):
            adapter_calls = (
                ("entry", orders.entry_client_order_id(), orders.entry_resting),
                ("exit", orders.exit_client_order_id(), orders.exit_resting),
            )
            for kind, client_order_id, live in adapter_calls:
                if not live:
                    continue
                try:
                    record = self._adapter.get_order(symbol=orders.symbol, client_order_id=client_order_id)
                except Exception as exc:
                    _log.warning("could not read %s: %s", client_order_id, exc)
                    continue
                if record is None:
                    continue
                if record.status == "FILLED":
                    price = record.avg_fill_price or record.price or 0.0
                    if kind == "entry":
                        orders.on_entry_filled(
                            Fill(price=price, quantity=record.filled_qty, timestamp=int(self._now()))
                        )
                    else:
                        orders.on_exit_filled(price, ts=int(self._now()))
                    noticed.append(f"{campaign_id}: {kind} filled at {price:,.4f}")
                elif not record.is_open:
                    # Cancelled or rejected out from under us. Clear the flag so
                    # the next tick re-places rather than waiting on a ghost.
                    if kind == "entry":
                        orders.entry_resting = False
                    else:
                        orders.exit_resting = False
        return noticed

    def on_fill(self, campaign_id: str, fill: Fill, *, side: str = "buy") -> None:
        orders = self.book.get(campaign_id)
        if not orders:
            return
        if side == "buy":
            orders.on_entry_filled(fill)
        else:
            orders.on_exit_filled(fill.price, ts=fill.timestamp)

    # ── going away and coming back ───────────────────────────────

    def prepare_for_sleep(self, *, reason: str = "sleep") -> dict:
        plan = plan_for_sleep(self.book, now=self._now(), reason=reason)
        cancelled: List[str] = []
        for campaign_id, intent in plan.intents:
            orders = self.book.get(campaign_id)
            if not orders:
                continue
            result = IntentExecutor(self._adapter, orders.symbol, quote_asset=self._config.quote_asset).apply(
                [intent], our_resting_exit_qty=self._resting_exit_qty(orders)
            )
            if intent.action == "cancel":
                cancelled.extend(result.cancelled)
                if result.cancelled:
                    orders.entry_resting = False
        record_sleep_outcome(plan.record, cancelled_ids=cancelled)
        return {"record": plan.record.to_dict(), "message": plan.message}

    def on_wake(self, saved: Optional[dict], *, first_run: bool = False) -> dict:
        """
        Run the ladder. Returns what the buyer is told and what was done.

        A missing record means a crash rather than a clean stop: there was no
        chance to cancel anything, so entries may have been resting the whole
        time. That is treated as the armed case, because assuming otherwise is
        exactly the mistake the record exists to prevent.

        Unless nothing has ever run here. A first install has no record for the
        innocent reason that there has never been a shutdown, and treating that
        as a crash told a brand-new buyer they had been "away for 24.0 hours"
        and that entries were being held back — one statement false, the other
        describing a restriction that is not actually applied. `first_run` is
        the caller's answer to "has this machine ever started before", which
        only it can know: the buyer key is created before this runs, so its
        presence proves nothing.
        """
        record = ShutdownRecord.from_dict(saved)
        if record is None and first_run:
            return self._first_run_report()
        if record is None:
            gap, armed = float("inf"), True
        else:
            gap = max(self._now() - record.shutdown_at, 0.0)
            armed = record.slept_armed
        known = gap != float("inf")
        plan = plan_recovery(gap if known else 24 * 3600, slept_armed=armed, gap_known=known)

        protected = []
        for campaign_id, orders in self.book.campaigns.items():
            if orders.base_qty <= 0 or orders.exit_resting:
                continue
            executor = IntentExecutor(self._adapter, orders.symbol, quote_asset=self._config.quote_asset)
            executor.apply(orders.exit_intents(), our_resting_exit_qty=0.0)
            protected.append(campaign_id)

        caught_up = []
        for campaign_id, orders in self.book.campaigns.items():
            if orders.exit_price is None or orders.base_qty <= 0:
                continue
            try:
                market_price = float(self._market.last_price(orders.symbol))
            except Exception as exc:
                # Same containment as the tick. This runs on WAKE, when the
                # most is at stake: one symbol's price failing must not stop
                # the other positions catching their targets up.
                _log.warning("no price for %s on wake: %s", orders.symbol, exc)
                continue
            intent = tp_catchup_intent(
                campaign_id,
                target=orders.exit_price,
                market_price=market_price,
                quantity=orders.base_qty,
            )
            if intent:
                IntentExecutor(self._adapter, orders.symbol, quote_asset=self._config.quote_asset).apply([intent])
                caught_up.append(campaign_id)

        message = wake_report(plan, record)
        if plan.requires_confirmation:
            # Held OPEN until the buyer clears it — the report alone used to
            # say "no new entries until you have looked" with nothing anywhere
            # to look at or press, which made the gate a permanent stop.
            self.awaiting_confirmation = message
        return {
            "band": plan.band,
            "requires_confirmation": plan.requires_confirmation,
            "steps": plan.step_names,
            "protected": protected,
            "tp_caught_up": caught_up,
            "message": message,
        }

    def _first_run_report(self) -> dict:
        """Nothing has ever run here, so there is nothing to recover.

        Deliberately not routed through `plan_recovery`: every band it can
        return describes coming back to something, and the honest answer here
        is that there is nothing to come back to. No position can exist, so the
        protect and catch-up passes have nothing to act on either.
        """
        return {
            "band": "first_run",
            "requires_confirmation": False,
            "steps": [],
            "protected": [],
            "tp_caught_up": [],
            "message": (
                "First run on this machine — nothing held, nothing to recover. "
                "Campaigns are joined as they start, so nothing already running will be picked up."
            ),
        }

    # ── the buyer's own switches ─────────────────────────────────

    def pause_opening(self) -> str:
        self.opening_paused = True
        return "Paused. Nothing new will be opened; open positions keep their exits."

    def resume_opening(self) -> str:
        self.opening_paused = False
        return "Resumed. New campaigns and entries are allowed again."

    def confirm_wake(self) -> str:
        """The buyer has read the wake report. The long-gap gate lifts."""
        self.awaiting_confirmation = ""
        return "Thanks — resuming. New entries are allowed again."

    def request_stand_down(self) -> str:
        """
        Run the sleep invariants on the next tick, from the buyer's button.

        A flag rather than a direct call: the UI thread must not drive the
        adapter concurrently with the tick, and twenty seconds of latency on a
        deliberate wind-down is cheaper than a race on live orders.
        """
        self._stand_down_requested = True
        return "Standing down on the next pass: buy orders will be cancelled, sell orders left protecting."

    def set_subscription(self, *, timeframes, source_exchanges, symbols) -> str:
        """Change which signals this machine follows. Takes effect at once.

        Safe live because it only decides what is joined NEXT. Campaigns
        already running keep their books and their exits: narrowing what you
        want to hear about is not an instruction to abandon a position.
        """
        self._client.set_subscription(timeframes=timeframes, source_exchanges=source_exchanges)
        self._config.symbols = [str(s).strip().upper() for s in symbols if str(s).strip()]
        self._config.timeframes = [str(t).strip().lower() for t in timeframes if str(t).strip()]
        self._config.signal_exchanges = [str(x).strip().lower() for x in source_exchanges if str(x).strip()]
        self._config.subscription_line = subscription_phrase(
            self._config.timeframes, self._config.signal_exchanges, self._config.symbols
        )
        running = len(self.book.campaigns)
        held = f" {running} campaign{'s' if running != 1 else ''} already running keep their exits." if running else ""
        return f"Now following {self._config.subscription_line}.{held}"

    def set_capital(self, usd: float) -> None:
        """Resize from now on.

        Reaches campaigns already running, because `plan()` is recomputed from
        this on every tick: rungs not yet filled resize, coin already bought
        keeps what it cost. That is the honest behaviour of a number every
        ladder is derived from, and the console says so when anything is open.
        """
        self._config.capital_usd = float(usd)

    def venue_change_blockers(self) -> List[str]:
        """Campaigns that make switching exchange unsafe right now."""
        return self.book.engaged()

    def rounds_view(self, limit: int = 50) -> List[dict]:
        rows = [
            {**row, "campaign_id": campaign_id, "symbol": orders.symbol, "exchange": orders.exchange}
            for campaign_id, orders in self.book.campaigns.items()
            for row in orders.closed_rounds
        ]
        rows.sort(key=lambda row: row.get("closed_ts") or 0, reverse=True)
        return rows[:limit]

    # ── what the buyer sees ──────────────────────────────────────

    def status(self) -> dict:
        may_open, reason = self._client.may_open_new
        if may_open and self.opening_paused:
            may_open, reason = False, "Paused by you — opening nothing new until you resume."
        elif may_open and self.awaiting_confirmation:
            may_open, reason = False, "Waiting for you to review what changed while this machine was away."
        rounds = self.rounds_view()
        return {
            "following": len([c for c in self._client.campaigns.values() if c.active]),
            "halted": [c.campaign_id for c in self._client.campaigns.values() if c.halted],
            "skipped": {
                c.campaign_id: c.skip_reason
                for c in self._client.campaigns.values()
                if c.skip_reason and not (c.skipped_as_old or c.skipped_unsubscribed)
            },
            # Folded, not listed. Both of these are the normal state of most of
            # the feed for any one buyer — predating this machine, or simply
            # not the product they bought — and a page of per-campaign alerts
            # buried the lines that ask for attention.
            "skipped_as_old": len([c for c in self._client.campaigns.values() if c.skipped_as_old]),
            "skipped_unsubscribed": len([c for c in self._client.campaigns.values() if c.skipped_unsubscribed]),
            "subscription": self._config.subscription_line,
            "opening_new": may_open,
            "posture_reason": reason,
            # The single most useful number in the product: knowable, changing
            # as the ladder moves, and it turns an abstract worry into a figure
            # a buyer can judge.
            "armed_exposure_usd": self.book.armed_exposure_usd(),
            "unprotected": self.book.unprotected(),
            "paused": self.opening_paused,
            "awaiting_confirmation": self.awaiting_confirmation,
            "prices": dict(self.last_prices),
            "capital_usd": self._config.capital_usd,
            "quote_asset": self._config.quote_asset,
            "rounds_closed": len(rounds),
            "rounds_net_est_usd": round(sum(row["net_est_usd"] for row in rounds), 2),
        }
