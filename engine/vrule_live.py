"""engine/vrule_live.py — the V-Rule (30-70) trading real money.

The paper V-Rule (engine/rule3070_paper.py) is a REPLAY: every five minutes it
re-runs the locked simulator over the whole window and derives every fill
from the geometry. That is exactly right for paper, where the modelled fill
IS the truth, and exactly wrong for live, where a real fill can slip, fill in
part, or not fill at all — a replay would recompute a ladder nobody owns and
be blind to the coin actually in the account.

So this module splits the rule in two, along the line where reality enters:

  STRUCTURE stays with the locked simulator. Standing mother, the dip, two
  greens, the first red that confirms the V — `tools.rule3070_sim.run_ladder`
  is run read-only and asked one thing: which Vs has it BORN. Its fill model
  is ignored. The nine-year backtest proved that scanner; it is not rewritten.

  THE LADDER runs here, over REAL fills. Touch, arm, the trailing entry, the
  30/70 pot, the sliding fibs after each buy, the target, the cancel — the
  same control flow as the simulator's `_step`, bar for bar, except that
  "fill now" becomes "rest this buy on the exchange" and the fills it reads
  back are the ones the exchange reports. The target is handed to the engine
  outright (tp_override_price) and the engine rests the sell.

  EXECUTION is the Cascade engine's — the trailing buy stop with its chase
  cap, the IOC fill window, the locked-balance TP sizing, idempotent client
  ids, restart reconciliation. Months of real-money bugs were paid for once;
  a second executor would pay for them again.

Every campaign this driver starts is `driven=True` in its OWN CascadeEngine
instance, walled off from the live Cascade the way Auto-Cascade_Fib is: its
own broker wrapper, its own writer lock, its own buckets. The two strategies'
engines net each other's coin off the shared balance (foreign_claims).

What is proven, and how: tools/vrule_live_parity.py runs this driver in paper
mode over real tape beside the locked simulator and requires every fill —
bar, price, dollars — and every target and every end to match. When they do,
the live driver IS the proven rule, with only the exchange between them.

Known, accepted divergences from the simulator once money is real:
  · a bar that makes a new low AND bounces to the (now lower) entry inside
    the same five minutes fills at the PREVIOUS bar's higher entry live,
    because the resting stop was set before the bar printed;
  · a stop that triggers but cannot fill inside its limit expires and is
    re-placed — the simulator always fills at the entry.
Both cost a little edge and never add risk.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

_log = logging.getLogger("cryptoforge.vrule_live")

STRATEGY = "v-rule"

# ── the arms, same shape as Auto-Cascade_Fib's ───────────────────────────
# The driver itself can be switched off on the server, books notwithstanding.
DRIVER_ARMED = os.getenv("CRYPTOFORGE_VRULE", "").strip().lower() not in {"0", "false", "no", "off"}
DISARMED_NOTE = "disarmed by CRYPTOFORGE_VRULE=0 on the server"
# Live is OFF unless armed here. A click can never reach real money alone:
# this env AND the venue's keys AND an explicit choice in the page.
LIVE_ARMED = os.getenv("CRYPTOFORGE_VRULE_LIVE", "").strip().lower() in {"1", "true", "yes", "on"}
LIVE_ARM_HINT = "CRYPTOFORGE_VRULE_LIVE=1"
# The most a single live book may size from, whatever its purse says.
LIVE_CEILING_USD = float(os.getenv("CRYPTOFORGE_VRULE_LIVE_MAX", "") or 2000.0)

# ── the locked rule's numbers ─────────────────────────────────────────────
# Mirrors of what engine.rule3070_paper.configure() sets on the simulator.
# A test asserts they are equal, so the two cannot drift apart silently.
SPLIT = (0.30, 0.70)
MIN_ORDER_USD = 5.5  # Binance minimum notional
MAX_BANDS = 2  # 2 bands = 4 buys, then hold for the target
MIN_NET_MARGIN = 0.0035  # the fee gate: a quarter of the fall must beat 0.35%


BUDGET_CAP_FRAC = 0.5  # never more than half the purse in the market
FOLD_AT_FRACTION = 0.25  # closed profit folds in at a quarter of the purse
FEE_PER_SIDE = 0.001  # the simulator's fee model, for the profit bank
WARMUP_DAYS = 30  # history the scanner is given; same as paper
HISTORY_LIMIT = 200  # ended ladders a book remembers (the parity harness lifts this)
SCAN_GRACE_SEC = 10  # after a 5m close, let the bar settle before reading it

# The gate is Binance's round trip (2 x 0.1%) plus this much edge. On a dearer
# venue the same edge sits above THAT venue's round trip — otherwise the rule
# arms buys whose target cannot pay the commission, the engine lifts the
# target above the rule's, and the ladder waits for a price the rule never
# asked for. On Binance the sum is exactly MIN_NET_MARGIN, so nothing moves.
MIN_NET_EDGE = MIN_NET_MARGIN - 2 * FEE_PER_SIDE


def venue_net_margin(campaign) -> float:
    """The fee gate for this campaign's venue: its round trip plus the edge."""
    rate = getattr(campaign, "fee_pct_per_side", None)
    if rate is None:
        return MIN_NET_MARGIN
    return round(2 * float(rate) / 100.0 + MIN_NET_EDGE, 10)


@dataclass
class Bar:
    timestamp: int
    open: float
    high: float
    low: float
    close: float


def bars_from_df(df) -> List[Bar]:
    """The replay window as plain bars, oldest first."""
    out: List[Bar] = []
    for ts, o, h, lo, c in zip(df.index, df["open"].values, df["high"].values, df["low"].values, df["close"].values):
        out.append(Bar(int(ts.timestamp()), float(o), float(h), float(lo), float(c)))
    return out


@dataclass
class Ladder:
    """One V, from birth to its target or its cancel — the simulator's
    Campaign with the fill model taken out. Every field the simulator's
    `_step` reads is here under the same name, so the port can be checked
    against it line by line."""

    vid: str  # mother ts - swing low ts, the paper journal's own id
    campaign_id: str  # the driven Campaign in the strategy engine
    mother_ts: int
    mother_high: float
    swing_low_ts: int
    swing_low: float
    swing_high_ts: int
    swing_high: float
    born_ts: int
    is_minor: bool = False
    # live state, named as in the simulator
    touched: bool = False
    touch_ts: int = 0
    lowest_low: float = 0.0
    lowest_low_ts: int = 0
    ultimate_low: float = 0.0
    fibB_low_anchor: float = 0.0
    s2_line: float = 0.0
    b2_line: float = 0.0
    pending: str = "30%"
    band: int = 1
    armed: bool = False
    line: float = 0.0
    exhausted: bool = False
    # what this driver has already acted on
    fills_seen: int = 0
    rounds_seen: int = 0
    last_ts: int = 0  # the last bar stepped
    ended: str = ""  # "", "target", "cancelled", "stopped", "lost"
    note: str = ""

    # ── the simulator's derived numbers ──
    @property
    def fibS2(self) -> float:
        return self.swing_high - 2 * (self.swing_high - self.swing_low)

    @property
    def fibB2(self) -> float:
        return self.mother_high - 2 * (self.mother_high - self.swing_low)

    @property
    def reference(self) -> float:
        return max(self.fibS2, self.fibB2)

    @property
    def fall_pct(self) -> float:
        low = min(self.lowest_low or self.swing_low, self.swing_low)
        return (self.mother_high - low) / self.mother_high * 100

    def entry_price(self) -> float:
        return self.lowest_low + 0.25 * (self.mother_high - self.lowest_low)

    def to_dict(self) -> dict:
        return {k: getattr(self, k) for k in self.__dataclass_fields__}

    @classmethod
    def from_dict(cls, data: dict) -> "Ladder":
        kwargs = {}
        for name, f in cls.__dataclass_fields__.items():
            if name in data:
                kwargs[name] = data[name]
        return cls(**kwargs)


def sim_avg_buy(fills) -> float:
    """The simulator's average buy: dollar-weighted PRICE, Σ(price·usd)/Σusd.

    Not the cost-basis average (Σusd/Σqty). It is what the locked rule
    measured its target from for nine years of backtest and every paper
    trade since, so the live target is measured from it too — changing it
    here would make live a different strategy from the one that was proven.
    """
    usd = sum(f.price * f.quantity for f in fills)
    return sum(f.price * (f.price * f.quantity) for f in fills) / usd if usd else 0.0


@dataclass
class Book:
    """One symbol's V-Rule book: its money and its ladders."""

    symbol: str
    exchange: str = ""
    mode: str = "paper"
    enabled: bool = False
    start_capital_usd: float = 2000.0
    purse_usd: float = 2000.0
    pocket_usd: float = 0.0  # closed profit not yet folded in
    folds: int = 0
    start_ts: int = 0  # the clock: only Vs born after this are traded
    history_start_ts: int = 0  # fixed at start, so the scanner's window never moves
    last_scan_ts: int = 0  # the last closed bar the scanner read
    ladders: Dict[str, Ladder] = field(default_factory=dict)  # insertion order = birth order
    history: List[dict] = field(default_factory=list)  # ended ladders, newest last
    tried: List[str] = field(default_factory=list)  # vids ever opened or skipped
    note: str = ""
    last_error: str = ""

    @property
    def key(self) -> str:
        return f"{self.symbol}:{self.exchange}".lower()

    @property
    def wallet_cap_usd(self) -> float:
        return round(self.purse_usd * BUDGET_CAP_FRAC, 2)

    @property
    def fold_threshold_usd(self) -> float:
        return round(self.purse_usd * FOLD_AT_FRACTION, 2)

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "mode": self.mode,
            "enabled": self.enabled,
            "start_capital_usd": self.start_capital_usd,
            "purse_usd": self.purse_usd,
            "pocket_usd": self.pocket_usd,
            "folds": self.folds,
            "start_ts": self.start_ts,
            "history_start_ts": self.history_start_ts,
            "last_scan_ts": self.last_scan_ts,
            "ladders": [ld.to_dict() for ld in self.ladders.values()],
            "history": list(self.history)[-HISTORY_LIMIT:],
            "tried": list(self.tried)[-500:],
            "note": self.note,
            "last_error": self.last_error,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Book":
        book = cls(symbol=str(data.get("symbol") or "").upper())
        book.exchange = str(data.get("exchange") or "")
        # A live book restored while the server is not armed comes back as
        # PAPER. Saved state must never resume real trading the operator has
        # switched off.
        book.mode = "live" if (str(data.get("mode") or "").lower() == "live" and LIVE_ARMED) else "paper"
        book.enabled = bool(data.get("enabled"))
        book.start_capital_usd = _positive(data.get("start_capital_usd"), 2000.0)
        book.purse_usd = _positive(data.get("purse_usd"), book.start_capital_usd)
        book.pocket_usd = float(data.get("pocket_usd") or 0.0)
        book.folds = int(data.get("folds") or 0)
        book.start_ts = int(data.get("start_ts") or 0)
        book.history_start_ts = int(data.get("history_start_ts") or 0)
        book.last_scan_ts = int(data.get("last_scan_ts") or 0)
        for row in data.get("ladders") or []:
            if isinstance(row, dict) and row.get("vid"):
                ladder = Ladder.from_dict(row)
                book.ladders[ladder.vid] = ladder
        book.history = [dict(x) for x in (data.get("history") or []) if isinstance(x, dict)]
        book.tried = [str(x) for x in (data.get("tried") or [])]
        book.note = str(data.get("note") or "")
        book.last_error = str(data.get("last_error") or "")
        return book


def _positive(value, fallback: float) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return fallback
    return number if number > 0 else fallback


class VRuleLive:
    """The driver. Owns the rule; the engine owns the orders."""

    def __init__(self, engine, window_loader=None, structure_scanner=None):
        self.engine = engine
        self.books: Dict[str, Book] = {}
        # Injectable for the parity harness and the tests: the real ones fetch
        # Binance and run the locked simulator; a test hands in a tape.
        self._load_window = window_loader or self._default_window_loader
        self._scan_structure = structure_scanner or self._default_structure_scanner
        self._last_tick_ts = 0.0
        # The venue windows, one per book, so a second venue's thirty days of
        # 1m-built 5m bars are pulled once and then only topped up each bar.
        self._venue_windows: Dict[str, object] = {}

    # ── books ────────────────────────────────────────────────────

    def _venue_broker(self, exchange: str = ""):
        """The client a book on `exchange` trades through — the engine's own
        default for a blank venue, the registry for a named one, None for a
        venue the engine was never given (never the default in its place)."""
        name = str(exchange or "").strip().lower()
        if not name or name == str(getattr(self.engine, "primary_broker_name", "") or "").lower():
            return getattr(self.engine, "broker", None)
        return (getattr(self.engine, "brokers", None) or {}).get(name)

    def _normalise_exchange(self, exchange: str = "") -> str:
        """The default venue is stored BLANK, whatever name the page sent —
        the book key is `symbol:exchange`, and two spellings of one venue
        would be two pots on one account. Unknown venues are refused here."""
        name = str(exchange or "").strip().lower()
        if name == str(getattr(self.engine, "primary_broker_name", "") or "").lower():
            return ""
        if name and self._venue_broker(name) is None:
            known = ", ".join(sorted(getattr(self.engine, "brokers", None) or {})) or "its default only"
            raise ValueError(f"The V-Rule's engine has no client for '{name}' (it has: {known}).")
        return name

    def live_available(self, exchange: str = "") -> bool:
        """Armed on the server AND this venue's keys exist. Per venue:
        Binance keys say nothing about CoinDCX."""
        broker = self._venue_broker(exchange)
        if broker is None or not getattr(broker, "live_armed", False):
            return False
        checker = getattr(broker, "_is_configured", None)
        return bool(checker()) if callable(checker) else False

    def _live_refusal(self, exchange: str = "") -> str:
        broker = self._venue_broker(exchange)
        if broker is not None and getattr(broker, "live_armed", False):
            label = str(
                getattr(broker, "venue_label", "") or getattr(broker, "display_name", "") or exchange or "This exchange"
            )
            return f"{label} API keys are not configured on the server — live is off there."
        return "Live is switched off on the server."

    def exchanges(self) -> List[dict]:
        """The venues a book may be opened on, in the Cascade page's shape."""
        lister = getattr(self.engine, "available_exchanges", None)
        if not callable(lister):
            return []
        out = []
        for venue in lister() or []:
            name = str(venue.get("name") or "")
            broker = self._venue_broker(name)
            label = str(getattr(broker, "venue_label", "") or venue.get("label") or name)
            out.append({**venue, "label": label, "live_available": self.live_available(name)})
        return out

    def set_book(
        self,
        symbol: str,
        *,
        enabled: Optional[bool] = None,
        mode: Optional[str] = None,
        capital_usd=None,
        exchange: str = "",
    ) -> Book:
        symbol = str(symbol or "").upper().strip()
        if not symbol:
            raise ValueError("symbol is required")
        if enabled and not DRIVER_ARMED:
            raise ValueError("The V-Rule live driver is switched off on the server (CRYPTOFORGE_VRULE=0).")
        exchange = self._normalise_exchange(exchange)
        wants_live = mode is not None and str(mode).lower() == "live"
        if wants_live and not self.live_available(exchange):
            raise ValueError(self._live_refusal(exchange))
        if wants_live and _positive(capital_usd, 0.0) > LIVE_CEILING_USD:
            raise ValueError(
                f"A live V-Rule book may size from at most ${LIVE_CEILING_USD:,.0f}. "
                f"Lower the purse, or raise CRYPTOFORGE_VRULE_LIVE_MAX on the server."
            )
        key = f"{symbol}:{exchange}".lower()
        book = self.books.get(key) or Book(symbol=symbol, exchange=exchange)
        if mode is not None:
            book.mode = "live" if wants_live else "paper"
        if capital_usd is not None:
            fresh = _positive(capital_usd, book.start_capital_usd)
            book.start_capital_usd = fresh
            if book.folds == 0 and book.pocket_usd <= 0:
                book.purse_usd = fresh
        if enabled is not None:
            was = book.enabled
            book.enabled = bool(enabled)
            if book.enabled and not was:
                # The clock starts now, like pressing Start on the paper book:
                # only Vs born from here count, and the scanner's window is
                # fixed from 30 days before so nothing ages out under a ladder.
                now = int(time.time())
                book.start_ts = now
                book.history_start_ts = now - WARMUP_DAYS * 86400
                book.note = "started — watching for the first V"
        self.books[key] = book
        return book

    async def stop_book(self, symbol: str, exchange: str = "") -> Book:
        """Switch a book off and take down every open ladder's entry.

        A ladder holding coin keeps its TP resting, as the engine guarantees;
        stopping must never strand a position with nothing to sell it.
        """
        key = f"{str(symbol or '').upper()}:{exchange}".lower()
        book = self.books.get(key)
        if book is None:
            raise ValueError(f"no V-Rule book for {symbol}")
        book.enabled = False
        for ladder in list(book.ladders.values()):
            if ladder.ended:
                continue
            try:
                await self.engine.stop_campaign(ladder.campaign_id, cancel_orders=True)
            except Exception as exc:  # pragma: no cover — a dead campaign is already stopped
                _log.warning("[V-RULE] %s stop of %s failed: %s", book.symbol, ladder.campaign_id, exc)
            ladder.ended = "stopped"
        book.note = "off"
        return book

    def status(self) -> dict:
        return {
            "strategy": STRATEGY,
            "armed": DRIVER_ARMED,
            "disarmed_reason": "" if DRIVER_ARMED else DISARMED_NOTE,
            "live_available": self.live_available(),
            "exchanges": self.exchanges(),
            "live_ceiling_usd": LIVE_CEILING_USD,
            "rules": {
                "split": list(SPLIT),
                "max_bands": MAX_BANDS,
                "min_net_margin": MIN_NET_MARGIN,
                "budget_cap_fraction": BUDGET_CAP_FRAC,
                "fold_at_fraction": FOLD_AT_FRACTION,
            },
            "books": [
                {
                    **{k: v for k, v in book.to_dict().items() if k != "ladders"},
                    "wallet_cap_usd": book.wallet_cap_usd,
                    "fold_threshold_usd": book.fold_threshold_usd,
                    "in_coin_usd": round(self._committed(book), 2),
                    "ladders": [self._ladder_row(book, ld) for ld in book.ladders.values() if not ld.ended],
                    "campaigns": sum(1 for ld in book.ladders.values() if not ld.ended),
                }
                for book in sorted(self.books.values(), key=lambda b: b.symbol)
            ],
        }

    def _ladder_row(self, book: Book, ladder: Ladder) -> dict:
        campaign = self.engine.campaigns.get(ladder.campaign_id)
        fills = list(getattr(campaign, "all_fills", []) or []) if campaign else []
        return {
            "vid": ladder.vid,
            "campaign_id": ladder.campaign_id,
            "mother_high": round(ladder.mother_high, 2),
            "minor": ladder.is_minor,
            "touched": ladder.touched,
            "armed": ladder.armed,
            "pending": f"{ladder.pending} of band {ladder.band}",
            "exhausted": ladder.exhausted,
            "buys": len(fills),
            "entry": round(ladder.entry_price(), 2) if ladder.touched else None,
            "target": round(_coerce(getattr(campaign, "tp_override_price", None)), 2) if fills else None,
            "fall_pct": round(ladder.fall_pct, 2) if ladder.touched else None,
            "note": ladder.note,
        }

    def load(self, payload: dict) -> None:
        self.books = {}
        for row in (payload or {}).get("books") or []:
            if isinstance(row, dict):
                book = Book.from_dict(row)
                if book.symbol:
                    self.books[book.key] = book

    def dump(self) -> dict:
        return {"books": [b.to_dict() for b in self.books.values()]}

    def claimed_base_qty(self, symbol: str, venue: str = "") -> float:
        """Coin this strategy's LIVE campaigns hold, for the other engines to net off."""
        symbol = str(symbol or "").upper()
        venue = str(venue or "").lower()
        total = 0.0
        for campaign in list(getattr(self.engine, "campaigns", {}).values()):
            if str(campaign.symbol or "").upper() != symbol:
                continue
            if str(getattr(campaign, "mode", "") or "") != "live":
                continue
            if venue and str(self.engine.venue_of(campaign) or "").lower() != venue:
                continue
            total += float(getattr(campaign, "filled_base_qty", 0.0) or 0.0)
            total += float(getattr(campaign, "residual_base_qty", 0.0) or 0.0)
        return total

    # ── the money ────────────────────────────────────────────────

    def _own_campaigns(self, book: Book) -> List:
        out = []
        for ladder in book.ladders.values():
            campaign = self.engine.campaigns.get(ladder.campaign_id)
            if campaign is not None:
                out.append((ladder, campaign))
        return out

    def _committed(self, book: Book, majors_only: bool = False) -> float:
        """What the book's open ladders have actually spent — the simulator's
        _COMMITTED (and _MAJOR_COMMITTED), read from real fills."""
        total = 0.0
        for ladder, campaign in self._own_campaigns(book):
            if ladder.ended:
                continue
            if majors_only and ladder.is_minor:
                continue
            total += sum(f.price * f.quantity for f in (campaign.all_fills or []))
        return total

    # The simulator measures the budget ONCE at the top of each bar and adds
    # each buy as it happens; a target taken on the same bar frees its money
    # only on the next. Reading live state mid-bar would let a later ladder
    # spend money an earlier one had just released, and the two would part.
    _bar_committed: Dict[str, float] = {}
    _bar_major_committed: Dict[str, float] = {}

    def _open_bar(self, book: Book) -> None:
        self._bar_committed[book.key] = self._committed(book)
        self._bar_major_committed[book.key] = self._committed(book, True)

    def _spend(self, book: Book, ladder: Ladder, usd: float) -> None:
        # The simulator adds a buy to _COMMITTED the moment it fills, but never
        # to _MAJOR_COMMITTED — that one is only re-measured at the top of the
        # next bar. So a minor sized later in the same bar does not yet see a
        # major's buy from earlier in it. Mirrored, quirk and all: the
        # alternative is a live book that sizes differently from the one the
        # backtest proved.
        self._bar_committed[book.key] = self._bar_committed.get(book.key, 0.0) + usd

    def _bank_round(self, book: Book, ladder: Ladder, campaign) -> None:
        """A target landed: its profit goes to the pocket, and folds at a quarter."""
        rounds = list(getattr(campaign, "rounds", []) or [])
        new = rounds[ladder.rounds_seen :]
        ladder.rounds_seen = len(rounds)
        if not new:
            return
        gained = sum(float(getattr(r, "pnl", 0.0) or 0.0) for r in new)
        book.pocket_usd = round(book.pocket_usd + gained, 8)
        if book.pocket_usd >= book.purse_usd * FOLD_AT_FRACTION and book.pocket_usd > 0:
            book.purse_usd = round(book.purse_usd + book.pocket_usd, 8)
            book.pocket_usd = 0.0
            book.folds += 1
            _log.info("[V-RULE] %s folded profit in — purse now $%.2f", book.symbol, book.purse_usd)

    # ── structure: what the locked simulator says was born ───────

    def _default_window_loader(self, book: Book):
        if not book.exchange:
            # The default venue: the paper book's own fetch, untouched.
            from engine.rule3070_paper import fetch_window

            return fetch_window(book.symbol, since_ts=book.history_start_ts)
        return self._venue_window(book)

    def _venue_window(self, book: Book):
        """CLOSED 5m candles from the book's OWN venue, oldest first.

        The rule's structure must come from the tape its buys rest on: a V
        that Binance printed and CoinDCX did not is a low no CoinDCX order
        can ever fill at. Fetched in full on the first call, then only from
        an hour before the last bar held, merged, and the still-forming bar
        dropped — the same window shape fetch_window returns for Binance.
        """
        import pandas as pd

        client = self._venue_broker(book.exchange)
        if client is None:
            raise LookupError(f"no client for venue '{book.exchange}'")
        cached = self._venue_windows.get(book.key)
        have = cached is not None and len(cached) > 0
        since = int(cached.index[-1].timestamp()) - 3600 if have else int(book.history_start_ts)
        start = datetime.utcfromtimestamp(max(since, 0)).strftime("%Y-%m-%d")
        fresh = client.get_candles(book.symbol, resolution="5m", start=start)
        if fresh is None or len(fresh) == 0:
            return cached if have else pd.DataFrame(columns=["open", "high", "low", "close"])
        fresh = fresh[["open", "high", "low", "close"]].astype(float)
        now_bucket = (int(time.time()) // 300) * 300
        keep = [
            (int(ts.timestamp()) >= book.history_start_ts) and (int(ts.timestamp()) < now_bucket) for ts in fresh.index
        ]
        fresh = fresh[keep]
        merged = pd.concat([cached, fresh]) if have else fresh
        merged = merged[~merged.index.duplicated(keep="last")].sort_index()
        # Trim to the book's CURRENT warm-up, not the one the cache was built
        # under. Turning a book off and on again moves history_start_ts
        # forward; without this the kept window still reaches back to the
        # older start, so the locked simulator is handed a longer window than
        # fetch_window would ever return for the same book on Binance — a
        # different scan, and a window that grows for as long as the book
        # lives.
        floor_ts = int(book.history_start_ts)
        if floor_ts > 0 and len(merged):
            merged = merged[merged.index >= pd.Timestamp(floor_ts, unit="s", tz="UTC")]
        merged.index.name = "datetime"
        self._venue_windows[book.key] = merged
        return merged

    def _default_structure_scanner(self, book: Book, df):
        """Run the locked simulator read-only and return its campaigns.

        Only their BIRTHS are used. Under the replay lock because the
        simulator keeps its configuration in module globals and the paper
        service shares them; the purse is set to this book's so minor sizing
        in the scanner (which affects nothing we read) stays sane.
        """
        import tools.rule3070_sim as sim
        from engine.rule3070_paper import REPLAY_LOCK, configure

        with REPLAY_LOCK:
            configure()
            sim.CAPITAL_USD = float(book.purse_usd)
            sim.BUDGET_FROM_TS = None
            try:
                return sim.run_ladder(df, minors=True)
            finally:
                sim.BUDGET_FROM_TS = None

    @staticmethod
    def _vid(c) -> str:
        return f"{int(c.mother_ts.timestamp())}-{int(c.swing_low_ts.timestamp())}"

    async def _adopt_births(self, book: Book, sim_campaigns, df) -> bool:
        """Open a ladder for every V the simulator has just born."""
        changed = False
        for c in sim_campaigns:
            vid = self._vid(c)
            if vid in book.ladders or vid in book.tried:
                continue
            born_ts = int(c.born_ts.timestamp()) if getattr(c, "born_ts", None) is not None else 0
            if not born_ts or born_ts < book.start_ts:
                continue  # before the clock — history, not a trade
            if c.fills:
                # The simulator already modelled a buy for this V — we were not
                # watching when it armed (a restart gap). Chasing it now would
                # be a different entry from the rule's. Let it go.
                book.tried.append(vid)
                book.note = "missed a V during downtime — waiting for the next"
                continue
            if str(c.status or "").startswith("CANCELLED"):
                book.tried.append(vid)
                continue
            mother_row = df.loc[c.mother_ts] if c.mother_ts in df.index else None
            mother_low = float(mother_row["low"]) if mother_row is not None else float(c.swing_low)
            result = await self.engine.start_campaign(
                symbol=book.symbol,
                capital_usd=book.purse_usd,
                mother_high=float(c.mother_high),
                mother_low=mother_low,
                mother_timestamp=int(c.mother_ts.timestamp()),
                mode=book.mode,
                timeframe="5m",
                mc_kind="major",
                exchange=book.exchange,
                strategy=STRATEGY,
                driven=True,
            )
            if result.get("error"):
                book.last_error = str(result["error"])
                book.tried.append(vid)
                _log.warning("[V-RULE] %s could not open a ladder for %s: %s", book.symbol, vid, result["error"])
                continue
            cid = str(result["campaign"]["campaign_id"])
            ladder = Ladder(
                vid=vid,
                campaign_id=cid,
                mother_ts=int(c.mother_ts.timestamp()),
                mother_high=float(c.mother_high),
                swing_low_ts=int(c.swing_low_ts.timestamp()),
                swing_low=float(c.swing_low),
                swing_high_ts=int(c.swing_high_ts.timestamp()),
                swing_high=float(c.swing_high),
                born_ts=born_ts,
                is_minor=bool(c.is_minor),
                line=0.0,
                last_ts=born_ts,
            )
            ladder.line = ladder.reference
            book.ladders[vid] = ladder
            book.tried.append(vid)
            book.last_error = ""
            changed = True
            _log.info(
                "[V-RULE] %s new %s ladder under mother %.2f (V low %.2f / high %.2f)",
                book.symbol,
                "minor" if ladder.is_minor else "major",
                ladder.mother_high,
                ladder.swing_low,
                ladder.swing_high,
            )
        return changed

    # ── the ladder, one bar at a time ────────────────────────────

    async def _step_ladder(self, book: Book, ladder: Ladder, campaign, bar: Bar) -> None:
        """The simulator's `_step`, over real fills. Read it beside that function."""
        from engine.cascade import ACTIVE_STATES, Candle

        ladder.last_ts = bar.timestamp
        # The engine reads a campaign's "last candle" for round timestamps and
        # the reuse line; a driven campaign has no history of its own, so it
        # is given the current bar.
        self.engine._candles[campaign.campaign_id] = [
            Candle(bar.timestamp, bar.open, bar.high, bar.low, bar.close, timeframe="5m")
        ]
        # Fills that landed since we last looked (live: ingested by the
        # executor; paper: booked by this driver a bar ago) move the fibs.
        self._absorb_fills(ladder, campaign)
        fills = list(campaign.all_fills or [])

        # A round closed = the target was taken. The trade is over.
        if len(campaign.rounds or []) > ladder.rounds_seen:
            self._bank_round(book, ladder, campaign)
            self._end_ladder(book, ladder, "target", "target hit")
            self.engine.complete_driven_campaign(campaign, "target", f"V-Rule target taken — {ladder.vid}")
            return
        if campaign.state not in ACTIVE_STATES:
            self._end_ladder(book, ladder, "lost", f"its campaign ended on its own ({campaign.state.lower()})")
            return

        # CANCEL: no fills and a close above the mother (or the bounce top).
        if not fills and bar.close > max(ladder.mother_high, ladder.swing_high):
            self._end_ladder(
                book, ladder, "cancelled", "broke above the mother before any buy — waiting for the next V"
            )
            self.engine.complete_driven_campaign(
                campaign, "cancelled", f"Close {bar.close:,.2f} above the mother before the first buy — {ladder.vid}"
            )
            return

        if not ladder.touched:
            if bar.high > ladder.swing_high:
                ladder.swing_high = bar.high
                ladder.swing_high_ts = bar.timestamp
                ladder.line = ladder.reference
            if bar.low <= ladder.reference:
                ladder.touched = True
                ladder.touch_ts = bar.timestamp
                ladder.lowest_low = bar.low
                ladder.lowest_low_ts = bar.timestamp
            else:
                ladder.note = "waiting for the reference line to be touched"
                return

        new_low = False
        if bar.low < ladder.lowest_low:
            ladder.lowest_low, ladder.lowest_low_ts, new_low = bar.low, bar.timestamp, True
            if ladder.band == 1 and ladder.pending == "70%" and ladder.armed:
                ladder.fibB_low_anchor = bar.low

        # From here the ORDER of "refresh the target" and "check the target"
        # follows the simulator's _step exactly, including its quirks: after
        # the fourth buy the target is frozen and never refreshed; a bar that
        # arms checks nothing; a bar that neither arms nor fills checks the
        # PREVIOUS bar's target before refreshing it; a fill bar refreshes and
        # checks nothing. Parity is measured against those, not against what
        # a tidier rule would do.
        if ladder.exhausted:
            ladder.note = "4 buys in — holding for the target"
            await self._paper_target_check(book, ladder, campaign, bar)
            return

        if not ladder.armed:
            if ladder.band == 1 and ladder.pending == "70%":
                if bar.low < ladder.ultimate_low:
                    ladder.armed = True
                    ladder.fibB_low_anchor = bar.low
            elif bar.close < ladder.line:
                # The buy order goes in AFTER this close — no same-bar fill.
                ladder.armed = True
                ladder.note = f"armed — {ladder.pending} buy waits for the entry"
                self._set_target(ladder, campaign)
                return
            if not ladder.armed:
                ladder.note = f"waiting for a close below {ladder.line:,.2f} to arm the {ladder.pending} buy"
                await self._paper_target_check(book, ladder, campaign, bar)
                if ladder.ended:
                    return
                self._set_target(ladder, campaign)
                return

        entry = ladder.entry_price()
        deep_enough = not fills or entry < fills[-1].price
        margin = venue_net_margin(campaign)
        if margin and 0.25 * (ladder.mother_high - ladder.lowest_low) / entry < margin:
            deep_enough = False  # the win would not beat the fee — wait for a deeper low

        if deep_enough:
            split = SPLIT[0] if ladder.pending == "30%" else SPLIT[1]
            majors_held = self._bar_major_committed.get(book.key, 0.0)
            base = book.purse_usd if not ladder.is_minor else max(book.purse_usd - majors_held, 0.0)
            unit = base / 50.0 if ladder.fall_pct > 50 else base / 100.0
            # The rule's floor is Binance's. A venue with a higher minimum
            # (stamped on the campaign at birth from its own product) lifts
            # it; on Binance the stamp is $5 and this changes nothing.
            usd = max(
                ladder.fall_pct * unit * split,
                MIN_ORDER_USD,
                _positive(getattr(campaign, "min_notional_usd", 0.0), 0.0),
            )
            if self._bar_committed.get(book.key, 0.0) + usd > book.purse_usd * BUDGET_CAP_FRAC:
                # No free money. The rule stays armed and waits; the engine
                # must NOT hold a buy stop it cannot pay for.
                self.engine.disarm_driven_entry(campaign)
                ladder.note = "armed, but the book is at its half-purse limit — waiting for money to free"
                self._set_target(ladder, campaign)
                await self._paper_target_check(book, ladder, campaign, bar)
                return
            self.engine.arm_driven_entry(
                campaign, usd, entry, Candle(bar.timestamp, bar.open, bar.high, bar.low, bar.close)
            )
            ladder.note = f"{ladder.pending} buy of ${usd:,.2f} resting at {entry:,.2f}"
            if book.mode == "paper" and bar.high >= entry and (not new_low or bar.close >= entry):
                # The simulator's fill: this bar reached the entry. Live, the
                # resting stop does this on the exchange and the fill comes
                # back through the executor instead. A fill bar refreshes the
                # target and checks nothing, as the simulator does.
                self.engine._fill_pending(campaign, entry, bar.timestamp)
                self._spend(book, ladder, usd)
                self._absorb_fills(ladder, campaign)
                self._set_target(ladder, campaign)
                return
        else:
            self.engine.disarm_driven_entry(campaign)
            ladder.note = "armed, but the entry is not below the last buy yet"

        self._set_target(ladder, campaign)
        await self._paper_target_check(book, ladder, campaign, bar)

    def _absorb_fills(self, ladder: Ladder, campaign) -> None:
        """Each new real fill slides the fibs exactly as the simulator's fill does."""
        fills = list(campaign.all_fills or [])
        while ladder.fills_seen < len(fills):
            buy_low = ladder.lowest_low
            if ladder.pending == "30%":
                ladder.ultimate_low = buy_low
                ladder.s2_line = buy_low - (ladder.swing_high - ladder.swing_low)
                ladder.pending = "70%"
                ladder.line = ladder.b2_line if ladder.band > 1 else ladder.reference
            else:
                ladder.b2_line = buy_low - (ladder.mother_high - ladder.swing_low)
                ladder.band += 1
                ladder.pending = "30%"
                ladder.line = ladder.s2_line
                if MAX_BANDS and ladder.band > MAX_BANDS:
                    ladder.exhausted = True  # crash brake: hold what we have
            ladder.armed = False
            ladder.fills_seen += 1
            # The engine already cleared the pot on a full fill; a partial fill
            # leaves the remainder resting, which the rule does not want — the
            # next buy is a new decision at a new low.
            self.engine.disarm_driven_entry(campaign)

    def _end_ladder(self, book: Book, ladder: Ladder, how: str, note: str) -> None:
        ladder.ended = how
        ladder.note = note
        book.history.append({**ladder.to_dict(), "ended_at": int(time.time())})
        del book.history[:-HISTORY_LIMIT]

    def _set_target(self, ladder: Ladder, campaign) -> None:
        fills = list(campaign.all_fills or [])
        if not fills:
            campaign.tp_override_price = None
            return
        campaign.tp_override_price = round(sim_avg_buy(fills) + 0.25 * (ladder.mother_high - ladder.lowest_low), 8)
        campaign.tp_price = campaign.tp_override_price

    async def _paper_target_check(self, book: Book, ladder: Ladder, campaign, bar: Bar) -> None:
        """Paper only: the simulator closes on a bar whose high reaches the target."""
        if book.mode != "paper":
            return
        fills = list(campaign.all_fills or [])
        target = campaign.tp_override_price
        if not fills or not target or bar.high < target:
            return
        self.engine._close_round(campaign, float(target))
        self._bank_round(book, ladder, campaign)
        self._end_ladder(book, ladder, "target", "target hit")
        self.engine.complete_driven_campaign(campaign, "target", f"V-Rule target {target:,.2f} taken — {ladder.vid}")

    # ── the tick ─────────────────────────────────────────────────

    def _due(self, book: Book, now: float) -> bool:
        """Once per closed 5m bar, a few seconds after it closes."""
        last_closed = (int(now) // 300) * 300
        if now < last_closed + SCAN_GRACE_SEC:
            last_closed -= 300
        return last_closed > book.last_scan_ts

    async def tick(self, now: Optional[float] = None) -> bool:
        if not DRIVER_ARMED:
            for book in self.books.values():
                if book.enabled and book.note != DISARMED_NOTE:
                    book.note = DISARMED_NOTE
            return False
        now = time.time() if now is None else now
        changed = False
        for book in list(self.books.values()):
            if not book.enabled or not self._due(book, now):
                continue
            try:
                changed |= await self._tick_book(book, now)
            except Exception as exc:  # one bad book must not stop the others
                book.last_error = str(exc)
                _log.warning("[V-RULE] %s tick failed: %s", book.symbol, exc)
        self._last_tick_ts = now
        return changed

    async def _tick_book(self, book: Book, now: float) -> bool:
        if book.mode == "live" and not self.live_available(book.exchange):
            book.note = "live is not armed on the server — not trading"
            return False
        if book.mode == "live" and book.purse_usd > LIVE_CEILING_USD:
            book.note = f"purse ${book.purse_usd:,.0f} is over the ${LIVE_CEILING_USD:,.0f} live ceiling — not trading"
            return False
        df = await asyncio.to_thread(self._load_window, book)
        if df is None or not len(df):
            book.note = "no candles yet"
            return False
        sim_campaigns = await asyncio.to_thread(self._scan_structure, book, df)
        bars = bars_from_df(df)
        changed = False
        # Every ladder advances through the same bar before any moves to the
        # next — the budget is measured per bar, across the book. And the bars
        # are stepped BEFORE new births are adopted, as the simulator steps
        # its campaigns before it scans: a ladder that ends on this bar frees
        # its mother for the V born on this same bar.
        oldest = min((ld.last_ts for ld in book.ladders.values() if not ld.ended), default=None)
        for bar in bars:
            if oldest is None or bar.timestamp <= oldest:
                continue
            self._open_bar(book)
            for ladder in list(book.ladders.values()):
                if ladder.ended or bar.timestamp <= ladder.last_ts:
                    continue
                campaign = self.engine.campaigns.get(ladder.campaign_id)
                if campaign is None:
                    self._end_ladder(book, ladder, "lost", "its campaign is gone from the engine")
                    continue
                await self._step_ladder(book, ladder, campaign, bar)
                ladder.last_ts = bar.timestamp
                changed = True
        # Ended ladders are history now; the engine's closed list has them.
        for vid, ladder in list(book.ladders.items()):
            if ladder.ended:
                del book.ladders[vid]
                changed = True
        changed |= await self._adopt_births(book, sim_campaigns, df)
        book.last_scan_ts = bars[-1].timestamp if bars else book.last_scan_ts
        open_count = sum(1 for ld in book.ladders.values() if not ld.ended)
        if not book.ladders:
            book.note = "watching for the next V"
        elif open_count:
            book.note = f"{open_count} ladder{'s' if open_count != 1 else ''} working"
        return changed


def _coerce(value) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
