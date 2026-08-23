"""engine/auto_cascade_fib.py — Auto-Cascade_Fib, Phil's automated cascade.

The Cascade page is hand-driven: Phil marks a mother candle and starts a
campaign on it. This strategy drives the same engine by itself, under the rules
that came out of the 2026-08 backtests (see the memory notes
proj_cascade_target_level_finding and proj_cascade_half_target_audit):

  sell HALF way back to the mother, not a quarter
      The one change that earned more on every coin and every window tested.
      Fewer trades, each worth several times more.

  climb 5m -> 15m -> 1h -> 4h, then STOP and hold
      A line that cannot reach its target keeps widening its ladder up the
      timeframes. Past 4h that stops paying, so the climb ends there and the
      campaign simply keeps trading the 4h rung for as long as it takes.

  one working 5m line at a time
      The moment the working line climbs to 1h it has become a slow, patient
      major. A fresh 5m line is anchored on the latest confirmed 5m swing high
      so something is always fighting the near move. Graduated majors keep
      running their own ladders alongside.

  never more than half the purse in coin
      Enforced by the engine's existing per-symbol capital group, which clamps
      funding rather than reserving money up front.

  fold profit in at 25%
      Closed profit waits in a pocket. When it is worth a quarter of the purse
      it is folded in, and the wallet cap — half the purse — grows with it. No
      money is ever added from outside; the purse only grows on its own wins.

WHAT THIS IS NOT: a second engine. Every campaign it starts is an ordinary
CascadeEngine campaign wearing `strategy="auto-cascade-fib"`, so orders, fills,
targets, mother breaks and reconciliation are the code that already runs live.
This module only decides WHEN to start one and HOW MUCH the book may hold.

The honest caveat, recorded because it will be asked: the backtests behind
these rules are dominated on SOL and ETH by single positions held through a
crash, and the size of the result moves with the starting balance. BTC was the
only instrument whose result held steady at every size — about 22-26% a year.
Treat that as the expectation, not the headline numbers.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

_log = logging.getLogger("cryptoforge.auto_cascade_fib")

STRATEGY = "auto-cascade-fib"

# ── the 2026-08-21 runaway, and what it changed ──────────────────────────
# On its first hour live this driver started a new SOLUSDT campaign every 15
# seconds — 20 in 45 minutes. Three causes, all now closed:
#
#   · The anchor was STALE: picked from closed 5m candles while the engine
#     judges breaks on the live 1m price. Now the anchor must stand above the
#     last closed 1m candle's high, or nothing starts.
#   · The same dead anchor was re-seeded forever. Now every anchor is used at
#     most once per book (tried_anchors), and every start opens a cooldown of
#     one full 5m bar before the next may even be considered.
#   · Its campaigns lived in Phil's LIVE engine — shared capital groups,
#     shared cross-campaign rules, shared alert inbox. Now the driver runs in
#     its own paper-only sandbox engine (see PaperOnlyBroker and the app
#     wiring): the live Cascade cannot see its campaigns and its campaigns
#     cannot see the live Cascade's money. Live mode is refused outright.
#
# The env switch remains as the off-switch: CRYPTOFORGE_AUTO_FIB=0 disables
# the driver entirely, books notwithstanding.
DRIVER_ARMED = os.getenv("CRYPTOFORGE_AUTO_FIB", "").strip().lower() not in {"0", "false", "no", "off"}

# ── live trading ─────────────────────────────────────────────────────────
# Live is OFF unless deliberately armed on the server. A UI click can never
# reach real money on its own: turning a book live needs this env AND the
# venue's keys AND an explicit choice in the page, three independent locks.
# The strategy's campaigns still live in their own engine, so the live
# Cascade's books, capital groups and cross-campaign rules stay untouched —
# that separation is what 2026-08-21 cost, and going live does not spend it.
LIVE_ARMED = os.getenv("CRYPTOFORGE_AUTO_FIB_LIVE", "").strip().lower() in {"1", "true", "yes", "on"}
# The most a single live book may ever hold in coin, whatever its purse says.
# A backstop against a purse typo, not a strategy rule: the wallet fraction is
# still the real limit and is almost always the binding one.
LIVE_CEILING_USD = float(os.getenv("CRYPTOFORGE_AUTO_FIB_LIVE_MAX", "") or 2000.0)
DISARMED_NOTE = "disarmed by CRYPTOFORGE_AUTO_FIB=0 on the server"

SEED_COOLDOWN_SEC = 300  # one full 5m bar between starts — the churn brake
TRIED_ANCHOR_LIMIT = 50  # how many used anchors a book remembers


class PaperOnlyBroker:
    """The strategy's broker: market data always, orders only when armed.

    The strategy engine gets its candles, tickers and product metadata from
    the same client the app already has. Order and account methods are the
    guarded half:

      · LIVE_ARMED false — the default — every order and account call raises,
        so no code path in the shared engine can reach the exchange even by
        mistake, and `_is_configured()` reports False so nothing offers live;
      · LIVE_ARMED true, keys present — they forward to the real client, and
        the engine's own `campaign.mode == "live"` gates decide which
        campaigns actually use them. Paper campaigns never call them at all.

    The name is kept: paper is still what this broker is for, and live is the
    exception it has to be asked for twice.
    """

    _FORWARD = {
        "async_get_candles",
        "get_candles",
        "get_product_by_symbol",
        "get_ticker",
    }
    _REFUSE = {
        "place_order",
        "cancel_order",
        "cancel_all_orders",
        "get_orders",
        "get_order",
        "get_wallet",
        "get_balances",
    }

    def __init__(self, real, live_armed: Optional[bool] = None, arm_hint: str = "CRYPTOFORGE_AUTO_FIB_LIVE=1"):
        self._real = real
        # Named in the refusal so the log says WHICH strategy's switch is off.
        # Two strategies now wrap the same client, each behind its own arm.
        self._arm_hint = str(arm_hint or "")
        # Captured per instance so a test can arm one without touching the
        # module, and so the value the engine was built with is the value it
        # keeps for its life — live must never flip under a running campaign.
        self._live_armed = LIVE_ARMED if live_armed is None else bool(live_armed)
        self.broker_name = str(getattr(real, "broker_name", "") or "")
        # The venue's own name, for the page; display_name below is for logs.
        self.venue_label = str(getattr(real, "display_name", "Broker"))
        self.display_name = self.venue_label + (
            " (strategy, live)" if self._live_armed else " (strategy, market data only)"
        )
        self.supports_cascade = True
        self.min_timeframe = str(getattr(real, "min_timeframe", "") or "")
        # The venue's commission rides along with its name. The engine stamps
        # a campaign's fee at birth from the client it will trade on, and
        # floors every take-profit above it; a wrapper that hid this would
        # have a CoinDCX book (0.2% a side) priced at Binance's 0.1% — a
        # target that sells below its own fee. Only set when the real client
        # declares one, so a client without it still falls back to the default.
        if getattr(real, "fee_pct_per_side", None) is not None:
            self.fee_pct_per_side = float(real.fee_pct_per_side)

    @property
    def live_armed(self) -> bool:
        return self._live_armed

    def _is_configured(self) -> bool:
        """Tradable only when armed AND the real client has its keys."""
        if not self._live_armed:
            return False
        checker = getattr(self._real, "_is_configured", None)
        return bool(checker()) if callable(checker) else False

    def __getattr__(self, name):
        if name in self._FORWARD:
            return getattr(self._real, name)
        if name in self._REFUSE:
            if self._live_armed:
                return getattr(self._real, name)

            hint = self._arm_hint

            def _refused(*_args, **_kwargs):
                raise RuntimeError(
                    f"PaperOnlyBroker refused {name}() — this strategy is not armed for live (set {hint} on the server)"
                )

            return _refused
        # Anything unknown is treated as missing, so getattr(..., default)
        # falls back the way it would on a client without the feature.
        raise AttributeError(name)


# ── the rules, as constants so a reader can check them against the notes ──
TP_FIB_LEVEL = 0.5  # sell half way back to the mother
CAP_TIMEFRAME = "4h"  # climb stops here and the campaign holds
GRADUATE_TIMEFRAME = "1h"  # reaching this makes the working line a major
WALLET_FRACTION = 0.5  # at most half the purse in coin at once
FOLD_AT_FRACTION = 0.25  # fold the pocket in when it is worth this much of the purse
SWING_BARS = 12  # 5m bars either side that make a high a swing high (an hour)
SWING_LOOKBACK_BARS = 576  # how far back to hunt for one (two days)


@dataclass
class Book:
    """One symbol's book: its money, and the campaigns the strategy owns."""

    symbol: str
    exchange: str = ""
    mode: str = "paper"  # paper | live, chosen the same way the Cascade page does
    enabled: bool = False
    start_capital_usd: float = 2000.0
    purse_usd: float = 2000.0  # grows only by folding realised profit in
    pocket_usd: float = 0.0  # realised profit not yet folded
    folds: int = 0
    rounds_seen: Dict[str, int] = field(default_factory=dict)
    graduated: List[str] = field(default_factory=list)
    # Anchor timestamps this book has already started a line on. An anchor is
    # used ONCE: re-seeding the same dead high every cycle is what the
    # 2026-08-21 runaway was made of.
    tried_anchors: List[int] = field(default_factory=list)
    # No new line before this wall-clock time — one full 5m bar after every
    # start, so even a brand-new anchor cannot churn faster than the chart.
    next_seed_ts: float = 0.0
    last_error: str = ""
    # What the book is doing when it is not starting anything. A book that
    # is correctly WAITING looks identical to a broken one without this.
    note: str = ""

    @property
    def key(self) -> str:
        return f"{self.symbol}:{self.exchange}".lower()

    @property
    def wallet_cap_usd(self) -> float:
        """The most this symbol may ever have in coin at one moment."""
        return round(self.purse_usd * WALLET_FRACTION, 2)

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
            "rounds_seen": dict(self.rounds_seen),
            "graduated": list(self.graduated),
            "tried_anchors": list(self.tried_anchors),
            "next_seed_ts": self.next_seed_ts,
            "last_error": self.last_error,
            "note": self.note,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Book":
        book = cls(symbol=str(data.get("symbol") or "").upper())
        book.exchange = str(data.get("exchange") or "")
        # A live book restored while the server is not armed comes back as
        # PAPER. Saved state must never be able to resume real trading that
        # the operator has since switched off.
        raw_mode = str(data.get("mode") or "").lower()
        book.mode = "live" if (raw_mode == "live" and LIVE_ARMED) else "paper"
        book.enabled = bool(data.get("enabled"))
        book.start_capital_usd = _positive(data.get("start_capital_usd"), 2000.0)
        book.purse_usd = _positive(data.get("purse_usd"), book.start_capital_usd)
        book.pocket_usd = float(data.get("pocket_usd") or 0.0)
        book.folds = int(data.get("folds") or 0)
        book.rounds_seen = {str(k): int(v) for k, v in (data.get("rounds_seen") or {}).items()}
        book.graduated = [str(x) for x in (data.get("graduated") or [])]
        book.tried_anchors = [int(x) for x in (data.get("tried_anchors") or [])]
        book.next_seed_ts = float(data.get("next_seed_ts") or 0.0)
        book.last_error = str(data.get("last_error") or "")
        book.note = str(data.get("note") or "")
        return book


def _positive(value, fallback: float) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return fallback
    return number if number > 0 else fallback


def latest_swing_high(candles, close: float, swing_bars: int = SWING_BARS, exclude_ts=()) -> Optional[object]:
    """The most recent confirmed 5m swing high still standing above price.

    Confirmed means `swing_bars` bars either side are lower, so the high is
    only ever anchored once the bars after it have printed — the campaign
    starts strictly in the past, the way Phil marks one after a high has
    clearly failed. `exclude_ts` are anchors already used up; the scan walks
    past them to the next older candidate rather than serving the same high
    twice. Returns the candle, or None to try again next bar.
    """
    if not candles:
        return None
    skip = {int(x) for x in exclude_ts}
    rows = list(candles)
    last = len(rows) - 1
    oldest = max(last - SWING_LOOKBACK_BARS, swing_bars)
    for pivot in range(last - swing_bars, oldest - 1, -1):
        if pivot < swing_bars:
            break
        high = rows[pivot].high
        if high <= close:
            continue  # price is already above it — not a high to fall from
        if int(rows[pivot].timestamp) in skip:
            continue  # already tried — a dead anchor is never re-seeded
        window = rows[pivot - swing_bars : min(pivot + swing_bars, last) + 1]
        if any(row.high > high for row in window if row is not rows[pivot]):
            continue
        return rows[pivot]
    return None


class AutoCascadeFib:
    """The driver. Owns no orders — it only starts campaigns and sets budgets."""

    def __init__(self, engine):
        self.engine = engine
        self.books: Dict[str, Book] = {}
        self._last_tick_ts: float = 0.0

    # ── configuration ────────────────────────────────────────────

    def _venue_broker(self, exchange: str = ""):
        """The client a book on `exchange` trades through.

        A blank venue is the engine's own default — the only venue there was
        before CoinDCX joined. A named venue resolves through the engine's
        registry, and an unknown one resolves to None rather than the default:
        a book must never read one exchange's keys and send its orders to
        another.
        """
        name = str(exchange or "").strip().lower()
        if not name or name == str(getattr(self.engine, "primary_broker_name", "") or "").lower():
            return getattr(self.engine, "broker", None)
        return (getattr(self.engine, "brokers", None) or {}).get(name)

    def _normalise_exchange(self, exchange: str = "") -> str:
        """The default venue is stored BLANK, whatever name the page sent.

        The book key is `symbol:exchange`, so a book saved as "binance" and
        one saved as "" would be two pots on the same account, each blind to
        the other's claims. Unknown venues are refused here, at the door.
        """
        name = str(exchange or "").strip().lower()
        if name == str(getattr(self.engine, "primary_broker_name", "") or "").lower():
            return ""
        if name and self._venue_broker(name) is None:
            known = ", ".join(sorted(getattr(self.engine, "brokers", None) or {})) or "its default only"
            raise ValueError(f"This strategy's engine has no client for '{name}' (it has: {known}).")
        return name

    def live_available(self, exchange: str = "") -> bool:
        """True only when real orders could actually be placed on `exchange`.

        Three independent locks, all of which must be open: the server arm
        (CRYPTOFORGE_AUTO_FIB_LIVE), THAT venue's own keys, and — implicitly —
        the strategy engine having been built with an armed broker, since the
        arm is captured at construction and never flips under a running
        campaign. Checked per venue: Binance keys say nothing about CoinDCX.
        """
        broker = self._venue_broker(exchange)
        if broker is None or not getattr(broker, "live_armed", False):
            return False
        checker = getattr(broker, "_is_configured", None)
        return bool(checker()) if callable(checker) else False

    def _live_refusal(self, exchange: str = "") -> str:
        """Why live is not on offer here — the arm, or THIS venue's keys."""
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

    def claimed_base_qty(self, symbol: str, venue: str = "") -> float:
        """Coin this strategy's campaigns hold, for the OTHER engine to net off.

        Both engines trade one account, and each one's own campaign list sees
        only half the claims on a symbol's balance. This is the half the live
        Cascade cannot see.
        """
        from engine.cascade import FINAL_STATES  # noqa: F401  (kept explicit for the reader)

        symbol = str(symbol or "").upper()
        venue = str(venue or "").lower()
        total = 0.0
        for campaign in list(getattr(self.engine, "campaigns", {}).values()):
            if str(campaign.symbol or "").upper() != symbol:
                continue
            if str(getattr(campaign, "mode", "") or "") != "live":
                continue  # paper coin is imaginary and claims nothing
            if venue and str(self.engine.venue_of(campaign) or "").lower() != venue:
                continue
            total += float(getattr(campaign, "filled_base_qty", 0.0) or 0.0)
            total += float(getattr(campaign, "residual_base_qty", 0.0) or 0.0)
        return total

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
            # Refuse rather than accept-and-ignore: a book that reads "On" over
            # a driver that will never tick is a trap this strategy has already
            # fallen into once.
            raise ValueError("Auto-Cascade_Fib is switched off on the server (CRYPTOFORGE_AUTO_FIB=0).")
        exchange = self._normalise_exchange(exchange)
        wants_live = mode is not None and str(mode).lower() == "live"
        if wants_live and not self.live_available(exchange):
            # Refuse rather than accept-and-ignore: a book reading "live" over
            # a broker that cannot place an order is the trap this strategy
            # has already fallen into once.
            raise ValueError(self._live_refusal(exchange))
        if wants_live and _positive(capital_usd, 0.0) > LIVE_CEILING_USD:
            raise ValueError(
                f"A live book may hold at most ${LIVE_CEILING_USD:,.0f}. "
                f"Lower the purse, or raise CRYPTOFORGE_AUTO_FIB_LIVE_MAX on the server."
            )
        key = f"{symbol}:{exchange}".lower()
        book = self.books.get(key) or Book(symbol=symbol, exchange=exchange)
        if enabled is not None:
            book.enabled = bool(enabled)
        if mode is not None:
            book.mode = "live" if wants_live else "paper"
        if capital_usd is not None:
            fresh = _positive(capital_usd, book.start_capital_usd)
            # Changing the size before any profit has been folded resets the
            # purse with it; afterwards it only moves the starting figure, so a
            # compounded purse is never quietly shrunk back.
            book.start_capital_usd = fresh
            if book.folds == 0 and book.pocket_usd <= 0:
                book.purse_usd = fresh
        self.books[key] = book
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
                "tp_fib_level": TP_FIB_LEVEL,
                "cap_timeframe": CAP_TIMEFRAME,
                "graduate_at": GRADUATE_TIMEFRAME,
                "wallet_fraction": WALLET_FRACTION,
                "fold_at_fraction": FOLD_AT_FRACTION,
            },
            "books": [
                {
                    **book.to_dict(),
                    "wallet_cap_usd": book.wallet_cap_usd,
                    "fold_threshold_usd": book.fold_threshold_usd,
                    "in_coin_usd": round(self._in_coin(book), 2),
                    # Lines still RUNNING. Counting the dead ones made the page
                    # say "1 line running" over an empty panel.
                    "campaigns": len(self._live_campaigns(book)),
                    "working_line": self._working_line_id(book),
                }
                for book in sorted(self.books.values(), key=lambda b: b.symbol)
            ],
        }

    def load(self, payload: dict) -> None:
        self.books = {}
        for row in (payload or {}).get("books") or []:
            if not isinstance(row, dict):
                continue
            book = Book.from_dict(row)
            if book.symbol:
                self.books[book.key] = book

    def dump(self) -> dict:
        return {"books": [book.to_dict() for book in self.books.values()]}

    # ── reading the engine ───────────────────────────────────────

    def _own_campaigns(self, book: Book) -> List:
        out = []
        for campaign in list(getattr(self.engine, "campaigns", {}).values()):
            if str(getattr(campaign, "strategy", "")) != STRATEGY:
                continue
            if str(campaign.symbol or "").upper() != book.symbol:
                continue
            if str(getattr(campaign, "exchange", "") or "") != book.exchange:
                continue
            out.append(campaign)
        return out

    def _live_campaigns(self, book: Book) -> List:
        from engine.cascade import FINAL_STATES

        return [c for c in self._own_campaigns(book) if c.state not in FINAL_STATES]

    def _in_coin(self, book: Book) -> float:
        return sum(float(getattr(c, "spent_usd", 0.0) or 0.0) for c in self._own_campaigns(book))

    def _working_line_id(self, book: Book) -> str:
        for campaign in self._live_campaigns(book):
            if str(campaign.mc_kind or "major").lower() == "minor":
                return campaign.campaign_id
        return ""

    # ── the money ────────────────────────────────────────────────

    def _bank_and_fold(self, book: Book) -> bool:
        """Move newly closed profit into the pocket, and fold at 25%."""
        changed = False
        for campaign in self._own_campaigns(book):
            seen = book.rounds_seen.get(campaign.campaign_id, 0)
            rounds = list(getattr(campaign, "rounds", []) or [])
            if len(rounds) <= seen:
                continue
            gained = sum(float(getattr(r, "pnl", 0.0) or 0.0) for r in rounds[seen:])
            book.rounds_seen[campaign.campaign_id] = len(rounds)
            book.pocket_usd = round(book.pocket_usd + gained, 8)
            changed = True
        # A pocket worth a quarter of the purse goes in WHOLE, and the wallet
        # cap grows with it. Whole, not in quarter-sized slices: Phil's rule is
        # "$100 purse, $25 profit, next trade sizes off $125". So one round
        # paying ten times the threshold still folds exactly once, and the
        # purse jumps by the full amount — there is never a remainder left
        # sitting in the pocket.
        if book.pocket_usd >= book.fold_threshold_usd > 0:
            book.purse_usd = round(book.purse_usd + book.pocket_usd, 8)
            book.pocket_usd = 0.0
            book.folds += 1
            changed = True
            _log.info(
                "[AUTO-FIB] %s folded profit in — purse now $%.2f, wallet cap $%.2f",
                book.symbol,
                book.purse_usd,
                book.wallet_cap_usd,
            )
        return changed

    def _apply_wallet_cap(self, book: Book) -> bool:
        """Keep the symbol's capital group equal to half the purse."""
        setter = getattr(self.engine, "set_capital_group", None)
        if setter is None:
            return False
        groups = getattr(self.engine, "capital_groups", {}) or {}
        key = f"{book.symbol}:{book.exchange}".lower()
        current = float(groups.get(key) or 0.0)
        wanted = book.wallet_cap_usd
        if abs(current - wanted) < 0.01:
            return False
        setter(book.symbol, wanted, book.exchange)
        return True

    # ── the working line ─────────────────────────────────────────

    def _graduate(self, book: Book) -> bool:
        """A 5m line that has climbed to 1h is a major from now on."""
        changed = False
        from engine.cascade import timeframe_seconds

        threshold = timeframe_seconds(GRADUATE_TIMEFRAME)
        for campaign in self._live_campaigns(book):
            if str(campaign.mc_kind or "major").lower() != "minor":
                continue
            if timeframe_seconds(campaign.timeframe) < threshold:
                continue
            campaign.mc_kind = "major"
            if campaign.campaign_id not in book.graduated:
                book.graduated.append(campaign.campaign_id)
            changed = True
            _log.info(
                "[AUTO-FIB] %s line %s reached %s — it is a major now, a fresh 5m line follows",
                book.symbol,
                campaign.campaign_id,
                campaign.timeframe,
            )
        return changed

    async def _latest_1m_high(self, book: Book) -> Optional[float]:
        """The last closed 1m candle's high — the freshest price we can hold.

        The engine judges mother breaks on the live 1m tape, so an anchor is
        only honest if it still stands above THIS, not merely above a 5m close
        that can be five minutes stale. That gap is how the runaway's anchor
        was already broken before its campaign existed.
        """
        try:
            rows = await self.engine._fetch_closed_candles(book.symbol, int(time.time()) - 600, timeframe="1m")
        except Exception:
            return None
        return float(rows[-1].high) if rows else None

    async def _seed_working_line(self, book: Book) -> bool:
        """Anchor a fresh 5m line on the latest confirmed swing high."""
        if self._working_line_id(book):
            book.note = "working a 5m line"
            return False
        if self._in_coin(book) >= book.wallet_cap_usd:
            book.note = "wallet full — no room to start another line"
            return False
        now = time.time()
        if now < book.next_seed_ts:
            # The churn brake. Whatever else looks startable, at most one start
            # per closed 5m bar — the 2026-08-21 runaway managed one every 15s.
            book.note = "cooling down after the last start"
            return False
        candles = await self.engine._fetch_closed_candles(book.symbol, int(now) - SWING_LOOKBACK_BARS * 300)
        if not candles:
            book.note = "no candles yet"
            return False
        anchor = latest_swing_high(candles, candles[-1].close, exclude_ts=book.tried_anchors)
        if anchor is None:
            # The ordinary case in a rising market, and the one that looks like
            # a fault: every high above the price is too recent to be confirmed
            # failed, so there is nothing honest to hang a mother on yet.
            book.note = "waiting for a high to fail above the price"
            return False
        if book.mode == "live":
            # Re-checked at START, not only when the book was saved: the purse
            # grows by folding profit in, so a book that was inside the ceiling
            # when it was created can drift past it without anyone touching it.
            if not self.live_available(book.exchange):
                book.note = "live is not armed on the server — not starting"
                return False
            if book.wallet_cap_usd > LIVE_CEILING_USD:
                book.note = f"live wallet cap ${book.wallet_cap_usd:,.0f} is over the ${LIVE_CEILING_USD:,.0f} ceiling"
                return False
        fresh_high = await self._latest_1m_high(book)
        if fresh_high is not None and anchor.high <= fresh_high:
            # The 1m tape has already reached the anchor. Starting now would be
            # born broken — the runaway's opening move. Do not blacklist it:
            # if price falls back below, this high becomes honest again.
            book.note = "the nearest failed high is already being retested — waiting"
            return False
        result = await self.engine.start_campaign(
            symbol=book.symbol,
            capital_usd=book.purse_usd,
            mother_high=anchor.high,
            mother_low=anchor.low,
            mother_timestamp=anchor.timestamp,
            mode=book.mode,
            timeframe="5m",
            mc_kind="minor",
            exchange=book.exchange,
            strategy=STRATEGY,
            tp_fib_level=TP_FIB_LEVEL,
            cap_timeframe=CAP_TIMEFRAME,
        )
        if result.get("error"):
            book.last_error = str(result["error"])
            # A failed start still opens the cooldown — retrying an error at
            # the monitor's pace is a different runaway, not a fix. The anchor
            # is NOT blacklisted: the error may be transient.
            book.next_seed_ts = now + SEED_COOLDOWN_SEC
            return False
        book.last_error = ""
        book.note = "started a 5m line"
        book.tried_anchors.append(int(anchor.timestamp))
        del book.tried_anchors[:-TRIED_ANCHOR_LIMIT]
        book.next_seed_ts = now + SEED_COOLDOWN_SEC
        _log.info("[AUTO-FIB] %s new 5m line on the swing high at %.8f", book.symbol, anchor.high)
        return True

    # ── the tick ─────────────────────────────────────────────────

    async def tick(self) -> bool:
        """Called once per monitor cycle. Does nothing unless a book is on."""
        # The kill switch comes FIRST, before any book is read. A book left
        # enabled in saved state must not be able to wake this up.
        if not DRIVER_ARMED:
            for book in self.books.values():
                if book.enabled and book.note != DISARMED_NOTE:
                    book.note = DISARMED_NOTE
            return False
        changed = False
        for book in list(self.books.values()):
            if not book.enabled:
                continue
            try:
                changed |= self._bank_and_fold(book)
                changed |= self._apply_wallet_cap(book)
                changed |= self._graduate(book)
                changed |= await self._seed_working_line(book)
            except Exception as exc:  # one bad book must not stop the others
                book.last_error = str(exc)
                _log.warning("[AUTO-FIB] %s tick failed: %s", book.symbol, exc)
        self._last_tick_ts = time.time()
        return changed
