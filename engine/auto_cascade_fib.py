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
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

_log = logging.getLogger("cryptoforge.auto_cascade_fib")

STRATEGY = "auto-cascade-fib"

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
            "last_error": self.last_error,
            "note": self.note,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Book":
        book = cls(symbol=str(data.get("symbol") or "").upper())
        book.exchange = str(data.get("exchange") or "")
        book.mode = "live" if str(data.get("mode") or "").lower() == "live" else "paper"
        book.enabled = bool(data.get("enabled"))
        book.start_capital_usd = _positive(data.get("start_capital_usd"), 2000.0)
        book.purse_usd = _positive(data.get("purse_usd"), book.start_capital_usd)
        book.pocket_usd = float(data.get("pocket_usd") or 0.0)
        book.folds = int(data.get("folds") or 0)
        book.rounds_seen = {str(k): int(v) for k, v in (data.get("rounds_seen") or {}).items()}
        book.graduated = [str(x) for x in (data.get("graduated") or [])]
        book.last_error = str(data.get("last_error") or "")
        book.note = str(data.get("note") or "")
        return book


def _positive(value, fallback: float) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return fallback
    return number if number > 0 else fallback


def latest_swing_high(candles, close: float, swing_bars: int = SWING_BARS) -> Optional[object]:
    """The most recent confirmed 5m swing high still standing above price.

    Confirmed means `swing_bars` bars either side are lower, so the high is
    only ever anchored once the bars after it have printed — the campaign
    starts strictly in the past, the way Phil marks one after a high has
    clearly failed. Returns the candle, or None to try again next bar.
    """
    if not candles:
        return None
    rows = list(candles)
    last = len(rows) - 1
    oldest = max(last - SWING_LOOKBACK_BARS, swing_bars)
    for pivot in range(last - swing_bars, oldest - 1, -1):
        if pivot < swing_bars:
            break
        high = rows[pivot].high
        if high <= close:
            continue  # price is already above it — not a high to fall from
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
        key = f"{symbol}:{exchange}".lower()
        book = self.books.get(key) or Book(symbol=symbol, exchange=exchange)
        if enabled is not None:
            book.enabled = bool(enabled)
        if mode is not None:
            book.mode = "live" if str(mode).lower() == "live" else "paper"
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

    async def _seed_working_line(self, book: Book) -> bool:
        """Anchor a fresh 5m line on the latest confirmed swing high."""
        if self._working_line_id(book):
            book.note = "working a 5m line"
            return False
        if self._in_coin(book) >= book.wallet_cap_usd:
            book.note = "wallet full — no room to start another line"
            return False
        candles = await self.engine._fetch_closed_candles(book.symbol, int(time.time()) - SWING_LOOKBACK_BARS * 300)
        if not candles:
            book.note = "no candles yet"
            return False
        anchor = latest_swing_high(candles, candles[-1].close)
        if anchor is None:
            # The ordinary case in a rising market, and the one that looks like
            # a fault: every high above the price is too recent to be confirmed
            # failed, so there is nothing honest to hang a mother on yet.
            book.note = "waiting for a high to fail above the price"
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
            return False
        book.last_error = ""
        book.note = "started a 5m line"
        _log.info("[AUTO-FIB] %s new 5m line on the swing high at %.8f", book.symbol, anchor.high)
        return True

    # ── the tick ─────────────────────────────────────────────────

    async def tick(self) -> bool:
        """Called once per monitor cycle. Does nothing unless a book is on."""
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
