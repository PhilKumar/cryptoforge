"""engine/option_seller_paper.py — the 4 PM BTC option seller, PAPER ONLY.

The rule, as it came out of the 17-Sep-2026 research (memory note
proj_delta_momentum_option_backtest):

  At 16:00 IST, measure how far Delta's own BTC index (.DEXBTUSD) has moved over
  the last 2, 3, 4, 6, 8 and 12 hours. Each window "votes" for a side when its
  move is at least 0.75% x sqrt(window / 6h) — a wider window must move more.
  If at least 5 of the 6 windows vote the SAME way, SELL the at-the-money
  option of today's expiry in that direction (up -> call, down -> put).
  Buy it back when its mark price doubles (the stop), or at 17:25 IST, five
  minutes before the 17:30 IST settlement. At most one trade a day.

Tested on 405 days of Delta's real marks, chosen on the first half only and
scored on the second: +1,710 per 1 BTC over 30 unseen trades with a worst
losing run of 308 — about the same money as the best single setting with a
quarter of its drawdown. Thirty trades is a thin sample, which is exactly why
this runs on paper first.

PAPER ONLY, BY CONSTRUCTION. This module reads Delta's PUBLIC market data and
nothing else: no API key, no signed request, no order call anywhere in it. A
test reads this file and fails if an order path ever appears.

What paper is FOR: to find out whether the live tape matches the backtest.
Every trade is therefore recorded twice —
  · at the MARK price, which is what the backtest used, and
  · at the real QUOTES (sell at the bid, buy back at the ask), which is what an
    order would actually get.
and the stop is judged both by the 15-second poll that a live order would rely
on AND by the 1-minute candle highs the backtest used, so a stop the poll
missed is visible rather than silently flattering the result.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import math
import time
from typing import Awaitable, Callable, Dict, List, Optional

_log = logging.getLogger("cryptoforge.option_seller")

STRATEGY = "option-seller"
BASE_URL = "https://api.india.delta.exchange/v2"
UNDERLYING = ".DEXBTUSD"
MARK_PREFIX = "MARK:"

LOOKBACKS_MIN = (120, 180, 240, 360, 480, 720)
BASE_MOVE_PCT = 0.75
SCALE_LOOKBACK_MIN = 360
MIN_VOTES = 5
STOP_MULT = 2.0

ENTRY_UTC = (10, 30)  # 16:00 IST
EXIT_UTC = (11, 55)  # 17:25 IST
SETTLE_UTC = (12, 0)  # 17:30 IST — Delta's daily BTC option settlement
# If the app was down at 16:00 it may still enter up to this late. Later than
# that the premium has decayed away from what the rule was tested on.
ENTRY_GRACE_SEC = 300

CONTRACT_BTC = 0.001
FEE_NOTIONAL = 0.0003
FEE_PREMIUM_CAP = 0.035
GST = 1.18

DEFAULT_SIZE_BTC = 0.1
MAX_SIZE_BTC = 10.0
EVENT_LIMIT = 200
DAY_LIMIT = 400

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

FetchJson = Callable[[str, dict], Awaitable[dict]]


def threshold_pct(lookback_min: int) -> float:
    """How far a window must move to vote — wider windows must move more."""
    return BASE_MOVE_PCT * math.sqrt(lookback_min / SCALE_LOOKBACK_MIN)


def fee_per_btc(price: float, spot: float) -> float:
    """Delta's option fee for one side, per 1 BTC of underlying, GST included."""
    return min(FEE_NOTIONAL * spot, FEE_PREMIUM_CAP * price) * GST


def count_votes(p0: float, refs: Dict[int, float]) -> Dict[str, object]:
    """The decision, as a pure function of the 16:00 price and each window's start.

    Returns the per-window moves, the votes on each side and the side to sell
    ("C", "P" or "" for no trade).
    """
    moves, votes = {}, {"C": 0, "P": 0}
    for lb in LOOKBACKS_MIN:
        ref = refs.get(lb)
        if not ref or ref <= 0 or p0 <= 0:
            continue
        move = (p0 - ref) / ref * 100.0
        moves[lb] = move
        if abs(move) >= threshold_pct(lb):
            votes["C" if move > 0 else "P"] += 1
    sides = [s for s, v in votes.items() if v >= MIN_VOTES]
    return {"moves": moves, "votes": votes, "side": sides[0] if len(sides) == 1 else ""}


def _utc(day: dt.date, hm) -> dt.datetime:
    return dt.datetime(day.year, day.month, day.day, hm[0], hm[1], tzinfo=dt.timezone.utc)


def _ist(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts, IST).strftime("%Y-%m-%d %H:%M:%S IST")


async def _requests_fetch(path: str, params: dict) -> dict:
    """Public GET against Delta. Runs in a thread: the app has ONE event loop."""
    import requests

    def go():
        r = requests.get(BASE_URL + path, params=params, timeout=10)
        r.raise_for_status()
        return r.json()

    return await asyncio.to_thread(go)


class OptionSellerPaper:
    """One book, one trade a day at most, paper only."""

    def __init__(self, fetch_json: Optional[FetchJson] = None, clock: Callable[[], float] = time.time):
        self._fetch = fetch_json or _requests_fetch
        self._clock = clock
        self.enabled = False
        self.size_btc = DEFAULT_SIZE_BTC
        self.days: Dict[str, dict] = {}
        self.events: List[dict] = []
        self._products_cache: Dict[str, List[dict]] = {}

    # ── persistence ──────────────────────────────────────────────────

    def dump(self) -> dict:
        return {
            "enabled": self.enabled,
            "size_btc": self.size_btc,
            "days": self.days,
            "events": self.events[-EVENT_LIMIT:],
        }

    def load(self, state: Optional[dict]) -> None:
        state = state or {}
        self.enabled = bool(state.get("enabled", False))
        try:
            self.size_btc = self._clean_size(state.get("size_btc", DEFAULT_SIZE_BTC))
        except ValueError:
            self.size_btc = DEFAULT_SIZE_BTC
        self.days = dict(state.get("days") or {})
        self.events = list(state.get("events") or [])[-EVENT_LIMIT:]

    @staticmethod
    def _clean_size(value) -> float:
        try:
            size = float(value)
        except (TypeError, ValueError):
            raise ValueError("size_btc must be a number")
        if not math.isfinite(size) or size <= 0 or size > MAX_SIZE_BTC:
            raise ValueError(f"size_btc must be above 0 and at most {MAX_SIZE_BTC:g}")
        # Delta trades whole contracts of 0.001 BTC.
        size = round(size / CONTRACT_BTC) * CONTRACT_BTC
        if size < CONTRACT_BTC:
            raise ValueError("size_btc must be at least one contract (0.001 BTC)")
        return round(size, 3)

    def configure(self, enabled=None, size_btc=None) -> None:
        if size_btc is not None:
            size = self._clean_size(size_btc)
            if size != self.size_btc:
                self.size_btc = size
                self._event("info", f"Paper size set to {size:g} BTC ({round(size / CONTRACT_BTC)} contracts)")
        if enabled is not None and not isinstance(enabled, bool):
            raise ValueError("enabled must be true or false")
        if enabled is not None and enabled != self.enabled:
            self.enabled = bool(enabled)
            self._event("info", "Paper trading switched ON" if self.enabled else "Paper trading switched OFF")

    # ── the tick ─────────────────────────────────────────────────────

    async def tick(self) -> bool:
        """Advance today's state. Returns True when something changed."""
        now = self._clock()
        today = dt.datetime.fromtimestamp(now, dt.timezone.utc).date()
        key = today.isoformat()
        entry_ts = _utc(today, ENTRY_UTC).timestamp()
        changed = False
        # An open position is ALWAYS managed first, whatever day it was opened
        # and even after the book is switched off: off means "open nothing
        # new", never "abandon what is held". A restart across midnight UTC
        # must not orphan yesterday's position.
        for held in [d for d in self.days.values() if d.get("status") == "open"]:
            changed = await self._manage(held, now) or changed
        rec = self.days.get(key)
        if rec and rec.get("status") == "open":
            return changed
        if not self.enabled or rec is not None:
            return changed
        if now < entry_ts:
            return changed
        if now > entry_ts + ENTRY_GRACE_SEC:
            self.days[key] = {
                "date": key,
                "status": "missed",
                "reason": "no decision at 16:00 IST — switched on later, or the app was not running",
            }
            self._event("warn", f"{key}: no 16:00 decision today — switched on later, or the app was not running")
            self._trim()
            return True
        return await self._decide(key, today, entry_ts) or changed

    async def _decide(self, key: str, today: dt.date, entry_ts: float) -> bool:
        first = entry_ts - max(LOOKBACKS_MIN) * 60 - 300
        try:
            payload = await self._fetch(
                "/history/candles",
                {"resolution": "1m", "symbol": UNDERLYING, "start": int(first), "end": int(entry_ts) + 120},
            )
        except Exception as exc:
            _log.warning("[OPTION-SELLER] index read failed, retrying: %s", exc)
            return False
        bars = {int(c["time"]): c for c in (payload.get("result") or [])}
        if int(entry_ts) not in bars:
            return False  # the 16:00 candle has not printed yet — try again next tick
        p0 = float(bars[int(entry_ts)]["open"])
        refs = {}
        ordered = sorted(bars)
        for lb in LOOKBACKS_MIN:
            at = int(entry_ts) - lb * 60
            if at in bars:
                refs[lb] = float(bars[at]["close"])
            else:
                earlier = [t for t in ordered if t <= at]
                if earlier:
                    refs[lb] = float(bars[earlier[-1]]["close"])
        decision = count_votes(p0, refs)
        rec = {
            "date": key,
            "index_at_entry": p0,
            "moves": {str(k): round(v, 4) for k, v in decision["moves"].items()},
            "votes": decision["votes"],
            "side": decision["side"],
        }
        if len(refs) < len(LOOKBACKS_MIN):
            rec.update(status="skipped", reason="the index history was incomplete — no decision")
            self.days[key] = rec
            self._event("warn", f"{key}: index history incomplete, no trade")
            self._trim()
            return True
        if not decision["side"]:
            votes = decision["votes"]
            rec.update(
                status="skipped",
                reason=f"no agreement — {votes['C']} windows up, {votes['P']} down, {MIN_VOTES} needed",
            )
            self.days[key] = rec
            self._event("info", f"{key}: no trade — {votes['C']} up / {votes['P']} down, {MIN_VOTES} needed")
            self._trim()
            return True
        try:
            contract = await self._atm_contract(today, decision["side"], p0)
            quote = await self._quote(contract["symbol"])
        except Exception as exc:
            rec.update(status="error", reason=f"could not price today's option: {exc}")
            self.days[key] = rec
            self._event("error", f"{key}: signal to sell {self._side_name(decision['side'])}, but {exc}")
            self._trim()
            return True
        mark = quote["mark"]
        if mark <= 0:
            rec.update(status="error", reason="the option had no mark price")
            self.days[key] = rec
            self._event("error", f"{key}: {contract['symbol']} had no mark price — no trade")
            self._trim()
            return True
        rec.update(
            status="open",
            symbol=contract["symbol"],
            strike=contract["strike"],
            size_btc=self.size_btc,
            contracts=round(self.size_btc / CONTRACT_BTC),
            entry_ts=entry_ts,
            entry_seen_ts=self._clock(),
            entry_mark=mark,
            entry_bid=quote["bid"],
            entry_ask=quote["ask"],
            spot=quote["spot"] or p0,
            stop_px=mark * STOP_MULT,
            last_mark=mark,
            last_seen_ts=self._clock(),
        )
        self.days[key] = rec
        self._event(
            "trade",
            f"{key}: SOLD {rec['contracts']} x {contract['symbol']} (paper) at mark {mark:,.2f}"
            f" (bid {self._px(quote['bid'])}) — {decision['votes'][decision['side']]} of 6 windows agreed;"
            f" stop at {rec['stop_px']:,.2f}",
        )
        self._trim()
        return True

    async def _manage(self, rec: dict, now: float) -> bool:
        day = dt.date.fromisoformat(rec["date"])
        exit_ts = _utc(day, EXIT_UTC).timestamp()
        settle_ts = _utc(day, SETTLE_UTC).timestamp()
        try:
            quote = await self._quote(rec["symbol"])
        except Exception as exc:
            if now >= settle_ts:
                # The contract has settled and no quote will come. Close it on
                # the last mark seen, and say so — never leave it open forever.
                return await self._close(
                    rec,
                    "time",
                    {"mark": rec["last_mark"], "bid": 0.0, "ask": 0.0},
                    now,
                    note=f"no quote after settlement ({exc}); closed on the last mark seen",
                )
            _log.warning("[OPTION-SELLER] %s quote failed, retrying: %s", rec["symbol"], exc)
            return False
        mark = quote["mark"]
        if mark > 0:
            rec["last_mark"] = mark
            rec["last_seen_ts"] = now
        if mark >= rec["stop_px"]:
            return await self._close(rec, "stop", quote, now)
        if now >= exit_ts:
            return await self._close(rec, "time", quote, now)
        return False

    async def _close(self, rec: dict, why: str, quote: dict, now: float, note: str = "") -> bool:
        exit_mark = quote["mark"] if quote["mark"] > 0 else rec["last_mark"]
        size = rec["size_btc"]
        spot = rec["spot"]
        fees = fee_per_btc(rec["entry_mark"], spot) + fee_per_btc(exit_mark, spot)
        per_btc_mark = rec["entry_mark"] - exit_mark - fees
        rec.update(
            status="closed",
            exit_why=why,
            exit_ts=now,
            exit_mark=exit_mark,
            exit_bid=quote.get("bid", 0.0),
            exit_ask=quote.get("ask", 0.0),
            fees_per_btc=fees,
            pnl_per_btc_mark=per_btc_mark,
            pnl_usd_mark=per_btc_mark * size,
        )
        # The honest twin: what the ORDERS would have got.
        if rec.get("entry_bid") and quote.get("ask"):
            q_fees = fee_per_btc(rec["entry_bid"], spot) + fee_per_btc(quote["ask"], spot)
            per_btc_q = rec["entry_bid"] - quote["ask"] - q_fees
            rec.update(pnl_per_btc_quote=per_btc_q, pnl_usd_quote=per_btc_q * size)
        else:
            rec.update(pnl_per_btc_quote=None, pnl_usd_quote=None)
        if note:
            rec["note"] = note
        # Did a 1m candle touch the stop that the 15s poll never saw? That gap
        # is the difference between this paper book and the backtest.
        rec["stop_touched_by_candle"] = await self._candle_touched_stop(rec, now)
        self._event(
            "trade",
            f"{rec['date']}: BOUGHT BACK {rec['symbol']} (paper, {why}) at mark {exit_mark:,.2f} —"
            f" {self._money(rec['pnl_usd_mark'])} at mark"
            + (
                f", {self._money(rec['pnl_usd_quote'])} at the real quotes"
                if rec.get("pnl_usd_quote") is not None
                else ""
            )
            + (" — a 1m candle HAD touched the stop earlier" if rec["stop_touched_by_candle"] and why != "stop" else "")
            + (f" ({note})" if note else ""),
        )
        return True

    async def _candle_touched_stop(self, rec: dict, now: float) -> Optional[bool]:
        try:
            payload = await self._fetch(
                "/history/candles",
                {
                    "resolution": "1m",
                    "symbol": MARK_PREFIX + rec["symbol"],
                    "start": int(rec["entry_ts"]),
                    "end": int(min(now, rec["entry_ts"] + 90 * 60)),
                },
            )
        except Exception:
            return None
        rows = [c for c in (payload.get("result") or []) if int(c["time"]) > int(rec["entry_ts"])]
        if not rows:
            return None
        return any(float(c["high"]) >= rec["stop_px"] for c in rows)

    # ── market data ──────────────────────────────────────────────────

    async def _atm_contract(self, today: dt.date, side: str, spot: float) -> dict:
        settle = _utc(today, SETTLE_UTC)
        key = f"{today.isoformat()}|{side}"
        rows = self._products_cache.get(key)
        if rows is None:
            rows = []
            after = None
            for _ in range(20):
                params = {
                    "contract_types": "call_options" if side == "C" else "put_options",
                    "states": "live",
                    "page_size": 500,
                }
                if after:
                    params["after"] = after
                page = await self._fetch("/products", params)
                for p in page.get("result") or []:
                    if (p.get("underlying_asset") or {}).get("symbol") != "BTC":
                        continue
                    st = str(p.get("settlement_time") or "")
                    try:
                        when = dt.datetime.fromisoformat(st.replace("Z", "+00:00"))
                    except ValueError:
                        continue
                    if when != settle:
                        continue
                    rows.append({"symbol": p["symbol"], "strike": float(p.get("strike_price") or 0)})
                after = (page.get("meta") or {}).get("after")
                if not after:
                    break
            self._products_cache = {key: rows}
        if not rows:
            raise RuntimeError(f"Delta lists no BTC {self._side_name(side)} settling today")
        return min(rows, key=lambda r: abs(r["strike"] - spot))

    async def _quote(self, symbol: str) -> dict:
        payload = await self._fetch(f"/tickers/{symbol}", {})
        t = payload.get("result") or {}
        quotes = t.get("quotes") or {}

        def num(v):
            try:
                x = float(v)
            except (TypeError, ValueError):
                return 0.0
            return x if math.isfinite(x) and x > 0 else 0.0

        return {
            "mark": num(t.get("mark_price")),
            "bid": num(quotes.get("best_bid")),
            "ask": num(quotes.get("best_ask")),
            "spot": num(t.get("spot_price")),
        }

    # ── reporting ────────────────────────────────────────────────────

    def status(self) -> dict:
        now = self._clock()
        today = dt.datetime.fromtimestamp(now, dt.timezone.utc).date()
        entry = _utc(today, ENTRY_UTC)
        if now > entry.timestamp() + ENTRY_GRACE_SEC:
            entry = _utc(today + dt.timedelta(days=1), ENTRY_UTC)
        closed = [d for d in self.days.values() if d.get("status") == "closed"]
        closed.sort(key=lambda d: d["date"])

        def total(field):
            vals = [d.get(field) for d in closed if d.get(field) is not None]
            return round(sum(vals), 2) if vals else None

        eq = peak = dd = 0.0
        for d in closed:
            eq += d.get("pnl_usd_mark") or 0.0
            peak = max(peak, eq)
            dd = max(dd, peak - eq)
        days = sorted(self.days.values(), key=lambda d: d["date"], reverse=True)
        return {
            "strategy": STRATEGY,
            "paper_only": True,
            "enabled": self.enabled,
            "size_btc": self.size_btc,
            "contracts": round(self.size_btc / CONTRACT_BTC),
            "rule": {
                "lookbacks_min": list(LOOKBACKS_MIN),
                "thresholds_pct": {str(lb): round(threshold_pct(lb), 3) for lb in LOOKBACKS_MIN},
                "min_votes": MIN_VOTES,
                "stop_mult": STOP_MULT,
                "entry_ist": "16:00",
                "exit_ist": "17:25",
            },
            "next_decision_ist": entry.astimezone(IST).strftime("%Y-%m-%d %H:%M IST"),
            "today": self.days.get(today.isoformat()),
            "open": next((d for d in days if d.get("status") == "open"), None),
            "totals": {
                "trades": len(closed),
                "wins": sum(1 for d in closed if (d.get("pnl_usd_mark") or 0) > 0),
                "stops": sum(1 for d in closed if d.get("exit_why") == "stop"),
                "pnl_usd_mark": total("pnl_usd_mark"),
                "pnl_usd_quote": total("pnl_usd_quote"),
                "worst_run_usd_mark": round(dd, 2),
                "days_decided": sum(1 for d in self.days.values() if d.get("status") in ("closed", "open", "skipped")),
                "missed": sum(1 for d in self.days.values() if d.get("status") == "missed"),
                "poll_missed_stops": sum(
                    1 for d in closed if d.get("stop_touched_by_candle") and d.get("exit_why") != "stop"
                ),
            },
            "days": days[:60],
            "events": list(reversed(self.events[-60:])),
        }

    def _event(self, level: str, message: str) -> None:
        self.events.append({"ts": self._clock(), "time": _ist(self._clock()), "level": level, "message": message})
        del self.events[:-EVENT_LIMIT]
        _log.info("[OPTION-SELLER] %s", message)

    def _trim(self) -> None:
        if len(self.days) > DAY_LIMIT:
            for k in sorted(self.days)[: len(self.days) - DAY_LIMIT]:
                del self.days[k]

    @staticmethod
    def _side_name(side: str) -> str:
        return "call" if side == "C" else "put"

    @staticmethod
    def _px(v: float) -> str:
        return f"{v:,.2f}" if v else "—"

    @staticmethod
    def _money(v: Optional[float]) -> str:
        if v is None:
            return "—"
        return f"{'+' if v >= 0 else '−'}${abs(v):,.2f}"
