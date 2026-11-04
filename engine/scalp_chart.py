"""engine/scalp_chart.py — the Scalp desk's chart and its price alarms.

Phil, 2026-11-04: "Please create the same kinda scalp page for cryptoforge as
well same as that of philforge". PhilForge's Scalp desk charts the contract
beside the order form: candles, a 20-EMA and each day's CPR with R1-R4/S1-S5
drawn by the server; Supertrend, RSI and the drawings by the page; and price
alarms the server keeps and rings once, on Telegram and on the page.

This module is the server's half for crypto, kept free of the app so it can be
tested on its own:

* `frame_to_candles` turns any broker's OHLC frame into the renderer's
  `[{t,o,h,l,c}]`, epoch seconds.
* `ema_overlay` and `cpr_overlays` build the studies in PhilForge's overlay
  shape, with its TradingView colours. Crypto never closes, so a "day" is the
  exchange's own daily candle -- the UTC day, 05:30 to 05:30 IST -- and each
  day's levels come from the DAILY bar before it, not from intraday bars that
  might not reach back that far (1000 one-minute bars is under 17 hours).
* `AlarmBook` holds the alarms: one list, each rung exactly once.
"""

from __future__ import annotations

import math
import time
from typing import Callable, Dict, List, Optional

CHART_TIMEFRAMES = ("1m", "3m", "5m", "15m", "1h")
TF_SECONDS = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "1h": 3600}
DAY_SEC = 86_400
MAX_BARS = 1500

# TradingView's colours, as PhilForge draws them: the CPR band blue with its
# pivot magenta, resistance red, support green, the 20-EMA blue.
CPR_BLUE, CPR_PIVOT, CPR_RED, CPR_GREEN = "#2962ff", "#d946ef", "#ef3340", "#34a853"
EMA_COLOR = "#2962ff"
EMA_PERIOD = 20


def price_digits(value: float) -> int:
    """The decimals a coin's price needs: BTC at 2, XRP at 4, a sub-dollar coin
    at 6. The page's renderer uses the same rule (`_pfChartPriceDigits`)."""
    a = abs(float(value or 0))
    if not math.isfinite(a) or a >= 100:
        return 2
    if a >= 1:
        return 4
    return 6


def fmt_price(value: float) -> str:
    digits = price_digits(value)
    text = f"{float(value):,.{digits}f}"
    if digits > 2:
        text = text.rstrip("0").rstrip(".") if "." in text else text
    return text


def symbol_key(symbol: str) -> str:
    """One name per coin across brokers: Delta's BTCUSD and Binance's BTCUSDT
    are the same market to an alarm, and GOLD is PAXG."""
    raw = str(symbol or "").strip().upper()
    if raw in {"GOLD", "GOLDUSDT"}:
        raw = "PAXGUSD"
    return raw[:-1] if raw.endswith("USDT") else raw


def frame_to_candles(frame, limit: int = MAX_BARS) -> List[dict]:
    """A broker's OHLC frame (UTC DatetimeIndex; open/high/low/close) as
    `[{t,o,h,l,c}]`, oldest first, NaN rows dropped, the last `limit` kept."""
    if frame is None or getattr(frame, "empty", True):
        return []
    out: List[dict] = []
    for stamp, row in frame.iterrows():
        try:
            o, h, lo, c = (float(row[k]) for k in ("open", "high", "low", "close"))
        except (KeyError, TypeError, ValueError):
            continue
        if not all(math.isfinite(v) and v > 0 for v in (o, h, lo, c)):
            continue
        out.append({"t": int(stamp.timestamp()), "o": o, "h": h, "l": lo, "c": c})
    out.sort(key=lambda b: b["t"])
    # One bar per timestamp: a page fetched across two calls can overlap.
    deduped: List[dict] = []
    for bar in out:
        if deduped and deduped[-1]["t"] == bar["t"]:
            deduped[-1] = bar
        else:
            deduped.append(bar)
    return deduped[-limit:] if limit else deduped


def ema_overlay(candles: List[dict], period: int = EMA_PERIOD) -> Optional[dict]:
    """The 20-EMA over closes, drawn only once it has `period` real bars."""
    if len(candles) < period + 1:
        return None
    k = 2.0 / (period + 1.0)
    ema = None
    points = []
    for i, bar in enumerate(candles):
        close = float(bar["c"])
        ema = close if ema is None else close * k + ema * (1.0 - k)
        if i >= period - 1:
            points.append({"t": bar["t"], "price": round(ema, 8)})
    return {"label": f"EMA {period}", "color": EMA_COLOR, "width": 1.3, "points": points, "group": "ema"}


def cpr_levels(high: float, low: float, close: float) -> List[tuple]:
    """CPR (TC/P/BC), R1-R4 and S1-S5 from one day's H/L/C: traditional floor
    pivots, as TradingView and PhilForge draw them."""
    p = (high + low + close) / 3.0
    bc = (high + low) / 2.0
    tc = 2.0 * p - bc
    bc, tc = min(bc, tc), max(bc, tc)
    rng = high - low
    r1, s1 = 2 * p - low, 2 * p - high
    r2, s2 = p + rng, p - rng
    r3, s3 = high + 2 * (p - low), low - 2 * (high - p)
    r4, s4 = 3 * p + (high - 3 * low), 3 * p - (3 * high - low)
    s5 = 4 * p - (4 * high - low)
    return [
        ("CPR TC", tc, CPR_BLUE),
        ("CPR P", p, CPR_PIVOT),
        ("CPR BC", bc, CPR_BLUE),
        ("R1", r1, CPR_RED),
        ("R2", r2, CPR_RED),
        ("R3", r3, CPR_RED),
        ("R4", r4, CPR_RED),
        ("S1", s1, CPR_GREEN),
        ("S2", s2, CPR_GREEN),
        ("S3", s3, CPR_GREEN),
        ("S4", s4, CPR_GREEN),
        ("S5", s5, CPR_GREEN),
    ]


def cpr_overlays(candles: List[dict], daily: List[dict], tf_sec: int) -> List[dict]:
    """Each UTC day on the chart carries the levels of the DAILY bar before it,
    drawn flat across that day's own bars -- the steps land on 05:30 IST, where
    the exchange's day turns. Only the newest day labels its lines (twelve
    labels a day would bury the chart). The newest day's lines run a little
    past the last bar, so the forming bars have them too."""
    if not candles or not daily:
        return []
    by_day = {int(b["t"]) // DAY_SEC: b for b in daily}
    days: Dict[int, List[dict]] = {}
    for bar in candles:
        days.setdefault(int(bar["t"]) // DAY_SEC, []).append(bar)
    ordered = sorted(days)
    out: List[dict] = []
    for index, day in enumerate(ordered):
        prev = by_day.get(day - 1)
        if not prev:
            continue
        bars = days[day]
        last = index == len(ordered) - 1
        start_t = int(bars[0]["t"])
        end_t = int(bars[-1]["t"])
        if last:
            end_t = min((day + 1) * DAY_SEC - tf_sec, end_t + 60 * max(int(tf_sec), 60))
        for label, price, color in cpr_levels(float(prev["h"]), float(prev["l"]), float(prev["c"])):
            out.append(
                {
                    "label": f"{label} ({fmt_price(price)})" if last else "",
                    "color": color,
                    "dash": [],
                    "width": 1.1 if label == "CPR P" else 0.9,
                    "opacity": 0.85 if label.startswith("CPR") else 0.55,
                    "points": [{"t": t, "price": round(price, 8)} for t in (start_t, max(end_t, start_t))],
                    "group": "cpr",
                }
            )
    return out


def build_chart_payload(symbol: str, timeframe: str, candles: List[dict], daily: List[dict]) -> dict:
    tf_sec = TF_SECONDS.get(timeframe, 300)
    overlays: List[dict] = []
    ema = ema_overlay(candles)
    if ema:
        overlays.append(ema)
    overlays.extend(cpr_overlays(candles, daily, tf_sec))
    return {
        "status": "ok",
        "symbol": symbol,
        "timeframe": timeframe,
        "instrument": {"symbol": symbol},
        "candles": candles,
        "overlays": overlays,
        "lines": [],
        "entries": [],
        "exits": [],
        "cpr_day": "UTC",
    }


# ── PRICE ALARMS ────────────────────────────────────────────────────────────
# Right-click the chart: an alarm at that price, drawn as an amber bell line.
# Which way it must go is fixed when it is set -- down to a level under the
# price, else up to it -- so "crossed or touched" is one comparison. Rung once:
# off the list, onto `fired` (kept briefly so every open page can show it), to
# Telegram.

FIRED_KEEP = 20
FIRED_SHOW_SEC = 600
MAX_ALARMS = 60


class AlarmBook:
    def __init__(self, load: Callable[[], dict], save: Callable[[dict], None], now_fn: Callable[[], float] = time.time):
        self._load = load
        self._save = save
        self._now = now_fn
        self._book: Optional[dict] = None

    def _get(self) -> dict:
        if self._book is None:
            raw = self._load() or {}
            self._book = {
                "alarms": [a for a in (raw.get("alarms") or []) if isinstance(a, dict)],
                "fired": [f for f in (raw.get("fired") or []) if isinstance(f, dict)],
            }
        return self._book

    def listing(self) -> dict:
        book = self._get()
        recent = int(self._now()) - FIRED_SHOW_SEC
        return {
            "status": "ok",
            "alarms": list(book["alarms"]),
            "fired": [f for f in book["fired"] if int(f.get("fired_at") or 0) >= recent],
        }

    def add(self, symbol: str, price: float, now_price: float = 0.0) -> dict:
        book = self._get()
        if len(book["alarms"]) >= MAX_ALARMS:
            raise ValueError(f"At most {MAX_ALARMS} alarms; remove some first")
        price = float(price)
        if not math.isfinite(price) or price <= 0:
            raise ValueError("An alarm needs a price above zero")
        price = round(price, price_digits(price))
        stamp = self._now()
        alarm = {
            "id": f"a{int(stamp * 1000):x}{len(book['alarms']):02d}",
            "symbol": symbol,
            "price": price,
            "dir": "down" if now_price and now_price > 0 and price < float(now_price) else "up",
            "at": int(stamp),
        }
        book["alarms"].append(alarm)
        self._save(book)
        return alarm

    def remove(self, alarm_id: str) -> int:
        """Remove an alarm, or silence one that has rung (Dismiss on any page)."""
        book = self._get()
        before = len(book["alarms"]) + len(book["fired"])
        book["alarms"] = [a for a in book["alarms"] if a.get("id") != alarm_id]
        book["fired"] = [f for f in book["fired"] if f.get("id") != alarm_id]
        removed = before - len(book["alarms"]) - len(book["fired"])
        if removed:
            self._save(book)
        return removed

    def count(self) -> int:
        return len(self._get()["alarms"])

    @staticmethod
    def hit(alarm: dict, price: float) -> bool:
        if not price or price <= 0:
            return False
        level = float(alarm.get("price") or 0)
        return price <= level if alarm.get("dir") == "down" else price >= level

    def fire(self, alarm_id: str, price: float) -> Optional[dict]:
        """Ring one alarm once. None if it is gone (already rung or removed)."""
        book = self._get()
        alarm = next((a for a in book["alarms"] if a.get("id") == alarm_id), None)
        if alarm is None:
            return None
        book["alarms"] = [a for a in book["alarms"] if a.get("id") != alarm_id]
        rung = {**alarm, "hit": round(float(price), price_digits(price)), "fired_at": int(self._now())}
        book["fired"] = (book["fired"] + [rung])[-FIRED_KEEP:]
        self._save(book)
        return rung

    def check(self, prices: Dict[str, float]) -> List[dict]:
        """Ring every alarm its symbol's price has reached. `prices` maps a
        `symbol_key` to its price now; a coin not in it is not judged."""
        rung = []
        for alarm in list(self._get()["alarms"]):
            price = float(prices.get(symbol_key(alarm.get("symbol"))) or 0)
            if self.hit(alarm, price):
                done = self.fire(alarm["id"], price)
                if done:
                    rung.append(done)
        return rung


def alarm_text(alarm: dict) -> tuple:
    """(title, body) for the Telegram message."""
    moved = "fell to" if alarm.get("dir") == "down" else "rose to"
    symbol = str(alarm.get("symbol") or "")
    return (
        f"\U0001f514 Price alarm · {symbol}",
        f"{moved} ${fmt_price(float(alarm.get('price') or 0))}\nNow ${fmt_price(float(alarm.get('hit') or 0))}",
    )


def prices_from_status(status: dict) -> Dict[str, float]:
    """Every price a Scalp status carries: the watched symbol's, and each open
    trade's mark. The server checks alarms on each status the engine pushes, so
    no loop of its own is needed (a loop started once dies at every deploy)."""
    out: Dict[str, float] = {}
    controls = (status or {}).get("entry_controls") or {}
    sym = symbol_key(controls.get("symbol"))
    try:
        price = float(controls.get("price") or 0)
    except (TypeError, ValueError):
        price = 0.0
    if sym and price > 0:
        out[sym] = price
    for trade in (status or {}).get("open_trades") or []:
        sym = symbol_key((trade or {}).get("symbol"))
        try:
            mark = float(trade.get("current_price") or trade.get("mark_price") or 0)
        except (TypeError, ValueError):
            mark = 0.0
        if sym and mark > 0 and sym not in out:
            out[sym] = mark
    return out
