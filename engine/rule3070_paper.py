"""engine/rule3070_paper.py — the 30-70 Rule paper trader as a site service.

One writer, one truth: every 5 minutes the service pulls the trailing 90 days
of closed 5m candles and REPLAYS the locked 30-70 engine (tools/rule3070_sim)
over them — minors, budget, the 0.35% fee gate, the 2-band brake, 25% profit
reinvestment. Fills and targets that land after the paper clock started are
paper trades; the replay-per-tick design means the console can never drift
from the engine the nine-year backtest proved.

The CLI (tools/rule3070_paper.py) and the site console share THIS module, and
a pid lockfile keeps them from ever writing the same journal at once.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import List, Optional

import pandas as pd
import requests

import tools.rule3070_sim as sim

_logger = logging.getLogger("cryptoforge.rule3070")

# History the engine is given. NOT a rolling window: the replay starts a fixed
# number of days BEFORE the paper clock and grows forward from there, so no
# mother, fib or open ladder can ever silently drop off the left edge — and it
# is exactly how a live start behaves. Whatever history you hand the engine at
# switch-on is the history it has; structure evolves organically after that.
# 30 days of warm-up: long enough for a standing mother and some Vs, short
# enough that the mother is a top you would actually trade against today.
WARMUP_DAYS = 30
WINDOW_DAYS = 90  # the CLI/one-off path with no paper clock to anchor to
# Phil, 2026-08-11 night: paper proves at his REAL size — $200, like the live
# account. Everything scales by percent, but the $5.50 Binance minimum order
# does not: at $200 the small buys clamp up to it, so paper at $200 is the
# honest test the $2,000 backtests could not give.
CAPITAL = 200.0
KLINES = "https://api.binance.com/api/v3/klines"
IST = "Asia/Kolkata"
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "out", "rule3070")
STATE_PATH = os.path.join(OUT, "paper_state.json")
JOURNAL_PATH = os.path.join(OUT, "paper_journal.jsonl")
LOCK_PATH = os.path.join(OUT, "paper.lock")
REPLAY_LOCK = threading.Lock()

# The V-Rule console deliberately follows the same six-instrument universe as
# the rest of CryptoForge. Every instrument owns a separate writer, clock and
# journal, so BTC can keep running while the console views or runs another coin.
SUPPORTED_SYMBOLS = (
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "PAXGUSDT",
)


def normalize_symbol(symbol: str) -> str:
    value = str(symbol or "").strip().upper()
    if value not in SUPPORTED_SYMBOLS:
        raise ValueError(f"Unsupported V-Rule instrument: {value or 'empty'}")
    return value


def _read_json(path: str) -> dict:
    with open(path) as fh:
        return json.load(fh)


def configure() -> None:
    """The locked config — the exact rules the reproducible verdict ran.

    Phil, 2026-08-11 night, after the funding audit: the tradeable version is
    the SAFE one — the book may never hold more than HALF the purse at once.
    That rule cut the 9-year backtest from a fantasy $1M (earned 100% all-in
    at every crash bottom) to an honest +$17,761 on BTC's $2,000 with money
    always free in hand. This is the version paper has to prove.
    """
    sim.CAPITAL_USD = CAPITAL
    sim.ENFORCE_BUDGET = True
    sim.BUDGET_CAP_FRAC = 0.5  # never more than half the purse in the market
    sim.MIN_NET_MARGIN = 0.0035
    sim.MAX_BANDS = 2
    sim.COMPOUND_AT_HALF = True
    sim.COMPOUND_SCHEDULE = (0.25,)


def fetch_window(symbol: str, days: int = WINDOW_DAYS, since_ts: int = 0) -> pd.DataFrame:
    """CLOSED 5m candles, oldest first — from `since_ts`, else trailing `days`."""
    end = int(time.time() * 1000)
    start = int(since_ts) * 1000 if since_ts else end - days * 86400 * 1000
    rows: list = []
    cursor = start
    while cursor < end:
        resp = requests.get(
            KLINES,
            params={"symbol": symbol, "interval": "5m", "startTime": cursor, "limit": 1000},
            timeout=30,
        )
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        rows.extend(batch)
        cursor = batch[-1][0] + 300_000
        if len(batch) < 1000:
            break
    now_bucket = (int(time.time()) // 300) * 300 * 1000
    df = pd.DataFrame(
        [(int(k[0]) // 1000, float(k[1]), float(k[2]), float(k[3]), float(k[4])) for k in rows if k[0] < now_bucket],
        columns=["ts", "open", "high", "low", "close"],
    )
    df.index = pd.to_datetime(df.pop("ts"), unit="s", utc=True)
    df.index.name = "datetime"
    return df


def campaign_id(c) -> str:
    """Mother + V start. The mother alone is NOT unique: when a trade ends and
    the mother still stands, the next V under the SAME mother starts the next
    campaign — so a mother-keyed journal silently swallowed the second trade's
    buys as duplicates, and a mother-keyed chart drew whichever came first."""
    return f"{int(c.mother_ts.timestamp())}-{int(c.swing_low_ts.timestamp())}"


def harvest(df: pd.DataFrame, start_ts: int, seen: set) -> tuple:
    """Replay the window; return (new events, open-position summaries, campaigns)."""
    configure()
    campaigns = sim.run_ladder(df, minors=True)
    last_close = float(df["close"].iloc[-1]) if len(df) else 0.0
    events: List[dict] = []
    opens: List[dict] = []
    for c in campaigns:
        mother = c.mother_ts.tz_convert(IST).strftime("%Y-%m-%d %H:%M")
        cid = campaign_id(c)
        for f in c.fills:
            if f.ts.timestamp() < start_ts:
                continue
            key = f"fill:{cid}:{f.label}"
            if key in seen:
                continue
            events.append(
                {
                    "kind": "BUY",
                    "key": key,
                    "ts": int(f.ts.timestamp()),
                    "mts": int(c.mother_ts.timestamp()),
                    "cid": cid,
                    "when": f.ts.tz_convert(IST).strftime("%Y-%m-%d %H:%M"),
                    "mother": mother,
                    "label": f.label,
                    "price": round(f.price, 2),
                    "usd": round(f.usd, 2),
                    "minor": bool(c.is_minor),
                    "fall_pct": round(c.fall_pct, 2),
                    "target": round(c.target or 0.0, 2),
                }
            )
        if c.status == "TARGET HIT" and c.target_ts is not None and c.fills:
            if c.target_ts.timestamp() >= start_ts:
                key = f"target:{mother}"
                if key not in seen:
                    cost = sum(f.usd for f in c.fills)
                    qty = sum(f.usd / f.price for f in c.fills)
                    net = qty * c.target - cost - 0.001 * (cost + qty * c.target)
                    events.append(
                        {
                            "kind": "TARGET",
                            "key": key,
                            "ts": int(c.target_ts.timestamp()),
                            "mts": int(c.mother_ts.timestamp()),
                            "cid": cid,
                            "when": c.target_ts.tz_convert(IST).strftime("%Y-%m-%d %H:%M"),
                            "mother": mother,
                            "price": round(c.target, 2),
                            "cost": round(cost, 2),
                            "net": round(net, 4),
                            "minor": bool(c.is_minor),
                            "buys": len(c.fills),
                        }
                    )
        elif c.fills and not c.status.startswith("CANCELLED"):
            cost = sum(f.usd for f in c.fills)
            qty = sum(f.usd / f.price for f in c.fills)
            opens.append(
                {
                    "mother": mother,
                    "mts": int(c.mother_ts.timestamp()),
                    "cid": cid,
                    "cost": round(cost, 2),
                    "unrealised": round(qty * last_close - cost, 2),
                    "target": round(c.target or 0.0, 2),
                    "buys": len(c.fills),
                    "minor": bool(c.is_minor),
                    "status": c.status,
                    "paper": bool(c.fills[0].ts.timestamp() >= start_ts),
                }
            )
    return events, opens, campaigns


def build_watch(campaigns: list, df: pd.DataFrame) -> dict:
    """What the engine is looking at between trades.

    Most of the time nothing fills, and a console that only lists fills looks
    dead. The scanner is never idle: a mother stands, a V forms, and buy
    orders sit armed a fraction of a percent under the tape. That is the work
    — so it gets shown.
    """
    if not len(df):
        return {}
    price = float(df["close"].iloc[-1])
    scan = dict(sim.LAST_SCAN)
    armed = []
    for c in campaigns:
        if c.fills or c.status.startswith("CANCELLED"):
            continue
        entry = c.entry_price()
        # an untouched campaign has no low yet, so its "entry" is nonsense
        if not c._touched or not entry or entry <= 0:
            continue
        armed.append(
            {
                "mother": c.mother_ts.tz_convert(IST).strftime("%Y-%m-%d %H:%M"),
                "mts": int(c.mother_ts.timestamp()),
                "cid": campaign_id(c),
                "mother_high": round(c.mother_high, 2),
                "entry": round(entry, 2),
                "away_pct": round((price - entry) / price * 100, 3),
                "pending": f"{c._pending} of band {c._band}",
                "minor": bool(c.is_minor),
                "fall_pct": round(c.fall_pct, 2),
                "pot": round(c.pot_usd, 2),
            }
        )
    armed.sort(key=lambda r: abs(r["away_pct"]))
    watch = {
        "price": round(price, 2),
        "bar_when": df.index[-1].tz_convert(IST).strftime("%Y-%m-%d %H:%M"),
        "stage": scan.get("stage", ""),
        "greens": scan.get("greens", 0),
        "armed_count": len(armed),
        "armed_near": sum(1 for r in armed if abs(r["away_pct"]) <= 1.0),
        "armed": armed[:60],
        "nearest_pct": armed[0]["away_pct"] if armed else None,
    }
    if scan.get("mother_ts") is not None:
        mh = float(scan["mother_high"])
        watch["mother"] = {
            "price": round(mh, 2),
            "when": scan["mother_ts"].tz_convert(IST).strftime("%Y-%m-%d %H:%M"),
            "below_pct": round((mh - price) / mh * 100, 2),
        }
    if scan.get("dip_ts") is not None:
        watch["dip"] = {
            "price": round(float(scan["dip_low"]), 2),
            "when": scan["dip_ts"].tz_convert(IST).strftime("%Y-%m-%d %H:%M"),
        }
    return watch


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


class Rule3070PaperService:
    """Background paper trader with start/stop/reset for the console."""

    def __init__(self, symbol: str = "BTCUSDT"):
        self.symbol = normalize_symbol(symbol)
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._status: dict = {"running": False}
        self._seen: set = set()
        self._state: dict = {}
        self._closed_count = 0
        self._closed_net = 0.0
        self._opens: List[dict] = []
        self._last_error = ""
        self._campaigns: list = []  # last replay, for charts and details
        self._df: Optional[pd.DataFrame] = None
        self._watch: dict = {}
        self._priming = False
        self._primed_at = 0.0
        self._selection_generation = 0
        self._activity: List[dict] = []  # newest last; the console reads it reversed

    @property
    def state_path(self) -> str:
        # Preserve the original BTC filenames so the active paper book shown in
        # the existing console continues without a migration or reset.
        name = "paper_state.json" if self.symbol == "BTCUSDT" else f"paper_state_{self.symbol}.json"
        return os.path.join(OUT, name)

    @property
    def journal_path(self) -> str:
        name = "paper_journal.jsonl" if self.symbol == "BTCUSDT" else f"paper_journal_{self.symbol}.jsonl"
        return os.path.join(OUT, name)

    @property
    def lock_path(self) -> str:
        # BTC keeps the original lock name so an existing CLI/site writer is
        # still detected across this upgrade. Other instruments can run beside
        # it, but never have two writers for their own paper book.
        name = "paper.lock" if self.symbol == "BTCUSDT" else f"paper_{self.symbol}.lock"
        return os.path.join(OUT, name)

    def _select_symbol_unlocked(self, symbol: str) -> None:
        self._selection_generation += 1
        self.symbol = normalize_symbol(symbol)
        self._status = {"running": False, "symbol": self.symbol}
        self._seen = set()
        self._state = {}
        self._closed_count = 0
        self._closed_net = 0.0
        self._opens = []
        self._last_error = ""
        self._campaigns = []
        self._df = None
        self._watch = {}
        self._priming = False
        self._primed_at = 0.0
        self._activity = []
        self._load_closed_totals()

    def select_symbol(self, symbol: str) -> dict:
        """Choose the paper book shown by the console while the writer is idle."""
        selected = normalize_symbol(symbol)
        with self._lock:
            running = bool(self._thread and self._thread.is_alive())
            if running and selected != self.symbol:
                raise RuntimeError(f"Stop the {self.symbol} paper trader before changing instruments")
            if selected != self.symbol:
                self._select_symbol_unlocked(selected)
            return self.status()

    # -- lifecycle ---------------------------------------------------

    def _prepare(self) -> None:
        self._acquire_writer_lock()
        os.makedirs(OUT, exist_ok=True)
        if os.path.exists(self.state_path):
            self._state = _read_json(self.state_path)
        else:
            self._state = {"start_ts": int(time.time()), "seen": []}
        # Fixed once, then never moved: the replay must see the same history on
        # every tick, or a mother could age out from under an open ladder.
        if not self._state.get("history_start_ts"):
            self._state["history_start_ts"] = int(self._state["start_ts"]) - WARMUP_DAYS * 86400
            self._write_state()
        self._seen = set(self._state.get("seen", []))
        self._load_closed_totals()

    def start(self, symbol: Optional[str] = None) -> dict:
        with self._lock:
            if self._thread and self._thread.is_alive():
                if symbol and normalize_symbol(symbol) != self.symbol:
                    raise RuntimeError(f"Stop the {self.symbol} paper trader before changing instruments")
                return self.status()
            if symbol and normalize_symbol(symbol) != self.symbol:
                self._select_symbol_unlocked(symbol)
            self._prepare()
            self._stop.clear()
            self._thread = threading.Thread(target=self._loop, name="rule3070-paper", daemon=True)
            self._thread.start()
            _logger.info("30-70 paper started on %s (start_ts %s)", self.symbol, self._state["start_ts"])
            return self.status()

    def run_foreground(self, once: bool = False) -> None:
        """The CLI path: same ticks, main thread, prints instead of a console."""
        self._prepare()
        print(
            f"30-70 paper on {self.symbol} — trades count from "
            f"{time.strftime('%Y-%m-%d %H:%M', time.localtime(self._state['start_ts']))}",
            flush=True,
        )
        while True:
            try:
                self._tick()
                s = self._status
                print(
                    f"[{time.strftime('%H:%M:%S')}] tick — close {s.get('last_close'):,} | "
                    f"paper opens {len([o for o in self._opens if o.get('paper')])} | "
                    f"closed {self._closed_count} net ${self._closed_net:+.2f}",
                    flush=True,
                )
            except Exception as exc:  # noqa: BLE001
                print(f"[{time.strftime('%H:%M:%S')}] tick failed: {exc}", flush=True)
            if once:
                self._release_writer_lock()
                return
            now = time.time()
            time.sleep(300 - (now % 300) + 10)

    def stop(self) -> dict:
        with self._lock:
            self._stop.set()
            if self._thread:
                self._thread.join(timeout=10)
            self._thread = None
            self._release_writer_lock()
            self._status["running"] = False
            return self.status()

    def reset(self) -> dict:
        """New paper clock. Only while stopped; the old journal is archived."""
        with self._lock:
            if self._thread and self._thread.is_alive():
                raise RuntimeError("Stop the paper trader before resetting it")
            stamp = time.strftime("%Y%m%d-%H%M%S")
            for path in (self.state_path, self.journal_path):
                if os.path.exists(path):
                    os.rename(path, f"{path}.{stamp}")
            self._seen = set()
            self._state = {}
            self._closed_count = 0
            self._closed_net = 0.0
            self._opens = []
            self._status = {"running": False}
            return {"reset": True, "archived_as": stamp}

    # -- the writer lock ---------------------------------------------

    def _acquire_writer_lock(self) -> None:
        os.makedirs(OUT, exist_ok=True)
        if os.path.exists(self.lock_path):
            try:
                with open(self.lock_path) as fh:
                    other = int(fh.read().strip() or 0)
            except ValueError:
                other = 0
            if other and other != os.getpid() and _pid_alive(other):
                raise RuntimeError(
                    f"Another paper writer is running (pid {other}) — stop it first "
                    f"(the nohup CLI runner and the site console must not write together)"
                )
        with open(self.lock_path, "w") as fh:
            fh.write(str(os.getpid()))

    def _release_writer_lock(self) -> None:
        try:
            owner = 0
            if os.path.exists(self.lock_path):
                with open(self.lock_path) as fh:
                    owner = int(fh.read().strip() or 0)
            if owner == os.getpid():
                os.remove(self.lock_path)
        except (ValueError, OSError):
            pass

    # -- state -------------------------------------------------------

    def _write_state(self) -> None:
        self._state["seen"] = sorted(self._seen)
        with open(self.state_path, "w") as fh:
            json.dump(self._state, fh)

    def _load_closed_totals(self) -> None:
        self._closed_count = 0
        self._closed_net = 0.0
        if not os.path.exists(self.journal_path):
            return
        with open(self.journal_path) as journal:
            for line in journal:
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if row.get("kind") == "TARGET":
                    self._closed_count += 1
                    self._closed_net += float(row.get("net") or 0.0)

    # -- the loop ----------------------------------------------------

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._tick()
                self._last_error = ""
            except Exception as exc:  # noqa: BLE001 — a paper trader survives bad ticks
                self._last_error = str(exc)
                _logger.warning("30-70 paper tick failed: %s", exc)
            now = time.time()
            self._stop.wait(300 - (now % 300) + 10)

    def _note(self, kind: str, text: str) -> None:
        self._activity.append({"ts": int(time.time()), "kind": kind, "text": text})
        del self._activity[:-80]

    def _history_start(self) -> int:
        if self._state.get("history_start_ts"):
            return int(self._state["history_start_ts"])
        if os.path.exists(self.state_path):
            try:
                st = _read_json(self.state_path)
                if st.get("history_start_ts"):
                    return int(st["history_start_ts"])
                if st.get("start_ts"):
                    return int(st["start_ts"]) - WARMUP_DAYS * 86400
            except (json.JSONDecodeError, OSError):
                pass
        # An instrument that has never been started is still previewed by the
        # console. Give that preview the exact history it would receive if the
        # user pressed Start now. Falling back to fetch_window's 90-day CLI
        # window made a stopped ETH book inherit May mothers in August.
        return int(time.time()) - WARMUP_DAYS * 86400

    def _replay_start_ts(self) -> int:
        """Paper cutoff for a replay, including the not-yet-started preview.

        Before a paper clock exists there cannot be paper trades. Treat now as
        the provisional cutoff so historical fills are warm-up ladders only.
        The old zero cutoff classified every preview fill as a paper buy.
        """
        if self._state.get("start_ts"):
            return int(self._state["start_ts"])
        if os.path.exists(self.state_path):
            try:
                return int(_read_json(self.state_path).get("start_ts") or time.time())
            except (json.JSONDecodeError, OSError):
                pass
        return int(time.time())

    def _tick(self) -> None:
        df = fetch_window(self.symbol, since_ts=self._history_start())
        with REPLAY_LOCK:
            events, opens, campaigns = harvest(df, int(self._state["start_ts"]), self._seen)
            watch = build_watch(campaigns, df)
            purse = round(float(sim.CAPITAL_USD), 2)
        self._campaigns, self._df = campaigns, df
        self._watch = watch
        for e in events:
            self._seen.add(e["key"])
            with open(self.journal_path, "a") as jf:
                jf.write(json.dumps(e) + "\n")
            if e["kind"] == "TARGET":
                self._closed_count += 1
                self._closed_net += float(e["net"])
                self._note("target", f"TARGET HIT {e['mother']} — {e['buys']} buys, net ${e['net']:+.2f}")
            else:
                self._note("buy", f"BUY {e['label']} at ${e['price']:,.2f} (${e['usd']:.2f}) — mother {e['mother']}")
            _logger.info("30-70 paper %s %s", e["kind"], e.get("label") or e.get("mother"))
        if events:
            self._write_state()
        elif self._watch:
            near = self._watch.get("nearest_pct")
            self._note(
                "tick",
                f"scanned to ${self._watch['price']:,.2f} — {self._watch['armed_count']} orders armed"
                + (f", nearest {near:+.2f}% away" if near is not None else "")
                + f" · {self._watch.get('stage', '')}",
            )
        self._opens = opens
        self._status = {
            "running": True,
            "symbol": self.symbol,
            "capital": CAPITAL,
            "purse": purse,
            "start_ts": int(self._state["start_ts"]),
            "last_tick_ts": int(time.time()),
            "last_close": round(float(df["close"].iloc[-1]), 2) if len(df) else None,
            "bars": int(len(df)),
        }

    # -- reads -------------------------------------------------------

    def prime(self) -> None:
        """Fill the watch state once, read-only, without starting the trader.

        A stopped console had nothing on it at all — no mother, no armed
        orders, no price — which reads as a broken page rather than an engine
        that has not been switched on. The replay is read-only (harvest never
        writes the journal), so this is safe with another writer running.

        ONLY the console asks for this (status?scan=1). It is a ~20s CPU-bound
        replay of a month of candles: hanging it off every /status call meant
        any route sweep — or any other page — could start one, and this process
        also runs the live Cascade engine. One at a time, once every 10 min.
        """
        now = time.time()
        if self._watch or self._priming or (self._thread and self._thread.is_alive()) or now - self._primed_at < 600:
            return
        self._primed_at = now
        self._priming = True
        generation = self._selection_generation
        symbol = self.symbol
        history_start = self._history_start()

        def run():
            try:
                df = fetch_window(symbol, since_ts=history_start)
                start_ts = self._replay_start_ts()
                with REPLAY_LOCK:
                    _, opens, campaigns = harvest(df, start_ts, set())
                    watch = build_watch(campaigns, df)
                    purse = round(float(sim.CAPITAL_USD), 2)
                if generation != self._selection_generation or symbol != self.symbol:
                    return
                self._campaigns, self._df, self._opens = campaigns, df, opens
                self._watch = watch
                self._status.setdefault("symbol", self.symbol)
                self._status["last_close"] = round(float(df["close"].iloc[-1]), 2)
                self._status["bars"] = int(len(df))
                self._status["purse"] = purse
                near = self._watch.get("nearest_pct")
                self._note(
                    "scan",
                    f"read-only scan at ${self._watch.get('price', 0):,.2f} — "
                    f"{self._watch.get('armed_count', 0)} orders armed"
                    + (f", nearest {near:+.2f}% away" if near is not None else "")
                    + " · engine not started",
                )
            except Exception as exc:  # noqa: BLE001
                if generation == self._selection_generation:
                    self._last_error = f"scan failed: {exc}"
            finally:
                if generation == self._selection_generation:
                    self._priming = False

        threading.Thread(target=run, name="rule3070-prime", daemon=True).start()

    def status(self, scan: bool = False) -> dict:
        running = bool(self._thread and self._thread.is_alive())
        if scan and not running:
            self.prime()
        snap = dict(self._status)
        snap["running"] = running
        snap.setdefault("symbol", self.symbol)
        snap["available_symbols"] = list(SUPPORTED_SYMBOLS)
        snap.setdefault("capital", CAPITAL)
        if not snap.get("start_ts") and os.path.exists(self.state_path):
            try:
                snap["start_ts"] = int(_read_json(self.state_path).get("start_ts") or 0)
            except (json.JSONDecodeError, OSError):
                pass
        paper_opens = [o for o in self._opens if o.get("paper")]
        warmup = [o for o in self._opens if not o.get("paper")]
        snap["opens"] = {
            "count": len(paper_opens),
            "cost": round(sum(o["cost"] for o in paper_opens), 2),
            "unrealised": round(sum(o["unrealised"] for o in paper_opens), 2),
            "rows": paper_opens[:200],
            "warmup_holding": len(warmup),
            "warmup_cost": round(sum(o["cost"] for o in warmup), 2),
            "warmup_unrealised": round(sum(o["unrealised"] for o in warmup), 2),
            "warmup_rows": warmup[-200:][::-1],
        }
        snap["watch"] = self._watch
        snap["activity"] = self._activity[-40:][::-1]
        if running:
            nxt = (int(time.time()) // 300) * 300 + 310
            snap["next_tick_ts"] = nxt if nxt > time.time() else nxt + 300
        snap["closed"] = {"count": self._closed_count, "net": round(self._closed_net, 4)}
        snap["last_error"] = self._last_error
        snap["writer_conflict"] = self._writer_conflict()
        return snap

    def _writer_conflict(self) -> str:
        if not os.path.exists(self.lock_path):
            return ""
        try:
            with open(self.lock_path) as fh:
                other = int(fh.read().strip() or 0)
        except (ValueError, OSError):
            return ""
        if other and other != os.getpid() and _pid_alive(other):
            return f"pid {other}"
        return ""

    def chart(self, mother, end_ts: int = 0, timeframe: str = "auto", pad: int = 36) -> dict:
        """One campaign in the payload the Cascade chart renderer already speaks.

        There is one chart on this site. Rather than draw a second one, the
        30-70's geometry is expressed in Cascade's vocabulary — the mother is a
        mother, each fib is a leg whose 0/1 are its anchors, buys are entries
        and the sale is an exit — so the 30-70 inherits the crosshair, the
        zoom, the timeframes and every fix that chart will ever get.

        Works while stopped too: with no cached replay a read-only one is run
        (harvest never writes the journal), so the console can always draw.
        """
        campaigns, df = self._campaigns, self._df
        if not campaigns or df is None:
            df = fetch_window(self.symbol, since_ts=self._history_start())
            start_ts = self._replay_start_ts()
            with REPLAY_LOCK:
                _, _, campaigns = harvest(df, start_ts, set())
            self._campaigns, self._df = campaigns, df
        want = str(mother)
        target_c = None
        for c in campaigns:
            if campaign_id(c) == want:
                target_c = c
                break
        if target_c is None:  # older journal rows keyed by mother alone
            for c in campaigns:
                if (
                    str(int(c.mother_ts.timestamp())) == want
                    or c.mother_ts.tz_convert(IST).strftime("%Y-%m-%d %H:%M") == want
                ):
                    target_c = c
                    break
        if target_c is None:
            raise KeyError(f"No campaign with mother {mother!r} in the current window")
        c = target_c
        idx = df.index
        m_pos = int(idx.get_indexer([c.mother_ts], method="nearest")[0])
        end_pos = len(df) - 1
        if end_ts:
            end_pos = int(idx.get_indexer([pd.Timestamp(int(end_ts), unit="s", tz="UTC")], method="nearest")[0])
        elif c.end_ts is not None:
            end_pos = min(len(df) - 1, int(idx.get_indexer([c.end_ts], method="nearest")[0]) + pad)
        lo_pos = max(0, m_pos - pad)
        win = df.iloc[lo_pos : end_pos + 1]

        # Roll-up: 'auto' picks the smallest bucket that still fits the whole
        # trade on one screen, the way the Cascade chart's Auto does. A months
        # -old ladder is 20,000 5m bars and unreadable at any zoom.
        buckets = {"5m": 1, "15m": 3, "1h": 12, "4h": 48, "1d": 288}
        requested = str(timeframe or "auto").lower()
        if requested in buckets:
            bucket, tf_label = buckets[requested], requested
        else:
            bucket, tf_label = 1, "5m"
            for label, size in buckets.items():
                bucket, tf_label = size, label
                if len(win) / size <= 500:
                    break
        candles = []
        for i in range(0, len(win), bucket):
            chunk = win.iloc[i : i + bucket]
            t0 = int(chunk.index[0].timestamp())
            candles.append(
                {
                    "t": t0,
                    "o": round(float(chunk["open"].iloc[0]), 2),
                    "h": round(float(chunk["high"].max()), 2),
                    "l": round(float(chunk["low"].min()), 2),
                    "c": round(float(chunk["close"].iloc[-1]), 2),
                    "is_mother": t0 <= int(c.mother_ts.timestamp()) < t0 + bucket * 300,
                }
            )
        candles = candles[-1500:]

        # Fib-S is leg 1 (blue), Fib-B is leg 2 (green) — the renderer colours a
        # leg by its id, so the two fibs read apart without a word of legend.
        legs = [
            {
                "leg_id": 1,
                "touch_high": round(c.swing_high, 2),
                "touch_timestamp": int(c.swing_high_ts.timestamp()),
                "low": round(c.swing_low, 2),
                # Only level 2 exists for this rule: reference = max(S2, B2) is
                # the whole trigger, and nothing is ever measured at 4.
                "levels": {"2": round(c.level("S", 2), 2)},
                "orders": [],
            },
            {
                "leg_id": 2,
                # Its 0 IS the mother line and its 1 is usually the same low as
                # the V fib's. Sending them again drew two lines at one price
                # with two labels fighting for the same slot, so the duplicates
                # are dropped and only this fib's own 2 and 4 are added.
                "touch_high": None,
                "touch_timestamp": int(c.mother_ts.timestamp()),
                "low": (
                    round(c.fibB_low_anchor, 2)
                    if c.fibB_low_anchor and abs(c.fibB_low_anchor - c.swing_low) > 0.01
                    else None
                ),
                "levels": {"2": round(c.level("B", 2), 2)},
                "orders": [],
            },
        ]
        entries = [{"t": int(f.ts.timestamp()), "price": round(f.price, 2), "usd": round(f.usd, 2)} for f in c.fills]
        cost = sum(f.usd for f in c.fills)
        exits = []
        if c.status == "TARGET HIT" and c.target_ts is not None and c.fills:
            qty = sum(f.usd / f.price for f in c.fills)
            exits.append(
                {
                    "t": int(c.target_ts.timestamp()),
                    "price": round(c.target, 2),
                    "pnl": round(qty * c.target - cost - 0.001 * (cost + qty * c.target), 2),
                    "avg_entry": round(c.avg_buy, 2),
                }
            )
        pending_entry = c.entry_price() if c._touched and not c._exhausted else 0.0
        start_ts_state = int(self._state.get("start_ts") or 0) if self._state else 0
        frozen = bool(end_ts) or c.status == "TARGET HIT" or c.status.startswith("CANCELLED")
        return {
            "status": "ok",
            "campaign_id": campaign_id(c),
            "symbol": self.symbol,
            "state": c.status,
            "mode": "paper",
            "mother": {"t": int(c.mother_ts.timestamp()), "high": round(c.mother_high, 2)},
            "timeframe": tf_label,
            "timeframe_auto": requested not in buckets,
            "timeframe_options": ["5m", "15m", "1h", "4h", "1d"],
            "campaign_timeframe": "5m",
            "candles": candles,
            "trendlines": [],
            "legs": legs,
            "fills": [{"timestamp": e["t"], "price": e["price"]} for e in entries],
            "entries": entries,
            "exits": exits,
            "avg_entry_price": round(c.avg_buy, 2) if c.fills else None,
            "tp_price": round(c.target, 2) if c.target else None,
            # The armed buy, drawn white. Cascade has no equivalent — its orders
            # rest on fib levels — so the renderer gained one optional line.
            "entry_price": round(pending_entry, 2) if pending_entry else None,
            "last_price": round(float(df["close"].iloc[-1]), 2),
            "frozen": frozen,
            "trade_end_ts": int(c.target_ts.timestamp()) if c.target_ts is not None else 0,
            "close_reason": c.status if frozen else "",
            # 30-70 specifics the Cascade payload has no home for
            "r37": {
                "v_type": c.v_type,
                "fall_pct": round(c.fall_pct, 2),
                "pot_usd": round(c.pot_usd, 2),
                "minor": bool(c.is_minor),
                "cost": round(cost, 2),
                "paper": bool(c.fills and c.fills[0].ts.timestamp() >= start_ts_state) if start_ts_state else False,
                "mother_when": c.mother_ts.tz_convert(IST).strftime("%Y-%m-%d %H:%M"),
                "touch_when": c.touch_ts.tz_convert(IST).strftime("%d %b %H:%M") if c.touch_ts is not None else None,
                "target_when": (
                    c.target_ts.tz_convert(IST).strftime("%Y-%m-%d %H:%M") if c.target_ts is not None else None
                ),
                "buys": [
                    {
                        "when": f.ts.tz_convert(IST).strftime("%d %b %H:%M"),
                        "price": round(f.price, 2),
                        "usd": round(f.usd, 2),
                        "label": f.label,
                    }
                    for f in c.fills
                ],
            },
        }

    def journal(self, limit: int = 200) -> List[dict]:
        if not os.path.exists(self.journal_path):
            return []
        rows = []
        with open(self.journal_path) as journal:
            for line in journal:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return rows[-limit:][::-1]
