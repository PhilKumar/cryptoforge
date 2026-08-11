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

WINDOW_DAYS = 90
CAPITAL = 2000.0
KLINES = "https://api.binance.com/api/v3/klines"
IST = "Asia/Kolkata"
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "out", "rule3070")
STATE_PATH = os.path.join(OUT, "paper_state.json")
JOURNAL_PATH = os.path.join(OUT, "paper_journal.jsonl")
LOCK_PATH = os.path.join(OUT, "paper.lock")


def configure() -> None:
    """The locked config — the exact rules the full-history verdict ran."""
    sim.CAPITAL_USD = CAPITAL
    sim.ENFORCE_BUDGET = True
    sim.MIN_NET_MARGIN = 0.0035
    sim.MAX_BANDS = 2
    sim.COMPOUND_AT_HALF = True
    sim.COMPOUND_SCHEDULE = (0.25,)


def fetch_window(symbol: str, days: int = WINDOW_DAYS) -> pd.DataFrame:
    """The trailing `days` of CLOSED 5m candles, oldest first."""
    end = int(time.time() * 1000)
    start = end - days * 86400 * 1000
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


def harvest(df: pd.DataFrame, start_ts: int, seen: set) -> tuple:
    """Replay the window; return (new events, open-position summaries, campaigns)."""
    configure()
    campaigns = sim.run_ladder(df, minors=True)
    last_close = float(df["close"].iloc[-1]) if len(df) else 0.0
    events: List[dict] = []
    opens: List[dict] = []
    for c in campaigns:
        mother = c.mother_ts.tz_convert(IST).strftime("%Y-%m-%d %H:%M")
        for f in c.fills:
            if f.ts.timestamp() < start_ts:
                continue
            key = f"fill:{mother}:{f.label}"
            if key in seen:
                continue
            events.append(
                {
                    "kind": "BUY",
                    "key": key,
                    "ts": int(f.ts.timestamp()),
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


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


class Rule3070PaperService:
    """Background paper trader with start/stop/reset for the console."""

    def __init__(self, symbol: str = "BTCUSDT"):
        self.symbol = symbol
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

    # -- lifecycle ---------------------------------------------------

    def _prepare(self) -> None:
        self._acquire_writer_lock()
        os.makedirs(OUT, exist_ok=True)
        if os.path.exists(STATE_PATH):
            self._state = json.load(open(STATE_PATH))
        else:
            self._state = {"start_ts": int(time.time()), "seen": []}
            self._write_state()
        self._seen = set(self._state.get("seen", []))
        self._load_closed_totals()

    def start(self) -> dict:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return self.status()
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
            for path in (STATE_PATH, JOURNAL_PATH):
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
        if os.path.exists(LOCK_PATH):
            try:
                other = int(open(LOCK_PATH).read().strip() or 0)
            except ValueError:
                other = 0
            if other and other != os.getpid() and _pid_alive(other):
                raise RuntimeError(
                    f"Another paper writer is running (pid {other}) — stop it first "
                    f"(the nohup CLI runner and the site console must not write together)"
                )
        with open(LOCK_PATH, "w") as fh:
            fh.write(str(os.getpid()))

    def _release_writer_lock(self) -> None:
        try:
            if os.path.exists(LOCK_PATH) and int(open(LOCK_PATH).read().strip() or 0) == os.getpid():
                os.remove(LOCK_PATH)
        except (ValueError, OSError):
            pass

    # -- state -------------------------------------------------------

    def _write_state(self) -> None:
        self._state["seen"] = sorted(self._seen)
        with open(STATE_PATH, "w") as fh:
            json.dump(self._state, fh)

    def _load_closed_totals(self) -> None:
        self._closed_count = 0
        self._closed_net = 0.0
        if not os.path.exists(JOURNAL_PATH):
            return
        for line in open(JOURNAL_PATH):
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

    def _tick(self) -> None:
        df = fetch_window(self.symbol)
        events, opens, _ = harvest(df, int(self._state["start_ts"]), self._seen)
        for e in events:
            self._seen.add(e["key"])
            with open(JOURNAL_PATH, "a") as jf:
                jf.write(json.dumps(e) + "\n")
            if e["kind"] == "TARGET":
                self._closed_count += 1
                self._closed_net += float(e["net"])
            _logger.info("30-70 paper %s %s", e["kind"], e.get("label") or e.get("mother"))
        if events:
            self._write_state()
        self._opens = opens
        self._status = {
            "running": True,
            "symbol": self.symbol,
            "capital": CAPITAL,
            "purse": round(float(sim.CAPITAL_USD), 2),
            "start_ts": int(self._state["start_ts"]),
            "last_tick_ts": int(time.time()),
            "last_close": round(float(df["close"].iloc[-1]), 2) if len(df) else None,
            "bars": int(len(df)),
        }

    # -- reads -------------------------------------------------------

    def status(self) -> dict:
        running = bool(self._thread and self._thread.is_alive())
        snap = dict(self._status)
        snap["running"] = running
        snap.setdefault("symbol", self.symbol)
        snap.setdefault("capital", CAPITAL)
        if not snap.get("start_ts") and os.path.exists(STATE_PATH):
            try:
                snap["start_ts"] = int(json.load(open(STATE_PATH)).get("start_ts") or 0)
            except (json.JSONDecodeError, OSError):
                pass
        paper_opens = [o for o in self._opens if o.get("paper")]
        snap["opens"] = {
            "count": len(paper_opens),
            "cost": round(sum(o["cost"] for o in paper_opens), 2),
            "unrealised": round(sum(o["unrealised"] for o in paper_opens), 2),
            "rows": paper_opens[:20],
            "warmup_holding": len(self._opens) - len(paper_opens),
        }
        snap["closed"] = {"count": self._closed_count, "net": round(self._closed_net, 4)}
        snap["last_error"] = self._last_error
        snap["writer_conflict"] = self._writer_conflict()
        return snap

    def _writer_conflict(self) -> str:
        if not os.path.exists(LOCK_PATH):
            return ""
        try:
            other = int(open(LOCK_PATH).read().strip() or 0)
        except (ValueError, OSError):
            return ""
        if other and other != os.getpid() and _pid_alive(other):
            return f"pid {other}"
        return ""

    def journal(self, limit: int = 200) -> List[dict]:
        if not os.path.exists(JOURNAL_PATH):
            return []
        rows = []
        for line in open(JOURNAL_PATH):
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return rows[-limit:][::-1]
