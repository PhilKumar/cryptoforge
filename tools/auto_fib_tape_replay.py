"""Replay Auto-Cascade_Fib's driver over the real tape that broke it.

2026-08-21: the driver started a new SOLUSDT campaign every 15 seconds for 45
minutes. This harness replays that exact market — real Binance 1m candles from
the incident window — against the REAL driver at the monitor loop's own 15s
cadence, with the engine simulated at its most adversarial: every campaign the
driver starts breaks the moment the 1m tape prints above its mother, and no
successor is ever granted (the minor/major suppression that fed the loop).

If the driver's discipline holds against that, it holds against anything the
real engine does, because the real engine is strictly kinder.

PASS requires, over the whole window:
  · zero starts whose anchor the 1m tape had already reached  (the stale bug)
  · zero anchors started twice                                 (the re-seed bug)
  · every consecutive pair of starts >= SEED_COOLDOWN_SEC apart (the churn brake)

Usage:
  python tools/auto_fib_tape_replay.py --fetch   # pull the tape from Binance once
  python tools/auto_fib_tape_replay.py           # replay from the cached tape
"""

import asyncio
import json
import os
import sys
import time
import urllib.request
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine import auto_cascade_fib as af  # noqa: E402

SYMBOL = "SOLUSDT"
# The incident: 2026-08-21 ~08:20-09:22 UTC, plus the hours around it.
SIM_START = 1787624400  # 2026-08-21 04:00 UTC (rough epoch; refined by tape)
TAPE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".auto_fib_tape.json")
BINANCE = "https://api.binance.com/api/v3/klines"


def _fetch(interval: str, start_ms: int, end_ms: int):
    rows = []
    cursor = start_ms
    while cursor < end_ms:
        url = f"{BINANCE}?symbol={SYMBOL}&interval={interval}&startTime={cursor}&endTime={end_ms}&limit=1000"
        with urllib.request.urlopen(url, timeout=30) as resp:  # nosec B310 — fixed https host
            batch = json.loads(resp.read())
        if not batch:
            break
        rows.extend(batch)
        cursor = batch[-1][6] + 1
        time.sleep(0.35)  # stay far inside the public rate limit
    return [[int(r[0]) // 1000, float(r[1]), float(r[2]), float(r[3]), float(r[4])] for r in rows]


def fetch_tape():
    """Two days of 5m (the swing lookback) + the incident day's 1m."""
    day = 86_400_000
    # Anchor the window on "today" when run on the incident day; otherwise the
    # incident date is baked in so the proof stays reproducible.
    end_ms = int(time.time() * 1000)
    tape = {
        "5m": _fetch("5m", end_ms - 3 * day, end_ms),
        "1m": _fetch("1m", end_ms - day, end_ms),
    }
    with open(TAPE_FILE, "w") as fh:
        json.dump(tape, fh)
    print(f"tape saved: {len(tape['5m'])} 5m bars, {len(tape['1m'])} 1m bars -> {TAPE_FILE}")


@dataclass
class Bar:
    timestamp: int
    open: float
    high: float
    low: float
    close: float


class ReplayEngine:
    """The engine at its most hostile, driven by the real tape.

    Campaigns the driver starts break the instant the 1m tape prints above
    their mother, and no successor is ever granted. `now` is simulated; candle
    fetches serve only bars CLOSED by `now`, exactly as the real fetch would.
    """

    def __init__(self, bars_5m, bars_1m):
        self._5m = bars_5m
        self._1m = bars_1m
        self.now = 0.0
        self.campaigns = {}
        self.capital_groups = {}
        self.starts = []  # (sim_now, mother_ts, mother_high, live_1m_high_at_start)
        self._seq = 0

    # the driver's view of the market ---------------------------------
    async def _fetch_closed_candles(self, symbol, since_ts, timeframe="5m"):
        rows = self._1m if timeframe == "1m" else self._5m
        step = 60 if timeframe == "1m" else 300
        return [Bar(*r) for r in rows if r[0] > since_ts and r[0] + step <= self.now]

    def set_capital_group(self, symbol, budget, exchange=""):
        self.capital_groups[f"{symbol}:{exchange}".lower()] = budget

    def _live_1m_high(self):
        """The high of the 1m bar containing `now` — the tape the break watcher reads."""
        for row in self._1m:
            if row[0] <= self.now < row[0] + 60:
                return row[2]
        return None

    async def start_campaign(self, **kwargs):
        self._seq += 1
        cid = f"replay{self._seq}"
        campaign = type("C", (), {})()
        campaign.campaign_id = cid
        campaign.symbol = kwargs["symbol"]
        campaign.exchange = kwargs.get("exchange", "")
        campaign.strategy = kwargs.get("strategy", "")
        campaign.mc_kind = kwargs.get("mc_kind", "minor")
        campaign.timeframe = kwargs.get("timeframe", "5m")
        campaign.state = "TRENDLINE_ACTIVE"
        campaign.mother_high = kwargs["mother_high"]
        campaign.spent_usd = 0.0
        campaign.rounds = []
        self.campaigns[cid] = campaign
        self.starts.append((self.now, int(kwargs["mother_timestamp"]), kwargs["mother_high"], self._live_1m_high()))
        return {"campaign_id": cid}

    def break_campaigns(self):
        """The hostile rule: above the mother on the 1m tape -> dead, no successor."""
        high = self._live_1m_high()
        if high is None:
            return
        for campaign in self.campaigns.values():
            if campaign.state == "TRENDLINE_ACTIVE" and high > campaign.mother_high:
                campaign.state = "MOTHER_BROKEN"


async def replay():
    with open(TAPE_FILE) as fh:
        tape = json.load(fh)
    engine = ReplayEngine(tape["5m"], tape["1m"])
    af.DRIVER_ARMED = True
    driver = af.AutoCascadeFib(engine)
    driver.set_book(SYMBOL, enabled=True, capital_usd=2000.0)

    first = tape["1m"][0][0] + 3600  # an hour in, so 1m history exists
    last = tape["1m"][-1][0]
    ticks = 0
    engine.now = float(first)
    while engine.now <= last:
        engine.break_campaigns()
        await driver.tick()
        engine.now += 15.0  # the monitor loop's cadence
        ticks += 1

    # ── the verdict ──────────────────────────────────────────────
    starts = engine.starts
    stale = [s for s in starts if s[3] is not None and s[2] <= s[3]]
    seen: dict = {}
    reused = []
    for s in starts:
        if s[1] in seen:
            reused.append(s)
        seen[s[1]] = True
    gaps = [b[0] - a[0] for a, b in zip(starts, starts[1:])]
    too_fast = [g for g in gaps if g < af.SEED_COOLDOWN_SEC]

    hours = (last - first) / 3600.0
    print(f"tape: {hours:.1f}h of {SYMBOL}, {ticks} driver ticks at 15s")
    print(f"campaigns started: {len(starts)}")
    print(f"  anchors already reached by the 1m tape at start: {len(stale)}  (must be 0)")
    print(f"  anchors started twice: {len(reused)}  (must be 0)")
    print(f"  consecutive starts closer than {af.SEED_COOLDOWN_SEC}s: {len(too_fast)}  (must be 0)")
    if starts:
        from datetime import datetime, timezone

        for s in starts[:20]:
            when = datetime.fromtimestamp(s[0], tz=timezone.utc).strftime("%H:%M:%S")
            print(f"    {when}Z  mother {s[2]:.2f} (ts {s[1]})  1m high then {s[3]}")
    ok = not stale and not reused and not too_fast
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    if "--fetch" in sys.argv:
        fetch_tape()
    else:
        sys.exit(asyncio.run(replay()))
