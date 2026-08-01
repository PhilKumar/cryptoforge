"""
tools/cascade_paper_prove.py — paper-prove the four timeframe phases by running
the REAL live stepping path over real Binance candles.

Why this exists rather than more unit tests, and rather than tools/cascade_backtest.py:

  * The unit tests call `_maybe_escalate` and friends directly with hand-built
    history. They prove the decisions. They do not prove the machine.
  * tools/cascade_backtest.py drives `_process_candle` directly, and escalation
    lives in `_candle_step` — so that harness structurally CANNOT exercise
    phase 2, which is exactly why it was never caught either way.

This one fakes the clock and the exchange, then calls `_campaign_tick`, the same
method the live monitor loop calls. Everything in between runs for real:
candle fetching and paging, the 1m mother-break watcher, escalation, geometry,
paper fills, take-profits, mother breaks and auto-restarts.

    .venv/bin/python tools/cascade_paper_prove.py --symbol SOLUSDT --days 30
    .venv/bin/python tools/cascade_paper_prove.py --symbol BTCUSDT --from 2026-06-15 --days 45
    .venv/bin/python tools/cascade_paper_prove.py --symbol SOLUSDT --days 30 --group 5000

1m klines are downloaded once and cached under tools/.paper_prove_cache; every
coarser timeframe is aggregated from them, so the timeframes cannot disagree.
"""

from __future__ import annotations

import argparse
import asyncio
import bisect
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

import pandas as pd
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine import cascade as cascade_module  # noqa: E402
from engine.cascade import Campaign, CascadeEngine  # noqa: E402

IST = timezone(timedelta(hours=5, minutes=30))
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".paper_prove_cache")
MINUTE = 60
KLINE_LIMIT = 1000

# Aggregated from 1m. 1m itself is the source and is served directly.
DERIVED = {"5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400, "1w": 604800}

Row = Tuple[int, float, float, float, float]  # ts, open, high, low, close


def _f(value) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _ist(ts: int) -> str:
    return datetime.fromtimestamp(ts, IST).strftime("%m-%d %H:%M")


# ── data ──────────────────────────────────────────────────────────


def fetch_1m(symbol: str, start_ts: int, end_ts: int, refetch: bool = False) -> List[Row]:
    """Every 1m bar in the window, cached. This is the only thing downloaded."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, f"{symbol}_1m_{start_ts}_{end_ts}.json")
    if os.path.exists(path) and not refetch:
        with open(path, "r", encoding="utf-8") as handle:
            return [tuple(r) for r in json.load(handle)]

    rows: List[Row] = []
    cursor = start_ts * 1000
    pages = 0
    while cursor < end_ts * 1000 and pages < 200:
        resp = requests.get(
            "https://data-api.binance.vision/api/v3/klines",
            params={"symbol": symbol, "interval": "1m", "startTime": cursor, "limit": KLINE_LIMIT},
            timeout=30,
        )
        resp.raise_for_status()
        batch = resp.json()
        if not isinstance(batch, list) or not batch:
            break
        rows.extend((int(k[0]) // 1000, float(k[1]), float(k[2]), float(k[3]), float(k[4])) for k in batch)
        cursor = int(batch[-1][0]) + MINUTE * 1000
        pages += 1
        print(f"\r  fetching 1m … {len(rows):,} bars", end="", flush=True)
    print()
    rows = [r for r in rows if start_ts <= r[0] <= end_ts]
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(rows, handle)
    return rows


def aggregate(rows: List[Row], bucket_sec: int) -> List[Row]:
    """Roll 1m bars up. Only COMPLETE buckets — a partial one is not a candle."""
    out: List[Row] = []
    cur_ts = None
    o = h = low = c = 0.0
    count = 0
    want = bucket_sec // MINUTE
    for ts, ro, rh, rl, rc in rows:
        bucket = ts - (ts % bucket_sec)
        if bucket != cur_ts:
            if cur_ts is not None and count == want:
                out.append((cur_ts, o, h, low, c))
            cur_ts, o, h, low, c, count = bucket, ro, rh, rl, rc, 0
        h, low, c = max(h, rh), min(low, rl), rc
        count += 1
    if cur_ts is not None and count == want:
        out.append((cur_ts, o, h, low, c))
    return out


# ── the fakes ─────────────────────────────────────────────────────


class ReplayClock:
    """Stands in for the `time` module inside engine.cascade."""

    def __init__(self, start: float):
        self.now = float(start)

    def time(self) -> float:
        return self.now

    def monotonic(self) -> float:
        return self.now

    def sleep(self, _seconds: float) -> None:  # pragma: no cover - never hit
        return None


class ReplayBroker:
    """Serves cached candles as klines would, and nothing else."""

    display_name = "Paper prove"

    def __init__(self, symbol: str, series: Dict[str, List[Row]], clock: ReplayClock, tick: str, min_notional: float):
        self.symbol = symbol
        self.series = series
        self.stamps = {tf: [r[0] for r in rows] for tf, rows in series.items()}
        self.clock = clock
        self.tick = tick
        self.min_notional = min_notional
        self.calls: Dict[str, int] = {}

    def _is_configured(self) -> bool:
        return True

    def get_product_by_symbol(self, symbol):
        return {
            "symbol": symbol,
            "broker_symbol": symbol,
            "min_notional": str(self.min_notional),
            "tick_size": self.tick,
            "base_asset": symbol.replace("USDT", ""),
        }

    def get_ticker(self, symbol):
        """Last traded price = the close of the newest complete 1m bar."""
        rows, stamps = self.series["1m"], self.stamps["1m"]
        idx = bisect.bisect_right(stamps, int(self.clock.now) - MINUTE) - 1
        price = rows[idx][4] if idx >= 0 else 0.0
        return {"symbol": symbol, "last_price": price, "mark_price": price}

    def get_wallet(self):
        return None

    def get_orders(self, product_id=None, state="open"):
        return []

    async def async_get_candles(self, symbol, resolution="5m", start=None, end=None, **_kw):
        self.calls[resolution] = self.calls.get(resolution, 0) + 1
        rows, stamps = self.series.get(resolution), self.stamps.get(resolution)
        if not rows:
            return pd.DataFrame()
        # Never hand back a bar the simulated clock has not reached — an
        # exchange has no future data, and "newest page" has to mean newest AS
        # OF NOW. Slicing the newest 1000 off the whole dataset instead made
        # every early no-start fetch return nothing, which looked exactly like
        # an engine that was not watching.
        hi = bisect.bisect_right(stamps, int(self.clock.now))
        if start:
            begin = int(datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
            lo = bisect.bisect_left(stamps, begin)
            window = rows[lo : min(lo + KLINE_LIMIT, hi)]
        else:
            window = rows[max(hi - KLINE_LIMIT, 0) : hi]
        if not window:
            return pd.DataFrame()
        return pd.DataFrame(
            {
                "open": [r[1] for r in window],
                "high": [r[2] for r in window],
                "low": [r[3] for r in window],
                "close": [r[4] for r in window],
                "volume": [1.0] * len(window),
            },
            index=pd.to_datetime([r[0] for r in window], unit="s", utc=True),
        )


# ── the run ───────────────────────────────────────────────────────


@dataclass
class CampaignTrace:
    campaign_id: str
    seq: int
    born_ts: int
    start_timeframe: str
    capital: float
    timeframes: List[Tuple[int, str]] = field(default_factory=list)  # (ts, tf) on each change
    end_state: str = ""
    end_ts: int = 0
    fibs: int = 0
    trendlines: int = 0
    entries: int = 0
    rounds: int = 0
    realised: float = 0.0
    deployed: float = 0.0


async def run(
    symbol: str,
    start_day: str,
    days: int,
    capital: float,
    group: float,
    siblings: int,
    enforce_group: bool,
    refetch: bool,
) -> None:
    span_start = int(datetime.fromisoformat(start_day).replace(tzinfo=IST).timestamp())
    span_end = span_start + days * 86400

    print(
        f"\n{symbol} · {start_day} → {days} days · capital ${capital:,.0f}"
        + (f" · group ${group:,.0f}" if group else "")
    )
    one_min = fetch_1m(symbol, span_start - 2 * 86400, span_end, refetch)
    if len(one_min) < 500:
        print("  not enough data returned; aborting")
        return
    series = {"1m": one_min}
    for tf, sec in DERIVED.items():
        series[tf] = aggregate(one_min, sec)
    print(f"  {len(one_min):,} 1m bars → " + ", ".join(f"{len(series[tf]):,} {tf}" for tf in DERIVED))

    # The mother: the highest 5m bar in the first six hours of the window. A
    # real top is what keeps a campaign alive long enough to climb the ladder.
    head = [r for r in series["5m"] if span_start <= r[0] <= span_start + 6 * 3600]
    if not head:
        print("  no candles at the start of the window; aborting")
        return
    mother = max(head, key=lambda r: r[2])
    # Extra concurrent starts for phase 3. The capital-group clamp only binds
    # when siblings are alive AT THE SAME TIME, and a restart chain never
    # produces that — the parent has ended before its child begins. Without
    # this the group budget is set, reported, and never actually tested.
    extra_mothers: List[Row] = []
    if siblings > 1:
        pool = sorted(
            (r for r in series["5m"] if span_start <= r[0] <= span_start + 12 * 3600),
            key=lambda r: -r[2],
        )
        for row in pool:
            if row[0] == mother[0]:
                continue
            if all(abs(row[0] - m[0]) > 3600 for m in [mother] + extra_mothers):
                extra_mothers.append(row)
            if len(extra_mothers) >= siblings - 1:
                break

    # The clock must start after EVERY mother, siblings included — start_campaign
    # rejects a mother in the future, and the siblings are picked from a wider
    # window than the primary.
    clock = ReplayClock(max([mother[0]] + [m[0] for m in extra_mothers]) + 600)
    cascade_module.time = clock  # the whole engine now runs on simulated time
    tick = "0.01" if mother[2] >= 1000 else ("0.001" if mother[2] >= 10 else "0.0001")

    alerts: List[str] = []
    events: List[Tuple[int, str, str]] = []
    broker = ReplayBroker(symbol, series, clock, tick, 5.0)
    engine = CascadeEngine(
        broker,
        on_alert=lambda title, body, level: alerts.append(f"{title}"),
        # on_event takes the event dict alone. A two-arg lambda here raised
        # TypeError, which the engine swallows by design — so the harness
        # silently reported zero breaks while six campaigns were breaking.
        on_event=lambda event: events.append(
            (int(clock.now), str(event.get("campaign_id") or ""), str(event.get("message") or ""))
        ),
    )
    if group:
        engine.capital_groups[symbol] = group
    if enforce_group:
        # GROUP_CAP_ENFORCED is parked False in production (Phil, 2026-07-28,
        # "until funds grow"), so the clamp cannot be proven without turning it
        # on for the run. This changes the harness only; nothing is written back.
        cascade_module.GROUP_CAP_ENFORCED = True

    campaign = Campaign(
        campaign_id="prove-1",
        symbol=symbol,
        capital_usd=capital,
        mother_high=mother[2],
        mother_low=mother[3],
        mother_timestamp=mother[0],
        mode="paper",
        timeframe="5m",
        start_timeframe="5m",
        escalates=True,
        min_notional_usd=5.0,
        tick_size=float(tick),
        last_processed_ts=mother[0],
        window_start_ts=mother[0],
        mother_watch_last_5m_ts=mother[0],
    )
    engine.campaigns[campaign.campaign_id] = campaign
    print(f"  mother {mother[2]:,.4f} / {mother[3]:,.4f} at {_ist(mother[0])} IST")

    for n, row in enumerate(extra_mothers, start=2):
        # Through start_campaign, not by hand — the group clamp lives there, so
        # constructing these directly would skip the very thing being proved.
        result = await engine.start_campaign(
            symbol=symbol,
            capital_usd=capital,
            mother_high=row[2],
            mother_low=row[3],
            mother_timestamp=row[0],
            mode="paper",
            timeframe="5m",
        )
        got = result.get("campaign", {}).get("capital_usd") if isinstance(result, dict) else None
        note = f"clamped to ${got:,.0f}" if got is not None and got < capital else (result.get("error") or "full")
        print(f"  sibling {n}: mother {row[2]:,.4f} at {_ist(row[0])} — {note}")
    print()

    traces: Dict[str, CampaignTrace] = {}

    def _trace(c: Campaign) -> CampaignTrace:
        t = traces.get(c.campaign_id)
        if t is None:
            t = CampaignTrace(c.campaign_id, c.seq, c.mother_timestamp, c.start_timeframe, c.capital_usd)
            t.timeframes.append((c.mother_timestamp, c.timeframe))
            traces[c.campaign_id] = t
        return t

    # Minute by minute, because 1m is the finest thing the engine watches.
    total = (span_end - int(clock.now)) // MINUTE
    stepped = 0
    while clock.now < span_end:
        clock.now += MINUTE
        stepped += 1
        if stepped % 2000 == 0:
            print(f"\r  replaying … {stepped:,}/{total:,} minutes, {len(traces)} campaign(s)", end="", flush=True)
        for c in list(engine.active_campaigns):
            t = _trace(c)
            before_tf = c.timeframe
            try:
                await engine._campaign_tick(c)
            except Exception as exc:  # a harness must not hide an engine fault
                print(f"\n  ENGINE RAISED on {c.campaign_id} at {_ist(int(clock.now))}: {type(exc).__name__}: {exc}")
                raise
            if c.timeframe != before_tf:
                t.timeframes.append((int(clock.now), c.timeframe))
        # Settle anything the tick scheduled before advancing the clock again.
        await asyncio.sleep(0)
    print()

    for c in list(engine.campaigns.values()) + [
        Campaign.from_dict(row) for row in engine.closed_campaigns if isinstance(row, dict)
    ]:
        t = _trace(c)
        t.end_state = c.state
        t.fibs = len([leg for leg in c.legs if leg.fib])
        t.trendlines = len(c.trendlines)
        t.entries = len(c.all_fills) + sum(len(r.fills or []) for r in c.rounds)
        t.rounds = len(c.rounds)
        t.realised = sum(r.pnl for r in c.rounds)
        # Closing a round clears all_fills, so the open position alone
        # understates what was put to work — a campaign that traded and exited
        # reported $0.00 deployed against a real profit.
        t.deployed = sum(f.price * f.quantity for f in c.all_fills) + sum(
            r.avg_entry * r.quantity for r in c.rounds if r.avg_entry and r.quantity
        )

    _report(symbol, traces, broker, alerts, events, engine)


def _report(symbol, traces, broker, alerts, events, engine) -> None:
    print("=" * 78)
    print(f"  PAPER PROVE · {symbol}")
    print("=" * 78)

    ordered = sorted(traces.values(), key=lambda t: t.born_ts)
    climbed = [t for t in ordered if len(t.timeframes) > 1]
    print(f"\n  Campaigns run: {len(ordered)}   ·   escalated at least once: {len(climbed)}")

    print("\n  Phase 1+2 — per-campaign timeframe and the escalation ladder")
    for t in ordered:
        journey = " → ".join(tf for _, tf in t.timeframes)
        when = _ist(t.born_ts)
        marks = ", ".join(f"{tf} at {_ist(ts)}" for ts, tf in t.timeframes[1:]) or "no climb"
        print(f"    #{t.seq:<3} born {when}  ${t.capital:>8,.0f}  {journey:<22} {t.end_state:<18} ({marks})")

    print("\n  What the engine actually did")
    head = f"    {'#':<4}{'fibs':>5}{'TLs':>5}{'entries':>9}{'rounds':>8}{'deployed':>11}{'realised':>10}"
    print(head)
    for t in ordered:
        print(
            f"    {t.seq:<4}{t.fibs:>5}{t.trendlines:>5}{t.entries:>9}{t.rounds:>8}"
            f"{t.deployed:>11,.2f}{t.realised:>10,.2f}"
        )

    totals = {
        "entries": sum(t.entries for t in ordered),
        "rounds": sum(t.rounds for t in ordered),
        "realised": sum(t.realised for t in ordered),
    }
    print(f"\n    totals: {totals['entries']} entries, {totals['rounds']} rounds, ${totals['realised']:,.2f} realised")

    print("\n  Candle fetches by timeframe (the live paging path)")
    for tf, n in sorted(broker.calls.items(), key=lambda kv: -kv[1]):
        print(f"    {tf:<5} {n:,}")

    breaks = [e for e in events if "broken above" in e[2]]
    print(f"\n  Mother breaks detected: {len(breaks)}")
    for ts, cid, msg in breaks[:8]:
        detail = "on a 1m candle" if "1m candle" in msg else "on its own timeframe"
        print(f"    {_ist(ts)}  {cid}  {detail}")
    if len(breaks) > 8:
        print(f"    … and {len(breaks) - 8} more")

    print("\n  Phase 3 — capital group")
    if engine.capital_groups:
        for sym, budget in engine.capital_groups.items():
            committed = sum(c.capital_usd for c in engine.active_campaigns if c.symbol == sym)
            print(f"    {sym}: budget ${budget:,.0f}, committed by live siblings ${committed:,.0f}")
        clamped = [t for t in ordered if t.capital < max(t.capital for t in ordered)]
        print(f"    clamp enforced: {cascade_module.GROUP_CAP_ENFORCED}")
        print(f"    campaigns clamped below the typed capital: {len(clamped)}")
        if not clamped:
            hint = "--enforce-group" if not cascade_module.GROUP_CAP_ENFORCED else "--siblings 3"
            print(f"    (no clamp bound; re-run with {hint})")
    else:
        print("    no group set (--group N to exercise it)")

    print("\n  Phase 4 — instrument stack view")
    stacks = engine.instrument_stacks()  # symbol -> roll-up dict
    if stacks:
        for sym, st in stacks.items():
            tfs = ",".join(str(t) for t in sorted(st.get("timeframes") or [])) or "-"
            print(
                f"    {sym:<9} active {st.get('active_count', 0):<3} live {st.get('live_count', 0):<3} "
                f"committed ${_f(st.get('committed_usd')):>9,.2f}  in-position ${_f(st.get('in_position_usd')):>8,.2f}  "
                f"realized ${_f(st.get('realized_pnl_usd')):>7,.2f}  rounds {st.get('rounds_closed', 0):<3} tf [{tfs}]"
            )
    else:
        print("    no stacks (nothing active at the end of the window)")

    if alerts:
        counted: Dict[str, int] = {}
        for a in alerts:
            counted[a] = counted.get(a, 0) + 1
        print("\n  Alerts raised")
        for title, n in sorted(counted.items(), key=lambda kv: -kv[1]):
            print(f"    {n:>4}x {title}")
    else:
        print("\n  Alerts raised: none")
    print()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--symbol", default="SOLUSDT")
    ap.add_argument("--from", dest="start_day", default=None, help="YYYY-MM-DD (IST). Default: days+2 ago")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--capital", type=float, default=2000.0)
    ap.add_argument("--group", type=float, default=0.0, help="capital-group budget for the symbol (phase 3)")
    ap.add_argument("--siblings", type=int, default=1, help="concurrent campaigns on the symbol (phase 3)")
    ap.add_argument(
        "--enforce-group",
        action="store_true",
        help="turn GROUP_CAP_ENFORCED on for this run (it is parked False in production)",
    )
    ap.add_argument("--refetch", action="store_true")
    args = ap.parse_args()

    start_day = args.start_day
    if not start_day:
        start_day = (datetime.now(IST) - timedelta(days=args.days + 2)).strftime("%Y-%m-%d")
    asyncio.run(
        run(
            args.symbol,
            start_day,
            args.days,
            args.capital,
            args.group,
            args.siblings,
            args.enforce_group,
            args.refetch,
        )
    )


if __name__ == "__main__":
    main()
