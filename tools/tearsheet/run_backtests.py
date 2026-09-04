"""tools/tearsheet/run_backtests.py — the numbers the three tearsheets publish.

One run of every shipped strategy over every coin's whole Binance history, so a
tearsheet quotes a measurement instead of a memory. Nothing here is a
reimplementation: Cascade Hybrid and Cascade_Auto drive `engine/cascade.py`
through the existing `tools/cascade_depth_sweep.Replay`, and the V-Rule drives
`tools/rule3070_sim.run_ladder` — the same two harnesses whose earlier windows
produced the results already on record.

THE CONFIGURATION IS THE SHIPPED ONE, never a tuned one. A tearsheet that
quotes a config the product does not run is a brochure, not a record:

  Cascade Hybrid  engine/cascade.py defaults — levels 2/4/8 @ 20/30/50,
                  target a quarter of the way back, full 5m→1w escalation.
  Cascade_Auto    engine/auto_cascade_fib.py — target HALF way back, climb
                  capped at 4h, the working 5m line graduates at 1h and a new
                  one is anchored, at most half the purse in coin, profit
                  folded in at 25%.
  V-Rule          engine/rule3070_paper.configure() — $-budget enforced at half
                  the purse, 0.35% fee gate, 2 bands, 25% compounding.

Every coin starts from the same $1,000 book and STAYS there: no top-ups, and no
profit folded back in. Cascade_Auto's live rule does fold at 25%, and it was
measured that way first — but the harness sizes every new line off the whole
pot, so a compounding pot makes the book unbounded: peak capital reached
$40,414 on ETH and $77,590 on SOL against a $1,000 purse. Those measure a
strategy with no wallet, not this one. Fixing the book at $1,000 keeps all
three sheets comparable and every number answerable. Peak capital actually committed is
reported beside every result, because that — not the starting number — is what
a book this shape really asks for.

    .venv/bin/python tools/tearsheet/run_backtests.py            # all of it
    .venv/bin/python tools/tearsheet/run_backtests.py --symbol BTCUSDT
    .venv/bin/python tools/tearsheet/run_backtests.py --strategy vrule

Writes tools/tearsheet/data/<strategy>_report_data.json.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import statistics
import sys
import time
from datetime import datetime, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.dirname(os.path.dirname(_HERE))
sys.path.insert(0, _REPO)

DATA_DIR = os.path.join(_HERE, "data")
SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "PAXGUSDT")
STRATEGIES = ("hybrid", "auto", "vrule")
CAPITAL = 1000.0
MONTHS = 110  # deeper than any coin's listing, so each one returns all it has
FEE = 0.001  # 0.1% per side, the rate every engine here already charges
WALLET_FRACTION = 0.5  # auto_cascade_fib: at most half the purse in coin at once


def _log(msg: str) -> None:
    print(msg, flush=True)


# ── cascade: hybrid and auto ─────────────────────────────────────────


def _cascade(symbol: str, auto: bool) -> dict:
    """One cascade book over one coin, through the real engine.

    `auto` swaps the hand-driven defaults for auto_cascade_fib's published
    rules. Both go through the same Replay, so the only difference between the
    two rows of a comparison is the configuration itself.
    """
    from tools.cascade_depth_sweep import MINOR_GAP_PCT, MINOR_SWING_BARS, run_one

    args = (
        symbol,
        "current",  # 2/4/8 @ 20/30/50 — the live ladder
        CAPITAL,
        MONTHS,
        True,  # escalate
        0.0,  # trail: the fixed target, per proj_cascade_trailing_target_verdict
        0,  # minors alongside — auto seeds its own, hybrid runs none
        "4h" if auto else "",  # CAP_TIMEFRAME
        "stay" if auto else "park",  # a capped auto line keeps trading its rung
        "bars",
        # NOT compounding, deliberately — see the note below.
        False,
        False,
        MINOR_GAP_PCT,
        MINOR_SWING_BARS,
        auto,  # spawn_on_escalation: GRADUATE_TIMEFRAME
        (),
        False,
    )
    # TP_FIB_LEVEL is read off the module at fill time and `_restore` does NOT
    # put it back, so it is set on EVERY run, not only the auto one. Setting it
    # for auto alone leaks the half target into whichever hybrid run follows,
    # and the two books would then differ by nothing at all.
    import engine.cascade as cascade

    cascade.TP_FIB_LEVEL = 0.5 if auto else 0.25

    if not auto:
        return run_one(args)

    # WALLET_FRACTION, the rule the sweep harness has no parameter for.
    #
    # auto_cascade_fib holds at most half the purse in coin at once. The
    # harness sizes every campaign it seeds at the FULL pot and, with a new
    # 5m line anchored at every graduation, runs many at once — so an
    # uncapped auto run compounds into a book that never existed: measured
    # first at $107,889 of peak capital on ETH and $341,229 on SOL, against
    # a $1,000 purse. Those are not this strategy's numbers, they are the
    # numbers of a strategy with no wallet.
    #
    # The clamp goes where the live one goes: a new line is refused while the
    # book already holds half the purse. Campaigns keep the size they were
    # born with, exactly as they do live.
    from tools import cascade_depth_sweep as sweep

    original = sweep.Replay._seed_campaign

    def clamped(self, index, mother=None, kind="major"):
        held = sum(c.spent_usd for c in self.engine.campaigns.values())
        held += sum(c.spent_usd for c in self.parked.values())
        if held >= WALLET_FRACTION * self.capital:
            return
        return original(self, index, mother=mother, kind=kind)

    sweep.Replay._seed_campaign = clamped
    try:
        return run_one(args)
    finally:
        sweep.Replay._seed_campaign = original


# ── the V-Rule ───────────────────────────────────────────────────────


def _vrule(symbol: str) -> dict:
    """One V-Rule book over one coin, with rule3070_pnl.py's own accounting.

    That tool prints; a tearsheet needs the numbers. The arithmetic below is
    lifted from its `main()` line for line — closed rounds net of fees, the
    bag valued at the last close, peak concurrent committed cost — so the
    sheet and the CLI can never disagree about the same window.
    """
    import pandas as pd

    import engine.rule3070_paper as paper
    import tools.rule3070_sim as sim
    from tools.fetch_5m_history import load as load_history

    paper.CAPITAL = CAPITAL
    paper.configure()  # the shipped gate: budget, half-purse cap, fee gate, folds
    sim.CAPITAL_USD = CAPITAL

    rows = load_history(symbol, MONTHS)
    frame = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close"])
    frame.index = pd.to_datetime(frame.pop("ts"), unit="s", utc=True)
    frame.index.name = "datetime"

    started = time.time()
    campaigns = sim.run_ladder(frame, minors=True)
    last_close = float(frame["close"].iloc[-1])

    closed, open_, events, holds, per_round = [], [], [], [], []
    monthly: dict = {}
    for c in campaigns:
        if not c.fills:
            continue
        cost = sum(f.usd for f in c.fills)
        qty = sum(f.usd / f.price for f in c.fills)
        for f in c.fills:
            events.append((f.ts, f.usd))
        events.append((c.end_ts, -cost))
        if c.status == "TARGET HIT":
            gross = qty * c.target - cost
            net = gross - FEE * (cost + qty * c.target)
            days = (c.target_ts - c.fills[0].ts).total_seconds() / 86400
            closed.append((c, cost, net, days))
            holds.append(days)
            per_round.append(net)
            monthly[c.target_ts.strftime("%Y-%m")] = round(monthly.get(c.target_ts.strftime("%Y-%m"), 0.0) + net, 4)
        else:
            open_.append((c, cost, qty * last_close - cost))

    events.sort(key=lambda e: (e[0], -e[1]))
    running = peak = 0.0
    for _, delta in events:
        running += delta
        peak = max(peak, running)

    # The equity curve is the closed book only: an open ladder has not paid
    # anything yet, and drawing its paper mark as equity is what makes a
    # never-closes strategy look like it is compounding.
    equity, pot = [], CAPITAL
    for c, _cost, net, _days in sorted(closed, key=lambda row: row[0].target_ts):
        pot += net
        equity.append([int(c.target_ts.timestamp()), round(pot, 4)])

    net_closed = sum(n for _, _, n, _ in closed)
    bag_cost = sum(cost for _, cost, _ in open_)
    bag_value = sum(cost + unreal for _, cost, unreal in open_)
    span_days = (frame.index[-1] - frame.index[0]).days
    return {
        "symbol": symbol,
        "config": "vrule",
        "label": "V-Rule · locked config",
        "capital": CAPITAL,
        "bars": int(len(frame)),
        "first_ts": int(frame.index[0].timestamp()),
        "last_ts": int(frame.index[-1].timestamp()),
        "span_days": span_days,
        "campaigns": len(campaigns),
        "rounds": len(closed),
        "wins": sum(1 for n in per_round if n > 0),
        "losses": sum(1 for n in per_round if n <= 0),
        "net_pnl": round(net_closed, 4),
        "fees": round(sum(FEE * (cost + (cost + net)) for _, cost, net, _ in closed), 4),
        "best_round": round(max(per_round), 4) if per_round else 0.0,
        "worst_round": round(min(per_round), 4) if per_round else 0.0,
        "median_round": round(statistics.median(per_round), 4) if per_round else 0.0,
        "peak_deployed": round(peak, 4),
        "median_hold_hours": round(statistics.median(holds) * 24, 2) if holds else 0.0,
        "max_hold_hours": round(max(holds) * 24, 2) if holds else 0.0,
        "stranded_cost": round(bag_cost, 4),
        "stranded_value": round(bag_value, 4),
        "open_positions": len(open_),
        "open_pnl": round(bag_value - bag_cost, 4),
        "total_pnl": round(net_closed + bag_value - bag_cost, 4),
        "final_capital": round(CAPITAL + net_closed, 4),
        "monthly": monthly,
        "equity": equity,
        "seconds": round(time.time() - started, 2),
    }


# ── shaping ──────────────────────────────────────────────────────────


def _shape(payload: dict, symbol: str, strategy: str) -> dict:
    """One row, in the vocabulary every sheet reads."""
    row = dict(payload)
    row["symbol"] = symbol
    row["strategy"] = strategy
    if "first_ts" not in row:
        from tools.fetch_5m_history import load as load_history

        rows = load_history(symbol, MONTHS)
        row["first_ts"], row["last_ts"] = int(rows[0][0]), int(rows[-1][0])
        row["span_days"] = (rows[-1][0] - rows[0][0]) // 86400
    years = max(row["span_days"] / 365.25, 1e-9)
    row["years"] = round(years, 3)
    total = row.get("total_pnl", 0.0)
    row["return_pct"] = round(total / CAPITAL * 100, 3)
    row["per_year_pct"] = round(total / years / CAPITAL * 100, 3)
    # On the money it really tied up, not the number at the top of the page.
    peak = row.get("peak_deployed") or 0.0
    row["per_year_on_peak_pct"] = round(total / years / peak * 100, 3) if peak else 0.0
    row["first_day"] = datetime.fromtimestamp(row["first_ts"], timezone.utc).strftime("%Y-%m-%d")
    row["last_day"] = datetime.fromtimestamp(row["last_ts"], timezone.utc).strftime("%Y-%m-%d")
    for key in ("equity", "monthly", "per_level_fills", "per_level_usd"):
        row.setdefault(key, [] if key == "equity" else {})
    return row


def run(strategy: str, symbols: tuple) -> dict:
    out = []
    for symbol in symbols:
        started = time.time()
        _log(f"  {strategy:<7} {symbol:<9} …")
        if strategy == "vrule":
            payload = _vrule(symbol)
        else:
            payload = _cascade(symbol, auto=(strategy == "auto"))
        row = _shape(payload, symbol, strategy)
        out.append(row)
        _log(
            f"  {strategy:<7} {symbol:<9} {row['years']:>4.1f}y  rounds {row.get('rounds', 0):>5,}  "
            f"closed ${row.get('net_pnl', 0):>10,.2f}  bag ${row.get('open_pnl', 0):>10,.2f}  "
            f"total ${row.get('total_pnl', 0):>10,.2f}  {row['per_year_pct']:>6.1f}%/yr  "
            f"peak ${row.get('peak_deployed', 0):>8,.0f}  [{time.time() - started:.0f}s]"
        )
    return {
        "strategy": strategy,
        "capital": CAPITAL,
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "fee_per_side_pct": FEE * 100,
        "coins": out,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--symbol", action="append", default=[], help="repeatable; default all four")
    ap.add_argument("--strategy", action="append", default=[], choices=STRATEGIES)
    args = ap.parse_args()
    logging.getLogger("cryptoforge.cascade").setLevel(logging.CRITICAL)
    symbols = tuple(s.upper() for s in args.symbol) or SYMBOLS
    strategies = tuple(args.strategy) or STRATEGIES
    os.makedirs(DATA_DIR, exist_ok=True)
    for strategy in strategies:
        _log(f"── {strategy}")
        book = run(strategy, symbols)
        path = os.path.join(DATA_DIR, f"{strategy}_report_data.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(book, handle, indent=1)
        _log(f"   → {os.path.relpath(path, _REPO)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
