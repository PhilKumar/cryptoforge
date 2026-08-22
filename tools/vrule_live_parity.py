"""Parity: the V-Rule live driver, in paper mode, against the locked simulator.

The live driver (engine/vrule_live.py) re-implements the simulator's ladder
over REAL fills. This harness is how that port is proven: run both over the
same tape — the driver in paper mode, so its fills are modelled the way the
simulator's are — and require every fill, every target and every end to
match, bar for bar, dollar for dollar.

The driver runs against the REAL CascadeEngine, not a stand-in, so the
engine-side booking (fills, rounds, fees, the profit fold) is the code that
will run with money.

  python tools/vrule_live_parity.py                    # seeded synthetic tape
  python tools/vrule_live_parity.py --real SOLUSDT 60  # 60 days of Binance 5m
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import math
import os
import sys
import time
from typing import Dict, List

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import engine.vrule_live as vr  # noqa: E402
import tools.rule3070_sim as sim  # noqa: E402
from engine.cascade import CascadeEngine  # noqa: E402
from engine.rule3070_paper import REPLAY_LOCK, configure  # noqa: E402
from engine.vrule_live import VRuleLive  # noqa: E402


class HarnessBroker:
    """Symbol metadata only. No candles to measure, no ticker, no orders."""

    broker_name = "binance"
    display_name = "Parity"
    min_timeframe = "5m"
    fee_pct_per_side = 0.1
    live_armed = False

    def _is_configured(self):
        return False

    def get_product_by_symbol(self, symbol):
        base = symbol[:-4] if symbol.upper().endswith("USDT") else symbol
        return {
            "symbol": symbol,
            "broker_symbol": symbol,
            "base_asset": base,
            "min_notional": "5.0",
            "tick_size": "0.01",
            "step_size": "0.00001",
        }

    async def async_get_candles(self, *args, **kwargs):
        return pd.DataFrame(columns=["open", "high", "low", "close"])

    def get_ticker(self, symbol):
        return {"last_price": 0.0}


def synthetic_tape(bars: int = 3000, seed: int = 7, start_price: float = 100.0) -> pd.DataFrame:
    """A seeded random walk of closed 5m bars ending at the last closed bar."""
    rng = np.random.default_rng(seed)
    end = (int(time.time()) // 300) * 300 - 300
    ts = np.arange(end - (bars - 1) * 300, end + 1, 300)
    rets = rng.normal(0.0, 0.003, size=bars)
    closes = start_price * np.exp(np.cumsum(rets))
    opens = np.concatenate([[start_price], closes[:-1]])
    wick = np.abs(rng.normal(0.0, 0.002, size=bars)) * closes
    highs = np.maximum(opens, closes) + wick
    lows = np.minimum(opens, closes) - np.abs(rng.normal(0.0, 0.002, size=bars)) * closes
    df = pd.DataFrame({"open": opens, "high": highs, "low": lows, "close": closes})
    df.index = pd.to_datetime(ts, unit="s", utc=True)
    df.index.name = "datetime"
    return df


def run_simulator(df: pd.DataFrame, purse: float):
    with REPLAY_LOCK:
        configure()
        sim.CAPITAL_USD = float(purse)
        sim.BUDGET_FROM_TS = None
        campaigns = sim.run_ladder(df, minors=True)
        final = {"purse": float(sim.CAPITAL_USD), "bank": float(sim._PROFIT_BANK)}
        sim.BUDGET_FROM_TS = None
    return campaigns, final


def _vid(c) -> str:
    return f"{int(c.mother_ts.timestamp())}-{int(c.swing_low_ts.timestamp())}"


async def run_driver(df: pd.DataFrame, purse: float, births, symbol: str = "BTCUSDT"):
    """Tick the driver once per bar over the tape, births fed in as the bars close."""
    engine = CascadeEngine(HarnessBroker())
    engine.start = lambda: None  # no monitor loop: the harness is the clock
    vr.HISTORY_LIMIT = 10**9  # every ended ladder must be there to compare
    state = {"i": 0}
    # Record fills and closes at the engine call, not from archived payloads:
    # the comparison must see exactly what the engine booked, with nothing
    # between it and the booking.
    engine.fill_log = {}
    engine.close_log = {}
    real_fill, real_close = engine._fill_pending, engine._close_round

    def logged_fill(campaign, price, timestamp, order_id="PAPER"):
        usd = campaign.pending_usd
        real_fill(campaign, price, timestamp, order_id)
        engine.fill_log.setdefault(campaign.campaign_id, []).append((int(timestamp), float(price), float(usd)))

    def logged_close(campaign, exit_price, sold_qty=None, sell_fee=None):
        real_close(campaign, exit_price, sold_qty=sold_qty, sell_fee=sell_fee)
        engine.close_log[campaign.campaign_id] = float(exit_price)

    engine._fill_pending = logged_fill
    engine._close_round = logged_close

    def window(_book):
        return df.iloc[: state["i"] + 1]

    def scanner(_book, dfw):
        last = dfw.index[-1]
        out = []
        for c in births:
            if c.born_ts is None or c.born_ts > last:
                continue
            clone = copy.copy(c)
            clone.fills = []  # the driver must never see the simulator's fills
            clone.status = "OPEN"
            out.append(clone)
        return out

    driver = VRuleLive(engine, window_loader=window, structure_scanner=scanner)
    book = driver.set_book(symbol, enabled=True, mode="paper", capital_usd=purse)
    book.start_ts = int(df.index[0].timestamp())
    book.history_start_ts = book.start_ts
    for i in range(len(df)):
        state["i"] = i
        now = float(int(df.index[i].timestamp()) + 300 + 11)
        await driver.tick(now)
    return driver, engine, book


def compare(sim_campaigns, driver, engine, book, purse: float, sim_final: dict) -> dict:
    sim_by: Dict[str, object] = {_vid(c): c for c in sim_campaigns if c.born_ts is not None}
    # Driver side: ended ladders are in book.history with their campaign id;
    # still-open ones in book.ladders. Campaign payloads hold the fills.
    drv: Dict[str, dict] = {}
    for row in book.history:
        cid = row["campaign_id"]
        drv[row["vid"]] = {
            "ended": row["ended"],
            "end_ts": int(row.get("last_ts") or 0),
            "fills": list(engine.fill_log.get(cid, [])),
            "target": engine.close_log.get(cid),
        }
    for vid, ladder in book.ladders.items():
        camp = engine.campaigns.get(ladder.campaign_id)
        drv[vid] = {
            "ended": "",
            "end_ts": 0,
            "fills": list(engine.fill_log.get(ladder.campaign_id, [])),
            "target": getattr(camp, "tp_override_price", None),
        }

    mismatches: List[str] = []
    checked = fills_checked = 0
    for vid, c in sim_by.items():
        d = drv.get(vid)
        if d is None:
            mismatches.append(f"{vid}: simulator born it, driver never opened it ({c.status})")
            continue
        checked += 1
        sfills = [(int(f.ts.timestamp()), float(f.price), float(f.usd)) for f in c.fills]
        if len(sfills) != len(d["fills"]):
            mismatches.append(f"{vid}: {len(sfills)} simulator fills vs {len(d['fills'])} driver fills")
            continue
        for (sts, sp, su), (dts, dp, du) in zip(sfills, d["fills"]):
            fills_checked += 1
            if sts != dts or abs(sp - dp) > 1e-6 * max(sp, 1) or abs(su - du) > 0.011:
                mismatches.append(f"{vid}: fill {sts}/{sp:.4f}/${su:.2f} vs {dts}/{dp:.4f}/${du:.2f}")
        s_end = "target" if c.status == "TARGET HIT" else ("cancelled" if c.status.startswith("CANCELLED") else "")
        if s_end != d["ended"]:
            mismatches.append(f"{vid}: simulator {c.status!r} vs driver ended={d['ended']!r}")
            continue
        if s_end:
            s_end_ts = int(c.end_ts.timestamp()) if c.end_ts is not None else 0
            if s_end_ts != d["end_ts"]:
                mismatches.append(f"{vid}: end bar {s_end_ts} vs {d['end_ts']}")
            if s_end == "target" and c.target and d["target"] and abs(c.target - d["target"]) > 1e-6 * c.target:
                mismatches.append(f"{vid}: target {c.target:.4f} vs {d['target']:.4f}")
    extra = set(drv) - set(sim_by)
    for vid in extra:
        mismatches.append(f"{vid}: driver opened a ladder the simulator never born")

    purse_ok = math.isclose(book.purse_usd, sim_final["purse"], rel_tol=1e-6, abs_tol=0.02)
    bank_ok = math.isclose(book.pocket_usd, sim_final["bank"], rel_tol=1e-6, abs_tol=0.02)
    if not purse_ok:
        mismatches.append(f"purse: driver ${book.purse_usd:.4f} vs simulator ${sim_final['purse']:.4f}")
    if not bank_ok:
        mismatches.append(f"profit bank: driver ${book.pocket_usd:.4f} vs simulator ${sim_final['bank']:.4f}")
    return {
        "campaigns": len(sim_by),
        "checked": checked,
        "fills": fills_checked,
        "targets": sum(1 for c in sim_by.values() if c.status == "TARGET HIT"),
        "cancelled": sum(1 for c in sim_by.values() if c.status.startswith("CANCELLED")),
        "open_at_end": sum(1 for c in sim_by.values() if c.status.startswith("OPEN")),
        "purse": {"driver": round(book.purse_usd, 4), "simulator": round(sim_final["purse"], 4)},
        "bank": {"driver": round(book.pocket_usd, 4), "simulator": round(sim_final["bank"], 4)},
        "last_error": book.last_error,
        "tried": len(book.tried),
        "mismatches": mismatches,
        "ok": not mismatches,
    }


def run_parity(df: pd.DataFrame, purse: float = 2000.0, symbol: str = "BTCUSDT") -> dict:
    campaigns, final = run_simulator(df, purse)
    driver, engine, book = asyncio.run(run_driver(df, purse, campaigns, symbol))
    return compare(campaigns, driver, engine, book, purse, final)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--real", nargs=2, metavar=("SYMBOL", "DAYS"), help="fetch real Binance 5m bars")
    ap.add_argument("--bars", type=int, default=3000)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--purse", type=float, default=2000.0)
    args = ap.parse_args()
    if args.real:
        from engine.rule3070_paper import fetch_window

        symbol, days = args.real[0].upper(), int(args.real[1])
        df = fetch_window(symbol, days=days)
        label = f"{symbol} real tape, {days} days, {len(df)} bars"
    else:
        symbol = "BTCUSDT"
        df = synthetic_tape(args.bars, args.seed)
        label = f"synthetic tape, seed {args.seed}, {len(df)} bars"
    started = time.time()
    report = run_parity(df, args.purse, symbol)
    print(f"{label} — {time.time() - started:.1f}s")
    print(
        f"campaigns {report['campaigns']} (targets {report['targets']}, cancelled {report['cancelled']}, "
        f"open {report['open_at_end']}) · fills compared {report['fills']}"
    )
    print(f"purse  driver ${report['purse']['driver']:,.4f}  simulator ${report['purse']['simulator']:,.4f}")
    print(f"bank   driver ${report['bank']['driver']:,.4f}  simulator ${report['bank']['simulator']:,.4f}")
    if report["last_error"]:
        print(f"last start error: {report['last_error']}")
    for m in report["mismatches"][:40]:
        print("  MISMATCH", m)
    print("PASS" if report["ok"] else f"FAIL ({len(report['mismatches'])} mismatches)")
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
