#!/usr/bin/env python3
"""
tools/exchange_parity_check.py — does the SAME mother candle behave the same
on two exchanges?

Phil's requirement for multi-venue Cascade: starting the same MC on Binance and
CoinDCX must produce the same campaign — same trendlines, same fib levels, same
entries — with nothing between them but slippage.

This replays the real engine twice over the same window, once on each venue's
own candles, and diffs the result. It reads market data only: no keys, no
orders, nothing written.

Usage
-----
    python3 tools/exchange_parity_check.py                 # BTCUSDT + SOLUSDT, 5m
    python3 tools/exchange_parity_check.py --symbol SOLUSDT --timeframe 15m
    python3 tools/exchange_parity_check.py --bars 300
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker.binance import BinanceSpotClient  # noqa: E402
from broker.coindcx_spot import CoinDCXSpotClient  # noqa: E402
from engine.cascade import Campaign, Candle, CascadeEngine  # noqa: E402
from tools.cascade_backtest import _OfflineBroker, _tick_for  # noqa: E402

# A price gap under this counts as slippage, not a behaviour difference.
PRICE_TOLERANCE_PCT = 0.05


def rows_from(df) -> list[tuple]:
    return [(int(ts.timestamp()), r["open"], r["high"], r["low"], r["close"]) for ts, r in df.iterrows()]


def replay(symbol: str, timeframe: str, rows: list[tuple], mother_index: int, bars: int) -> dict:
    """Run the real engine over one venue's candles and describe what it built."""
    mother = rows[mother_index]
    engine = CascadeEngine(_OfflineBroker(symbol, _tick_for(mother[2])))
    campaign = Campaign(
        campaign_id="parity",
        symbol=symbol,
        capital_usd=2000.0,
        mother_high=mother[2],
        mother_low=mother[3],
        mother_timestamp=mother[0],
        mode="paper",
        timeframe=timeframe,
        escalates=False,  # hold the timeframe still: escalation is not what we are comparing
        min_notional_usd=5.0,
        tick_size=float(_tick_for(mother[2])),
        last_processed_ts=mother[0],
        window_start_ts=mother[0],
    )
    engine.campaigns["parity"] = campaign
    history: list[Candle] = []
    for ts, o, h, low, c in rows[mother_index + 1 : mother_index + 1 + bars]:
        candle = Candle(ts, o, h, low, c)
        history.append(candle)
        engine._candles["parity"] = list(history)
        engine._process_candle(campaign, candle)
    return {
        "mother_high": mother[2],
        "mother_low": mother[3],
        "state": campaign.state,
        "trendlines": [
            (tl.anchor1_timestamp, round(tl.anchor1_price, 8), tl.anchor2_timestamp, round(tl.anchor2_price, 8))
            for tl in campaign.trendlines
        ],
        "fib_legs": [(round(leg.touch_high, 8), round(leg.low, 8)) for leg in campaign.legs],
        "entries": [(round(f.price, 8), round(f.quantity, 8)) for f in campaign.all_fills],
        # None until something fills — a campaign that never entered is a
        # perfectly valid outcome to compare, so normalise rather than crash.
        "avg_entry": round(campaign.avg_entry_price or 0.0, 8),
        "spent_usd": round(campaign.spent_usd, 6),
        "rounds": len(campaign.rounds),
    }


def _close(a: float, b: float) -> bool:
    if a == b:
        return True
    scale = max(abs(a), abs(b), 1e-9)
    return abs(a - b) / scale * 100 <= PRICE_TOLERANCE_PCT


def compare(left: dict, right: dict) -> list[str]:
    """Differences that are NOT explained by slippage."""
    problems = []
    if left["state"] != right["state"]:
        problems.append(f"state differs: {left['state']} vs {right['state']}")
    for field in ("trendlines", "fib_legs", "entries"):
        if len(left[field]) != len(right[field]):
            problems.append(f"{field}: {len(left[field])} vs {len(right[field])}")
            continue
        for i, (a, b) in enumerate(zip(left[field], right[field])):
            for j, (x, y) in enumerate(zip(a, b)):
                # Timestamps must match exactly; prices may drift by slippage.
                exact = isinstance(x, int) and isinstance(y, int)
                if (x != y) if exact else (not _close(float(x), float(y))):
                    problems.append(f"{field}[{i}] item {j}: {x} vs {y}")
    if left["rounds"] != right["rounds"]:
        problems.append(f"rounds closed: {left['rounds']} vs {right['rounds']}")
    if not _close(left["avg_entry"], right["avg_entry"]):
        problems.append(f"avg entry: {left['avg_entry']} vs {right['avg_entry']}")
    return problems


def run_symbol(symbol: str, timeframe: str, bars: int) -> bool:
    print(f"\n=== {symbol} · {timeframe} ===")
    binance = BinanceSpotClient().get_candles(symbol, resolution=timeframe)
    coindcx = CoinDCXSpotClient().get_candles(symbol, resolution=timeframe)
    if not len(binance) or not len(coindcx):
        print("  could not fetch candles from both venues")
        return False

    # Compare only the window both venues actually cover, so a longer history on
    # one side cannot masquerade as a behaviour difference.
    shared = binance.index.intersection(coindcx.index)
    if len(shared) < bars + 5:
        print(f"  only {len(shared)} shared bars; need {bars + 5}")
        return False
    shared = shared[-(bars + 5) :]
    b_rows = rows_from(binance.loc[shared])
    c_rows = rows_from(coindcx.loc[shared])

    diffs = sum(1 for a, b in zip(b_rows, c_rows) if a[2] != b[2] or a[3] != b[3])
    print(f"  shared bars: {len(shared)}   bars whose high/low differ at all: {diffs}")

    # The mother must be a bar price actually falls away from, or the campaign
    # breaks on the next candle and the comparison proves nothing: two engines
    # agreeing that nothing happened is not parity. Pick the bar followed by the
    # DEEPEST fall, which is what arms fibs and produces fills — the part of the
    # behaviour most worth comparing.
    head = max(3, len(b_rows) // 2)

    def fall_from(i: int) -> float:
        after = b_rows[i + 1 :]
        if not after:
            return 0.0
        return (b_rows[i][2] - min(r[3] for r in after)) / b_rows[i][2]

    mother_index = max(range(head), key=fall_from)
    print(f"  mother bar: index {mother_index} of {len(b_rows)} — fall after it {fall_from(mother_index) * 100:.2f}%")

    left = replay(symbol, timeframe, b_rows, mother_index, bars)
    right = replay(symbol, timeframe, c_rows, mother_index, bars)
    print(
        f"  mother       Binance {left['mother_high']}/{left['mother_low']}   CoinDCX {right['mother_high']}/{right['mother_low']}"
    )
    print(f"  state        {left['state']}  |  {right['state']}")
    print(f"  trendlines   {len(left['trendlines'])}  |  {len(right['trendlines'])}")
    print(f"  fib legs     {len(left['fib_legs'])}  |  {len(right['fib_legs'])}")
    print(f"  entries      {len(left['entries'])}  |  {len(right['entries'])}")

    problems = compare(left, right)
    if problems:
        print("  MISMATCH:")
        for p in problems:
            print(f"    - {p}")
        return False
    print("  PARITY OK — identical behaviour within slippage tolerance")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", action="append", help="repeatable; default BTCUSDT and SOLUSDT")
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--bars", type=int, default=200)
    args = parser.parse_args()
    symbols = args.symbol or ["BTCUSDT", "SOLUSDT"]
    ok = all([run_symbol(s.upper(), args.timeframe, args.bars) for s in symbols])
    print("\n" + ("all symbols in parity" if ok else "PARITY FAILED — see mismatches above"))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
