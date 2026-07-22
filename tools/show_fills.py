#!/usr/bin/env python3
"""
tools/show_fills.py — what the EXCHANGE says it filled, and when.

The engine log records when it *noticed* a fill, not when Binance executed it.
Those are different, and confusing them cost an evening: BTCUSDT #36 logged
"Bought $7.60 at 65,844.03" at 16:25 IST, but the buy stop had been resting at
65,843.98 since 13:23 with price crossing back above it minutes later. Either
the exchange really did wait three hours, or the engine took three hours to
ingest a fill it should have seen in ten seconds — and only Binance's own
timestamp can say which.

Read-only. Places nothing, cancels nothing. All times in IST.

    venv/bin/python tools/show_fills.py
    venv/bin/python tools/show_fills.py --symbol BTCUSDT --hours 12
"""

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker import get_broker_client  # noqa: E402

IST = timezone(timedelta(hours=5, minutes=30))


def ist(ms) -> str:
    try:
        return datetime.fromtimestamp(int(ms) / 1000, IST).strftime("%m-%d %H:%M:%S")
    except Exception:
        return "?"


def num(value, default=0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--symbol", action="append", help="restrict to this symbol; repeatable")
    parser.add_argument("--hours", type=float, default=24.0, help="how far back to show (default 24)")
    args = parser.parse_args()

    client = get_broker_client()
    checker = getattr(client, "_is_configured", None)
    if callable(checker) and not checker():
        print("Broker API keys are not configured.")
        return 1

    base = getattr(client, "base_url", "?")
    env = "TESTNET" if "testnet" in str(base).lower() else "LIVE"
    print(f"Broker: {getattr(client, 'display_name', '?')}   environment: {env}")
    print(f"Times are IST. Showing the last {args.hours:g}h.\n")

    try:
        trades = client.get_order_history(force_refresh=True) or []
    except Exception as exc:
        print(f"Could not fetch trade history: {exc}")
        return 1

    cutoff_ms = (datetime.now(timezone.utc) - timedelta(hours=args.hours)).timestamp() * 1000
    rows = []
    for row in trades:
        if not isinstance(row, dict):
            continue
        when = row.get("time") or row.get("timestamp") or row.get("created_at")
        if when is None or num(when, 0) < cutoff_ms:
            continue
        symbol = str(row.get("symbol") or row.get("product_symbol") or "")
        if args.symbol and symbol not in args.symbol:
            continue
        rows.append((num(when), symbol, row))

    if not rows:
        print("No fills in that window.")
        return 0

    rows.sort()
    print(f"{'filled (IST)':<16}{'symbol':<10}{'side':<6}{'price':>13}{'quantity':>14}{'value $':>11}   order id")
    print("-" * 92)
    for when, symbol, row in rows:
        is_buyer = row.get("isBuyer")
        side = str(row.get("side") or ("BUY" if is_buyer else "SELL" if is_buyer is not None else "?")).upper()
        price = num(row.get("price"))
        qty = num(row.get("qty") or row.get("size") or row.get("quantity"))
        quote = num(row.get("quoteQty")) or price * qty
        print(
            f"{ist(when):<16}{symbol:<10}{side:<6}{price:>13,.2f}{qty:>14.8f}"
            f"{quote:>11,.2f}   {row.get('orderId') or row.get('order_id') or '-'}"
        )

    print(f"\n{len(rows)} fill(s).")
    print("Compare a BUY's time here with the engine's 'Bought ...' line for the same price.")
    print("A gap means the engine was slow to ingest the fill, not that the exchange was slow to make it.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
