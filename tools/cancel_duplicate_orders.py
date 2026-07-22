#!/usr/bin/env python3
"""
tools/cancel_duplicate_orders.py — find and remove duplicate cascade orders.

A bug fixed in 0615f16 could place several take-profit sells against a single
position: a placement reply carrying no order id was stored as "", read as "no
order resting", and re-placed on the next sync. The fix stops new duplicates.
It cannot remove the ones already sitting on the exchange, and every one of
them can still fill — so a position of 0.189 SOL guarded by five identical
sells will oversell if price reaches the target.

Cascade orders are recognisable: their client id is cf-csc-{campaign}-tp-{rev}
or cf-csc-{campaign}-buy-{rev}. Orders in the same group are duplicates of one
another; the NEWEST is the one the engine is tracking, so the older ones go.

Lists only, unless you pass --apply. Nothing is cancelled by a dry run.

    .venv/bin/python tools/cancel_duplicate_orders.py
    .venv/bin/python tools/cancel_duplicate_orders.py --apply

Reads credentials from .env exactly the way the app does. It never prints a
key, and it only ever touches orders whose client id starts with "cf-csc-".
"""

import argparse
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker import get_broker_client  # noqa: E402

CASCADE_PREFIX = "cf-csc-"
# cf-csc-{campaign}-tp-{rev} / cf-csc-{campaign}-buy-{rev}: everything up to the
# revision identifies the order's PURPOSE, so two ids sharing it are the same
# intent placed twice.
_GROUP = re.compile(r"^(cf-csc-.+-(?:tp|buy))-(\d+)$")


def group_key(client_id: str):
    match = _GROUP.match(client_id or "")
    if not match:
        return None, 0
    return match.group(1), int(match.group(2))


def collect(client, symbols):
    groups = {}
    for symbol in symbols:
        try:
            rows = client.get_orders(symbol, "open") or []
        except Exception as exc:
            print(f"  ! could not list open orders for {symbol}: {exc}")
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            client_id = str(row.get("clientOrderId") or "")
            if not client_id.startswith(CASCADE_PREFIX):
                continue
            key, rev = group_key(client_id)
            if key is None:
                continue
            groups.setdefault((symbol, key), []).append(
                {
                    "order_id": str(row.get("orderId") or ""),
                    "client_id": client_id,
                    "rev": rev,
                    "side": row.get("side"),
                    "qty": row.get("origQty"),
                    "price": row.get("price"),
                    "stop": row.get("stopPrice"),
                }
            )
    return groups


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--symbol", action="append", help="symbol to check; repeatable (default: common pairs)")
    parser.add_argument("--apply", action="store_true", help="actually cancel; without this it only lists")
    args = parser.parse_args()

    symbols = args.symbol or ["SOLUSDT", "BTCUSDT", "ETHUSDT", "PAXGUSDT", "XRPUSDT", "DOGEUSDT"]

    client = get_broker_client()
    checker = getattr(client, "_is_configured", None)
    if callable(checker) and not checker():
        print("Broker API keys are not configured — nothing to do.")
        return 1
    base = getattr(client, "base_url", "?")
    env = "TESTNET" if "testnet" in str(base).lower() else "LIVE"
    print(f"Broker: {getattr(client, 'display_name', '?')}   environment: {env}   ({base})")
    print(f"Checking: {', '.join(symbols)}\n")

    groups = collect(client, symbols)
    if not groups:
        print("No cascade orders are resting. Nothing to clean up.")
        return 0

    duplicates = {k: v for k, v in groups.items() if len(v) > 1}
    for (symbol, key), orders in sorted(groups.items()):
        orders.sort(key=lambda o: o["rev"])
        mark = "DUPLICATED" if len(orders) > 1 else "ok"
        print(f"{symbol}  {key}-*   {len(orders)} resting   [{mark}]")
        for order in orders:
            keep = order is orders[-1]
            note = "KEEP (newest)" if keep and len(orders) > 1 else ("" if len(orders) == 1 else "cancel")
            print(
                f"    id {order['order_id']:<12} {order['client_id']:<34} "
                f"{str(order['side']):<5} qty {order['qty']}  price {order['price']}  {note}"
            )
        print()

    if not duplicates:
        print("No duplicates — every cascade order is unique. Nothing to cancel.")
        return 0

    doomed = [o for orders in duplicates.values() for o in sorted(orders, key=lambda x: x["rev"])[:-1]]
    print(f"{len(doomed)} duplicate order(s) would be cancelled, keeping the newest of each group.")
    if not args.apply:
        print("\nDry run — nothing was cancelled. Re-run with --apply to do it.")
        return 0

    print("\nCancelling...")
    failed = 0
    for (symbol, _key), orders in sorted(duplicates.items()):
        for order in sorted(orders, key=lambda x: x["rev"])[:-1]:
            try:
                client.cancel_order(order["order_id"], symbol)
                print(f"  cancelled {order['order_id']}  ({order['client_id']})")
            except Exception as exc:
                failed += 1
                print(f"  ! FAILED {order['order_id']}: {exc}")
    print(f"\nDone. {len(doomed) - failed} cancelled, {failed} failed.")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
