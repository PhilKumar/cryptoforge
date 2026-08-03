#!/usr/bin/env python3
"""
tools/coindcx_spot_check.py — READ-ONLY proof that the CoinDCX spot private
API path actually works.

The spot adapter's public half (products, candles, tickers) has been exercised
against the live API. Its private half — signing, balances, order listing — has
never run against a real account. This script closes exactly that gap and
nothing more.

It is read-only BY CONSTRUCTION: it calls only getters. It never places an
order, never cancels one, never changes a setting, and never switches the
active broker. Running it cannot move money.

Keys are read from the environment, or from the repo's gitignored .env that
config.py already loads. They are NEVER printed — not in full, not partially,
not in an error message.

Usage
-----
    python3 tools/coindcx_spot_check.py

    # or, without touching .env at all:
    COINDCX_API_KEY=... COINDCX_API_SECRET=... python3 tools/coindcx_spot_check.py
"""

from __future__ import annotations

import os
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker.coindcx_spot import CoinDCXSpotClient  # noqa: E402

# Methods this script is permitted to touch. Anything that could move money is
# absent on purpose; the guard below fails loudly if that ever stops being true.
_READ_ONLY_CALLS = {
    "get_products",
    "get_product_by_symbol",
    "get_ticker",
    "get_wallet",
    "get_positions",
    "get_orders",
    "get_order",
    "get_order_history",
}
_FORBIDDEN_CALLS = {"place_order", "place_order_verified", "cancel_order", "exit_position", "set_leverage"}

_results: list[tuple[str, bool, str]] = []


def step(label: str, fn, *, detail=lambda value: "", validate=None) -> object | None:
    """Run one read-only probe, record PASS/FAIL, never raise.

    `validate` exists because the adapter deliberately swallows API errors and
    returns `{"error": ...}` or `[]` so the running app degrades instead of
    crashing. That is right for production and wrong for a verifier: without a
    validator this script reported a clean pass against invalid credentials.
    Anything that reads the account must therefore either probe the raw signed
    endpoint (so a 401 raises) or validate what came back.
    """
    try:
        value = fn()
    except Exception as exc:  # noqa: BLE001 - the whole point is to report, not crash
        message = str(exc)
        _results.append((label, False, message[:200]))
        print(f"  [FAIL] {label}\n         {message[:200]}")
        return None
    problem = validate(value) if validate else None
    if problem:
        _results.append((label, False, str(problem)[:200]))
        print(f"  [FAIL] {label}\n         {str(problem)[:200]}")
        return None
    text = detail(value)
    _results.append((label, True, text))
    print(f"  [ ok ] {label}{(' — ' + text) if text else ''}")
    return value


def skip(label: str) -> None:
    """Downstream account reads are meaningless once auth has failed, and the
    adapter would report them as empty successes. Say 'skipped', not 'ok'."""
    print(f"  [skip] {label} — auth failed, nothing to read")


def _error_in(value) -> str | None:
    """The adapter's soft-failure shape: a dict carrying an 'error' key."""
    if isinstance(value, dict) and value.get("error"):
        return str(value["error"])
    return None


def main() -> int:
    print("CoinDCX spot — read-only private path check")
    print("=" * 58)

    if _FORBIDDEN_CALLS & _READ_ONLY_CALLS:
        print("Refusing to run: the allow-list contains a mutating call.")
        return 2

    client = CoinDCXSpotClient()

    if not client._is_configured():
        print(
            "\nNo CoinDCX API keys visible to this process.\n\n"
            "Provide them either way — the script reads them from the environment\n"
            "and never prints or stores them:\n\n"
            "  1) add COINDCX_API_KEY / COINDCX_API_SECRET to the gitignored .env, or\n"
            "  2) export them for a single run:\n"
            "     COINDCX_API_KEY=... COINDCX_API_SECRET=... python3 tools/coindcx_spot_check.py\n\n"
            "Read-only ('view') API permissions are enough for every check here.\n"
        )
        return 1

    print(f"keys: present (key ends ...{str(client.api_key)[-4:]})")
    print(f"host: {client.base_url}   quote asset: {client.quote_asset}\n")

    print("PUBLIC (already known good — re-checked so a failure below is unambiguous)")
    products = step(
        "markets load",
        client.get_products,
        detail=lambda p: f"{len(p)} {client.quote_asset} spot markets",
    )
    step(
        "ticker reads",
        lambda: client.get_ticker("BTCUSDT"),
        detail=lambda t: f"BTCUSDT last {t.get('last_price')}",
    )

    print("\nPRIVATE (the untested half — signing and account reads)")
    # Probe the raw signed endpoint first: _private_post raises on a 4xx, so a
    # bad key, a clock skew or a broken signature fails here loudly instead of
    # being smoothed into an empty list further up.
    authed = (
        step(
            "credentials accepted (raw signed request)",
            lambda: client._private_post("/exchange/v1/users/balances"),
            detail=lambda r: f"{len(r)} currency rows" if isinstance(r, list) else f"returned {type(r).__name__}",
            validate=lambda r: None if isinstance(r, list) else f"expected a list of balances, got {type(r).__name__}",
        )
        is not None
    )

    wallet = step(
        "wallet parses into free/locked rows",
        client.get_wallet,
        detail=lambda w: f"{len(w)} funded assets" if isinstance(w, list) else f"returned {type(w).__name__}",
        validate=lambda w: _error_in(w) or (None if isinstance(w, list) else f"expected rows, got {type(w).__name__}"),
    )

    held: list[str] = []
    if isinstance(wallet, list):
        print("\n         balances as the engine will read them:")
        for row in wallet[:12]:
            asset = row.get("asset_symbol")
            free = row.get("free_balance")
            locked = row.get("locked_balance")
            print(f"           {str(asset):<8} free {str(free):>18}   locked {str(locked):>18}")
            # {asset}{quote} is a guess, not a market. A fiat balance like INR
            # yields "INRUSDT", which is not listed anywhere — probing it
            # reports a confident "0 open orders" about a market that does not
            # exist, and hides real orders sitting elsewhere. Keep only the
            # guesses the exchange actually lists.
            if asset and asset != client.quote_asset and client.get_product_by_symbol(f"{asset}{client.quote_asset}"):
                held.append(f"{asset}{client.quote_asset}")
        if len(wallet) > 12:
            print(f"           … {len(wallet) - 12} more")
        # Cascade reads free/locked separately; a wallet that parses but always
        # reports zero locked is the failure mode that breaks TP sizing.
        if not any(row.get("locked_balance") not in (None, "", "0") for row in wallet):
            print("         note: every locked balance is zero — expected only if you have no resting orders.")

    if authed:
        step(
            "positions derive from balances",
            client.get_positions,
            detail=lambda p: f"{len(p)} priced positions",
        )
    else:
        skip("positions derive from balances")

    # History first: it names the markets actually traded, which is a far
    # better place to look for resting orders than a guess built from balances.
    step(
        "trade history endpoint",
        lambda: client._private_post("/exchange/v1/orders/trade_history", {"limit": 50, "sort": "desc"}),
        detail=lambda r: f"{len(r) if isinstance(r, list) else '?'} rows",
    )
    history = []
    if authed:
        history = (
            step(
                "trade history parse through the adapter",
                client.get_order_history,
                detail=lambda t: f"{len(t)} recent fills",
                validate=lambda t: None if isinstance(t, list) else f"expected a list, got {type(t).__name__}",
            )
            or []
        )
    else:
        skip("trade history parse through the adapter")

    traded = []
    for row in history:
        market = str(row.get("symbol") or row.get("market") or "").upper()
        if market and market not in traded and client.get_product_by_symbol(market):
            traded.append(market)
    probes = client.unique([*held, *traded, "BTCUSDT"])[:6]

    if authed:
        found = []

        def scan_open_orders() -> list:
            rows = []
            for market in probes:
                raw = client._private_post("/exchange/v1/orders/active_orders", {"market": market})
                orders = raw.get("orders") if isinstance(raw, dict) else raw
                for order in orders or []:
                    rows.append((market, order))
            found.extend(rows)
            return rows

        step(
            f"open orders across {len(probes)} market(s)",
            scan_open_orders,
            detail=lambda r: f"{len(r)} resting order(s) on {', '.join(probes)}",
        )
        for market, order in found:
            print(
                f"           {market}: {order.get('side')} {order.get('order_type')} "
                f"qty {order.get('total_quantity')} @ {order.get('price_per_unit')} ({order.get('status')})"
            )
        step(
            "open orders parse through the adapter",
            lambda: client.get_orders(probes[0], "open"),
            detail=lambda o: f"{len(o)} parsed on {probes[0]}",
            validate=lambda o: None if isinstance(o, list) else f"expected a list, got {type(o).__name__}",
        )
    else:
        skip("open orders scan")
        skip("open orders parse through the adapter")

    if products:
        # Must FAIL when the market is unlisted: an empty dict rendering as
        # "step None minNotional None" is not a passing lookup.
        step(
            f"product lookup ({probes[0]})",
            lambda: client.get_product_by_symbol(probes[0]),
            detail=lambda p: f"step {p.get('step_size')} minNotional {p.get('min_notional')}",
            validate=lambda p: None if p else f"{probes[0]} is not a listed market",
        )

    passed = sum(1 for _, ok, _ in _results if ok)
    failed = len(_results) - passed
    print("\n" + "=" * 58)
    print(f"{passed} passed, {failed} failed   (no order was placed or cancelled)")
    if failed:
        print("\nfailed steps:")
        for label, ok, message in _results:
            if not ok:
                print(f"  - {label}: {message}")
        print("\nThe adapter is not ready for live trading until these read.")
        return 1
    print("\nThe private path works. Live CoinDCX trading is still a separate,")
    print("deliberate decision — switching the broker points the WHOLE app at")
    print("CoinDCX and needs a cascade reset; do not flip prod casually.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\ninterrupted")
        raise SystemExit(130) from None
    except Exception:  # noqa: BLE001
        traceback.print_exc()
        raise SystemExit(2) from None
