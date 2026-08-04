"""CoinDCX, the power hooks, and what the buyer reads.

The CoinDCX tests are mostly about the three ways it differs from Binance in
kind rather than in style — the signature covering exact body bytes, base and
target being the reverse of everywhere else, and cancel taking the exchange's
own id.

The power tests are about honesty. Windows gives about two seconds' warning
before suspending, which is not enough to cancel an order and confirm it, so a
Windows buyer is running with less protection than a Mac buyer. That has to
surface in what they read, not sit in a comment.
"""

import json
import unittest

from executor.coindcx import CoinDCXSpotAdapter
from executor.exchange import DuplicateOrder, ExchangeError, InsufficientBalance, IntentExecutor
from executor.orders import OrderIntent
from executor.power import (
    MACOS,
    UNKNOWN,
    WINDOWS,
    PlatformPower,
    SleepInhibitor,
    detect,
    suspend_advice,
    sync_inhibitor,
)
from executor.report import (
    attention_label,
    campaign_start_notice,
    irreducible_risk,
    needs_acknowledgement,
    running_status,
)

MARKETS = [
    {
        "coindcx_name": "SOLUSDT",
        "target_currency_short_name": "SOL",
        "base_currency_short_name": "USDT",
        "base_currency_precision": 2,
        "step": "0.001",
        "min_notional": "5.0",
    }
]


class FakeHTTP:
    def __init__(self, routes=None):
        self.routes = dict(routes or {})
        self.calls = []

    def __call__(self, method, url, body, headers):
        path = url.split("coindcx.com", 1)[-1] if "coindcx.com" in url else url
        self.calls.append({"method": method, "path": path, "body": body, "headers": dict(headers)})
        handler = self.routes.get(path)
        if handler is None:
            return 404, {"message": "not found"}
        return handler(body) if callable(handler) else handler


def _adapter(routes=None):
    routes = dict(routes or {})
    routes.setdefault("/exchange/v1/markets_details", (200, MARKETS))
    http = FakeHTTP(routes)
    return CoinDCXSpotAdapter(api_key="KEY", api_secret="SECRET", http=http, now_ms=lambda: 1785770000000), http


class CoinDCXSigningTests(unittest.TestCase):
    def test_the_signature_covers_the_exact_bytes_that_are_sent(self):
        """Re-serializing between signing and sending is a silent auth failure."""
        import hashlib
        import hmac

        adapter, http = _adapter({"/exchange/v1/users/balances": (200, [])})
        adapter.free_balance("USDT")
        call = http.calls[-1]
        self.assertIsInstance(call["body"], str)
        expected = hmac.new(b"SECRET", call["body"].encode(), hashlib.sha256).hexdigest()
        self.assertEqual(call["headers"]["X-AUTH-SIGNATURE"], expected)

    def test_private_reads_are_posts(self):
        adapter, http = _adapter({"/exchange/v1/users/balances": (200, [])})
        adapter.free_balance("USDT")
        self.assertEqual(http.calls[-1]["method"], "POST")

    def test_the_secret_is_never_sent(self):
        adapter, http = _adapter({"/exchange/v1/users/balances": (200, [])})
        adapter.free_balance("USDT")
        self.assertNotIn("SECRET", str(http.calls))


class CoinDCXRulesTests(unittest.TestCase):
    def test_target_is_the_coin_and_base_is_the_settlement_currency(self):
        """The reverse of everywhere else. Reading it the usual way round hands
        back USDT as the asset to sell, which looks like an empty balance."""
        adapter, _ = _adapter()
        rules = adapter.symbol_rules("SOLUSDT")
        self.assertEqual(rules.base_asset, "SOL")
        self.assertNotEqual(rules.base_asset, "USDT")

    def test_the_tick_comes_from_the_quote_precision(self):
        adapter, _ = _adapter()
        self.assertAlmostEqual(adapter.symbol_rules("SOLUSDT").tick_size, 0.01, places=9)

    def test_an_unlisted_market_says_so(self):
        adapter, _ = _adapter()
        with self.assertRaises(ExchangeError):
            adapter.symbol_rules("NOPEUSDT")


class CoinDCXOrderTests(unittest.TestCase):
    def test_a_stop_limit_carries_both_prices(self):
        adapter, http = _adapter(
            {"/exchange/v1/orders/create": (200, {"orders": [{"id": "1", "client_order_id": "c", "status": "open"}]})}
        )
        adapter.place(
            symbol="SOLUSDT",
            side="buy",
            order_type="stop_limit",
            quantity=0.067,
            price=162.02,
            stop_price=162.00,
            client_order_id="c",
        )
        body = json.loads(http.calls[-1]["body"])
        self.assertEqual(body["order_type"], "stop_limit")
        self.assertAlmostEqual(body["price_per_unit"], 162.02)
        self.assertAlmostEqual(body["stop_price"], 162.0)
        self.assertAlmostEqual(body["total_quantity"], 0.067)

    def test_cancel_looks_the_order_up_then_cancels_by_exchange_id(self):
        """CoinDCX's cancel does not take a client id."""
        adapter, http = _adapter(
            {
                "/exchange/v1/orders/status": (200, {"id": "999", "client_order_id": "c", "status": "open"}),
                "/exchange/v1/orders/cancel": (200, {}),
            }
        )
        adapter.cancel(symbol="SOLUSDT", client_order_id="c")
        self.assertEqual(json.loads(http.calls[-1]["body"])["id"], "999")

    def test_cancelling_something_already_gone_raises_for_the_caller_to_absorb(self):
        adapter, _ = _adapter({"/exchange/v1/orders/status": (400, {"message": "Order not found"})})
        with self.assertRaises(ExchangeError):
            adapter.cancel(symbol="SOLUSDT", client_order_id="c")

    def test_a_missing_order_reads_as_none(self):
        adapter, _ = _adapter({"/exchange/v1/orders/status": (400, {"message": "Order not found"})})
        self.assertIsNone(adapter.get_order(symbol="SOLUSDT", client_order_id="c"))

    def test_cancelled_maps_to_our_own_spelling(self):
        adapter, _ = _adapter(
            {"/exchange/v1/orders/status": (200, {"id": "1", "client_order_id": "c", "status": "cancelled"})}
        )
        record = adapter.get_order(symbol="SOLUSDT", client_order_id="c")
        self.assertEqual(record.status, "CANCELLED")
        self.assertFalse(record.is_open)

    def test_a_partial_fill_is_derived_from_the_remaining_quantity(self):
        adapter, _ = _adapter(
            {
                "/exchange/v1/orders/status": (
                    200,
                    {
                        "id": "1",
                        "client_order_id": "c",
                        "status": "partially_filled",
                        "total_quantity": 0.10,
                        "remaining_quantity": 0.04,
                        "avg_price": 162.0,
                    },
                )
            }
        )
        record = adapter.get_order(symbol="SOLUSDT", client_order_id="c")
        self.assertAlmostEqual(record.filled_qty, 0.06, places=9)
        self.assertTrue(record.is_open)

    def test_errors_are_told_apart(self):
        for message, expected in (
            ("Duplicate client order id", DuplicateOrder),
            ("Insufficient funds", InsufficientBalance),
        ):
            adapter, _ = _adapter({"/exchange/v1/orders/create": (400, {"message": message})})
            with self.assertRaises(expected):
                adapter.place(
                    symbol="SOLUSDT",
                    side="buy",
                    order_type="limit",
                    quantity=0.05,
                    price=162.0,
                    stop_price=None,
                    client_order_id="c",
                )

    def test_it_drives_through_the_intent_executor(self):
        adapter, http = _adapter(
            {
                "/exchange/v1/users/balances": (200, [{"currency": "USDT", "balance": "500"}]),
                "/exchange/v1/orders/status": (400, {"message": "Order not found"}),
                "/exchange/v1/orders/create": (
                    200,
                    {"orders": [{"id": "7", "client_order_id": "cfx-c1-e1", "status": "open"}]},
                ),
            }
        )
        result = IntentExecutor(adapter, "SOLUSDT").apply(
            [
                OrderIntent(
                    action="place",
                    kind="entry",
                    client_order_id="cfx-c1-e1",
                    side="buy",
                    order_type="stop_limit",
                    price=162.02,
                    stop_price=162.00,
                    usd_notional=11.0,
                )
            ]
        )
        self.assertEqual(len(result.placed), 1)
        self.assertAlmostEqual(json.loads(http.calls[-1]["body"])["total_quantity"], 0.067)


class PowerTests(unittest.TestCase):
    def test_each_platform_is_described_honestly(self):
        self.assertTrue(MACOS.can_cancel_on_suspend)
        self.assertFalse(WINDOWS.can_cancel_on_suspend)
        self.assertFalse(UNKNOWN.can_prevent_idle_sleep)

    def test_detection_maps_the_three_it_knows(self):
        self.assertIs(detect("Darwin"), MACOS)
        self.assertIs(detect("Windows"), WINDOWS)
        self.assertIs(detect("Plan9"), UNKNOWN)

    def test_windows_is_told_it_cannot_cancel_in_time(self):
        """Less protection than a Mac buyer, and they deserve to know."""
        advice = suspend_advice(WINDOWS, armed_exposure_usd=7.25)
        self.assertIn("2 seconds", advice)
        self.assertIn("$7.25", advice)

    def test_a_mac_with_exposure_needs_no_warning(self):
        self.assertIsNone(suspend_advice(MACOS, armed_exposure_usd=7.25))

    def test_windows_with_nothing_at_risk_is_not_nagged(self):
        self.assertIsNone(suspend_advice(WINDOWS, armed_exposure_usd=0.0))

    def test_the_machine_is_held_awake_only_while_something_can_fill(self):
        """Tied to exposure, not to the app being open — a laptop that never
        sleeps because a trading app is running gets the app turned off."""
        spawned = []

        class FakeProcess:
            def terminate(self_inner):
                spawned.append("terminated")

        inhibitor = SleepInhibitor(power=MACOS, runner=lambda command: (spawned.append(command), FakeProcess())[1])
        self.assertTrue(sync_inhibitor(inhibitor, armed_exposure_usd=7.25))
        self.assertTrue(inhibitor.held)
        sync_inhibitor(inhibitor, armed_exposure_usd=0.0)
        self.assertFalse(inhibitor.held)
        self.assertIn("terminated", spawned)

    def test_a_platform_that_cannot_inhibit_reports_it_rather_than_pretending(self):
        inhibitor = SleepInhibitor(power=UNKNOWN, runner=lambda command: None)
        self.assertFalse(inhibitor.acquire())
        self.assertFalse(inhibitor.held)

    def test_a_failed_inhibitor_degrades_rather_than_stopping_trading(self):
        def explode(command):
            raise OSError("caffeinate is not installed")

        inhibitor = SleepInhibitor(power=MACOS, runner=explode)
        self.assertFalse(inhibitor.acquire())
        self.assertFalse(inhibitor.held)


class ReportTests(unittest.TestCase):
    def test_a_five_minute_campaign_is_labelled_high_attention(self):
        label, _ = attention_label("5m")
        self.assertEqual(label, "high attention")
        self.assertTrue(needs_acknowledgement("5m"))

    def test_an_hourly_campaign_is_hands_off_and_needs_no_click(self):
        self.assertEqual(attention_label("1h")[0], "hands off")
        self.assertFalse(needs_acknowledgement("1h"))

    def test_a_coarsened_ladder_says_so_at_the_moment_it_starts(self):
        lines = campaign_start_notice("15m", fidelity="coarse")
        self.assertTrue(any("fewer, deeper entries" in line for line in lines))

    def test_the_exposure_leads_the_status(self):
        lines = running_status({"armed_exposure_usd": 7.25, "opening_new": True})
        self.assertIn("$7.25", lines[0])

    def test_no_resting_buys_says_nothing_can_fill(self):
        lines = running_status({"armed_exposure_usd": 0.0, "opening_new": True})
        self.assertIn("Nothing can fill", lines[0])

    def test_an_unprotected_position_is_called_out(self):
        lines = running_status({"armed_exposure_usd": 0.0, "opening_new": True, "unprotected": ["c1"]})
        self.assertTrue(any("no sell order" in line for line in lines))

    def test_a_halt_says_existing_positions_are_still_managed(self):
        lines = running_status({"armed_exposure_usd": 0.0, "opening_new": True, "halted": ["c1"]})
        self.assertTrue(any("still managed" in line for line in lines))

    def test_a_blocked_posture_explains_itself(self):
        lines = running_status({"armed_exposure_usd": 0.0, "opening_new": False, "posture_reason": "Feed is stale."})
        self.assertIn("Feed is stale.", lines)

    def test_the_windows_asymmetry_reaches_the_status_lines(self):
        lines = running_status({"armed_exposure_usd": 7.25, "opening_new": True}, power=WINDOWS)
        self.assertTrue(any("2 seconds" in line for line in lines))

    def test_the_disclosure_admits_what_is_irreducible(self):
        text = irreducible_risk()
        self.assertIn("may still", text)
        self.assertIn("cannot be liquidated", text)


class PlatformShapeTests(unittest.TestCase):
    def test_a_custom_platform_can_be_described_without_code_changes(self):
        custom = PlatformPower("BSD", True, 10.0, True, "fine")
        self.assertIsNone(suspend_advice(custom, armed_exposure_usd=5.0))


if __name__ == "__main__":
    unittest.main()
