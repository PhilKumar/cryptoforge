"""The Binance adapter.

This is the layer that touches a buyer's real account, so the tests are mostly
about the ways Binance fails QUIETLY. Every case below produces something that
looks like "the order just did not go through" rather than an obvious error,
which is exactly why each one is pinned:

- CANCELED has one L. An unmapped status reads as still-open forever.
- str(0.00001) is '1e-05', which Binance rejects as malformed.
- "Duplicate order sent" and "insufficient balance" are the SAME error code.
- A missing order must read as None, or the idempotent adopt-or-place decision
  upstream turns into a refusal to trade.
"""

import hashlib
import hmac
import unittest

from executor.binance import BinanceSpotAdapter, format_decimal
from executor.exchange import (
    BelowMinNotional,
    DuplicateOrder,
    ExchangeError,
    InsufficientBalance,
    IntentExecutor,
)
from executor.orders import OrderIntent

EXCHANGE_INFO = {
    "symbols": [
        {
            "symbol": "SOLUSDT",
            "baseAsset": "SOL",
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.01000000"},
                {"filterType": "LOT_SIZE", "stepSize": "0.00100000"},
                {"filterType": "NOTIONAL", "minNotional": "5.00000000"},
            ],
        }
    ]
}


class FakeHTTP:
    """Scripted Binance. Routes are (method, path) -> (status, body) or callable."""

    def __init__(self, routes=None):
        self.routes = dict(routes or {})
        self.calls = []

    def __call__(self, method, url, params, headers):
        path = url.split("binance.com", 1)[-1] if "binance.com" in url else url
        self.calls.append({"method": method, "path": path, "params": dict(params), "headers": dict(headers)})
        handler = self.routes.get((method, path))
        if handler is None:
            return 404, {"code": -1121, "msg": "Invalid symbol."}
        return handler(params) if callable(handler) else handler


def _adapter(routes=None, **kwargs):
    routes = dict(routes or {})
    routes.setdefault(("GET", "/api/v3/exchangeInfo"), (200, EXCHANGE_INFO))
    http = FakeHTTP(routes)
    adapter = BinanceSpotAdapter(api_key="KEY", api_secret="SECRET", http=http, now_ms=lambda: 1785770000000, **kwargs)
    return adapter, http


class SigningTests(unittest.TestCase):
    def test_private_calls_are_signed_and_carry_the_key_header(self):
        adapter, http = _adapter({("GET", "/api/v3/account"): (200, {"balances": []})})
        adapter.free_balance("USDT")
        call = http.calls[-1]
        self.assertEqual(call["headers"]["X-MBX-APIKEY"], "KEY")
        self.assertIn("signature", call["params"])

    def test_the_signature_is_over_the_query_without_itself(self):
        adapter, http = _adapter({("GET", "/api/v3/account"): (200, {"balances": []})})
        adapter.free_balance("USDT")
        params = http.calls[-1]["params"]
        signature = params.pop("signature")
        from urllib.parse import urlencode

        expected = hmac.new(b"SECRET", urlencode(params).encode(), hashlib.sha256).hexdigest()
        self.assertEqual(signature, expected)

    def test_public_calls_are_not_signed(self):
        adapter, http = _adapter()
        adapter.symbol_rules("SOLUSDT")
        self.assertNotIn("signature", http.calls[-1]["params"])
        self.assertEqual(http.calls[-1]["headers"], {})

    def test_the_secret_never_appears_in_a_request(self):
        """It signs; it is never sent."""
        adapter, http = _adapter({("GET", "/api/v3/account"): (200, {"balances": []})})
        adapter.free_balance("USDT")
        self.assertNotIn("SECRET", str(http.calls))


class NumberFormattingTests(unittest.TestCase):
    """str() is not a serializer for an exchange."""

    def test_small_quantities_do_not_go_out_in_scientific_notation(self):
        self.assertEqual(format_decimal(0.00001, 0.00001), "0.00001")
        self.assertNotIn("e", format_decimal(0.00001, 0.00001))

    def test_precision_is_cut_to_the_filters_own_step(self):
        self.assertEqual(format_decimal(0.0679123, 0.001), "0.067")

    def test_it_floors_rather_than_rounds(self):
        """Rounding up invents coin the buyer does not have."""
        self.assertEqual(format_decimal(0.0699, 0.001), "0.069")

    def test_prices_follow_the_tick(self):
        self.assertEqual(format_decimal(162.0289, 0.01), "162.02")


class RulesTests(unittest.TestCase):
    def test_filters_come_from_the_exchange_not_the_feed(self):
        adapter, _ = _adapter()
        rules = adapter.symbol_rules("SOLUSDT")
        self.assertEqual(rules.tick_size, 0.01)
        self.assertEqual(rules.step_size, 0.001)
        self.assertEqual(rules.min_notional_usd, 5.0)
        self.assertEqual(rules.base_asset, "SOL")

    def test_rules_are_fetched_once_per_symbol(self):
        adapter, http = _adapter()
        adapter.symbol_rules("SOLUSDT")
        adapter.symbol_rules("SOLUSDT")
        self.assertEqual(len([c for c in http.calls if c["path"] == "/api/v3/exchangeInfo"]), 1)

    def test_an_unlisted_symbol_says_so(self):
        adapter, _ = _adapter({("GET", "/api/v3/exchangeInfo"): (200, {"symbols": []})})
        with self.assertRaises(ExchangeError):
            adapter.symbol_rules("NOPEUSDT")


class PlacementTests(unittest.TestCase):
    def test_a_stop_limit_carries_both_prices_and_a_time_in_force(self):
        adapter, http = _adapter(
            {("POST", "/api/v3/order"): (200, {"clientOrderId": "cfx-c1-e1", "orderId": 1, "status": "NEW"})}
        )
        adapter.place(
            symbol="SOLUSDT",
            side="buy",
            order_type="stop_limit",
            quantity=0.067,
            price=162.02,
            stop_price=162.00,
            client_order_id="cfx-c1-e1",
        )
        params = http.calls[-1]["params"]
        self.assertEqual(params["type"], "STOP_LOSS_LIMIT")
        self.assertEqual(params["stopPrice"], "162")
        self.assertEqual(params["price"], "162.02")
        self.assertEqual(params["timeInForce"], "GTC")
        self.assertEqual(params["newClientOrderId"], "cfx-c1-e1")

    def test_a_market_order_carries_no_price_or_time_in_force(self):
        adapter, http = _adapter(
            {("POST", "/api/v3/order"): (200, {"clientOrderId": "x", "orderId": 2, "status": "FILLED"})}
        )
        adapter.place(
            symbol="SOLUSDT",
            side="sell",
            order_type="market",
            quantity=0.05,
            price=None,
            stop_price=None,
            client_order_id="x",
        )
        params = http.calls[-1]["params"]
        self.assertEqual(params["type"], "MARKET")
        self.assertNotIn("price", params)
        self.assertNotIn("timeInForce", params)

    def test_a_limit_without_a_price_is_refused_before_it_is_sent(self):
        adapter, http = _adapter()
        with self.assertRaises(ExchangeError):
            adapter.place(
                symbol="SOLUSDT",
                side="sell",
                order_type="limit",
                quantity=0.05,
                price=None,
                stop_price=None,
                client_order_id="x",
            )
        self.assertEqual([c for c in http.calls if c["path"] == "/api/v3/order"], [])


class StatusMappingTests(unittest.TestCase):
    """The one-L spelling, and the states that mean 'gone'."""

    def _order(self, status):
        adapter, _ = _adapter(
            {
                ("GET", "/api/v3/order"): (
                    200,
                    {"clientOrderId": "c", "orderId": 1, "status": status, "origQty": "1", "executedQty": "0"},
                )
            }
        )
        return adapter.get_order(symbol="SOLUSDT", client_order_id="c")

    def test_canceled_with_one_l_is_understood(self):
        record = self._order("CANCELED")
        self.assertEqual(record.status, "CANCELLED")
        self.assertFalse(record.is_open)

    def test_an_expired_stop_limit_is_not_still_open(self):
        """It triggered into a price it could not fill. Waiting on it is forever."""
        self.assertFalse(self._order("EXPIRED").is_open)

    def test_a_partial_fill_is_still_open(self):
        self.assertTrue(self._order("PARTIALLY_FILLED").is_open)

    def test_the_average_fill_price_comes_from_the_quote_total(self):
        adapter, _ = _adapter(
            {
                ("GET", "/api/v3/order"): (
                    200,
                    {
                        "clientOrderId": "c",
                        "orderId": 1,
                        "status": "FILLED",
                        "origQty": "0.050",
                        "executedQty": "0.050",
                        "cummulativeQuoteQty": "8.100",
                    },
                )
            }
        )
        record = adapter.get_order(symbol="SOLUSDT", client_order_id="c")
        self.assertAlmostEqual(record.avg_fill_price, 162.0, places=6)


class ErrorTranslationTests(unittest.TestCase):
    """-2010 is two very different things."""

    def _failing(self, body):
        return _adapter({("POST", "/api/v3/order"): (400, body)})[0]

    def test_a_duplicate_is_not_an_insufficient_balance(self):
        adapter = self._failing({"code": -2010, "msg": "Duplicate order sent."})
        with self.assertRaises(DuplicateOrder):
            adapter.place(
                symbol="SOLUSDT",
                side="buy",
                order_type="limit",
                quantity=0.05,
                price=162.0,
                stop_price=None,
                client_order_id="c",
            )

    def test_an_insufficient_balance_is_named_as_one(self):
        adapter = self._failing({"code": -2010, "msg": "Account has insufficient balance for requested action."})
        with self.assertRaises(InsufficientBalance):
            adapter.place(
                symbol="SOLUSDT",
                side="buy",
                order_type="limit",
                quantity=0.05,
                price=162.0,
                stop_price=None,
                client_order_id="c",
            )

    def test_a_filter_failure_is_a_min_notional(self):
        adapter = self._failing({"code": -1013, "msg": "Filter failure: NOTIONAL"})
        with self.assertRaises(BelowMinNotional):
            adapter.place(
                symbol="SOLUSDT",
                side="buy",
                order_type="limit",
                quantity=0.001,
                price=1.0,
                stop_price=None,
                client_order_id="c",
            )

    def test_an_unknown_error_still_carries_its_code(self):
        adapter = self._failing({"code": -1021, "msg": "Timestamp for this request is outside of the recvWindow."})
        with self.assertRaises(ExchangeError) as caught:
            adapter.place(
                symbol="SOLUSDT",
                side="buy",
                order_type="limit",
                quantity=0.05,
                price=162.0,
                stop_price=None,
                client_order_id="c",
            )
        self.assertIn("-1021", str(caught.exception))


class MissingOrderTests(unittest.TestCase):
    def test_an_order_that_does_not_exist_reads_as_none(self):
        """Raising here would turn 'not placed yet' into a refusal to place."""
        adapter, _ = _adapter({("GET", "/api/v3/order"): (400, {"code": -2013, "msg": "Order does not exist."})})
        self.assertIsNone(adapter.get_order(symbol="SOLUSDT", client_order_id="c"))

    def test_a_real_failure_still_raises(self):
        adapter, _ = _adapter({("GET", "/api/v3/order"): (400, {"code": -1021, "msg": "Timestamp out of window."})})
        with self.assertRaises(ExchangeError):
            adapter.get_order(symbol="SOLUSDT", client_order_id="c")


class ThroughIntentExecutorTests(unittest.TestCase):
    """The adapter under the layer that actually drives it."""

    def test_an_entry_goes_out_end_to_end(self):
        adapter, http = _adapter(
            {
                ("GET", "/api/v3/account"): (200, {"balances": [{"asset": "USDT", "free": "500.0"}]}),
                ("GET", "/api/v3/order"): (400, {"code": -2013, "msg": "Order does not exist."}),
                ("POST", "/api/v3/order"): (200, {"clientOrderId": "cfx-c1-e1", "orderId": 7, "status": "NEW"}),
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
        sent = [c for c in http.calls if c["path"] == "/api/v3/order" and c["method"] == "POST"][0]["params"]
        # $11 at the limit price of 162.02, floored to the 0.001 step.
        self.assertEqual(sent["quantity"], "0.067")

    def test_an_order_that_already_landed_is_adopted_not_doubled(self):
        adapter, http = _adapter(
            {
                ("GET", "/api/v3/order"): (
                    200,
                    {"clientOrderId": "cfx-c1-e1", "orderId": 7, "status": "NEW", "origQty": "0.067"},
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
        self.assertEqual(len(result.adopted), 1)
        self.assertEqual([c for c in http.calls if c["method"] == "POST"], [])


if __name__ == "__main__":
    unittest.main()
