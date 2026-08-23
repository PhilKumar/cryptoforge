import json
import os
import re
import tempfile
import unittest
from importlib import import_module
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from starlette.requests import Request as StarletteRequest

from broker.binance import BinanceSpotClient
from broker.coindcx_spot import CoinDCXSpotClient
from broker.delta import DeltaClient
from engine.scalp import ScalpEngine, ScalpTrade

ROOT = Path(__file__).resolve().parents[1]


class FakeSpotBroker:
    broker_name = "binance"
    display_name = "Binance Spot"
    fee_pct_per_side = 0.1

    def __init__(self):
        self.verified_calls = []
        self.position = {"base_size": 9.0, "mark_price": 101.0}

    def scalp_capabilities(self):
        return {
            "spot_only": True,
            "supports_short": False,
            "supports_leverage": False,
            "supports_post_only": False,
            "supports_base_quantity": True,
            "leverage_options": [1],
            "order_types": [
                "market",
                "limit",
                "stop_market",
                "stop_limit",
                "take_profit_market",
                "take_profit_limit",
                "trailing_stop",
            ],
        }

    def get_product_by_symbol(self, symbol):
        return {"id": symbol}

    def get_ticker(self, symbol):
        return {"last_price": 100.0, "mark_price": 100.0}

    async def place_order_verified(self, **kwargs):
        self.verified_calls.append(kwargs)
        if kwargs.get("side") == "buy":
            return {
                "id": "buy-1",
                "verified": True,
                "fill_price": 100.0,
                "filled_size": 1.0,
                "net_base_filled": 0.999,
                "quote_size": 100.0,
                "paid_commission": 0.1,
            }
        return {
            "id": "sell-1",
            "verified": True,
            "fill_price": 101.0,
            "filled_size": kwargs.get("base_qty"),
            "quote_size": float(kwargs.get("base_qty") or 0) * 101.0,
            "paid_commission": 0.101,
        }

    def get_position(self, product_id, strict=False):
        return dict(self.position)


class ScalpSpotSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def test_spot_entry_is_cash_only_and_exit_uses_exact_net_base_quantity(self):
        broker = FakeSpotBroker()
        engine = ScalpEngine(broker)
        engine.start = lambda: None

        entered = await engine.enter_trade(
            symbol="BTCUSDT",
            side="LONG",
            leverage=1,
            qty_mode="usdt",
            qty_value=100.0,
            mode="live",
            order_type="market",
        )

        self.assertEqual(entered["status"], "ok")
        trade = engine.open_trades[entered["trade_id"]]
        self.assertEqual(trade.leverage, 1)
        self.assertAlmostEqual(trade.base_qty, 0.999)
        self.assertEqual(trade.broker_name, "binance")
        self.assertAlmostEqual(trade.entry_fee, 0.1)

        trade.current_price = 101.0
        exited = await engine.exit_trade(trade.trade_id, reason="manual")
        self.assertEqual(exited["status"], "ok")
        self.assertEqual(broker.verified_calls[-1]["side"], "sell")
        self.assertAlmostEqual(broker.verified_calls[-1]["base_qty"], 0.999)

    async def test_spot_short_and_leverage_are_rejected_before_broker_call(self):
        broker = FakeSpotBroker()
        engine = ScalpEngine(broker)
        engine.start = lambda: None

        short = await engine.enter_trade(
            symbol="BTCUSDT", side="SHORT", leverage=1, qty_value=100, mode="live", order_type="market"
        )
        leveraged = await engine.enter_trade(
            symbol="BTCUSDT", side="LONG", leverage=10, qty_value=100, mode="live", order_type="market"
        )

        self.assertEqual(short["error_code"], "short_not_supported")
        self.assertEqual(leveraged["error_code"], "leverage_not_supported")
        self.assertEqual(broker.verified_calls, [])

    async def test_maker_only_is_not_exposed_without_resting_order_ownership(self):
        broker = FakeSpotBroker()
        engine = ScalpEngine(broker)
        result = await engine.enter_trade(
            symbol="BTCUSDT",
            side="LONG",
            leverage=1,
            qty_value=100,
            mode="live",
            order_type="maker_only",
            entry_limit_price=99,
        )
        self.assertEqual(result["error_code"], "order_type_not_supported")
        self.assertEqual(broker.verified_calls, [])

    async def test_spot_reconcile_does_not_import_unrelated_wallet_holdings(self):
        broker = FakeSpotBroker()
        engine = ScalpEngine(broker)
        trade = ScalpTrade(
            trade_id=1,
            symbol="BTCUSDT",
            side="LONG",
            product_id="BTCUSDT",
            size=100,
            entry_price=100,
            leverage=1,
            base_qty=1.0,
            broker_name="binance",
            broker_label="Binance Spot",
            market_type="spot",
        )
        engine.open_trades[1] = trade

        result = await engine.reconcile_broker_positions(force=True)

        self.assertEqual(result["checked"], 1)
        self.assertAlmostEqual(engine.open_trades[1].base_qty, 1.0)
        self.assertAlmostEqual(engine.open_trades[1].size, 100.0)
        self.assertAlmostEqual(engine.open_trades[1].entry_price, 100.0)

    async def test_scalp_kill_only_closes_scalp_owned_positions(self):
        broker = FakeSpotBroker()
        engine = ScalpEngine(broker)
        engine.start = lambda: None
        entered = await engine.enter_trade(
            symbol="BTCUSDT", side="LONG", leverage=1, qty_value=100, mode="live", order_type="market"
        )
        pending_id = entered["trade_id"] + 1
        from engine.scalp import PendingScalpEntry

        engine.pending_entries[pending_id] = PendingScalpEntry(
            entry_id=pending_id,
            symbol="BTCUSDT",
            side="LONG",
            size=100,
            leverage=1,
            broker_name="binance",
            market_type="spot",
        )
        engine.open_trades[entered["trade_id"]].current_price = 101.0

        result = await engine.kill()

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["cancelled_pending"], [pending_id])
        self.assertEqual(result["closed_trades"], [entered["trade_id"]])
        self.assertFalse(engine.open_trades)
        self.assertFalse(engine.pending_entries)


class SpotOrderLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_binance_partial_fill_cancels_remainder_before_verification(self):
        client = BinanceSpotClient()
        partial = {"orderId": 42, "status": "PARTIALLY_FILLED", "executedQty": "0.4", "cummulativeQuoteQty": "40"}
        cancelled = {**partial, "status": "CANCELED"}
        with (
            patch.object(client, "place_order", return_value=dict(partial)),
            patch.object(client, "get_order", side_effect=[dict(partial), dict(cancelled)]),
            patch.object(
                client,
                "get_product_by_symbol",
                return_value={"base_asset": "BTC", "quote_asset": "USDT"},
            ),
            patch.object(client, "cancel_order", return_value={"status": "CANCELED"}) as cancel,
            patch("broker.binance.asyncio.sleep", new=AsyncMock()),
        ):
            result = await client.place_order_verified("BTCUSDT", 40, "buy", max_verify_attempts=1)

        self.assertTrue(result["verified"])
        self.assertEqual(result["order_lifecycle"], "partial_cancelled")
        self.assertTrue(result["cancelled_remainder"])
        cancel.assert_called_once_with(42, "BTCUSDT")

    async def test_coindcx_unfilled_order_is_cancelled_not_reported_as_position(self):
        client = CoinDCXSpotClient()
        submitted = {"orderId": "abc", "status": "NEW", "executedQty": "0"}
        cancelled = {**submitted, "status": "CANCELED"}
        with (
            patch.object(client, "place_order", return_value=dict(submitted)),
            patch.object(client, "get_order", side_effect=[dict(submitted), dict(cancelled)]),
            patch.object(client, "cancel_order", return_value={"status": "CANCELED"}) as cancel,
            patch("broker.coindcx_spot.asyncio.sleep", new=AsyncMock()),
        ):
            result = await client.place_order_verified("BTCUSDT", 40, "buy", max_verify_attempts=1)

        self.assertFalse(result["verified"])
        self.assertEqual(result["order_lifecycle"], "cancelled")
        self.assertFalse(result["requires_attention"])
        cancel.assert_called_once_with("abc", "BTCUSDT")

    def test_binance_scalp_uses_spot_endpoints_only(self):
        source = (ROOT / "broker" / "binance.py").read_text(encoding="utf-8")
        self.assertIn('"/api/v3/order"', source)
        self.assertNotIn("/sapi/v1/margin/order", source)

    async def test_delta_unfilled_limit_is_cancelled_before_returning(self):
        client = DeltaClient()
        submitted = {"id": 77, "state": "open", "size": 10, "unfilled_size": 10}
        cancelled = {**submitted, "state": "cancelled"}
        with (
            patch.object(client, "place_order", return_value=dict(submitted)),
            patch.object(client, "get_orders", side_effect=[[dict(submitted)], [dict(cancelled)]]),
            patch.object(client, "cancel_order", return_value={"state": "cancelled"}) as cancel,
            patch("broker.delta.asyncio.sleep", new=AsyncMock()),
        ):
            result = await client.place_order_verified(
                27, 10, "buy", order_type="limit_order", limit_price=100, max_verify_attempts=1
            )

        self.assertFalse(result["verified"])
        self.assertEqual(result["order_lifecycle"], "cancelled")
        self.assertFalse(result["requires_attention"])
        cancel.assert_called_once_with(77, 27)


class ScalpUiWiringTests(unittest.TestCase):
    def test_every_static_scalp_handler_is_defined(self):
        html = (ROOT / "strategy.html").read_text(encoding="utf-8")
        js = (ROOT / "static" / "cryptoforge-app.js").read_text(encoding="utf-8")
        scalp = html.split('<div id="scalp-page"', 1)[1].split("</div><!-- /scalp-page -->", 1)[0]
        handlers = set(re.findall(r'data-cf-(?:click|change)="([A-Za-z_$][\w$]*)\s*\(', scalp))
        self.assertTrue(handlers)
        missing = sorted(
            name for name in handlers if not re.search(rf"(?:async\s+)?function\s+{re.escape(name)}\s*\(", js)
        )
        self.assertEqual(missing, [])

    def test_scalp_kill_is_isolated_from_global_emergency_stop(self):
        html = (ROOT / "strategy.html").read_text(encoding="utf-8")
        js = (ROOT / "static" / "cryptoforge-app.js").read_text(encoding="utf-8")
        scalp = html.split('<div id="scalp-page"', 1)[1].split("</div><!-- /scalp-page -->", 1)[0]
        kill_body = js.split("async function cfKillScalp", 1)[1].split("\n}", 1)[0]
        self.assertIn("cfKillScalp()", scalp)
        self.assertNotIn("emergencyStop()", scalp)
        self.assertNotIn("emergencyStop", kill_body)
        self.assertIn("/api/scalp/kill", kill_body)

    def test_new_scalp_controls_are_viewer_blocked(self):
        js = (ROOT / "static" / "cryptoforge-app.js").read_text(encoding="utf-8")
        for handler in ("cfUpdateScalpBroker", "cfCheckScalpBroker", "cfCancelScalpPending", "cfKillScalp"):
            self.assertRegex(js, rf"['\"]{handler}['\"]")

    def test_execution_mode_is_a_wired_switch_and_status_actions_share_one_row(self):
        html = (ROOT / "strategy.html").read_text(encoding="utf-8")
        js = (ROOT / "static" / "cryptoforge-app.js").read_text(encoding="utf-8")
        css = (ROOT / "static" / "cryptoforge-app.css").read_text(encoding="utf-8")
        scalp = html.split('<div id="scalp-page"', 1)[1].split("</div><!-- /scalp-page -->", 1)[0]

        self.assertIn('id="cf-scalp-mode"', scalp)
        self.assertIn('role="switch"', scalp)
        self.assertIn('data-cf-click="cfToggleScalpExecutionMode()"', scalp)
        self.assertNotIn('<select id="cf-scalp-mode"', scalp)
        self.assertIn("function cfToggleScalpExecutionMode()", js)
        self.assertIn("cfSyncScalpExecutionModeToggle();", js)
        self.assertIn('class="cf-scalp-status-button-row"', scalp)
        self.assertIn("grid-template-columns: repeat(3, minmax(0, 1fr));", css)


class ScalpBrokerRouteIsolationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        if not os.getenv("CRYPTOFORGE_PIN"):
            self.skipTest("CRYPTOFORGE_PIN is required to import the application")
        self.app_module = import_module("app")
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._orig_state_db = self.app_module._STATE_DB_FILE
        self._orig_scalp_engine = self.app_module._scalp_engine
        self._orig_scalp_broker = self.app_module._scalp_broker
        self._orig_global_broker = self.app_module.delta
        self.addAsyncCleanup(self._restore)

        self.app_module._STATE_DB_FILE = os.path.join(self._tmp.name, "state.db")
        self.app_module._scalp_engine = None
        self.app_module._scalp_broker = self.app_module.get_broker_client("delta")
        self.app_module._persist_selected_scalp_broker_name("delta")
        self.app_module._rate_limits.clear()

    async def _restore(self):
        engine = self.app_module._scalp_engine
        if engine is not None and hasattr(engine, "shutdown"):
            await engine.shutdown()
        self.app_module._STATE_DB_FILE = self._orig_state_db
        self.app_module._scalp_engine = self._orig_scalp_engine
        self.app_module._scalp_broker = self._orig_scalp_broker

    @staticmethod
    def _request(payload, path="/api/scalp/broker", method="PUT"):
        body = json.dumps(payload).encode("utf-8")
        delivered = False

        async def _receive():
            nonlocal delivered
            if delivered:
                return {"type": "http.request", "body": b"", "more_body": False}
            delivered = True
            return {"type": "http.request", "body": body, "more_body": False}

        return StarletteRequest(
            {
                "type": "http",
                "http_version": "1.1",
                "method": method,
                "scheme": "http",
                "path": path,
                "raw_path": path.encode(),
                "query_string": b"",
                "headers": [(b"content-type", b"application/json")],
                "client": ("127.0.0.1", 1234),
                "server": ("testserver.local", 80),
            },
            _receive,
        )

    async def test_scalp_broker_switch_does_not_change_global_live_broker(self):
        global_before = self.app_module.delta

        result = await self.app_module.update_scalp_broker(self._request({"broker": "binance"}))

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["current_broker"], "binance")
        self.assertIs(self.app_module.delta, global_before)
        self.assertIs(self.app_module.delta, self._orig_global_broker)

    async def test_invalid_scalp_broker_is_rejected_instead_of_falling_back(self):
        with self.assertRaises(HTTPException) as raised:
            await self.app_module.update_scalp_broker(self._request({"broker": "not-a-broker"}))
        self.assertEqual(raised.exception.status_code, 400)
        self.assertEqual(self.app_module._load_selected_scalp_broker_name(), "delta")

    async def test_scalp_broker_payload_declares_all_three_execution_venues(self):
        payload = await self.app_module.get_scalp_broker_settings()
        brokers = {item["name"] for item in payload["available_brokers"]}
        self.assertEqual(brokers, {"binance", "coindcx", "delta"})
        binance = next(item for item in payload["available_brokers"] if item["name"] == "binance")
        self.assertTrue(binance["capabilities"]["spot_only"])
        self.assertFalse(binance["capabilities"]["supports_short"])
        self.assertEqual(binance["capabilities"]["leverage_options"], [1])


if __name__ == "__main__":
    unittest.main()
