import asyncio
import json
import os
import tempfile
import time
import unittest
from datetime import date, datetime, timedelta
from importlib import import_module
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
import pandas as pd

from broker.delta import DeltaClient, _CircuitBreaker, _normalize_result_list
from engine.live import LiveEngine
from engine.live import _select_signal_rows as select_live_signal_rows
from engine.paper_trading import PaperTradingEngine
from engine.paper_trading import _select_signal_rows as select_paper_signal_rows
from engine.scalp import ScalpEngine

_DEFAULT_PRODUCT = object()


class FakeLiveBroker:
    def __init__(self, *, product=_DEFAULT_PRODUCT, position=None, entry_fill=101.5, exit_fill=102.25):
        self.product = {"id": 11} if product is _DEFAULT_PRODUCT else product
        self.position = position or {}
        self.entry_fill = entry_fill
        self.exit_fill = exit_fill
        self.verified_calls = []
        self.leverage_calls = []
        self.candles = pd.DataFrame(
            [
                {"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 1.0},
                {"open": 100.0, "high": 102.0, "low": 99.5, "close": 101.0, "volume": 1.2},
            ]
        )

    def get_product_by_symbol(self, symbol):
        return self.product

    def set_leverage(self, product_id, leverage):
        self.leverage_calls.append((product_id, leverage))
        return {"status": "ok"}

    async def async_get_candles(self, symbol, resolution="5m", start=None, end=None):
        return self.candles.copy()

    async def place_order_verified(self, **kwargs):
        self.verified_calls.append(kwargs)
        fill_price = self.exit_fill if kwargs.get("reduce_only") else self.entry_fill
        return {
            "id": f"ord-{len(self.verified_calls)}",
            "verified": True,
            "verified_at_attempt": 1,
            "fill_price": fill_price,
        }

    def get_position(self, product_id, strict=False):
        return self.position


class FakeScalpDelta:
    def __init__(self, ticker_prices=None):
        self.verified_calls = []
        self.ticker_prices = list(ticker_prices or [100.0])
        self.product_calls = 0
        self.ticker_calls = 0

    def get_product_by_symbol(self, symbol):
        self.product_calls += 1
        return {"id": 77}

    def get_ticker(self, symbol):
        self.ticker_calls += 1
        price = self.ticker_prices[0] if self.ticker_prices else 100.0
        if len(self.ticker_prices) > 1:
            price = self.ticker_prices.pop(0)
        return {"mark_price": price}

    def to_delta_symbol(self, symbol):
        if symbol.endswith("USDT"):
            return symbol[:-1]
        return symbol

    def from_delta_symbol(self, symbol):
        if symbol.endswith("USD") and not symbol.endswith("USDT"):
            return symbol + "T"
        return symbol

    async def place_order_verified(self, **kwargs):
        self.verified_calls.append(kwargs)
        fill_price = 100.5 if not kwargs.get("reduce_only") else 101.25
        return {
            "id": f"scalp-{len(self.verified_calls)}",
            "verified": True,
            "verified_at_attempt": 1,
            "fill_price": fill_price,
        }


class RejectingExitScalpDelta(FakeScalpDelta):
    async def place_order_verified(self, **kwargs):
        if kwargs.get("reduce_only"):
            self.verified_calls.append(kwargs)
            return {
                "id": f"scalp-reject-{len(self.verified_calls)}",
                "verified": False,
                "fill_status": "rejected",
                "order_lifecycle": "rejected",
                "exchange_state": "rejected",
                "verification_state": "rejected",
                "verification_summary": "Exchange rejected reduce-only exit",
                "error": "Exchange rejected reduce-only exit",
            }
        return await super().place_order_verified(**kwargs)


class FakeWSFeed:
    def __init__(self):
        self.on_ticker = None
        self.connected = False
        self.subscribed = []

    async def connect(self):
        self.connected = True

    async def subscribe_ticker(self, symbol):
        self.subscribed.append(symbol)

    async def disconnect(self):
        self.connected = False

    def emit(self, symbol, price):
        if self.on_ticker:
            self.on_ticker(symbol, {"mark_price": price})


class CircuitBreakerTests(unittest.TestCase):
    def test_check_blocks_open_breaker_and_resets_after_timeout(self):
        breaker = _CircuitBreaker(failure_threshold=1, recovery_timeout=1.0)
        breaker.record_failure()

        with self.assertRaisesRegex(Exception, "OPEN"):
            breaker.check()

        breaker._opened_at = time.time() - 2.0
        breaker.check()
        self.assertEqual(breaker.state, breaker.CLOSED)
        self.assertTrue(breaker.call_allowed())

    def test_normalize_result_list_wraps_single_dict(self):
        self.assertEqual(_normalize_result_list(None), [])
        self.assertEqual(_normalize_result_list([{"a": 1}]), [{"a": 1}])
        self.assertEqual(_normalize_result_list({"a": 1}), [{"a": 1}])


class DeltaPositionNormalizationTests(unittest.TestCase):
    def test_get_position_handles_single_dict_result(self):
        client = DeltaClient()
        with (
            patch.object(client, "_is_configured", return_value=True),
            patch.object(
                client,
                "_get",
                return_value={"result": {"product_id": 11, "size": "2", "entry_price": "100.5"}},
            ),
        ):
            position = client.get_position(11, strict=True)

        self.assertEqual(position.get("product_id"), 11)
        self.assertEqual(position.get("size"), "2")

    def test_get_positions_handles_single_dict_result(self):
        client = DeltaClient()
        with (
            patch.object(client, "_is_configured", return_value=True),
            patch.object(
                client,
                "_get",
                return_value={"result": {"product_id": 11, "size": "2"}},
            ),
        ):
            positions = client.get_positions()

        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0].get("product_id"), 11)

    def test_from_delta_symbol_preserves_paxg_contract(self):
        self.assertEqual(DeltaClient.from_delta_symbol("PAXGUSD"), "PAXGUSD")
        self.assertEqual(DeltaClient.from_delta_symbol("BTCUSD"), "BTCUSDT")


class LiveEngineHardeningTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)

    def _make_engine(self, broker, run_id):
        engine = LiveEngine(broker, run_id=run_id)
        engine._state_file = os.path.join(self._tmp.name, f"{run_id}.json")
        engine.configure(
            strategy={
                "run_name": run_id,
                "symbol": "BTCUSDT",
                "leverage": 10,
                "trade_side": "LONG",
                "indicators": [],
                "max_trades_per_day": 1,
                "stoploss_pct": 5,
                "target_profit_pct": 10,
                "initial_capital": 100,
                "position_size_pct": 100,
                "candle_interval": "5m",
                "poll_interval": 0,
            },
            entry_conditions=[{"left": "x", "operator": "is_above", "right": "y"}],
            exit_conditions=[],
        )
        engine._start_ws_feed = AsyncMock(return_value=None)
        engine._stop_ws_feed = AsyncMock(return_value=None)
        return engine

    async def test_start_refuses_missing_product(self):
        broker = FakeLiveBroker(product=None)
        engine = self._make_engine(broker, "missing-product")
        events = []

        async def callback(event):
            events.append(event)

        await engine.start(callback=callback)

        self.assertFalse(engine.running)
        self.assertFalse(engine.open_trades)
        self.assertTrue(any("Product not found" in e.get("message", "") for e in events))

    async def test_reconcile_recovers_exchange_position(self):
        broker = FakeLiveBroker(position={"size": "3", "entry_price": "102.5", "margin": "30", "leverage": "10"})
        engine = self._make_engine(broker, "reconcile-recover")

        await engine._reconcile_exchange_position(11, "BTCUSDT", 10)

        self.assertEqual(len(engine.open_trades), 1)
        trade = engine.open_trades[0]
        self.assertEqual(trade["side"], "LONG")
        self.assertEqual(trade["size"], 3.0)
        self.assertEqual(trade["entry_price"], 102.5)
        self.assertTrue(engine.in_trade)

    async def test_reconcile_clears_phantom_local_trade_when_exchange_is_flat(self):
        broker = FakeLiveBroker(position={})
        engine = self._make_engine(broker, "reconcile-clear")
        engine.open_trades = [
            {
                "id": 1,
                "symbol": "BTCUSDT",
                "side": "LONG",
                "entry_price": 100.0,
                "size": 2,
                "notional": 200.0,
                "margin": 20.0,
                "leverage": 10,
            }
        ]
        engine.in_trade = True

        await engine._reconcile_exchange_position(11, "BTCUSDT", 10)

        self.assertFalse(engine.open_trades)
        self.assertFalse(engine.in_trade)

    async def test_live_start_uses_verified_order_path(self):
        broker = FakeLiveBroker(position={})
        engine = self._make_engine(broker, "verified-entry")

        async def callback(event):
            if event.get("type") == "entry":
                engine.stop()

        with (
            patch("engine.live.compute_dynamic_indicators", side_effect=lambda df, indicators: df),
            patch("engine.live.eval_condition_group", side_effect=lambda row, conditions, prev: True),
        ):
            await engine.start(callback=callback)

        self.assertEqual(len(broker.verified_calls), 1)
        self.assertEqual(broker.verified_calls[0]["side"], "buy")
        self.assertEqual(engine.open_trades[0]["order_id"], "ord-1")
        self.assertEqual(engine.open_trades[0]["entry_price"], 101.5)
        self.assertEqual(broker.verified_calls[0]["size"], 1000)

    def test_live_signal_rows_ignore_forming_last_candle(self):
        idx = pd.to_datetime(["2026-03-25 12:00:00+00:00", "2026-03-25 12:05:00+00:00"])
        df = pd.DataFrame(
            [
                {"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 1.0},
                {"open": 101.0, "high": 102.0, "low": 100.5, "close": 101.5, "volume": 1.0},
            ],
            index=idx,
        )
        latest_row, signal_row, signal_prev, candle_closed = select_live_signal_rows(
            df, "5m", pd.Timestamp("2026-03-25 12:07:00+00:00").to_pydatetime()
        )
        self.assertFalse(candle_closed)
        self.assertEqual(float(latest_row["close"]), 101.5)
        self.assertEqual(signal_row.name, idx[0])
        self.assertEqual(signal_prev.name, idx[0])


class PaperEngineSignalTests(unittest.TestCase):
    def test_paper_signal_rows_ignore_forming_last_candle(self):
        idx = pd.to_datetime(["2026-03-25 12:00:00+00:00", "2026-03-25 12:05:00+00:00"])
        df = pd.DataFrame(
            [
                {"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 1.0},
                {"open": 101.0, "high": 102.0, "low": 100.5, "close": 101.5, "volume": 1.0},
            ],
            index=idx,
        )
        latest_row, signal_row, signal_prev, candle_closed = select_paper_signal_rows(
            df, "5m", pd.Timestamp("2026-03-25 12:07:00+00:00").to_pydatetime()
        )
        self.assertFalse(candle_closed)
        self.assertEqual(float(latest_row["close"]), 101.5)
        self.assertEqual(signal_row.name, idx[0])
        self.assertEqual(signal_prev.name, idx[0])


class PaperEngineParityTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)

    async def test_paper_engine_uses_fixed_qty_and_applies_fees(self):
        broker = FakeLiveBroker(position={})
        engine = PaperTradingEngine(broker, run_id="paper-fixed-qty")
        engine._state_file = os.path.join(self._tmp.name, "paper-fixed-qty.json")
        engine.configure(
            strategy={
                "run_name": "paper-fixed-qty",
                "symbol": "BTCUSDT",
                "leverage": 10,
                "trade_side": "LONG",
                "indicators": [],
                "max_trades_per_day": 1,
                "stoploss_pct": 50,
                "target_profit_pct": 50,
                "trailing_sl_pct": 0,
                "fee_pct": 0.05,
                "compounding": True,
                "initial_capital": 100,
                "position_size_pct": 100,
                "position_size_mode": "fixed_qty",
                "fixed_qty": 0.25,
                "candle_interval": "5m",
                "poll_interval": 0,
            },
            entry_conditions=[{"left": "entry", "operator": "is_above", "right": "x"}],
            exit_conditions=[{"left": "exit", "operator": "is_above", "right": "x"}],
        )
        engine._start_ws_feed = AsyncMock(return_value=None)
        engine._stop_ws_feed = AsyncMock(return_value=None)

        async def callback(event):
            if event.get("type") == "entry":
                engine._ws_price = 105.0
            elif event.get("type") == "exit":
                engine.stop()

        with (
            patch("engine.paper_trading.compute_dynamic_indicators", side_effect=lambda df, indicators: df),
            patch(
                "engine.paper_trading.eval_condition_group",
                side_effect=lambda row, conditions, prev: bool(conditions),
            ),
        ):
            await engine.start(callback=callback)

        self.assertEqual(len(engine.closed_trades), 1)
        trade = engine.closed_trades[0]
        self.assertEqual(trade["size"], 25)
        self.assertGreater(trade["gross_pnl"], trade["pnl"])
        self.assertGreater(trade["fees"], 0)


class HistoryPersistenceTests(unittest.TestCase):
    @staticmethod
    def _sample_trade():
        return {
            "symbol": "BTCUSDT",
            "side": "LONG",
            "entry_time": "2026-03-25T09:15:00",
            "exit_time": "2026-03-25T09:20:00",
            "entry_price": 100.0,
            "exit_price": 101.25,
            "pnl": 12.5,
            "exit_reason": "Signal Exit",
        }

    def test_save_engine_run_to_history_skips_duplicate_single_trade_history(self):
        os.environ.setdefault("CRYPTOFORGE_PIN", "123456")
        app_module = import_module("app")
        trade = self._sample_trade()
        existing_runs = [{"id": 1, "mode": "paper", "run_name": "Paper Alpha", "trade_count": 1, "trades": [trade]}]
        status = {
            "strategy_name": "Paper Alpha",
            "symbol": "BTCUSDT",
            "leverage": 5,
            "closed_trades": [trade],
        }

        with (
            patch.object(app_module, "_load_runs", return_value=existing_runs),
            patch.object(app_module, "_save_runs") as save_runs,
        ):
            app_module._save_engine_run_to_history(status, "paper")

        save_runs.assert_not_called()

    def test_save_trade_to_history_skips_existing_trade_signature(self):
        os.environ.setdefault("CRYPTOFORGE_PIN", "123456")
        app_module = import_module("app")
        trade = self._sample_trade()
        existing_runs = [{"id": 1, "mode": "paper", "run_name": "Paper Alpha", "trade_count": 1, "trades": [trade]}]

        with (
            patch.object(app_module, "_load_runs", return_value=existing_runs),
            patch.object(app_module, "_save_runs") as save_runs,
        ):
            app_module._save_trade_to_history(trade, "paper", "Paper Alpha")

        save_runs.assert_not_called()


class WebSocketFeedContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_connect_returns_once_socket_is_ready(self):
        from engine.ws_feed import DeltaWSFeed

        feed = DeltaWSFeed()
        blocker = asyncio.Event()

        async def fake_do_connect():
            feed._connected = True
            feed._ws = SimpleNamespace(closed=False)
            feed._connected_event.set()
            await blocker.wait()

        feed._do_connect = fake_do_connect

        await asyncio.wait_for(feed.connect(), 0.2)

        self.assertTrue(feed.connected)
        self.assertIsNotNone(feed._connect_task)
        self.assertFalse(feed._connect_task.done())

        feed._running = False
        blocker.set()
        await asyncio.wait_for(feed._connect_task, 0.2)

    async def test_authenticate_uses_key_auth_payload(self):
        from engine.ws_feed import DeltaWSFeed

        feed = DeltaWSFeed()
        feed._api_key = "test-key"
        feed._api_secret = "test-secret"
        sent = []
        feed._ws = SimpleNamespace(closed=False, send_json=AsyncMock(side_effect=lambda payload: sent.append(payload)))

        await feed._authenticate()

        self.assertEqual(sent[0]["type"], "key-auth")
        self.assertEqual(sent[0]["payload"]["api-key"], "test-key")
        self.assertTrue(sent[0]["payload"]["signature"])

    async def test_subscribe_ticker_uses_symbols_payload(self):
        from engine.ws_feed import DeltaWSFeed

        feed = DeltaWSFeed()
        sent = []
        feed._ws = SimpleNamespace(closed=False, send_json=AsyncMock(side_effect=lambda payload: sent.append(payload)))

        await feed.subscribe_ticker("BTCUSDT")

        self.assertEqual(
            sent[0],
            {
                "type": "subscribe",
                "payload": {"channels": [{"name": "v2/ticker", "symbols": ["BTCUSD"]}]},
            },
        )
        self.assertIn("v2/ticker:BTCUSD", feed.get_status()["subscribed_channels"])

    async def test_dispatch_ticker_message_without_channel_routes_handler(self):
        from engine.ws_feed import DeltaWSFeed

        feed = DeltaWSFeed()
        seen = []
        feed.on_ticker = lambda symbol, ticker: seen.append((symbol, ticker))

        await feed._dispatch({"type": "v2/ticker", "symbol": "BTCUSD", "mark_price": "104.25"})

        self.assertEqual(seen, [("BTCUSD", {"type": "v2/ticker", "symbol": "BTCUSD", "mark_price": "104.25"})])

    async def test_key_auth_success_flushes_pending_private_subscriptions(self):
        from engine.ws_feed import DeltaWSFeed

        feed = DeltaWSFeed()
        sent = []
        feed._ws = SimpleNamespace(closed=False, send_json=AsyncMock(side_effect=lambda payload: sent.append(payload)))
        feed._pending_auth_channels.add(feed._subscription_key("positions", ["all"]))

        await feed._dispatch({"type": "key-auth", "success": True, "status": "authenticated"})

        self.assertTrue(feed.authenticated)
        self.assertEqual(
            sent[0],
            {
                "type": "subscribe",
                "payload": {"channels": [{"name": "positions", "symbols": ["all"]}]},
            },
        )

    async def test_failed_connect_closes_client_session(self):
        import engine.ws_feed as ws_mod
        from engine.ws_feed import DeltaWSFeed

        class FailingSession:
            def __init__(self):
                self.closed = False

            async def ws_connect(self, *args, **kwargs):
                raise OSError("network down")

            async def close(self):
                self.closed = True

        session = FailingSession()
        feed = DeltaWSFeed()
        feed._running = True
        feed.on_disconnect = lambda reason: setattr(feed, "_running", False)

        with patch.object(ws_mod.aiohttp, "ClientSession", return_value=session):
            await feed._do_connect()

        self.assertTrue(session.closed)
        self.assertIsNone(feed._session)
        status = feed.get_status()
        self.assertIn(status["connection_state"], {"reconnecting", "disconnected"})
        self.assertIn("network down", status["last_disconnect_reason"])


class ScalpEngineHardeningTests(unittest.IsolatedAsyncioTestCase):
    async def test_scalp_paper_entry_skips_broker_lookups_without_cached_price(self):
        class OfflineScalpDelta(FakeScalpDelta):
            def get_product_by_symbol(self, symbol):
                self.product_calls += 1
                raise AssertionError("paper mode should not look up broker products")

            def get_ticker(self, symbol):
                self.ticker_calls += 1
                raise AssertionError("paper mode should not block on broker tickers")

        delta = OfflineScalpDelta()
        engine = ScalpEngine(delta)
        engine.start = lambda: None

        entered = await engine.enter_trade(
            symbol="BTCUSDT",
            side="LONG",
            size=100,
            leverage=10,
            target_usd=10,
            sl_usd=5,
            mode="paper",
        )

        self.assertEqual(entered["status"], "ok")
        self.assertEqual(delta.product_calls, 0)
        self.assertEqual(delta.ticker_calls, 0)
        self.assertEqual(entered["trade"]["order_id"], "PAPER")

    async def test_scalp_live_entry_and_exit_use_verified_orders(self):
        delta = FakeScalpDelta()
        engine = ScalpEngine(delta)
        engine.start = lambda: None

        entered = await engine.enter_trade(
            symbol="BTCUSDT",
            side="LONG",
            size=100,
            leverage=10,
            target_usd=10,
            sl_usd=5,
            mode="live",
        )
        self.assertEqual(entered["status"], "ok")
        self.assertEqual(entered["trade"]["order_id"], "scalp-1")
        self.assertEqual(entered["trade"]["entry_price"], 100.5)

        trade_id = entered["trade_id"]
        engine.open_trades[trade_id].current_price = 101.0
        exited = await engine.exit_trade(trade_id, reason="manual")

        self.assertEqual(exited["status"], "ok")
        self.assertEqual(len(delta.verified_calls), 2)
        self.assertFalse(engine.open_trades)
        self.assertEqual(engine.closed_trades[-1]["exit_order_id"], "scalp-2")

    async def test_scalp_live_exit_rejection_keeps_trade_open(self):
        delta = RejectingExitScalpDelta()
        engine = ScalpEngine(delta)
        engine.start = lambda: None

        entered = await engine.enter_trade(
            symbol="BTCUSDT",
            side="LONG",
            size=100,
            leverage=10,
            target_usd=10,
            sl_usd=5,
            mode="live",
        )
        trade_id = entered["trade_id"]
        engine.open_trades[trade_id].current_price = 101.0

        exited = await engine.exit_trade(trade_id, reason="manual")

        self.assertEqual(exited["status"], "error")
        self.assertIn("rejected", exited["message"].lower())
        self.assertIn(trade_id, engine.open_trades)
        self.assertFalse(engine.closed_trades)
        self.assertEqual(engine.get_status()["execution_metrics"]["phase"], "exit_reject")
        self.assertEqual(engine.get_status()["execution_metrics"]["order_lifecycle"], "rejected")
        self.assertEqual(engine.get_status()["execution_metrics"]["verification_state"], "rejected")
        self.assertIn("rejected", engine.get_status()["execution_metrics"]["verification_summary"].lower())

    async def test_scalp_trade_action_lock_blocks_conflicting_updates(self):
        delta = FakeScalpDelta()
        engine = ScalpEngine(delta)
        engine.start = lambda: None

        entered = await engine.enter_trade(
            symbol="BTCUSDT",
            side="LONG",
            size=100,
            leverage=10,
            target_usd=10,
            sl_usd=5,
            mode="paper",
        )
        trade_id = entered["trade_id"]
        lock = engine._trade_action_lock(trade_id)
        await lock.acquire()
        try:
            result = await engine.update_trade_targets(trade_id, target_usd=15)
        finally:
            lock.release()

        self.assertEqual(result["status"], "error")
        self.assertIn("action in progress", result["message"].lower())

    async def test_scalp_ws_ticker_updates_trade_price(self):
        delta = FakeScalpDelta()
        engine = ScalpEngine(delta)
        engine.start = lambda: None
        entered = await engine.enter_trade(
            symbol="BTCUSDT",
            side="LONG",
            size=100,
            leverage=10,
            target_usd=10,
            sl_usd=5,
            mode="paper",
        )
        trade_id = entered["trade_id"]

        engine._handle_ticker("BTCUSD", {"mark_price": 103.75})

        trade = engine.open_trades[trade_id]
        self.assertEqual(trade.entry_price, 103.75)
        self.assertEqual(trade.current_price, 103.75)
        self.assertTrue(trade._post_entry_price_ready)
        self.assertEqual(engine.get_status()["open_trades"][0]["mark_price"], 103.75)

    async def test_scalp_paper_entry_backfills_pct_targets_when_first_tick_arrives(self):
        class OfflineScalpDelta(FakeScalpDelta):
            def get_product_by_symbol(self, symbol):
                raise AssertionError("paper mode should not look up broker products")

            def get_ticker(self, symbol):
                raise AssertionError("paper mode should not block on broker tickers")

        delta = OfflineScalpDelta()
        engine = ScalpEngine(delta)
        engine.start = lambda: None
        entered = await engine.enter_trade(
            symbol="BTCUSDT",
            side="LONG",
            size=100,
            leverage=10,
            target_pct=10,
            sl_pct=5,
            mode="paper",
        )
        trade_id = entered["trade_id"]
        trade = engine.open_trades[trade_id]

        self.assertEqual(trade.entry_price, 0.0)
        self.assertEqual(trade.target_price, 0.0)
        self.assertEqual(trade.sl_price, 0.0)

        engine._handle_ticker("BTCUSD", {"mark_price": 100.0})

        self.assertEqual(trade.entry_price, 100.0)
        self.assertEqual(trade.current_price, 100.0)
        self.assertEqual(trade.target_price, 101.0)
        self.assertEqual(trade.sl_price, 99.5)

    async def test_scalp_trade_waits_for_fresh_price_before_pnl_exit_checks(self):
        delta = FakeScalpDelta()
        engine = ScalpEngine(delta)
        engine.start = lambda: None

        entered = await engine.enter_trade(
            symbol="BTCUSDT",
            side="LONG",
            size=10000,
            leverage=50,
            target_usd=100,
            sl_usd=100,
            mode="live",
        )
        trade = engine.open_trades[entered["trade_id"]]

        self.assertFalse(trade.can_evaluate_exit(trade.entry_time))
        trade._post_entry_price_ready = True
        self.assertFalse(trade.can_evaluate_exit(trade.entry_time + timedelta(seconds=1)))
        self.assertTrue(trade.can_evaluate_exit(trade.entry_time + timedelta(seconds=3)))
        self.assertEqual(trade.check_exit(99.0), "sl_usd_hit")

    async def test_scalp_status_exposes_ws_feed_diagnostics(self):
        delta = FakeScalpDelta()
        engine = ScalpEngine(delta)
        now = datetime.now()
        engine._ws_feed = SimpleNamespace(
            connected=True,
            get_status=lambda: {
                "authenticated": False,
                "messages_received": 42,
                "reconnect_count": 3,
                "last_error": "stale ticker",
                "subscribed_channels": ["v2/ticker:BTCUSD"],
                "pending_auth_channels": [],
            },
        )
        engine._last_price_ts["BTCUSDT"] = now
        engine._last_price_source["BTCUSDT"] = "rest_quote"

        status = engine.get_status()

        self.assertTrue(status["feed_metrics"]["ws_connected"])
        self.assertEqual(status["feed_metrics"]["messages_received"], 42)
        self.assertEqual(status["feed_metrics"]["reconnect_count"], 3)
        self.assertEqual(status["feed_metrics"]["last_error"], "stale ticker")
        self.assertEqual(status["feed_metrics"]["subscribed_channels"], ["v2/ticker:BTCUSD"])

    async def test_scalp_status_tracks_watch_symbol_entry_controls(self):
        delta = FakeScalpDelta()
        engine = ScalpEngine(delta)
        engine.start = lambda: None
        engine.watch_symbol("BTCUSDT")

        waiting = engine.get_status()
        self.assertEqual(waiting["entry_controls"]["symbol"], "BTCUSDT")
        self.assertEqual(waiting["entry_controls"]["state"], "waiting")
        self.assertFalse(waiting["entry_controls"]["paper_allowed"])
        self.assertFalse(waiting["entry_controls"]["live_allowed"])

        engine._record_price("BTCUSD", 101.25, source="ws")
        fresh = engine.get_status()
        self.assertEqual(fresh["entry_controls"]["symbol"], "BTCUSDT")
        self.assertEqual(fresh["entry_controls"]["state"], "fresh")
        self.assertTrue(fresh["entry_controls"]["paper_allowed"])
        self.assertTrue(fresh["entry_controls"]["live_allowed"])

    async def test_scalp_status_blocks_stale_watch_symbol_entries(self):
        delta = FakeScalpDelta()
        engine = ScalpEngine(delta)
        engine.start = lambda: None
        engine.watch_symbol("BTCUSDT")
        engine._last_prices["BTCUSDT"] = 100.0
        engine._last_price_source["BTCUSDT"] = "rest_quote"
        engine._last_price_ts["BTCUSDT"] = datetime.utcnow() - timedelta(seconds=12)

        stale = engine.get_status()

        self.assertEqual(stale["entry_controls"]["symbol"], "BTCUSDT")
        self.assertEqual(stale["entry_controls"]["state"], "stale")
        self.assertFalse(stale["entry_controls"]["paper_allowed"])
        self.assertFalse(stale["entry_controls"]["live_allowed"])
        self.assertIn("stale", stale["entry_controls"]["reason"].lower())

    async def test_scalp_guardrail_arms_pending_entry_until_price_crosses(self):
        delta = FakeScalpDelta(ticker_prices=[100.0, 100.0, 104.0, 105.5])
        engine = ScalpEngine(delta)
        self.addCleanup(engine.stop)
        engine._ensure_ws_feed = AsyncMock(return_value=None)
        engine._stop_ws_feed = AsyncMock(return_value=None)

        entered = await engine.enter_trade(
            symbol="BTCUSDT",
            side="LONG",
            size=10000,
            leverage=50,
            target_usd=100,
            sl_usd=100,
            guardrail_price=105.0,
            mode="paper",
        )

        self.assertEqual(entered["status"], "pending")
        self.assertEqual(len(engine.pending_entries), 1)
        self.assertFalse(engine.open_trades)

        engine.start()
        await asyncio.sleep(1.3)

        self.assertFalse(engine.pending_entries)
        self.assertEqual(len(engine.open_trades), 1)
        trade = next(iter(engine.open_trades.values()))
        self.assertEqual(trade.guardrail_price, 105.0)
        self.assertGreaterEqual(trade.entry_price, 105.0)

    async def test_scalp_limit_entry_arms_pending_base_quantity(self):
        delta = FakeScalpDelta(ticker_prices=[100.0])
        engine = ScalpEngine(delta)
        engine.start = lambda: None

        entered = await engine.enter_trade(
            symbol="BTCUSDT",
            side="LONG",
            leverage=10,
            qty_mode="base",
            qty_value=0.0015,
            entry_limit_price=99.0,
            target_price=102.0,
            sl_price=98.0,
            mode="paper",
        )

        self.assertEqual(entered["status"], "pending")
        pending = entered["pending_entry"]
        self.assertEqual(pending["qty_mode"], "base")
        self.assertAlmostEqual(pending["base_qty"], 0.0015)
        self.assertEqual(pending["entry_limit_price"], 99.0)
        self.assertEqual(pending["entry_stop_price"], 0.0)

    async def test_scalp_add_to_trade_scales_position_from_base_quantity(self):
        delta = FakeScalpDelta(ticker_prices=[100.0, 100.0])
        engine = ScalpEngine(delta)
        engine.start = lambda: None
        engine._record_price("BTCUSD", 100.0, source="ws")

        entered = await engine.enter_trade(
            symbol="BTCUSDT",
            side="LONG",
            leverage=10,
            qty_mode="base",
            qty_value=0.0015,
            target_usd=10,
            sl_usd=5,
            mode="paper",
        )
        trade_id = entered["trade_id"]
        trade = engine.open_trades[trade_id]
        initial_size = trade.size
        self.assertGreater(initial_size, 0)

        scaled = await engine.add_to_trade(trade_id, qty_mode="base", qty_value=0.0005)

        self.assertEqual(scaled["status"], "ok")
        updated = engine.open_trades[trade_id]
        self.assertGreater(updated.size, initial_size)
        self.assertGreater(updated.base_qty, 0.0015)
        self.assertEqual(updated.entry_price, 100.0)

    async def test_scalp_execution_metrics_track_entry_target_add_exit_lifecycle(self):
        delta = FakeScalpDelta()
        engine = ScalpEngine(delta)
        engine.start = lambda: None
        engine._record_price("BTCUSD", 100.0, source="ws")

        entered = await engine.enter_trade(
            symbol="BTCUSDT",
            side="LONG",
            size=100,
            leverage=10,
            target_usd=10,
            sl_usd=5,
            mode="live",
        )
        trade_id = entered["trade_id"]
        entry_exec = engine.get_status()["execution_metrics"]
        self.assertEqual(entry_exec["phase"], "entry")
        self.assertEqual(entry_exec["order_lifecycle"], "filled")
        self.assertEqual(entry_exec["trade_id"], trade_id)

        updated = await engine.update_trade_targets(trade_id, target_usd=12, sl_usd=4)
        self.assertEqual(updated["status"], "ok")
        target_exec = engine.get_status()["execution_metrics"]
        self.assertEqual(target_exec["phase"], "targets")
        self.assertEqual(target_exec["order_lifecycle"], "updated")

        added = await engine.add_to_trade(trade_id, qty_mode="base", qty_value=0.001)
        self.assertEqual(added["status"], "ok")
        add_exec = engine.get_status()["execution_metrics"]
        self.assertEqual(add_exec["phase"], "scale_in")
        self.assertEqual(add_exec["order_lifecycle"], "filled")
        self.assertGreater(add_exec["requested_size"], 0)

        engine.open_trades[trade_id].current_price = 101.0
        exited = await engine.exit_trade(trade_id, reason="manual")
        self.assertEqual(exited["status"], "ok")
        exit_exec = engine.get_status()["execution_metrics"]
        self.assertEqual(exit_exec["phase"], "exit")
        self.assertEqual(exit_exec["order_lifecycle"], "filled")


class RouteAuditTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.app_module = import_module("app")

    async def test_dashboard_summary_aggregates_all_todays_saved_runs(self):
        fake_runs = [
            {"id": 1, "mode": "paper", "created_at": "2026-03-24 11:00:00", "total_pnl": 10, "trade_count": 1},
            {"id": 2, "mode": "paper", "created_at": "2026-03-26 10:00:00", "total_pnl": 25, "trade_count": 2},
            {"id": 3, "mode": "paper", "started_at": "2026-03-26 14:00:00", "total_pnl": -5, "trade_count": 1},
            {"id": 4, "mode": "live", "created_at": "2026-03-26 15:00:00", "total_pnl": 12, "trade_count": 3},
        ]
        with (
            patch.object(self.app_module, "_load", return_value=[]),
            patch.object(self.app_module, "_load_runs", return_value=fake_runs),
            patch.object(self.app_module, "paper_engines", {}),
            patch.object(self.app_module, "live_engines", {}),
            patch.object(self.app_module, "_today_local_date", return_value=date(2026, 3, 26)),
        ):
            summary = await self.app_module.dashboard_summary(None)

        self.assertEqual(summary["paper_pnl"], 20)
        self.assertEqual(summary["paper_trades"], 3)
        self.assertEqual(summary["live_pnl"], 12)
        self.assertEqual(summary["live_trades"], 3)
        self.assertEqual(summary["today_pnl"], 32)

    async def test_paper_status_with_missing_run_id_uses_stopped_snapshot(self):
        running_engine = type(
            "RunningEngine", (), {"running": True, "get_status": lambda self: {"strategy_name": "Other"}}
        )()
        with (
            patch.object(self.app_module, "paper_engines", {"other-run": running_engine}),
            patch.object(
                self.app_module,
                "_stopped_engines",
                {"target-run": {"strategy_name": "Stopped Paper", "total_pnl": 44, "mode": "paper"}},
            ),
        ):
            status = await self.app_module.paper_status("target-run")

        self.assertEqual(status["strategy_name"], "Stopped Paper")
        self.assertFalse(status["running"])


class SessionSecurityTests(unittest.TestCase):
    def setUp(self):
        self.app_module = import_module("app")
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._orig_session_file = self.app_module._SESSION_FILE
        self._orig_state_dir = getattr(self.app_module, "_STATE_DIR", None)
        self.app_module._SESSION_FILE = os.path.join(self._tmp.name, "sessions.json")
        self.app_module._STATE_DIR = self._tmp.name
        self.addCleanup(self._restore_session_file)

    def _restore_session_file(self):
        self.app_module._SESSION_FILE = self._orig_session_file
        if self._orig_state_dir is not None:
            self.app_module._STATE_DIR = self._orig_state_dir

    @staticmethod
    def _request(user_agent="CryptoForgeTest/1.0", ip="127.0.0.1"):
        return SimpleNamespace(
            headers={"user-agent": user_agent, "x-forwarded-for": ip}, client=SimpleNamespace(host=ip)
        )

    def test_legacy_string_session_records_still_validate(self):
        token = "legacy-token"
        self.app_module._save_sessions({token: (datetime.now() + timedelta(minutes=10)).isoformat()})

        self.assertTrue(self.app_module._validate_session(token))

    def test_session_rejects_user_agent_mismatch(self):
        token = self.app_module._create_session(request=self._request(user_agent="UA-A"))

        self.assertFalse(self.app_module._validate_session(token, request=self._request(user_agent="UA-B")))

    def test_session_expires_after_idle_timeout(self):
        token = self.app_module._create_session(request=self._request())
        sessions = self.app_module._load_sessions()
        sessions[token]["last_seen_at"] = (
            datetime.now() - timedelta(seconds=self.app_module._SESSION_IDLE_SEC + 60)
        ).isoformat()
        self.app_module._save_sessions(sessions)

        self.assertFalse(self.app_module._validate_session(token, request=self._request()))


class AuthRouteSessionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.app_module = import_module("app")
        self._tmp = tempfile.TemporaryDirectory()
        self.addAsyncCleanup(self._tmp.cleanup)
        self._orig_session_file = self.app_module._SESSION_FILE
        self._orig_state_dir = getattr(self.app_module, "_STATE_DIR", None)
        self.app_module._STATE_DIR = self._tmp.name
        self.app_module._SESSION_FILE = os.path.join(self._tmp.name, "sessions.json")
        self.addAsyncCleanup(self._restore_session_file)

    async def _restore_session_file(self):
        engine = getattr(self.app_module, "_scalp_engine", None)
        if engine is not None:
            await engine.shutdown()
            self.app_module._scalp_engine = None
        self.app_module._SESSION_FILE = self._orig_session_file
        if self._orig_state_dir is not None:
            self.app_module._STATE_DIR = self._orig_state_dir

    async def test_login_session_allows_authenticated_scalp_write(self):
        transport = httpx.ASGITransport(app=self.app_module.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver.local") as client:
            login = await client.post("/api/auth/login", json={"password": self.app_module.AUTH_PIN})
            self.assertEqual(login.status_code, 200)

            status = await client.get("/api/auth/status")
            self.assertEqual(status.status_code, 200)
            self.assertTrue(status.json()["authenticated"])

            prime = await client.get("/api/scalp/status", params={"symbol": "BTCUSDT"})
            self.assertEqual(prime.status_code, 200)
            self.app_module._scalp_engine._record_price("BTCUSD", 101.25, source="ws")

            csrf = client.cookies.get("cryptoforge_csrf") or ""
            enter = await client.post(
                "/api/scalp/enter",
                json={
                    "symbol": "BTCUSDT",
                    "side": "BUY",
                    "qty_usdt": 1000,
                    "leverage": 50,
                    "sl_usd": 100,
                    "tp_usd": 100,
                    "mode": "paper",
                },
                headers={"X-CSRF-Token": csrf, "X-Requested-With": "XMLHttpRequest"},
            )

        self.assertEqual(enter.status_code, 200)
        self.assertEqual(enter.json()["status"], "ok")

    async def test_scalp_enter_rejects_invalid_numeric_payloads(self):
        transport = httpx.ASGITransport(app=self.app_module.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver.local") as client:
            await client.post("/api/auth/login", json={"password": self.app_module.AUTH_PIN})
            csrf = client.cookies.get("cryptoforge_csrf") or ""
            bad = await client.post(
                "/api/scalp/enter",
                json={"symbol": "BTCUSDT", "side": "BUY", "qty_value": "oops", "leverage": 10, "mode": "paper"},
                headers={"X-CSRF-Token": csrf, "X-Requested-With": "XMLHttpRequest"},
            )

        self.assertEqual(bad.status_code, 400)
        self.assertEqual(bad.json()["error"]["detail"], "Invalid qty_value")

    async def test_scalp_target_update_rejects_invalid_values(self):
        transport = httpx.ASGITransport(app=self.app_module.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver.local") as client:
            await client.post("/api/auth/login", json={"password": self.app_module.AUTH_PIN})
            csrf = client.cookies.get("cryptoforge_csrf") or ""
            await client.get("/api/scalp/status", params={"symbol": "BTCUSDT"})
            self.app_module._scalp_engine._record_price("BTCUSD", 101.25, source="ws")
            entered = await client.post(
                "/api/scalp/enter",
                json={
                    "symbol": "BTCUSDT",
                    "side": "BUY",
                    "qty_value": 1000,
                    "qty_mode": "usdt",
                    "leverage": 10,
                    "mode": "paper",
                },
                headers={"X-CSRF-Token": csrf, "X-Requested-With": "XMLHttpRequest"},
            )
            trade_id = entered.json()["trade_id"]
            bad = await client.put(
                f"/api/scalp/trades/{trade_id}/targets",
                json={"target_price": "bad-price"},
                headers={"X-CSRF-Token": csrf, "X-Requested-With": "XMLHttpRequest"},
            )

        self.assertEqual(bad.status_code, 400)
        self.assertEqual(bad.json()["error"]["detail"], "Invalid target_price")

    async def test_scalp_add_rejects_invalid_quantity(self):
        transport = httpx.ASGITransport(app=self.app_module.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver.local") as client:
            await client.post("/api/auth/login", json={"password": self.app_module.AUTH_PIN})
            csrf = client.cookies.get("cryptoforge_csrf") or ""
            bad = await client.post(
                "/api/scalp/trades/1/add",
                json={"qty_mode": "base", "qty_value": "bad-qty"},
                headers={"X-CSRF-Token": csrf, "X-Requested-With": "XMLHttpRequest"},
            )

        self.assertEqual(bad.status_code, 400)
        self.assertEqual(bad.json()["error"]["detail"], "Invalid qty_value")

    async def test_scalp_status_rejects_unsupported_symbol(self):
        transport = httpx.ASGITransport(app=self.app_module.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver.local") as client:
            await client.post("/api/auth/login", json={"password": self.app_module.AUTH_PIN})
            bad = await client.get("/api/scalp/status", params={"symbol": "INVALID"})

        self.assertEqual(bad.status_code, 400)
        self.assertEqual(bad.json()["error"]["detail"], "Unsupported scalp symbol: INVALID")

    async def test_scalp_status_omits_archive_payloads_by_default(self):
        transport = httpx.ASGITransport(app=self.app_module.app)
        with (
            patch.object(self.app_module, "_load_scalp_trades", return_value=[{"trade_id": 1}]),
            patch.object(self.app_module, "_load_scalp_events", return_value=[{"msg": "from-disk"}]),
        ):
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver.local") as client:
                await client.post("/api/auth/login", json={"password": self.app_module.AUTH_PIN})
                status = await client.get("/api/scalp/status", params={"symbol": "BTCUSDT"})

        self.assertEqual(status.status_code, 200)
        payload = status.json()
        self.assertNotIn("file_trades", payload)
        self.assertNotIn("file_events", payload)

    async def test_scalp_activity_route_returns_archive_payloads(self):
        transport = httpx.ASGITransport(app=self.app_module.app)
        with (
            patch.object(self.app_module, "_load_scalp_trades", return_value=[{"trade_id": 7, "symbol": "BTCUSDT"}]),
            patch.object(
                self.app_module, "_load_scalp_events", return_value=[{"msg": "from-disk", "time": "10:00:00"}]
            ),
        ):
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver.local") as client:
                await client.post("/api/auth/login", json={"password": self.app_module.AUTH_PIN})
                activity = await client.get("/api/scalp/activity")

        self.assertEqual(activity.status_code, 200)
        payload = activity.json()
        self.assertEqual(payload["file_trades"][0]["trade_id"], 7)
        self.assertEqual(payload["file_events"][0]["msg"], "from-disk")

    async def test_scalp_exit_missing_trade_returns_404(self):
        transport = httpx.ASGITransport(app=self.app_module.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver.local") as client:
            await client.post("/api/auth/login", json={"password": self.app_module.AUTH_PIN})
            csrf = client.cookies.get("cryptoforge_csrf") or ""
            missing = await client.post(
                "/api/scalp/exit",
                json={"trade_id": 999999},
                headers={"X-CSRF-Token": csrf, "X-Requested-With": "XMLHttpRequest"},
            )

        self.assertEqual(missing.status_code, 404)
        self.assertIn("not found", missing.json()["error"]["detail"].lower())

    async def test_scalp_target_update_returns_409_when_trade_action_is_busy(self):
        transport = httpx.ASGITransport(app=self.app_module.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver.local") as client:
            await client.post("/api/auth/login", json={"password": self.app_module.AUTH_PIN})
            csrf = client.cookies.get("cryptoforge_csrf") or ""
            await client.get("/api/scalp/status", params={"symbol": "BTCUSDT"})
            self.app_module._scalp_engine._record_price("BTCUSD", 101.25, source="ws")
            entered = await client.post(
                "/api/scalp/enter",
                json={
                    "symbol": "BTCUSDT",
                    "side": "BUY",
                    "qty_value": 1000,
                    "qty_mode": "usdt",
                    "leverage": 10,
                    "mode": "paper",
                },
                headers={"X-CSRF-Token": csrf, "X-Requested-With": "XMLHttpRequest"},
            )
            trade_id = entered.json()["trade_id"]
            lock = self.app_module._scalp_engine._trade_action_lock(trade_id)
            await lock.acquire()
            try:
                conflicted = await client.put(
                    f"/api/scalp/trades/{trade_id}/targets",
                    json={"target_usd": 25},
                    headers={"X-CSRF-Token": csrf, "X-Requested-With": "XMLHttpRequest"},
                )
            finally:
                lock.release()

        self.assertEqual(conflicted.status_code, 409)
        self.assertIn("action in progress", conflicted.json()["error"]["detail"].lower())

    async def test_scalp_status_response_keeps_tight_csp_headers(self):
        transport = httpx.ASGITransport(app=self.app_module.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver.local") as client:
            await client.post("/api/auth/login", json={"password": self.app_module.AUTH_PIN})
            response = await client.get("/api/scalp/status", params={"symbol": "BTCUSDT"})

        csp = response.headers.get("content-security-policy", "")
        self.assertIn("connect-src 'self' ws: wss:;", csp)
        self.assertNotIn("connect-src 'self' ws: wss: https:", csp)
        self.assertIn("frame-src 'none'", csp)


class StatePathMigrationTests(unittest.TestCase):
    def test_resolve_state_file_migrates_flat_scalp_state_into_subdirectory(self):
        app_module = import_module("app")
        with tempfile.TemporaryDirectory() as tmp:
            legacy_here = os.path.join(tmp, "repo")
            os.makedirs(legacy_here, exist_ok=True)
            flat_state_path = os.path.join(tmp, "scalp_runtime.json")
            with open(flat_state_path, "w") as handle:
                json.dump({"open_trades": [{"trade_id": 1}]}, handle)

            orig_here = app_module._HERE
            orig_state_dir = app_module._STATE_DIR
            try:
                app_module._HERE = legacy_here
                app_module._STATE_DIR = tmp
                legacy_path, state_path = app_module._resolve_state_file("scalp_runtime.json", "scalp")
            finally:
                app_module._HERE = orig_here
                app_module._STATE_DIR = orig_state_dir

            self.assertEqual(legacy_path, os.path.join(legacy_here, "scalp_runtime.json"))
            self.assertEqual(state_path, os.path.join(tmp, "scalp", "scalp_runtime.json"))
            self.assertTrue(os.path.exists(state_path))
            with open(state_path, "r") as handle:
                migrated = json.load(handle)
            self.assertEqual(migrated["open_trades"][0]["trade_id"], 1)


class ScalpRuntimePersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.app_module = import_module("app")
        self._tmp = tempfile.TemporaryDirectory()
        self.addAsyncCleanup(self._tmp.cleanup)
        self._orig_runtime_file = self.app_module._SCALP_RUNTIME_FILE
        self._orig_state_dir = getattr(self.app_module, "_STATE_DIR", None)
        self.app_module._STATE_DIR = self._tmp.name
        self.app_module._SCALP_RUNTIME_FILE = os.path.join(self._tmp.name, "scalp_runtime.json")
        self.addAsyncCleanup(self._restore_runtime_file)

    async def _restore_runtime_file(self):
        engine = getattr(self.app_module, "_scalp_engine", None)
        if engine is not None:
            await engine.shutdown()
            self.app_module._scalp_engine = None
        self.app_module._SCALP_RUNTIME_FILE = self._orig_runtime_file
        if self._orig_state_dir is not None:
            self.app_module._STATE_DIR = self._orig_state_dir

    async def test_runtime_snapshot_restores_open_scalp_trade(self):
        delta = FakeScalpDelta(ticker_prices=[101.25, 101.25])
        engine = ScalpEngine(delta)
        engine.start = lambda: None
        engine._record_price("BTCUSD", 101.25, source="ws")

        entered = await engine.enter_trade(
            symbol="BTCUSDT",
            side="LONG",
            leverage=10,
            qty_mode="base",
            qty_value=0.0015,
            target_usd=10,
            sl_usd=5,
            mode="paper",
        )
        self.assertEqual(entered["status"], "ok")

        status = engine.get_status("BTCUSDT")
        self.app_module._save_scalp_runtime(self.app_module._snapshot_scalp_runtime(status))

        restored_engine = ScalpEngine(FakeScalpDelta([101.25]))
        self.addAsyncCleanup(restored_engine.shutdown)
        restored = self.app_module._restore_scalp_runtime(restored_engine)

        self.assertTrue(restored)
        self.assertEqual(len(restored_engine.open_trades), 1)
        restored_trade = next(iter(restored_engine.open_trades.values()))
        self.assertEqual(restored_trade.symbol, "BTCUSDT")
        self.assertGreater(restored_trade.current_price, 0)

    async def test_route_entry_persists_runtime_snapshot(self):
        transport = httpx.ASGITransport(app=self.app_module.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver.local") as client:
            await client.post("/api/auth/login", json={"password": self.app_module.AUTH_PIN})
            csrf = client.cookies.get("cryptoforge_csrf") or ""
            await client.get("/api/scalp/status", params={"symbol": "BTCUSDT"})
            self.app_module._scalp_engine._record_price("BTCUSD", 101.25, source="ws")
            entered = await client.post(
                "/api/scalp/enter",
                json={
                    "symbol": "BTCUSDT",
                    "side": "BUY",
                    "qty_mode": "base",
                    "qty_value": 0.0015,
                    "leverage": 10,
                    "mode": "paper",
                },
                headers={"X-CSRF-Token": csrf, "X-Requested-With": "XMLHttpRequest"},
            )

        self.assertEqual(entered.status_code, 200)
        runtime = self.app_module._load_scalp_runtime()
        self.assertEqual(len(runtime.get("open_trades") or []), 1)
        self.assertEqual((runtime.get("open_trades") or [])[0].get("symbol"), "BTCUSDT")

    async def test_scalp_action_routes_return_runtime_snapshot(self):
        transport = httpx.ASGITransport(app=self.app_module.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver.local") as client:
            await client.post("/api/auth/login", json={"password": self.app_module.AUTH_PIN})
            csrf = client.cookies.get("cryptoforge_csrf") or ""
            await client.get("/api/scalp/status", params={"symbol": "BTCUSDT"})
            self.app_module._scalp_engine._record_price("BTCUSD", 101.25, source="ws")
            entered = await client.post(
                "/api/scalp/enter",
                json={
                    "symbol": "BTCUSDT",
                    "side": "BUY",
                    "qty_mode": "base",
                    "qty_value": 0.0015,
                    "leverage": 10,
                    "mode": "paper",
                },
                headers={"X-CSRF-Token": csrf, "X-Requested-With": "XMLHttpRequest"},
            )
            entry_payload = entered.json()
            trade_id = entry_payload["trade"]["trade_id"]
            added = await client.post(
                f"/api/scalp/trades/{trade_id}/add",
                json={"qty_mode": "base", "qty_value": 0.0005},
                headers={"X-CSRF-Token": csrf, "X-Requested-With": "XMLHttpRequest"},
            )

        self.assertEqual(entered.status_code, 200)
        self.assertEqual(len(entry_payload.get("open_trades") or []), 1)
        self.assertEqual(entry_payload.get("open_trades")[0].get("symbol"), "BTCUSDT")
        self.assertEqual(added.status_code, 200)
        add_payload = added.json()
        self.assertEqual(add_payload.get("status"), "ok")
        self.assertEqual(len(add_payload.get("open_trades") or []), 1)
        self.assertGreater(
            add_payload.get("open_trades")[0].get("base_qty", 0),
            entry_payload.get("open_trades")[0].get("base_qty", 0),
        )
        self.assertEqual((add_payload.get("execution_metrics") or {}).get("phase"), "scale_in")


class RouteAuditContinuationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.app_module = import_module("app")

    async def test_live_status_with_missing_run_id_uses_stopped_snapshot(self):
        running_engine = type(
            "RunningEngine", (), {"running": True, "get_status": lambda self: {"strategy_name": "Other"}}
        )()
        with (
            patch.object(self.app_module, "live_engines", {"other-live": running_engine}),
            patch.object(
                self.app_module,
                "_stopped_engines",
                {"target-live": {"strategy_name": "Stopped Live", "total_pnl": 77, "mode": "live"}},
            ),
        ):
            status = await self.app_module.live_status("target-live")

        self.assertEqual(status["strategy_name"], "Stopped Live")
        self.assertFalse(status["running"])
        self.assertEqual(status["run_id"], "target-live")
        self.assertEqual(status["mode"], "live")

    async def test_scalp_persist_trade_saves_disk_copy_and_results_history(self):
        trade = HistoryPersistenceTests._sample_trade()
        with (
            patch.object(self.app_module, "_load_scalp_trades", return_value=[]),
            patch.object(self.app_module, "_save_scalp_trades") as save_scalp_trades,
            patch.object(self.app_module, "_save_scalp_trade_to_history") as save_scalp_history,
        ):
            self.app_module._scalp_persist_trade(trade)

        save_scalp_trades.assert_called_once()
        save_scalp_history.assert_called_once_with(trade)


if __name__ == "__main__":
    unittest.main()
