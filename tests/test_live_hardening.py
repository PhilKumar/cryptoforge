import os
import tempfile
import time
import unittest
from importlib import import_module
from unittest.mock import AsyncMock, patch

import pandas as pd

from broker.delta import _CircuitBreaker
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
    def __init__(self):
        self.verified_calls = []

    def get_product_by_symbol(self, symbol):
        return {"id": 77}

    def get_ticker(self, symbol):
        return {"mark_price": 100.0}

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


class ScalpEngineHardeningTests(unittest.IsolatedAsyncioTestCase):
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

        self.assertEqual(engine.open_trades[trade_id].current_price, 103.75)
        self.assertEqual(engine.get_status()["open_trades"][0]["mark_price"], 103.75)


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
        self.assertEqual(status["run_id"], "target-run")
        self.assertEqual(status["mode"], "paper")

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


if __name__ == "__main__":
    unittest.main()
