import os
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

import pandas as pd

from broker.delta import _CircuitBreaker
from engine.live import LiveEngine
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

    async def place_order_verified(self, **kwargs):
        self.verified_calls.append(kwargs)
        fill_price = 100.5 if not kwargs.get("reduce_only") else 101.25
        return {
            "id": f"scalp-{len(self.verified_calls)}",
            "verified": True,
            "verified_at_attempt": 1,
            "fill_price": fill_price,
        }


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


class ScalpEngineHardeningTests(unittest.IsolatedAsyncioTestCase):
    async def test_scalp_live_entry_and_exit_use_verified_orders(self):
        delta = FakeScalpDelta()
        engine = ScalpEngine(delta)
        engine._running = True

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


if __name__ == "__main__":
    unittest.main()
