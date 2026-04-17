import os
import tempfile
import unittest
from importlib import import_module

import pandas as pd

from engine.backtest import run_backtest
from engine.indicators import compute_dynamic_indicators


def _load_app_module():
    os.environ.setdefault("CRYPTOFORGE_PIN", "123456")
    os.environ.setdefault("DELTA_API_KEY", "dummy")
    os.environ.setdefault("DELTA_API_SECRET", "dummy")
    return import_module("app")


class StrategyRuntimeTests(unittest.TestCase):
    def test_normalize_runtime_injects_default_entry_for_interval(self):
        app_module = _load_app_module()

        runtime = app_module._normalize_strategy_runtime(
            indicators=[],
            entry_conditions=[],
            exit_conditions=[],
            candle_interval="15m",
        )

        self.assertEqual(runtime["entry_conditions"][0]["right"], "EMA_20_15m")
        self.assertEqual(runtime["exit_conditions"], [])
        self.assertIn("EMA_20_15m", runtime["indicators"])
        self.assertFalse(runtime["errors"])
        self.assertTrue(any("No exit conditions" in w for w in runtime["warnings"]))

    def test_normalize_runtime_autoloads_missing_indicator_dependency(self):
        app_module = _load_app_module()

        runtime = app_module._normalize_strategy_runtime(
            indicators=[],
            entry_conditions=[
                {
                    "left": "MACD_12_26_9_5m__histogram",
                    "operator": "crosses_above",
                    "right": "number",
                    "right_number_value": "0",
                    "connector": "AND",
                }
            ],
            exit_conditions=[],
            candle_interval="5m",
        )

        self.assertIn("MACD_12_26_9_5m", runtime["indicators"])
        self.assertEqual(runtime["unsupported_fields"], [])
        self.assertEqual(runtime["unresolved_fields"], [])

    def test_normalize_runtime_flags_signal_candle_as_unsupported(self):
        app_module = _load_app_module()

        runtime = app_module._normalize_strategy_runtime(
            indicators=[],
            entry_conditions=[
                {
                    "left": "Signal_Candle_Close",
                    "operator": "is_above",
                    "right": "current_close",
                    "connector": "AND",
                }
            ],
            exit_conditions=[],
            candle_interval="5m",
        )

        self.assertIn("Signal_Candle_Close", runtime["unsupported_fields"])

    def test_normalize_runtime_uses_legacy_condition_keys(self):
        app_module = _load_app_module()

        runtime = app_module._normalize_strategy_runtime(
            indicators=[],
            entry_conditions=[
                {
                    "lhs": "EMA_20_5m",
                    "operator": "is_above",
                    "rhs": "current_close",
                    "logic": "AND",
                }
            ],
            exit_conditions=[],
            candle_interval="5m",
        )

        self.assertEqual(runtime["entry_conditions"][0]["left"], "EMA_20_5m")
        self.assertEqual(runtime["entry_conditions"][0]["right"], "current_close")
        self.assertEqual(runtime["entry_conditions"][0]["connector"], "AND")

    def test_estimate_warmup_days_expands_for_monthly_cpr(self):
        app_module = _load_app_module()

        warmup_days = app_module._estimate_warmup_days("5m", ["CPR_Month_0.2_0.5"])

        self.assertGreaterEqual(warmup_days, 70)


class IndicatorComputationTests(unittest.TestCase):
    def test_resampled_hourly_ema_uses_completed_hour_only(self):
        idx = pd.date_range("2026-03-25 00:00:00+00:00", periods=24, freq="5min")
        df = pd.DataFrame(
            [
                {
                    "open": float(i + 1),
                    "high": float(i + 1.5),
                    "low": float(i + 0.5),
                    "close": float(i + 1),
                    "volume": 1.0,
                }
                for i in range(24)
            ],
            index=idx,
        )

        out = compute_dynamic_indicators(df, ["EMA_2_1h"], base_interval="5m")
        ema_series = out["EMA_2_1h"]

        self.assertTrue(ema_series.iloc[:12].isna().all())
        self.assertTrue((ema_series.iloc[12:] == 12.0).all())

    def test_macd_hourly_suffix_creates_resampled_columns(self):
        idx = pd.date_range("2026-03-25 00:00:00+00:00", periods=36, freq="5min")
        df = pd.DataFrame(
            [
                {
                    "open": float(i + 1),
                    "high": float(i + 1.5),
                    "low": float(i + 0.5),
                    "close": float(i + 1),
                    "volume": 1.0,
                }
                for i in range(36)
            ],
            index=idx,
        )

        out = compute_dynamic_indicators(df, ["MACD_12_26_9_1h"], base_interval="5m")

        self.assertIn("MACD_12_26_9_1h__line", out.columns)
        self.assertIn("MACD_12_26_9_1h__signal", out.columns)
        self.assertIn("MACD_12_26_9_1h__histogram", out.columns)

    def test_resampled_vwap_uses_completed_higher_timeframe(self):
        idx = pd.date_range("2026-03-25 00:00:00+00:00", periods=6, freq="5min")
        df = pd.DataFrame(
            [
                {"open": 10.0, "high": 12.0, "low": 9.0, "close": 11.0, "volume": 1.0},
                {"open": 11.0, "high": 13.0, "low": 10.0, "close": 12.0, "volume": 2.0},
                {"open": 12.0, "high": 14.0, "low": 11.0, "close": 13.0, "volume": 3.0},
                {"open": 13.0, "high": 15.0, "low": 12.0, "close": 14.0, "volume": 4.0},
                {"open": 14.0, "high": 16.0, "low": 13.0, "close": 15.0, "volume": 5.0},
                {"open": 15.0, "high": 17.0, "low": 14.0, "close": 16.0, "volume": 6.0},
            ],
            index=idx,
        )

        out = compute_dynamic_indicators(df, ["VWAP_15m"], base_interval="5m")
        vwap_series = out["VWAP_15m"]

        self.assertTrue(vwap_series.iloc[:3].isna().all())
        self.assertAlmostEqual(vwap_series.iloc[3], 12.0, places=6)
        self.assertAlmostEqual(vwap_series.iloc[4], 12.0, places=6)
        self.assertAlmostEqual(vwap_series.iloc[5], 12.0, places=6)

    def test_backtest_returns_clean_error_when_trimmed_range_is_empty(self):
        idx = pd.date_range("2026-03-20 00:00:00+00:00", periods=6, freq="5min")
        df = pd.DataFrame(
            [
                {"open": 10.0, "high": 12.0, "low": 9.0, "close": 11.0, "volume": 1.0},
                {"open": 11.0, "high": 13.0, "low": 10.0, "close": 12.0, "volume": 2.0},
                {"open": 12.0, "high": 14.0, "low": 11.0, "close": 13.0, "volume": 3.0},
                {"open": 13.0, "high": 15.0, "low": 12.0, "close": 14.0, "volume": 4.0},
                {"open": 14.0, "high": 16.0, "low": 13.0, "close": 15.0, "volume": 5.0},
                {"open": 15.0, "high": 17.0, "low": 14.0, "close": 16.0, "volume": 6.0},
            ],
            index=idx,
        )

        result = run_backtest(
            df,
            entry_conditions=[],
            exit_conditions=[],
            strategy_config={"from_date": "2026-03-22", "indicators": [], "candle_interval": "5m"},
        )

        self.assertEqual(result["status"], "error")
        self.assertIn("No data available", result["message"])

    def test_backtest_with_no_entry_conditions_keeps_zero_trades(self):
        idx = pd.date_range("2026-03-20 00:00:00+00:00", periods=8, freq="5min")
        df = pd.DataFrame(
            [
                {"open": 10.0, "high": 12.0, "low": 9.0, "close": 11.0, "volume": 1.0},
                {"open": 11.0, "high": 13.0, "low": 10.0, "close": 12.0, "volume": 2.0},
                {"open": 12.0, "high": 14.0, "low": 11.0, "close": 13.0, "volume": 3.0},
                {"open": 13.0, "high": 15.0, "low": 12.0, "close": 14.0, "volume": 4.0},
                {"open": 14.0, "high": 16.0, "low": 13.0, "close": 15.0, "volume": 5.0},
                {"open": 15.0, "high": 17.0, "low": 14.0, "close": 16.0, "volume": 6.0},
                {"open": 16.0, "high": 18.0, "low": 15.0, "close": 17.0, "volume": 7.0},
                {"open": 17.0, "high": 19.0, "low": 16.0, "close": 18.0, "volume": 8.0},
            ],
            index=idx,
        )

        result = run_backtest(
            df,
            entry_conditions=[],
            exit_conditions=[],
            strategy_config={"from_date": "2026-03-20", "indicators": [], "candle_interval": "5m"},
        )

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["stats"]["total_trades"], 0)

    def test_backtest_cost_model_applies_execution_and_funding_drag(self):
        idx = pd.date_range("2026-03-20 00:00:00+00:00", periods=5, freq="5min")
        df = pd.DataFrame(
            [
                {"open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0, "volume": 1.0},
                {"open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0, "volume": 1.0},
                {"open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0, "volume": 1.0},
                {"open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0, "volume": 1.0},
                {"open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0, "volume": 1.0},
            ],
            index=idx,
        )

        always_true = [{"left": "current_close", "operator": "is_above", "right": "number", "right_number_value": 0}]
        result = run_backtest(
            df,
            entry_conditions=always_true,
            exit_conditions=always_true,
            strategy_config={
                "from_date": "2026-03-20",
                "indicators": [],
                "candle_interval": "5m",
                "initial_capital": 1000,
                "leverage": 1,
                "position_size_mode": "fixed_qty",
                "fixed_qty": 1,
                "max_trades_per_day": 1,
                "spread_bps": 10,
                "slippage_bps": 5,
                "funding_bps_per_8h": 800,
            },
        )

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["stats"]["total_trades"], 1)
        self.assertAlmostEqual(result["stats"]["total_execution_cost"], 0.2, places=2)
        self.assertGreater(result["stats"]["total_funding"], 0)
        self.assertLess(result["stats"]["total_pnl"], -0.2)
        self.assertEqual(result["trades"][0]["execution_cost"], 0.2)
        self.assertGreater(result["trades"][0]["funding"], 0)


class BacktestRoutePersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_backtest_run_persists_roundtrip_fields(self):
        app_module = _load_app_module()

        idx = pd.date_range("2026-03-20 00:00:00+00:00", periods=8, freq="5min")
        df = pd.DataFrame(
            [
                {"open": 10.0, "high": 12.0, "low": 9.0, "close": 11.0, "volume": 1.0},
                {"open": 11.0, "high": 13.0, "low": 10.0, "close": 12.0, "volume": 2.0},
                {"open": 12.0, "high": 14.0, "low": 11.0, "close": 13.0, "volume": 3.0},
                {"open": 13.0, "high": 15.0, "low": 12.0, "close": 14.0, "volume": 4.0},
                {"open": 14.0, "high": 16.0, "low": 13.0, "close": 15.0, "volume": 5.0},
                {"open": 15.0, "high": 17.0, "low": 14.0, "close": 16.0, "volume": 6.0},
                {"open": 16.0, "high": 18.0, "low": 15.0, "close": 17.0, "volume": 7.0},
                {"open": 17.0, "high": 19.0, "low": 16.0, "close": 18.0, "volume": 8.0},
            ],
            index=idx,
        )

        original_run_file = app_module.RUNS_FILE
        original_legacy_run_file = app_module._LEGACY_RUNS_FILE
        original_state_db_file = getattr(app_module, "_STATE_DB_FILE", "")
        original_fetch_data = app_module._fetch_data
        original_run_backtest = app_module.run_backtest

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                app_module.RUNS_FILE = os.path.join(tmpdir, "runs.json")
                app_module._LEGACY_RUNS_FILE = os.path.join(tmpdir, "legacy-runs.json")
                app_module._STATE_DB_FILE = os.path.join(tmpdir, "cryptoforge_state.db")
                app_module._fetch_data = lambda **_: df

                def _fake_run_backtest(**kwargs):
                    return {
                        "status": "success",
                        "stats": {"total_trades": 1, "total_pnl": 123.45},
                        "monthly": [],
                        "yearly": [],
                        "day_of_week": [],
                        "trades": [
                            {
                                "entry_time": "2026-03-20 00:10:00",
                                "exit_time": "2026-03-20 00:15:00",
                                "entry_price": 12.0,
                                "exit_price": 13.0,
                                "pnl": 123.45,
                            }
                        ],
                        "equity": [],
                    }

                app_module.run_backtest = _fake_run_backtest

                payload = app_module.StrategyPayload(
                    run_name="Roundtrip",
                    symbol="BTCUSDT",
                    from_date="2026-03-20",
                    to_date="2026-03-20",
                    initial_capital=25000,
                    leverage=7,
                    trade_side="SHORT",
                    position_size_pct=42,
                    position_size_mode="fixed_qty",
                    fixed_qty=0.25,
                    stoploss_pct=3.5,
                    target_profit_pct=8.5,
                    trailing_sl_pct=1.25,
                    max_trades_per_day=2,
                    max_daily_loss=9.0,
                    fee_pct=0.05,
                    slippage_bps=3.0,
                    spread_bps=8.0,
                    funding_bps_per_8h=1.5,
                    compounding=True,
                    indicators=["EMA_20_5m"],
                    entry_conditions=[],
                    exit_conditions=[],
                    candle_interval="5m",
                )

                result = await app_module.api_run_backtest(payload)
                self.assertEqual(result["status"], "success")

                saved_runs = app_module._load_runs()
                self.assertEqual(len(saved_runs), 1)
                saved = saved_runs[0]
                self.assertEqual(saved["position_size_mode"], "fixed_qty")
                self.assertEqual(saved["fixed_qty"], 0.25)
                self.assertEqual(saved["compounding"], True)
                self.assertEqual(saved["fee_pct"], 0.05)
                self.assertEqual(saved["slippage_bps"], 3.0)
                self.assertEqual(saved["spread_bps"], 8.0)
                self.assertEqual(saved["funding_bps_per_8h"], 1.5)
                self.assertEqual(saved["max_daily_loss"], 9.0)
        finally:
            app_module.RUNS_FILE = original_run_file
            app_module._LEGACY_RUNS_FILE = original_legacy_run_file
            app_module._STATE_DB_FILE = original_state_db_file
            app_module._fetch_data = original_fetch_data
            app_module.run_backtest = original_run_backtest


if __name__ == "__main__":
    unittest.main()
