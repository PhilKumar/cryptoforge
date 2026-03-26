import os
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


if __name__ == "__main__":
    unittest.main()
