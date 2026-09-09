"""What the engines are allowed to do ON the single event loop.

There is ONE uvicorn worker, so anything slow inside a monitor tick is the
whole site being slow. On 2026-09-09 at 12:37 IST every endpoint returned 504
and Phil's "Got it" hung; py-spy on the live worker showed the loop inside
V-Rule's per-bar conversion (pandas' own datetime iterator) and inside
Cascade-Auto re-rendering the ENTIRE status for a wallet cap that had moved a
cent. /api/health was taking 2.4-7.6s.
"""

import ast
import os
import unittest

_HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _source(*parts):
    with open(os.path.join(_HERE, *parts), encoding="utf-8") as handle:
        return handle.read()


class VRuleBarsOffTheLoopTests(unittest.TestCase):
    def test_bars_from_df_is_awaited_in_a_thread(self):
        """It walks the whole replay window; pandas iteration is not cheap."""
        tree = ast.parse(_source("engine", "vrule_live.py"))
        calls = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                target = node.func
                name = getattr(target, "id", None) or getattr(target, "attr", None)
                if name == "bars_from_df":
                    calls.append(node)
                if name == "to_thread" and node.args:
                    first = node.args[0]
                    if getattr(first, "id", "") == "bars_from_df":
                        calls.append("threaded")
        self.assertIn("threaded", calls, "bars_from_df is built on the event loop")

    def test_the_window_load_and_scan_stay_off_the_loop_too(self):
        source = _source("engine", "vrule_live.py")
        self.assertIn("asyncio.to_thread(self._load_window", source)
        self.assertIn("asyncio.to_thread(self._scan_structure", source)


class WalletCapDoesNotRenderTheWholeStatusTests(unittest.TestCase):
    """Cascade-Auto re-set its cap every tick as the purse drifted."""

    def setUp(self):
        from engine.cascade import CascadeEngine

        self.engine = CascadeEngine.__new__(CascadeEngine)
        self.engine.capital_groups = {}
        self.engine.primary_broker_name = "binance"
        self.emits = []
        self.engine.on_update = lambda status: self.emits.append(status)

    def test_setting_a_group_still_publishes_by_default(self):
        """A hand edit in the UI must repaint the page."""
        self.engine.get_status = lambda: {"campaigns": []}
        self.engine.group_committed_usd = lambda *a, **k: 0.0
        self.engine.set_capital_group("BTCUSDT", 500.0)
        self.assertEqual(len(self.emits), 1)

    def test_a_tick_can_set_it_without_rendering_anything(self):
        def explode():
            raise AssertionError("the whole status was rendered inside a tick")

        self.engine.get_status = explode
        self.engine.group_committed_usd = lambda *a, **k: 0.0
        self.engine.set_capital_group("BTCUSDT", 500.0, emit=False)
        self.assertEqual(self.engine.capital_groups and len(self.engine.capital_groups), 1)
        self.assertEqual(self.emits, [])

    def test_clearing_a_group_from_a_tick_is_silent_too(self):
        def explode():
            raise AssertionError("the whole status was rendered inside a tick")

        self.engine.get_status = explode
        self.engine.group_committed_usd = lambda *a, **k: 0.0
        self.engine.set_capital_group("BTCUSDT", 500.0, emit=False)
        self.engine.set_capital_group("BTCUSDT", 0, emit=False)
        self.assertEqual(self.emits, [])

    def test_the_auto_driver_asks_for_the_silent_form(self):
        source = _source("engine", "auto_cascade_fib.py")
        self.assertIn("emit=False", source, "the wallet cap still emits a full status per tick")


if __name__ == "__main__":
    unittest.main()
