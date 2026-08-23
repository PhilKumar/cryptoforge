"""
Where a buy or sell arrow hangs, run out of the real bundle.

Phil, 2026-08-23: "The arrows are not showing correctly.. The buy has to be
below the candle not above the candle."

Both renderers anchored the marker to the FILL PRICE. On a normal fill that
puts it inside the candle body; when the price is not one the bar traded it
leaves the bar entirely. His ETHUSDT buy was booked at 2,390.25 — paper fills
at the limit cap, the pessimistic end — on a 10:40 bar whose high was 2,377.21,
so the arrow floated thirteen dollars clear of its own candle.

The marker now hangs off the BAR: a buy under the bar's low, a sell over its
high, falling back to the price only when the bar is not in the window.
"""

import json
import os
import shutil
import subprocess
import unittest

_HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_APP_JS = os.path.join(_HERE, "static", "cryptoforge-app.js")
_NODE = shutil.which("node")

_HARNESS = r"""
const fs = require('fs');
const src = fs.readFileSync(process.argv[1], 'utf8');
const start = src.indexOf('function _cfChartBarEdge(');
if (start < 0) throw new Error('MISSING FUNCTION: _cfChartBarEdge');
const end = src.indexOf('function _cfChartCanvasMarkers', start);
eval(src.slice(start, end));
const a = JSON.parse(process.argv[2]);
process.stdout.write(JSON.stringify(_cfChartBarEdge(a.candles, a.t, a.price, a.side)));
"""

# The real 5m bars around Phil's fill, from Binance.
BARS = [
    {"t": 1787461200, "o": 2378.48, "h": 2380.90, "l": 2363.26, "c": 2372.60},  # 10:35
    {"t": 1787461800, "o": 2372.58, "h": 2377.21, "l": 2365.79, "c": 2366.41},  # 10:40 — the fill bar
    {"t": 1787462100, "o": 2366.49, "h": 2379.60, "l": 2355.71, "c": 2379.43},  # 10:45
]
FILL_TS = 1787461800
FILL_PRICE = 2390.25  # the limit cap — ABOVE the bar's own high


@unittest.skipUnless(_NODE, "node is not installed")
class ChartMarkerAnchorTests(unittest.TestCase):
    def _edge(self, t, price, side, candles=None):
        proc = subprocess.run(
            [
                _NODE,
                "-e",
                _HARNESS,
                "--",
                _APP_JS,
                json.dumps({"candles": BARS if candles is None else candles, "t": t, "price": price, "side": side}),
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return json.loads(proc.stdout)

    def test_a_buy_hangs_off_its_bars_low_not_the_fill_price(self):
        self.assertEqual(self._edge(FILL_TS, FILL_PRICE, "buy"), 2365.79)

    def test_the_fill_price_really_was_above_that_bar(self):
        """The exact condition that made the arrow float: not a hypothetical."""
        self.assertGreater(FILL_PRICE, 2377.21, "the booked price is above the bar's high")

    def test_a_sell_hangs_off_its_bars_high(self):
        self.assertEqual(self._edge(FILL_TS, 2417.48, "sell"), 2377.21)

    def test_it_picks_the_bar_the_fill_falls_INSIDE_not_the_next_one(self):
        """A fill at 10:43 belongs to the 10:40 bar, which opened before it."""
        self.assertEqual(self._edge(FILL_TS + 180, 2390.25, "buy"), 2365.79)

    def test_a_fill_older_than_the_window_falls_back_to_its_price(self):
        """A roll-up window can start after an old fill — never drop the marker."""
        self.assertEqual(self._edge(BARS[0]["t"] - 3600, 2390.25, "buy"), 2390.25)

    def test_no_candles_at_all_falls_back_to_the_price(self):
        self.assertEqual(self._edge(FILL_TS, 2390.25, "buy", candles=[]), 2390.25)

    def test_a_bar_with_a_broken_low_falls_back_rather_than_drawing_at_zero(self):
        bad = [{"t": FILL_TS, "o": 1, "h": 1, "l": 0, "c": 1}]
        self.assertEqual(self._edge(FILL_TS, 2390.25, "buy", candles=bad), 2390.25)
