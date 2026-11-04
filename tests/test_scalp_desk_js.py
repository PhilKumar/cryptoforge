"""The Scalp desk's page half, run under Node: the renderer brought from
PhilForge (static/cryptoforge-scalp-chart.js) and the desk that drives it
(static/cryptoforge-scalp-desk.js).

What changed in the move and must stay changed:

* the bar clock is 24/7 -- PhilForge's is NSE's 09:15-15:30, which on a coin
  would put no forming bar and no countdown on screen for 18 hours a day;
* prices keep a coin's decimals (XRP is not BTC);
* the forming bar grows from the Scalp engine's live price for THIS coin and
  never another's (Delta calls it BTCUSD, Binance BTCUSDT: the same coin).

And the page wiring: B and S are the form's own order (cfSubmitScalp), and no
control carries an inline handler -- the CSP forbids them (script-src-attr
'none'), so one would be a dead button.
"""

import json
import os
import re
import shutil
import subprocess
import unittest

_HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CHART = os.path.join(_HERE, "static", "cryptoforge-scalp-chart.js")
_DESK = os.path.join(_HERE, "static", "cryptoforge-scalp-desk.js")
_HTML = os.path.join(_HERE, "strategy.html")
_NODE = shutil.which("node")

_HARNESS = r"""
const fs = require('fs');
const noop = () => {};
const el = { addEventListener: noop, classList: { contains: () => false, toggle: noop, add: noop, remove: noop } };
global.window = global;
global.document = {
  readyState: 'complete', hidden: false, body: el, documentElement: { getAttribute: () => 'dark' },
  addEventListener: noop, getElementById: () => null, querySelector: () => null, querySelectorAll: () => [],
};
global.localStorage = { getItem: () => null, setItem: noop };
global.setInterval = () => 0;
global.setTimeout = () => 0;
global.addEventListener = noop;
global.matchMedia = () => ({ matches: false });
var _cfLatestScalpStatus = {};
eval(fs.readFileSync(process.argv[1], 'utf8'));
eval(fs.readFileSync(process.argv[2], 'utf8'));

const out = {};
// 24/7: any moment is inside a bar, on the epoch grid.
const now = 1_800_000_123;
const w = pfChartBarWindow(now, 300);
out.win = w;
out.winDayOpen = w.open % 86400;
// Saturday 03:00 UTC: PhilForge's clock said "no session".
out.weekendWin = !!pfChartBarWindow(1_799_978_400 + 3 * 3600, 60);
out.prices = [pfChartPrice(78123.456, true), pfChartPrice(2.3456789, true), pfChartPrice(0.12345678, true), pfChartPrice(78000, false)];
out.money = _pfChartInr(1234.5);
out.keys = [cfScalpDesk.symKey('BTCUSDT'), cfScalpDesk.symKey('btcusd'), cfScalpDesk.symKey('GOLD')];

// The forming bar.
const tf = 60;
const nowSec = Math.floor(Date.now() / 1000);
const winStart = Math.floor(nowSec / tf) * tf;
const prev = { t: winStart - tf, o: 100, h: 101, l: 99, c: 100 };
_cfLatestScalpStatus = { entry_controls: { symbol: 'BTCUSD', price: 105 }, open_trades: [] };
const data = { symbol: 'BTCUSDT', timeframe: '1m', candles: [prev] };
out.grewNew = cfScalpDesk.applyLiveBar(data, 'k');
out.newBar = data.candles[data.candles.length - 1];
_cfLatestScalpStatus = { entry_controls: { symbol: 'BTCUSD', price: 107 }, open_trades: [] };
out.grewAgain = cfScalpDesk.applyLiveBar(data, 'k');
out.updated = data.candles[data.candles.length - 1];
out.count = data.candles.length;
// Another coin's price never moves this chart.
_cfLatestScalpStatus = { entry_controls: { symbol: 'ETHUSDT', price: 2000 }, open_trades: [] };
out.otherCoin = cfScalpDesk.applyLiveBar({ symbol: 'BTCUSDT', timeframe: '1m', candles: [prev] }, 'k2');
// ...but an open trade's mark on this coin does.
_cfLatestScalpStatus = { entry_controls: {}, open_trades: [{ symbol: 'BTCUSD', current_price: 110 }] };
const viaTrade = { symbol: 'BTCUSDT', timeframe: '1m', candles: [prev] };
out.viaTrade = cfScalpDesk.applyLiveBar(viaTrade, 'k3') && viaTrade.candles[viaTrade.candles.length - 1].c;

// Supertrend and RSI from the chart's own candles.
const bars = [];
for (let i = 0; i < 60; i++) {
  const p = i < 30 ? 100 + i : 130 - (i - 30) * 2;
  bars.push({ t: i * 60, o: p, h: p + 1, l: p - 1, c: p });
}
out.stDirs = cfScalpDesk.supertrendRuns(bars, 10, 2).map(r => r.dir);
const rsi = cfScalpDesk.rsiSeries(bars, 14);
out.rsiLen = rsi.length;
out.rsiRange = rsi.every(p => p.v >= 0 && p.v <= 100);
console.log(JSON.stringify(out));
"""


@unittest.skipUnless(_NODE, "node is not installed")
class ScalpDeskJsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        proc = subprocess.run(
            [_NODE, "-e", _HARNESS, _CHART, _DESK], capture_output=True, text=True, timeout=60, cwd=_HERE
        )
        if proc.returncode != 0:
            raise AssertionError(proc.stderr or proc.stdout)
        cls.out = json.loads(proc.stdout.strip().splitlines()[-1])

    def test_the_bar_clock_runs_around_the_clock(self):
        w = self.out["win"]
        self.assertEqual(w["start"], 1_800_000_000)
        self.assertEqual(w["close"], 1_800_000_300)
        self.assertEqual(self.out["winDayOpen"], 0)
        self.assertTrue(self.out["weekendWin"])

    def test_prices_keep_a_coins_decimals_and_money_is_dollars(self):
        self.assertEqual(self.out["prices"], ["78,123.46", "2.3457", "0.123457", "78,000"])
        self.assertTrue(self.out["money"].startswith("$"))

    def test_one_name_per_coin(self):
        self.assertEqual(self.out["keys"], ["BTCUSD", "BTCUSD", "PAXGUSD"])

    def test_the_forming_bar_grows_from_this_coins_live_price(self):
        self.assertTrue(self.out["grewNew"])
        bar = self.out["newBar"]
        self.assertEqual((bar["o"], bar["h"], bar["l"], bar["c"]), (105, 105, 105, 105))
        self.assertTrue(self.out["grewAgain"])
        self.assertEqual(self.out["count"], 2)
        self.assertEqual((self.out["updated"]["h"], self.out["updated"]["c"]), (107, 107))
        self.assertFalse(self.out["otherCoin"])
        self.assertEqual(self.out["viaTrade"], 110)

    def test_supertrend_flips_with_the_trend_and_rsi_is_bounded(self):
        self.assertEqual(self.out["stDirs"][0], 1)
        self.assertIn(-1, self.out["stDirs"])
        self.assertGreater(self.out["rsiLen"], 0)
        self.assertTrue(self.out["rsiRange"])


class ScalpDeskPageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(_HTML, encoding="utf-8") as fh:
            cls.html = fh.read()
        with open(_DESK, encoding="utf-8") as fh:
            cls.desk = fh.read()

    def _dock(self):
        start = self.html.index('id="cf-scalp-dock"')
        return self.html[start : self.html.index("</section>", start)]

    def test_the_dock_sits_beside_the_form(self):
        desk = self.html[self.html.index('<div class="cf-scalp-desk">') : self.html.index("<!-- /cf-scalp-desk -->")]
        self.assertIn('id="cf-scalp-dock"', desk)
        self.assertIn("cf-scalp-form-card", desk)
        self.assertIn('id="cf-scalp-event-log"', desk)

    def test_scripts_load_after_the_app_renderer_first(self):
        app = self.html.index("/static/cryptoforge-app.js")
        chart = self.html.index("/static/cryptoforge-scalp-chart.js")
        desk = self.html.index("/static/cryptoforge-scalp-desk.js")
        self.assertLess(app, chart)
        self.assertLess(chart, desk)
        self.assertIn("/static/cryptoforge-scalp-desk.css", self.html)

    def test_no_inline_handlers_in_the_dock(self):
        self.assertIsNone(re.search(r"\son[a-z]+=", self._dock()))

    def test_b_and_s_are_the_forms_own_order(self):
        self.assertIn("cfSubmitScalp(direction)", self.desk)
        self.assertIn("cfExitScalpTrade(", self.desk)
        dock = self._dock()
        for ident in ("cf-desk-ticket-buy", "cf-desk-ticket-sell"):
            tag = dock[dock.rindex("<", 0, dock.index(ident)) : dock.index(">", dock.index(ident))]
            self.assertIn("read-only-hide", tag, f"{ident} must hide for a viewer")


if __name__ == "__main__":
    unittest.main()
