"""The ROI-by-trade chart draws CLOSED trades, with the running total.

Phil, 2026-08-17: "Why this showing red in between as all trades were in
profit?" The red bar was an OPEN BTC ladder that had sold one slice at its
Cascade TP -- a profit -- but the journal measures a slice against the average
of everything still held in that coin, so it read -0.59%. Not a loss, not
closed, and yet drawn on a card titled "every closed trade". And: "make this
as a cumulative profit as Binance does" -- a running net-P&L line over the
bars, on its own dollar axis.

And 03-Oct-2026, on ninety trades' worth of rotated coin names collapsing
into a grey smear: "See x axis... the letters are not visible anymore". A
label per bar cannot survive a dense book, so the axis groups consecutive
trades of the same coin and names the run, with months underneath — and
prints neither where it would not fit.

Runs the real renderer out of static/cryptoforge-app.js under Node.
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
function extract(name) {
  const start = src.indexOf('function ' + name + '(');
  if (start === -1) throw new Error('not found: ' + name);
  let i = src.indexOf('{', start), depth = 0;
  for (;; i++) {
    if (src[i] === '{') depth++;
    else if (src[i] === '}') { depth--; if (depth === 0) break; }
  }
  return src.slice(start, i + 1);
}
eval(extract('_escapeHtml'));
eval(extract('_cfJournalUsd'));
eval(extract('_cfJournalPct'));
eval(extract('_cfJournalRoiSvg'));

const trades = [
  { trade_id: 'SOLUSDT-1', coin: 'SOLUSDT', status: 'Closed', roi_pct: 0.8, pnl_usd: 0.40 },
  { trade_id: 'BTCUSDT-2', coin: 'BTCUSDT', status: 'Closed', roi_pct: 0.3, pnl_usd: 0.10 },
  // the open ladder that sold one slice: never a bar
  { trade_id: 'BTCUSDT-3', coin: 'BTCUSDT', status: 'Open', roi_pct: -0.585, pnl_usd: -0.0369 },
  { trade_id: 'PAXGUSDT-4', coin: 'PAXGUSDT', status: 'Closed', roi_pct: 0.2, pnl_usd: 0.05 },
];
const svg = _cfJournalRoiSvg(trades);
const rects = (svg.match(/<rect /g) || []).length;

// The axis, at the density that broke it: 90 trades in runs of one coin.
const coins = ['SOLUSDT', 'ETHUSDT', 'BTCUSDT'];
const many = [];
for (let i = 0; i < 90; i++) {
  const month = 5 + Math.floor(i / 30);
  many.push({
    trade_id: 'T-' + i,
    coin: coins[Math.floor(i / 30)],
    status: 'Closed',
    roi_pct: 0.5,
    pnl_usd: 0.1,
    date: '2026-0' + month + '-' + String((i % 28) + 1).padStart(2, '0'),
  });
}
const dense = _cfJournalRoiSvg(many);
// Text nodes that are neither an axis percentage nor a dollar figure. The
// month labels carry an escaped apostrophe (May &#39;26), so matching a raw
// one finds nothing — which is a test bug, not a missing label.
const labelsOf = (s) =>
  (s.match(/<text[^>]*>([^<]+)<\/text>/g) || [])
    .map((m) => m.replace(/<[^>]*>/g, ''))
    .filter((t) => !t.includes('%') && !t.includes('$'));
// One coin, 300 trades, fifty months: the thinning case.
const long = [];
for (let i = 0; i < 300; i++) {
  long.push({
    trade_id: 'L-' + i,
    coin: 'BTCUSDT',
    status: 'Closed',
    roi_pct: 0.4,
    pnl_usd: 0.1,
    date: '20' + String(24 + Math.floor(i / 60)) + '-' + String((i % 12) + 1).padStart(2, '0') + '-05',
  });
}
const longSvg = _cfJournalRoiSvg(long);
const reds = (svg.match(/var\(--red/g) || []).length;
const paths = (svg.match(/<path d="M/g) || []).length;
console.log(JSON.stringify({
  rects, reds, paths,
  hasOpenId: svg.includes('BTCUSDT-3'),
  lastCumulative: (svg.match(/cumulative net P&amp;L ([^ ]+) after 3 closed trades/) || [])[1] || null,
  emptyMessage: _cfJournalRoiSvg([{ status: 'Open', roi_pct: 1, pnl_usd: 1, coin: 'X', trade_id: 'x' }]),
  rotated: (dense.match(/rotate\(/g) || []).length,
  denseLabels: labelsOf(dense),
  denseBrackets: (dense.match(/stroke-width="2"/g) || []).length,
  longLabels: labelsOf(longSvg),
}));
"""


@unittest.skipIf(_NODE is None, "node is not installed")
class JournalRoiChartTests(unittest.TestCase):
    def setUp(self):
        proc = subprocess.run([_NODE, "-e", _HARNESS, _APP_JS], capture_output=True, text=True, timeout=60)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.out = json.loads(proc.stdout.strip().splitlines()[-1])

    def test_only_closed_trades_become_bars(self):
        self.assertEqual(self.out["rects"], 3)
        self.assertFalse(self.out["hasOpenId"], "the open ladder is not drawn")
        self.assertEqual(self.out["reds"], 0, "and so nothing is red when every closed trade won")

    def test_the_running_total_is_drawn_and_adds_up(self):
        self.assertEqual(self.out["paths"], 1, "one cumulative line")
        self.assertEqual(self.out["lastCumulative"], "$0.55")

    def test_an_all_open_book_says_so_instead_of_drawing_nothing(self):
        self.assertIn("No closed trades yet", self.out["emptyMessage"])

    def test_ninety_trades_do_not_print_ninety_labels(self):
        """The smear. One label per bar at 6px a bar is unreadable by
        construction, whatever it is rotated to."""
        self.assertEqual(self.out["rotated"], 0, "nothing on this axis is rotated any more")
        self.assertLessEqual(len(self.out["denseLabels"]), 12, "an axis you can read has a handful of labels")

    def test_the_axis_names_the_coin_runs_and_the_months(self):
        labels = self.out["denseLabels"]
        for coin in ("SOL", "ETH", "BTC"):
            self.assertIn(coin, labels, "each stretch of trades says which coin it was")
        self.assertTrue(any("26" in text and "&#39;" in text for text in labels), "and roughly when it happened")
        self.assertEqual(self.out["denseBrackets"], 3, "one bracket per run, even where the name does not fit")

    def test_month_labels_thin_out_rather_than_collide(self):
        """300 trades across four years: printing every month start would be
        the same smear in a different alphabet."""
        months = [text for text in self.out["longLabels"] if "&#39;" in text]
        self.assertGreater(len(months), 2, "the axis still says when")
        self.assertLessEqual(len(months), 12, "but not every month of four years")


if __name__ == "__main__":
    unittest.main()
