"""The ROI-by-trade chart draws CLOSED trades, with the running total.

Phil, 2026-08-17: "Why this showing red in between as all trades were in
profit?" The red bar was an OPEN BTC ladder that had sold one slice at its
Cascade TP -- a profit -- but the journal measures a slice against the average
of everything still held in that coin, so it read -0.59%. Not a loss, not
closed, and yet drawn on a card titled "every closed trade". And: "make this
as a cumulative profit as Binance does" -- a running net-P&L line over the
bars, on its own dollar axis.

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
const reds = (svg.match(/var\(--red/g) || []).length;
const paths = (svg.match(/<path d="M/g) || []).length;
console.log(JSON.stringify({
  rects, reds, paths,
  hasOpenId: svg.includes('BTCUSDT-3'),
  lastCumulative: (svg.match(/cumulative net P&amp;L ([^ ]+) after 3 closed trades/) || [])[1] || null,
  emptyMessage: _cfJournalRoiSvg([{ status: 'Open', roi_pct: 1, pnl_usd: 1, coin: 'X', trade_id: 'x' }]),
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


if __name__ == "__main__":
    unittest.main()
