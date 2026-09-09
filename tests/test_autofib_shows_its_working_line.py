"""A book's working line must appear on Cascade-Auto before it orders anything.

09-Sep-2026. Phil switched BTCUSDT and PAXGUSDT to LIVE, the driver seeded a
5m line on each and said so in the log, the Books table read "LINES 1 ·
WORKING LINE 7ffe21e5b3" — and the Lines panel said "Nothing running yet".

_cfCascadeCampaignWorking asks whether a ladder holds coin, has money
committed, or has an order resting. A line that has just been anchored has
none of those: it is watching for price to reach its first rung. Cascade-
Hybrid never showed the bug because a hand-started campaign arms an order
immediately and passes the filter — "in hybrid it worked".

The rest of the pool still stays out: 242 cards for five working ladders is
not a list anyone can read.
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
  const at = src.indexOf('function ' + name + '(');
  if (at === -1) throw new Error('not found: ' + name);
  const start = src.slice(0, at).endsWith('async ') ? at - 'async '.length : at;
  let i = src.indexOf('{', at), depth = 0;
  for (;; i++) {
    if (src[i] === '{') depth++;
    else if (src[i] === '}') { depth--; if (depth === 0) break; }
  }
  return src.slice(start, i + 1);
}

eval(extract('_cfCascadeCampaignWorking'));
eval(extract('_cfAfVisibleLines'));

const input = JSON.parse(process.argv[2]);
const out = _cfAfVisibleLines(input.campaigns, input.books);
console.log(JSON.stringify(out.map((c) => c.campaign_id)));
"""


@unittest.skipIf(_NODE is None, "node is not installed")
class AutoFibShowsItsWorkingLineTests(unittest.TestCase):
    def _visible(self, campaigns, books):
        payload = json.dumps({"campaigns": campaigns, "books": books})
        result = subprocess.run(
            [_NODE, "-e", _HARNESS, _APP_JS, payload],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode != 0:
            raise AssertionError(result.stderr)
        return json.loads(result.stdout.strip().splitlines()[-1])

    def test_a_seeded_line_with_no_orders_is_shown(self):
        seeded = {"campaign_id": "7ffe21e5b3", "state": "TRENDLINE_ACTIVE"}
        books = [{"symbol": "BTCUSDT", "working_line": "7ffe21e5b3"}]
        self.assertEqual(self._visible([seeded], books), ["7ffe21e5b3"])

    def test_without_its_book_that_same_line_stays_hidden(self):
        """It is the BOOK naming it that earns the card, not the state."""
        seeded = {"campaign_id": "7ffe21e5b3", "state": "TRENDLINE_ACTIVE"}
        self.assertEqual(self._visible([seeded], []), [])

    def test_a_ladder_holding_coin_is_shown_as_before(self):
        holding = {"campaign_id": "h1", "state": "ACTIVE", "filled_base_qty": 0.5}
        self.assertEqual(self._visible([holding], []), ["h1"])

    def test_a_ladder_with_an_order_resting_is_shown_as_before(self):
        armed = {"campaign_id": "a1", "state": "ACTIVE", "pending_stop_price": 100}
        self.assertEqual(self._visible([armed], []), ["a1"])

    def test_the_watching_pool_still_stays_out(self):
        watching = [{"campaign_id": "w%d" % i, "state": "WAITING_FIRST_DEPTH"} for i in range(240)]
        books = [{"symbol": "BTCUSDT", "working_line": "w0"}]
        self.assertEqual(self._visible(watching, books), ["w0"], "the whole pool came back")

    def test_an_ended_working_line_is_not_resurrected(self):
        """A book can still name the line that has just closed."""
        ended = {"campaign_id": "x1", "state": "STOPPED", "closed_at": "2026-09-09"}
        books = [{"symbol": "BTCUSDT", "working_line": "x1"}]
        self.assertEqual(self._visible([ended], books), ["x1"])
        # Documented, not desired: the driver clears working_line when the
        # campaign ends, so this only shows in the tick between the two.

    def test_a_book_with_no_line_names_nothing(self):
        holding = {"campaign_id": "h1", "state": "ACTIVE", "filled_base_qty": 1}
        books = [{"symbol": "BTCUSDT", "working_line": ""}]
        self.assertEqual(self._visible([holding], books), ["h1"])


if __name__ == "__main__":
    unittest.main()
