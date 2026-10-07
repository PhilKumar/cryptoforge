"""The Lines panel and the engine's alert must count the same world.

Phil, 07-Oct-2026: "You told 16 but it shows only 10". Both were right. The
page listed WORKING ladders — holding coin or with an order armed — while the
engine's campaign-count alert counts every OPEN campaign, and the six that
were open with nothing armed appeared in neither number. The page said
"85 more are watching or finished", which hid the difference inside a word.

So the panel now prints open-but-waiting as its own figure and the total open
alongside it, and a book's header stops printing "7 campaigns (8 live)" — two
different populations in one sentence.

Runs the real renderers out of static/cryptoforge-app.js under Node.
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
eval(extract('_cfCascadeCampaignWorking'));
eval(extract('_cfCascadeCampaignHasEnded'));
eval(extract('_cfAfVisibleLines'));
eval(extract('_cfCascadeStackCount'));

// Phil's real shape on 07-Oct: 16 open (10 of them working), 79 finished.
const camps = [];
for (let i = 0; i < 10; i++) camps.push({ campaign_id: 'w' + i, state: 'TRENDLINE_ACTIVE', filled_base_qty: 1 });
for (let j = 0; j < 6; j++) camps.push({ campaign_id: 'i' + j, state: 'TRENDLINE_ACTIVE' });
for (let k = 0; k < 79; k++) camps.push({ campaign_id: 'd' + k, state: 'MOTHER_BROKEN', closed_at: 'x' });

const working = _cfAfVisibleLines(camps, []);
const idle = camps.filter((c) => !_cfCascadeCampaignHasEnded(c) && !_cfCascadeCampaignWorking(c));
const ended = camps.filter(_cfCascadeCampaignHasEnded);

console.log(JSON.stringify({
  working: working.length,
  idle: idle.length,
  ended: ended.length,
  openTotal: working.length + idle.length,
  headers: {
    partial: _cfCascadeStackCount(7, { active_count: 8, live_count: 8 }),
    all: _cfCascadeStackCount(3, { active_count: 3, live_count: 3 }),
    mixed: _cfCascadeStackCount(2, { active_count: 5, live_count: 3 }),
    single: _cfCascadeStackCount(1, { active_count: 1, live_count: 0 }),
  },
}));
"""


@unittest.skipIf(_NODE is None, "node is not installed")
class LinesCountTests(unittest.TestCase):
    def setUp(self):
        proc = subprocess.run([_NODE, "-e", _HARNESS, _APP_JS], capture_output=True, text=True, timeout=60)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.out = json.loads(proc.stdout.strip().splitlines()[-1])

    def test_working_plus_waiting_is_what_the_alert_counts(self):
        """The reconciliation. 10 shown + 6 idle = the 16 the engine alerts on."""
        self.assertEqual(self.out["working"], 10)
        self.assertEqual(self.out["idle"], 6)
        self.assertEqual(self.out["openTotal"], 16)

    def test_finished_is_counted_apart_from_waiting(self):
        """They used to share one number, which is what hid the difference."""
        self.assertEqual(self.out["ended"], 79)
        self.assertNotEqual(self.out["ended"], self.out["idle"] + self.out["ended"])

    def test_a_book_header_never_mixes_shown_with_open(self):
        self.assertEqual(self.out["headers"]["partial"], "7 of 8 campaigns open (all live)")
        self.assertNotIn("(8 live)", self.out["headers"]["partial"])

    def test_a_header_with_nothing_hidden_stays_plain(self):
        self.assertEqual(self.out["headers"]["all"], "3 campaigns open (all live)")
        self.assertEqual(self.out["headers"]["single"], "1 campaign open")

    def test_a_paper_and_live_mix_still_says_how_many_are_live(self):
        self.assertEqual(self.out["headers"]["mixed"], "2 of 5 campaigns open (3 live)")


if __name__ == "__main__":
    unittest.main()
