"""
The V-Rule chart's stage line and line labels, run out of the real bundle.

Phil, 2026-08-23, on the V-Rule chart: "I am completely confused on how I need
to see the chart.. My brain is draining." The chart drew five lines and two
different percentages and left the READER to work out which stage the setup was
in — and worse, it drew the white buy line as soon as the reference was
touched, so it showed an order that did not exist. These pin what each stage
says.

Also pinned: that `_cfR37LineLabel` EXISTS. It was deleted by accident while
removing the paragraph it sat next to, leaving the renderers calling a function
that was gone — `node --check` passes that happily, because a missing reference
is not a syntax error. Only running it found it.
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
function extract(name, until) {
  const start = src.indexOf('function ' + name + '(');
  if (start < 0) throw new Error('MISSING FUNCTION: ' + name);
  const end = src.indexOf(until, start);
  if (end < 0) throw new Error('could not bound ' + name);
  return src.slice(start, end);
}
const _escapeHtml = v => String(v);
eval(extract('_cfR37LineLabel', '// ONE line saying which'));
eval(extract('_cfR37Stage', 'function _cfR37ChartHtml'));
const strip = h => String(h).replace(/<[^>]+>/g, '').trim();
const arg = JSON.parse(process.argv[2]);
const out = arg.fn === 'label'
  ? _cfR37LineLabel(arg.d, arg.price, arg.fallback)
  : strip(_cfR37Stage(arg.d, arg.r));
process.stdout.write(JSON.stringify(out));
"""

REF = 75673.85
LOW = 75545.67
ENTRY = 76534.25
R = {"reference": REF, "lowest_low": LOW, "touch_when": "23 Aug 10:45", "trigger_when": "23 Aug 16:10"}


@unittest.skipUnless(_NODE, "node is not installed")
class VRuleChartStageTests(unittest.TestCase):
    def _run(self, payload):
        proc = subprocess.run(
            [_NODE, "-e", _HARNESS, "--", _APP_JS, json.dumps(payload)],
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return json.loads(proc.stdout)

    def _stage(self, d, r=None):
        return self._run({"fn": "stage", "d": d, "r": R if r is None else r})

    # ── the three stages ──────────────────────────────────────────

    def test_waiting_says_a_close_is_needed_and_names_no_order(self):
        out = self._stage({"reference_price": REF})
        self.assertIn("WAITING", out)
        self.assertIn("CLOSE below 75,673.85", out)
        self.assertIn("only touched it", out)
        self.assertNotIn("76,534.25", out)  # there is no buy yet — do not show one

    def test_armed_names_the_buy_and_the_low_it_follows(self):
        out = self._stage({"reference_price": REF, "entry_price": ENTRY})
        self.assertIn("ARMED", out)
        self.assertIn("76,534.25", out)
        self.assertIn("75,545.67", out)
        self.assertNotIn("WAITING", out)

    def test_in_names_the_target_and_says_the_reference_is_frozen(self):
        out = self._stage(
            {"reference_price": REF, "entry_price": ENTRY, "avg_entry_price": ENTRY, "tp_price": 77522.83}
        )
        self.assertIn("IN at", out)
        self.assertIn("77,522.83", out)
        self.assertIn("frozen", out)

    def test_a_chart_with_no_reference_at_all_says_nothing(self):
        """Nothing to report beats a half-sentence: neither side has the line."""
        self.assertEqual(self._stage({}, {}), "")

    def test_the_reference_is_taken_from_either_side_of_the_payload(self):
        """r37.reference and the top-level reference_price are the same number;
        the stage line must read whichever one arrived."""
        self.assertIn("75,673.85", self._stage({}, R))
        self.assertIn("75,673.85", self._stage({"reference_price": REF}, {}))

    # ── the line labels ───────────────────────────────────────────

    def _label(self, d, price, fallback):
        return self._run({"fn": "label", "d": d, "price": price, "fallback": fallback})

    def test_the_reference_line_is_named_rather_than_numbered(self):
        """The gutter fits about twenty characters. A longer label is sliced off
        at the canvas edge — Phil got "a CLOSE below this line" with no name in
        front of it — so the line carries its NAME and the stage sentence below
        carries the meaning."""
        out = self._label({"reference_price": REF}, REF, "2 (75,673.85)")
        self.assertIn("REFERENCE", out)
        self.assertNotIn("2 (", out, "the bare fib number is what it replaces")
        self.assertLessEqual(len(out), 22, "longer than this and the gutter cuts it off")

    def test_the_explaining_happens_in_the_stage_line_not_on_the_gutter(self):
        self.assertIn("CLOSE below 75,673.85", self._stage({"reference_price": REF}))

    def test_every_other_level_keeps_its_plain_label(self):
        self.assertEqual(self._label({"reference_price": REF}, 75002, "2 (75,002)"), "2 (75,002)")

    def test_a_cascade_chart_sends_no_reference_and_is_untouched(self):
        self.assertEqual(self._label({}, 75002, "2 (75,002)"), "2 (75,002)")
