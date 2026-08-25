"""The fills table's clock and its P&L label, run out of the real bundle.

Phil, 2026-08-24, screenshot of Recent Filled Orders: "Are these real or
paper? Analyze and let me know what is this PL amount actually...."

Two lies on one screen. The broker stamps fills in UTC with an explicit
+00:00, and _getTradeDateParts regexed the digits out and appended a literal
' IST' — his 16:51 exit read "11:21:21 IST", 5h30m early on every broker row.
And the P&L cell said "net realized" under a FIFO figure computed across the
WHOLE account's coins: his 27-cent cascade exit showed +$4.25 because the
sell was matched against BTC bought on 13-14 Aug, an inventory gain that no
strategy earned that day.

The fix converts only stamps that declare their zone (or raw epochs); the
engine's own naive stamps are already IST and must pass through unchanged.
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
function grab(name, stop) {
  const start = src.indexOf(name);
  if (start < 0) throw new Error('MISSING FUNCTION: ' + name);
  const end = src.indexOf(stop, start + name.length);
  if (end < 0) throw new Error('MISSING STOP: ' + stop);
  eval.call(globalThis, src.slice(start, end));
}
grab('function _cfIstDateParts(', 'function _tradeDateSortValue');
grab('function _tradeDateSortValue(', 'function _fmtTradeDateTime');
grab('function _cfFilledOrderPnlSubtext(', 'let _portfolioSummary');
const q = JSON.parse(process.argv[2]);
let out;
if (q.fn === 'parts') out = _getTradeDateParts(q.raw);
else if (q.fn === 'sort') out = _tradeDateSortValue(q.raw);
else out = _cfFilledOrderPnlSubtext(q.order, q.hasPnl);
process.stdout.write(JSON.stringify(out));
"""


@unittest.skipUnless(_NODE, "node is not installed")
class FilledOrdersTimeAndLabelTests(unittest.TestCase):
    def _run(self, **q):
        proc = subprocess.run(
            [_NODE, "-e", _HARNESS, "--", _APP_JS, json.dumps(q)],
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return json.loads(proc.stdout)

    # ── the clock ─────────────────────────────────────────────────────

    def test_the_broker_utc_stamp_becomes_real_ist(self):
        """The exact stamps behind Phil's screenshot: shown 11:21, lived 16:51."""
        parts = self._run(fn="parts", raw="2026-08-23T11:21:20.049000+00:00")
        self.assertEqual(parts["time"], "16:51:20 IST")
        self.assertEqual(parts["date"], "23 Aug 2026")
        parts = self._run(fn="parts", raw="2026-08-23T11:21:21.700000+00:00")
        self.assertEqual(parts["time"], "16:51:21 IST")

    def test_a_zulu_stamp_converts_too(self):
        parts = self._run(fn="parts", raw="2026-08-23T11:21:20Z")
        self.assertEqual(parts["time"], "16:51:20 IST")

    def test_the_engines_own_naive_stamp_passes_through_untouched(self):
        """cascade closed_at is already IST; converting it would break every
        engine table to fix one."""
        parts = self._run(fn="parts", raw="2026-08-24 20:05:04")
        self.assertEqual(parts["label"], "24 Aug 2026, 20:05:04 IST")

    def test_an_epoch_stamp_lands_in_ist(self):
        # 1787484080 s = 2026-08-23 11:21:20 UTC — the same fill, in seconds and ms.
        for raw in ("1787484080", "1787484080549"):
            parts = self._run(fn="parts", raw=raw)
            self.assertEqual(parts["time"][:5], "16:51", raw)

    def test_a_non_ist_offset_is_honoured_not_pattern_matched(self):
        """The rule is 'believe the offset', not 'assume UTC'."""
        parts = self._run(fn="parts", raw="2026-08-23T16:51:20+05:30")
        self.assertEqual(parts["time"], "16:51:20 IST")

    def test_sorting_reads_the_offset_instead_of_the_digits(self):
        utc = self._run(fn="sort", raw="2026-08-23T11:21:20+00:00")
        ist = self._run(fn="sort", raw="2026-08-23T16:51:20+05:30")
        self.assertEqual(utc, ist)  # same instant, whatever zone wrote it down

    # ── the label ─────────────────────────────────────────────────────

    def test_the_pnl_cell_says_what_the_number_is(self):
        self.assertEqual(self._run(fn="sub", order={}, hasPnl=True), "account FIFO net")
        self.assertEqual(self._run(fn="sub", order={"pnl_status": "broker"}, hasPnl=True), "broker net")

    def test_unrealized_states_keep_their_words(self):
        self.assertEqual(self._run(fn="sub", order={"pnl_status": "entry"}, hasPnl=False), "entry fill")
        self.assertEqual(self._run(fn="sub", order={}, hasPnl=False), "not realized")
