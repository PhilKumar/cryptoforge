"""
The Paper Journal on the Cascade-Auto page, and the rename that came with it.

Phil, 2026-08-24: "Add paper journal in Cascade_Auto same like other strategies
and rename Cascade_Auto to Cascade-Auto".

Closed Campaigns is one row per LINE. A journal everywhere else on this site is
one row per ROUND — every buy and every target — so this panel is the Cascade
page's own Closed Rounds ledger under this page's ids, not a second renderer.

Two things had to give way for that:
  · the ledger kept its filter, its page number and its rows in three MODULE
    globals, so a second table on another page would have fought the first for
    all three. They are per-table now, the same shape as _cfClosedBooks.
  · the ledger lists paper rounds but never counts them, which is right when
    real money is in the same table and useless here — every row in this
    sandbox is paper, so the summary would have read "0 live rounds closed"
    above a full table. `paperOnly` counts them and says they are paper.

The rename is display only: the stored slug is and stays `auto-cascade-fib`,
which is what every campaign on disk carries.
"""

import json
import os
import re
import shutil
import subprocess
import unittest

_HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_HTML = os.path.join(_HERE, "strategy.html")
_JS = os.path.join(_HERE, "static", "cryptoforge-app.js")
_NODE = shutil.which("node")


def _read(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


class AutoFibPaperJournalMarkupTests(unittest.TestCase):
    def setUp(self):
        self.html = _read(_HTML)
        self.js = _read(_JS)

    def _autofib_section(self):
        at = self.html.index('id="autofib-page"')
        nxt = re.search(r'<div id="[^"]+" class="page-section"', self.html[at + 40 :])
        return self.html[at : at + 40 + nxt.start()] if nxt else self.html[at:]

    def test_the_panel_is_on_the_cascade_auto_page(self):
        section = self._autofib_section()
        self.assertIn('id="cf-af-ledger-panel"', section)
        self.assertIn("Paper Journal", section)
        for part in ("body", "meta", "filters", "pager"):
            self.assertIn('id="cf-af-ledger-%s"' % part, section, part)

    def test_it_carries_the_same_columns_as_the_cascade_ledger(self):
        """One renderer writes both tables, so a column it omits renders blank."""
        section = self._autofib_section()
        table = section[section.index('id="cf-af-ledger-panel"') :]
        table = table[: table.index("</table>")]
        self.assertEqual(len(re.findall(r"<th[ >]", table)), 14)
        self.assertIn('colspan="14"', table)

    def test_it_is_wired_to_the_shared_renderer_as_paper_only(self):
        self.assertIn("cfRenderCascadeLedger(data || {}, 'cf-af-ledger', { paperOnly: true });", self.js)

    def test_the_cascade_page_keeps_its_own_ids_untouched(self):
        for part in ("body", "meta", "filters", "pager"):
            self.assertIn('id="cf-cascade-ledger-%s"' % part, self.html, part)

    def test_the_ledger_state_is_per_table_not_module_globals(self):
        """Three globals would have made the two tables fight over one page number."""
        for dead in ("_cfCascadeLedgerAll", "_cfCascadeLedgerCoin", "_cfCascadeLedgerPage"):
            self.assertNotIn(dead, self.js, dead)
        self.assertIn("function _cfLedgerBook(key)", self.js)


class CascadeAutoRenameTests(unittest.TestCase):
    def test_the_old_name_is_gone_from_everything_shipped(self):
        roots = ("app.py", "strategy.html", "static/cryptoforge-app.js", "engine")
        offenders = []
        for root in roots:
            path = os.path.join(_HERE, root)
            files = (
                [path]
                if os.path.isfile(path)
                else [
                    os.path.join(dirpath, name)
                    for dirpath, _, names in os.walk(path)
                    for name in names
                    if name.endswith((".py", ".js", ".html"))
                ]
            )
            for f in files:
                if "Cascade_Auto" in _read(f):
                    offenders.append(os.path.relpath(f, _HERE))
        self.assertEqual(offenders, [], "underscore name still shipped in: %s" % offenders)

    def test_the_new_name_is_what_the_alert_headline_says(self):
        from engine.cascade import strategy_label

        self.assertEqual(strategy_label("auto-cascade-fib"), "Cascade-Auto")

    def test_the_stored_slug_did_not_move(self):
        """Renaming the label must not orphan a single campaign on disk."""
        from engine.auto_cascade_fib import STRATEGY

        self.assertEqual(STRATEGY, "auto-cascade-fib")


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
grab('var _cfLedgerBooks', 'function cfCascadeSetLedgerFilter');
const q = JSON.parse(process.argv[2]);
const a = _cfLedgerBook(q.a);
a.page = 7; a.coin = 'BTCUSDT';
const b = _cfLedgerBook(q.b);
process.stdout.write(JSON.stringify({ bPage: b.page, bCoin: b.coin, aPage: a.page, aCoin: a.coin }));
"""


@unittest.skipUnless(_NODE, "node is not installed")
class LedgerBookIsolationTests(unittest.TestCase):
    def test_paging_one_table_does_not_move_the_other(self):
        proc = subprocess.run(
            [_NODE, "-e", _HARNESS, "--", _JS, json.dumps({"a": "cf-af-ledger", "b": "cf-cascade-ledger"})],
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        out = json.loads(proc.stdout)
        self.assertEqual((out["aPage"], out["aCoin"]), (7, "BTCUSDT"))
        self.assertEqual((out["bPage"], out["bCoin"]), (0, "ALL"))
