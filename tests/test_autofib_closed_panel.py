"""
The Closed Campaigns panel on the Cascade_Auto page.

Phil, 2026-08-24: "Cascade_Auto has no Closed Campaigns panel ... Add one with
freezed chart". It is the SAME table and the SAME renderer as the Cascade
page's, given this page's ids — not a second one.

Two things had to change behind it:
  · /api/auto-fib/status now carries the sandbox engine's closed history, which
    is the only place those rows exist;
  · DELETE /api/cascade/closed/{id} looks in the sandbox engines first. Without
    that, Remove on this page reported success and deleted nothing, because the
    live engine has never heard of a strategy's campaign.

The frozen chart needs no work: the chart endpoint already answers a sandbox id
from the sandbox engine and serves an ended campaign from its snapshot. The
test below pins that it still does, since that is what "freezed chart" rests on.
"""

import os
import re
import unittest

_HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_HTML = os.path.join(_HERE, "strategy.html")
_JS = os.path.join(_HERE, "static", "cryptoforge-app.js")
_APP = os.path.join(_HERE, "app.py")


def _read(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


class AutoFibClosedPanelMarkupTests(unittest.TestCase):
    def setUp(self):
        self.html = _read(_HTML)
        self.js = _read(_JS)

    def _autofib_section(self):
        at = self.html.index('id="autofib-page"')
        nxt = re.search(r'<div id="[^"]+" class="page-section"', self.html[at + 40 :])
        return self.html[at : at + 40 + nxt.start()] if nxt else self.html[at:]

    def test_the_panel_is_on_the_cascade_auto_page(self):
        section = self._autofib_section()
        for part in ("cf-af-closed-panel", "cf-af-closed-body", "cf-af-closed-filters", "cf-af-closed-pager"):
            self.assertIn(part, section, part)

    def test_it_is_not_a_copy_of_the_cascade_table(self):
        self.assertNotIn("cf-cascade-closed-body", self._autofib_section())

    def test_the_column_span_matches_the_number_of_columns(self):
        """A short colspan leaves the empty row hanging under a wider table."""
        section = self._autofib_section()
        panel = section[section.index("cf-af-closed-panel") :]
        panel = panel[: panel.index("</section>")]
        self.assertEqual(len(re.findall(r"<th[ >]", panel)), 9)  # not <thead
        self.assertIn('colspan="9"', panel)

    def test_it_is_drawn_from_this_pages_own_status(self):
        fn = self.js[self.js.index("function cfAfRenderLines(data)") :][:700]
        self.assertIn("cfRenderCascadeClosed(", fn)
        self.assertIn("data.closed_campaigns", fn)
        self.assertIn("'cf-af-closed'", fn)

    def test_the_cascade_page_still_calls_it_with_no_key(self):
        self.assertIn(
            "cfRenderCascadeClosed(Array.isArray(data.closed_campaigns) ? data.closed_campaigns : [])",
            self.js,
        )

    def test_each_table_keeps_its_own_filter_and_page(self):
        """One global filter meant paging one table paged the other."""
        self.assertIn("var _cfClosedBooks = {}", self.js)
        for gone in ("_cfCascadeClosedAll", "_cfCascadeClosedCoin", "_cfCascadeClosedPage"):
            self.assertNotIn(gone, self.js, "%s is a shared global and must be gone" % gone)

    def test_changing_the_filter_returns_to_the_first_page(self):
        fn = self.js[self.js.index("function cfCascadeSetClosedFilter") :][:400]
        self.assertIn("book.page = 0", fn)


class AutoFibClosedBackendTests(unittest.TestCase):
    def setUp(self):
        self.app = _read(_APP)

    def test_the_status_carries_the_sandbox_history(self):
        fn = self.app[self.app.index("async def auto_fib_status()") :][:1600]
        self.assertIn('"closed_campaigns": list(engine.closed_campaigns)', fn)

    def test_remove_looks_in_the_sandbox_engines_before_the_live_one(self):
        fn = self.app[self.app.index("async def cascade_purge_closed") :][:1800]
        self.assertIn("_auto_fib_engine", fn)
        self.assertIn("_vrule_engine", fn)
        live_purge = fn.index("eng.purge_closed_campaign")
        sandbox_check = fn.index("_auto_fib_engine")
        self.assertLess(sandbox_check, live_purge, "the sandbox must be checked FIRST")

    def test_removing_a_sandbox_row_persists_that_engine_not_the_live_one(self):
        fn = self.app[self.app.index("async def cascade_purge_closed") :][:1800]
        self.assertIn("_save_auto_fib_runtime", fn)
        self.assertIn("_save_vrule_runtime", fn)

    def test_the_frozen_chart_still_answers_a_sandbox_id(self):
        """What "freezed chart" rests on — pinned so it cannot quietly go."""
        fn = self.app[self.app.index("async def cascade_campaign_chart") :][:1400]
        self.assertIn("_auto_fib_engine", fn)
        self.assertIn("sandbox.closed_campaigns", fn)
