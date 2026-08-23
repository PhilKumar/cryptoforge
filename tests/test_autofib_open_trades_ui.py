"""
The Open Trades table on the Auto-Cascade_Fib tab.

Phil, 2026-08-23: "Add a open trade section in auto-cascade_fib tab same like
cascade". It is the SAME renderer with this page's ids, not a second one — but
with the Action column off, because that button posts to
/api/cascade/campaigns/<id>/liquidate, the LIVE Cascade engine, which does not
own a strategy's campaigns.

Also pinned: the Cascade page's own ids are unchanged, since an e2e probe and
the pager both address them.
"""

import os
import re
import unittest

_HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_HTML = os.path.join(_HERE, "strategy.html")
_JS = os.path.join(_HERE, "static", "cryptoforge-app.js")
_CSS = os.path.join(_HERE, "static", "cryptoforge-app.css")


def _read(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


class AutoFibOpenTradesTests(unittest.TestCase):
    def setUp(self):
        self.html = _read(_HTML)
        self.js = _read(_JS)

    def test_the_tab_has_a_mount_and_a_meta_line(self):
        self.assertIn('id="cf-af-trades"', self.html)
        self.assertIn('id="cf-af-trades-meta"', self.html)

    def test_it_sits_on_the_autofib_page_and_not_somewhere_else(self):
        page = self.html.index('id="autofib-page"')
        # The NEXT page-section div, not this one's own class attribute.
        nxt = re.search(r'<div id="[^"]+" class="page-section"', self.html[page + 40 :])
        section = self.html[page : page + 40 + nxt.start()] if nxt else self.html[page:]
        self.assertIn('id="cf-af-trades"', section)
        self.assertNotIn('id="cf-cascade-trades"', section, "it must not be the Cascade table")

    def test_the_renderer_is_called_with_the_action_column_off(self):
        call = re.search(
            r"cfRenderCascadeTrades\(\s*Array\.isArray\(data && data\.campaigns\)[^;]*?"
            r"mountId:\s*'cf-af-trades'\s*,\s*actions:\s*false",
            self.js,
            re.S,
        )
        self.assertIsNotNone(call, "the auto-fib table must be drawn without the Action column")

    def test_the_action_column_is_conditional_in_both_the_head_and_the_body(self):
        self.assertIn("(actions ? '<th>Action</th>' : '')", self.js)
        self.assertIn("(actions ? '<td>' + _cfCascadeTradeAction(c) + '</td>' : '')", self.js)

    def test_the_market_sell_button_still_posts_only_to_the_cascade_engine(self):
        """If this ever stops being true, actions:false stops being the guard."""
        self.assertIn("'/api/cascade/campaigns/' + encodeURIComponent(campaignId) + '/liquidate'", self.js)

    def test_the_cascade_page_keeps_its_original_ids(self):
        self.assertIn('id="cf-cascade-trades"', self.html)
        self.assertIn("o.mountId || 'cf-cascade-trades'", self.js)

    def test_the_table_and_pager_ids_follow_the_mount_instead_of_being_fixed(self):
        self.assertIn("var tableId = mountId + '-table';", self.js)
        self.assertIn("_renderTablePager(tableId, tableId, mountId + '-pagination');", self.js)


class StrategySubnavStickyTests(unittest.TestCase):
    """Phil: "The page scroll has to show the 3 strategies heading... It hides
    those currently when scrolled below"."""

    def setUp(self):
        self.css = _read(_CSS)
        self.js = _read(_JS)

    def test_the_switcher_sticks_below_the_measured_topbar(self):
        rule = re.search(r"\.cf-strat-subnav \{(.*?)\}", self.css, re.S)
        self.assertIsNotNone(rule)
        body = rule.group(1)
        self.assertIn("position: sticky", body)
        self.assertIn("top: var(--cf-shell-h", body)

    def test_it_paints_a_background_so_content_cannot_scroll_through_it(self):
        rule = re.search(r"\.cf-strat-subnav \{(.*?)\}", self.css, re.S)
        self.assertIn("background:", rule.group(1))

    def test_the_shell_height_variable_is_actually_maintained(self):
        """top: var(--cf-shell-h) is only correct while something sets it."""
        self.assertIn("setProperty('--cf-shell-h'", self.js)
        self.assertIn("ResizeObserver", self.js)
