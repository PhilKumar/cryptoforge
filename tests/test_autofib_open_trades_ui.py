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


class StrategySubnavInHeaderTests(unittest.TestCase):
    """The strategy switcher belongs to the header, not to the pages.

    It used to be copied into all three strategy pages, so it scrolled away
    with the content. Making it sticky INSIDE the page was the wrong fix — it
    left the tabs floating over the content mid-scroll (Phil, 2026-08-23: "not
    putting something inside like a fool's work ... like attaching it to the
    top headers like journal, portfolio"). One copy now lives in
    .sticky-shell, beside the nav, and is shown only on a strategy page.
    """

    def setUp(self):
        self.html = _read(_HTML)
        self.js = _read(_JS)
        self.css = _read(_CSS)

    def test_there_is_exactly_one_switcher(self):
        # Count ELEMENTS, not substrings — the one element mentions the name
        # twice, in its class and in its id.
        self.assertEqual(len(re.findall(r'<div class="cf-strat-tabs cf-strat-subnav"', self.html)), 1)
        self.assertEqual(len(re.findall(r'id="cf-strat-subnav"', self.html)), 1)

    def test_it_lives_inside_the_sticky_shell(self):
        shell_start = self.html.index('<div class="sticky-shell">')
        shell_end = self.html.index("</div><!-- /sticky-shell -->")
        at = self.html.index('id="cf-strat-subnav"')
        self.assertGreater(at, shell_start)
        self.assertLess(at, shell_end, "the switcher must be inside the header shell")

    def test_no_strategy_page_carries_its_own_copy(self):
        for page in ("cascade-page", "rule3070-page", "autofib-page"):
            at = self.html.index('id="%s"' % page)
            nxt = re.search(r'<div id="[^"]+" class="page-section"', self.html[at + 40 :])
            section = self.html[at : at + 40 + nxt.start()] if nxt else self.html[at:]
            self.assertNotIn("cf-strat-subnav", section, "%s still has its own switcher" % page)

    def test_it_starts_hidden_and_css_can_hide_a_flex_row(self):
        self.assertIn('id="cf-strat-subnav"', self.html)
        bar = self.html[self.html.index('id="cf-strat-subnav"') :][:400]
        self.assertIn("hidden", bar, "it must not show on non-strategy pages before any JS runs")
        # .cf-strat-tabs sets display:flex, which beats the hidden attribute —
        # so an explicit rule is required or a "hidden" bar still shows.
        self.assertIn(".cf-strat-subnav[hidden] { display: none; }", self.css)

    def test_it_is_no_longer_stuck_to_the_page(self):
        rule = re.search(r"\.cf-strat-subnav \{(.*?)\}", self.css, re.S).group(1)
        self.assertNotIn("position: sticky", rule)

    def test_every_card_names_the_page_it_opens(self):
        for page in ("cascade-page", "rule3070-page", "autofib-page"):
            self.assertIn('data-cf-strat-page="%s"' % page, self.html)

    def test_the_state_is_synced_from_the_one_place_pages_change(self):
        self.assertIn("function cfSyncStrategySubnav(pageId)", self.js)
        shell = re.search(r"function cfSetActivePageShell\(.*?\n\}", self.js, re.S).group(0)
        self.assertIn("cfSyncStrategySubnav(pageId)", shell)

    def test_the_already_active_shortcut_also_syncs_it(self):
        """showPage returns early when the page is already open — the switcher
        still has to be right, or re-clicking a tab blanks its highlight."""
        early = self.js[self.js.index("if (alreadyActive && !opts.forceReload) {") :][:400]
        self.assertIn("cfSyncStrategySubnav(pageId)", early)
