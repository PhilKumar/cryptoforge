"""
The Open Trades table on the Auto-Cascade_Fib tab.

Phil, 2026-08-23: "Add a open trade section in auto-cascade_fib tab same like
cascade". It is the SAME renderer with this page's ids, not a second one, and
it is still asked for with actions:false — a healthy sandbox position is not
something to offer a market sell against.

That USED to be justified by the button posting to the live Cascade engine,
which does not own a strategy's campaigns. It no longer is: the route resolves
the campaign to the engine that owns it (_engine_holding_campaign), and a
STRANDED row — ended, still holding, no resting sell — is given its exit on
any page. Without that, four stopped Cascade-Auto positions had no way out at
all on 09-Sep-2026, the column being off here and the card that carries the
button being drawn only for working ladders.

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
        """Still conditional — but a STRANDED row now opens it.

        It used to hang on `actions` alone, which meant a stopped Cascade-Auto
        position holding coin had no exit anywhere: the column was off on this
        page and the card that carries the button is only drawn for working
        ladders. See tests/test_stranded_position_has_a_way_out.py.
        """
        self.assertIn("(showActions ? '<th>Action</th>' : '')", self.js)
        self.assertIn("var showActions = actions || open.some(_cfTradeIsStranded);", self.js)
        self.assertIn("actions || _cfTradeIsStranded(c) ? _cfCascadeTradeAction(c) : ''", self.js)

    def test_a_sandbox_position_is_never_sold_through_the_LIVE_engine(self):
        """`actions:false` used to be the guard. The server is the guard now.

        The button posts to one path for every strategy, so the route has to
        pick the engine that actually owns the campaign — otherwise a
        Cascade-Auto position either answers "not found" or, far worse, is
        matched against a live campaign of the same id.
        """
        self.assertIn("'/api/cascade/campaigns/' + encodeURIComponent(campaignId) + '/liquidate'", self.js)
        with open(os.path.join(_HERE, "app.py"), encoding="utf-8") as handle:
            app_source = handle.read()
        self.assertIn("eng, persist = _engine_holding_campaign(campaign_id)", app_source)
        self.assertIn("def _engine_holding_campaign(campaign_id: str):", app_source)

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

    def test_cascade_auto_is_the_first_tab(self):
        """Phil's order, 09-Sep-2026: Auto first, then the other two.

        Auto is the one he is taking live; it should not be the tab he has to
        reach past the other two to find.
        """
        bar_at = self.html.index('id="cf-strat-subnav"')
        bar = self.html[bar_at : self.html.index("</div>", self.html.index("Cascade-Auto", bar_at))]
        order = re.findall(r'data-cf-strat-page="([a-z0-9-]+)"', bar)
        self.assertEqual(order[:3], ["autofib-page", "cascade-page", "rule3070-page"])

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
