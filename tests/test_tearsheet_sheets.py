"""The three published tearsheets, and the route that serves them.

Phil, 2026-09-03: tearsheets for Cascade Hybrid, Cascade_Auto and the V-Rule
over every coin's whole history, in PhilForge's pattern with CryptoForge's own
skin. What can go wrong quietly, and is therefore checked here:

  · a sheet drawing a class the stylesheet never defined (the reader's
    vocabulary is fixed — .kpis/.kpi/.lede/.note/.shead and so on);
  · the reader JS looking for ids the document does not carry, which leaves
    the contents rail empty and the search dead, with no error anywhere;
  · a sheet quoting a rate against the $1,000 nameplate instead of the capital
    the book really used, which is the one number on the page a reader would
    act on;
  · the route falling back to the wrong document for an unknown key.
"""

import json
import os
import re
import sys
import unittest
from importlib import import_module

_HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_HERE, "tools", "tearsheet"))

DOCS = os.path.join(_HERE, "docs", "assets")
SHEETS = {
    "hybrid": "cascade-hybrid-tearsheet.html",
    "auto": "cascade-auto-tearsheet.html",
    "vrule": "vrule-tearsheet.html",
}


def _published(key):
    path = os.path.join(DOCS, SHEETS[key])
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as handle:
        return handle.read()


class SheetKitTests(unittest.TestCase):
    """The shared look, before any sheet uses it."""

    def setUp(self):
        self.kit = import_module("sheet_kit")

    def test_every_class_the_builder_draws_is_styled(self):
        """Trap paid for on PhilForge's Gap Carry sheet: a builder invents a
        class, the page renders with no error, and the section is unstyled."""
        builder = import_module("build_sheets")
        source = open(builder.__file__, encoding="utf-8").read()
        drawn = set(re.findall(r"class='([a-z0-9 \-]+)'", source))
        names = {cls for group in drawn for cls in group.split()}
        # Classes the reader JS or the shell owns rather than the stylesheet.
        names -= {"tr"}
        for name in sorted(names):
            with self.subTest(name):
                styled = self.kit.STYLE + import_module("build_sheets").EXTRA_CSS
                self.assertIn(f".{name}", styled, f".{name} is drawn but never styled")

    def test_each_sheet_gets_its_own_accent(self):
        seen = {}
        for key in SHEETS:
            css = self.kit.recolour(self.kit.STYLE, key)
            light = re.search(r"--accent:(#[0-9a-f]{6})", css).group(1)
            self.assertNotIn(light, seen, f"{key} shares its accent with {seen.get(light)}")
            seen[light] = key

    def test_the_reader_is_carried_whole(self):
        """READER_JS and LANG_JS bring their own <script> tags; wrapping them
        nests the tag and kills the block silently."""
        self.assertTrue(self.kit.READER_JS.lstrip().startswith("<script>"))
        self.assertIn("</script>", self.kit.READER_JS)


@unittest.skipIf(_published("hybrid") is None, "sheets not built in this checkout")
class PublishedSheetTests(unittest.TestCase):
    def test_the_reader_finds_the_ids_it_binds_to(self):
        """READER_JS reads #document-body and #document-toc and builds the rail
        from `.shead h2`; LANG_JS reads #langbar. Miss one and the document
        looks fine and does nothing."""
        for key in SHEETS:
            html = _published(key)
            if html is None:
                continue
            with self.subTest(key):
                for needed in ("document-body", "document-toc", "langbar", "tearsheet-search", "reading-progress-bar"):
                    self.assertIn(f'id="{needed}"', html, f"{key} is missing {needed}")
                # The builder quotes attributes with ' — assert on the class, not
                # on one spelling of it.
                self.assertIn("shead", html)

    def test_every_section_can_reach_the_contents_rail(self):
        """The rail is built from sections that have a `.shead h2`; one without
        is a section no reader can navigate to."""
        for key in SHEETS:
            html = _published(key)
            if html is None:
                continue
            sections = re.findall(r"<section id=['\"]([a-z]+)['\"]>(.*?)</section>", html, re.S)
            with self.subTest(key):
                self.assertGreaterEqual(len(sections), 5)
                for anchor, body in sections:
                    self.assertIn("shead", body, f"{key}/{anchor} has no heading")

    def test_the_risk_notes_are_on_every_sheet(self):
        """The bag, not the closed rounds, is where this family's risk lives —
        a sheet that omits it is selling rather than reporting."""
        for key in SHEETS:
            html = _published(key)
            if html is None:
                continue
            with self.subTest(key):
                self.assertIn("winner by construction", html)
                self.assertIn("bag is the whole risk", html)

    def test_no_rate_is_quoted_against_the_nameplate(self):
        """Peak capital used, never the $1,000 the page starts from: the purse
        compounds and the book routinely holds more than it."""
        for key in SHEETS:
            html = _published(key)
            if html is None:
                continue
            with self.subTest(key):
                self.assertIn("peak capital", html.lower())


@unittest.skipIf(_published("hybrid") is None, "sheets not built in this checkout")
class SheetDataTests(unittest.TestCase):
    """The numbers behind the page, checked for internal agreement."""

    def _book(self, key):
        path = os.path.join(_HERE, "tools", "tearsheet", "data", f"{key}_report_data.json")
        if not os.path.exists(path):
            self.skipTest(f"{key} measurements not in this checkout")
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)

    def test_the_monthly_book_sums_to_the_closed_profit(self):
        """The equity curve is drawn from the monthly book, so the two have to
        be the same money — otherwise the curve is decorative."""
        for key in SHEETS:
            book = self._book(key)
            for coin in book["coins"]:
                with self.subTest(f"{key}/{coin['symbol']}"):
                    self.assertAlmostEqual(sum(coin["monthly"].values()), coin["net_pnl"], places=2)

    def test_total_is_closed_plus_bag(self):
        for key in SHEETS:
            book = self._book(key)
            for coin in book["coins"]:
                with self.subTest(f"{key}/{coin['symbol']}"):
                    self.assertAlmostEqual(coin["net_pnl"] + coin["open_pnl"], coin["total_pnl"], places=2)

    def test_every_coin_carries_its_real_window(self):
        """BTC and ETH list from 2017, SOL and PAX Gold from 2020. A window
        that quietly shortened would flatter the per-year rate."""
        for key in SHEETS:
            book = self._book(key)
            spans = {c["symbol"]: c["years"] for c in book["coins"]}
            with self.subTest(key):
                self.assertGreater(spans["BTCUSDT"], 8.5)
                self.assertGreater(spans["ETHUSDT"], 8.5)
                self.assertGreater(spans["SOLUSDT"], 5.5)
                self.assertGreater(spans["PAXGUSDT"], 5.5)


class ContentSecurityPolicyTests(unittest.TestCase):
    """The document must survive this app's own CSP.

    Phil, 2026-09-04, on the first published version: the tearsheet opened as
    raw serif text over black shapes. It carried its whole look in an inline
    <style> and its reader in inline <script>, the way PhilForge's sheets do —
    but this app sends `style-src-elem 'self'` and `script-src-elem 'self'`, so
    the browser dropped both and rendered the bare markup. Nothing errored
    anywhere; it simply looked like garbage.
    """

    def setUp(self):
        self.app_module = import_module("app")

    def test_the_policy_still_forbids_inline(self):
        """If this ever relaxes, the reason for the split below is gone — but
        until it does, the split is load-bearing."""
        source = open(self.app_module.__file__, encoding="utf-8").read()
        policy = source.split("csp = (", 1)[1].split(")", 1)[0]
        self.assertIn("style-src-elem 'self'", policy)
        self.assertIn("script-src-elem 'self'", policy)
        self.assertNotIn("style-src-elem 'self' 'unsafe-inline'", policy)

    def test_no_sheet_carries_an_inline_style_or_script(self):
        for key in SHEETS:
            html = _published(key)
            if html is None:
                continue
            with self.subTest(key):
                self.assertNotIn("<style", html, f"{key} has an inline <style> the CSP will drop")
                self.assertNotIn("<script", html, f"{key} has an inline <script> the CSP will drop")

    def test_the_look_and_the_reader_ship_as_static_files(self):
        static = os.path.join(_HERE, "static")
        self.assertTrue(os.path.exists(os.path.join(static, "tearsheet.js")))
        for key in SHEETS:
            with self.subTest(key):
                self.assertTrue(os.path.exists(os.path.join(static, f"tearsheet-{key}.css")))

    def test_the_reader_file_is_loadable_javascript(self):
        """It is assembled from two blocks that each carried their own <script>
        tag; leaving one in is a syntax error on line one and a dead page."""
        with open(os.path.join(_HERE, "static", "tearsheet.js"), encoding="utf-8") as handle:
            js = handle.read()
        self.assertNotIn("<script", js)
        self.assertNotIn("</script>", js)
        self.assertIn("document-toc", js)
        self.assertIn("langbar", js)


class TearsheetRouteTests(unittest.TestCase):
    def setUp(self):
        self.app_module = import_module("app")

    def test_every_sheet_is_registered_before_anything_links_to_it(self):
        """An unregistered key falls back to Cascade Hybrid silently, so the
        registry and the pages that link into it must agree."""
        registry = self.app_module._TEARSHEET_DOCS
        self.assertEqual(set(registry), set(SHEETS))
        with open(os.path.join(_HERE, "strategy.html"), encoding="utf-8") as handle:
            page = handle.read()
        for key in SHEETS:
            with self.subTest(key):
                self.assertIn(f"/assets/tearsheet?doc={key}", page)

    def test_the_route_points_at_the_files_the_builder_writes(self):
        for key, filename in SHEETS.items():
            with self.subTest(key):
                self.assertTrue(self.app_module._TEARSHEET_DOCS[key].endswith(filename))


if __name__ == "__main__":
    unittest.main()
