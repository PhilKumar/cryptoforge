"""The rail's markup and its client-side rendering.

The quick-asset switcher is gone: it set `selectedCrypto` and jumped to the
builder page, whose own coin grid still does that, so removing it cost a
shortcut and not the ability to choose an instrument.
"""

import json
import os
import re
import shutil
import subprocess
import sys
import unittest

_HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _HERE)

_HTML = os.path.join(_HERE, "strategy.html")
_CSS = os.path.join(_HERE, "static", "cryptoforge-app.css")
_JS = os.path.join(_HERE, "static", "cryptoforge-app.js")
_NODE = shutil.which("node")


class SwitcherIsGoneTests(unittest.TestCase):
    def setUp(self):
        self.html = open(_HTML, encoding="utf-8").read()
        self.css = open(_CSS, encoding="utf-8").read()
        self.js = open(_JS, encoding="utf-8").read()

    def test_no_trace_of_the_old_strip_anywhere(self):
        """Markup, styling and script together — a leftover rule is dead weight
        that the next reader has to prove is dead."""
        for name, blob in (("html", self.html), ("css", self.css), ("js", self.js)):
            for token in ("asset-switcher", "asset-pill", "setQuickAsset", "inr-rate-badge"):
                self.assertNotIn(token, blob, f"{token} still in {name}")

    def test_the_builder_can_still_choose_a_coin(self):
        """What the strip was a shortcut TO must outlive it."""
        self.assertIn('id="crypto-selector"', self.html)
        self.assertIn("function selectCrypto(", self.js)


class RailMarkupTests(unittest.TestCase):
    def setUp(self):
        self.html = open(_HTML, encoding="utf-8").read()
        self.css = open(_CSS, encoding="utf-8").read()

    def test_the_rail_carries_the_ids_the_script_writes_to(self):
        for node_id in (
            "cf-market-rail",
            "cf-mr-fx",
            "cf-mr-fx-rate",
            "cf-mr-fx-note",
            "cf-mr-news-view",
            "cf-mr-news-track",
        ):
            self.assertIn(f'id="{node_id}"', self.html, node_id)

    def test_every_class_the_rail_uses_is_actually_styled(self):
        """An invented class renders as bare unstyled text; it has happened
        here more than once."""
        used = set(re.findall(r'class="(mr-[a-z-]+|market-rail)"', self.html))
        for cls in used:
            self.assertIn("." + cls, self.css, f"{cls} has no CSS rule")

    def test_it_sits_in_the_sticky_shell_so_the_panes_measure_it(self):
        """--cf-shell-h is measured off .sticky-shell; a rail outside it would
        leave the scrolling panes sized for a header that is not there."""
        shell = self.html.index('class="sticky-shell"')
        after = self.html[shell:]
        rail = after.index('id="cf-market-rail"')
        nav = after.index('class="nav-bar"')
        self.assertLess(rail, nav, "the rail must be inside the shell, above the nav")

    def test_both_themes_are_painted_and_neither_is_only_a_media_query(self):
        for token in (".market-rail", ".mr-news-item", ".mr-fx-rate"):
            self.assertIn(token, self.css)
        self.assertIn('[data-theme="light"] .market-rail', self.css)

    def test_motion_can_be_turned_off(self):
        block = self.css[self.css.index("@media (prefers-reduced-motion: reduce)") :][:600]
        self.assertIn(".mr-news-track", block)


@unittest.skipUnless(_NODE, "node is not installed")
class RailRenderTests(unittest.TestCase):
    """_cfMarketRailNewsHtml, out of the real bundle."""

    HARNESS = r"""
const fs = require('fs');
const src = fs.readFileSync(process.argv[1], 'utf8');
function grab(start, stop) {
  const s = src.indexOf(start);
  if (s < 0) throw new Error('MISSING: ' + start);
  const e = src.indexOf(stop, s + start.length);
  if (e < 0) throw new Error('MISSING STOP: ' + stop);
  eval.call(globalThis, src.slice(s, e));
}
grab('function _escapeHtml(', 'function ');
grab('function _cfMarketRailNewsHtml(', 'function _cfMarketRailRender');
process.stdout.write(_cfMarketRailNewsHtml(JSON.parse(process.argv[2])));
"""

    def _render(self, items):
        proc = subprocess.run(
            [_NODE, "-e", self.HARNESS, "--", _JS, json.dumps(items)],
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return proc.stdout

    def test_a_headline_becomes_a_link_that_opens_safely(self):
        out = self._render([{"title": "BTC up", "link": "https://example.com/a", "source": "CoinDesk"}])
        self.assertIn('href="https://example.com/a"', out)
        self.assertIn('rel="noopener noreferrer nofollow"', out)
        self.assertIn('target="_blank"', out)
        self.assertIn("CoinDesk", out)

    def test_a_headline_with_markup_is_escaped_not_rendered(self):
        out = self._render([{"title": "<img src=x onerror=alert(1)>", "link": "", "source": "F"}])
        self.assertNotIn("<img", out)
        self.assertIn("&lt;img", out)

    def test_a_javascript_link_never_becomes_an_anchor(self):
        """The server strips these; this is the second lock, because the value
        is written by an outlet and not by us."""
        for bad in ("javascript:alert(1)", "JaVaScRiPt:alert(1)", "data:text/html,<script>"):
            out = self._render([{"title": "x", "link": bad, "source": "F"}])
            self.assertNotIn("<a ", out, bad)
            self.assertIn('<span class="mr-news-item"', out)

    def test_a_quote_in_the_source_name_cannot_break_out_of_the_chip(self):
        out = self._render([{"title": "t", "link": "", "source": '"><script>x</script>'}])
        self.assertNotIn("<script", out)

    def test_every_headline_is_rendered(self):
        items = [{"title": f"H{n}", "link": "", "source": "F"} for n in range(5)]
        self.assertEqual(self._render(items).count("mr-news-item"), 5)
