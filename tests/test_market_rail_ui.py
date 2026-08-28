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
    """_cfMarketRailLine, out of the real bundle.

    The rail rolls one headline at a time, bottom to top — not a horizontal
    marquee (Phil, 2026-08-28: "not as a marquee.. it has to roll down to up
    like one one message").
    """

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
grab('function _cfMarketRailAgo(', 'function _cfMarketRailStopRoll');
const items = JSON.parse(process.argv[2]);
process.stdout.write(items.map(_cfMarketRailLine).join(''));
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

    def test_each_headline_is_its_own_line(self):
        """One line per headline is what makes the step a whole line high."""
        out = self._render([{"title": f"H{n}", "link": "", "source": "F"} for n in range(5)])
        self.assertEqual(out.count('class="mr-news-line"'), 5)

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

    def test_the_title_sits_in_its_own_span_so_it_can_ellipsis(self):
        """A headline wider than the rail must end in "…", not be sliced."""
        out = self._render([{"title": "a long one", "link": "", "source": "F"}])
        self.assertIn('class="mr-news-title"', out)

    def test_age_is_shown_when_known_and_omitted_when_not(self):
        # Offsets sit INSIDE their bucket, not on its edge: the clock ticks
        # between building the stamp here and reading Date.now() in node, and
        # exactly 600s intermittently renders as "9m".
        import time as _t

        now = _t.time()
        for offset, expected in ((630, "10m"), (9000, "2h"), (86400 * 3 + 600, "3d")):
            out = self._render([{"title": "t", "link": "", "source": "F", "ts": now - offset}])
            self.assertIn('class="mr-news-time"', out)
            self.assertIn(expected, out, f"{offset}s should read {expected}")

        none = self._render([{"title": "t", "link": "", "source": "F", "ts": 0}])
        self.assertNotIn("mr-news-time", none)

        # Older than a week says nothing rather than "412d".
        stale = self._render([{"title": "t", "link": "", "source": "F", "ts": now - 86400 * 40}])
        self.assertNotIn("mr-news-time", stale)


class VerticalRollTests(unittest.TestCase):
    """The roll itself, read out of the source.

    These are structural rather than behavioural — the behaviour was verified
    in a browser (one line visible at rest, hover holds, leaving resumes) —
    but each pins a thing whose loss would be silent.
    """

    def setUp(self):
        self.js = open(_JS, encoding="utf-8").read()
        self.css = open(_CSS, encoding="utf-8").read()

    def test_the_step_and_the_line_height_are_the_same_variable(self):
        """If they ever drift, a headline parks half in and half out of view."""
        self.assertIn("height: var(--mr-line)", self.css)
        self.assertIn("calc(var(--mr-index, 0) * var(--mr-line) * -1)", self.css)

    def test_the_window_is_exactly_one_line_tall(self):
        view = self.css[self.css.index(".mr-news-view {") :][:400]
        self.assertIn("height: var(--mr-line)", view)
        self.assertIn("overflow: hidden", view)

    def test_the_first_headline_is_repeated_at_the_tail(self):
        """So the roll only ever travels upward and the wrap is off-screen."""
        self.assertIn("if (items.length > 1) lines += _cfMarketRailLine(items[0]);", self.js)

    def test_the_wrap_back_to_the_top_is_not_animated(self):
        self.assertIn(".mr-news-track.is-instant { transition: none; }", self.css)
        self.assertIn("void track.offsetHeight;", self.js)

    def test_hovering_holds_the_roll(self):
        self.assertIn("if (view.matches(':hover')) return;", self.js)

    def test_a_hidden_tab_does_not_roll(self):
        self.assertIn("if (document.hidden) return;", self.js)

    def test_a_single_headline_does_not_roll_against_itself(self):
        self.assertIn("if (items.length > 1) {", self.js)
        self.assertIn("_cfMarketRailCount < 2", self.js)

    def test_nothing_of_the_horizontal_marquee_is_left_on_the_rail(self):
        for token in ("cfNewsMarquee", "is-rolling", "mr-news-dot"):
            self.assertNotIn(token, self.css, token)
        self.assertNotIn("cfNewsMarquee", self.js)
