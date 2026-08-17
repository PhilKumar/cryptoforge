"""The phone's tab strip reads in the same order as the desktop's, Journal first.

The phone strip used to carry a grouped `order:` scheme (Trading, Analysis,
Setup) left over from a grid that no longer exists — and it named
`#nav-cascade`, a tab that no longer exists either, so the Strategies tab had
no order at all, fell to the front, and Journal landed fourth. Phil,
2026-08-17: "Mobile has the tabs not arranged properly starting from journal."

The strip follows DOM order now: no `order:` on any nav tab, anywhere in the
sheet, and the first tab in strategy.html is Journal.
"""

import os
import re
import unittest

_HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_APP_CSS = os.path.join(_HERE, "static", "cryptoforge-app.css")
_SHELL = os.path.join(_HERE, "strategy.html")


class NavOrderTests(unittest.TestCase):
    def test_no_nav_tab_is_reordered_by_css(self):
        with open(_APP_CSS, encoding="utf-8") as fh:
            css = fh.read()
        offenders = re.findall(r"#nav-[a-z-]+\s*\{[^}]*\border\s*:", css)
        self.assertEqual(offenders, [], f"nav tabs must follow DOM order on every screen: {offenders}")
        self.assertNotIn(".nav-tab", "".join(re.findall(r"\.nav-tab[^{]*\{[^}]*\border\s*:[^}]*\}", css)))

    def test_journal_is_the_first_tab_in_the_shell(self):
        with open(_SHELL, encoding="utf-8") as fh:
            html = fh.read()
        ids = re.findall(r'<button id="(nav-[a-z]+)" class="nav-tab', html)
        self.assertEqual(ids[0], "nav-journal", ids)
        self.assertEqual(
            ids,
            [
                "nav-journal",
                "nav-portfolio",
                "nav-strategies",
                "nav-dashboard",
                "nav-scalp",
                "nav-live",
                "nav-builder",
                "nav-market",
                "nav-results",
            ],
        )


if __name__ == "__main__":
    unittest.main()
