"""The published tearsheets — one per strategy PER COIN — and the route.

Phil, 2026-09-03: tearsheets in PhilForge's pattern with CryptoForge's skin.
Phil, 2026-09-19: "the tearsheet language is not uniform.. in tamil section it
shows english wordings... I need all the puttable heading on the tearsheets of
philforge here as well ... separate coins separate results and don't merge all
in one".

What can go wrong quietly, and is therefore checked here:

  · a sentence with no Tamil twin — it shows through in the Tamil view;
  · a PhilForge heading missing from a sheet;
  · one coin's numbers leaking into another's, or a coin sheet disagreeing
    with the measurements it was drawn from;
  · a class the stylesheet never defined, or an id the reader binds to;
  · anything inline, which this app's CSP drops without an error.
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
DATA = os.path.join(_HERE, "tools", "tearsheet", "data")
COINS = {"BTCUSDT": "btc", "ETHUSDT": "eth", "SOLUSDT": "sol", "PAXGUSDT": "paxg"}
CASCADE = ("hybrid", "auto", "vrule")
SHEETS = [f"{s}-{c}" for s in CASCADE for c in COINS.values()] + ["optsell-btc"]

# PhilForge's section set (Supertrend / Gap Carry), in its order.
PHILFORGE_HEADINGS = (
    "Read this first",
    "The finding that matters most",
    "The programme at a glance",
    "Charges, in full",
    "Daily income across the whole cycle",
    "Daily P&amp;L ledger",
    "Cumulative curve",
    "Year by year",
    "Month by month",
    "How much capital this needs",
    "Sizing up as the book earns",
    "Risk register",
    "Which day of the week pays",
    "Best ten, worst ten",
    "Recorded configuration snapshot",
    "Method",
    "What this document is not",
)


def _published(key):
    path = os.path.join(DOCS, f"{key}-tearsheet.html")
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as handle:
        return handle.read()


def _data(strategy):
    with open(os.path.join(DATA, f"{strategy}_report_data.json"), encoding="utf-8") as handle:
        return json.load(handle)


def _english_outside_t(html):
    """Latin words a Tamil reader would see: everything left once both language
    spans, tags and attributes are removed."""
    text = re.sub(r'<i lang="(en|ta)">.*?</i>', " ", html, flags=re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    allowed = {"CRYPTOFORGE", "BTC", "ETH", "SOL", "PAXG"}
    words = set(re.findall(r"\b[A-Za-z][A-Za-z\-]{2,}\b", text)) - allowed
    return words


class SheetKitTests(unittest.TestCase):
    def setUp(self):
        self.kit = import_module("sheet_kit")
        self.builder = import_module("build_coin_sheets")

    def test_every_class_the_builder_draws_is_styled(self):
        """Trap paid for on PhilForge's Gap Carry sheet: a builder invents a
        class, the page renders with no error, and the section is unstyled."""
        source = open(self.builder.__file__, encoding="utf-8").read()
        drawn = set(re.findall(r"class='([a-z0-9 \-]+)'", source)) | set(re.findall(r'class="([a-z0-9 \-]+)"', source))
        names = {c for group in drawn for c in group.split()} - {"tr"}
        styled = self.kit.STYLE + self.builder.LANG_CSS + self.builder.SHEET_CSS
        for name in sorted(names):
            with self.subTest(name):
                self.assertIn(f".{name}", styled, f".{name} is drawn but never styled")

    def test_each_strategy_gets_its_own_accent(self):
        seen = {}
        for key in (*CASCADE, "optsell"):
            css = self.kit.recolour(self.kit.STYLE, key)
            light = re.search(r"--accent:(#[0-9a-f]{6})", css).group(1)
            self.assertNotIn(light, seen, f"{key} shares its accent with {seen.get(light)}")
            seen[light] = key

    def test_the_reader_is_carried_whole(self):
        self.assertTrue(self.kit.READER_JS.lstrip().startswith("<script>"))
        self.assertIn("</script>", self.kit.READER_JS)


@unittest.skipIf(_published("hybrid-btc") is None, "sheets not built in this checkout")
class PublishedSheetTests(unittest.TestCase):
    def test_every_sheet_exists(self):
        for key in SHEETS:
            with self.subTest(key):
                self.assertIsNotNone(_published(key), f"{key} was not built")

    def test_every_philforge_heading_is_on_every_sheet(self):
        for key in SHEETS:
            html = _published(key)
            for heading in PHILFORGE_HEADINGS:
                with self.subTest(f"{key}: {heading}"):
                    self.assertIn(f'<i lang="en">{heading}</i>', html)

    def test_no_english_shows_through_in_tamil(self):
        """Every visible sentence goes through t(en, ta). What is left once both
        language spans are removed is what BOTH readers see — it may only be
        the brand and the coin tickers."""
        for key in SHEETS:
            with self.subTest(key):
                self.assertEqual(_english_outside_t(_published(key)), set())

    def test_every_english_span_has_a_tamil_twin(self):
        for key in SHEETS:
            html = _published(key)
            with self.subTest(key):
                self.assertEqual(html.count('<i lang="en">'), html.count('<i lang="ta">'))

    def test_the_reader_finds_the_ids_it_binds_to(self):
        for key in SHEETS:
            html = _published(key)
            with self.subTest(key):
                for needed in ("document-body", "document-toc", "langbar", "tearsheet-search", "reading-progress-bar"):
                    self.assertIn(f'id="{needed}"', html, f"{key} is missing {needed}")
                self.assertIn('id="ledger-years"', html)
                self.assertIn('id="cycle"', html)

    def test_every_section_can_reach_the_contents_rail(self):
        for key in SHEETS:
            html = _published(key)
            sections = re.findall(r"<section id=['\"]([a-z\-]+)['\"][^>]*>(.*?)</section>", html, re.S)
            with self.subTest(key):
                self.assertGreaterEqual(len(sections), 15)
                for anchor, body in sections:
                    self.assertIn("shead", body, f"{key}/{anchor} has no heading")

    def test_the_coin_buttons_link_every_coin_and_mark_this_one(self):
        for strategy in CASCADE:
            for coin in COINS.values():
                html = _published(f"{strategy}-{coin}")
                with self.subTest(f"{strategy}-{coin}"):
                    for other in COINS.values():
                        self.assertIn(f"/assets/tearsheet?doc={strategy}-{other}", html)
                    self.assertRegex(html, rf"doc={strategy}-{coin}'\s+aria-current='page'")

    def test_the_cascade_risk_notes_are_on_every_cascade_sheet(self):
        for strategy in CASCADE:
            for coin in COINS.values():
                html = _published(f"{strategy}-{coin}")
                with self.subTest(f"{strategy}-{coin}"):
                    self.assertIn("winner by construction", html)
                    self.assertIn("bag is the whole risk", html)
                    self.assertIn("peak capital", html.lower())


@unittest.skipIf(_published("hybrid-btc") is None, "sheets not built in this checkout")
class CoinNumbersTests(unittest.TestCase):
    """Each coin's sheet is that coin's measurement and nothing else."""

    def test_the_daily_book_is_the_closed_profit(self):
        for strategy in CASCADE:
            for coin in _data(strategy)["coins"]:
                with self.subTest(f"{strategy}/{coin['symbol']}"):
                    self.assertTrue(coin.get("daily"), "no daily book — re-run run_backtests.py")
                    self.assertAlmostEqual(sum(d[0] for d in coin["daily"].values()), coin["net_pnl"], places=1)
                    self.assertEqual(sum(d[1] for d in coin["daily"].values()), coin["rounds"])

    def test_the_monthly_book_is_the_daily_book_by_month(self):
        for strategy in CASCADE:
            for coin in _data(strategy)["coins"]:
                by_month = {}
                for day, (net, _n, _fees) in coin["daily"].items():
                    by_month[day[:7]] = by_month.get(day[:7], 0.0) + net
                for month, value in coin["monthly"].items():
                    with self.subTest(f"{strategy}/{coin['symbol']}/{month}"):
                        self.assertAlmostEqual(by_month.get(month, 0.0), value, places=1)

    def test_each_sheet_quotes_its_own_coin(self):
        """The headline closed profit on BTC's sheet is BTC's, and no other
        coin's total appears as a headline there."""
        builder = import_module("build_coin_sheets")
        for strategy in CASCADE:
            coins = _data(strategy)["coins"]
            for coin in coins:
                html = _published(f"{strategy}-{COINS[coin['symbol']]}")
                glance = html.split("id='glance'", 1)[1].split("</section>", 1)[0]
                with self.subTest(f"{strategy}/{coin['symbol']}"):
                    self.assertIn(builder.usd(coin["net_pnl"]), glance)
                    for other in coins:
                        if other["symbol"] != coin["symbol"] and abs(other["net_pnl"] - coin["net_pnl"]) > 0.01:
                            self.assertNotIn(f">{builder.usd(other['net_pnl'])}<", glance)


class OptionSellerSheetTests(unittest.TestCase):
    def setUp(self):
        self.html = _published("optsell-btc")
        if self.html is None:
            self.skipTest("option seller sheet not built in this checkout")
        with open(os.path.join(DATA, "optsell_report_data.json"), encoding="utf-8") as handle:
            self.book = json.load(handle)

    def test_the_monthly_book_sums_to_the_total(self):
        self.assertAlmostEqual(sum(self.book["monthly"].values()), self.book["totals"]["net"], places=1)
        self.assertAlmostEqual(sum(t["net"] for t in self.book["trades"]), self.book["totals"]["net"], places=1)

    def test_every_trade_obeys_the_engines_rule(self):
        from engine import option_seller_paper as osp

        self.assertEqual(self.book["rule"]["min_votes"], osp.MIN_VOTES)
        self.assertEqual(self.book["rule"]["stop_mult"], osp.STOP_MULT)
        self.assertEqual(self.book["rule"]["lookbacks_min"], list(osp.LOOKBACKS_MIN))
        days = [t["day"] for t in self.book["trades"]]
        self.assertEqual(len(days), len(set(days)), "at most one trade a day")
        for t in self.book["trades"]:
            with self.subTest(t["day"]):
                self.assertGreaterEqual(t["votes"], osp.MIN_VOTES)
                if t["why"] == "stop":
                    # prices are stored to 4 decimals
                    self.assertGreaterEqual(t["exit"], t["entry"] * osp.STOP_MULT - 1e-3)

    def test_the_unseen_half_is_the_verified_number(self):
        """+1,709.59 over 30 trades after 2026-02-27, worst run 308.47 — the
        figures that passed all five checks."""
        after = self.book["splits"][0]["after"]
        self.assertEqual(self.book["splits"][0]["cut"], "2026-02-27")
        self.assertEqual(after["trades"], 30)
        self.assertAlmostEqual(after["net"], 1709.59, places=1)
        self.assertAlmostEqual(after["worst_run"], 308.47, places=1)

    def test_its_own_sections_survive(self):
        for needed in ("Chosen on one half, tested on the other", "The days it skips", "Every trade"):
            with self.subTest(needed):
                self.assertIn(f'<i lang="en">{needed}</i>', self.html)
        for needed in ("A small sample", "not the real quotes", "1-minute candles"):
            with self.subTest(needed):
                self.assertIn(needed, self.html)


class ContentSecurityPolicyTests(unittest.TestCase):
    """The document must survive this app's own CSP (see 2026-09-04: inline
    style and script were dropped and the sheet rendered as raw markup)."""

    def setUp(self):
        self.app_module = import_module("app")

    def test_the_policy_still_forbids_inline(self):
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
        for key in (*CASCADE, "optsell"):
            with self.subTest(key):
                self.assertTrue(os.path.exists(os.path.join(static, f"tearsheet-{key}.css")))

    def test_the_reader_file_is_loadable_javascript(self):
        with open(os.path.join(_HERE, "static", "tearsheet.js"), encoding="utf-8") as handle:
            js = handle.read()
        self.assertNotIn("<script", js)
        self.assertNotIn("</script>", js)
        for needed in ("document-toc", "langbar", "data-series", "coin-switch"):
            self.assertIn(needed, js)


class TearsheetRouteTests(unittest.TestCase):
    def setUp(self):
        self.app_module = import_module("app")

    def test_every_sheet_is_registered(self):
        self.assertEqual(set(self.app_module._TEARSHEET_DOCS), set(SHEETS))
        for key in SHEETS:
            with self.subTest(key):
                self.assertTrue(self.app_module._TEARSHEET_DOCS[key].endswith(f"{key}-tearsheet.html"))

    def test_a_link_written_before_the_split_opens_bitcoin(self):
        with open(os.path.join(_HERE, "strategy.html"), encoding="utf-8") as handle:
            page = handle.read()
        for key in (*CASCADE, "optsell"):
            with self.subTest(key):
                self.assertEqual(self.app_module._TEARSHEET_ALIASES[key], f"{key}-btc")
                self.assertIn(f"/assets/tearsheet?doc={key}", page)

    def test_the_assets_page_offers_every_strategy(self):
        with open(os.path.join(_HERE, "static", "cryptoforge-app.js"), encoding="utf-8") as handle:
            js = handle.read()
        docs = re.search(r"var _CF_ASSET_DOCS = \[([^\]]*)\]", js).group(1)
        with open(os.path.join(_HERE, "strategy.html"), encoding="utf-8") as handle:
            page = handle.read()
        for key in (*CASCADE, "optsell"):
            with self.subTest(key):
                self.assertIn(f"'{key}'", docs)
                self.assertIn(f'data-cf-assets-doc="{key}"', page)


def test_ledger_year_chips_use_the_class_the_kit_styles():
    """The year filter must be `ledger-controls` — the kit styles nothing for a
    bare button (Phil saw grey browser buttons on 2026-09-09)."""
    for key in SHEETS:
        html = _published(key)
        if html is None:
            continue
        m = re.search(r"<div class=['\"]([a-z-]+)['\"] id=['\"]ledger-years['\"]", html)
        assert m, f"{key}: no ledger year bar"
        assert m.group(1) == "ledger-controls", key


if __name__ == "__main__":
    unittest.main()
