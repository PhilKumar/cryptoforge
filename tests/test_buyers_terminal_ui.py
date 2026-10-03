"""The buyers terminal: a card per buyer, and what each card must be able to say.

Phil, 03-Oct-2026: "Now build the buyers terminal properly". A table of names
answered "who may connect" and nothing else. These are the structural checks —
that the page and the script actually carry the terminal, that a buyer who has
never reported still renders, and that nothing on the card can print a secret.
"""

import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _read(*parts):
    with open(os.path.join(ROOT, *parts), encoding="utf-8") as handle:
        return handle.read()


HTML = _read("strategy.html")
JS = _read("static", "cryptoforge-app.js")
CSS = _read("static", "cryptoforge-app.css")


def test_the_panel_holds_cards_not_a_table():
    assert '<div class="cf-buyers-grid" id="cf-feed-buyers-body">' in HTML
    assert '<div class="cf-buyers-summary" id="cf-buyers-summary">' in HTML
    # The old six-column table must be gone, or two renderers fight over one id.
    assert "<th>Days Left</th>" not in HTML


def test_every_card_field_the_renderer_writes_has_a_style():
    for cls in (
        "cf-buyer-card",
        "cf-buyer-head",
        "cf-buyer-facts",
        "cf-buyer-positions",
        "cf-buyer-note",
        "cf-buyer-actions",
        "cf-buyers-stat",
        "cf-buyers-empty",
    ):
        assert f".{cls}" in CSS, f"{cls} is written by the JS and styled nowhere"


def test_the_four_health_states_are_all_drawn():
    for state in ("ok", "idle", "warn", "bad"):
        assert f'.cf-buyer-card[data-health="{state}"]' in CSS


def test_trouble_sorts_above_everyone_who_is_fine():
    body = JS[JS.index("function _cfFeedBuyersRender(") :][:1600]
    assert "bad: 0" in body and "warn: 1" in body, "a broken machine must not sort below a healthy one"


def test_a_buyer_who_never_reported_still_renders():
    card = JS[JS.index("function _cfBuyerCard(") : JS.index("function _cfFeedBuyersRender(")]
    assert "'never'" in card
    assert "report ?" in card or "if (report)" in card, "the card must not assume a report exists"


def test_the_refresh_cannot_overlap_or_wipe_a_half_typed_form():
    body = JS[JS.index("function cfBuyersAutoToggle(") :][:900]
    assert "document.hidden" in body
    assert "cf-feed-buyer-form" in body, "a repaint during registration would wipe the form"
    assert "60000" in body, "a machine reports once a minute; polling faster is wasted sockets"


def test_the_card_prints_no_credential_field():
    card = JS[JS.index("function _cfBuyerCard(") : JS.index("function _cfFeedBuyersRender(")]
    for banned in ("api_key", "api_secret", "public_key", "secret"):
        assert banned not in card, f"the terminal must never render {banned}"


def test_every_value_on_a_card_is_escaped():
    """Buyer labels and a machine's own error text are attacker-controlled."""
    card = JS[JS.index("function _cfBuyerCard(") : JS.index("function _cfFeedBuyersRender(")]
    for raw in re.findall(r"\+ (?:String\()?(?:row|report)\.[a-z_]+", card):
        assert False, f"unescaped value on the card: {raw}"
