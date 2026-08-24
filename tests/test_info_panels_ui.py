"""Presentation contract for CryptoForge's fold-down information panels."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HTML = (ROOT / "strategy.html").read_text(encoding="utf-8")
JS = (ROOT / "static" / "cryptoforge-app.js").read_text(encoding="utf-8")
CSS = (ROOT / "static" / "cryptoforge-app.css").read_text(encoding="utf-8")


def test_all_three_strategy_manuals_are_bilingual_fold_down_panels():
    targets = (
        "cf-cascade-strategy-info",
        "cf-vrule-strategy-info",
        "cf-af-strategy-info",
    )
    for target in targets:
        assert f'data-cf-info-target="{target}"' in HTML
        assert f'id="{target}" class="cf-info-panel cf-info-doc"' in HTML

    assert HTML.count('data-cf-info-language="en"') == 3
    assert HTML.count('data-cf-info-language="ta"') == 3
    assert HTML.count('data-cf-info-language-button="en"') == 3
    assert HTML.count('data-cf-info-language-button="ta"') == 3
    assert "தமிழ்" in HTML


def test_old_always_visible_strategy_summaries_are_removed():
    assert "Mother-candle trendline system: resting limit buys" not in HTML
    assert "Its own engine and its own book. The Cascade page's campaigns are never touched." not in HTML
    assert "Paper replay above, real money below in its own engine" not in HTML


def test_every_plain_info_icon_uses_the_same_normal_flow_reveal_engine():
    assert "function _cfInfoPanel(trigger)" in JS
    assert "anchor.insertAdjacentElement('afterend', panel);" in JS
    assert "function cfInfoToggle(trigger)" in JS
    assert "cf-info-bubble" not in JS
    assert "cf-info-bubble" not in CSS
    assert ".cf-info-panel[hidden] { display: none !important; }" in CSS
    assert ".cf-info-panel {" in CSS


def test_language_choice_is_global_and_remembered():
    assert "function _cfApplyInfoLanguage(language)" in JS
    assert "localStorage.setItem('cf-info-language', language)" in JS
    assert "localStorage.getItem('cf-info-language')" in JS


def test_asset_versions_change_with_the_new_css_and_javascript():
    assert "/static/cryptoforge-app.css?v=20260824-bilingual-info-panels" in HTML
    assert "/static/cryptoforge-app.js?v=20260824-bilingual-info-panels" in HTML
