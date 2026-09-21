"""The installed app's icon must be able to change.

Phil, 22-Sep-2026, on the Mac: "I need the favicon for the install as an app in
my mac as well.. it still shows old icon". The mark changed on 17-Aug-2026, but
login.html still linked the icon with a June ?v= token and strategy.html with a
July one, and /apple-touch-icon.png was served `immutable` for a week — so a
browser that had the old picture was told never to look again.
"""

import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _read(*parts):
    with open(os.path.join(ROOT, *parts), encoding="utf-8") as handle:
        return handle.read()


def _token(html, rel):
    m = re.search(rf'<link rel="{rel}" href="[^"?]+\?v=([^"]+)"', html)
    assert m, f"no {rel} link"
    return m.group(1)


def test_both_front_doors_ask_for_the_same_icon_version():
    login, app = _read("login.html"), _read("strategy.html")
    for rel in ("apple-touch-icon", "manifest"):
        assert _token(login, rel) == _token(app, rel), f"{rel} versions differ between login.html and strategy.html"


def test_the_login_page_has_a_tab_icon():
    assert '<link rel="icon" href="/favicon.ico"' in _read("login.html")


def test_the_touch_icon_is_never_marked_immutable():
    src = _read("app.py")
    body = src[src.index("async def apple_touch_icon") :]
    body = body[: body.index("@app.")]
    code = "\n".join(line for line in body.splitlines() if not line.strip().startswith("#"))
    assert "max-age=" in code
    assert "immutable" not in code


def test_the_manifest_icons_are_the_current_mark():
    manifest = _read("static", "manifest.webmanifest")
    for size in ("192", "512"):
        assert f"/static/pwa-icons/icon-{size}.png?v=" in manifest
