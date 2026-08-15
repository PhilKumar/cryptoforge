#!/usr/bin/env python3
"""Build a deployable, CSP-safe copy of the Dōjima landing page.

DOJIMA_LANDING.html is the source of truth and stays a single self-contained
file (it doubles as the shareable artifact). Production, however, sends:

    script-src 'self'; script-src-elem 'self'; script-src-attr 'none';
    style-src-elem 'self'; style-src-attr 'unsafe-inline'

so an inline <style>, an inline <script>, and any onclick=/onsubmit= are
silently dropped there — a page with no styling and a dead button, with no
console error to explain it. Inline style="" attributes are fine and are left
alone.

This script emits dist/ with those three things externalised, the six base64
plates written out as cacheable files, and the access form wired to a mailto:
link (form-action 'self' forbids submitting a form to mailto:, but navigating
a link to it is not a form submission, so the link is the CSP-safe route).

    python3 build_dojima_dist.py

dist/ is untracked scratch. Nothing that is served changes until its contents
are copied into BOTH front doors — CryptoForge's static/landing/index.html and
PhilForge's static/landing/forge.html, which are the same page on two hosts.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import re
import shutil
from pathlib import Path

HERE = Path(__file__).resolve().parent
SRC = HERE / "DOJIMA_LANDING.html"
DIST = HERE / "dist"

# Both apps serve this page at "/" while its files live under /static/landing/,
# so a relative href="img/plate_hero.jpg" resolves to /img/plate_hero.jpg and
# 404s — an unstyled page with no pictures and no console clue beyond the 404s
# themselves. Every asset reference is therefore rewritten to an absolute path.
# Pass --base "" to keep them relative for local preview out of dist/.
DEFAULT_BASE = "/static/landing/"

# One page, two hosts — but a social card cannot be relative: og:image is
# fetched by a crawler with no page context, so a path-only value is dropped
# and the link previews with no picture at all. It has to name a host, and
# philforge.in is the one that owns the story.
CANON_HOST = "https://philforge.in"


# Any attribute, not a fixed list of them: the films are wired through
# `poster=` and a lazy `data-film=`, and rewriting only href/src left those two
# pointing at /film/clips/… — a hero that stays black and three 404s.
ASSET_ATTR = re.compile(r'([a-zA-Z-]+=")(img/|film/|dojima\.)')
ASSET_URL = re.compile(r"(url\()(img/|film/)")


def rebase(html: str, base: str) -> str:
    """Point every asset reference at `base` instead of the current directory."""
    if not base:
        return html
    html = ASSET_ATTR.sub(lambda m: m.group(1) + base + m.group(2), html)
    html = ASSET_URL.sub(lambda m: m.group(1) + base + m.group(2), html)
    return re.sub(
        r'((?:property="og:image"|name="twitter:image") content=")(' + re.escape(base) + ")",
        lambda m: m.group(1) + CANON_HOST + m.group(2),
        html,
    )


# Where access requests go. One line to change if Phil moves to a role address
# on philforge.in; it is also injected into the page copy so the two can never
# drift apart.
ACCESS_EMAIL = "phil.shiny@gmail.com"

PLATE_NAMES = [
    "plate_hero.jpg",
    "plate_merchant.jpg",
    "plate_ledger.jpg",
    "plate_candle.jpg",
    "plate_bridge.jpg",
    "plate_cta.jpg",
]

# The inert button is replaced by a link whose href is rebuilt from the form on
# every keystroke, so the mail client opens with the four answers already in it.
MAILTO_JS = """
/* ── access request ─────────────────────────────────────────────
   The form cannot POST anywhere (this is a static page, and CSP's
   form-action 'self' would block a mailto: action anyway), so the
   request travels as a pre-filled mail draft instead. Nothing is
   collected here and nothing is claimed to be received. */
(function(){
  var TO=%(email)r;
  var form=document.getElementById('accessForm');
  var link=document.getElementById('accessSend');
  if(!form||!link)return;
  form.addEventListener('submit',function(e){e.preventDefault();link.click();});
  function val(id){var el=document.getElementById(id);return el?el.value.trim():'';}
  function build(){
    var name=val('f1'), email=val('f2'), cap=val('f3'), dd=val('f4');
    var subject='PhilForge’s Dōjima — access request'+(name?' — '+name:'');
    var body=[
      'Name: '+(name||'—'),
      'Email: '+(email||'—'),
      'Capital I would deploy: '+cap,
      'Maximum drawdown I can hold: '+dd,
      '',
      'Sent from the Dōjima access form.'
    ].join('\\n');
    link.href='mailto:'+TO+'?subject='+encodeURIComponent(subject)
             +'&body='+encodeURIComponent(body);
  }
  form.addEventListener('input',build);
  form.addEventListener('change',build);
  build();
})();
"""


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--base", default=DEFAULT_BASE, help='URL prefix for assets (default "%(default)s"; "" for relative)'
    )
    args = ap.parse_args()

    html = SRC.read_text(encoding="utf-8")
    DIST.mkdir(exist_ok=True)
    (DIST / "img").mkdir(exist_ok=True)

    # ── 1. plates out of the HTML and into cacheable files ──────────────
    plates: list[str] = []
    # The hero and the CTA are the same plate; writing it once keeps a visitor
    # from downloading 345 KB twice.
    by_digest: dict[str, str] = {}

    marks: list[str] = []

    def swap_plate(m: re.Match[str]) -> str:
        mime, raw = m.group(1), base64.b64decode(m.group(2))
        # Content key for de-duplication only — nothing here is a security
        # decision, so the weak-hash warning does not apply.
        digest = hashlib.sha1(raw, usedforsecurity=False).hexdigest()
        if digest in by_digest:
            return f"img/{by_digest[digest]}"
        # The plates are all JPEG and are named by the order they appear in.
        # The PhilForge brand mark is the one PNG, and it sits in the nav —
        # ahead of the hero — so it must NOT consume a plate slot or every
        # plate after it would be misnamed.
        if mime == "png":
            # Name by the artwork's own width rather than by document order:
            # the favicon <link> sits in <head>, ahead of the nav, so an
            # order-based name would hand the favicon the nav mark's filename.
            width = int.from_bytes(raw[16:20], "big") if raw[12:16] == b"IHDR" else 0
            name = {64: "mark_favicon.png", 120: "mark_philforge.png"}.get(width, f"mark_{width or len(marks)}.png")
            marks.append(name)
        else:
            idx = len(plates)
            name = PLATE_NAMES[idx] if idx < len(PLATE_NAMES) else f"plate_{idx}.jpg"
            plates.append(name)
        (DIST / "img" / name).write_bytes(raw)
        by_digest[digest] = name
        return f"img/{name}"

    html = re.sub(r"data:image/(jpeg|png|webp);base64,([A-Za-z0-9+/=]+)", swap_plate, html)

    # ── 2. the <style> block, which style-src-elem would drop ───────────
    style = re.search(r"<style>(.*?)</style>", html, re.S)
    if not style:
        raise SystemExit("no <style> block found — has the source changed?")
    (DIST / "dojima.css").write_text(style.group(1).strip() + "\n", encoding="utf-8")
    html = html.replace(style.group(0), '<link rel="stylesheet" href="dojima.css">')

    # ── 3. inline handlers, which script-src-attr 'none' would drop ─────
    # The form's only job was to swallow its own submit; the button lied about
    # having sent something. Both become real elements wired up in the JS.
    html = html.replace(
        '<form class="form rv" data-d="3" onsubmit="event.preventDefault()">',
        '<form class="form rv" data-d="3" id="accessForm">',
    )
    old_button = re.search(
        r'<button class="btn btn-p" type="button"\s*\n?\s*onclick="[^"]*">(.*?)</button>', html, re.S
    )
    if not old_button:
        raise SystemExit("access button not found — has the source changed?")
    html = html.replace(
        old_button.group(0),
        '<a class="btn btn-p" id="accessSend" href="mailto:' + ACCESS_EMAIL + '">' + old_button.group(1) + "</a>",
    )

    # ── 4. the <script> block, which script-src-elem would drop ─────────
    script = re.search(r"<script>(.*?)</script>", html, re.S)
    if not script:
        raise SystemExit("no <script> block found — has the source changed?")
    js = script.group(1).strip() + "\n" + (MAILTO_JS % {"email": ACCESS_EMAIL})
    (DIST / "dojima.js").write_text(js, encoding="utf-8")
    html = html.replace(script.group(0), '<script src="dojima.js" defer></script>')

    # ── 5. film assets ─────────────────────────────────────────────────
    src_clips = HERE / "film" / "clips"
    dst_clips = DIST / "film" / "clips"
    dst_clips.mkdir(parents=True, exist_ok=True)
    for f in sorted(src_clips.glob("*_web.mp4")) + sorted(src_clips.glob("*_poster.jpg")):
        shutil.copy2(f, dst_clips / f.name)

    # ── 6. absolute asset paths, because the page is served at "/" ──────
    html = rebase(html, args.base)
    (DIST / "index.html").write_text(html, encoding="utf-8")

    # ── 7. refuse to ship anything the CSP would silently kill ──────────
    bad = {
        "inline <style>": "<style>" in html,
        "inline <script>": re.search(r"<script>(?!\s*</script>)", html) is not None,
        "onclick=": "onclick=" in html,
        "onsubmit=": "onsubmit=" in html,
        "base64 image": ";base64," in html.replace("svg+xml,%3Csvg", ""),
        # A single missed reference is enough to blank the page at "/".
        "relative asset": bool(args.base) and (ASSET_ATTR.search(html) or ASSET_URL.search(html)) is not None,
    }
    failed = [k for k, v in bad.items() if v]
    if failed:
        raise SystemExit("CSP check failed, still present: " + ", ".join(failed))

    kb = lambda p: p.stat().st_size / 1024  # noqa: E731
    print(f"index.html   {kb(DIST / 'index.html'):8.0f} KB  (source was {kb(SRC):.0f} KB)")
    print(f"dojima.css   {kb(DIST / 'dojima.css'):8.0f} KB")
    print(f"dojima.js    {kb(DIST / 'dojima.js'):8.0f} KB")
    print(f"plates       {sum(kb(DIST / 'img' / p) for p in plates):8.0f} KB  ({len(plates)} files)")
    if marks:
        print(f"brand mark   {sum(kb(DIST / 'img' / m) for m in marks):8.0f} KB  ({len(marks)} files)")
    print(f"film         {sum(kb(f) for f in dst_clips.iterdir()):8.0f} KB  ({len(list(dst_clips.iterdir()))} files)")
    print(f"access mail → {ACCESS_EMAIL}")
    print("CSP check     passed (no inline style/script/handlers, no base64)")


if __name__ == "__main__":
    main()
