"""tools/tearsheet/build_coin_sheets.py — one tearsheet per strategy PER COIN.

Phil, 19-Sep-2026, on the first CryptoForge sheets:

  "the tearsheet language is not uniform.. in tamil section it shows english
   wordings... I need all the puttable heading on the tearsheets of philforge
   here as well ... separate coins separate results and don't merge all in one"

So this replaces build_sheets.py and build_option_seller.py with one builder:

  · ONE DOCUMENT PER COIN. Cascade-Hybrid on Bitcoin is its own sheet, with its
    own headline, curve, ledger and risk register; a row of coin buttons at the
    top moves between the coins. Nothing is ever summed across coins.
  · PHILFORGE'S SECTIONS, in PhilForge's order — read this first, the finding
    that matters most, the programme at a glance, charges in full, daily income
    across the whole cycle, the daily ledger, the cumulative curve, year by
    year, month by month, how much capital this needs, sizing up, the risk
    register, which day of the week pays, best ten and worst ten, the recorded
    configuration, method, and what this document is not.
  · EVERY VISIBLE WORD THROUGH t(en, ta), numbers included in both sentences.
    A string with no Tamil twin is a bug, and a test looks for them.

    .venv/bin/python tools/tearsheet/run_backtests.py        # the Cascade data
    python3 tools/tearsheet/optsell_data.py <research dir>   # the option data
    python3 tools/tearsheet/build_coin_sheets.py             # every sheet

Writes docs/assets/<strategy>-<coin>-tearsheet.html (fragments app.py wraps),
static/tearsheet-<strategy>.css and static/tearsheet.js.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import pathlib
import statistics
import sys

_HERE = pathlib.Path(__file__).resolve().parent
_REPO = _HERE.parent.parent
sys.path.insert(0, str(_HERE))

import sheet_kit as kit  # noqa: E402
from i18n import LANG_CSS, LANG_JS, t, t_attr  # noqa: E402

DATA_DIR = _HERE / "data"
OUT_DIR = _REPO / "docs" / "assets"
STATIC_DIR = _REPO / "static"

COINS = {
    # symbol: (slug, English, Tamil)
    "BTCUSDT": ("btc", "Bitcoin", "பிட்காயின்"),
    "ETHUSDT": ("eth", "Ether", "ஈதர்"),
    "SOLUSDT": ("sol", "Solana", "சொலானா"),
    "PAXGUSDT": ("paxg", "PAX Gold", "பாக்ஸ் கோல்டு"),
}
DOW_EN = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
DOW_TA = ("திங்கள்", "செவ்வாய்", "புதன்", "வியாழன்", "வெள்ளி", "சனி", "ஞாயிறு")
MON_EN = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
MON_TA = ("ஜன", "பிப்", "மார்", "ஏப்", "மே", "ஜூன்", "ஜூலை", "ஆக", "செப்", "அக்", "நவ", "டிச")

# ── money and small words ─────────────────────────────────────────────


def usd(n, dp=2):
    """Dollars, signed the way a ledger signs them: -$1.00, $1.00."""
    try:
        v = float(n)
    except (TypeError, ValueError):
        return "—"
    return f"{'-' if v < 0 else ''}${abs(v):,.{dp}f}"


def sgn(n, dp=2):
    """Dollars with an explicit + on a gain."""
    s = usd(n, dp)
    return s if s.startswith("-") or s == "—" else "+" + s


def pct(n, dp=1):
    try:
        return f"{float(n):,.{dp}f}%"
    except (TypeError, ValueError):
        return "—"


cls = kit.cls


def r(n):
    """The money formatter the vendored PhilForge helpers take."""
    return usd(n, 2)


def days_from_hours(h):
    return round(float(h or 0) / 24.0, 1)


def ta_num(n):
    """Numbers stay Arabic numerals in the Tamil text — as on PhilForge."""
    return n


# ── the page's own CSS, on top of the shared kit ──────────────────────

SHEET_CSS = """
table.heat { table-layout:fixed; width:100%; }
table.heat th, table.heat td { text-align:center; padding:7px 4px; font-size:12px; }
table.heat th:first-child, table.heat td:first-child { text-align:left; width:76px; }
table.heat tbody tr:nth-child(even) { background:var(--surface-2); }
table.prose { min-width:0; }
table.prose td { white-space:normal; text-align:left; vertical-align:top; line-height:1.55; }
table.prose th[scope='row'] { width:230px; white-space:normal; text-align:left; vertical-align:top; }
td.label { text-align:left; white-space:normal; vertical-align:middle; }
.two-up { display:grid; grid-template-columns:repeat(auto-fit,minmax(360px,1fr)); gap:12px; }
.two-up > * { min-width:0; }
.canvas-wrap { position:relative; width:100%; }
canvas { display:block; width:100%; height:340px; touch-action:pan-y; }
.tip { position:absolute; pointer-events:none; opacity:0; transform:translate(-50%,-100%);
       background:var(--surface); border:1px solid var(--line); border-radius:8px;
       padding:8px 11px; box-shadow:var(--shadow); font-family:var(--mono);
       font-size:11.5px; line-height:1.55; white-space:nowrap; z-index:5;
       transition:opacity .12s ease; }
.tip b { display:block; font-size:10px; letter-spacing:.1em; text-transform:uppercase;
         color:var(--muted); margin-bottom:3px; font-weight:800; }
.legend { display:flex; flex-wrap:wrap; gap:6px 18px; margin-top:12px;
          font-family:var(--mono); font-size:11px; color:var(--muted); }
.legend span { display:inline-flex; align-items:center; gap:7px; }
.legend i:not([lang]) { width:14px; height:3px; border-radius:2px; display:block; }
.legend i.bar:not([lang]) { height:9px; width:7px; border-radius:2px; }
.coin-switch { display:flex; flex-wrap:wrap; gap:8px; margin:0 0 18px; align-items:center; }
.coin-switch > span { font:600 10px var(--mono); letter-spacing:.12em; text-transform:uppercase;
                      color:var(--muted); margin-right:4px; }
.coin-switch a { display:inline-flex; flex-direction:column; gap:2px; min-width:118px;
                 padding:9px 14px; border:1px solid var(--line); border-radius:12px;
                 background:var(--surface); color:var(--ink-2); text-decoration:none; }
.coin-switch a strong { font-family:var(--mono); font-size:13px; letter-spacing:.04em; }
.coin-switch a small { font-size:11px; color:var(--muted); }
.coin-switch a:hover { border-color:var(--accent); }
.coin-switch a[aria-current='page'] { border-color:var(--accent); background:var(--accent-soft); color:var(--ink); }
.coin-switch a[aria-current='page'] strong { color:var(--accent); }
"""

# The daily-income canvas. PhilForge inlines this with its data; this app's CSP
# drops any inline <script>, so it ships in static/tearsheet.js and reads its
# series off the canvas's own data-series attribute.
CYCLE_JS = r"""
(function () {
  var cv = document.getElementById('cycle'), tip = document.getElementById('cycle-tip');
  if (!cv) return;
  var DATA = [];
  try { DATA = JSON.parse(cv.getAttribute('data-series') || '[]'); } catch (e) { DATA = []; }
  if (DATA.length < 2) return;
  var box = cv.parentNode, hover = -1, geom = null;
  function tok(n) { return getComputedStyle(document.documentElement).getPropertyValue(n).trim(); }
  function ta() { return document.documentElement.getAttribute('data-lang') === 'ta'; }
  function money(v) {
    var s = Math.abs(v).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    return (v < 0 ? '-' : '') + '$' + s;
  }
  function short(v) {
    var a = Math.abs(v);
    var s = a >= 1000 ? (a / 1000).toFixed(a >= 10000 ? 0 : 1) + 'k' : a.toFixed(0);
    return (v < 0 ? '-' : '') + '$' + s;
  }
  function draw() {
    var dpr = window.devicePixelRatio || 1;
    var w = box.clientWidth, h = 340;
    cv.width = w * dpr; cv.height = h * dpr; cv.style.height = h + 'px';
    var g = cv.getContext('2d');
    g.setTransform(dpr, 0, 0, dpr, 0, 0);
    g.clearRect(0, 0, w, h);
    var padL = 66, padR = 14, padT = 14, padB = 108;
    var iw = w - padL - padR, ih = h - padT - padB;
    var cums = DATA.map(function (d) { return d[2]; });
    var days = DATA.map(function (d) { return d[1]; });
    var cMin = Math.min(0, Math.min.apply(null, cums)), cMax = Math.max.apply(null, cums);
    var dMax = Math.max.apply(null, days.map(Math.abs));
    var line = tok('--curve'), muted = tok('--muted'), grid = tok('--line');
    var pos = tok('--pos-fill'), neg = tok('--neg-fill');
    var X = function (i) { return padL + i / (DATA.length - 1) * iw; };
    var Y = function (v) { return padT + (cMax - v) / ((cMax - cMin) || 1) * ih; };
    g.font = '10px ui-monospace, Menlo, monospace';
    g.textAlign = 'right'; g.textBaseline = 'middle';
    for (var s = 0; s <= 5; s++) {
      var v = cMin + (cMax - cMin) * s / 5, y = Y(v);
      g.strokeStyle = grid; g.lineWidth = 1;
      g.beginPath(); g.moveTo(padL, y + 0.5); g.lineTo(w - padR, y + 0.5); g.stroke();
      g.fillStyle = muted; g.fillText(short(v), padL - 8, y);
    }
    var bw = Math.max(1, iw / DATA.length * 0.7), barH = 38, barZero = h - 46;
    g.strokeStyle = grid; g.beginPath(); g.moveTo(padL, barZero + 0.5); g.lineTo(w - padR, barZero + 0.5); g.stroke();
    for (var i = 0; i < DATA.length; i++) {
      var p = DATA[i][1];
      if (!p) continue;
      var hgt = Math.abs(p) / (dMax || 1) * barH;
      g.fillStyle = 'rgba(' + (p > 0 ? pos : neg) + ',' + (i === hover ? 0.95 : 0.45) + ')';
      g.fillRect(X(i) - bw / 2, p > 0 ? barZero - hgt : barZero, bw, hgt);
    }
    g.fillStyle = muted; g.fillText(ta() ? 'நாள்' : 'day', padL - 8, barZero);
    g.beginPath();
    for (var j = 0; j < DATA.length; j++) { var x = X(j), yy = Y(DATA[j][2]); j ? g.lineTo(x, yy) : g.moveTo(x, yy); }
    g.strokeStyle = line; g.lineWidth = 1.8; g.lineJoin = 'round'; g.stroke();
    g.textAlign = 'center'; g.textBaseline = 'top';
    var seen = {};
    for (var k = 0; k < DATA.length; k++) {
      var yr = DATA[k][0].slice(0, 4);
      if (seen[yr]) continue;
      seen[yr] = 1;
      g.strokeStyle = grid; g.beginPath(); g.moveTo(X(k) + 0.5, padT); g.lineTo(X(k) + 0.5, h - padB); g.stroke();
      g.fillStyle = muted; g.fillText(yr, X(k), h - padB + 6);
    }
    if (hover >= 0) {
      g.strokeStyle = muted; g.lineWidth = 1; g.setLineDash([3, 3]);
      g.beginPath(); g.moveTo(X(hover) + 0.5, padT); g.lineTo(X(hover) + 0.5, h - padB); g.stroke();
      g.setLineDash([]); g.fillStyle = line;
      g.beginPath(); g.arc(X(hover), Y(DATA[hover][2]), 3.5, 0, 6.284); g.fill();
    }
    geom = { padL: padL, iw: iw, X: X, Y: Y };
  }
  function at(ev) {
    var rect = cv.getBoundingClientRect();
    var x = (ev.touches ? ev.touches[0].clientX : ev.clientX) - rect.left;
    return Math.max(0, Math.min(DATA.length - 1, Math.round((x - geom.padL) / geom.iw * (DATA.length - 1))));
  }
  function show(ev) {
    hover = at(ev); draw();
    var d = DATA[hover], n = d[3], word = cv.getAttribute(ta() ? 'data-noun-ta' : 'data-noun-en') || '';
    tip.innerHTML = '<b>' + d[0] + '</b>' + (ta() ? 'நாள் ' : 'day ') + money(d[1]) + ' &middot; ' + n + ' ' + word +
      '<br>' + (ta() ? 'மொத்தம் ' : 'running ') + money(d[2]);
    tip.style.opacity = 1;
    tip.style.left = Math.min(box.clientWidth - 20, Math.max(70, geom.X(hover))) + 'px';
    tip.style.top = (geom.Y(d[2]) - 12) + 'px';
  }
  function hide() { hover = -1; tip.style.opacity = 0; draw(); }
  cv.addEventListener('mousemove', show);
  cv.addEventListener('mouseleave', hide);
  cv.addEventListener('touchstart', show, { passive: true });
  cv.addEventListener('touchmove', show, { passive: true });
  cv.addEventListener('touchend', hide);
  window.addEventListener('resize', draw);
  new MutationObserver(draw).observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme', 'data-lang'] });
  draw();
})();

/* The coin buttons open a sibling sheet. Inside the Assets frame the workspace
   passes ?theme=; carrying it across keeps the next coin in the same skin. */
(function () {
  var theme = new URLSearchParams(location.search).get('theme');
  if (!theme) return;
  [].forEach.call(document.querySelectorAll('.coin-switch a'), function (a) {
    a.href = a.getAttribute('href') + '&theme=' + encodeURIComponent(theme);
  });
})();
"""


# ── the words each strategy carries ───────────────────────────────────
# Every claim is about what the SHIPPED engine does, named beside it.

STRATEGIES = {
    "hybrid": {
        "title": ("Cascade-Hybrid", "Cascade-Hybrid"),
        "mark": "CH",
        "source": "engine/cascade.py · tools/cascade_depth_sweep.py",
        "kicker": (
            "Hand-driven mother candles · levels 2/4/8 · quarter target",
            "கையால் குறித்த mother candle · levels 2/4/8 · கால் பங்கு இலக்கு",
        ),
        "lede": (
            "A mother candle is marked on the chart; the engine draws a ladder beneath it, buys three rungs "
            "with 20%, 30% and 50% of the money as price falls, and sells the whole basket a quarter of the way "
            "back to the high. This sheet runs that book over every 5-minute bar Binance has published for {coin}.",
            "Chart-இல் ஒரு mother candle குறிக்கப்படுகிறது; engine அதன் கீழ் ஒரு ladder வரைந்து, விலை இறங்கும்போது "
            "பணத்தின் 20%, 30%, 50%-இல் மூன்று rung-களை வாங்கி, உச்சத்தை நோக்கி கால் பங்கு திரும்பியதும் முழுவதையும் "
            "விற்கிறது. {coin_ta}-க்கு Binance வெளியிட்ட ஒவ்வொரு 5-நிமிட bar-இலும் இந்த book இயக்கப்பட்டது.",
        ),
        "rules": [
            (
                ("Mother candle", "தாய் candle"),
                (
                    "Chosen by hand on the chart. The engine never picks one for this book.",
                    "Chart-இல் கையால் தேர்வு. இந்த book-க்கு engine தானாக எதையும் தேர்வு செய்யாது.",
                ),
            ),
            (
                ("Ladder", "ஏணி"),
                (
                    "Fibonacci levels 2, 4 and 8 below the mother, funded 20%, 30% and 50% of the fall.",
                    "Mother-க்குக் கீழே Fibonacci levels 2, 4, 8; வீழ்ச்சியின் 20%, 30%, 50% நிதி.",
                ),
            ),
            (
                ("Entry", "நுழைவு"),
                (
                    "Levels 2 and 4 wait as buy stops above a falling market; level 8 rests as a limit order.",
                    "Levels 2, 4 இறங்கும் சந்தைக்கு மேல் buy stop-ஆகக் காத்திருக்கும்; level 8 limit order-ஆக இருக்கும்.",
                ),
            ),
            (
                ("Target", "இலக்கு"),
                (
                    "A quarter of the way from the average entry back to the mother high.",
                    "சராசரி நுழைவிலிருந்து mother உச்சம் நோக்கி கால் பங்கு.",
                ),
            ),
            (
                ("Timeframe", "நேர அளவு"),
                (
                    "A campaign that cannot reach its target climbs 5m → 15m → 1h → 4h → 1d → 1w.",
                    "இலக்கை அடைய முடியாத campaign 5m → 15m → 1h → 4h → 1d → 1w என ஏறும்.",
                ),
            ),
            (
                ("Stop-loss", "Stop-loss"),
                ("None. A round closes only at its target.", "இல்லை. ஒரு round இலக்கில் மட்டுமே முடியும்."),
            ),
            (("Costs", "செலவுகள்"), ("0.1% a side on every buy and every sale.", "ஒவ்வொரு வாங்குதலுக்கும் விற்பனைக்கும் 0.1%.")),
        ],
    },
    "auto": {
        "title": ("Cascade-Auto", "Cascade-Auto"),
        "mark": "CA",
        "source": "engine/auto_cascade_fib.py · tools/cascade_depth_sweep.py",
        "kicker": (
            "Self-driving · half target · climbs to 4h · at most half the purse in coin",
            "தானியங்கி · அரை இலக்கு · 4h வரை · பணப்பையில் பாதி வரை மட்டுமே நாணயம்",
        ),
        "lede": (
            "The same engine with nobody marking the chart. It finds its own mother candles, keeps one 5-minute "
            "line working near price, promotes that line when it reaches an hour and starts a fresh one, and "
            "sells half of the way back instead of a quarter. This sheet runs it over every 5-minute bar of {coin}.",
            "அதே engine, ஆனால் chart-ஐ யாரும் குறிப்பதில்லை. தானே mother candle-களைக் கண்டுபிடித்து, விலைக்கு அருகில் "
            "ஒரு 5-நிமிட line-ஐ இயக்கி, அது ஒரு மணி நேரத்தை அடைந்ததும் உயர்த்தி புதியதைத் தொடங்கி, கால் பங்குக்குப் "
            "பதில் பாதி திரும்பியதும் விற்கிறது. {coin_ta}-இன் ஒவ்வொரு 5-நிமிட bar-இலும் இயக்கப்பட்டது.",
        ),
        "rules": [
            (
                ("Mother candle", "தாய் candle"),
                (
                    "Found by the engine at a confirmed 5-minute swing high, at most one start per 5-minute bar.",
                    "உறுதியான 5-நிமிட swing high-இல் engine தானே கண்டுபிடிக்கும்; ஒரு 5-நிமிட bar-க்கு ஒரு தொடக்கம் மட்டும்.",
                ),
            ),
            (
                ("Ladder", "ஏணி"),
                ("The same levels 2, 4 and 8 at 20%, 30% and 50%.", "அதே levels 2, 4, 8 — 20%, 30%, 50%."),
            ),
            (("Target", "இலக்கு"), ("Half of the way back to the mother high.", "Mother உச்சம் நோக்கி பாதி தூரம்.")),
            (
                ("Timeframe", "நேர அளவு"),
                (
                    "Climbs 5m → 15m → 1h → 4h, then stays on 4h for as long as it takes.",
                    "5m → 15m → 1h → 4h வரை ஏறி, பின் எவ்வளவு காலமானாலும் 4h-இலேயே இருக்கும்.",
                ),
            ),
            (
                ("Promotion", "உயர்வு"),
                (
                    "A line that reaches 1h becomes a major, and a new 5-minute line is started near price.",
                    "1h-ஐ அடையும் line major ஆகும்; விலைக்கு அருகில் புதிய 5-நிமிட line தொடங்கும்.",
                ),
            ),
            (
                ("Wallet", "பணப்பை"),
                (
                    "At most half the purse in coin at once; a new line is refused above that.",
                    "ஒரே நேரத்தில் பணப்பையின் பாதி வரை மட்டுமே நாணயம்; அதற்கு மேல் புதிய line மறுக்கப்படும்.",
                ),
            ),
            (
                ("Stop-loss", "Stop-loss"),
                ("None. A round closes only at its target.", "இல்லை. ஒரு round இலக்கில் மட்டுமே முடியும்."),
            ),
            (("Costs", "செலவுகள்"), ("0.1% a side on every buy and every sale.", "ஒவ்வொரு வாங்குதலுக்கும் விற்பனைக்கும் 0.1%.")),
        ],
    },
    "vrule": {
        "title": ("V-Rule", "V-Rule"),
        "mark": "VR",
        "source": "tools/rule3070_sim.py · engine/rule3070_paper.py",
        "kicker": (
            "A measured fall · a confirmed turn · one 30/70 ladder",
            "அளவிட்ட வீழ்ச்சி · உறுதியான திருப்பம் · ஒரு 30/70 ஏணி",
        ),
        "lede": (
            "It waits under a standing mother for a dip, two green candles and a confirming red — the V — then "
            "buys back through a line a quarter of the way up off the low, 30% of the pot and then 70%, four buys "
            "at most, and sells the whole ladder at one target. This sheet runs it over every 5-minute bar of {coin}.",
            "நிற்கும் mother-க்குக் கீழே ஒரு dip, இரண்டு பச்சை candle, உறுதிசெய்யும் ஒரு சிவப்பு — அதுவே V — "
            "வரும் வரை காத்திருந்து, தாழ்விலிருந்து கால் பங்கு மேலே உள்ள கோட்டைக் கடக்கும்போது pot-இன் 30% பின் 70% "
            "வாங்கி, அதிகபட்சம் நான்கு வாங்குதல், முழு ஏணியையும் ஒரே இலக்கில் விற்கிறது. {coin_ta}-இன் ஒவ்வொரு "
            "5-நிமிட bar-இலும் இயக்கப்பட்டது.",
        ),
        "rules": [
            (
                ("The V", "V அமைப்பு"),
                (
                    "A dip below the standing mother, two green candles since that dip, and the first red confirms it.",
                    "நிற்கும் mother-க்குக் கீழே dip, அதன் பின் இரண்டு பச்சை candle, முதல் சிவப்பு உறுதிசெய்யும்.",
                ),
            ),
            (
                ("Entry", "நுழைவு"),
                (
                    "A line a quarter of the way from the lowest low back to the mother high; it follows the low down.",
                    "குறைந்த தாழ்விலிருந்து mother உச்சம் நோக்கி கால் பங்கில் ஒரு கோடு; தாழ்வு இறங்கினால் அதுவும் இறங்கும்.",
                ),
            ),
            (
                ("Sizing", "அளவு"),
                (
                    "The pot is the fall's percentage of the purse, split 30% then 70%, two bands — four buys, then it holds.",
                    "Pot = வீழ்ச்சியின் சதவீதம் × பணப்பை; 30% பின் 70%, இரண்டு பட்டைகள் — நான்கு வாங்குதல், பிறகு காத்திருக்கும்.",
                ),
            ),
            (
                ("Fee gate", "கட்டண வாசல்"),
                (
                    "A buy waits until the expected gain clears 0.35% of price.",
                    "எதிர்பார்க்கும் லாபம் விலையின் 0.35%-ஐத் தாண்டும் வரை வாங்குதல் காத்திருக்கும்.",
                ),
            ),
            (
                ("Budget", "நிதி"),
                ("Never more than half the purse committed at once.", "ஒரே நேரத்தில் பணப்பையின் பாதிக்கு மேல் ஈடுபடுத்தப்படாது."),
            ),
            (
                ("Target", "இலக்கு"),
                (
                    "Average buy plus a quarter of the fall, the whole ladder at once.",
                    "சராசரி வாங்கு விலை + வீழ்ச்சியின் கால் பங்கு; முழு ஏணியும் ஒன்றாக.",
                ),
            ),
            (
                ("Stop-loss", "Stop-loss"),
                ("None. A round closes only at its target.", "இல்லை. ஒரு round இலக்கில் மட்டுமே முடியும்."),
            ),
            (("Costs", "செலவுகள்"), ("0.1% a side on every buy and every sale.", "ஒவ்வொரு வாங்குதலுக்கும் விற்பனைக்கும் 0.1%.")),
        ],
    },
    "optsell": {
        "title": ("Option Seller", "Option Seller"),
        "mark": "OS",
        "source": "engine/option_seller_paper.py · the Delta research harness",
        "kicker": (
            "Delta daily BTC options · 4 PM IST · sell into a strong move",
            "Delta தினசரி BTC options · மாலை 4 · வலுவான நகர்வில் விற்பனை",
        ),
        "lede": (
            "At 4 PM IST it checks whether {coin} has moved strongly one way over the last 2 to 12 hours. When at "
            "least 5 of 6 windows agree, it sells the at-the-money option of that day's 5:30 PM expiry in the "
            "direction of the move, and buys it back at 5:25 PM — or at once if its price doubles.",
            "மாலை 4 மணிக்கு, கடந்த 2 முதல் 12 மணி நேரத்தில் {coin_ta} ஒரே திசையில் வலுவாக நகர்ந்ததா என்று பார்க்கிறது. "
            "6-இல் குறைந்தது 5 நேர அளவுகள் ஒப்புக்கொண்டால், அன்று மாலை 5:30-க்கு முடியும் at-the-money option-ஐ "
            "நகர்வின் திசையில் விற்று, 5:25-க்கு — அல்லது அதன் விலை இரட்டிப்பானால் உடனே — திரும்ப வாங்குகிறது.",
        ),
        "rules": [
            (
                ("When", "எப்போது"),
                (
                    "Once a day at 4:00 PM IST, on Delta's own Bitcoin index.",
                    "தினமும் ஒருமுறை மாலை 4:00 மணிக்கு, Delta-வின் சொந்த Bitcoin index-இல்.",
                ),
            ),
            (
                ("The check", "சோதனை"),
                (
                    "The move over the last 2, 3, 4, 6, 8 and 12 hours. Each window votes when it moved at least 0.43%, 0.53%, 0.61%, 0.75%, 0.87% and 1.06%.",
                    "கடந்த 2, 3, 4, 6, 8, 12 மணி நேர நகர்வு. ஒவ்வொன்றும் 0.43%, 0.53%, 0.61%, 0.75%, 0.87%, 1.06% நகர்ந்தால் வாக்கு.",
                ),
            ),
            (
                ("Trade only if", "எப்போது டிரேடு"),
                (
                    "At least 5 of the 6 windows vote the same way. Otherwise nothing happens that day.",
                    "6-இல் குறைந்தது 5 ஒரே திசையில் வாக்களித்தால் மட்டும். இல்லையெனில் அன்று எதுவும் இல்லை.",
                ),
            ),
            (
                ("What it sells", "எதை விற்கிறது"),
                (
                    "Up move: the at-the-money call. Down move: the at-the-money put. Of the option that settles at 5:30 PM the same day.",
                    "மேல் நகர்வு: at-the-money call. கீழ் நகர்வு: at-the-money put. அன்று மாலை 5:30-க்கு settle ஆகும் option.",
                ),
            ),
            (
                ("Calm weekends", "அமைதியான வார இறுதி"),
                (
                    "On a Saturday or Sunday when no window moved enough, it also sells the at-the-money option, in "
                    "the direction of the last 2 hours. Calm weekdays are skipped.",
                    "சனி அல்லது ஞாயிறு அன்று எந்த நேர அளவும் போதுமான அளவு நகரவில்லை என்றால், கடந்த 2 மணி நேர "
                    "திசையில் at-the-money option-ஐயும் விற்கும். அமைதியான வார நாட்கள் தவிர்க்கப்படும்.",
                ),
            ),
            (
                ("Stop", "Stop"),
                (
                    "Buy it back at once if its price reaches twice what it was sold for.",
                    "விற்ற விலையின் இருமடங்கை அடைந்தால் உடனே திரும்ப வாங்கும்.",
                ),
            ),
            (
                ("Exit", "வெளியேற்றம்"),
                (
                    "Otherwise buy it back at 5:25 PM IST, five minutes before settlement.",
                    "இல்லையெனில் settlement-க்கு ஐந்து நிமிடம் முன், மாலை 5:25-க்கு திரும்ப வாங்கும்.",
                ),
            ),
            (("Limit", "வரம்பு"), ("At most one trade a day.", "நாளுக்கு அதிகபட்சம் ஒரு டிரேடு.")),
            (
                ("Costs", "செலவுகள்"),
                (
                    "Delta's option fee on both legs — 0.03% of the Bitcoin value, capped at 3.5% of the premium — plus 18% GST.",
                    "இரு பக்கத்திலும் Delta option கட்டணம் — Bitcoin மதிப்பின் 0.03%, premium-இன் 3.5% வரை — கூடுதலாக 18% GST.",
                ),
            ),
        ],
    },
}


# ── building a book from the data ─────────────────────────────────────


def _series(daily: dict) -> list:
    """[date, day_net, running, n] per closing day, oldest first."""
    out, run = [], 0.0
    for d in sorted(daily):
        net, n = float(daily[d][0]), int(daily[d][1])
        run += net
        out.append([d, round(net, 2), round(run, 2), n])
    return out


def _drawdown(series):
    peak = peak_at = None
    worst, frm, to = 0.0, "", ""
    for d, _net, run, _n in series:
        if peak is None or run > peak:
            peak, peak_at = run, d
        if peak - run > worst:
            worst, frm, to = peak - run, peak_at, d
    return round(worst, 2), frm, to


def _tables(series):
    by_year, by_month, by_dow = {}, {}, {}
    for d, net, _run, n in series:
        day = dt.date.fromisoformat(d)
        for table, key in ((by_year, d[:4]), (by_month, d[:7]), (by_dow, day.weekday())):
            row = table.setdefault(key, {"net": 0.0, "days": 0, "n": 0, "green": 0})
            row["net"] += net
            row["days"] += 1
            row["n"] += n
            row["green"] += 1 if net > 0 else 0
    return by_year, by_month, by_dow


def cascade_book(strategy: str, raw: dict, coin: dict) -> dict:
    daily = coin.get("daily") or {}
    if not daily:
        raise SystemExit(f"{strategy}/{coin['symbol']}: no daily book — re-run run_backtests.py")
    series = _series(daily)
    by_year, by_month, by_dow = _tables(series)
    fees = float(coin.get("fees") or 0.0)
    net = float(coin["net_pnl"])
    gross = float(coin.get("gross_pnl") or (net + fees))
    dd, dd_from, dd_to = _drawdown(series)
    ranked = sorted(series, key=lambda s: s[1])
    return {
        "family": "cascade",
        "strategy": strategy,
        "symbol": coin["symbol"],
        "capital": float(coin["capital"]),
        "fee_side": float(raw.get("fee_per_side_pct") or 0.1),
        "generated": raw.get("generated", ""),
        "series": series,
        "by_year": by_year,
        "by_month": by_month,
        "by_dow": by_dow,
        "net": net,
        "gross": gross,
        "fees": fees,
        "rounds": int(coin.get("rounds") or 0),
        "wins": int(coin.get("wins") or 0),
        "losses": int(coin.get("losses") or 0),
        "open_pnl": float(coin.get("open_pnl") or 0.0),
        "total": float(coin.get("total_pnl") or 0.0),
        "bag_cost": float(coin.get("stranded_cost") or 0.0),
        "bag_value": float(coin.get("stranded_value") or 0.0),
        "open_n": int(coin.get("open_positions") or 0),
        "peak": float(coin.get("peak_deployed") or 0.0),
        "avg_deployed": coin.get("avg_deployed"),
        "time_in": coin.get("time_in_position_pct"),
        "median_hold_d": days_from_hours(coin.get("median_hold_hours")),
        "max_hold_d": days_from_hours(coin.get("max_hold_hours")),
        "best_round": float(coin.get("best_round") or 0.0),
        "deepest": coin.get("deepest_fill_pct"),
        "escalated": coin.get("escalated"),
        "graduated": coin.get("graduated"),
        "campaigns": int(coin.get("campaigns") or 0),
        "bars": int(coin.get("bars") or 0),
        "label": coin.get("label", ""),
        "first": coin["first_day"],
        "last": coin["last_day"],
        "years": float(coin["years"]),
        "per_year_peak": float(coin.get("per_year_on_peak_pct") or 0.0),
        "dd": dd,
        "dd_from": dd_from,
        "dd_to": dd_to,
        "best10": list(reversed(ranked[-10:])),
        "worst10": ranked[:10],
        "monthly_published": coin.get("monthly") or {},
    }


def option_book(raw: dict) -> dict:
    daily = {}
    for tr in raw["trades"]:
        d = daily.setdefault(tr["day"], [0.0, 0, 0.0])
        d[0] += tr["net"]
        d[1] += 1
        d[2] += tr["fees"]
    series = _series(daily)
    by_year, by_month, by_dow = _tables(series)
    dd, dd_from, dd_to = _drawdown(series)
    nets = [tr["net"] for tr in raw["trades"]]
    wins = [x for x in nets if x > 0]
    losses = [x for x in nets if x <= 0]
    ranked = sorted(raw["trades"], key=lambda x: x["net"])
    fees = float(raw["fees"])
    net = float(raw["totals"]["net"])
    return {
        "family": "option",
        "strategy": "optsell",
        "symbol": "BTCUSDT",
        "raw": raw,
        "generated": raw["generated"],
        "series": series,
        "by_year": by_year,
        "by_month": by_month,
        "by_dow": by_dow,
        "net": net,
        "gross": net + fees,
        "fees": fees,
        "rounds": len(nets),
        "wins": len(wins),
        "losses": len(losses),
        "avg_win": sum(wins) / len(wins) if wins else 0.0,
        "avg_loss": sum(losses) / len(losses) if losses else 0.0,
        "pf": (sum(wins) / -sum(losses)) if losses and sum(losses) else None,
        "median": statistics.median(nets) if nets else 0.0,
        "minus5": sum(sorted(nets)[:-5]) if len(nets) > 5 else None,
        "best": ranked[-1],
        "worst": ranked[0],
        "capital": float(raw["avg_capital"]),
        "first": raw["first_day"],
        "last": raw["last_day"],
        "days": int(raw["days"]),
        "years": (dt.date.fromisoformat(raw["last_day"]) - dt.date.fromisoformat(raw["first_day"])).days / 365.25,
        "dd": dd,
        "dd_from": dd_from,
        "dd_to": dd_to,
        "best10": list(reversed(ranked[-10:])),
        "worst10": ranked[:10],
    }


# ── pieces every sheet draws ──────────────────────────────────────────


def section(anchor, en, ta, body, intro=None):
    head = f"<h2>{t(en, ta)}</h2>"
    if intro:
        head = f"<div>{head}<p>{t(*intro)}</p></div>"
    return f"<section id='{anchor}'><div class='shead'>{head}</div>{body}</section>"


def note(en_h, ta_h, en, ta, warn=False):
    klass = "note note-warn" if warn else "note"
    return f"<div class='{klass}'><h2 class='note-h'>{t(en_h, ta_h)}</h2><p>{t(en, ta)}</p></div>"


def kpi_grid(items):
    cells = "".join(
        f"<div class='kpi'><div class='kpi-l'>{t(*label)}</div>"
        f"<div class='kpi-v num {c}'>{value}</div><div class='kpi-s'>{t(*sub)}</div></div>"
        for label, value, c, sub in items
    )
    return f"<div class='kpis'>{cells}</div>"


def table(head, rows, klass=""):
    th = "".join(f"<th scope='col'>{t(*h)}</th>" for h in head)
    k = f" class='{klass}'" if klass else ""
    return f"<div class='tblwrap'><table{k}><thead><tr>{th}</tr></thead><tbody>{rows}</tbody></table></div>"


def prose_table(items):
    rows = "".join(f"<tr><th scope='row'>{t(*k)}</th><td>{t(*v)}</td></tr>" for k, v in items)
    return f"<div class='tblwrap'><table class='prose'><tbody>{rows}</tbody></table></div>"


def coin_switch(strategy, symbol, available):
    links = ""
    for sym in available:
        slug, en, ta = COINS[sym]
        cur = " aria-current='page'" if sym == symbol else ""
        links += (
            f"<a href='/assets/tearsheet?doc={strategy}-{slug}'{cur}>"
            f"<strong>{sym.replace('USDT', '')}</strong><small>{t(en, ta)}</small></a>"
        )
    return (
        f"<nav class='coin-switch' {t_attr('aria-label', 'Coin', 'நாணயம்')}><span>{t('Coin', 'நாணயம்')}</span>{links}</nav>"
    )


def hero(cfg, book, coin_en, coin_ta, chips):
    meta = "".join(f"<div class='meta-chip'><span>{t(*k)}</span><strong>{t(*v)}</strong></div>" for k, v in chips)
    lede_en, lede_ta = cfg["lede"]
    return f"""
<header class="document-hero">
  <div class="hero-copy">
    <p class="eyebrow"><b>CRYPTOFORGE</b>{t(*cfg["kicker"])}</p>
    <h1>{t(cfg["title"][0] + " — " + coin_en, cfg["title"][1] + " — " + coin_ta)}</h1>
    <p class="lede">{t(lede_en.format(coin=coin_en), lede_ta.format(coin_ta=coin_ta))}</p>
    <div class="document-meta">{meta}</div>
  </div>
  <div class="system-sigil" aria-hidden="true">
    <div class="sigil-ring ring-one"></div>
    <div class="sigil-ring ring-two"></div>
    <div class="sigil-ring ring-three"></div>
    <div class="sigil-core"><span>{cfg["mark"]}</span></div>
    <div class="sigil-label label-one">{book["symbol"].replace("USDT", "")}</div>
    <div class="sigil-label label-two">{sgn(book["net"], 0)}</div>
  </div>
</header>
"""


def charges(book):
    per = book["fees"] / book["rounds"] if book["rounds"] else 0.0
    if book["family"] == "option":
        intro = (
            "Every dollar between the option premium kept and the account: Delta's fee on the sale and on the "
            "buy-back — 0.03% of the Bitcoin value, capped at 3.5% of the premium — plus 18% GST on the fee.",
            "வைத்துக்கொண்ட premium-க்கும் கணக்குக்கும் இடையிலான ஒவ்வொரு டாலரும்: விற்பனையிலும் திரும்ப வாங்குதலிலும் "
            "Delta கட்டணம் — Bitcoin மதிப்பின் 0.03%, premium-இன் 3.5% வரை — கட்டணத்தின் மீது 18% GST.",
        )
        unit = ("per 1 BTC", "1 BTC-க்கு")
    else:
        intro = (
            f"Every dollar between the gross result and the account: {book['fee_side']:g}% on every buy and on every "
            f"sale, charged on both legs of every closed round.",
            f"மொத்த முடிவுக்கும் கணக்குக்கும் இடையிலான ஒவ்வொரு டாலரும்: ஒவ்வொரு வாங்குதலுக்கும் விற்பனைக்கும் "
            f"{book['fee_side']:g}%, முடிந்த ஒவ்வொரு round-இன் இரு பக்கங்களிலும்.",
        )
        unit = ("closed rounds", "முடிந்த rounds")
    rows = (
        f"<tr class='trow-total'><th scope='row'>{t(*unit)}</th>"
        f"<td class='num {cls(book['gross'])}'>{usd(book['gross'])}</td>"
        f"<td class='num neg'>{usd(-book['fees'])}</td>"
        f"<td class='num {cls(book['net'])}'><strong>{usd(book['net'])}</strong></td>"
        f"<td class='num'>{usd(per)}</td>"
        f"<td class='num'>{pct(100 * book['fees'] / book['gross'] if book['gross'] else 0)}</td></tr>"
    )
    return section(
        "charges",
        "Charges, in full",
        "கட்டணங்கள், முழுமையாக",
        table(
            (
                ("Book", "புத்தகம்"),
                ("Gross", "மொத்தம்"),
                ("Charges", "கட்டணம்"),
                ("Net", "நிகரம்"),
                ("Charges per trade", "டிரேடுக்கு கட்டணம்"),
                ("Share of gross", "மொத்தத்தில் பங்கு"),
            ),
            rows,
        ),
        intro,
    )


def cycle(book, noun):
    s = book["series"]
    days = len(s)
    green = sum(1 for d in s if d[1] > 0)
    avg = sum(d[1] for d in s) / days if days else 0.0
    best = max(s, key=lambda d: d[1])
    worst = min(s, key=lambda d: d[1])
    data = json.dumps(s, separators=(",", ":"))
    return section(
        "daily-income",  # NOT "cycle": that id belongs to the canvas the chart script looks up
        "Daily income across the whole cycle",
        "முழு சுழற்சியின் தினசரி வருமானம்",
        f"""<div class="panel">
    <div class="canvas-wrap">
      <canvas id="cycle" role="img" data-noun-en="{noun[0]}" data-noun-ta="{noun[1]}" data-series='{data}'
        {t_attr("aria-label", f"Daily profit and running total over {days} days", f"{days} நாட்களின் தினசரி லாபமும் ஓடும் மொத்தமும்")}></canvas>
      <div class="tip" id="cycle-tip" role="status"></div>
    </div>
    <div class="legend">
      <span><i style="background:var(--curve)"></i>{t("running total", "ஓடும் மொத்தம்")}</span>
      <span><i class="bar" style="background:rgba(var(--pos-fill),.55)"></i>{t("profitable day", "லாப நாள்")}</span>
      <span><i class="bar" style="background:rgba(var(--neg-fill),.55)"></i>{t("losing day", "நஷ்ட நாள்")}</span>
      <span>{t("hover or drag for any single day", "ஒரு நாளைப் பார்க்க நகர்த்துங்கள்")}</span>
    </div>
    <div class="axis"><span>{t(f"{days} days", f"{days} நாட்கள்")}</span>
      <span>{t(f"{green} green ({100 * green / days:.0f}%)", f"{green} பச்சை ({100 * green / days:.0f}%)")}</span>
      <span>{t(f"average day {r(avg)} · best {r(best[1])} · worst {r(worst[1])}", f"சராசரி நாள் {r(avg)} · சிறந்தது {r(best[1])} · மோசமானது {r(worst[1])}")}</span></div>
  </div>""",
        (
            "Every day in the record on which something closed. Bars are that day's net; the line is the running "
            "total. Days with nothing closed are left out, so the spacing is by activity, not by calendar.",
            "ஏதேனும் முடிந்த ஒவ்வொரு நாளும். கம்பிகள் அந்நாளின் நிகரம்; கோடு ஓடும் மொத்தம். எதுவும் முடியாத நாட்கள் "
            "இல்லை; ஆகவே இடைவெளி நாட்காட்டியால் அல்ல, செயல்பாட்டால்.",
        ),
    )


def ledger(book, noun):
    s = book["series"]
    days = len(s)
    green = sum(1 for d in s if d[1] > 0)
    years = sorted({d[0][:4] for d in s})
    btns = f'<button type="button" data-year="all" aria-pressed="true">{t("All", "அனைத்தும்")}</button>'
    btns += "".join(f'<button type="button" data-year="{y}" aria-pressed="false">{y}</button>' for y in years)
    rows = "".join(
        f'<tr data-year="{d[0][:4]}"><th scope="row">{d[0]}</th><td>{d[3]}</td>'
        f'<td class="{cls(d[1])}">{r(d[1])}</td><td>{r(d[2])}</td></tr>'
        for d in reversed(s)
    )
    avg = sum(d[1] for d in s) / days if days else 0.0
    best = max(s, key=lambda d: d[1])
    worst = min(s, key=lambda d: d[1])
    return f"""
<section id="ledger" data-total="{days}">
  <div class="shead"><div><h2>{t("Daily P&amp;L ledger", "தினசரி லாப-நஷ்ட பதிவேடு")}</h2>
    <p>{t(f"Every day on which something closed, newest first, with the running total beside it. Filter by year, or scroll all {days} rows.", f"ஏதேனும் முடிந்த ஒவ்வொரு நாளும், புதியது முதலில், ஓடும் மொத்தத்துடன். ஆண்டு வாரியாக வடிகட்டலாம், அல்லது {days} வரிகளையும் பார்க்கலாம்.")}</p></div></div>
  <div class="ledger-controls" id="ledger-years">{btns}</div>
  <div class="ledger-scroll" tabindex="0" role="region" {t_attr("aria-label", "Daily profit and loss ledger", "தினசரி லாப நஷ்ட பதிவேடு")}>
    <table>
      <thead><tr>
        <th scope="col">{t("Date (IST)", "தேதி (IST)")}</th>
        <th scope="col">{t(*noun)}</th>
        <th scope="col">{t("Day net", "நாளின் நிகரம்")}</th>
        <th scope="col">{t("Running total", "ஓடும் மொத்தம்")}</th>
      </tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
  <div class="ledger-foot">
    <span><span id="ledger-count">{days}</span> {t("days shown", "நாட்கள் காட்டப்படுகின்றன")}</span>
    <span>{t(f"green {green} · red {days - green}", f"பச்சை {green} · சிவப்பு {days - green}")}</span>
    <span>{t(f"average {r(avg)}", f"சராசரி {r(avg)}")}</span>
    <span>{t(f"best {r(best[1])} · worst {r(worst[1])}", f"சிறந்தது {r(best[1])} · மோசமானது {r(worst[1])}")}</span>
  </div>
</section>"""


def curve(book):
    s = book["series"]
    points = [[d[0], d[2]] for d in s]
    if len(points) < 2:
        points = [[book["first"], 0.0], [book["last"], points[0][1] if points else 0.0]]
    line, area, dd, zero_y, hi, lo = kit.curve_svg(points)
    body = (
        f"<div class='panel'><div class='chart'><svg viewBox='0 0 1040 260' preserveAspectRatio='none' role='img' "
        f"{t_attr('aria-label', 'Running total of closed profit', 'முடிந்த லாபத்தின் ஓடும் மொத்தம்')}>"
        f"<path d='{dd}' fill='rgba(var(--neg-fill),.13)'/>"
        f"<path d='{area}' fill='rgba(var(--accent-rgb),.10)'/>"
        f"<line x1='0' x2='1040' y1='{zero_y:.1f}' y2='{zero_y:.1f}' stroke='var(--line-strong)' stroke-width='1'/>"
        f"<path d='{line}' stroke='var(--accent)' stroke-width='1.6' fill='none'/>"
        f"</svg></div>"
        f"<div class='axis'><span class='num'>{book['first']}</span>"
        f"<span class='num'>{t(f'low {usd(lo, 0)} · peak {usd(hi, 0)} · shaded = below the previous high', f'குறைவு {usd(lo, 0)} · உச்சம் {usd(hi, 0)} · நிழல் = முந்தைய உச்சத்துக்குக் கீழே')}</span>"
        f"<span class='num'>{book['last']}</span></div></div>"
    )
    intro = (
        (
            "Closed profit only, one point per closing day. An open ladder has paid nothing yet, so it is not drawn "
            "here — its value is in the open bag.",
            "முடிந்த லாபம் மட்டும், ஒரு முடிவு நாளுக்கு ஒரு புள்ளி. திறந்த ஏணி இன்னும் எதுவும் தரவில்லை; அதனால் இங்கே "
            "வரையப்படவில்லை — அதன் மதிப்பு திறந்த கைவசத்தில் உள்ளது.",
        )
        if book["family"] == "cascade"
        else (
            "Net after fees, one point per trade day. Shading marks every stretch spent below the previous high.",
            "கட்டணத்துக்குப் பின் நிகரம், ஒரு டிரேடு நாளுக்கு ஒரு புள்ளி. நிழல் = முந்தைய உச்சத்துக்குக் கீழே இருந்த காலம்.",
        )
    )
    return section("curve", "Cumulative curve", "ஒட்டுமொத்த வளைவு", body, intro)


def years(book, noun):
    rows = ""
    for y in sorted(book["by_year"]):
        v = book["by_year"][y]
        rows += (
            f"<tr><th scope='row'>{y}</th><td class='num'>{v['n']}</td><td class='num'>{v['days']}</td>"
            f"<td class='num {cls(v['net'])}'><strong>{usd(v['net'])}</strong></td>"
            f"<td class='num'>{usd(v['net'] / v['n']) if v['n'] else '—'}</td></tr>"
        )
    return section(
        "years",
        "Year by year",
        "ஆண்டு வாரியாக",
        table(
            (("Year", "ஆண்டு"), noun, ("Days", "நாட்கள்"), ("Net", "நிகரம்"), ("Per trade", "டிரேடுக்கு")),
            rows,
        ),
    )


def months(book):
    bm = book["by_month"]
    ys = sorted({k[:4] for k in bm})
    top = max([abs(v["net"]) for v in bm.values()] or [1]) or 1
    head = "".join(f"<th scope='col'>{t(MON_EN[i], MON_TA[i])}</th>" for i in range(12))
    body = ""
    for y in ys:
        cells, tot = "", 0.0
        for m in range(1, 13):
            key = f"{y}-{m:02d}"
            in_window = book["first"][:7] <= key <= book["last"][:7]
            v = bm.get(key)
            if v is None:
                cells += f"<td class='num flat'>{'0' if in_window else '·'}</td>"
                continue
            tot += v["net"]
            w = min(1.0, abs(v["net"]) / top)
            cells += f"<td class='num mcell {cls(v['net'])}' style='--w:{w:.2f}'>{sgn(v['net'], 0)}</td>"
        body += f"<tr><th scope='row'>{y}<div class='kpi-s num {cls(tot)}'>{sgn(tot, 0)}</div></th>{cells}</tr>"
    return section(
        "months",
        "Month by month",
        "மாத வாரியாக",
        f"<div class='tblwrap'><table class='heat'><thead><tr><th scope='col'>{t('Year', 'ஆண்டு')}</th>{head}</tr></thead>"
        f"<tbody>{body}</tbody></table></div>",
        (
            "Net by the month it closed in. Colour is the size against the largest month. 0 is a month in the "
            "window with nothing closed; a dot is outside the window.",
            "முடிந்த மாதத்தின்படி நிகரம். நிறம் = பெரிய மாதத்துடன் ஒப்பீடு. 0 = காலத்துக்குள் எதுவும் முடியாத மாதம்; "
            "புள்ளி = காலத்துக்கு வெளியே.",
        ),
    )


def weekdays(book):
    rows = ""
    for i in range(7):
        v = book["by_dow"].get(i)
        if not v:
            continue
        rows += (
            f"<tr><th scope='row'>{t(DOW_EN[i], DOW_TA[i])}</th><td class='num'>{v['days']}</td>"
            f"<td class='num'>{pct(100 * v['green'] / v['days'])}</td>"
            f"<td class='num {cls(v['net'])}'><strong>{usd(v['net'])}</strong></td>"
            f"<td class='num'>{usd(v['net'] / v['days'])}</td></tr>"
        )
    return section(
        "weekday",
        "Which day of the week pays",
        "வாரத்தின் எந்த நாள் லாபம் தருகிறது",
        table(
            (
                ("Closing day (IST)", "முடிந்த கிழமை (IST)"),
                ("Days", "நாட்கள்"),
                ("Green", "பச்சை"),
                ("Net", "நிகரம்"),
                ("Per day", "நாளுக்கு"),
            ),
            rows,
        ),
        (
            "Crypto trades every day of the week, so all seven are here.",
            "Crypto வாரத்தின் ஏழு நாட்களும் வர்த்தகமாகிறது; ஆகவே ஏழும் இங்கே.",
        ),
    )


def best_worst(book, noun):
    def rows_days(items):
        return "".join(
            f"<tr><th scope='row'>{d[0]}</th><td class='num'>{d[3]}</td>"
            f"<td class='num {cls(d[1])}'><strong>{usd(d[1])}</strong></td><td class='num'>{usd(d[2])}</td></tr>"
            for d in items
        )

    def rows_trades(items):
        out = ""
        for x in items:
            side = t("call", "call") if x["side"] == "C" else t("put", "put")
            if x.get("leg") == "weekend-calm":
                side += " · " + t("calm weekend", "அமைதி வார இறுதி")
            why = t("stop", "stop") if x["why"] == "stop" else t("5:25 PM", "மாலை 5:25")
            out += (
                f"<tr><th scope='row'>{x['day']}</th><td>{side} {x['strike']:,}</td>"
                f"<td class='num'>{x['entry']:,.2f} → {x['exit']:,.2f}</td><td>{why}</td>"
                f"<td class='num {cls(x['net'])}'><strong>{sgn(x['net'])}</strong></td></tr>"
            )
        return out

    if book["family"] == "option":
        head = (
            ("Day", "நாள்"),
            ("Sold", "விற்றது"),
            ("Premium", "Premium"),
            ("Ended by", "முடிவு"),
            ("Net per 1 BTC", "1 BTC-க்கு நிகரம்"),
        )
        best, worst = rows_trades(book["best10"]), rows_trades(book["worst10"])
    else:
        head = (("Day (IST)", "நாள் (IST)"), noun, ("Day net", "நாளின் நிகரம்"), ("Running total", "ஓடும் மொத்தம்"))
        best, worst = rows_days(book["best10"]), rows_days(book["worst10"])
    lead_best = f"<p class='kpi-s'>{t('The ten best', 'சிறந்த பத்து')}</p>"
    lead_worst = f"<p class='kpi-s'>{t('The ten worst', 'மோசமான பத்து')}</p>"
    body = f"<div class='two-up'><div>{lead_best}{table(head, best)}</div><div>{lead_worst}{table(head, worst)}</div></div>"
    intro = (
        (
            "The ten biggest closing days and the ten smallest. No closed day here is a loss — this book never sells "
            "below its target — so the worst ten are simply its thinnest days.",
            "மிகப் பெரிய பத்து முடிவு நாட்களும் மிகச் சிறிய பத்தும். இங்கே எந்த முடிந்த நாளும் நஷ்டம் அல்ல — இந்த book "
            "இலக்குக்குக் கீழே விற்பதில்லை — ஆகவே மோசமான பத்து என்பது அதன் மிகக் குறைந்த நாட்கள்.",
        )
        if book["family"] == "cascade"
        else (
            "The ten best trades and the ten worst, per 1 BTC after fees.",
            "சிறந்த பத்து, மோசமான பத்து டிரேடுகள், 1 BTC-க்கு, கட்டணத்துக்குப் பின்.",
        )
    )
    return section("tenten", "Best ten, worst ten", "சிறந்த பத்து, மோசமான பத்து", body, intro)


def method_and_not(steps, running):
    items = "".join(f"<li>{t(en, ta)}</li>" for en, ta in steps)
    body_m = f"<div class='panel'><ol class='method'>{items}</ol></div>"
    today = section(
        "today", "What is running today", "இன்று இயங்குவது என்ன", f"<div class='panel'><p>{t(*running)}</p></div>"
    )
    method = section("method", "Method", "முறை", body_m)
    notp = section(
        "not",
        "What this document is not",
        "இந்த ஆவணம் எது அல்ல",
        "<div class='note note-warn'><p>"
        + t(
            "It is a backtest. It assumes every order filled at the recorded price, with no rejection, no partial fill "
            "and no slippage beyond the modelled costs. Live trading adds all three. An exchange can change its fees, "
            "its margin and its listings, and a coin's past behaviour is no promise about its future. Nothing here is "
            "investment advice or an offer to manage money.",
            "இது ஒரு backtest. ஒவ்வொரு order-உம் பதிவான விலையில், நிராகரிப்பு இல்லாமல், பகுதி நிறைவேற்றம் இல்லாமல், "
            "கணக்கிட்ட செலவுகளுக்கு மேல் slippage இல்லாமல் நிறைவேறியதாகக் கருதுகிறது. நேரடி வர்த்தகம் இந்த மூன்றையும் "
            "சேர்க்கும். ஒரு exchange தன் கட்டணம், margin, பட்டியலை மாற்றலாம்; ஒரு நாணயத்தின் கடந்த நடத்தை அதன் "
            "எதிர்காலத்துக்கு உத்தரவாதம் அல்ல. இங்குள்ள எதுவும் முதலீட்டு ஆலோசனையோ பணத்தை நிர்வகிக்கும் சலுகையோ அல்ல.",
        )
        + "</p></div>",
    )
    return today + method + notp


# ── the Cascade family ────────────────────────────────────────────────


def cascade_sheet(strategy, book, coin_en, coin_ta, available):
    cfg = STRATEGIES[strategy]
    b = book
    noun = ("Rounds", "Rounds")
    recent_from = f"{int(b['last'][:4]) - 1}-{b['last'][5:10]}"
    recent = sum(d[1] for d in b["series"] if d[0] >= recent_from)
    share = 100 * recent / b["net"] if b["net"] else 0.0
    green_days = len(b["series"])
    top5 = sum(d[1] for d in b["best10"][:5])
    top5_share = 100 * top5 / b["net"] if b["net"] else 0.0
    every = (b["years"] * 365.25) / green_days if green_days else 0.0
    chips = [
        (("Window", "காலம்"), (f"{b['first']} → {b['last']}", f"{b['first']} → {b['last']}")),
        (("Years", "ஆண்டுகள்"), (f"{b['years']:.1f}", f"{b['years']:.1f}")),
        (("Rounds closed", "முடிந்த rounds"), (f"{b['rounds']:,}", f"{b['rounds']:,}")),
        (("Purse", "பணப்பை"), (usd(b["capital"], 0), usd(b["capital"], 0))),
        (("Costs", "செலவுகள்"), (f"{b['fee_side']:g}% a side", f"ஒரு பக்கத்துக்கு {b['fee_side']:g}%")),
        (("Built", "உருவாக்கியது"), (b["generated"][:10], b["generated"][:10])),
    ]
    read_first = note(
        "Read this first",
        "முதலில் இதைப் படியுங்கள்",
        f"This is {coin_en} on its own — no other coin is added in anywhere on this page. It is the shipped "
        f"configuration run through the real engine over {b['bars']:,} five-minute bars, on a fixed "
        f"{usd(b['capital'], 0)} purse that is never topped up and never compounded. Every rate is quoted against "
        f"the peak capital the book actually used, not the purse. The closed rounds, the monthly book and the open "
        f"bag are checked to add up to the cent by the test suite on every change.",
        f"இது {coin_ta} மட்டும் — இந்தப் பக்கத்தில் வேறு எந்த நாணயமும் சேர்க்கப்படவில்லை. வெளியிடப்பட்ட அமைப்பு "
        f"உண்மையான engine வழியாக {b['bars']:,} ஐந்து-நிமிட bars-இல், {usd(b['capital'], 0)} நிலையான பணப்பையில் "
        f"இயக்கப்பட்டது — கூடுதல் பணம் இல்லை, கூட்டு வட்டி இல்லை. ஒவ்வொரு விகிதமும் பணப்பைக்கு அல்ல, book உண்மையில் "
        f"பயன்படுத்திய உச்ச மூலதனத்துக்கு எதிராகக் கூறப்படுகிறது. முடிந்த rounds, மாதக் கணக்கு, திறந்த கைவசம் "
        f"ஆகியவை ஒவ்வொரு மாற்றத்திலும் test suite-ஆல் சென்ட் வரை சரிபார்க்கப்படுகின்றன.",
    )
    finding = note(
        "The finding that matters most",
        "மிக முக்கியமான கண்டுபிடிப்பு",
        f"Over {b['years']:.1f} years this book closed {b['rounds']:,} rounds for {usd(b['net'])}, and every one of "
        f"them closed at its target — it has no stop-loss, so it never books a loss. The loss is not gone; it waits "
        f"in the open bag: at the last close {usd(b['bag_cost'], 0)} of {coin_en} was still held, worth "
        f"{usd(b['bag_value'], 0)} ({sgn(b['open_pnl'])}). The last twelve months earned {usd(recent)}, "
        f"{share:.0f}% of the whole, and its five best days alone made {usd(top5)} — {top5_share:.0f}% of all the "
        f"closed profit. Read the total, not the closed figure: {usd(b['total'])}, which is "
        f"{pct(b['per_year_peak'])} a year on the {usd(b['peak'], 0)} it actually used.",
        f"{b['years']:.1f} ஆண்டுகளில் இந்த book {b['rounds']:,} rounds-ஐ {usd(b['net'])}-க்கு முடித்தது; ஒவ்வொன்றும் "
        f"இலக்கிலேயே முடிந்தது — stop-loss இல்லாததால் நஷ்டம் ஒருபோதும் பதிவாவதில்லை. நஷ்டம் மறையவில்லை; அது திறந்த "
        f"கைவசத்தில் காத்திருக்கிறது: கடைசி close-இல் {usd(b['bag_cost'], 0)} மதிப்பிலான {coin_ta} இன்னும் "
        f"வைத்திருந்தது, இப்போது {usd(b['bag_value'], 0)} ({sgn(b['open_pnl'])}). கடந்த பன்னிரண்டு மாதங்கள் "
        f"{usd(recent)} ஈட்டின — மொத்தத்தில் {share:.0f}%; அதன் சிறந்த ஐந்து நாட்கள் மட்டுமே {usd(top5)} ஈட்டின — "
        f"முடிந்த லாபத்தில் {top5_share:.0f}%. முடிந்த எண்ணை அல்ல, மொத்தத்தைப் படியுங்கள்: "
        f"{usd(b['total'])} — உண்மையில் பயன்படுத்திய {usd(b['peak'], 0)}-க்கு ஆண்டுக்கு {pct(b['per_year_peak'])}.",
        warn=True,
    )
    avg_round = b["net"] / b["rounds"] if b["rounds"] else 0.0
    glance = [
        (
            ("Closed profit", "முடிந்த லாபம்"),
            usd(b["net"]),
            cls(b["net"]),
            (f"{b['rounds']:,} rounds, after fees", f"{b['rounds']:,} rounds, கட்டணத்துக்குப் பின்"),
        ),
        (
            ("Open bag", "திறந்த கைவசம்"),
            sgn(b["open_pnl"]),
            cls(b["open_pnl"]),
            (f"{usd(b['bag_cost'], 0)} of coin still held", f"{usd(b['bag_cost'], 0)} நாணயம் இன்னும் கைவசம்"),
        ),
        (
            ("Total", "மொத்தம்"),
            usd(b["total"]),
            cls(b["total"]),
            ("closed profit plus the bag at the last close", "முடிந்த லாபம் + கடைசி close-இல் கைவசம்"),
        ),
        (
            ("A year, on money used", "ஆண்டுக்கு, பயன்பட்ட பணத்தில்"),
            pct(b["per_year_peak"]),
            cls(b["per_year_peak"]),
            ("against the peak capital actually used", "உண்மையில் பயன்பட்ட உச்ச மூலதனத்துக்கு"),
        ),
        (
            ("Rounds won", "வென்ற rounds"),
            f"{b['wins']:,}/{b['rounds']:,}",
            "",
            ("every close is at target — see the risk register", "ஒவ்வொரு முடிவும் இலக்கில் — ரிஸ்க் பதிவேட்டைப் பாருங்கள்"),
        ),
        (
            ("Average round", "சராசரி round"),
            usd(avg_round),
            cls(avg_round),
            (f"best round {usd(b['best_round'])}", f"சிறந்த round {usd(b['best_round'])}"),
        ),
        (
            ("Days that paid", "லாபம் தந்த நாட்கள்"),
            f"{green_days:,}",
            "",
            (f"about one every {every:.1f} days", f"சுமார் {every:.1f} நாளுக்கு ஒன்று"),
        ),
        (("Best day", "சிறந்த நாள்"), usd(b["best10"][0][1]), "pos", (b["best10"][0][0], b["best10"][0][0])),
        (
            ("Peak capital used", "உச்ச மூலதனம்"),
            usd(b["peak"], 0),
            "",
            ("the most in the market at once", "ஒரே நேரத்தில் சந்தையில் இருந்த அதிகபட்சம்"),
        ),
        (
            ("Time in the market", "சந்தையில் இருந்த நேரம்"),
            pct(b["time_in"]) if b["time_in"] is not None else f"{b['open_n']}",
            "",
            (
                ("share of all bars holding coin", "நாணயம் வைத்திருந்த bars-இன் பங்கு")
                if b["time_in"] is not None
                else ("ladders still open at the end", "இறுதியில் திறந்த ஏணிகள்")
            ),
        ),
        (
            ("Held, entry to exit", "வைத்திருந்த காலம்"),
            f"{b['median_hold_d']:g}",
            "",
            (
                f"days, median · longest {b['max_hold_d']:,.0f} days",
                f"நாட்கள், இடைநிலை · நீளமானது {b['max_hold_d']:,.0f} நாட்கள்",
            ),
        ),
        (
            ("Fees paid", "செலுத்திய கட்டணம்"),
            usd(-b["fees"]),
            "neg",
            ("already taken out of every figure", "எல்லா எண்களிலும் ஏற்கனவே கழிக்கப்பட்டது"),
        ),
    ]
    bag_value = usd(b["bag_value"], 0)
    capital_rows = (
        f"<tr><th scope='row'>{t('Purse', 'பணப்பை')}</th><td class='num'>{usd(b['capital'], 0)}</td><td>{t('the money the book is given, never topped up', 'book-க்கு கொடுக்கப்பட்ட பணம்; கூடுதல் இல்லை')}</td></tr>"
        f"<tr><th scope='row'>{t('Peak capital used', 'உச்ச மூலதனம்')}</th><td class='num'>{usd(b['peak'], 0)}</td><td>{t('the most that was ever in coin at once', 'ஒரே நேரத்தில் நாணயத்தில் இருந்த அதிகபட்சம்')}</td></tr>"
        + (
            f"<tr><th scope='row'>{t('Average capital in coin', 'சராசரி மூலதனம்')}</th><td class='num'>{usd(b['avg_deployed'], 0)}</td><td>{t('across every bar of the window', 'காலத்தின் ஒவ்வொரு bar-இலும் சராசரி')}</td></tr>"
            if b["avg_deployed"] is not None
            else ""
        )
        + f"<tr><th scope='row'>{t('Still held at the end', 'இறுதியில் கைவசம்')}</th><td class='num'>{usd(b['bag_cost'], 0)}</td><td>{t('worth ' + bag_value + ' at the last close', 'கடைசி close-இல் மதிப்பு ' + bag_value)}</td></tr>"
    )
    capital = section(
        "capital-needed",
        "How much capital this needs",
        "இதற்கு எவ்வளவு மூலதனம் தேவை",
        f"<div class='tblwrap'><table class='prose'><tbody>{capital_rows}</tbody></table></div>",
        (
            f"Keep at least the peak capital used in the account, and expect it to be tied up for long stretches: "
            f"the longest single hold on {coin_en} ran {b['max_hold_d']:,.0f} days.",
            f"குறைந்தது உச்ச மூலதனத்தை கணக்கில் வைத்திருங்கள்; நீண்ட காலம் முடங்கியிருக்கும் என எதிர்பாருங்கள்: "
            f"{coin_ta}-இல் மிக நீண்ட ஒற்றை நிலை {b['max_hold_d']:,.0f} நாட்கள்.",
        ),
    )
    sizing_rows = ""
    for m in (1, 2, 5, 10):
        sizing_rows += (
            f"<tr><th scope='row'>{m}×</th><td class='num'>{usd(b['capital'] * m, 0)}</td>"
            f"<td class='num'>{usd(b['peak'] * m, 0)}</td>"
            f"<td class='num {cls(b['net'])}'>{usd(b['net'] * m, 0)}</td>"
            f"<td class='num {cls(b['total'])}'><strong>{usd(b['total'] * m, 0)}</strong></td>"
            f"<td class='num'>{pct(b['per_year_peak'])}</td></tr>"
        )
    sizing = section(
        "sizing",
        "Sizing up as the book earns",
        "book சம்பாதிக்கும்போது அளவை உயர்த்துதல்",
        table(
            (
                ("Size", "அளவு"),
                ("Purse", "பணப்பை"),
                ("Peak used", "உச்ச பயன்பாடு"),
                ("Closed profit", "முடிந்த லாபம்"),
                ("Total", "மொத்தம்"),
                ("A year on money used", "ஆண்டுக்கு"),
            ),
            sizing_rows,
        ),
        (
            "Fees here are a percentage, not a flat charge, so a bigger purse earns proportionally more and the rate "
            "does not change. What changes is the money tied up — and a bigger order moves a thinner market.",
            "இங்கே கட்டணம் நிலையான தொகை அல்ல, சதவீதம்; ஆகவே பெரிய பணப்பை விகிதத்துக்கு ஏற்ப அதிகம் ஈட்டும், விகிதம் "
            "மாறாது. மாறுவது முடங்கும் பணம் — பெரிய order மெல்லிய சந்தையை நகர்த்தும்.",
        ),
    )
    risks = [
        (
            ("Every closed round is a winner by construction", "முடிந்த ஒவ்வொரு round-உம் அமைப்பாலேயே வெற்றி"),
            (
                f"There is no stop-loss. A round closes only at its target, so the {b['rounds']:,} closed rounds contain "
                f"{b['losses']} losses. The win rate is not a skill measurement.",
                f"Stop-loss இல்லை. Round இலக்கில் மட்டுமே முடியும்; ஆகவே முடிந்த {b['rounds']:,} rounds-இல் {b['losses']} "
                f"நஷ்டங்கள். வெற்றி விகிதம் திறமையின் அளவீடு அல்ல.",
            ),
        ),
        (
            ("The bag is the whole risk", "கைவசம் இருப்பதே முழு ஆபத்து"),
            (
                f"{usd(b['bag_cost'], 0)} of {coin_en} was still held when the window ended, valued at the last close "
                f"at {usd(b['bag_value'], 0)}. A long fall is carried, not cut.",
                f"காலம் முடிந்தபோது {usd(b['bag_cost'], 0)} மதிப்பிலான {coin_ta} இன்னும் கைவசம்; கடைசி close-இல் "
                f"{usd(b['bag_value'], 0)}. நீண்ட வீழ்ச்சி வெட்டப்படுவதில்லை, சுமக்கப்படுகிறது.",
            ),
        ),
        (
            ("Money is held for a long time", "பணம் நீண்ட காலம் முடங்குகிறது"),
            (
                f"The median hold was {b['median_hold_d']:g} days; the longest ran {b['max_hold_d']:,.0f} days.",
                f"இடைநிலை வைத்திருப்பு {b['median_hold_d']:g} நாட்கள்; மிக நீண்டது {b['max_hold_d']:,.0f} நாட்கள்.",
            ),
        ),
        (
            ("Candle-resolution fills, no slippage", "Candle அளவில் நிறைவேற்றம், slippage இல்லை"),
            (
                "Binance spot 5-minute bars. A stop fills when a bar's high reaches it, and a bar that took an entry "
                "cannot also take the target. Fees are charged; spread and slippage are not modelled.",
                "Binance spot 5-நிமிட bars. ஒரு bar-இன் உச்சம் stop-ஐ அடைந்தால் நிறைவேறும்; நுழைவு எடுத்த bar இலக்கையும் "
                "எடுக்க முடியாது. கட்டணம் கணக்கிடப்படுகிறது; spread, slippage கணக்கிடப்படவில்லை.",
            ),
        ),
        (
            ("A fixed purse, not the live wallet", "நிலையான பணப்பை, நேரடி wallet அல்ல"),
            (
                "The purse is fixed and profit is not folded back in, so the book is measured, not compounded. Live, "
                "profit is folded in at 25% and the purse grows.",
                "பணப்பை நிலையானது; லாபம் மீண்டும் சேர்க்கப்படவில்லை — ஆகவே book அளவிடப்படுகிறது, கூட்டப்படவில்லை. "
                "நேரடியாக, லாபம் 25%-இல் சேர்க்கப்பட்டு பணப்பை வளரும்.",
            ),
        ),
    ]
    risk = section(
        "risk",
        "Risk register",
        "ரிஸ்க் பதிவேடு",
        "".join(f"<div class='note note-warn'><h2 class='note-h'>{t(*h)}</h2><p>{t(*p)}</p></div>" for h, p in risks),
    )
    snapshot_items = [(k, v) for k, v in cfg["rules"]] + [
        (("Purse", "பணப்பை"), (f"{usd(b['capital'], 0)}, fixed", f"{usd(b['capital'], 0)}, நிலையானது")),
        (("Configuration label", "அமைப்புப் பெயர்"), (b["label"], b["label"])),
        (
            ("Window", "காலம்"),
            (
                f"{b['first']} → {b['last']}, {b['bars']:,} five-minute bars",
                f"{b['first']} → {b['last']}, {b['bars']:,} ஐந்து-நிமிட bars",
            ),
        ),
        (("Measured from", "அளவிடப்பட்டது"), (cfg["source"], cfg["source"])),
    ]
    snapshot = section("config", "Recorded configuration snapshot", "பதிவான அமைப்பு", prose_table(snapshot_items))
    steps = [
        (
            f"Every 5-minute bar Binance has published for {coin_en} was replayed through the shipped engine, candle by candle.",
            f"{coin_ta}-க்கு Binance வெளியிட்ட ஒவ்வொரு 5-நிமிட bar-உம் வெளியிடப்பட்ட engine வழியாக candle-candle-ஆக மீண்டும் இயக்கப்பட்டது.",
        ),
        (
            f"The purse is {usd(b['capital'], 0)} and stays there: no top-ups, no compounding.",
            f"பணப்பை {usd(b['capital'], 0)}; அப்படியே இருக்கும்: கூடுதல் இல்லை, கூட்டு இல்லை.",
        ),
        (
            f"Every closed round is booked on the IST day it closed, net of {b['fee_side']:g}% a side.",
            f"முடிந்த ஒவ்வொரு round-உம் அது முடிந்த IST நாளில், ஒரு பக்கத்துக்கு {b['fee_side']:g}% கழித்து பதிவாகிறது.",
        ),
        (
            "Anything still held at the end is valued at the last close and reported as the open bag, never as profit.",
            "இறுதியில் கைவசம் உள்ளவை கடைசி close-இல் மதிப்பிடப்பட்டு திறந்த கைவசமாகக் காட்டப்படுகின்றன, லாபமாக அல்ல.",
        ),
    ]
    running = (
        f"{cfg['title'][0]} runs in CryptoForge on its own engine and its own capital book. This document is the "
        f"recorded backtest for {coin_en}; the live book is on the strategy page.",
        f"{cfg['title'][1]} CryptoForge-இல் தனி engine, தனி மூலதனக் கணக்குடன் இயங்குகிறது. இந்த ஆவணம் {coin_ta}-க்கான "
        f"பதிவான backtest; நேரடி book strategy பக்கத்தில் உள்ளது.",
    )
    body = (
        read_first
        + finding
        + section("glance", "The programme at a glance", "ஒரே பார்வையில்", kpi_grid(glance))
        + charges(b)
        + cycle(b, ("rounds", "rounds"))
        + ledger(b, noun)
        + curve(b)
        + years(b, noun)
        + months(b)
        + capital
        + sizing
        + risk
        + weekdays(b)
        + best_worst(b, noun)
        + snapshot
        + method_and_not(steps, running)
    )
    return cfg, chips, body


# ── the Option Seller ─────────────────────────────────────────────────


def option_sheet(book, coin_en, coin_ta, available):
    cfg = STRATEGIES["optsell"]
    b = book
    raw = b["raw"]
    oos = raw["splits"][0]
    wk = raw.get("weekend_splits", [None])[0]
    wk_tot = raw.get("weekend_totals") or {"trades": 0, "net": 0.0, "win_pct": 0.0, "worst_run": 0.0}
    st_tot = raw.get("strong_totals") or raw["totals"]
    unseen_net = oos["after"]["net"] + (wk["after"]["net"] if wk else 0.0)
    unseen_n = oos["after"]["trades"] + (wk["after"]["trades"] if wk else 0)
    noun = ("Trades", "டிரேடுகள்")
    chips = [
        (("Window", "காலம்"), (f"{b['first']} → {b['last']}", f"{b['first']} → {b['last']}")),
        (("Days", "நாட்கள்"), (f"{b['days']}", f"{b['days']}")),
        (("Trades", "டிரேடுகள்"), (f"{b['rounds']}", f"{b['rounds']}")),
        (("Size", "அளவு"), ("per 1 BTC", "1 BTC-க்கு")),
        (("Costs", "செலவுகள்"), ("Delta fees + 18% GST", "Delta கட்டணம் + 18% GST")),
        (("Built", "உருவாக்கியது"), (b["generated"], b["generated"])),
    ]
    read_first = note(
        "Read this first",
        "முதலில் இதைப் படியுங்கள்",
        f"Every figure is per 1 BTC of underlying, after fees; at the paper size of 0.1 BTC divide by ten. The rule was "
        f"chosen on the first half of the days only, and all five checks — no look-ahead, votes recounted from raw "
        f"candles, every trade recomputed by hand, stops no better than a doubling, real option prices — passed before "
        f"any number was published. Delta lists daily options on {coin_en} only for about the last 405 days.",
        f"ஒவ்வொரு எண்ணும் 1 BTC-க்கு, கட்டணத்துக்குப் பின்; 0.1 BTC paper அளவுக்கு பத்தால் வகுங்கள். விதி முதல் பாதி "
        f"நாட்களில் மட்டுமே தேர்வானது; ஐந்து சோதனைகளும் — எதிர்காலம் பார்க்கவில்லை, மூல candle-களிலிருந்து வாக்குகள் "
        f"மீண்டும் எண்ணப்பட்டன, ஒவ்வொரு டிரேடும் கையால் மறுகணக்கிடப்பட்டது, stop இருமடங்கை விட நல்லதாக நிரம்பவில்லை, "
        f"உண்மையான option விலைகள் — எந்த எண்ணும் வெளியிடும் முன் தேறின. Delta {coin_ta}-இன் தினசரி options-ஐ சுமார் "
        f"கடைசி 405 நாட்களுக்கு மட்டுமே வைத்திருக்கிறது.",
    )
    finding = note(
        "The finding that matters most",
        "மிக முக்கியமான கண்டுபிடிப்பு",
        f"Two rules, and when they pay. The strong-move rule made {sgn(oos['before']['per_trade'])} a trade on the days "
        f"it was chosen on and {sgn(oos['after']['per_trade'])} on the {oos['after']['trades']} days it had never seen — "
        f"plan on the second. The calm-weekend rule, added on 19-Sep-2026, made {sgn(wk_tot['net'], 0)} over "
        f"{wk_tot['trades']} Saturdays and Sundays. A calm WEEKDAY is the opposite: the same sale lost "
        f"{sgn(raw['day_kinds']['calm_weekday']['net'], 0)} over {raw['day_kinds']['calm_weekday']['trades']} days, "
        f"which is why it is skipped.",
        f"இரண்டு விதிகள், எப்போது பலன் தருகின்றன. வலுவான நகர்வு விதி தேர்வான நாட்களில் டிரேடுக்கு "
        f"{sgn(oos['before']['per_trade'])}, அது பார்க்காத {oos['after']['trades']} நாட்களில் {sgn(oos['after']['per_trade'])} — "
        f"இரண்டாவதை வைத்துத் திட்டமிடுங்கள். 19-செப்-2026 அன்று சேர்த்த அமைதியான வார இறுதி விதி {wk_tot['trades']} "
        f"சனி, ஞாயிறுகளில் {sgn(wk_tot['net'], 0)} ஈட்டியது. அமைதியான வார நாள் நேர்மாறானது: அதே விற்பனை "
        f"{raw['day_kinds']['calm_weekday']['trades']} நாட்களில் {sgn(raw['day_kinds']['calm_weekday']['net'], 0)} — "
        f"அதனால்தான் தவிர்க்கப்படுகிறது.",
        warn=True,
    )
    pf = f"{b['pf']:.2f}" if b["pf"] else "—"
    rod = f"{b['net'] / b['dd']:.2f}×" if b["dd"] else "—"
    glance = [
        (
            ("Net profit", "நிகர லாபம்"),
            sgn(b["net"]),
            cls(b["net"]),
            ("per 1 BTC, after every fee", "1 BTC-க்கு, எல்லா கட்டணத்துக்கும் பின்"),
        ),
        (
            ("On days it never saw", "பார்க்காத நாட்களில்"),
            sgn(unseen_net),
            cls(unseen_net),
            (
                f"{unseen_n} trades from {oos['cut']}, both rules",
                f"{oos['cut']} முதல் {unseen_n} டிரேடுகள், இரு விதிகளும்",
            ),
        ),
        (
            ("Trades", "டிரேடுகள்"),
            f"{b['rounds']}",
            "",
            (f"{b['wins']} won · {b['losses']} lost", f"{b['wins']} வெற்றி · {b['losses']} நஷ்டம்"),
        ),
        (
            ("Win rate", "வெற்றி விகிதம்"),
            pct(100 * b["wins"] / b["rounds"]),
            "",
            (
                f"win {sgn(b['avg_win'])} · loss {usd(b['avg_loss'])}",
                f"வெற்றி {sgn(b['avg_win'])} · நஷ்டம் {usd(b['avg_loss'])}",
            ),
        ),
        (("Profit factor", "லாப காரணி"), pf, "", ("money won ÷ money lost", "வென்ற பணம் ÷ இழந்த பணம்")),
        (
            ("Max drawdown", "அதிகபட்ச இறக்கம்"),
            usd(-b["dd"]),
            "neg",
            (f"{b['dd_from']} → {b['dd_to']}", f"{b['dd_from']} → {b['dd_to']}"),
        ),
        (
            ("Return / drawdown", "வருவாய் / இறக்கம்"),
            rod,
            "",
            ("profit per dollar of the worst dip", "மோசமான இறக்கத்தின் ஒரு டாலருக்கு லாபம்"),
        ),
        (
            ("Minus the best five", "சிறந்த ஐந்து இல்லாமல்"),
            sgn(b["minus5"]) if b["minus5"] is not None else "—",
            cls(b["minus5"] or 0),
            ("the book without its five biggest wins", "ஐந்து பெரிய வெற்றிகள் இல்லாமல்"),
        ),
        (
            ("Average · median trade", "சராசரி · இடைநிலை டிரேடு"),
            f"{sgn(b['net'] / b['rounds'])} · {sgn(b['median'])}",
            cls(b["net"]),
            ("after fees", "கட்டணத்துக்குப் பின்"),
        ),
        (
            ("Best · worst trade", "சிறந்த · மோசமான டிரேடு"),
            f"{sgn(b['best']['net'], 0)} · {sgn(b['worst']['net'], 0)}",
            "",
            (f"{b['best']['day']} · {b['worst']['day']}", f"{b['best']['day']} · {b['worst']['day']}"),
        ),
        (
            ("Capital per trade", "டிரேடுக்கு மூலதனம்"),
            "≈ " + usd(b["capital"], 0),
            "",
            ("0.5% margin on the Bitcoin value + the premium", "Bitcoin மதிப்பில் 0.5% margin + premium"),
        ),
        (
            ("Fees paid", "செலுத்திய கட்டணம்"),
            usd(-b["fees"], 0),
            "neg",
            ("both legs, GST included", "இரு பக்கமும், GST உட்பட"),
        ),
    ]
    split_rows = ""
    for s in raw["splits"] + raw.get("weekend_splits", []):
        for i, (part_en, part_ta, st) in enumerate(
            (
                (f"before {s['cut']}", f"{s['cut']}-க்கு முன்", s["before"]),
                (f"from {s['cut']}", f"{s['cut']} முதல்", s["after"]),
            )
        ):
            label = (
                f"<td rowspan='2' class='label'><strong>{t(s['label_en'], s['label_ta'])}</strong></td>"
                if i == 0
                else ""
            )
            split_rows += (
                f"<tr>{label}<td class='num'>{t(part_en, part_ta)}</td><td class='num'>{st['trades']}</td>"
                f"<td class='num'>{pct(st['win_pct'])}</td><td class='num {cls(st['per_trade'])}'>{sgn(st['per_trade'])}</td>"
                f"<td class='num {cls(st['net'])}'><strong>{sgn(st['net'], 0)}</strong></td><td class='num neg'>{usd(-st['worst_run'])}</td></tr>"
            )
    unseen = section(
        "unseen",
        "Chosen on one half, tested on the other",
        "ஒரு பாதியில் தேர்வு, மறு பாதியில் சோதனை",
        table(
            (
                ("How it was split", "எப்படி பிரிக்கப்பட்டது"),
                ("Part", "பகுதி"),
                ("Trades", "டிரேடுகள்"),
                ("Won", "வெற்றி"),
                ("Per trade", "டிரேடுக்கு"),
                ("Total", "மொத்தம்"),
                ("Worst run", "மோசமான தொடர்"),
            ),
            split_rows,
        ),
        (
            "The strong-move rule's settings were chosen on the first half of the days only; the second half is what it made "
            "on days it had never seen, and moving the split to one third or two thirds keeps that part profitable. The "
            "calm-weekend rule was stated before any variant was looked at, and both of its halves paid.",
            "வலுவான நகர்வு விதியின் அமைப்புகள் முதல் பாதி நாட்களில் மட்டுமே தேர்வானவை; இரண்டாம் பாதி அது பார்க்காத "
            "நாட்கள். பிரிவை மூன்றில் ஒன்று அல்லது இரண்டுக்கு நகர்த்தினாலும் அது லாபத்தில் இருக்கிறது. அமைதியான வார இறுதி "
            "விதி எந்த மாற்றத்தையும் பார்க்கும் முன் வரையறுக்கப்பட்டது; அதன் இரு பாதிகளும் லாபம் தந்தன.",
        ),
    )
    k = raw["day_kinds"]
    kinds = ""
    for key, en, ta in (
        ("strong", "Strong move — 5 or 6 windows agree (traded)", "வலுவான நகர்வு — 5 அல்லது 6 ஒப்புதல் (டிரேடு)"),
        ("weekend_calm", "Calm Saturday or Sunday (traded)", "அமைதியான சனி அல்லது ஞாயிறு (டிரேடு)"),
        ("mixed", "Mixed — 1 to 4 windows (skipped)", "கலப்பு — 1 முதல் 4 (தவிர்க்கப்பட்டது)"),
        ("calm_weekday", "Calm weekday (skipped)", "அமைதியான வார நாள் (தவிர்க்கப்பட்டது)"),
    ):
        st = k[key]
        kinds += (
            f"<tr><th scope='row'>{t(en, ta)}</th><td class='num'>{st['trades']}</td><td class='num'>{pct(st['win_pct'])}</td>"
            f"<td class='num'>{st['stops']}</td><td class='num'>{k['avg_premium'][key]:,.0f}</td>"
            f"<td class='num {cls(st['per_trade'])}'>{sgn(st['per_trade'])}</td>"
            f"<td class='num {cls(st['net'])}'><strong>{sgn(st['net'], 0)}</strong></td><td class='num neg'>{usd(-st['worst_run'])}</td></tr>"
        )
    skips = section(
        "skips",
        "The days it skips",
        "தவிர்க்கும் நாட்கள்",
        table(
            (
                ("Kind of day at 4 PM", "மாலை 4 மணி நாள் வகை"),
                ("Days", "நாட்கள்"),
                ("Won", "வெற்றி"),
                ("Stopped", "Stop ஆனவை"),
                ("Premium", "Premium"),
                ("Per trade", "டிரேடுக்கு"),
                ("Total", "மொத்தம்"),
                ("Worst run", "மோசமான தொடர்"),
            ),
            kinds,
        ),
        (
            "Every kind of day at 4 PM, and what the same sale made on it. After a strong move the option is dearer, so "
            "there is more premium to keep. A calm weekend stays calm, so a small premium is still kept. A calm weekday "
            "is often a pause before a move, and the stop fires on half of them.",
            "மாலை 4 மணியின் ஒவ்வொரு நாள் வகையும், அதே விற்பனை அதில் ஈட்டியதும். வலுவான நகர்வுக்குப் பின் option விலை "
            "அதிகம்; வைத்துக்கொள்ள அதிக premium. அமைதியான வார இறுதி அமைதியாகவே இருக்கும்; சிறிய premium-உம் "
            "கிடைக்கும். அமைதியான வார நாள் பெரும்பாலும் ஒரு நகர்வுக்கு முன் இடைவெளி; அவற்றில் பாதியில் stop அடிக்கிறது.",
        ),
    )
    prem = raw["day_kinds"]["avg_premium"]["strong"]
    margin_s, prem_s = usd(b["capital"] - prem, 0), usd(prem, 0)
    cap_s, cap10_s, keep_s = usd(b["capital"], 0), usd(b["capital"] / 10, 0), usd(b["capital"] + b["dd"], 0)
    cap_items = [
        (
            ("Margin", "Margin"),
            (
                f"0.5% of the Bitcoin value — about {margin_s} per 1 BTC",
                f"Bitcoin மதிப்பின் 0.5% — 1 BTC-க்கு சுமார் {margin_s}",
            ),
        ),
        (
            ("Premium", "Premium"),
            (
                f"the option price, held against the sale — about {prem_s} per 1 BTC on an average trade day",
                f"Option விலை, விற்பனைக்கு எதிராக வைக்கப்படும் — சராசரி டிரேடு நாளில் 1 BTC-க்கு சுமார் {prem_s}",
            ),
        ),
        (
            ("Capital per trade", "டிரேடுக்கு மூலதனம்"),
            (f"about {cap_s} per 1 BTC — {cap10_s} at 0.1 BTC", f"1 BTC-க்கு சுமார் {cap_s} — 0.1 BTC-இல் {cap10_s}"),
        ),
        (
            ("Keep in the account", "கணக்கில் வைத்திருக்க"),
            (
                f"at least the capital plus the worst losing run: {keep_s} per 1 BTC",
                f"குறைந்தது மூலதனம் + மோசமான தொடர் இழப்பு: 1 BTC-க்கு {keep_s}",
            ),
        ),
    ]
    capital = section(
        "capital-needed",
        "How much capital this needs",
        "இதற்கு எவ்வளவு மூலதனம் தேவை",
        prose_table(cap_items),
        (
            "An estimate: paper trading never asks Delta for a margin figure, and Delta can change its margin.",
            "இது மதிப்பீடு: paper trading Delta-விடம் margin கேட்பதில்லை; Delta தன் margin-ஐ மாற்றலாம்.",
        ),
    )
    size_rows = ""
    for sz in (0.1, 0.25, 0.5, 1.0):
        size_rows += (
            f"<tr><th scope='row'>{sz:g} BTC</th><td class='num'>{int(round(sz / 0.001))}</td>"
            f"<td class='num'>{usd(b['capital'] * sz, 0)}</td>"
            f"<td class='num {cls(unseen_net)}'>{sgn(unseen_net * sz, 0)}</td>"
            f"<td class='num neg'>{usd(-b['dd'] * sz, 0)}</td>"
            f"<td class='num {cls(b['worst']['net'])}'>{sgn(b['worst']['net'] * sz, 0)}</td></tr>"
        )
    sizing = section(
        "sizing",
        "Sizing up as the book earns",
        "book சம்பாதிக்கும்போது அளவை உயர்த்துதல்",
        table(
            (
                ("Size", "அளவு"),
                ("Contracts", "Contracts"),
                ("Capital per trade", "டிரேடுக்கு மூலதனம்"),
                ("Unseen half made, both rules", "பார்க்காத பாதி, இரு விதிகளும்"),
                ("Worst losing run", "மோசமான தொடர் இழப்பு"),
                ("Worst single trade", "மோசமான ஒற்றை டிரேடு"),
            ),
            size_rows,
        ),
        (
            "Fees are a percentage, so money scales straight with size. Grow only after the paper book has matched the "
            "unseen-half number for a couple of months.",
            "கட்டணம் சதவீதம்; ஆகவே பணம் அளவுடன் நேராக உயரும். Paper book சில மாதங்கள் பார்க்காத பாதி எண்ணுக்கு இணையாக "
            "வந்த பின்னரே அளவை உயர்த்துங்கள்.",
        ),
    )
    risks = [
        (
            ("A small sample", "சிறிய மாதிரி"),
            (
                f"{b['rounds']} trades in {b['days']} days, and only {oos['after']['trades']} on unseen days. Enough to say the idea is not noise, not enough to promise a number.",
                f"{b['days']} நாட்களில் {b['rounds']} டிரேடுகள்; பார்க்காத நாட்களில் {oos['after']['trades']} மட்டும். யோசனை தற்செயல் அல்ல என்று சொல்ல போதும்; ஒரு எண்ணை உறுதியளிக்கப் போதாது.",
            ),
        ),
        (
            ("Tested at the mark, not the real quotes", "Mark விலையில் சோதனை, உண்மையான quote-இல் அல்ல"),
            (
                "The backtest sells and buys back at Delta's mark price with no slippage. A real order sells at the bid and buys back at the ask. The paper book records both on every trade.",
                "Backtest Delta-வின் mark விலையில் slippage இல்லாமல் விற்று வாங்குகிறது. உண்மையான order bid-இல் விற்று ask-இல் வாங்கும். Paper book ஒவ்வொரு டிரேடிலும் இரண்டையும் பதிவு செய்கிறது.",
            ),
        ),
        (
            ("The stop is judged on 1-minute candles", "Stop 1-நிமிட candle-இல் மதிப்பிடப்படுகிறது"),
            (
                "Live, the price is checked every 15 seconds, so a fast spike can be caught late or missed. The paper book flags every such case.",
                "நேரடியாக விலை 15 வினாடிக்கு ஒருமுறை பார்க்கப்படுகிறது; வேகமான உயர்வு தாமதமாகப் பிடிபடலாம் அல்லது தவறலாம். Paper book அத்தகைய ஒவ்வொன்றையும் குறிக்கிறது.",
            ),
        ),
        (
            ("A loss is large next to the capital", "இழப்பு மூலதனத்துடன் ஒப்பிட பெரியது"),
            (
                f"The worst single trade lost {usd(b['worst']['net'])} per 1 BTC against about {usd(b['capital'], 0)} of capital.",
                f"மோசமான ஒற்றை டிரேடு 1 BTC-க்கு {usd(b['worst']['net'])} இழந்தது; மூலதனம் சுமார் {usd(b['capital'], 0)}.",
            ),
        ),
        (
            ("One venue, a rolling window", "ஒரு இடம், நகரும் காலம்"),
            (
                "Delta keeps about 405 days of option history and drops the oldest day each day, so a rebuild next month covers a slightly different window.",
                "Delta சுமார் 405 நாள் option வரலாற்றை வைத்திருந்து தினமும் பழைய நாளை நீக்குகிறது; அடுத்த மாத மறுகட்டமைப்பு சற்று வேறு காலத்தை உள்ளடக்கும்.",
            ),
        ),
    ]
    risk = section(
        "risk",
        "Risk register",
        "ரிஸ்க் பதிவேடு",
        "".join(f"<div class='note note-warn'><h2 class='note-h'>{t(*h)}</h2><p>{t(*p)}</p></div>" for h, p in risks),
    )
    trade_rows = ""
    for x in reversed(raw["trades"]):
        side = t("call", "call") if x["side"] == "C" else t("put", "put")
        if x.get("leg") == "weekend-calm":
            side += " · " + t("calm weekend", "அமைதி வார இறுதி")
        why = t("stop", "stop") if x["why"] == "stop" else t("5:25 PM", "மாலை 5:25")
        trade_rows += (
            f"<tr data-year='{x['day'][:4]}'><th scope='row'>{x['day']}</th><td>{side} {x['strike']:,}</td>"
            f"<td class='num'>{x['btc']:,.0f}</td><td class='num'>{x['votes']}/6</td>"
            f"<td class='num'>{x['entry']:,.2f}</td><td class='num'>{x['exit']:,.2f}</td><td>{why}</td>"
            f"<td class='num {cls(x['net'])}'><strong>{sgn(x['net'])}</strong></td>"
            f"<td class='num {cls(x['net'])}'>{sgn(x['net'] / 10)}</td></tr>"
        )
    every_trade = section(
        "trades",
        "Every trade",
        "ஒவ்வொரு டிரேடும்",
        table(
            (
                ("Day", "நாள்"),
                ("Sold", "விற்றது"),
                ("BTC at 4 PM", "மாலை 4 BTC"),
                ("Agreed", "ஒப்புதல்"),
                ("Sold at", "விற்ற விலை"),
                ("Bought back", "திரும்ப வாங்கியது"),
                ("Why", "ஏன்"),
                ("Per 1 BTC", "1 BTC-க்கு"),
                ("At 0.1 BTC", "0.1 BTC-இல்"),
            ),
            trade_rows,
        ),
        ("Newest first, after fees.", "புதியது முதலில், கட்டணத்துக்குப் பின்."),
    )
    snapshot_items = [(k2, v2) for k2, v2 in cfg["rules"]] + [
        (
            ("Window", "காலம்"),
            (f"{b['first']} → {b['last']}, {b['days']} days", f"{b['first']} → {b['last']}, {b['days']} நாட்கள்"),
        ),
        (("Measured from", "அளவிடப்பட்டது"), (cfg["source"], cfg["source"])),
    ]
    snapshot = section("config", "Recorded configuration snapshot", "பதிவான அமைப்பு", prose_table(snapshot_items))
    steps = [
        (
            "Every day Delta still publishes was replayed at 4 PM IST from Delta's own 1-minute index and option candles.",
            "Delta இன்னும் வெளியிடும் ஒவ்வொரு நாளும் Delta-வின் சொந்த 1-நிமிட index, option candle-களில் மாலை 4 மணிக்கு மீண்டும் இயக்கப்பட்டது.",
        ),
        (
            "The sale is priced at the first real 1-minute bar at or after 4 PM; the stop fires on a 1-minute high at twice the premium; otherwise the buy-back is at 5:25 PM.",
            "விற்பனை மாலை 4 அல்லது அதன் பின் முதல் உண்மையான 1-நிமிட bar-இல்; stop 1-நிமிட உச்சம் premium-இன் இருமடங்கை அடைந்தால்; இல்லையெனில் மாலை 5:25-க்கு திரும்ப வாங்குதல்.",
        ),
        (
            "Fees are Delta's schedule on both legs with 18% GST. Money is per 1 BTC of underlying.",
            "கட்டணம் இரு பக்கமும் Delta அட்டவணைப்படி, 18% GST உடன். பணம் 1 BTC-க்கு.",
        ),
        (
            "The settings were chosen on the first half of the days, then scored on the second, and five independent checks passed before publication.",
            "அமைப்புகள் முதல் பாதியில் தேர்வாகி இரண்டாம் பாதியில் மதிப்பிடப்பட்டன; வெளியிடும் முன் ஐந்து சுயாதீன சோதனைகள் தேறின.",
        ),
    ]
    running = (
        "The Option Seller runs in CryptoForge on PAPER only, at 0.1 BTC, recording every trade at the mark and at the real quotes. Nothing here has traded real money.",
        "Option Seller CryptoForge-இல் PAPER மட்டும், 0.1 BTC-இல் இயங்குகிறது; ஒவ்வொரு டிரேடையும் mark-இலும் உண்மையான quote-இலும் பதிவு செய்கிறது. இதில் எதுவும் உண்மையான பணத்தில் வர்த்தகம் ஆகவில்லை.",
    )
    body = (
        read_first
        + finding
        + section("glance", "The programme at a glance", "ஒரே பார்வையில்", kpi_grid(glance))
        + charges(b)
        + cycle(b, ("trades", "டிரேடுகள்"))
        + ledger(b, noun)
        + curve(b)
        + years(b, noun)
        + months(b)
        + capital
        + sizing
        + unseen
        + skips
        + risk
        + weekdays(b)
        + best_worst(b, noun)
        + every_trade
        + snapshot
        + method_and_not(steps, running)
    )
    return cfg, chips, body


# ── the document ──────────────────────────────────────────────────────


def document(cfg, chips, body, book, coin_en, coin_ta, available):
    return f"""<div class="reading-progress" aria-hidden="true"><span id="reading-progress-bar"></span></div>
<div class="wrap">
{hero(cfg, book, coin_en, coin_ta, chips)}
{coin_switch(book["strategy"], book["symbol"], available)}
<div class="reader-toolbar">
  <label class="document-search" for="tearsheet-search">
    <svg viewBox="0 0 24 24"><circle cx="11" cy="11" r="7"/><path d="M20 20l-4-4"/></svg>
    <input id="tearsheet-search" type="search" autocomplete="off"
           placeholder="Search this tearsheet"
           data-ph-en="Search this tearsheet" data-ph-ta="இந்த ஆவணத்தில் தேடு">
  </label>
  <span class="search-status" id="search-status"></span>
  <div class="langbar" id="langbar" role="group" {t_attr("aria-label", "Language", "மொழி")}>
    <button type="button" data-lang="en" aria-selected="true">EN</button>
    <button type="button" data-lang="ta" aria-selected="false">தமிழ்</button>
  </div>
</div>
<div class="reader-layout">
  <nav id="document-toc" {t_attr("aria-label", "Contents", "உள்ளடக்கம்")}></nav>
  <article class="document-body" id="document-body">{body}
    <p class="kpi-s">{t("Measured from " + cfg["source"], cfg["source"] + " மூலம் அளவிடப்பட்டது")} · {book["generated"][:10]}</p>
  </article>
</div>
</div>
"""


def sheets():
    """Yield (key, strategy, symbol, html) for every sheet there is data for."""
    for strategy in ("hybrid", "auto", "vrule"):
        path = DATA_DIR / f"{strategy}_report_data.json"
        if not path.exists():
            continue
        raw = json.load(open(path, encoding="utf-8"))
        coins = [c for c in raw["coins"] if c["symbol"] in COINS]
        available = [c["symbol"] for c in coins]
        for coin in coins:
            slug, en, ta = COINS[coin["symbol"]]
            book = cascade_book(strategy, raw, coin)
            cfg, chips, body = cascade_sheet(strategy, book, en, ta, available)
            yield f"{strategy}-{slug}", strategy, coin["symbol"], document(cfg, chips, body, book, en, ta, available)
    path = DATA_DIR / "optsell_report_data.json"
    if path.exists():
        raw = json.load(open(path, encoding="utf-8"))
        book = option_book(raw)
        _slug, en, ta = COINS["BTCUSDT"]
        cfg, chips, body = option_sheet(book, en, ta, ["BTCUSDT"])
        yield "optsell-btc", "optsell", "BTCUSDT", document(cfg, chips, body, book, en, ta, ["BTCUSDT"])


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    for strategy in ("hybrid", "auto", "vrule", "optsell"):
        css = kit.recolour(kit.STYLE, strategy).replace("{{", "{").replace("}}", "}") + LANG_CSS + SHEET_CSS
        (STATIC_DIR / f"tearsheet-{strategy}.css").write_text(css.rstrip("\n") + "\n", encoding="utf-8")
    js = "\n".join(block.replace("<script>", "").replace("</script>", "") for block in (kit.READER_JS, LANG_JS))
    (STATIC_DIR / "tearsheet.js").write_text((js + "\n" + CYCLE_JS).rstrip("\n") + "\n", encoding="utf-8")
    for key, _strategy, _symbol, html in sheets():
        path = OUT_DIR / f"{key}-tearsheet.html"
        path.write_text(html, encoding="utf-8")
        print(f"{key:<12} → {os.path.relpath(path, _REPO)}  ({path.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
