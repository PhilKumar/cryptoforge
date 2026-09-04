"""tools/tearsheet/build_sheets.py — render the three CryptoForge tearsheets.

One builder, three documents. PhilForge keeps a parent and its children in
separate files because each of its sheets shows a different shape of book; here
the three strategies come out of one runner in one shape, so the difference
between the sheets is their NUMBERS and their WORDS, and everything else is
shared by construction rather than by copy-paste. The look lives in
`sheet_kit.py`; nothing below invents a class.

    .venv/bin/python tools/tearsheet/build_sheets.py           # all three
    .venv/bin/python tools/tearsheet/build_sheets.py hybrid

Reads tools/tearsheet/data/<key>_report_data.json (written by run_backtests.py)
and writes docs/assets/<key>-tearsheet.html — a FRAGMENT, which app.py wraps in
its own shell so the workspace keeps ownership of the theme.
"""

from __future__ import annotations

import json
import os
import pathlib
import sys

_HERE = pathlib.Path(__file__).resolve().parent
_REPO = _HERE.parent.parent
sys.path.insert(0, str(_HERE))

import sheet_kit as kit  # noqa: E402
from i18n import LANG_CSS, LANG_JS, t, t_attr  # noqa: E402

# `table.heat` is the one rule the shared kit deliberately leaves to the sheet
# that draws it — the year grid is the only table on this page that wants a
# fixed, narrow, centred numeric column, and no other sheet in the family has
# one. Without it the year table renders as an ordinary left-aligned table and
# a decade of numbers will not fit the card.
EXTRA_CSS = """
table.heat { table-layout:fixed; width:100%; }
table.heat th, table.heat td { text-align:center; padding:7px 4px; font-size:12px; }
table.heat th:first-child, table.heat td:first-child { text-align:left; width:76px; }
table.heat td.num { font-family:var(--mono); font-variant-numeric:tabular-nums; }
table.heat tbody tr:nth-child(even) { background:var(--surface-2); }
"""

OUT_DIR = _REPO / "docs" / "assets"
DATA_DIR = _HERE / "data"

COIN_NAME = {"BTCUSDT": "Bitcoin", "ETHUSDT": "Ether", "SOLUSDT": "Solana", "PAXGUSDT": "PAX Gold"}
COIN_TA = {"BTCUSDT": "பிட்காயின்", "ETHUSDT": "ஈதர்", "SOLUSDT": "சொலானா", "PAXGUSDT": "பாக்ஸ் கோல்டு"}


def usd(n, dp=2):
    """Money, always signed the way a ledger signs it."""
    try:
        v = float(n)
    except (TypeError, ValueError):
        return "—"
    return f"{'-' if v < 0 else ''}${abs(v):,.{dp}f}"


def pct(n, dp=1):
    try:
        return f"{float(n):,.{dp}f}%"
    except (TypeError, ValueError):
        return "—"


# ── the words each sheet carries ─────────────────────────────────────
# Only these differ between the three documents. Every one of them is a claim
# about what the SHIPPED engine does, checked against the module named beside
# it; a sheet that describes a configuration the product does not run is a
# brochure, and this file refuses to be one.

SHEETS = {
    "hybrid": {
        "title_en": "Cascade Hybrid",
        "mark": "CH",
        "title_ta": "Cascade Hybrid",
        "file": "cascade-hybrid-tearsheet.html",
        "source": "engine/cascade.py",
        "kicker_en": "Hand-driven mother candles · levels 2/4/8 · quarter target",
        "kicker_ta": "கையால் தேர்ந்தெடுக்கப்பட்ட mother candle · levels 2/4/8 · கால் target",
        "lede_en": (
            "Phil marks a mother candle; the engine draws the ladder beneath it, funds three rungs "
            "at 20/30/50 of the fall, and sells a quarter of the way back to the high. This is that "
            "book, run candle by candle over every 5-minute bar Binance has published for each coin."
        ),
        "lede_ta": (
            "Phil ஒரு mother candle-ஐ குறிக்கிறார்; engine அதற்குக் கீழே ladder வரைந்து, வீழ்ச்சியின் "
            "20/30/50 விகிதத்தில் மூன்று rung-களுக்கு நிதி அளித்து, உயர்வை நோக்கி கால் பங்கு திரும்பியதும் "
            "விற்கிறது. ஒவ்வொரு நாணயத்தின் ஒவ்வொரு 5-நிமிட bar-லும் இந்த book இயக்கப்பட்டது."
        ),
        "rules": [
            ("Mother candle", "தாய் candle", "Chosen by hand, on the chart. The engine never picks one for this book."),
            ("Ladder", "ஏணி", "Fib levels 2 / 4 / 8 below the mother, funded 20% / 30% / 50% of the fall."),
            ("Entry", "நுழைவு", "Levels 2 and 4 rest as buy stops above a falling market; level 8 is a resting limit."),
            (
                "Target",
                "இலக்கு",
                "A quarter of the way back from the average entry to the mother high (TP_FIB_LEVEL 0.25).",
            ),
            ("Escalation", "உயர்வு", "A campaign that cannot reach its target climbs 5m → 15m → 1h → 4h → 1d → 1w."),
            ("Costs", "செலவுகள்", "0.1% a side, charged on both legs of every round."),
        ],
    },
    "auto": {
        "title_en": "Cascade-Auto",
        "mark": "CA",
        "title_ta": "Cascade-Auto",
        "file": "cascade-auto-tearsheet.html",
        "source": "engine/auto_cascade_fib.py",
        "kicker_en": "Self-driving · half target · climbs to 4h · folds profit at 25%",
        "kicker_ta": "தானியங்கி · அரை target · 4h வரை · 25%-இல் லாபம் மடிப்பு",
        "lede_en": (
            "The same engine, with nobody marking the chart. It finds its own mother candles, keeps one "
            "5-minute line working near price, graduates that line to major when it reaches an hour and "
            "anchors a fresh one, and sells half the way back instead of a quarter."
        ),
        "lede_ta": (
            "அதே engine, ஆனால் chart-ஐ யாரும் குறிக்கவில்லை. இது தானாகவே mother candle-களைக் கண்டறிந்து, "
            "விலைக்கு அருகில் ஒரு 5-நிமிட line-ஐ வேலையில் வைத்து, அது ஒரு மணி நேரத்தை அடைந்ததும் major-ஆக "
            "உயர்த்தி புதிய ஒன்றை நங்கூரமிடுகிறது; கால் பங்குக்குப் பதிலாக அரை பங்கு திரும்பியதும் விற்கிறது."
        ),
        "rules": [
            (
                "Mother candle",
                "தாய் candle",
                "Found by the driver at a confirmed 5-minute swing high, one start per 5m bar.",
            ),
            ("Ladder", "ஏணி", "The same 2 / 4 / 8 ladder at 20% / 30% / 50% — the engine underneath is unchanged."),
            (
                "Target",
                "இலக்கு",
                "HALF the way back to the mother high (TP_FIB_LEVEL 0.5). The one change that earned more on every window tested.",
            ),
            (
                "Climb",
                "ஏறுதல்",
                "5m → 15m → 1h → 4h, then it stops and keeps trading the 4h rung for as long as it takes.",
            ),
            (
                "Graduation",
                "பட்டமளிப்பு",
                "Reaching 1h makes the working line a major; a new 5m line is anchored so something always fights the near move.",
            ),
            (
                "Wallet",
                "பணப்பை",
                "At most half the purse in coin at once; closed profit folds in at 25% and the cap grows with it.",
            ),
        ],
        "extra_limits": [
            (
                "The wallet rule is only half-kept here",
                "பணப்பை விதி இங்கு பாதியே கடைப்பிடிக்கப்படுகிறது",
                "Live, this book holds at most half the purse in coin and folds closed profit back in at 25%. "
                "This page measures it on a FIXED $1,000 book instead, with nothing folded back — because the "
                "harness sizes every new line off the whole pot, so a compounding pot has no ceiling. Measured "
                "the compounding way, peak capital reached $8,520 on BTC, $40,414 on ETH and $77,590 on SOL "
                "against that same $1,000 purse; with no wallet refusal at all it reached $107,889 on ETH and "
                "$341,229 on SOL. Those describe a book with no wallet, not this one. The fixed book is the "
                "honest, comparable measurement — and it is the same book the other two sheets are measured on.",
            ),
        ],
    },
    "vrule": {
        "title_en": "The V-Rule",
        "mark": "VR",
        "title_ta": "V-Rule",
        "file": "vrule-tearsheet.html",
        "source": "tools/rule3070_sim.py · engine/rule3070_paper.py",
        "kicker_en": "A measured fall · a confirmed turn · one 30/70 ladder",
        "kicker_ta": "அளவிடப்பட்ட வீழ்ச்சி · உறுதிசெய்யப்பட்ட திருப்பம் · ஒரு 30/70 ஏணி",
        "lede_en": (
            "It waits under a standing mother for a dip, two greens and a confirming red — the V — then "
            "buys back through a line a quarter of the way up off the low, 30% of the pot and then 70%, "
            "four buys at most, and sells the whole ladder at one target."
        ),
        "lede_ta": (
            "நிற்கும் mother-க்குக் கீழே ஒரு dip, இரண்டு பச்சை மற்றும் உறுதிசெய்யும் ஒரு சிவப்பு — அதுவே V — "
            "அதன் பிறகு தாழ்விலிருந்து கால் பங்கு மேலே உள்ள ஒரு கோட்டைக் கடக்கும்போது வாங்குகிறது; pot-இன் 30% "
            "பின்னர் 70%, அதிகபட்சம் நான்கு வாங்குதல்கள், முழு ladder-ஐயும் ஒரே target-இல் விற்கிறது."
        ),
        "rules": [
            (
                "The V",
                "V அமைப்பு",
                "A dip below the standing mother, two green candles since that dip, and the first red confirms it.",
            ),
            (
                "Entry",
                "நுழைவு",
                "A line a quarter of the way from the lowest low back to the mother high; it trails the low down.",
            ),
            (
                "Sizing",
                "அளவு",
                "The pot is the fall percentage of the purse, split 30% then 70%, two bands — four buys, then it holds.",
            ),
            (
                "Fee gate",
                "கட்டண வாசல்",
                "A buy waits until the expected win clears 0.35% of price, so a win cannot be a donation.",
            ),
            ("Budget", "நிதி", "Never more than half the purse committed at once; profit folds in at 25%."),
            (
                "Target",
                "இலக்கு",
                "Average buy plus a quarter of the fall, the whole ladder at once, net of 0.1% a side.",
            ),
        ],
    },
}


# ── pieces ───────────────────────────────────────────────────────────


def hero(cfg: dict, book: dict, span: str) -> str:
    coins = book["coins"]
    total = sum(c["total_pnl"] for c in coins)
    rounds = sum(c.get("rounds", 0) for c in coins)
    chips = [
        ("Window", "காலம்", span),
        ("Coins", "நாணயங்கள்", f"{len(coins)}"),
        ("Rounds closed", "முடிந்த சுற்றுகள்", f"{rounds:,}"),
        ("Purse each", "ஒவ்வொன்றுக்கும்", usd(book["capital"], 0)),
        ("Costs", "செலவுகள்", f"{book['fee_per_side_pct']:g}% a side"),
        ("Built", "உருவாக்கப்பட்டது", book["generated"]),
    ]
    meta = "".join(f"<div class='meta-chip'><span>{t(en, ta)}</span><strong>{v}</strong></div>" for en, ta, v in chips)
    return f"""
<header class="document-hero">
  <div class="hero-copy">
    <p class="eyebrow"><b>CRYPTOFORGE</b>{t(cfg["kicker_en"], cfg["kicker_ta"])}</p>
    <h1>{t(cfg["title_en"], cfg["title_ta"])}</h1>
    <p class="lede">{t(cfg["lede_en"], cfg["lede_ta"])}</p>
    <div class="document-meta">{meta}</div>
  </div>
  <div class="system-sigil" aria-hidden="true">
    <div class="sigil-ring ring-one"></div>
    <div class="sigil-ring ring-two"></div>
    <div class="sigil-ring ring-three"></div>
    <div class="sigil-core"><span>{cfg["mark"]}</span></div>
    <div class="sigil-label label-one">{span.split(" → ")[0]}</div>
    <div class="sigil-label label-two">{usd(total, 0)}</div>
  </div>
</header>
"""


def headline(book: dict) -> str:
    coins = book["coins"]
    total = sum(c["total_pnl"] for c in coins)
    closed = sum(c["net_pnl"] for c in coins)
    bag = sum(c["open_pnl"] for c in coins)
    peak = sum(c["peak_deployed"] for c in coins)
    rounds = sum(c.get("rounds", 0) for c in coins)
    years = max(c["years"] for c in coins)
    per_year = total / years / peak * 100 if peak else 0.0
    held = sum(c["stranded_cost"] for c in coins)
    kpis = [
        ("Total, all four", "மொத்தம்", usd(total), kit.cls(total), "closed profit plus what the open bags are worth"),
        (
            "A year, on money used",
            "ஆண்டுக்கு",
            pct(per_year),
            kit.cls(per_year),
            "against the peak capital actually committed",
        ),
        ("Closed profit", "முடிந்த லாபம்", usd(closed), kit.cls(closed), f"{rounds:,} rounds, net of fees"),
        ("Open bag", "திறந்த கைவசம்", usd(bag), kit.cls(bag), f"{usd(held, 0)} of coin still held at the last close"),
        ("Peak capital used", "உச்ச மூலதனம்", usd(peak, 0), "", "the most that was ever in the market at once"),
    ]
    cells = "".join(
        f"<div class='kpi'><div class='kpi-l'>{t(en, ta)}</div>"
        f"<div class='kpi-v num {c}'>{v}</div><div class='kpi-s'>{s}</div></div>"
        for en, ta, v, c, s in kpis
    )
    return f"<div class='kpis'>{cells}</div>"


def coin_table(book: dict) -> str:
    head = "".join(
        f"<th>{t(en, ta)}</th>"
        for en, ta in (
            ("Coin", "நாணயம்"),
            ("Window", "காலம்"),
            ("Rounds", "சுற்றுகள்"),
            ("Closed", "முடிந்தது"),
            ("Open bag", "கைவசம்"),
            ("Total", "மொத்தம்"),
            ("A year on money used", "ஆண்டுக்கு"),
            ("Peak used", "உச்சம்"),
        )
    )
    rows = ""
    for c in book["coins"]:
        rows += (
            f"<tr><td><strong>{c['symbol'].replace('USDT', '')}</strong>"
            f"<div class='kpi-s'>{t(COIN_NAME.get(c['symbol'], ''), COIN_TA.get(c['symbol'], ''))}</div></td>"
            f"<td class='num'>{c['years']:.1f}y<div class='kpi-s num'>{c['first_day']} → {c['last_day']}</div></td>"
            f"<td class='num'>{c.get('rounds', 0):,}</td>"
            f"<td class='num {kit.cls(c['net_pnl'])}'>{usd(c['net_pnl'])}</td>"
            f"<td class='num {kit.cls(c['open_pnl'])}'>{usd(c['open_pnl'])}</td>"
            f"<td class='num {kit.cls(c['total_pnl'])}'><strong>{usd(c['total_pnl'])}</strong></td>"
            f"<td class='num {kit.cls(c['per_year_on_peak_pct'])}'>{pct(c['per_year_on_peak_pct'])}</td>"
            f"<td class='num'>{usd(c['peak_deployed'], 0)}</td></tr>"
        )
    return f"<div class='tblwrap'><table><thead><tr>{head}</tr></thead><tbody>{rows}</tbody></table></div>"


def curves(book: dict) -> str:
    """One closed-book equity curve per coin.

    The curve is the CLOSED book only. An open ladder has paid nothing yet, and
    drawing its paper mark as equity is exactly what makes a strategy that
    never closes a loss look like it is compounding.
    """
    out = []
    for c in book["coins"]:
        name = c["symbol"]
        # curve_svg takes (label, value) pairs and hands back SIX pieces, of
        # which `zero` is a y-COORDINATE, not path data.
        #
        # The cascade harness records `equity` only while COMPOUNDING, because
        # that is the only time its pot moves; a hand-driven book therefore has
        # an empty list and would draw nothing. The monthly book is present for
        # every strategy and sums exactly to the closed P&L, so the curve is the
        # running total of that — the same line, sampled monthly instead of per
        # round.
        points = list(c["equity"])
        if not points:
            running = 0.0
            points = []
            for month in sorted(c["monthly"]):
                running += c["monthly"][month]
                points.append([month, round(running, 4)])
        # curve_svg spreads the points across the width with `i / (len - 1)`,
        # so a book with a single change of pot divides by zero. Repeating the
        # one point draws it as the flat line it actually is.
        if len(points) == 1:
            points = [[c["first_day"], points[0][1]], [c["last_day"], points[0][1]]]
        elif not points:
            points = [[c["first_day"], 0.0], [c["last_day"], 0.0]]
        line, area, dd, zero_y, hi, lo = kit.curve_svg(points)
        out.append(
            f"<div class='panel'><h2>{c['symbol'].replace('USDT', '')} "
            f"<span class='kpi-s num'>{c['first_day']} → {c['last_day']}</span></h2>"
            f"<div class='chart'><svg viewBox='0 0 1040 260' preserveAspectRatio='none' role='img' "
            f"{t_attr('aria-label', name + ' closed-book equity', name + ' முடிந்த book equity')}>"
            f"<path d='{area}' fill='rgba(var(--accent-rgb),.10)'/>"
            f"<line x1='0' x2='1040' y1='{zero_y:.1f}' y2='{zero_y:.1f}' stroke='var(--line-strong)' stroke-width='1'/>"
            f"<path d='{line}' stroke='var(--accent)' stroke-width='1.6' fill='none'/>"
            f"</svg></div>"
            f"<div class='axis'><span class='num'>{usd(lo, 0)}</span>"
            f"<span class='num'>{c.get('rounds', 0):,} rounds</span>"
            f"<span class='num'>{usd(hi, 0)}</span></div></div>"
        )
    return "".join(out)


def yearly(book: dict) -> str:
    """Money booked per calendar year, per coin — where the freeze shows."""
    years, per = set(), {}
    for c in book["coins"]:
        acc: dict = {}
        for month, value in c["monthly"].items():
            acc[month[:4]] = acc.get(month[:4], 0.0) + value
        per[c["symbol"]] = acc
        years |= set(acc)
    order = sorted(years)
    head = "".join(f"<th class='num'>{y}</th>" for y in order)
    rows = ""
    for c in book["coins"]:
        acc = per[c["symbol"]]
        cells = "".join(
            f"<td class='num {kit.cls(acc.get(y, 0))}'>{usd(acc[y], 0) if y in acc else '·'}</td>" for y in order
        )
        rows += f"<tr><td><strong>{c['symbol'].replace('USDT', '')}</strong></td>{cells}</tr>"
    return (
        f"<div class='tblwrap'><table class='heat'><thead><tr><th>{t('Coin', 'நாணயம்')}</th>{head}</tr></thead>"
        f"<tbody>{rows}</tbody></table></div>"
    )


def rules_section(cfg: dict) -> str:
    rows = "".join(f"<tr><td><strong>{t(en, ta)}</strong></td><td>{text}</td></tr>" for en, ta, text in cfg["rules"])
    return f"<div class='tblwrap'><table><tbody>{rows}</tbody></table></div>"


def limits(book: dict, cfg: dict) -> str:
    """What the numbers above do NOT say. Every sheet carries this."""
    coins = book["coins"]
    losses = sum(c.get("losses", 0) for c in coins)
    held = sum(c["stranded_cost"] for c in coins)
    open_n = sum(c.get("open_positions", 0) for c in coins)
    longest = max((c.get("max_hold_hours", 0) for c in coins), default=0) / 24
    notes = [
        (
            "Every closed round is a winner by construction",
            "முடிந்த ஒவ்வொரு சுற்றும் அமைப்பாலேயே வெற்றி",
            f"There is no stop-loss anywhere in this family. A round closes when its target is reached, so "
            f"the {sum(c.get('rounds', 0) for c in coins):,} closed rounds contain {losses} losses. "
            f"The risk did not disappear — it is sitting in the open bag below.",
        ),
        (
            "The bag is the whole risk",
            "கைவசம் இருப்பதே முழு ஆபத்து",
            f"{open_n} ladders were still holding {usd(held, 0)} of coin when the window ended. "
            f"They are valued here at the last close, not at what they might fetch.",
        ),
        (
            "Money is held for a long time",
            "பணம் நீண்ட காலம் பிடிக்கப்படுகிறது",
            f"The longest single hold ran {longest:,.0f} days. A book like this is not a stream of income; "
            f"it is capital committed and waited on.",
        ),
        (
            "The purse grows only on its own profit",
            "பணப்பை தன் லாபத்தில் மட்டுமே வளர்கிறது",
            f"Each coin starts from {usd(book['capital'], 0)} and is never topped up. Because the budget cap "
            f"scales with a compounding purse, peak capital committed runs above the starting number — that "
            f"is why every rate on this page is measured against peak capital used, not the nameplate.",
        ),
        (
            "One market, one venue, no slippage model",
            "ஒரே சந்தை, ஒரே இடம்",
            "Binance spot 5-minute bars. Fills are candle-resolution: a stop fills when a bar's high reaches "
            "it, and a bar that took an entry is never also allowed to take the target. Fees are charged at "
            "0.1% a side; spread and slippage are not modelled.",
        ),
    ]
    # A sheet may carry its own caveat beyond the five every sheet carries —
    # Cascade-Auto's wallet rule is the reason this exists.
    notes.extend(cfg.get("extra_limits", []))
    return "".join(
        f"<div class='note note-warn'><h2>{t(en, ta)}</h2><p class='lede'>{body}</p></div>" for en, ta, body in notes
    )


def ledger(book: dict) -> str:
    """Every month that booked money, filterable by year."""
    rows_by_year: dict = {}
    for c in book["coins"]:
        for month, value in c["monthly"].items():
            rows_by_year.setdefault(month[:4], []).append((month, c["symbol"], value))
    years = sorted(rows_by_year)
    btns = f'<button type="button" data-year="all" aria-pressed="true">{t("All", "அனைத்தும்")}</button>'
    btns += "".join(f'<button type="button" data-year="{y}" aria-pressed="false">{y}</button>' for y in years)
    rows = ""
    count = 0
    for y in years:
        for month, symbol, value in sorted(rows_by_year[y]):
            count += 1
            rows += (
                f"<tr data-year='{y}'><td class='num'>{month}</td>"
                f"<td>{symbol.replace('USDT', '')}</td>"
                f"<td class='num {kit.cls(value)}'>{usd(value)}</td></tr>"
            )
    return (
        f"<div id='ledger-years' class='reader-toolbar'>{btns}</div>"
        f"<p class='kpi-s'><span id='ledger-count'>{count}</span> {t('months', 'மாதங்கள்')}</p>"
        f"<div class='tblwrap'><table><thead><tr><th>{t('Month', 'மாதம்')}</th><th>{t('Coin', 'நாணயம்')}</th>"
        f"<th>{t('Booked', 'பதிவானது')}</th></tr></thead><tbody>{rows}</tbody></table></div>"
    )


SECTIONS = (
    ("headline", "The headline", "தலைப்புச் செய்தி"),
    ("coins", "Coin by coin", "நாணயம் வாரியாக"),
    ("curves", "The closed book, over time", "முடிந்த book, காலப்போக்கில்"),
    ("years", "Year by year", "ஆண்டு வாரியாக"),
    ("rules", "The rules it ran", "இயங்கிய விதிகள்"),
    ("limits", "What this does not show", "இது காட்டாதவை"),
    ("ledger", "The ledger", "பேரேடு"),
)


def build(key: str) -> pathlib.Path:
    cfg = SHEETS[key]
    book = json.load(open(DATA_DIR / f"{key}_report_data.json", encoding="utf-8"))
    coins = book["coins"]
    span = f"{min(c['first_day'] for c in coins)} → {max(c['last_day'] for c in coins)}"

    css = kit.recolour(kit.STYLE, key).replace("{{", "{").replace("}}", "}") + LANG_CSS + EXTRA_CSS
    bodies = {
        "headline": headline(book),
        "coins": coin_table(book),
        "curves": curves(book),
        "years": yearly(book),
        "rules": rules_section(cfg),
        "limits": limits(book, cfg),
        "ledger": ledger(book),
    }
    # `.shead h2` is the hook READER_JS copies into the contents rail, and the
    # section number is drawn by `.shead::before` off a CSS counter — writing
    # "§1" by hand would print it twice.
    sections = "".join(
        f"<section id='{anchor}'><div class='shead'><h2>{t(en, ta)}</h2></div>{bodies[anchor]}</section>"
        for anchor, en, ta in SECTIONS
    )

    doc = f"""<style>{css}</style>
<div class="reading-progress" aria-hidden="true"><span id="reading-progress-bar"></span></div>
<div class="wrap">
{hero(cfg, book, span)}
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
  <article class="document-body" id="document-body">{sections}
    <p class="kpi-s">{t("Measured from", "அளவிடப்பட்டது")} {cfg["source"]} · {book["generated"]}</p>
  </article>
</div>
</div>
{kit.READER_JS}
{LANG_JS}
"""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUT_DIR / cfg["file"]
    path.write_text(doc, encoding="utf-8")
    return path


def main() -> int:
    keys = sys.argv[1:] or list(SHEETS)
    for key in keys:
        if key not in SHEETS:
            print(f"unknown sheet: {key}")
            return 1
        if not (DATA_DIR / f"{key}_report_data.json").exists():
            print(f"skipped {key}: no data yet — run tools/tearsheet/run_backtests.py --strategy {key}")
            continue
        path = build(key)
        print(f"{key:<7} → {os.path.relpath(path, _REPO)}  ({path.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
