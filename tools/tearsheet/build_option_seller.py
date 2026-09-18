"""tools/tearsheet/build_option_seller.py — render the Option Seller tearsheet.

The fourth sheet in the family, and the odd one out: the other three are
Cascade ladders across four coins with no stop-loss, this is one Bitcoin
option sold on a strong-move day and bought back the same afternoon, with a
stop. So it borrows the family's LOOK (sheet_kit, i18n, the reader, the
shell app.py wraps it in) but not build_sheets.py's coin-and-bag sections,
which would describe a book this is not.

    python3 tools/tearsheet/optsell_data.py ~/Documents/delta-momentum-research
    python3 tools/tearsheet/build_option_seller.py

Reads tools/tearsheet/data/optsell_report_data.json and writes
docs/assets/option-seller-tearsheet.html (a FRAGMENT) and
static/tearsheet-optsell.css.
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
from build_sheets import EXTRA_CSS, pct, usd  # noqa: E402
from i18n import LANG_CSS, LANG_JS, t, t_attr  # noqa: E402

KEY = "optsell"
FILE = "option-seller-tearsheet.html"
DATA = _HERE / "data" / "optsell_report_data.json"
OUT_DIR = _REPO / "docs" / "assets"
STATIC_DIR = _REPO / "static"
PAPER_BTC = 0.1  # the size the paper book runs at; every $ figure is also shown at it

# The kit sets every cell nowrap and right-aligned — right for numbers, wrong
# for a sentence, which then runs off the card. Rules, checks and the split's
# labels are sentences.
SHEET_CSS = (
    EXTRA_CSS
    + """
table.prose { min-width:0; }
table.prose td { white-space:normal; text-align:left; vertical-align:top; line-height:1.55; }
table.prose td:first-child { width:220px; white-space:nowrap; }
td.label { text-align:left; white-space:normal; vertical-align:middle; }
"""
)

MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")


def signed(n, dp=0):
    """A ledger figure with an explicit + on gains."""
    s = usd(n, dp)
    return s if s.startswith("-") or s == "—" else "+" + s


def hero(book: dict) -> str:
    tot = book["totals"]
    span = f"{book['first_day']} → {book['last_day']}"
    chips = [
        ("Window", "காலம்", span),
        ("Days", "நாட்கள்", f"{book['days']}"),
        ("Trades", "டிரேடுகள்", f"{tot['trades']}"),
        ("Size", "அளவு", "per 1 BTC"),
        ("Costs", "செலவுகள்", "Delta fees + 18% GST"),
        ("Built", "உருவாக்கப்பட்டது", book["generated"]),
    ]
    meta = "".join(f"<div class='meta-chip'><span>{t(en, ta)}</span><strong>{v}</strong></div>" for en, ta, v in chips)
    return f"""
<header class="document-hero">
  <div class="hero-copy">
    <p class="eyebrow"><b>CRYPTOFORGE</b>{
        t(
            "Delta daily BTC options · 4 PM IST · sell into a strong move",
            "Delta தினசரி BTC options · மாலை 4 · வலுவான நகர்வில் விற்பனை",
        )
    }</p>
    <h1>{t("Option Seller", "Option Seller")}</h1>
    <p class="lede">{
        t(
            "At 4 PM IST it checks whether Bitcoin has moved strongly one way over the last 2 to 12 hours. "
            "When at least 5 of 6 windows agree, it sells the at-the-money option of that day's 5:30 PM expiry "
            "in the direction of the move, and buys it back at 5:25 PM — or at once if its price doubles. "
            "This is that rule over every day Delta still publishes.",
            "மாலை 4 மணிக்கு, கடந்த 2 முதல் 12 மணி நேரத்தில் Bitcoin ஒரே திசையில் வலுவாக நகர்ந்ததா என்று பார்க்கிறது. "
            "6-இல் குறைந்தது 5 நேர அளவுகள் ஒப்புக்கொண்டால், அன்று மாலை 5:30-க்கு முடியும் at-the-money option-ஐ "
            "நகர்வின் திசையில் விற்று, 5:25-க்கு — அல்லது விலை இரட்டிப்பானால் உடனே — திரும்ப வாங்குகிறது. "
            "Delta இன்னும் வெளியிடும் ஒவ்வொரு நாளிலும் இந்த விதி சோதிக்கப்பட்டது.",
        )
    }</p>
    <div class="document-meta">{meta}</div>
  </div>
  <div class="system-sigil" aria-hidden="true">
    <div class="sigil-ring ring-one"></div>
    <div class="sigil-ring ring-two"></div>
    <div class="sigil-ring ring-three"></div>
    <div class="sigil-core"><span>OS</span></div>
    <div class="sigil-label label-one">{book["first_day"]}</div>
    <div class="sigil-label label-two">{signed(tot["net"])}</div>
  </div>
</header>
"""


def headline(book: dict) -> str:
    tot = book["totals"]
    oos = book["splits"][0]["after"]
    wins = round(tot["win_pct"] * tot["trades"] / 100)
    kpis = [
        (
            "Total, per 1 BTC",
            "மொத்தம், 1 BTC-க்கு",
            signed(tot["net"]),
            kit.cls(tot["net"]),
            f"{signed(tot['net'] * PAPER_BTC, 2)} at the paper size of {PAPER_BTC:g} BTC, fees paid",
        ),
        (
            "On days it never saw",
            "பார்க்காத நாட்களில்",
            signed(oos["net"]),
            kit.cls(oos["net"]),
            f"{oos['trades']} trades after {book['splits'][0]['cut']} — the number to expect",
        ),
        (
            "Won",
            "வெற்றி",
            pct(tot["win_pct"]),
            "",
            f"{wins} of {tot['trades']}; {tot['stops']} bought back at the stop",
        ),
        (
            "Worst losing run",
            "மோசமான தொடர் இழப்பு",
            usd(-tot["worst_run"]),
            "neg",
            f"{usd(-tot['worst_run'] * PAPER_BTC, 2)} at {PAPER_BTC:g} BTC",
        ),
        (
            "Trades",
            "டிரேடுகள்",
            f"{tot['trades']}",
            "",
            f"in {book['days']} days — about one a week; most days it does nothing",
        ),
        (
            "Per trade, unseen days",
            "ஒரு டிரேடுக்கு, பார்க்காத நாட்கள்",
            signed(oos["per_trade"], 2),
            kit.cls(oos["per_trade"]),
            f"{signed(oos['per_trade'] * PAPER_BTC, 2)} at {PAPER_BTC:g} BTC",
        ),
        (
            "Fees paid",
            "செலுத்திய கட்டணம்",
            usd(-book["fees"], 0),
            "neg",
            "both legs, GST included — already taken out of every number above",
        ),
        (
            "Capital per trade",
            "ஒரு டிரேடுக்கு மூலதனம்",
            "≈ " + usd(book["avg_capital"], 0),
            "",
            f"per 1 BTC: {book['rule']['im_pct']:g}% margin on the Bitcoin value + the premium",
        ),
    ]
    cells = "".join(
        f"<div class='kpi'><div class='kpi-l'>{t(en, ta)}</div>"
        f"<div class='kpi-v num {c}'>{v}</div><div class='kpi-s'>{s}</div></div>"
        for en, ta, v, c, s in kpis
    )
    return f"<div class='kpis'>{cells}</div>"


def split_table(book: dict) -> str:
    head = "".join(
        f"<th>{t(en, ta)}</th>"
        for en, ta in (
            ("How it was split", "எப்படி பிரிக்கப்பட்டது"),
            ("Part", "பகுதி"),
            ("Trades", "டிரேடுகள்"),
            ("Won", "வெற்றி"),
            ("Per trade", "ஒரு டிரேடுக்கு"),
            ("Total", "மொத்தம்"),
            ("Worst run", "மோசமான தொடர்"),
        )
    )
    rows = ""
    for s in book["splits"]:
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
            rows += (
                f"<tr>{label}<td class='num'>{t(part_en, part_ta)}</td>"
                f"<td class='num'>{st['trades']}</td><td class='num'>{pct(st['win_pct'])}</td>"
                f"<td class='num {kit.cls(st['per_trade'])}'>{signed(st['per_trade'], 2)}</td>"
                f"<td class='num {kit.cls(st['net'])}'><strong>{signed(st['net'])}</strong></td>"
                f"<td class='num neg'>{usd(-st['worst_run'])}</td></tr>"
            )
    intro = t(
        "The settings were chosen on the first half of the days only. The second half is what the rule "
        "made on days it had never seen. It made less per trade there — expect the lower number.",
        "அமைப்புகள் முதல் பாதி நாட்களில் மட்டுமே தேர்ந்தெடுக்கப்பட்டன. இரண்டாம் பாதி — விதி பார்க்காத நாட்கள் — "
        "உண்மையான சோதனை. அங்கு ஒரு டிரேடுக்குக் குறைவாகச் சம்பாதித்தது — குறைந்த எண்ணையே எதிர்பாருங்கள்.",
    )
    return (
        f"<p class='lede'>{intro}</p>"
        f"<div class='tblwrap'><table><thead><tr>{head}</tr></thead><tbody>{rows}</tbody></table></div>"
    )


def curve(book: dict) -> str:
    running, points = 0.0, []
    for tr in book["trades"]:
        running += tr["net"]
        points.append([tr["day"], round(running, 2)])
    line, area, _dd, zero_y, hi, lo = kit.curve_svg(points)
    cut = book["splits"][0]["cut"]
    cut_i = next((i for i, p in enumerate(points) if p[0] >= cut), len(points) - 1)
    cut_x = cut_i / (len(points) - 1) * 1040
    return (
        f"<div class='panel'><h2>{t('Every trade, added up', 'ஒவ்வொரு டிரேடும், கூட்டி')} "
        f"<span class='kpi-s num'>{book['first_day']} → {book['last_day']}</span></h2>"
        f"<div class='chart'><svg viewBox='0 0 1040 260' preserveAspectRatio='none' role='img' "
        f"{t_attr('aria-label', 'Running total of every trade, per 1 BTC', 'ஒவ்வொரு டிரேடின் கூட்டுத்தொகை, 1 BTC-க்கு')}>"
        f"<path d='{area}' fill='rgba(var(--accent-rgb),.10)'/>"
        f"<line x1='0' x2='1040' y1='{zero_y:.1f}' y2='{zero_y:.1f}' stroke='var(--line-strong)' stroke-width='1'/>"
        f"<line x1='{cut_x:.1f}' x2='{cut_x:.1f}' y1='0' y2='260' stroke='var(--line-strong)' stroke-width='1' stroke-dasharray='5 5'/>"
        f"<path d='{line}' stroke='var(--accent)' stroke-width='1.6' fill='none'/>"
        f"</svg></div>"
        f"<div class='axis'><span class='num'>{usd(lo, 0)}</span>"
        f"<span class='num'>{t('dashed line: where the unseen half begins', 'கோடு: பார்க்காத பாதி தொடங்கும் இடம்')} · {cut}</span>"
        f"<span class='num'>{usd(hi, 0)}</span></div></div>"
    )


def months(book: dict) -> str:
    years = sorted({m[:4] for m in book["monthly"]})
    head = "".join(f"<th class='num'>{m}</th>" for m in MONTHS)
    rows = ""
    for y in years:
        cells = ""
        for i in range(12):
            key = f"{y}-{i + 1:02d}"
            in_window = book["first_day"][:7] <= key <= book["last_day"][:7]
            if key in book["monthly"]:
                v = book["monthly"][key]
                cells += f"<td class='num {kit.cls(v)}'>{signed(v)}</td>"
            else:
                cells += f"<td class='num flat'>{'0' if in_window else '·'}</td>"
        total = sum(v for k, v in book["monthly"].items() if k.startswith(y))
        rows += f"<tr><td><strong>{y}</strong><div class='kpi-s num {kit.cls(total)}'>{signed(total)}</div></td>{cells}</tr>"
    note = t(
        "Per 1 BTC, fees paid. 0 means no day in that month passed the check, so nothing was traded.",
        "1 BTC-க்கு, கட்டணம் செலுத்திய பின். 0 என்றால் அந்த மாதம் எந்த நாளும் சோதனையில் தேறவில்லை.",
    )
    return (
        f"<div class='tblwrap'><table class='heat'><thead><tr><th>{t('Year', 'ஆண்டு')}</th>{head}</tr></thead>"
        f"<tbody>{rows}</tbody></table></div><p class='kpi-s'>{note}</p>"
    )


def day_kinds(book: dict) -> str:
    k = book["day_kinds"]
    head = "".join(
        f"<th>{t(en, ta)}</th>"
        for en, ta in (
            ("Kind of day at 4 PM", "மாலை 4 மணி நாள் வகை"),
            ("Days", "நாட்கள்"),
            ("Won", "வெற்றி"),
            ("Stopped", "Stop"),
            ("Premium", "Premium"),
            ("Per trade", "ஒரு டிரேடுக்கு"),
            ("Total", "மொத்தம்"),
            ("Worst run", "மோசமான தொடர்"),
        )
    )
    rows = ""
    for key, en, ta in (
        ("strong", "Strong move — 5 or 6 windows agree (TRADED)", "வலுவான நகர்வு — 5/6 ஒப்புதல் (டிரேடு)"),
        ("mixed", "Mixed — 1 to 4 windows (skipped)", "கலப்பு — 1 முதல் 4 (தவிர்க்கப்பட்டது)"),
        ("calm", "Calm — no window moved enough (skipped)", "அமைதி — எதுவும் போதுமான நகர்வு இல்லை (தவிர்க்கப்பட்டது)"),
    ):
        st = k[key]
        rows += (
            f"<tr><td><strong>{t(en, ta)}</strong></td><td class='num'>{st['trades']}</td>"
            f"<td class='num'>{pct(st['win_pct'])}</td><td class='num'>{st['stops']}</td>"
            f"<td class='num'>{k['avg_premium'][key]:,.0f}</td>"
            f"<td class='num {kit.cls(st['per_trade'])}'>{signed(st['per_trade'], 2)}</td>"
            f"<td class='num {kit.cls(st['net'])}'><strong>{signed(st['net'])}</strong></td>"
            f"<td class='num neg'>{usd(-st['worst_run'])}</td></tr>"
        )
    intro = t(
        "Why it waits. The same sale, made every other day, earns almost nothing: after a strong move the "
        "option is dearer, so there is more premium to keep in the same 85 minutes. On a calm day the premium "
        "is small, fees take most of it, and the stop still fires about one day in three.",
        "ஏன் காத்திருக்கிறது: மற்ற நாட்களில் இதே விற்பனை கிட்டத்தட்ட எதுவும் சம்பாதிக்கவில்லை. வலுவான நகர்வுக்குப் பின் "
        "option விலை அதிகம், அதே 85 நிமிடத்தில் அதிக premium கிடைக்கும். அமைதியான நாளில் premium சிறியது; "
        "கட்டணம் பெரும்பகுதியை எடுக்கிறது; stop மூன்றில் ஒரு நாள் அடிக்கிறது.",
    )
    return (
        f"<p class='lede'>{intro}</p>"
        f"<div class='tblwrap'><table><thead><tr>{head}</tr></thead><tbody>{rows}</tbody></table></div>"
    )


def rules(book: dict) -> str:
    r = book["rule"]
    th = ", ".join(f"{int(lb) // 60}h {v:.2f}%" for lb, v in r["thresholds_pct"].items())
    items = [
        ("When", "எப்போது", "Once a day at 4:00 PM IST, on Delta's own Bitcoin index (.DEXBTUSD)."),
        (
            "The check",
            "சோதனை",
            f"Bitcoin's move over the last 2, 3, 4, 6, 8 and 12 hours. Each window votes when it moved at least: {th}.",
        ),
        (
            "Trade only if",
            "டிரேடு எப்போது",
            f"At least {r['min_votes']} of the 6 windows vote the same way. Otherwise nothing happens that day.",
        ),
        (
            "What it sells",
            "எதை விற்கிறது",
            "Up move: the at-the-money CALL. Down move: the at-the-money PUT. Of the option that settles at 5:30 PM the same day.",
        ),
        (
            "Stop",
            "Stop",
            f"Buy it back at once if its price reaches {r['stop_mult']:g}× what it was sold for.",
        ),
        ("Exit", "வெளியேற்றம்", "Otherwise buy it back at 5:25 PM IST, five minutes before settlement."),
        ("Limit", "வரம்பு", "At most one trade a day."),
        (
            "Costs",
            "செலவுகள்",
            "Delta's option fee on both legs — 0.03% of the Bitcoin value, capped at 3.5% of the premium — plus 18% GST.",
        ),
    ]
    rows = "".join(f"<tr><td><strong>{t(en, ta)}</strong></td><td>{text}</td></tr>" for en, ta, text in items)
    return f"<div class='tblwrap'><table class='prose'><tbody>{rows}</tbody></table></div>"


def checks(book: dict) -> str:
    items = [
        (
            "No look-ahead",
            "எதிர்காலம் பார்க்கவில்லை",
            "Every sale is priced at a real 1-minute bar that opened at or after 4 PM.",
        ),
        (
            "The votes, recounted",
            "வாக்குகள் மீண்டும் எண்ணப்பட்டன",
            f"All {book['totals']['trades']} trades had their votes recounted from the raw index candles; every one had at least 5.",
        ),
        (
            "Every trade by hand",
            "ஒவ்வொரு டிரேடும் கையால்",
            "Each profit recomputed independently from the sale price, buy-back price and fees.",
        ),
        (
            "Stops fill no better than a doubling",
            "Stop இரட்டிப்பை விட நல்லதாக நிரம்பவில்லை",
            "A stop is filled at twice the premium or worse, never better.",
        ),
        (
            "Real option prices",
            "உண்மையான option விலைகள்",
            "Premiums near expiry checked against what the option was actually worth at settlement.",
        ),
        (
            "Not a lucky setting",
            "அதிர்ஷ்ட அமைப்பு அல்ல",
            "Nearby settings (a lower or higher move, 3/4/6 votes) all stay profitable on the unseen half, "
            "and moving the split point to one third or two thirds keeps the second part profitable.",
        ),
    ]
    rows = "".join(f"<tr><td><strong>{t(en, ta)}</strong></td><td>{text}</td></tr>" for en, ta, text in items)
    intro = t(
        "No number on this page was reported until every check below had passed.",
        "கீழே உள்ள ஒவ்வொரு சோதனையும் தேறிய பின்னரே இப்பக்கத்தின் எண்கள் வெளியிடப்பட்டன.",
    )
    return f"<p class='lede'>{intro}</p><div class='tblwrap'><table class='prose'><tbody>{rows}</tbody></table></div>"


def limits(book: dict) -> str:
    tot = book["totals"]
    worst = min(x["net"] for x in book["trades"])
    oos = book["splits"][0]
    notes = [
        (
            "A small sample",
            "சிறிய மாதிரி",
            f"{tot['trades']} trades in {book['days']} days, and only {oos['after']['trades']} on days the rule "
            f"had never seen. That is enough to say the idea is not noise, not enough to promise a number.",
        ),
        (
            "The first half flattered it",
            "முதல் பாதி அதிகமாகக் காட்டியது",
            f"{signed(oos['before']['per_trade'], 2)} a trade on the days the settings were chosen on, "
            f"{signed(oos['after']['per_trade'], 2)} on the days after. Plan on the second.",
        ),
        (
            "Tested at the mark, not the real quotes",
            "Mark விலையில் சோதனை, உண்மையான quote-இல் அல்ல",
            "The backtest sells and buys back at Delta's mark price with no slippage. A real order sells at "
            "the bid and buys back at the ask. The paper book records both on every trade, so this gap is being "
            "measured now rather than assumed.",
        ),
        (
            "The stop is judged on 1-minute candles",
            "Stop 1-நிமிட candle-இல் மதிப்பிடப்படுகிறது",
            "The test stops out when a 1-minute high reaches twice the premium. Live, the price is checked every "
            "15 seconds, so a fast spike can be caught late or missed. The paper book flags every such case.",
        ),
        (
            "A loss is large next to the capital",
            "இழப்பு மூலதனத்துடன் ஒப்பிட பெரியது",
            f"The worst single trade lost {usd(worst)} per 1 BTC against about {usd(book['avg_capital'], 0)} "
            f"of capital tied up. Keep at least the capital plus the worst losing run "
            f"({usd(book['avg_capital'] + tot['worst_run'], 0)} per 1 BTC) in the account. The capital figure uses "
            f"the {book['rule']['im_pct']:g}% margin Delta lists today, which Delta can change.",
        ),
        (
            "One venue, a rolling window",
            "ஒரு இடம், நகரும் காலம்",
            "Delta publishes about 405 days of option history and drops the oldest day each day, so a rebuild "
            "next month covers a slightly different window. Bitcoin only; Delta Exchange India only.",
        ),
    ]
    return "".join(
        f"<div class='note note-warn'><h2>{t(en, ta)}</h2><p class='lede'>{body}</p></div>" for en, ta, body in notes
    )


def ledger(book: dict) -> str:
    years = sorted({x["day"][:4] for x in book["trades"]})
    btns = f'<button type="button" data-year="all" aria-pressed="true">{t("All", "அனைத்தும்")}</button>'
    btns += "".join(f'<button type="button" data-year="{y}" aria-pressed="false">{y}</button>' for y in years)
    rows = ""
    for x in reversed(book["trades"]):
        side = t("call", "call") if x["side"] == "C" else t("put", "put")
        why = t("stop", "stop") if x["why"] == "stop" else t("5:25 PM", "மாலை 5:25")
        rows += (
            f"<tr data-year='{x['day'][:4]}'><td class='num'>{x['day']}</td>"
            f"<td>{side} {x['strike']:,}</td>"
            f"<td class='num'>{x['btc']:,.0f}</td>"
            f"<td class='num'>{x['votes']}/6</td>"
            f"<td class='num'>{x['entry']:,.2f}</td>"
            f"<td class='num'>{x['exit']:,.2f}</td>"
            f"<td>{why}</td>"
            f"<td class='num {kit.cls(x['net'])}'>{signed(x['net'], 2)}</td>"
            f"<td class='num {kit.cls(x['net'])}'>{signed(x['net'] * PAPER_BTC, 2)}</td></tr>"
        )
    head = "".join(
        f"<th>{t(en, ta)}</th>"
        for en, ta in (
            ("Day", "நாள்"),
            ("Sold", "விற்றது"),
            ("BTC at 4 PM", "மாலை 4 BTC"),
            ("Agreed", "ஒப்புதல்"),
            ("Sold at", "விற்ற விலை"),
            ("Bought back", "திரும்ப வாங்கியது"),
            ("Why", "ஏன்"),
            ("Per 1 BTC", "1 BTC-க்கு"),
            (f"At {PAPER_BTC:g} BTC", f"{PAPER_BTC:g} BTC-இல்"),
        )
    )
    return (
        f"<div id='ledger-years' class='ledger-controls'>{btns}</div>"
        f"<p class='kpi-s'><span id='ledger-count'>{len(book['trades'])}</span> {t('trades, newest first, fees paid', 'டிரேடுகள், புதியவை முதலில், கட்டணம் செலுத்தியது')}</p>"
        f"<div class='tblwrap'><table><thead><tr>{head}</tr></thead><tbody>{rows}</tbody></table></div>"
    )


SECTIONS = (
    ("headline", "The headline", "தலைப்புச் செய்தி"),
    ("unseen", "Chosen on one half, tested on the other", "ஒரு பாதியில் தேர்வு, மறு பாதியில் சோதனை"),
    ("curve", "Every trade, over time", "ஒவ்வொரு டிரேடும், காலப்போக்கில்"),
    ("months", "Month by month", "மாதம் வாரியாக"),
    ("days", "The days it skips", "தவிர்க்கும் நாட்கள்"),
    ("rules", "The rules it ran", "இயங்கிய விதிகள்"),
    ("checks", "How it was checked", "எப்படி சோதிக்கப்பட்டது"),
    ("limits", "What this does not show", "இது காட்டாதவை"),
    ("ledger", "The ledger", "பேரேடு"),
)


def build() -> pathlib.Path:
    book = json.load(open(DATA, encoding="utf-8"))
    css = kit.recolour(kit.STYLE, KEY).replace("{{", "{").replace("}}", "}") + LANG_CSS + SHEET_CSS
    bodies = {
        "headline": headline(book),
        "unseen": split_table(book),
        "curve": curve(book),
        "months": months(book),
        "days": day_kinds(book),
        "rules": rules(book),
        "checks": checks(book),
        "limits": limits(book),
        "ledger": ledger(book),
    }
    sections = "".join(
        f"<section id='{anchor}'><div class='shead'><h2>{t(en, ta)}</h2></div>{bodies[anchor]}</section>"
        for anchor, en, ta in SECTIONS
    )
    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    (STATIC_DIR / f"tearsheet-{KEY}.css").write_text(css.rstrip("\n") + "\n", encoding="utf-8")
    # The reader is shared with the other three sheets; build_sheets.py owns
    # the file and it is identical, so it is written the same way here.
    reader_js = "\n".join(block.replace("<script>", "").replace("</script>", "") for block in (kit.READER_JS, LANG_JS))
    (STATIC_DIR / "tearsheet.js").write_text(reader_js.rstrip("\n") + "\n", encoding="utf-8")

    doc = f"""<div class="reading-progress" aria-hidden="true"><span id="reading-progress-bar"></span></div>
<div class="wrap">
{hero(book)}
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
    <p class="kpi-s">{t("Measured from", "அளவிடப்பட்டது")} the Delta research harness (momentum_bt.run_day), rule as in engine/option_seller_paper.py · {book["generated"]}</p>
  </article>
</div>
</div>
"""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUT_DIR / FILE
    path.write_text(doc, encoding="utf-8")
    return path


def main() -> int:
    if not DATA.exists():
        print("no data yet — run tools/tearsheet/optsell_data.py <research dir> first")
        return 1
    path = build()
    print(f"{KEY} → {os.path.relpath(path, _REPO)}  ({path.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
