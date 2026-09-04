"""tools/tearsheet/sheet_kit.py — the one document the three sheets are cut from.

CryptoForge publishes three tearsheets — Cascade Hybrid, Cascade_Auto and the
V-Rule — and they are one family, not three designs. Every rule of the layout
lives here: the palette, the reading rail, the search, the curve, the ledger,
the risk notes. A sheet supplies its numbers and its words; it never supplies a
look.

The PATTERN is PhilForge's (tools/tearsheet/build_report.py in that repo, whose
class vocabulary and reader JS this vendors verbatim), because a reader who has
read one of Phil's tearsheets should not have to learn a second document. The
SKIN is CryptoForge's own: the terminal's obsidian ground, Rajdhani headings
over Sora text, and a cyan/amber/teal accent set instead of PhilForge's
blueprint violet.

Vendored, not imported: the two repos deploy separately, and a document that
could not be rebuilt without the other repo checked out beside it is a document
that will one day fail to rebuild.

The vocabulary, so nothing invents a class that was never styled:
  .wrap .eyebrow .lede .num .pos .neg .flat
  .document-hero > (.hero-copy, .system-sigil)   — EXACTLY two children
  .document-meta > .meta-chip
  .reader-toolbar .document-search .reader-layout .contents
  .panel .kpis > .kpi > (.kpi-l, .kpi-v, .kpi-s)
  .split .tblwrap .chart .axis .note .note-warn
  table.heat is the one rule a sheet may add for itself.
"""

from __future__ import annotations

STYLE = """
/* The palette, the grid ground, the type and the card geometry below are the
   PhilForge blueprint reader's (static/architecture-document.css), so this
   document reads as one more page of the site rather than a foreign PDF
   dropped into a frame. Two deliberate departures from that stylesheet:

   1. The accent is VIOLET, not the reader's teal. The Assets viewbar marks each
      document with its own tint — amber for CryptoForge, teal for PhilForge —
      and the tearsheet is a third document, so it takes the reader palette's
      third hue and the tab carries a matching dot.
      Light mode cannot use the bright violet: #a78bfa is 2.65:1 on a white card.
      The light value is set for the WORST light ground it lands on, which is
      not the card but the accent-tinted risk chip (--accent-soft over the card).
      #6d4bd8 clears the card at 5.6:1 and the chip at 4.93:1. Re-check against
      the CHIP, not the card, if it is ever changed.
   2. The cumulative-profit line does NOT use the accent. Teal beside the
      profit-green day bars is two neighbouring hues carrying different meanings;
      the line gets --curve (blue, also from the reader's palette) instead. */
:root {{
  color-scheme: light dark;
  --paper:#f2f5fa; --surface:#ffffff; --surface-2:#f2f5fa; --surface-3:#e8eef7;
  --ink:#0b1220; --ink-2:#2b3a52; --muted:#54637a; --dim:#54637a;
  --line:rgba(20,42,72,.15); --line-2:rgba(20,42,72,.09); --line-strong:rgba(20,42,72,.26);
  --grid:rgba(16,40,70,.05);
  --accent:#0e7490; --accent-rgb:14,116,144; --accent-soft:rgba(14,116,144,.09);
  --curve:#a1610a; --curve-rgb:161,97,10;
  --pos:#0f6b52; --neg:#a4342f; --flat:#5b6a7c;
  --pos-fill:15,107,82; --neg-fill:164,52,47;
  --shadow:0 1px 2px rgba(6,14,26,.05), 0 8px 24px rgba(6,14,26,.05);
  --sans:"Sora",ui-sans-serif,-apple-system,"Segoe UI",Roboto,sans-serif;
  --display:"Rajdhani","Sora",ui-sans-serif,-apple-system,sans-serif;
  --mono:"Azeret Mono",ui-monospace,"SF Mono",Menlo,Consolas,monospace;
}}
@media (prefers-color-scheme: dark) {{
  :root:not([data-theme="light"]) {{
    --paper:#040814; --surface:#080f1e; --surface-2:#0c1526; --surface-3:#111c31;
    --ink:#e8f2ff; --ink-2:#c2d2e6; --muted:#8492a6; --dim:#8492a6;
    --line:rgba(148,178,211,.16); --line-2:rgba(148,178,211,.10); --line-strong:rgba(148,178,211,.30);
    --grid:rgba(120,160,205,.045);
    --accent:#22d3ee; --accent-rgb:34,211,238; --accent-soft:rgba(34,211,238,.10);
    --curve:#f59e0b; --curve-rgb:245,158,11;
    --pos:#2dd4bf; --neg:#fb7185; --flat:#8492a6;
    --pos-fill:45,212,191; --neg-fill:251,113,133;
    --shadow:0 1px 2px rgba(0,0,0,.45), 0 10px 30px rgba(0,0,0,.4);
  }}
}}
:root[data-theme="dark"] {{
  --paper:#040814; --surface:#080f1e; --surface-2:#0c1526; --surface-3:#111c31;
  --ink:#e8f2ff; --ink-2:#c2d2e6; --muted:#8492a6; --dim:#8492a6;
  --line:rgba(148,178,211,.16); --line-2:rgba(148,178,211,.10); --line-strong:rgba(148,178,211,.30);
  --grid:rgba(120,160,205,.045);
  --accent:#22d3ee; --accent-rgb:34,211,238; --accent-soft:rgba(34,211,238,.10);
  --curve:#f59e0b; --curve-rgb:245,158,11;
  --pos:#2dd4bf; --neg:#fb7185; --flat:#8492a6;
  --pos-fill:45,212,191; --neg-fill:251,113,133;
  --shadow:0 1px 2px rgba(0,0,0,.45), 0 10px 30px rgba(0,0,0,.4);
}}
* {{ box-sizing:border-box; }}
html {{ scroll-behavior:smooth; }}
body {{
  margin:0; color:var(--ink);
  font-family:var(--sans); font-size:15px; line-height:1.6;
  -webkit-text-size-adjust:100%;
  background-color:var(--paper);
  background-image:linear-gradient(var(--grid) 1px,transparent 1px),
                   linear-gradient(90deg,var(--grid) 1px,transparent 1px),
                   radial-gradient(circle at 75% 0%,rgba(var(--accent-rgb),.075),transparent 28%);
  background-size:64px 64px,64px 64px,auto;
}}
.wrap {{ width:min(1500px, calc(100% - 40px)); margin:0 auto; padding:0 0 72px; }}
/* Eyebrow and h1 are set to the reader's exact values (9px/.13em, and a 64px
   cap on a 1-line height), so the two documents measure the same side by side
   rather than merely looking similar. */
.eyebrow {{
  margin:0 0 14px;
  font-family:var(--mono); font-size:9px; font-weight:600;
  letter-spacing:.13em; text-transform:uppercase; color:var(--muted);
}}
.eyebrow > .tr > i, .eyebrow b {{ font-style:normal; }}
.eyebrow b {{ color:var(--accent); margin-right:8px; font-weight:600; }}
h1 {{ font-family:var(--display); max-width:850px; font-size:clamp(38px,5vw,64px); line-height:1; letter-spacing:-.05em;
     margin:0; font-weight:600; text-wrap:balance; }}
h2 {{ font-family:var(--display); font-size:21px; letter-spacing:-.015em; margin:0 0 4px; font-weight:700; text-wrap:balance; }}
h3, .note-h {{ font-size:14px; letter-spacing:-.005em; margin:0 0 10px; font-weight:700; }}
p {{ margin:0 0 12px; max-width:68ch; color:var(--ink-2); }}
a {{ color:var(--accent); }}
.lede {{ font-size:16.5px; color:var(--ink-2); max-width:70ch; }}
.num {{ font-family:var(--mono); font-variant-numeric:tabular-nums; }}
.pos {{ color:var(--pos); }} .neg {{ color:var(--neg); }} .flat {{ color:var(--flat); }}

/* ── Reader chrome: hero, toolbar, rail, section cards ─────────────────────
   Geometry copied from static/architecture-document.css so a reader who
   switches tabs between a blueprint and this tearsheet sees one design. */
.reading-progress {{ position:fixed; inset:0 0 auto; height:2px; z-index:200; background:transparent; }}
.reading-progress span {{ display:block; width:0; height:100%;
  background:linear-gradient(90deg,var(--accent),var(--curve));
  box-shadow:0 0 8px rgba(var(--accent-rgb),.7); }}

.document-hero {{ min-height:340px; display:grid; grid-template-columns:1fr 400px;
  align-items:center; gap:48px; border-bottom:1px solid var(--line); }}
.hero-copy {{ padding:48px 0; min-width:0; }}
.hero-copy .lede {{ max-width:70ch; margin:20px 0 0; color:var(--muted);
  font-size:16px; line-height:1.72; }}
.document-meta {{ margin-top:27px; display:flex; flex-wrap:wrap; gap:8px; }}
.meta-chip {{ min-height:44px; padding:8px 12px; display:grid; align-content:center; gap:4px;
  border:1px solid var(--line); border-radius:8px; background:var(--surface); }}
.meta-chip > span {{ color:var(--dim); font:500 9px var(--mono); letter-spacing:.1em; text-transform:uppercase; }}
.meta-chip strong {{ font:600 11px/1.45 var(--mono); }}

.system-sigil {{ position:relative; width:340px; height:260px; justify-self:center;
  display:grid; place-items:center; }}
.sigil-ring {{ position:absolute; border:1px solid var(--line); border-radius:50%; }}
.ring-one {{ width:250px; height:250px; }}
.ring-two {{ width:205px; height:120px; transform:rotate(-24deg); border-color:rgba(var(--accent-rgb),.26); }}
.ring-three {{ width:138px; height:138px; border-color:rgba(var(--accent-rgb),.34); }}
.sigil-core {{ width:82px; height:82px; display:grid; place-items:center;
  border:1px solid rgba(var(--accent-rgb),.42); border-radius:50%;
  background:rgba(var(--accent-rgb),.1); color:var(--accent); font:600 15px var(--mono);
  box-shadow:0 0 50px rgba(var(--accent-rgb),.09); }}
.sigil-label {{ position:absolute; padding:6px 8px; border:1px solid var(--line); border-radius:6px;
  background:var(--surface); color:var(--muted); font:500 7px var(--mono); letter-spacing:.12em; }}
.label-one {{ right:4px; top:56px; }}
.label-two {{ left:12px; bottom:52px; }}

/* A body on each orbit (Phil, 2026-08-24: "a small planet rotating on the GC
   orbit in every direction ... something sensible and meaningful").

   No new markup: the rings ALREADY exist on every sheet, so each one spins and
   carries a dot on its own edge as ::after. That is why this lives here in the
   parent stylesheet -- all four tearsheets borrow this block, so all four get
   it without touching four builders.

   The directions and speeds are not arbitrary. The inner body runs faster than
   the outer one and the other way round, which is how real orbits read: closer
   in, shorter period. The middle body follows its ellipse with a motion path so
   the ring keeps its deliberate -24 degree tilt.

   `prefers-reduced-motion` stops both, and the whole sigil is already hidden in
   print and on small screens. */
@keyframes sigil-orbit-cw {{ from {{ transform:rotate(0deg); }} to {{ transform:rotate(360deg); }} }}
@keyframes sigil-orbit-ccw {{ from {{ transform:rotate(360deg); }} to {{ transform:rotate(0deg); }} }}
@keyframes sigil-orbit-ellipse {{ from {{ offset-distance:0%; }} to {{ offset-distance:100%; }} }}
.ring-one {{ animation:sigil-orbit-cw 32s linear infinite; }}
.ring-three {{ animation:sigil-orbit-ccw 17s linear infinite; }}
.ring-one::after, .ring-two::after, .ring-three::after {{
  content:''; position:absolute; left:50%; border-radius:50%;
  background:var(--accent); }}
.ring-one::after {{ width:7px; height:7px; top:-4px; margin-left:-3.5px;
  box-shadow:0 0 10px 2px rgba(var(--accent-rgb),.45); opacity:.85; }}
.ring-three::after {{ width:5px; height:5px; top:-3px; margin-left:-2.5px;
  box-shadow:0 0 8px 1px rgba(var(--accent-rgb),.55); }}
.ring-two::after {{ width:6px; height:6px; left:0; top:0;
  offset-path:ellipse(50% 50% at 50% 50%); offset-distance:0%; offset-rotate:0deg;
  animation:sigil-orbit-ellipse 24s linear infinite;
  box-shadow:0 0 9px 1px rgba(var(--accent-rgb),.5); opacity:.92; }}
@media (prefers-reduced-motion: reduce) {{
  .ring-one, .ring-three, .ring-two::after {{ animation:none; }}
}}

/* THE SEARCH BAR FREEZES (Phil, 2026-08-24). The language toggle, the search
   box and the document-state readout are the controls you reach for WHILE
   reading, so they have to stay reachable while the document moves under them.
   `--surface` is translucent, so an opaque backdrop is needed or the text of
   the section scrolling beneath shows through the bar. */
.reader-toolbar {{ min-height:74px; margin:24px 0; padding:11px; display:flex; align-items:center;
  justify-content:space-between; gap:20px; flex-wrap:wrap;
  position:sticky; top:0; z-index:40;
  background-color:var(--paper); background-image:linear-gradient(var(--surface), var(--surface));
  border:1px solid var(--line); border-radius:13px;
  box-shadow:0 6px 18px rgba(0,0,0,.18); }}
@media print {{ .reader-toolbar {{ position:static; box-shadow:none; }} }}
.document-search {{ width:min(420px,100%); min-height:48px; padding:0 12px; display:flex;
  align-items:center; gap:10px; border:1px solid var(--line); border-radius:9px; background:var(--surface-2); }}
.document-search svg {{ width:18px; height:18px; flex:none; fill:none; stroke:var(--dim); stroke-width:1.7; }}
.document-search input {{ min-width:0; flex:1; border:0; outline:0; color:var(--ink);
  background:transparent; font-size:14px; font-family:var(--sans); }}
.document-search input::placeholder {{ color:var(--dim); }}
kbd {{ padding:4px 7px; border:1px solid var(--line-strong); border-radius:5px; color:var(--muted);
  background:var(--surface); font:500 10px var(--mono); }}
.search-status {{ padding-right:10px; color:var(--muted); font:500 10px var(--mono);
  text-transform:uppercase; letter-spacing:.08em; }}

.reader-layout {{ display:grid; grid-template-columns:250px minmax(0,1fr); gap:30px; align-items:start; }}
.document-rail {{ position:sticky; top:98px; align-self:start; min-width:0; }}
.rail-sticky {{ max-height:calc(100vh - 116px); overflow-y:auto; overscroll-behavior:contain;
  padding-right:8px; scrollbar-width:thin; scrollbar-gutter:stable; }}
.rail-label {{ margin:0 0 12px; color:var(--dim); font:600 10px var(--mono); letter-spacing:.12em;
  text-transform:uppercase; }}
#document-toc {{ display:grid; gap:3px; }}
#document-toc a {{ padding:10px 11px; border-left:1px solid var(--line); color:var(--muted);
  text-decoration:none; font-size:12px; line-height:1.45; transition:color .15s, border-color .15s, background .15s; }}
#document-toc a:hover {{ color:var(--ink); background:var(--surface-2); }}
#document-toc a.active {{ color:var(--accent); border-left-color:var(--accent);
  background:rgba(var(--accent-rgb),.055); }}
#document-toc a[hidden] {{ display:none; }}
.rail-card {{ margin-top:20px; padding:14px; display:grid; gap:8px; border:1px solid var(--line);
  border-radius:10px; background:var(--surface); }}
.rail-card > span {{ color:var(--muted); font:500 9px var(--mono); letter-spacing:.1em; }}
.rail-card strong {{ font-size:12px; line-height:1.45; }}
.rail-card strong i:not([lang]) {{ display:inline-block; width:6px; height:6px; margin-right:5px;
  border-radius:50%; background:var(--accent); box-shadow:0 0 8px rgba(var(--accent-rgb),.7); }}
.rail-card small {{ color:var(--muted); font-size:10px; line-height:1.5; }}

.document-body {{ min-width:0; display:grid; gap:15px; counter-reset:sec; }}
.document-body > section {{ min-width:0; max-width:100%; counter-increment:sec;
  scroll-margin-top:18px; padding:28px; border:1px solid var(--line); border-radius:15px;
  background:var(--surface); }}
.document-body > section[hidden] {{ display:none; }}
.document-body > .note {{ counter-increment:none; }}
.shead {{ display:flex; align-items:flex-start; gap:13px;
          border-bottom:1px solid var(--line); padding-bottom:17px; margin-bottom:20px; }}
.shead::before {{ content:"\\00A7" counter(sec); flex:none; min-width:34px; padding:6px 8px;
  border:1px solid rgba(var(--accent-rgb),.3); border-radius:6px; color:var(--accent);
  font:600 10px var(--mono); text-align:center; }}
.shead > div {{ min-width:0; }}
.shead h2 {{ font-size:clamp(22px,2.3vw,30px); letter-spacing:-.03em; line-height:1.18; margin:0; }}
.shead p {{ margin:8px 0 0; font-size:13.5px; color:var(--muted); }}
.empty-search {{ min-height:200px; display:grid; place-items:center; padding:30px;
  border:1px dashed var(--line-strong); border-radius:14px; color:var(--muted); text-align:center; }}

.kpis {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:1px;
         background:var(--line); border:1px solid var(--line); border-radius:12px; overflow:hidden; }}
@media (max-width:900px) {{ .kpis {{ grid-template-columns:repeat(2,minmax(0,1fr)); }} }}
@media (max-width:420px) {{ .kpis {{ grid-template-columns:minmax(0,1fr); }} }}
.kpi {{ background:var(--surface); padding:16px 18px 18px; }}
.kpi-l {{ font-family:var(--mono); font-size:9.5px; font-weight:700; letter-spacing:.16em;
          text-transform:uppercase; color:var(--muted); }}
.kpi-v {{ font-family:var(--mono); font-variant-numeric:tabular-nums;
          font-size:26px; font-weight:700; letter-spacing:-.02em; margin-top:8px; line-height:1.1; }}
.kpi-s {{ font-size:12px; color:var(--muted); margin-top:5px; }}

.panel {{ background:var(--surface); border:1px solid var(--line); border-radius:12px;
          padding:20px 22px; box-shadow:var(--shadow); }}
.panel + .panel {{ margin-top:14px; }}
.note {{ border-left:3px solid var(--accent); background:var(--accent-soft);
         border-radius:0 10px 10px 0; padding:14px 18px; }}
.note p:last-child {{ margin-bottom:0; }}
.note-warn {{ border-left-color:var(--neg); background:rgba(var(--neg-fill),.08); }}

.chart {{ width:100%; overflow-x:auto; }}
svg {{ display:block; width:100%; height:auto; }}
.axis {{ display:flex; justify-content:space-between; flex-wrap:wrap; gap:3px 14px;
         font-family:var(--mono); font-size:10.5px; color:var(--muted); margin-top:8px; }}
.smalls {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(250px,1fr)); gap:14px; margin-top:14px; }}
.small h3 {{ display:flex; justify-content:space-between; align-items:baseline; gap:10px; margin-bottom:8px; }}
.small h3 span {{ font-family:var(--mono); font-size:13px; font-weight:700; }}

.tblwrap {{ overflow-x:auto; border:1px solid var(--line); border-radius:12px; background:var(--surface); }}
table {{ border-collapse:collapse; width:100%; min-width:520px; }}
th, td {{ padding:9px 14px; text-align:right; font-size:13px; white-space:nowrap;
          font-family:var(--mono); font-variant-numeric:tabular-nums; border-bottom:1px solid var(--line-2); }}
thead th {{ position:sticky; top:0; background:var(--surface-2); color:var(--muted);
            font-size:9.5px; font-weight:800; letter-spacing:.14em; text-transform:uppercase; }}
tbody th, td:first-child {{ text-align:left; }}
tbody th {{ font-weight:600; color:var(--ink); }}
tbody tr:last-child td, tbody tr:last-child th {{ border-bottom:0; }}
.trow-total th, .trow-total td {{ border-top:2px solid var(--line); background:var(--surface-2); }}
.bar-cell {{ width:34%; min-width:120px; }}
.bar {{ display:block; height:8px; width:var(--p); border-radius:999px; }}
.bar-pos {{ background:rgba(var(--pos-fill),.55); }}
.bar-neg {{ background:rgba(var(--neg-fill),.55); }}

.mgrid {{ border:1px solid var(--line); border-radius:12px; background:var(--surface);
          padding:14px 16px; overflow-x:auto; }}
.mrow {{ display:grid; grid-template-columns:52px minmax(560px,1fr) 108px; gap:12px; align-items:center; }}
.mrow + .mrow {{ margin-top:6px; }}
.mrow-y {{ font-family:var(--mono); font-size:12px; font-weight:700; color:var(--muted); }}
.mrow-cells {{ display:grid; grid-template-columns:repeat(12,1fr); gap:4px; }}
.mrow-t {{ font-family:var(--mono); font-variant-numeric:tabular-nums;
           font-size:12.5px; font-weight:700; text-align:right; }}
.mcell {{ border-radius:6px; padding:7px 4px 6px; text-align:center; line-height:1.15;
          border:1px solid transparent; }}
.mcell-void {{ background:repeating-linear-gradient(135deg,var(--line-2) 0 4px,transparent 4px 8px);
               border-radius:6px; opacity:.55; }}
/* Alpha is capped at .46, not .88: the value sits ON this tint, and a
   full-strength fill drops the ink to 2.35:1 in dark mode. */
.mcell.pos {{ background:rgba(var(--pos-fill),calc(var(--w) * .40 + .06)); }}
.mcell.neg {{ background:rgba(var(--neg-fill),calc(var(--w) * .40 + .06)); }}
.mcell-m {{ display:block; font-family:var(--mono); font-size:8.5px; font-weight:700;
            letter-spacing:.1em; color:var(--muted); }}
.mcell-v {{ display:block; font-family:var(--mono); font-variant-numeric:tabular-nums;
            font-size:11px; font-weight:700; color:var(--ink); margin-top:2px; }}
.mlegend {{ display:flex; align-items:center; gap:8px; margin-top:12px;
            font-family:var(--mono); font-size:10.5px; color:var(--muted); }}
.mlegend i:not([lang]) {{ display:block; width:34px; height:9px; border-radius:3px; }}

.split {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(300px,1fr)); gap:14px; }}
.deflist {{ margin:0; }}
.deflist div {{ display:flex; justify-content:space-between; gap:14px; padding:7px 0;
                border-bottom:1px solid var(--line-2); }}
.deflist div:last-child {{ border-bottom:0; }}
.deflist dt {{ font-size:13px; color:var(--ink-2); }}
.deflist dd {{ margin:0; font-family:var(--mono); font-variant-numeric:tabular-nums;
               font-size:13px; font-weight:700; text-align:right; }}
ol.method {{ padding-left:20px; margin:0; }}
ol.method li {{ margin-bottom:10px; color:var(--ink-2); }}
ol.method li:last-child {{ margin-bottom:0; }}
footer {{ margin-top:52px; padding-top:20px; border-top:1px solid var(--line);
          font-size:12.5px; color:var(--muted); }}
@media (max-width:1050px) {{
  .document-hero {{ grid-template-columns:1fr 300px; }}
  .system-sigil {{ width:280px; }}
  .reader-layout {{ grid-template-columns:210px minmax(0,1fr); gap:20px; }}
}}
@media (max-width:760px) {{
  .wrap {{ width:calc(100% - 24px); }}
  .document-hero {{ min-height:0; grid-template-columns:1fr; gap:0; }}
  .hero-copy {{ padding:36px 0 8px; }}
  .system-sigil {{ display:none; }}
  .reader-toolbar {{ display:grid; }}
  .document-search {{ width:100%; }}
  .search-status {{ display:none; }}
  .reader-layout {{ grid-template-columns:minmax(0,1fr); }}
  .document-rail {{ display:none; }}
  .document-body > section {{ padding:20px; }}
}}
@media (max-width:640px) {{
  .wrap {{ padding-bottom:56px; }}
  .mrow {{ grid-template-columns:44px minmax(520px,1fr) 92px; }}
  .kpi-v {{ font-size:22px; }}
  .document-meta {{ display:grid; grid-template-columns:1fr 1fr; }}
  .document-body > section {{ padding:16px; }}
  .shead {{ gap:9px; }}
}}
@media (prefers-reduced-motion:no-preference) {{
  .curve-line {{ stroke-dasharray:4200; stroke-dashoffset:4200; animation:draw 1.5s ease-out forwards; }}
  @keyframes draw {{ to {{ stroke-dashoffset:0; }} }}
}}
@media (prefers-reduced-motion:reduce) {{
  html {{ scroll-behavior:auto; }}
  *, *::before, *::after {{ animation-duration:.01ms !important; transition-duration:.01ms !important; }}
}}
:focus-visible {{ outline:2px solid var(--accent); outline-offset:3px; }}
@media print {{
  body {{ background:#fff; background-image:none; }}
  .reading-progress, .reader-toolbar, .document-rail, .system-sigil, .skip-link {{ display:none !important; }}
  .wrap {{ width:100%; }}
  .reader-layout, .document-body {{ display:block; }}
  .document-body > section {{ break-inside:avoid; margin:0 0 14px; box-shadow:none; }}
}}
{EXTRA_CSS}
{LANG_CSS}
"""

READER_JS = """
<script>
/* The blueprint reader's behaviours, on this document's own markup: a contents
   rail built from the section headings, scroll-spy, section search and the
   reading-progress bar. Nothing here is loaded from the site — the tearsheet is
   also published as a standalone file, so it has to carry its own copy. */
(function () {
  var body = document.getElementById('document-body');
  var toc = document.getElementById('document-toc');
  var status = document.getElementById('search-status');
  var input = document.getElementById('tearsheet-search');
  if (!body || !toc) return;

  var sections = [].filter.call(body.children, function (el) { return el.tagName === 'SECTION'; });
  var pairs = [];

  sections.forEach(function (sec, i) {
    if (!sec.id) sec.id = 'sec-' + (i + 1);
    var heading = sec.querySelector('.shead h2');
    if (!heading) return;
    var link = document.createElement('a');
    link.href = '#' + sec.id;
    /* The heading is bilingual markup — two <i> elements, one hidden by CSS.
       Copying it wholesale keeps the contents list in whichever language the
       reader has chosen, with no second translation table to maintain. */
    link.innerHTML = heading.innerHTML;
    link.addEventListener('click', function (e) {
      e.preventDefault();
      sec.scrollIntoView({ block: 'start' });
    });
    toc.appendChild(link);
    pairs.push({ section: sec, link: link });
  });

  if ('IntersectionObserver' in window) {
    var spy = new IntersectionObserver(function (entries) {
      var seen = entries.filter(function (e) { return e.isIntersecting; })
        .sort(function (a, b) { return a.boundingClientRect.top - b.boundingClientRect.top; })[0];
      if (!seen) return;
      pairs.forEach(function (p) {
        p.link.classList.toggle('active', p.section === seen.target);
      });
    }, { rootMargin: '-20px 0px -70% 0px', threshold: 0 });
    pairs.forEach(function (p) { spy.observe(p.section); });
  }

  function label(en, ta) {
    return '<span class="tr"><i lang="en">' + en + '</i><i lang="ta">' + ta + '</i></span>';
  }

  function search() {
    var q = input.value.trim().toLowerCase();
    var shown = 0;
    pairs.forEach(function (p) {
      var hit = !q || p.section.textContent.toLowerCase().indexOf(q) !== -1;
      p.section.hidden = !hit;
      p.link.hidden = !hit;
      if (hit) shown += 1;
    });
    status.innerHTML = q
      ? label(shown + ' section' + (shown === 1 ? '' : 's') + ' found', shown + ' \\u0baa\\u0bbf\\u0bb0\\u0bbf\\u0bb5\\u0bc1\\u0b95\\u0bb3\\u0bcd')
      : label('Full document', '\\u0bae\\u0bc1\\u0bb4\\u0bc1 \\u0b86\\u0bb5\\u0ba3\\u0bae\\u0bcd');
    var empty = body.querySelector('.empty-search');
    if (empty) empty.remove();
    if (q && shown === 0) {
      var note = document.createElement('div');
      note.className = 'empty-search';
      note.textContent = 'No section of this tearsheet contains \\u201C' + input.value.trim() + '\\u201D.';
      body.appendChild(note);
    }
  }

  if (input) {
    input.addEventListener('input', search);
    document.addEventListener('keydown', function (e) {
      if ((e.metaKey || e.ctrlKey) && e.key && e.key.toLowerCase() === 'k') {
        e.preventDefault();
        input.focus();
      }
    });
  }

  var bar = document.getElementById('reading-progress-bar');
  if (bar) {
    var tick = function () {
      var room = document.documentElement.scrollHeight - window.innerHeight;
      bar.style.width = (room > 0 ? Math.min(100, (window.scrollY / room) * 100) : 0) + '%';
    };
    addEventListener('scroll', tick, { passive: true });
    addEventListener('resize', tick, { passive: true });
    tick();
  }
})();
</script>
"""

CHART_JS = """
<script>
(function () {
  var DATA = __SERIES__;
  var cv = document.getElementById('cycle'), tip = document.getElementById('cycle-tip');
  if (!cv) return;
  var box = cv.parentNode, hover = -1, geom = null;

  function tok(n) { return getComputedStyle(document.documentElement).getPropertyValue(n).trim(); }
  function rupee(v) {
    var s = Math.abs(Math.round(v)).toString(), o = '';
    if (s.length > 3) {
      var h = s.slice(0, -3), t = s.slice(-3), p = [];
      while (h.length > 2) { p.unshift(h.slice(-2)); h = h.slice(0, -2); }
      if (h) p.unshift(h);
      o = p.join(',') + ',' + t;
    } else o = s;
    return (v < 0 ? '-' : '') + '\\u20B9' + o;
  }

  function draw() {
    var dpr = window.devicePixelRatio || 1;
    var w = box.clientWidth, h = 340;
    cv.width = w * dpr; cv.height = h * dpr;
    cv.style.height = h + 'px';
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

    // grid + rupee axis
    g.font = '10px ui-monospace, Menlo, monospace';
    g.textAlign = 'right'; g.textBaseline = 'middle';
    var steps = 5;
    for (var s = 0; s <= steps; s++) {
      var v = cMin + (cMax - cMin) * s / steps, y = Y(v);
      g.strokeStyle = grid; g.lineWidth = 1;
      g.beginPath(); g.moveTo(padL, y + 0.5); g.lineTo(w - padR, y + 0.5); g.stroke();
      g.fillStyle = muted;
      g.fillText(Math.round(v / 1000) + 'k', padL - 8, y);
    }

    // daily bars, hugging the zero line of their own half-height band
    var bw = Math.max(1, iw / DATA.length * 0.7);
    var barH = 38, barZero = h - 46;
    g.strokeStyle = grid; g.lineWidth = 1;
    g.beginPath(); g.moveTo(padL, barZero + 0.5); g.lineTo(w - padR, barZero + 0.5); g.stroke();
    for (var i = 0; i < DATA.length; i++) {
      var p = DATA[i][1];
      if (!p) continue;
      var hgt = Math.abs(p) / (dMax || 1) * barH;
      g.fillStyle = 'rgba(' + (p > 0 ? pos : neg) + ',' + (i === hover ? 0.95 : 0.45) + ')';
      g.fillRect(X(i) - bw / 2, p > 0 ? barZero - hgt : barZero, bw, hgt);
    }
    g.fillStyle = muted; g.textAlign = 'right'; g.textBaseline = 'middle';
    g.fillText('day', padL - 8, barZero);

    // cumulative line
    g.beginPath();
    for (var j = 0; j < DATA.length; j++) {
      var x = X(j), y = Y(DATA[j][2]);
      j ? g.lineTo(x, y) : g.moveTo(x, y);
    }
    g.strokeStyle = line; g.lineWidth = 1.8; g.lineJoin = 'round'; g.stroke();

    // year ticks
    g.textAlign = 'center'; g.textBaseline = 'top'; g.fillStyle = muted;
    var seen = {};
    for (var k = 0; k < DATA.length; k++) {
      var yr = DATA[k][0].slice(0, 4);
      if (seen[yr]) continue;
      seen[yr] = 1;
      g.strokeStyle = grid;
      g.beginPath(); g.moveTo(X(k) + 0.5, padT); g.lineTo(X(k) + 0.5, h - padB); g.stroke();
      g.fillText(yr, X(k), h - padB + 6);
    }

    if (hover >= 0) {
      g.strokeStyle = muted; g.lineWidth = 1; g.setLineDash([3, 3]);
      g.beginPath(); g.moveTo(X(hover) + 0.5, padT); g.lineTo(X(hover) + 0.5, h - padB); g.stroke();
      g.setLineDash([]);
      g.fillStyle = line;
      g.beginPath(); g.arc(X(hover), Y(DATA[hover][2]), 3.5, 0, 6.284); g.fill();
    }
    geom = { padL: padL, iw: iw, X: X, Y: Y };
  }

  function at(ev) {
    var rect = cv.getBoundingClientRect();
    var x = (ev.touches ? ev.touches[0].clientX : ev.clientX) - rect.left;
    var i = Math.round((x - geom.padL) / geom.iw * (DATA.length - 1));
    return Math.max(0, Math.min(DATA.length - 1, i));
  }
  function show(ev) {
    hover = at(ev); draw();
    var d = DATA[hover];
    tip.innerHTML = '<b>' + d[0] + '</b>' +
      'day ' + rupee(d[1]) + '  &middot; ' + d[3] + ' trade' + (d[3] > 1 ? 's' : '') + '<br>' +
      'cumulative ' + rupee(d[2]);
    tip.style.opacity = 1;
    tip.style.left = Math.min(box.clientWidth - 20, Math.max(70, geom.X(hover))) + 'px';
    tip.style.top = (geom.Y(d[2]) - 12) + 'px';
  }
  function hide() { hover = -1; tip.style.opacity = 0; draw(); }

  cv.addEventListener('mousemove', show);
  cv.addEventListener('mouseleave', hide);
  cv.addEventListener('touchstart', function (e) { show(e); }, { passive: true });
  cv.addEventListener('touchmove', function (e) { show(e); }, { passive: true });
  cv.addEventListener('touchend', hide);
  window.addEventListener('resize', draw);
  if (window.matchMedia) {
    var mq = window.matchMedia('(prefers-color-scheme: dark)');
    (mq.addEventListener ? mq.addEventListener.bind(mq, 'change') : mq.addListener.bind(mq))(draw);
  }
  new MutationObserver(draw).observe(document.documentElement,
    { attributes: true, attributeFilter: ['data-theme'] });
  draw();
})();
</script>
"""

ACCENTS = {
    # light, dark. The light value is measured against the accent-tinted chip
    # (--accent-soft over the card), which is the worst ground it lands on, not
    # against the card. cyan 4.62:1, amber 6.01:1, teal 4.83:1.
    "hybrid": ("#0e7490", "#22d3ee"),  # cyan — the hand-driven book
    "auto": ("#92400e", "#f59e0b"),  # amber — the one that drives itself
    "vrule": ("#0f766e", "#2dd4bf"),  # teal — the ladder
}


def recolour(css: str, doc: str) -> str:
    """Repaint the shared stylesheet in one sheet\'s own hue.

    Everything a reader sees tinted resolves through `--accent`, so one
    substitution moves the whole document. The pill that opens each sheet in the
    app carries the SAME pair, which is the point: the colour of the pill is a
    promise about what opens.
    """
    light, dark = ACCENTS[doc]
    base_light, base_dark = ACCENTS["hybrid"]
    out = css
    for old, new in ((base_light, light), (base_dark, dark)):
        if old == new:
            continue
        rgb = ",".join(str(int(new[i : i + 2], 16)) for i in (1, 3, 5))
        old_rgb = ",".join(str(int(old[i : i + 2], 16)) for i in (1, 3, 5))
        out = out.replace(f"--accent:{old}", f"--accent:{new}")
        out = out.replace(f"--accent-rgb:{old_rgb}", f"--accent-rgb:{rgb}")
        out = out.replace(f"rgba({old_rgb},", f"rgba({rgb},")
    return out


def r(n, dp=0):
    """Indian-format a rupee figure with a sign."""
    n = float(n)
    sign = "-" if n < 0 else ""
    v = abs(n)
    if dp:
        whole, frac = divmod(round(v * 100), 100)
        tail = f".{frac:02d}"
    else:
        whole, tail = int(round(v)), ""
    s = str(whole)
    if len(s) > 3:
        head, last3 = s[:-3], s[-3:]
        parts = []
        while len(head) > 2:
            parts.insert(0, head[-2:])
            head = head[:-2]
        if head:
            parts.insert(0, head)
        s = ",".join(parts + [last3])
    return f"{sign}₹{s}{tail}"


def lakh(n):
    return f"{n / 100000:.2f}L"


def cls(n):
    return "pos" if n > 0 else ("neg" if n < 0 else "flat")


def curve_svg(points, w=1040, h=260, pad=1):
    ys = [p[1] for p in points]
    lo, hi = min(ys + [0]), max(ys)
    span = (hi - lo) or 1
    n = len(points) - 1

    def X(i):
        return i / n * w

    def Y(v):
        return h - (v - lo) / span * (h - pad * 2) - pad

    line = " ".join(f"{'M' if i == 0 else 'L'}{X(i):.1f},{Y(v):.1f}" for i, (_, v) in enumerate(points))
    area = line + f" L{w},{Y(lo):.1f} L0,{Y(lo):.1f} Z"
    zero = Y(0)
    # running peak, to shade the underwater stretches
    peak, under = -1e18, []
    for i, (_, v) in enumerate(points):
        peak = max(peak, v)
        under.append((X(i), Y(v), Y(peak)))
    dd = " ".join(f"{'M' if i == 0 else 'L'}{x:.1f},{yp:.1f}" for i, (x, _, yp) in enumerate(under))
    dd += " " + " ".join(f"L{x:.1f},{y:.1f}" for x, y, _ in reversed(under)) + " Z"
    return line, area, dd, zero, hi, lo


def spark(points, w=330, h=84):
    ys = [p[1] for p in points]
    lo, hi = min(ys + [0]), max(ys)
    span = (hi - lo) or 1
    n = len(points) - 1
    pts = [(i / n * w, h - (v - lo) / span * (h - 6) - 3) for i, (_, v) in enumerate(points)]
    line = " ".join(f"{'M' if i == 0 else 'L'}{x:.1f},{y:.1f}" for i, (x, y) in enumerate(pts))
    zero = h - (0 - lo) / span * (h - 6) - 3
    return line, zero


def daily_ledger(series, t, t_attr, r, cls, noun_en="trades", noun_ta="டிரேடுகள்"):
    """The five-year sheet's compact ledger: ONE ROW PER DAY, not per trade.

    Phil, 2026-09-02: "I don't want that long list of day trades but cutshort
    like the one in Options". The siblings each carried the whole book instead
    -- every night with its contract, RSI, strike and gap -- which is a
    different document from a ledger. Four columns, a year filter, and a
    running balance is what the template means by one.

    `series` is [date, day_net, running_total, n_trades] per trading day, which
    is what every book can produce whatever its rows look like underneath.
    """
    days = len(series)
    green = sum(1 for d in series if d[1] > 0)
    years = sorted({str(d[0])[:4] for d in series})
    btns = "".join(f'<button type="button" data-year="{y}" aria-pressed="false">{y}</button>' for y in years)
    rows = "".join(
        f'<tr data-year="{str(d[0])[:4]}">'
        f'<th scope="row">{d[0]}</th>'
        f"<td>{d[3]}</td>"
        f'<td class="{cls(d[1])}">{r(d[1])}</td>'
        f"<td>{r(d[2])}</td></tr>"
        for d in series
    )
    best = max(series, key=lambda d: d[1]) if series else ("", 0, 0, 0)
    worst = min(series, key=lambda d: d[1]) if series else ("", 0, 0, 0)
    avg = (sum(d[1] for d in series) / days) if days else 0.0
    return f"""
<section id="ledger">
  <div class="shead"><div><h2>{t("Daily P&amp;L ledger", "தினசரி லாப-நஷ்ட பதிவேடு")}</h2>
    <p>{t(f"Every trading day in the record, with the running balance beside it. Filter by year, or scroll the whole {days} rows.", f"பதிவில் உள்ள ஒவ்வொரு வர்த்தக நாளும், அதனுடன் ஓடும் இருப்பும். ஆண்டு வாரியாக வடிகட்டலாம், அல்லது {days} வரிகளையும் உருட்டிப் பார்க்கலாம்.")}</p></div></div>
  <div class="ledger-controls" id="ledger-years">
    <button type="button" data-year="all" aria-pressed="true">{t("All", "அனைத்தும்")}</button>
    {btns}
  </div>
  <div class="ledger-scroll" id="ledger-body" tabindex="0" role="region"
       {t_attr("aria-label", "Daily profit and loss ledger", "தினசரி லாப நஷ்ட பதிவு")} data-total="{days}">
    <table id="ledger-table" data-total="{days}">
      <thead><tr>
        <th scope="col">{t("Date", "தேதி")}</th>
        <th scope="col">{t(noun_en.capitalize(), noun_ta)}</th>
        <th scope="col">{t("Day net", "நாளின் நிகரம்")}</th>
        <th scope="col">{t("Running total", "ஓடும் மொத்தம்")}</th>
      </tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
  <div class="ledger-foot">
    <span><span id="ledger-count">{days}</span> {t("days shown", "நாட்கள் காட்டப்படுகின்றன")}</span>
    <span>{t("green", "பச்சை")} {green} &middot; {t("red", "சிவப்பு")} {days - green}</span>
    <span>{t("average", "சராசரி")} {r(avg)}</span>
    <span style="margin-left:auto">{t("best", "சிறந்தது")} {r(best[1])} &middot; {t("worst", "மோசமானது")} {r(worst[1])}</span>
  </div>
</section>"""


def daily_series(rows, date_of, net_of):
    """Fold any book's rows into [date, day_net, running, n] per trading day."""
    from collections import defaultdict

    by_day = defaultdict(lambda: [0.0, 0])
    for row in rows:
        d = str(date_of(row))[:10]
        by_day[d][0] += float(net_of(row) or 0.0)
        by_day[d][1] += 1
    out, run = [], 0.0
    for d in sorted(by_day):
        run += by_day[d][0]
        out.append([d, round(by_day[d][0], 2), round(run, 2), by_day[d][1]])
    return out


def method_and_limits(t, steps, running=None):
    """The three sections every sheet in this family ends with.

    Method, what is running today, and what the document is not. The last was
    the same paragraph on all five and is kept here rather than copied four
    times -- a disclaimer that drifts between documents is worse than none.
    `steps` is [(english, tamil)] describing how THAT book was produced, which
    is the only part that differs.
    """
    items = "".join(f"<li>{t(en, ta)}</li>" for en, ta in steps)
    today = ""
    if running:
        today = f"""
<section>
  <div class="shead"><div><h2>{t("What is running today", "இன்று இயங்குவது என்ன")}</h2></div></div>
  <div class="panel"><p>{t(running[0], running[1])}</p></div>
</section>
"""
    return f"""{today}
<section>
  <div class="shead"><div><h2>{t("Method", "முறை")}</h2></div></div>
  <div class="panel"><ol class="method">{items}</ol></div>
  <div class="note note-warn" style="margin-top:14px">
    <h2 class="note-h">{t("What this document is not", "இந்த ஆவணம் எது அல்ல")}</h2>
    <p>{t("It is a backtest. It assumes every signal was filled at the recorded premium, with no rejection, no partial fill and no slippage beyond the modelled costs. Live execution adds all three. Past behaviour of an index, its lot size and its expiry calendar is not a commitment that any of them stay put &mdash; and this record already contains two such changes. Nothing here is investment advice or an offer to manage money.", "இது ஒரு பேக்டெஸ்ட். ஒவ்வொரு சிக்னலும் பதிவான பிரீமியத்தில் நிறைவேறியதாக, நிராகரிப்பு இல்லாமல், பகுதி நிறைவேற்றம் இல்லாமல், கணக்கிட்ட கட்டணங்களுக்கு மேல் ஸ்லிப்பேஜ் இல்லாமல் கருதுகிறது. லைவ் செயல்பாடு இந்த மூன்றையும் சேர்க்கிறது. ஒரு குறியீட்டின் கடந்தகால நடத்தை, அதன் லாட் அளவு, எக்ஸ்பயரி நாட்காட்டி ஆகியவை அப்படியே நீடிக்கும் என்பதற்கு உத்தரவாதம் இல்லை &mdash; இந்தப் பதிவிலேயே அத்தகைய இரண்டு மாற்றங்கள் உள்ளன. இங்குள்ள எதுவும் முதலீட்டு ஆலோசனை அல்ல, பணத்தை நிர்வகிக்கும் சலுகையும் அல்ல.")}</p>
  </div>
</section>"""


def cycle_section(series, t, r, noun_en="trading days", noun_ta="வர்த்தக நாட்கள்"):
    """The daily-income canvas: a bar a day, a line for the running total.

    The last section of the five-year sheet the siblings did not have, and the
    only one that was a build rather than a rename -- its drawing code lives in
    CHART_JS, which they did not borrow. They do now, and it guards on the
    canvas existing (`if (!cv) return`), so borrowing it costs a sheet nothing
    if it ever drops the section.

    `series` is the same [date, day_net, running, n] the ledger takes, so a
    book that can draw one can draw the other.
    """
    days = len(series)
    green = sum(1 for d in series if d[1] > 0)
    avg = (sum(d[1] for d in series) / days) if days else 0.0
    best = max(series, key=lambda d: d[1]) if series else ("", 0)
    worst = min(series, key=lambda d: d[1]) if series else ("", 0)
    first = series[0][0] if series else ""
    last = series[-1][0] if series else ""
    total = series[-1][2] if series else 0
    return f"""
<section>
  <div class="shead">
    <div><h2>{t("Daily income across the whole cycle", "முழு சுழற்சியின் தினசரி வருமானம்")}</h2>
    <p>{t(f"Every one of the {days} {noun_en} in the record. Bars are that day's net income; the line is the running total. Hover any day for its figure.", f"பதிவில் உள்ள {days} {noun_ta} அனைத்தும். கம்பிகள் அந்நாளின் நிகர வருமானம்; கோடு ஓடும் மொத்தம். எந்த நாளின் மீதும் சுட்டியை வையுங்கள்.")}</p></div>
  </div>
  <div class="panel">
    <div class="canvas-wrap">
      <canvas id="cycle" role="img"
        aria-label="Daily profit bars and cumulative net profit for all {days} {noun_en} from {first} to {last}, ending at {r(total)}"></canvas>
      <div class="tip" id="cycle-tip" role="status"></div>
    </div>
    <div class="legend">
      <span><i style="background:var(--curve)"></i>{t("cumulative net", "ஒட்டுமொத்த நிகரம்")}</span>
      <span><i class="bar" style="background:rgba(var(--pos-fill),.55)"></i>{t("profitable day", "லாப நாள்")}</span>
      <span><i class="bar" style="background:rgba(var(--neg-fill),.55)"></i>{t("losing day", "நஷ்ட நாள்")}</span>
      <span style="margin-left:auto">{t("hover or drag for any single day", "ஒரு நாளைப் பார்க்க நகர்த்துங்கள்")}</span>
    </div>
    <div class="axis"><span>{days} {t(noun_en, noun_ta)}</span>
      <span>{green} {t("green", "பச்சை")} ({100 * green / days if days else 0:.0f}%)</span>
      <span>{t("average day", "சராசரி நாள்")} {r(avg)} &middot; {t("best", "சிறந்தது")} {r(best[1])} &middot; {t("worst", "மோசமானது")} {r(worst[1])}</span></div>
  </div>
</section>"""
