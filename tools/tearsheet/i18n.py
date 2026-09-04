"""Bilingual helper for the tearsheet.

Every visible string goes through `t(en, ta)`. It emits both languages inline;
CSS shows one. Doing it inline (rather than rendering the whole body twice)
keeps element IDs unique, so the canvas and the daily table exist once.
"""


def t(en, ta):
    return f'<span class="tr"><i lang="en">{en}</i><i lang="ta">{ta}</i></span>'


def t_attr(name, en, ta):
    """Bilingual text for an ATTRIBUTE, where `t()`'s markup cannot go.

    `t()` emits a <span>, and inside `aria-label="…"` its very first quote
    closes the attribute — the browser then renders the rest of the tag as
    visible text. That is what put `" data-total="891">` on the page above the
    daily ledger. CSS cannot switch an attribute either, so the English text
    sits in the attribute itself and the pair rides alongside in data-*, which
    LANG_JS swaps on toggle — the same trick the search placeholder already uses.
    """
    esc = lambda s: str(s).replace("&", "&amp;").replace('"', "&quot;").replace("<", "&lt;")  # noqa: E731
    return f'{name}="{esc(en)}" data-l10n-attr="{esc(name)}" data-l10n-en="{esc(en)}" data-l10n-ta="{esc(ta)}"'


LANG_CSS = """
.tr > i { font-style:normal; }
html:not([data-lang="ta"]) .tr > i[lang="ta"] { display:none; }
html[data-lang="ta"] .tr > i[lang="en"] { display:none; }
html[data-lang="ta"] i[lang="ta"] { font-family:"Noto Sans Tamil","Latha","Nirmala UI",sans-serif; line-height:1.75; }
/* The language bar sits inside the reader toolbar beside the search box, so it
   carries no outer margin of its own. */
.langbar { display:flex; gap:4px; padding:4px; margin:0;
           border:1px solid var(--line); border-radius:999px; background:var(--surface-2);
           width:max-content; }
.langbar button { appearance:none; border:0; cursor:pointer; background:transparent;
                  color:var(--muted); font-family:var(--mono); font-size:11px; font-weight:800;
                  letter-spacing:.12em; text-transform:uppercase; padding:7px 16px;
                  border-radius:999px; transition:background .15s ease, color .15s ease; }
.langbar button:hover { color:var(--ink); }
.langbar button[aria-selected="true"] { background:var(--accent); color:var(--surface); }
:root[data-theme="dark"] .langbar button[aria-selected="true"],
html[data-theme="dark"] .langbar button[aria-selected="true"] { color:#0A1119; }
.langbar button:focus-visible { outline:2px solid var(--accent); outline-offset:2px; }

/* Daily ledger */
/* The legend items are inline-flex, so they must NOT be given
   `overflow-wrap:anywhere` — it drops their min-content width to one glyph and
   the labels wrap a character per line. They wrap as whole items instead, and
   the long hover hint (the only string that overflowed in Tamil) is simply
   dropped on a phone, where there is no hover anyway. */
/* No `margin-left:auto` on the hint: inside a wrapping flex row it resolves
   against the wrong line's free space and pushes ~50px of scrollable overflow
   onto the document. It sits as an ordinary fourth item, and is dropped on
   touch widths where there is no hover to hint at. */
/* No opacity here: it composites --muted down to 4.07:1 on the light surface,
   under the 4.5:1 floor. The hint is already de-emphasised by --muted alone. */
.legend > span:last-child { color:var(--muted); }
@media (max-width: 960px) { .legend > span:last-child { display:none; } }
.ledger-controls { display:flex; flex-wrap:wrap; gap:6px; margin-bottom:12px; align-items:center; }
.ledger-controls button { appearance:none; cursor:pointer; background:var(--surface-2);
  border:1px solid var(--line); color:var(--ink-2); font-family:var(--mono); font-size:11px;
  font-weight:700; letter-spacing:.08em; padding:6px 13px; border-radius:999px; }
.ledger-controls button[aria-pressed="true"] { background:var(--accent); color:var(--surface);
  border-color:var(--accent); }
:root[data-theme="dark"] .ledger-controls button[aria-pressed="true"],
html[data-theme="dark"] .ledger-controls button[aria-pressed="true"] { color:#0A1119; }
.ledger-scroll { max-height:520px; overflow:auto; border:1px solid var(--line);
                 border-radius:12px; background:var(--surface); }
.ledger-scroll table { min-width:520px; }
.ledger-scroll thead th { position:sticky; top:0; z-index:1; }
.ledger-foot { display:flex; flex-wrap:wrap; gap:8px 22px; margin-top:10px;
               font-family:var(--mono); font-size:11.5px; color:var(--muted); }
tr.wk td, tr.wk th { background:var(--surface-2); }
"""

LANG_JS = """
<script>
(function () {
  var root = document.documentElement, bar = document.getElementById('langbar');
  function set(l) {
    root.setAttribute('data-lang', l);
    root.setAttribute('lang', l === 'ta' ? 'ta' : 'en');
    [].forEach.call(bar.querySelectorAll('button'), function (b) {
      b.setAttribute('aria-selected', String(b.dataset.lang === l));
    });
    // A placeholder is an attribute, so it cannot carry the two-<i> markup the
    // rest of the page uses; it is the one string that has to be swapped here.
    var find = document.getElementById('tearsheet-search');
    if (find) find.placeholder = find.dataset['ph' + (l === 'ta' ? 'Ta' : 'En')];
    // Every other attribute string, marked up by t_attr(). Putting t()'s span
    // in an attribute closes it on the first quote and spills the tag onto the
    // page, so they carry their translation in data-* and get swapped here.
    [].forEach.call(document.querySelectorAll('[data-l10n-attr]'), function (el) {
      var v = el.getAttribute('data-l10n-' + (l === 'ta' ? 'ta' : 'en'));
      if (v !== null) el.setAttribute(el.getAttribute('data-l10n-attr'), v);
    });
    try { localStorage.setItem('pf-tearsheet-lang', l); } catch (e) {}
    window.dispatchEvent(new Event('resize'));
  }
  bar.addEventListener('click', function (e) {
    var b = e.target.closest('button'); if (b) set(b.dataset.lang);
  });
  var saved = null;
  try { saved = localStorage.getItem('pf-tearsheet-lang'); } catch (e) {}
  set(saved === 'ta' ? 'ta' : 'en');

  // Daily ledger year filter
  var lg = document.getElementById('ledger');
  if (!lg) return;
  var ctl = document.getElementById('ledger-years');
  ctl.addEventListener('click', function (e) {
    var b = e.target.closest('button'); if (!b) return;
    var y = b.dataset.year;
    [].forEach.call(ctl.querySelectorAll('button'), function (x) {
      x.setAttribute('aria-pressed', String(x === b));
    });
    [].forEach.call(lg.querySelectorAll('tbody tr'), function (row) {
      row.style.display = (y === 'all' || row.dataset.year === y) ? '' : 'none';
    });
    document.getElementById('ledger-count').textContent =
      (y === 'all' ? lg.dataset.total : lg.querySelectorAll('tbody tr[data-year="' + y + '"]').length);
  });
})();
</script>
"""
