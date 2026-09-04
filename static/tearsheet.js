

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
      ? label(shown + ' section' + (shown === 1 ? '' : 's') + ' found', shown + ' \u0baa\u0bbf\u0bb0\u0bbf\u0bb5\u0bc1\u0b95\u0bb3\u0bcd')
      : label('Full document', '\u0bae\u0bc1\u0bb4\u0bc1 \u0b86\u0bb5\u0ba3\u0bae\u0bcd');
    var empty = body.querySelector('.empty-search');
    if (empty) empty.remove();
    if (q && shown === 0) {
      var note = document.createElement('div');
      note.className = 'empty-search';
      note.textContent = 'No section of this tearsheet contains \u201C' + input.value.trim() + '\u201D.';
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
