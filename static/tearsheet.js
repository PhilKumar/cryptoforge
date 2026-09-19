

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
