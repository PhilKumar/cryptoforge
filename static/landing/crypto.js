/* CryptoForge landing — all motion lives here, external, under the CSP.
   Contract: nothing draws off-screen or in a hidden tab, canvases are
   sized once per resize at a capped DPR (never from their own attributes),
   and reduced-motion turns every loop off. */
(function () {
  'use strict';

  /* The terminal used to live at "/", so old bookmarks and PWA shortcuts still
     point at /#cascade and friends. Hand those straight through to /app rather
     than dropping someone on the marketing page. This runs before anything
     else draws — e2e-tests/04-app-shell-navigation asserts it. */
  const APP_TABS = ['journal', 'portfolio', 'cascade', 'dashboard', 'scalp',
    'live', 'builder', 'market', 'results'];
  const hash = (location.hash || '').replace('#', '');
  if (APP_TABS.indexOf(hash) !== -1) { location.replace('/app#' + hash); return; }

  const reduce = matchMedia('(prefers-reduced-motion: reduce)').matches;
  const DPR = Math.min(devicePixelRatio || 1, 2);

  /* ── size a canvas from CSS px, once — never read back its own attrs ── */
  function fit(c, scale) {
    const s = scale || 1;
    const r = c.getBoundingClientRect();
    const w = Math.max(1, Math.round(r.width * DPR * s));
    const h = Math.max(1, Math.round(r.height * DPR * s));
    if (c.width !== w || c.height !== h) { c.width = w; c.height = h; }
    return { w, h };
  }

  /* one visibility gate for every loop: off-screen or hidden tab = no frames */
  const running = new Map();
  const io = 'IntersectionObserver' in window
    ? new IntersectionObserver((es) => es.forEach((e) => running.set(e.target, e.isIntersecting)), { threshold: 0.02 })
    : null;
  const live = (el) => !document.hidden && (running.get(el) !== false);
  const watch = (el) => { if (io) io.observe(el); else running.set(el, true); };

  /* ── nav ────────────────────────────────────────────────────────── */
  const nav = document.getElementById('nav');
  const onScroll = () => nav.classList.toggle('stuck', scrollY > 24);
  addEventListener('scroll', onScroll, { passive: true });
  onScroll();

  /* ── the mobile menu ────────────────────────────────────────────── */
  /* Below 860px the nav links live in a panel. Every way a person expects to
     dismiss a menu closes it: the button, Escape, tapping a link, tapping
     outside it, and widening the window past the breakpoint. */
  (function menu() {
    const btn = document.getElementById('navToggle');
    const panel = document.getElementById('navlinks');
    if (!btn || !panel) return;
    const root = document.documentElement;
    const isOpen = () => btn.getAttribute('aria-expanded') === 'true';
    const set = (open) => {
      btn.setAttribute('aria-expanded', open ? 'true' : 'false');
      panel.classList.toggle('open', open);
      root.classList.toggle('nav-open', open);
    };
    btn.addEventListener('click', () => set(!isOpen()));
    // Closing on link click matters here: every section link is a same-page
    // anchor, so without this the panel would stay over the thing you asked for.
    panel.addEventListener('click', (e) => { if (e.target.closest('a')) set(false); });
    addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && isOpen()) { set(false); btn.focus(); }
    });
    document.addEventListener('click', (e) => {
      if (isOpen() && !panel.contains(e.target) && !btn.contains(e.target)) set(false);
    });
    // Rotating a phone to landscape can cross the breakpoint, which would leave
    // the panel class on a row of links that is visible anyway.
    const mq = matchMedia('(max-width: 860px)');
    const sync = () => { if (!mq.matches && isOpen()) set(false); };
    mq.addEventListener ? mq.addEventListener('change', sync) : addEventListener('resize', sync);
  })();

  /* ── reveals ────────────────────────────────────────────────────── */
  const rv = [...document.querySelectorAll('.rv')];
  if (io && !reduce) {
    const ro = new IntersectionObserver((es) => es.forEach((e) => {
      if (e.isIntersecting) { e.target.classList.add('in'); ro.unobserve(e.target); }
    }), { threshold: 0.18, rootMargin: '0px 0px -6% 0px' });
    rv.forEach((el) => ro.observe(el));
  } else {
    rv.forEach((el) => el.classList.add('in'));
  }

  /* ── count-up ───────────────────────────────────────────────────── */
  const counters = [...document.querySelectorAll('[data-count]')];
  const runCount = (el) => {
    const to = +el.dataset.count, suf = el.dataset.suffix || '';
    if (reduce) { el.textContent = to + suf; return; }
    const t0 = performance.now(), dur = 1400;
    const tick = (t) => {
      const p = Math.min(1, (t - t0) / dur), e = 1 - Math.pow(1 - p, 3);
      el.textContent = Math.round(to * e) + suf;
      if (p < 1) requestAnimationFrame(tick);
    };
    requestAnimationFrame(tick);
  };
  if (io) {
    const co = new IntersectionObserver((es) => es.forEach((e) => {
      if (e.isIntersecting) { runCount(e.target); co.unobserve(e.target); }
    }), { threshold: 0.6 });
    counters.forEach((el) => co.observe(el));
  } else counters.forEach(runCount);

  /* ── tape ───────────────────────────────────────────────────────── */
  const tape = document.getElementById('tapeRun');
  if (tape) {
    const items = [
      ['Binance', 'mainnet'], ['every trade', 'journaled'], ['fees', 'shown, not rounded'],
      ['losing days', 'published'], ['kill all', 'one tap'], ['accounts', 'password · authenticator · passkey'],
      ['viewer role', 'see everything, change nothing'], ['alerts', 'wait to be seen'], ['times', 'IST'],
      ['no', 'opinions'], ['no', 'return figure'], ['no', 'advice'], ['installs', 'as an app'],
    ];
    const frag = document.createDocumentFragment();
    for (let k = 0; k < 2; k++) items.forEach(([a, b]) => {
      const s = document.createElement('span');
      const strong = document.createElement('b'); strong.textContent = a;
      s.appendChild(strong); s.appendChild(document.createTextNode(' ' + b));
      frag.appendChild(s);
    });
    tape.appendChild(frag);
  }

  /* ── cursor light on glass cards + desk window ──────────────────── */
  const lit = [...document.querySelectorAll('.card, .desk-win')];
  if (matchMedia('(hover: hover)').matches) {
    lit.forEach((el) => el.addEventListener('pointermove', (e) => {
      const r = el.getBoundingClientRect();
      el.style.setProperty('--mx', ((e.clientX - r.left) / r.width * 100).toFixed(1) + '%');
      el.style.setProperty('--my', ((e.clientY - r.top) / r.height * 100).toFixed(1) + '%');
    }, { passive: true }));
  }

  /* ── desk window tilt (desktop pointer only, tiny) ──────────────── */
  const tiltWrap = document.getElementById('deskTilt');
  const win = document.getElementById('deskWin');
  if (tiltWrap && win && !reduce && matchMedia('(hover: hover) and (min-width: 941px)').matches) {
    let tx = 0, ty = 0, cx = 0, cy = 0, raf = 0;
    const step = () => {
      cx += (tx - cx) * 0.12; cy += (ty - cy) * 0.12;
      win.style.transform = `rotateX(${cy.toFixed(2)}deg) rotateY(${cx.toFixed(2)}deg)`;
      if (Math.abs(tx - cx) > 0.01 || Math.abs(ty - cy) > 0.01) raf = requestAnimationFrame(step); else raf = 0;
    };
    const kick = () => { if (!raf) raf = requestAnimationFrame(step); };
    tiltWrap.addEventListener('pointermove', (e) => {
      const r = tiltWrap.getBoundingClientRect();
      tx = ((e.clientX - r.left) / r.width - 0.5) * 6;
      ty = -((e.clientY - r.top) / r.height - 0.5) * 5;
      kick();
    }, { passive: true });
    tiltWrap.addEventListener('pointerleave', () => { tx = 0; ty = 0; kick(); });
  }

  /* ── seeded randomness so the illustration is the same every visit ─ */
  function rng(seed) {
    let s = seed >>> 0;
    return () => { s = (s * 1664525 + 1013904223) >>> 0; return s / 4294967296; };
  }

  /* ── aurora: a few soft blobs drifting, drawn small and blurred by CSS */
  function aurora(canvas, palette, speed) {
    if (!canvas || reduce) return;
    const ctx = canvas.getContext('2d');
    const rnd = rng(7);
    const blobs = palette.map((col, i) => ({
      col, r: 0.42 + rnd() * 0.25,
      x: rnd(), y: rnd(), a: rnd() * Math.PI * 2, s: 0.05 + rnd() * 0.06, k: 0.6 + i * 0.35,
    }));
    let size = { w: 1, h: 1 }, last = 0;
    const resize = () => { size = fit(canvas, 0.14); };
    resize();
    watch(canvas);
    const frame = (t) => {
      requestAnimationFrame(frame);
      if (!live(canvas)) return;
      if (t - last < 40) return; // 25 fps is plenty under 38px of blur
      last = t;
      const { w, h } = size;
      ctx.clearRect(0, 0, w, h);
      ctx.globalCompositeOperation = 'lighter';
      const tt = t * 0.00004 * speed;
      blobs.forEach((b) => {
        const x = (0.5 + 0.42 * Math.cos(tt * b.k + b.a)) * w;
        const y = (0.5 + 0.36 * Math.sin(tt * b.k * 0.8 + b.a * 1.7)) * h;
        const rad = b.r * Math.max(w, h);
        const g = ctx.createRadialGradient(x, y, 0, x, y, rad);
        g.addColorStop(0, b.col[0]); g.addColorStop(1, b.col[1]);
        ctx.fillStyle = g;
        ctx.fillRect(x - rad, y - rad, rad * 2, rad * 2);
      });
      ctx.globalCompositeOperation = 'source-over';
    };
    requestAnimationFrame(frame);
    addEventListener('resize', resize);
  }
  aurora(document.getElementById('aurora'), [
    ['rgba(79,231,245,.55)', 'rgba(79,231,245,0)'],
    ['rgba(154,140,255,.50)', 'rgba(154,140,255,0)'],
    ['rgba(245,166,35,.22)', 'rgba(245,166,35,0)'],
    ['rgba(38,120,220,.35)', 'rgba(38,120,220,0)'],
  ], 1);
  aurora(document.getElementById('aurora2'), [
    ['rgba(245,166,35,.30)', 'rgba(245,166,35,0)'],
    ['rgba(79,231,245,.30)', 'rgba(79,231,245,0)'],
    ['rgba(154,140,255,.28)', 'rgba(154,140,255,0)'],
  ], 0.8);

  /* ── stars: a still field of faint ticks with a slow twinkle ─────── */
  (function stars() {
    const c = document.getElementById('stars');
    if (!c) return;
    const ctx = c.getContext('2d');
    const rnd = rng(19);
    let pts = [], size = { w: 1, h: 1 };
    const seed = () => {
      size = fit(c);
      const n = Math.round((size.w * size.h) / (DPR * DPR) / 9000);
      pts = Array.from({ length: n }, () => ({ x: rnd(), y: rnd(), r: 0.5 + rnd() * 1.1, p: rnd() * Math.PI * 2, s: 0.4 + rnd() * 0.8 }));
    };
    seed();
    const draw = (t) => {
      const { w, h } = size;
      ctx.clearRect(0, 0, w, h);
      pts.forEach((p) => {
        const a = reduce ? 0.5 : 0.28 + 0.32 * (0.5 + 0.5 * Math.sin(t * 0.0006 * p.s + p.p));
        ctx.fillStyle = `rgba(210,228,255,${a.toFixed(3)})`;
        ctx.beginPath(); ctx.arc(p.x * w, p.y * h, p.r * DPR, 0, Math.PI * 2); ctx.fill();
      });
    };
    if (reduce) { draw(0); return; }
    watch(c);
    let last = 0;
    const frame = (t) => { requestAnimationFrame(frame); if (!live(c) || t - last < 66) return; last = t; draw(t); };
    requestAnimationFrame(frame);
    addEventListener('resize', () => { seed(); draw(0); });
  })();

  /* ── the desk chart: a synthetic candle series that keeps walking ─── */
  (function desk() {
    const c = document.getElementById('chart');
    if (!c) return;
    const ctx = c.getContext('2d');
    const rnd = rng(2026);
    const N = 64;
    let price = 100;
    const candles = [];
    const mk = () => {
      const o = price;
      const drift = 0.0006, vol = 0.006;
      const cl = o * (1 + drift + (rnd() - 0.5) * 2 * vol);
      const hi = Math.max(o, cl) * (1 + rnd() * vol * 0.6);
      const lo = Math.min(o, cl) * (1 - rnd() * vol * 0.6);
      price = cl;
      return { o, h: hi, l: lo, c: cl };
    };
    for (let i = 0; i < N; i++) candles.push(mk());
    let size = fit(c);
    const pxv = document.getElementById('pxv');
    const tDone = document.getElementById('tDone');
    const rows = document.getElementById('rows');
    let done = 41;
    const fmt = (v) => (v * 612.5).toFixed(2); // synthetic, so no real level is implied
    const pairs = ['ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'AVAX', 'LINK', 'DOT'];
    const addRow = () => {
      if (!rows) return;
      const won = rnd() < 0.56;
      const pct = (won ? 1 : -1) * (0.2 + rnd() * 1.3);
      const el = document.createElement('div');
      el.className = 'desk-row';
      const cell = (cls, text) => { const n = document.createElement('span'); n.className = cls; n.textContent = text; return n; };
      el.append(
        cell('s', pairs[Math.floor(rnd() * pairs.length)] + '/USDT'),
        cell('t', won ? 'closed · target' : 'closed · stop'),
        cell('p ' + (won ? 'up' : 'dn'), (pct > 0 ? '+' : '−') + Math.abs(pct).toFixed(2) + '%'),
        cell('f', 'fee 0.10%'),
      );
      rows.prepend(el);
      requestAnimationFrame(() => el.classList.add('in'));
      while (rows.children.length > 3) rows.lastElementChild.remove();
      done += 1;
      if (tDone) tDone.textContent = String(done);
    };
    addRow(); addRow(); addRow();

    let anim = 0; // 0..1 progress of the newest candle forming
    let last = 0;
    const draw = () => {
      const { w, h } = size;
      ctx.clearRect(0, 0, w, h);
      const padT = 18 * DPR, padB = 14 * DPR, padL = 10 * DPR, padR = 64 * DPR;
      const vis = candles.slice(-N);
      let lo = Infinity, hi = -Infinity;
      vis.forEach((k) => { lo = Math.min(lo, k.l); hi = Math.max(hi, k.h); });
      const span = (hi - lo) || 1;
      const Y = (v) => padT + (1 - (v - lo) / span) * (h - padT - padB);
      const cw = (w - padL - padR) / N;
      // grid
      ctx.strokeStyle = 'rgba(255,255,255,.07)'; ctx.lineWidth = 1;
      for (let i = 1; i < 4; i++) { const y = padT + (h - padT - padB) * i / 4; ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(w - padR, y); ctx.stroke(); }
      // faint area under closes
      ctx.beginPath();
      vis.forEach((k, i) => { const x = padL + (i + 0.5) * cw; i ? ctx.lineTo(x, Y(k.c)) : ctx.moveTo(x, Y(k.c)); });
      ctx.lineTo(padL + (vis.length - 0.5) * cw, h - padB); ctx.lineTo(padL + 0.5 * cw, h - padB); ctx.closePath();
      const g = ctx.createLinearGradient(0, padT, 0, h - padB);
      g.addColorStop(0, 'rgba(79,231,245,.16)'); g.addColorStop(1, 'rgba(79,231,245,0)');
      ctx.fillStyle = g; ctx.fill();
      // candles
      vis.forEach((k, i) => {
        const x = padL + (i + 0.5) * cw;
        const isLast = i === vis.length - 1;
        const o = k.o;
        const cl = isLast ? o + (k.c - o) * anim : k.c;
        const hh = isLast ? Math.max(o, cl) + (k.h - Math.max(k.o, k.c)) * anim : k.h;
        const ll = isLast ? Math.min(o, cl) - (Math.min(k.o, k.c) - k.l) * anim : k.l;
        const up = cl >= o;
        const col = up ? '74,222,155' : '255,122,147';
        ctx.strokeStyle = `rgba(${col},.9)`; ctx.lineWidth = Math.max(1, DPR);
        ctx.beginPath(); ctx.moveTo(x, Y(hh)); ctx.lineTo(x, Y(ll)); ctx.stroke();
        const bw = Math.max(2 * DPR, cw * 0.58);
        const top = Y(Math.max(o, cl)), bot = Y(Math.min(o, cl));
        ctx.fillStyle = `rgba(${col},.88)`;
        if (isLast) { ctx.shadowColor = `rgba(${col},.8)`; ctx.shadowBlur = 14 * DPR; }
        ctx.fillRect(x - bw / 2, top, bw, Math.max(1.5 * DPR, bot - top));
        ctx.shadowBlur = 0;
      });
      // last-price line
      const lastC = vis[vis.length - 1];
      const lp = lastC.o + (lastC.c - lastC.o) * anim;
      const y = Y(lp);
      ctx.setLineDash([3 * DPR, 4 * DPR]); ctx.strokeStyle = 'rgba(245,166,35,.7)';
      ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(w - padR + 6 * DPR, y); ctx.stroke(); ctx.setLineDash([]);
      ctx.fillStyle = 'rgba(245,166,35,.18)';
      ctx.fillRect(w - padR + 6 * DPR, y - 8 * DPR, padR - 12 * DPR, 16 * DPR);
      ctx.fillStyle = '#FFC65A'; ctx.font = `${10 * DPR}px "JetBrains Mono", ui-monospace, monospace`; ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
      ctx.fillText(fmt(lp), w - padR / 2 + 3 * DPR, y);
      if (pxv) { pxv.textContent = fmt(lp); pxv.className = lp >= lastC.o ? 'up' : 'dn'; }
    };
    draw();
    if (reduce) { anim = 1; draw(); return; }
    watch(c);
    let t0 = performance.now();
    const period = 2600;
    const frame = (t) => {
      requestAnimationFrame(frame);
      if (!live(c)) { t0 = t - anim * period; return; }
      if (t - last < 33) return; last = t;
      anim = Math.min(1, (t - t0) / period);
      draw();
      if (anim >= 1) {
        candles.push(mk()); if (candles.length > 400) candles.splice(0, candles.length - N);
        t0 = t; anim = 0;
        if (rnd() < 0.34) addRow();
      }
    };
    requestAnimationFrame(frame);
    addEventListener('resize', () => { size = fit(c); draw(); });
  })();

  /* ── film ───────────────────────────────────────────────────────── */
  /* The performance contract, unchanged from Dōjima because it was paid for
     once already: exactly ONE clip decoding at a time, paused the moment it
     leaves the viewport or the tab is hidden, and nothing fetched until it is
     actually wanted. A landing page that hangs the machine is worse than one
     with no film on it.
     Screen width is NOT part of that contract, and gating on it was a defect
     Dōjima already paid for (c9959c4): a phone got posters at best and, with
     the band video hidden too, empty rectangles at worst. One clip at a time is
     what makes this safe, and that holds at every width. The refusals below are
     the ones the device actually asks for — the user's own motion preference,
     save-data, and a 2g line. Note that iOS Low Power Mode and Reduce Motion
     suppress autoplay themselves; the page cannot override either, and the
     poster is what shows then. */
  (function film() {
    const films = [...document.querySelectorAll('video[data-film]')];
    if (!films.length || !('IntersectionObserver' in window)) return;
    const conn = navigator.connection || {};
    const wanted = !reduce
      && !conn.saveData
      && !/(^|-)2g$/.test(conn.effectiveType || '');
    if (!wanted) return;

    const hero = document.querySelector('.hero');
    let playing = null;
    const hush = (v) => { if (v && !v.paused) v.pause(); };

    const fo = new IntersectionObserver((es) => es.forEach((e) => {
      const v = e.target;
      if (e.isIntersecting) {
        if (!v.getAttribute('src')) { v.setAttribute('src', v.dataset.film); v.load(); }
        if (playing && playing !== v) hush(playing);
        playing = v;
        const p = v.play();
        if (p) {
          p.then(() => {
            v.classList.add('lit');
            // Only once the plate is genuinely running does the aurora step
            // back — if the clip is blocked or slow, the canvas stays.
            if (v.id === 'heroFilm' && hero) hero.classList.add('filmed');
          }).catch(() => { /* autoplay refused: the poster is already showing */ });
        }
      } else {
        hush(v);
        if (playing === v) playing = null;
      }
    }), { threshold: 0.2 });
    films.forEach((v) => fo.observe(v));

    addEventListener('visibilitychange', () => {
      if (document.hidden) films.forEach(hush);
      else if (playing) { const p = playing.play(); if (p) p.catch(() => {}); }
    });
  })();

  /* ── the record ─────────────────────────────────────────────────── */
  /* Reads a static snapshot that app.py writes from the private journal's own
     figures. The landing never calls /api/journal/trades: that makes three
     Binance calls per request and returns the account balance, and this page is
     served to anyone. If the file is missing or empty the section stays hidden
     — a landing page must never show a placeholder number where a real one
     belongs. */
  (function record() {
    const sec = document.getElementById('record');
    const cv = document.getElementById('ledger');
    if (!sec || !cv) return;

    const money = (v) => (v < 0 ? '−$' : '$') + Math.abs(v).toFixed(2);
    const pct = (v) => (v > 0 ? '+' : v < 0 ? '−' : '') + Math.abs(v).toFixed(2) + '%';

    fetch('/static/landing/ledger.json', { credentials: 'omit' })
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => {
        /* The journal card's table. Real closed rounds with the instrument
           masked, and it stays hidden unless the ledger actually carries some.
           It used to be four hand-written rows with real pair names — invented
           evidence sitting directly above the sentence "the journal is the
           product", which is the one place on this page that could not afford
           it. Empty beats plausible. Runs before the series gate below, because
           the rounds and the daily curve fail independently. */
        const jrn = document.getElementById('jrn');
        if (jrn && d && Array.isArray(d.recent) && d.recent.length) {
          const feeTxt = (v) => (Math.round(v * 1000) % 10 === 0 ? v.toFixed(2) : v.toFixed(3)) + '%';
          const frag = document.createDocumentFragment();
          d.recent.forEach((t) => {
            const row = document.createElement('div');
            row.className = 'r';
            const cell = (tag, text, cls) => {
              const el = document.createElement(tag);
              el.textContent = text;
              if (cls) el.className = cls;
              row.appendChild(el);
            };
            const res = +t.result_pct || 0;
            cell('b', t.pair || '—');
            cell('em', t.closed || '—');
            cell('span', t.hold || '—');
            /* null means the commission was taken in BNB: the USD figure comes
               back as zero, and "0.00%" under a promise that fees are shown as
               what they were would be the only false line on the page. */
            cell('span', t.fee_pct === null || t.fee_pct === undefined ? '—' : feeTxt(+t.fee_pct), 'fee');
            cell('span', pct(res), res > 0 ? 'up' : res < 0 ? 'dn' : '');
            frag.appendChild(row);
          });
          jrn.appendChild(frag);
          jrn.hidden = false;
        }

        if (!d || !d.trades || !Array.isArray(d.series) || !d.series.length) return;
        sec.hidden = false;

        const set = (id, text, cls) => {
          const el = document.getElementById(id);
          if (!el) return;
          el.textContent = text;
          if (cls) el.classList.add(cls);
        };
        const net = +d.net_usd || 0;
        const p = d.net_pct_of_capital;
        set('ledPct', p === null || p === undefined ? '—' : pct(p), net > 0 ? 'up' : net < 0 ? 'dn' : '');
        set('ledNet', money(net) + ' on ' + money(+d.capital_base_usd || 0) + ' funded');
        set('ledTrades', String(d.trades));
        set('ledWL', (d.wins || 0) + ' won · ' + (d.losses || 0) + ' lost');
        set('ledSince', (d.since || '').slice(0, 10));
        set('ledDays', (d.trading_days || 0) + ' days with a closed trade');
        set('ledFees', money(+d.fees_usd || 0));
        set('ledAs', 'as of ' + (d.as_of || '—'));
        set('ledBase', money(+d.capital_base_usd || 0));

        /* The curve, plus each day on its own lane underneath. The daily bars
           need that separate lane with its own centreline: hung off the
           cumulative zero they are invisible, because that zero sits on the
           floor of the plot — and the losing days are the entire argument, so
           losing them is a correctness bug, not a cosmetic one. */
        const s = d.series;
        let size = { w: 1, h: 1 };
        const fitLed = () => {
          const r = cv.getBoundingClientRect();
          const w = Math.max(1, Math.round(r.width * DPR));
          const h = Math.max(1, Math.round(r.height * DPR));
          if (cv.width !== w || cv.height !== h) { cv.width = w; cv.height = h; }
          size = { w, h };
        };
        const ctx = cv.getContext('2d');
        let grown = 0;

        const drawLed = (progress) => {
          const { w, h } = size;
          ctx.clearRect(0, 0, w, h);
          const padL = 8 * DPR, padR = 62 * DPR, padT = 14 * DPR;
          const barLane = 62 * DPR, gap = 16 * DPR;
          const curveH = h - padT - barLane - gap;
          const cums = s.map((x) => x.c);
          const lo = Math.min(0, ...cums), hi = Math.max(0, ...cums);
          const span = (hi - lo) || 1;
          const X = (i) => padL + (s.length === 1 ? 0.5 : i / (s.length - 1)) * (w - padL - padR);
          const Y = (v) => padT + (1 - (v - lo) / span) * curveH;
          const shown = Math.max(1, Math.round(s.length * progress));

          // zero line on the cumulative plot
          ctx.strokeStyle = 'rgba(255,255,255,.16)'; ctx.lineWidth = 1;
          ctx.beginPath(); ctx.moveTo(padL, Y(0)); ctx.lineTo(w - padR, Y(0)); ctx.stroke();

          // area under the curve
          ctx.beginPath();
          for (let i = 0; i < shown; i++) { const x = X(i), y = Y(s[i].c); i ? ctx.lineTo(x, y) : ctx.moveTo(x, y); }
          ctx.lineTo(X(shown - 1), Y(0)); ctx.lineTo(X(0), Y(0)); ctx.closePath();
          const g = ctx.createLinearGradient(0, padT, 0, padT + curveH);
          g.addColorStop(0, 'rgba(79,231,245,.20)'); g.addColorStop(1, 'rgba(79,231,245,0)');
          ctx.fillStyle = g; ctx.fill();

          // the curve
          ctx.beginPath();
          for (let i = 0; i < shown; i++) { const x = X(i), y = Y(s[i].c); i ? ctx.lineTo(x, y) : ctx.moveTo(x, y); }
          ctx.strokeStyle = '#4FE7F5'; ctx.lineWidth = 2 * DPR;
          ctx.lineJoin = 'round'; ctx.stroke();

          // endpoint + its value in the right margin
          const last = s[shown - 1];
          ctx.fillStyle = '#4FE7F5';
          ctx.beginPath(); ctx.arc(X(shown - 1), Y(last.c), 3.5 * DPR, 0, Math.PI * 2); ctx.fill();
          ctx.font = `${11 * DPR}px "JetBrains Mono", ui-monospace, monospace`;
          ctx.textAlign = 'left'; ctx.textBaseline = 'middle';
          ctx.fillStyle = '#A6F3FA';
          ctx.fillText((last.c < 0 ? '−$' : '$') + Math.abs(last.c).toFixed(2), w - padR + 9 * DPR, Y(last.c));

          // the daily lane
          const laneMid = h - barLane / 2;
          ctx.strokeStyle = 'rgba(255,255,255,.10)';
          ctx.beginPath(); ctx.moveTo(padL, laneMid); ctx.lineTo(w - padR, laneMid); ctx.stroke();
          const peak = Math.max(...s.map((x) => Math.abs(x.p))) || 1;
          const bw = Math.max(2 * DPR, Math.min(11 * DPR, (w - padL - padR) / s.length * 0.62));
          for (let i = 0; i < shown; i++) {
            const v = s[i].p;
            const hgt = (Math.abs(v) / peak) * (barLane / 2 - 5 * DPR);
            ctx.fillStyle = v >= 0 ? 'rgba(74,222,155,.85)' : 'rgba(255,122,147,.9)';
            ctx.fillRect(X(i) - bw / 2, v >= 0 ? laneMid - hgt : laneMid, bw, Math.max(1.5 * DPR, hgt));
          }
          ctx.fillStyle = 'rgba(142,152,170,1)';
          ctx.font = `${9.5 * DPR}px "JetBrains Mono", ui-monospace, monospace`;
          ctx.textAlign = 'left';
          ctx.fillText('PER DAY', w - padR + 9 * DPR, laneMid);
        };

        fitLed(); drawLed(reduce ? 1 : 0);
        addEventListener('resize', () => { fitLed(); drawLed(grown || 1); });
        if (reduce) { grown = 1; return; }

        // Draw it on once, when it is actually looked at.
        if (!('IntersectionObserver' in window)) { grown = 1; drawLed(1); return; }
        const lo2 = new IntersectionObserver((es) => es.forEach((e) => {
          if (!e.isIntersecting) return;
          lo2.unobserve(e.target);
          const t0 = performance.now(), dur = 1500;
          const step = (t) => {
            const q = Math.min(1, (t - t0) / dur);
            grown = 1 - Math.pow(1 - q, 3);
            drawLed(grown);
            if (q < 1) requestAnimationFrame(step);
          };
          requestAnimationFrame(step);
        }), { threshold: 0.35 });
        lo2.observe(cv);
      })
      .catch(() => { /* no snapshot yet — the section simply never appears */ });
  })();

  /* ── access request: a mail draft, never a submission ───────────── */
  (function access() {
    const TO = 'phil.shiny@gmail.com';
    const form = document.getElementById('accessForm');
    const link = document.getElementById('accessSend');
    if (!form || !link) return;
    form.addEventListener('submit', (e) => { e.preventDefault(); link.click(); });
    const val = (id) => { const el = document.getElementById(id); return el ? el.value.trim() : ''; };
    const build = () => {
      const name = val('f1'), email = val('f2'), cap = val('f3'), dd = val('f4');
      const subject = 'CryptoForge — access request' + (name ? ' — ' + name : '');
      const body = [
        'Name: ' + (name || '—'),
        'Email: ' + (email || '—'),
        'Capital I would deploy: ' + (cap || '—'),
        'Maximum drawdown I can hold: ' + (dd || '—'),
        '',
        'Sent from the CryptoForge access form.',
      ].join('\n');
      link.href = 'mailto:' + TO + '?subject=' + encodeURIComponent(subject) + '&body=' + encodeURIComponent(body);
    };
    form.addEventListener('input', build);
    form.addEventListener('change', build);
    build();
  })();
})();
