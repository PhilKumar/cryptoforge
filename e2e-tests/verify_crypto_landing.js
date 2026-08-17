/**
 * Verify the CryptoForge landing (static/landing/index.html + crypto.css/js)
 * against the real production CSP.
 *
 *   python3 <scratchpad>/csp_server.py 8098 <repo root>
 *   cd e2e-tests && node verify_crypto_landing.js http://localhost:8098/static/landing/index.html
 *
 * Under `style-src-elem 'self'` / `script-src-attr 'none'` an inline style,
 * script or on* handler is dropped with no console error, so "it looked fine
 * from file://" proves nothing. This loads the page from a server that sends
 * the exact header app.py sends and asserts the page is actually alive.
 */
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');

const URL = process.argv[2] || 'http://localhost:8098/static/landing/index.html';
const SHOTS = process.argv[3] || '/tmp';
const results = [];
const check = (name, pass, detail) => {
  results.push({ name, pass });
  console.log(`${pass ? '  ok  ' : ' FAIL '} ${name}${detail ? ' — ' + detail : ''}`);
};

// Walks every film band that actually has a box at the current width and reports
// what each one did when it was reached. Which bands those are is width-dependent
// — the plate's framed copy only exists on a phone — so the walk asks the layout
// rather than assuming a count. Reads `decoding` across ALL clips at each stop,
// because "one at a time" is the whole performance contract.
const bandWalk = async (pg) => {
  // Ask whether the element has a BOX, not what its own display says. A video
  // inside a display:none section still computes display:block for itself —
  // getComputedStyle does not inherit the ancestor's none — so reading the
  // video's display reports every band as shown at every width.
  const shown = await pg.evaluate(() => [...document.querySelectorAll('.filmband video')]
    .map((v, i) => (v.offsetWidth || v.offsetHeight || v.getClientRects().length ? i : -1))
    .filter((i) => i >= 0));
  const out = [];
  for (const idx of shown) {
    await pg.evaluate((i) => {
      document.documentElement.style.scrollBehavior = 'auto';
      const v = document.querySelectorAll('.filmband video')[i];
      const r = v.getBoundingClientRect();
      scrollTo(0, r.top + scrollY - (innerHeight - r.height) / 2);
    }, idx);
    await pg.waitForTimeout(2600);
    out.push(await pg.evaluate((i) => {
      const all = [...document.querySelectorAll('video[data-film]')];
      const v = document.querySelectorAll('.filmband video')[i];
      const r = v.closest('.frame').getBoundingClientRect();
      const inView = (x) => { const b = x.getBoundingClientRect();
        return b.bottom > 0 && b.top < innerHeight && (b.width || b.height); };
      const live = all.filter((x) => !x.paused);
      return { playing: !v.paused, lit: v.classList.contains('lit'), poster: !!v.poster,
               op: +getComputedStyle(v).opacity, decoding: live.length,
               // Two bands can share one phone screen — on prod the record
               // section is hidden, so the plate and the first act are separated
               // only by the tape. The observer then correctly starts the second
               // and hushes the first, so "the band I scrolled to is playing" is
               // NOT the promise. The promise is: one clip decoding, and the one
               // decoding is on screen. Reaching a band still has to WANT it,
               // which is what fetched proves.
               fetched: !!v.getAttribute('src'),
               liveInView: live.length > 0 && live.every(inView),
               ratio: +(r.width / r.height).toFixed(3),
               w: Math.round(r.width), h: Math.round(r.height) };
    }, idx));
  }
  return out;
};

(async () => {
  // ── static: nothing the CSP would silently drop ─────────────────────
  // The page is reachable two ways — as a file under /static/landing/ and as
  // the app's own "/" — so fall back to index.html when the URL names no file.
  const file = new (require('url').URL)(URL).pathname.split('/').pop() || 'index.html';
  const htmlPath = path.resolve(__dirname, '..', 'static', 'landing', file);
  const html = fs.readFileSync(htmlPath, 'utf8');
  check('no inline <style>/<script>', !/<style[\s>]/i.test(html) && !/<script(?![^>]*\bsrc=)/i.test(html));
  check('no inline event handlers', !/\son[a-z]+\s*=/i.test(html));
  check('no javascript: urls', !/javascript:/i.test(html));
  check('body carries the landing marker', /<body[^>]*data-landing="crypto"/.test(html));

  const browser = await chromium.launch();

  // ── desktop ─────────────────────────────────────────────────────────
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: 2 });
  const errors = [];
  // Before the first snapshot exists, ledger.json legitimately 404s and the
  // page handles it by hiding the record section. That one 404 is expected;
  // every other console error still fails the run.
  const expected = (t) => /ledger\.json/.test(t) || (/404/.test(t) && /Failed to load resource/.test(t) && ledgerMissing);
  let ledgerMissing = false;
  page.on('console', (m) => { if (m.type() === 'error' && !expected(m.text())) errors.push(m.text()); });
  page.on('pageerror', (e) => errors.push('pageerror: ' + e.message));
  page.on('requestfailed', (r) => { if (!/fonts\.(googleapis|gstatic)\.com/.test(r.url())) errors.push('requestfailed: ' + r.url()); });

  // Ask the TARGET whether it has a snapshot, not the local disk. Checking
  // local files here meant a run against production tolerated nothing (the
  // fixture existed here) and reported prod's expected 404 as a page error.
  ledgerMissing = await page.request.get(new (require('url').URL)('/static/landing/ledger.json', URL).href)
    .then((r) => !r.ok()).catch(() => true);
  await page.goto(URL, { waitUntil: 'load' });
  await page.waitForTimeout(2600);

  const csp = errors.filter((e) => /Content Security Policy|Refused to/i.test(e));
  check('no CSP violations', csp.length === 0, csp.slice(0, 3).join(' | '));
  check('no page errors', errors.length === 0, errors.slice(0, 3).join(' | '));

  const d = await page.evaluate(() => {
    const cs = getComputedStyle(document.body);
    const h1 = document.querySelector('h1');
    const first = h1.querySelector('.rise > span');
    return {
      bodyBg: cs.backgroundColor,
      h1Size: parseFloat(getComputedStyle(h1).fontSize),
      h1Font: getComputedStyle(h1).fontFamily,
      risen: getComputedStyle(first).transform === 'none' || getComputedStyle(first).transform === 'matrix(1, 0, 0, 1, 0, 0)',
      rows: document.querySelectorAll('#rows .desk-row').length,
      rowsLit: document.querySelectorAll('#rows .desk-row.in').length,
      tape: document.getElementById('tapeRun').children.length,
      pxv: document.getElementById('pxv').textContent,
      done: +document.getElementById('tDone').textContent,
      disp: document.fonts ? document.fonts.check('600 20px Sora') : null,
      signIns: [...document.querySelectorAll('a[href="/app"]')].length,
      offsite: [...document.querySelectorAll('a[href^="http"]')].map((a) => a.href),
    };
  });
  check('external stylesheet applied', d.bodyBg === 'rgb(5, 6, 11)', `bg=${d.bodyBg}`);
  check('display type sized', d.h1Size > 55, `${d.h1Size}px · ${d.h1Font.split(',')[0]}`);
  check('headline has risen', d.risen);
  check('external script ran (tape built)', d.tape === 26, `tape items=${d.tape}`);
  check('desk price is printing', /^\d/.test(d.pxv), `last=${d.pxv}`);
  check('desk rows written and lit', d.rows === 3 && d.rowsLit === 3, `${d.rowsLit}/${d.rows}`);
  check('every sign-in goes to /app', d.signIns >= 3, `${d.signIns} links`);
  check('only philforge.in is linked off-site', d.offsite.every((u) => /^https:\/\/philforge\.in\//.test(u)), d.offsite.join(', '));
  // "This one is very dull" was a contrast complaint, so measure it rather
  // than eyeball it: every run of visible text must clear WCAG AA against the
  // ground it is actually painted on (4.5:1 body, 3:1 for >=18.66px bold).
  const dim = await page.evaluate(() => {
    const lum = (c) => {
      const [r, g, b] = c.map((v) => { v /= 255; return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4); });
      return 0.2126 * r + 0.7152 * g + 0.0722 * b;
    };
    const parse = (s) => { const m = s.match(/rgba?\((\d+), ?(\d+), ?(\d+)(?:, ?([\d.]+))?/); return m ? { c: [+m[1], +m[2], +m[3]], a: m[4] === undefined ? 1 : +m[4] } : null; };
    const over = (fg, bg) => fg.c.map((v, i) => v * fg.a + bg[i] * (1 - fg.a));
    // Walk up for the first opaque-enough background colour. A gradient has no
    // single colour to measure against, so anything painted on one is reported
    // separately rather than scored as if it sat on the void — otherwise the
    // amber gradient button reads as 1.04:1 dark-on-dark, which it plainly is not.
    const ground = (el) => {
      let node = el, acc = null;
      while (node && node !== document.documentElement) {
        const cs = getComputedStyle(node);
        if (/gradient/.test(cs.backgroundImage)) return null;
        const bg = parse(cs.backgroundColor);
        if (bg && bg.a > 0 && !acc) acc = over(bg, [5, 6, 11]);
        node = node.parentElement;
      }
      return acc || [5, 6, 11];
    };
    const out = [];
    let onGradient = 0;
    for (const el of document.querySelectorAll('p,span,h1,h2,h3,h4,a,div,li,label,small,b,em')) {
      const text = [...el.childNodes].some((n) => n.nodeType === 3 && n.textContent.trim());
      if (!text) continue;
      const cs = getComputedStyle(el);
      if (cs.visibility === 'hidden' || cs.display === 'none' || cs.webkitTextFillColor === 'rgba(0, 0, 0, 0)') continue;
      const fg = parse(cs.color);
      if (!fg || fg.a === 0) continue;
      const size = parseFloat(cs.fontSize);
      const large = size >= 24 || (size >= 18.66 && +cs.fontWeight >= 700);
      const bg = ground(el);
      if (!bg) { onGradient++; continue; }
      const l1 = lum(over(fg, bg)), l2 = lum(bg);
      const ratio = (Math.max(l1, l2) + 0.05) / (Math.min(l1, l2) + 0.05);
      if (ratio < (large ? 3 : 4.5)) out.push(`${el.tagName.toLowerCase()}.${(el.className || '-').toString().split(' ')[0]} ${size}px ${cs.color} = ${ratio.toFixed(2)}:1`);
    }
    return { fails: [...new Set(out)].slice(0, 6), onGradient };
  });
  check('every text run clears WCAG AA', dim.fails.length === 0,
    dim.fails.join(' | ') || `all measurable text >= 4.5:1 (${dim.onGradient} runs sit on a gradient and are not scorable)`);
  if (d.disp !== null) console.log(`       (Sora loaded: ${d.disp} — falls back to the system stack offline)`);

  // ── the record: it must be fed by the snapshot or not appear at all ──
  // A landing page showing a placeholder where a real number belongs is worse
  // than showing nothing, and the section is the one place on this page
  // carrying figures a reader could act on.
  const rec = await page.evaluate(async () => {
    const res = await fetch('/static/landing/ledger.json', { credentials: 'omit' }).catch(() => null);
    const has = !!(res && res.ok);
    const led = has ? await res.json().catch(() => null) : null;
    const sec = document.getElementById('record');
    const cv = document.getElementById('ledger');
    if (cv) cv.scrollIntoView({ behavior: 'instant', block: 'center' });
    await new Promise((r) => setTimeout(r, 2400));
    let ink = 0;
    if (cv && cv.width > 1) {
      const { data } = cv.getContext('2d').getImageData(0, 0, cv.width, cv.height);
      for (let i = 3; i < data.length; i += 4 * 53) if (data[i] > 10) ink++;
    }
    const txt = (id) => (document.getElementById(id) || {}).textContent || '';
    return {
      has, hidden: sec ? sec.hidden : null, ink,
      w: cv ? cv.width : 0,
      pct: txt('ledPct'), trades: txt('ledTrades'), base: txt('ledBase'), asOf: txt('ledAs'),
      annualised: led ? led.annualised : null,
      netOfFees: led ? led.net_of_fees : null,
      source: led ? led.source : '',
      keys: led ? Object.keys(led) : [],
      down: led && led.series ? led.series.filter((x) => x.p < 0).length : 0,
    };
  });
  if (!rec.has) {
    check('record section hides with no snapshot', rec.hidden === true, 'ledger.json absent, section hidden');
  } else {
    check('record section appears with a snapshot', rec.hidden === false);
    check('record numbers filled in', /%$/.test(rec.pct) && /^\d+$/.test(rec.trades) && /^\$/.test(rec.base),
      `${rec.pct} · ${rec.trades} trades · base ${rec.base}`);
    check('record is stamped with a date', /as of \d{4}-\d{2}-\d{2}/.test(rec.asOf), rec.asOf);
    check('ledger curve painted', rec.w > 1 && rec.ink > 200, `${rec.w}px wide, ${rec.ink} inked samples`);
    // The claims the page makes about its own number have to be true in the data.
    check('snapshot is net of fees and not annualised', rec.netOfFees === true && rec.annualised === false,
      `net_of_fees=${rec.netOfFees} annualised=${rec.annualised}`);
    check('snapshot says it is live fills only', /live fills/i.test(rec.source) || /TEST FIXTURE/.test(rec.source), rec.source);
    // The public file must not leak the account. These keys exist in the
    // private journal summary and must never reach a page served to anyone.
    const leaked = ['account_value_usd', 'account_start_usd', 'account_roi_pct', 'invested_usd',
      'open_invested_usd', 'by_coin', 'trades_detail'].filter((k) => rec.keys.includes(k));
    check('snapshot leaks no account figures', leaked.length === 0, leaked.join(', ') || rec.keys.length + ' public keys, none of them balances');
    // This used to assert `rec.down > 0` — that losing days EXIST. That is a
    // property of Phil's trading, not of the page, and it went red the moment
    // the real snapshot replaced the fixture (48 closed rounds, none of them a
    // loss, because the engine has no path that closes one). What the page owes
    // is that each day is drawn on the side its sign says, which is a promise
    // the code can actually keep — and which was being broken.
    const lane = await page.evaluate(() => {
      const cv = document.getElementById('ledger');
      const ctx = cv.getContext('2d');
      const DPR = Math.min(2, devicePixelRatio || 1);
      const mid = Math.round(cv.height - (62 * DPR) / 2);
      const strip = (y0) => ctx.getImageData(0, y0, cv.width, 29).data;
      const tally = (d) => { let g = 0, r = 0;
        for (let i = 0; i < d.length; i += 4) {
          if (d[i + 3] < 40) continue;
          if (d[i + 1] > 120 && d[i + 1] > d[i] + 40 && d[i + 1] > d[i + 2] + 25) g++;
          else if (d[i] > 150 && d[i] > d[i + 1] + 40) r++;
        }
        return { g, r }; };
      return { above: tally(strip(mid - 30)), below: tally(strip(mid + 2)) };
    });
    // A winning day below the centreline reads as a loss, and vice versa. The
    // min-height clamp was applied to the bar's height but not its origin, so
    // every day too small to draw — most of them, against a $1.09 peak — grew
    // DOWNWARD from the line. 90 green pixels sat on the losing side.
    check('each day is drawn on the side its sign says',
      lane.below.g === 0 && lane.above.r === 0,
      `${lane.below.g} winning px below the line, ${lane.above.r} losing px above it`);
    // And the lane must actually be carrying the days it was given.
    check('the daily lane is drawn', lane.above.g + lane.below.r > 0,
      `${lane.above.g} up px, ${lane.below.r} down px (${rec.down} down days in the series)`);
  }

  // canvases actually painted something
  const paint = await page.evaluate(() => ['aurora', 'stars', 'chart'].map((id) => {
    const c = document.getElementById(id);
    const ctx = c.getContext('2d');
    const { data } = ctx.getImageData(0, 0, c.width, c.height);
    let inked = 0;
    for (let i = 3; i < data.length; i += 4 * 97) if (data[i] > 8) inked++;
    return { id, w: c.width, h: c.height, inked };
  }));
  const blank = paint.filter((p) => p.w < 2 || p.inked === 0);
  check('all three canvases painted', blank.length === 0, paint.map((p) => `${p.id} ${p.w}x${p.h}`).join(', '));

  // dpr-2 stability: the self-doubling bug grows the backing store every frame
  const sample = () => page.evaluate(() => [...document.querySelectorAll('canvas')].map((c) => ({ id: c.id, w: c.width, h: c.height })));
  const s1 = await sample();
  await page.waitForTimeout(1800);
  const s2 = await sample();
  const grew = s2.filter((c, i) => c.w !== s1[i].w || c.h !== s1[i].h);
  check('canvases stable at dpr 2', grew.length === 0, grew.length ? 'CHANGED: ' + grew.map((c) => c.id).join(',') : s2.map((c) => `${c.id} ${c.w}x${c.h}`).join(', '));

  // ── the doors and the other desk ────────────────────────────────────
  const links = await page.evaluate(() => {
    const all = [...document.querySelectorAll('a[href]')];
    const hrefs = all.map((a) => a.getAttribute('href'));
    const label = (sub) => all.filter((a) => a.textContent.trim().toLowerCase().includes(sub)).length;
    return {
      viewer: label('viewer sign-in'),
      dojima: hrefs.filter((h) => h === 'https://philforge.in/').length,
      eqDesk: hrefs.filter((h) => h === 'https://philforge.in/app').length,
      // The equities side must be NAMED, wherever it points. This used to
      // demand the literal /equities URL, which the page had already dropped on
      // purpose — an orphaned story page — so the check was red while claiming
      // to describe the page. Assert the promise, not one spelling of it.
      eqNamed: all.filter((a) => /equit/i.test(a.textContent)).length,
      relOnExternal: all.filter((a) => /^https?:/.test(a.getAttribute('href')))
        .every((a) => (a.getAttribute('rel') || '').includes('noopener')),
      footerCols: document.querySelectorAll('footer .col').length,
      dead: hrefs.filter((h) => !h || h === '#'),
    };
  });
  check('viewer sign-in is offered', links.viewer >= 2, `${links.viewer} viewer links`);
  check('the equities side is linked', links.dojima >= 1 && links.eqNamed >= 2 && links.eqDesk >= 1,
    `Dōjima ${links.dojima} · named "equit…" ${links.eqNamed} · desk ${links.eqDesk}`);
  check('every external link is rel=noopener', links.relOnExternal);
  check('footer carries the three link columns', links.footerCols === 3, `${links.footerCols} columns`);
  check('no placeholder hrefs', links.dead.length === 0, links.dead.join(', ') || 'every href names a real target');

  // ── film: the performance contract is the assertion ─────────────────
  // Real footage, so the failure that matters is not "is it pretty" but "does
  // it decode more than one clip at a time, keep running off-screen, or ship
  // bytes to a phone". Each of those hung Phil's machine once already.
  // Back to the top FIRST. The record check above scrolls the ledger into view,
  // which correctly pauses the hero clip — reading it afterwards reports
  // playing=false and blames the page for the contract working. Instant, not
  // smooth: html{scroll-behavior:smooth} makes scrollTo animate, and a check a
  // moment later still sees mid-page.
  await page.evaluate(() => { document.documentElement.style.scrollBehavior = 'auto'; scrollTo(0, 0); });
  await page.waitForTimeout(1200);
  const filmHero = await page.evaluate(async () => {
    const v = document.getElementById('heroFilm');
    if (!v) return null;
    for (let i = 0; i < 40 && !v.getAttribute('src'); i++) await new Promise((r) => setTimeout(r, 100));
    await new Promise((r) => setTimeout(r, 1400));
    return { src: !!v.getAttribute('src'), playing: !v.paused, lit: v.classList.contains('lit'),
             poster: !!v.poster, filmed: document.querySelector('.hero').classList.contains('filmed'),
             aurora: getComputedStyle(document.getElementById('aurora')).opacity };
  });
  if (filmHero) {
    check('hero plate loads and plays', filmHero.src && filmHero.playing && filmHero.lit,
      `src=${filmHero.src} playing=${filmHero.playing} lit=${filmHero.lit}`);
    check('hero plate has a poster', filmHero.poster);
    // Two ambient layers at once is mush, and the canvas only stands down once
    // the clip is genuinely running — a blocked clip must leave the aurora up.
    check('aurora stands down for the film', filmHero.filmed && +filmHero.aurora === 0,
      `filmed=${filmHero.filmed} aurora opacity=${filmHero.aurora}`);
  }

  // The plate is in the markup twice: the hero backdrop, and a framed copy that
  // is now retired at every width. Exactly one may have a box, and the one
  // without a box must never be fetched.
  const twins = await page.evaluate(() => {
    const p = [...document.querySelectorAll('video[data-film*="00_plate"]')];
    const boxed = (v) => !!(v.offsetWidth || v.offsetHeight || v.getClientRects().length);
    return { total: p.length, boxed: p.filter(boxed).length,
             hiddenFetched: p.filter((v) => !boxed(v) && v.getAttribute('src')).length };
  });
  check('exactly one plate has a box', twins.total === 2 && twins.boxed === 1 && twins.hiddenFetched === 0,
    `${twins.boxed}/${twins.total} boxed, ${twins.hiddenFetched} hidden-but-fetched`);

  const bands = await bandWalk(page);
  // Not a hardcoded count — the arc grows as Phil generates shots, and a fixed
  // number here just goes stale and starts failing on good work. The promise is
  // that every band in the markup actually has a box and runs.
  const bandsInMarkup = await page.evaluate(() =>
    document.querySelectorAll('.filmband:not(.bandphone) video').length);
  check('every film band in the markup is shown', bands.length === bandsInMarkup && bands.length >= 2,
    `${bands.length} shown of ${bandsInMarkup} in the markup`);
  bands.forEach((st, i) => check(`band ${i + 1} plays, alone, at 16:9`,
    st.fetched && st.poster && st.decoding === 1 && st.liveInView
      && Math.abs(st.ratio - 16 / 9) < 0.02,
    `fetched=${st.fetched} decoding=${st.decoding} on-screen=${st.liveInView} ${st.w}px @ ${st.ratio}`));

  await page.screenshot({ path: `${SHOTS}/crypto_hero_1440.png` });

  // Legacy bookmarks: the terminal used to live at "/", so /#cascade and its
  // eight siblings must be handed to /app rather than landing on marketing.
  // Deleting the old landing.js silently broke this once.
  // It must be a real navigation, not a hash poke: the rescue runs once at
  // boot, so assigning location.hash on an already-open page proves nothing.
  const rescue = [];
  for (const tab of ['cascade', 'market', 'journal']) {
    // Step off the page first: a goto that changes only the fragment is a
    // same-document navigation, so the document never reloads and the boot
    // code under test never runs. This check quietly passed nothing until
    // the about:blank hop was added.
    await page.goto('about:blank');
    await page.goto(URL + '#' + tab, { waitUntil: 'load' });
    await page.waitForTimeout(500);
    rescue.push({ tab, to: await page.evaluate(() => location.pathname + location.hash) });
  }
  const badHash = rescue.filter((r) => !r.to.endsWith('/app#' + r.tab));
  check('legacy /#tab bookmarks reach /app', badHash.length === 0, rescue.map((r) => `#${r.tab}→${r.to}`).join(', '));
  await page.goto(URL, { waitUntil: 'load' });
  await page.waitForTimeout(800);

  // reveals + count-up on scroll
  await page.evaluate(() => document.getElementById('method').scrollIntoView({ behavior: 'instant', block: 'start' }));
  await page.waitForTimeout(1500);
  await page.evaluate(() => document.querySelector('.stripe').scrollIntoView({ behavior: 'instant', block: 'center' }));
  await page.waitForTimeout(2200);
  const sc = await page.evaluate(() => ({
    revealed: document.querySelectorAll('.rv.in').length,
    total: document.querySelectorAll('.rv').length,
    counted: document.querySelector('[data-count]').textContent,
    stuck: document.getElementById('nav').classList.contains('stuck'),
  }));
  check('reveal observer fired', sc.revealed > 0, `${sc.revealed}/${sc.total}`);
  check('count-up reached 100%', sc.counted === '100%', sc.counted);
  check('nav goes to glass on scroll', sc.stuck);
  await page.screenshot({ path: `${SHOTS}/crypto_method_1440.png` });

  await page.evaluate(() => document.getElementById('desk').scrollIntoView({ behavior: 'instant', block: 'start' }));
  await page.waitForTimeout(1600);
  await page.screenshot({ path: `${SHOTS}/crypto_desk_1440.png` });
  await page.evaluate(() => document.getElementById('honesty').scrollIntoView({ behavior: 'instant', block: 'start' }));
  await page.waitForTimeout(1600);
  await page.screenshot({ path: `${SHOTS}/crypto_honesty_1440.png` });

  // access form → mailto
  await page.evaluate(() => document.getElementById('access').scrollIntoView({ behavior: 'instant', block: 'start' }));
  await page.waitForTimeout(1200);
  await page.fill('#f1', 'Test Person');
  await page.fill('#f2', 'test@firm.com');
  await page.selectOption('#f3', { index: 2 });
  await page.selectOption('#f4', { index: 1 });
  await page.waitForTimeout(200);
  const href = await page.getAttribute('#accessSend', 'href');
  const decoded = decodeURIComponent(href || '');
  check('access link is a mailto', /^mailto:[^?]+@/.test(href || ''), (href || '').slice(0, 40));
  check('mailto carries all four answers', decoded.includes('Test Person') && decoded.includes('test@firm.com') && /Capital I would deploy: \$/.test(decoded) && /Maximum drawdown I can hold: Up/.test(decoded));
  await page.screenshot({ path: `${SHOTS}/crypto_access_1440.png` });
  await page.close();

  // ── mobile ──────────────────────────────────────────────────────────
  const m = await browser.newPage({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true, deviceScaleFactor: 3 });
  await m.goto(URL, { waitUntil: 'load' });
  await m.waitForTimeout(2200);
  const mob = await m.evaluate(() => ({
    scrollW: document.documentElement.scrollWidth,
    inner: innerWidth,
    navHidden: getComputedStyle(document.getElementById('navlinks')).visibility === 'hidden',
    toggleShown: getComputedStyle(document.getElementById('navToggle')).display !== 'none',
    tilt: document.getElementById('deskWin').style.transform || '',
    films: (() => { const v = [...document.querySelectorAll('video[data-film]')];
      const hero = document.getElementById('heroFilm');
      const boxed = (x) => !!(x.offsetWidth || x.offsetHeight || x.getClientRects().length);
      return { total: v.length, fetched: v.filter((x) => x.getAttribute('src')).length,
               posters: v.filter((x) => x.poster).length,
               heroBoxed: boxed(hero), heroLit: hero.classList.contains('lit'),
               heroPlaying: !hero.paused, heroOp: +getComputedStyle(hero).opacity,
               filmed: document.querySelector('.hero').classList.contains('filmed'),
               // The framed phone copy is retired: it must have no box anywhere,
               // and therefore must never cost a byte.
               strayBoxed: v.filter((x) => x.closest('.bandphone') && boxed(x)).length,
               strayFetched: v.filter((x) => x.closest('.bandphone') && x.getAttribute('src')).length }; })(),
    h1: parseFloat(getComputedStyle(document.querySelector('h1')).fontSize),
    deskW: Math.round(document.getElementById('deskWin').getBoundingClientRect().width),
    stacked: getComputedStyle(document.querySelector('.desk-body')).gridTemplateColumns.split(' ').length === 1,
  }));
  check('mobile viewport honoured', mob.inner <= 400, `innerWidth=${mob.inner}`);
  check('no horizontal overflow', mob.scrollW <= mob.inner + 1, `scrollW=${mob.scrollW}`);
  check('nav collapses into a menu button', mob.navHidden && mob.toggleShown,
    `links hidden=${mob.navHidden} button shown=${mob.toggleShown}`);
  check('headline scaled down', mob.h1 < 52, `${mob.h1}px`);
  check('desk window fits and stacks', mob.deskW <= mob.inner && mob.stacked, `${mob.deskW}px wide, stacked=${mob.stacked}`);
  check('no tilt on touch', mob.tilt === '');
  // A phone gets the SAME hero as a wide screen: the plate behind the copy. Two
  // earlier versions of this block asserted the opposite and each matched a bug
  // Phil then reported — first "no video bytes on a phone" (his film was
  // invisible), then "no hero backdrop on a phone" (his plate was an isolated
  // picture card). The page's promise is structural: one hero, one treatment.
  check('every clip carries a poster', mob.films.posters === mob.films.total,
    `${mob.films.posters}/${mob.films.total} posters`);
  check('hero plate is the backdrop on a phone too',
    mob.films.heroBoxed && mob.films.heroPlaying && mob.films.heroLit
      && mob.films.heroOp > 0.9 && mob.films.filmed,
    `boxed=${mob.films.heroBoxed} playing=${mob.films.heroPlaying} opacity=${mob.films.heroOp} filmed=${mob.films.filmed}`);
  check('no isolated picture card anywhere',
    mob.films.strayBoxed === 0 && mob.films.strayFetched === 0,
    `${mob.films.strayBoxed} boxed, ${mob.films.strayFetched} fetched`);
  // Only the hero is in view at load, so only its clip may have been wanted.
  check('phone fetches only the hero before it scrolls', mob.films.fetched === 1,
    `${mob.films.fetched}/${mob.films.total} fetched at scroll 0`);
  // The two act bands stay framed pictures — those ARE photographs of a moment.
  const mBands = await bandWalk(m);
  // Structural parity is the real assertion: a phone must get the same film as a
  // wide screen, not a subset. Comparing against the desktop walk catches a band
  // that silently drops out below a breakpoint — which is exactly the bug class
  // that started all of this.
  check('phone shows every band the desktop shows', mBands.length === bands.length,
    `${mBands.length} on the phone vs ${bands.length} on the desktop`);
  mBands.forEach((st, i) => check(`act band ${i + 1} plays on a phone, alone and visible`,
    st.fetched && st.op > 0.9 && st.decoding === 1 && st.liveInView
      && st.w <= mob.inner && st.h > 100 && Math.abs(st.ratio - 16 / 9) < 0.02,
    `fetched=${st.fetched} opacity=${st.op} decoding=${st.decoding} on-screen=${st.liveInView} ${st.w}×${st.h}`));
  // The poster must stand in when the clip is refused, or the frame is a hole.
  // Reduced motion is the refusal the page can actually be tested against.
  const rm = await browser.newPage({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true, deviceScaleFactor: 3, reducedMotion: 'reduce' });
  await rm.goto(URL, { waitUntil: 'load' });
  await rm.waitForTimeout(1800);
  const still = await rm.evaluate(async () => {
    const v = document.querySelector('.filmband video');
    v.scrollIntoView({ block: 'center' });
    await new Promise((r) => setTimeout(r, 1200));
    return { fetched: [...document.querySelectorAll('video[data-film]')].filter((x) => x.getAttribute('src')).length,
             op: +getComputedStyle(v).opacity, poster: !!v.poster };
  });
  await rm.close();
  check('reduced motion keeps the still, fetches nothing', still.fetched === 0 && still.poster && still.op > 0.9,
    `fetched=${still.fetched} poster=${still.poster} opacity=${still.op}`);

  // ── the mobile menu ─────────────────────────────────────────────────
  // This page used to simply hide its nav links under 720px, which left a phone
  // with no way to reach a single section. The menu is the only route now, so
  // every part of it is asserted: it opens, it holds every link at a thumb-sized
  // target, it fits the screen, and it closes the way people expect.
  const menu = await m.evaluate(async () => {
    const btn = document.getElementById('navToggle');
    const panel = document.getElementById('navlinks');
    if (!btn || !panel) return null;
    const wait = (ms) => new Promise((r) => setTimeout(r, ms));
    btn.click(); await wait(450);
    const links = [...panel.querySelectorAll('a')];
    const open = {
      vis: getComputedStyle(panel).visibility,
      expanded: btn.getAttribute('aria-expanded'),
      locked: getComputedStyle(document.documentElement).overflow === 'hidden',
      count: links.length,
      allShown: links.every((a) => a.getBoundingClientRect().width > 0),
      minTap: Math.min(...links.map((a) => Math.round(a.getBoundingClientRect().height))),
      fits: panel.getBoundingClientRect().bottom <= innerHeight + 1,
      hasViewer: links.some((a) => /viewer/i.test(a.textContent)),
    };
    panel.querySelector('a[href^="#"]').click(); await wait(450);
    const afterLink = { expanded: btn.getAttribute('aria-expanded'),
                        locked: getComputedStyle(document.documentElement).overflow === 'hidden' };
    btn.click(); await wait(300);
    dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
    await wait(300);
    return { open, afterLink, afterEsc: btn.getAttribute('aria-expanded') };
  });
  if (menu) {
    check('menu opens with every link reachable',
      menu.open.vis === 'visible' && menu.open.expanded === 'true' && menu.open.count === 7
      && menu.open.allShown && menu.open.hasViewer,
      `${menu.open.count} links, viewer=${menu.open.hasViewer}`);
    check('menu targets are thumb-sized and on screen', menu.open.minTap >= 44 && menu.open.fits,
      `smallest ${menu.open.minTap}px tall, fits=${menu.open.fits}`);
    check('menu locks the page behind it', menu.open.locked);
    check('menu closes on a link, and unlocks', menu.afterLink.expanded === 'false' && !menu.afterLink.locked);
    check('menu closes on Escape', menu.afterEsc === 'false');
  }
  await m.screenshot({ path: `${SHOTS}/crypto_hero_390.png` });
  await m.screenshot({ path: `${SHOTS}/crypto_full_390.png`, fullPage: true });
  await m.close();

  await browser.close();
  const failed = results.filter((r) => !r.pass);
  console.log(`\n${results.length - failed.length}/${results.length} checks passed`);
  process.exit(failed.length ? 1 : 0);
})();
