/**
 * Verify the built Dōjima landing page against the real production CSP.
 *
 * The failure this guards against is silent: under `style-src-elem 'self'` and
 * `script-src-attr 'none'` an inline <style>/<script>/onclick is dropped with no
 * console error and no visual clue beyond an unstyled page, so "it looked fine
 * locally" proves nothing. This loads the built page from a server sending the
 * exact header app.py sends and asserts the page is actually alive.
 *
 *   node verify_dojima_dist.js http://localhost:8096
 */
const { chromium } = require('playwright');

const BASE = process.argv[2] || 'http://localhost:8096';
const results = [];
const check = (name, pass, detail) => {
  results.push({ name, pass, detail });
  console.log(`${pass ? '  ok  ' : ' FAIL '} ${name}${detail ? ' — ' + detail : ''}`);
};

(async () => {
  const browser = await chromium.launch();

  // ── desktop ────────────────────────────────────────────────────────────
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  const errors = [];
  page.on('console', (m) => { if (m.type() === 'error') errors.push(m.text()); });
  page.on('pageerror', (e) => errors.push('pageerror: ' + e.message));
  page.on('requestfailed', (r) => errors.push('requestfailed: ' + r.url()));

  await page.goto(`${BASE}/index.html`, { waitUntil: 'load' });
  await page.waitForTimeout(2500);

  const csp = errors.filter((e) => /Content Security Policy|Refused to/i.test(e));
  check('no CSP violations', csp.length === 0, csp.slice(0, 3).join(' | '));
  check('no page errors', errors.length === 0, errors.slice(0, 3).join(' | '));

  const d = await page.evaluate(() => {
    const body = getComputedStyle(document.body);
    const h1 = document.querySelector('h1');
    return {
      sheets: document.styleSheets.length,
      bodyBg: body.backgroundColor,
      h1Size: parseFloat(getComputedStyle(h1).fontSize),
      h1Serif: /Hoefler|Georgia|serif/i.test(getComputedStyle(h1).fontFamily),
      tape: document.getElementById('tapeRun').children.length,
      videos: document.querySelectorAll('video[data-film]').length,
      revealed: document.querySelectorAll('.rv.in').length,
      counted: document.querySelector('[data-count]')?.textContent || '',
    };
  });

  // The stylesheet is the whole point: if style-src-elem had dropped it, the
  // body would be transparent and the h1 would sit at the UA default 32px.
  check('external stylesheet applied', d.sheets >= 1 && d.bodyBg === 'rgb(6, 7, 10)', `sheets=${d.sheets} bg=${d.bodyBg}`);
  check('display type styled', d.h1Serif && d.h1Size > 40, `${d.h1Size}px serif=${d.h1Serif}`);
  // The script is the other half: the tape is built in JS from an array.
  check('external script ran', d.tape === 24, `tape items=${d.tape}`);
  // All five shots: hero port, merchant, chalk, candle, bridge.
  check('five films present', d.videos === 5, `videos=${d.videos}`);

  // ── the hero film, checked while the hero is still on screen ───────────
  const film = await page.evaluate(async () => {
    const v = document.getElementById('heroFilm');
    for (let i = 0; i < 40 && !v.getAttribute('src'); i++) await new Promise((r) => setTimeout(r, 100));
    await new Promise((r) => setTimeout(r, 1200));
    return { src: !!v.getAttribute('src'), playing: !v.paused, lit: v.classList.contains('lit'), t: +v.currentTime.toFixed(2) };
  });
  check('hero film loaded + playing', film.src && film.playing, `src=${film.src} playing=${film.playing} t=${film.t}`);
  check('hero film faded in', film.lit, `lit=${film.lit}`);

  // Reveals and count-ups are scroll-triggered, so nothing should have fired at
  // the top — scroll to the proof band and let the observers do their work.
  await page.evaluate(() => document.getElementById('proof').scrollIntoView({ behavior: 'instant', block: 'start' }));
  await page.waitForTimeout(1200);
  const revealed = await page.evaluate(() => ({
    n: document.querySelectorAll('.rv.in').length,
    total: document.querySelectorAll('.rv').length,
  }));
  check('reveal observer fired', revealed.n > 0, `${revealed.n}/${revealed.total} revealed`);

  // The equity chart and its headline figure have their own observer at 40%
  // visibility, so the canvas itself has to be centred before they run.
  await page.evaluate(() => document.getElementById('eq').scrollIntoView({ behavior: 'instant', block: 'center' }));
  await page.waitForTimeout(2600);
  const chart = await page.evaluate(() => {
    const c = document.getElementById('eq');
    const r = c.getBoundingClientRect();
    return {
      bigno: document.getElementById('bigno').textContent,
      w: c.width, h: c.height,
      expectW: Math.round(r.width * devicePixelRatio),
      expectH: Math.round(r.height * devicePixelRatio),
    };
  });
  check('count-up ran', /[1-9]/.test(chart.bigno), `headline stat="${chart.bigno}"`);
  // The canvas-doubling bug that once painted a white box grows the backing
  // store every frame, so the assertion is that it still matches css px x dpr.
  check('equity canvas sized sanely', chart.w === chart.expectW && chart.h === chart.expectH,
    `${chart.w}x${chart.h} vs expected ${chart.expectW}x${chart.expectH}`);

  // ── the access form now carries the answers ────────────────────────────
  await page.fill('#f1', 'Test Person');
  await page.fill('#f2', 'test@firm.com');
  await page.selectOption('#f3', { index: 2 });
  await page.selectOption('#f4', { index: 1 });
  await page.waitForTimeout(200);
  const href = await page.getAttribute('#accessSend', 'href');
  const decoded = decodeURIComponent(href || '');
  check('access link is a mailto', /^mailto:[^?]+@/.test(href || ''), (href || '').slice(0, 40));
  check('mailto carries name + email', decoded.includes('Test Person') && decoded.includes('test@firm.com'));
  check('mailto carries capital + drawdown', /Capital I would deploy: .+/.test(decoded) && /Maximum drawdown I can hold: .+/.test(decoded));
  const claims = await page.evaluate(() => document.body.innerText.includes('Received — we will write back'));
  check('no false "Received" claim', !claims);

  // The way in for someone already approved. Accounts are opened by hand in the
  // admin console, so this must NOT read as a sign-up: assert the door AND the
  // sentence that says there is no self-serve form behind it.
  // Phil, 2026-08-17: the sign-in asks WHICH desk — equities or crypto — and
  // each answer is that desk's own door. Both sign-in links open the picker;
  // the picker holds exactly the two hosts.
  const signin = await page.evaluate(async () => {
    const a = document.querySelector('.signin a');
    const note = document.querySelector('.signin-note');
    const pick = document.getElementById('viewerpick');
    const before = pick ? getComputedStyle(pick).display : 'missing';
    a.click();
    await new Promise((r) => setTimeout(r, 150));
    const after = pick ? getComputedStyle(pick).display : 'missing';
    const eq = document.getElementById('viewerpick-equities');
    const cr = document.getElementById('viewerpick-crypto');
    const focused = document.activeElement === eq;
    document.getElementById('viewerpick-close').click();
    await new Promise((r) => setTimeout(r, 150));
    const closed = pick ? getComputedStyle(pick).display : 'missing';
    return a ? { href: a.getAttribute('href'), text: a.textContent.trim(), note: (note && note.textContent) || '',
      before, after, closed, focused, eq: eq && eq.href, cr: cr && cr.href, hash: location.hash } : null;
  });
  check('approved viewers have a way in', !!signin && signin.href === '#viewerpick' && /no sign-up form/i.test(signin.note),
    signin ? `${signin.href} — "${signin.text}"` : 'no sign-in link');
  check('the sign-in asks which desk', !!signin && signin.before === 'none' && signin.after === 'flex'
    && signin.closed === 'none' && signin.focused && signin.hash === '',
    signin ? `before=${signin.before} after=${signin.after} closed=${signin.closed} focus-on-equities=${signin.focused} hash="${signin.hash}"` : 'no picker');
  check('the picker offers the two desks by host', !!signin
    && signin.eq === 'https://philforge.in/app' && signin.cr === 'https://crypto.philforge.in/app',
    signin ? `${signin.eq} | ${signin.cr}` : 'no picker');

  // ── retina: the canvas-doubling failure is invisible at dpr 1 ──────────
  // A HiDPI helper that reads back the same height attribute it writes grows
  // the backing store every frame until allocation fails and Chrome paints a
  // white broken-image box. At dpr 1 the multiplier is 1, so it never shows.
  const hi = await browser.newPage({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: 2 });
  await hi.goto(`${BASE}/index.html`, { waitUntil: 'load' });
  await hi.evaluate(() => document.getElementById('eq').scrollIntoView({ behavior: 'instant', block: 'center' }));
  await hi.waitForTimeout(2600);
  await hi.evaluate(() => document.getElementById('cndl').scrollIntoView({ behavior: 'instant', block: 'center' }));
  await hi.waitForTimeout(2600);
  // Sample twice across many frames: the signature of the bug is a backing
  // store that keeps growing, so stability over time is the real assertion
  // (a pixel or two off css x dpr is just sub-pixel layout rounding).
  const sample = () => hi.evaluate(() =>
    [...document.querySelectorAll('canvas')].map((c) => {
      const r = c.getBoundingClientRect();
      return { id: c.id, w: c.width, h: c.height, want: Math.round(r.width * devicePixelRatio) };
    }));
  const first = await sample();
  await hi.waitForTimeout(1800);
  const second = await sample();
  const grew = second.filter((c, i) => c.w > first[i].w || c.h > first[i].h);
  const wild = second.filter((c) => c.want > 0 && c.w > c.want * 1.5);
  check('canvases stable at dpr 2', grew.length === 0 && wild.length === 0,
    grew.length ? 'GROWING: ' + grew.map((c) => c.id).join(',')
      : wild.length ? 'oversized: ' + wild.map((c) => `${c.id} ${c.w}≫${c.want}`).join(',')
      : second.map((c) => `${c.id} ${c.w}x${c.h}`).join(', '));
  await hi.close();

  // ── mobile: the viewport meta is what makes the breakpoints real ───────
  const m = await browser.newPage({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true, deviceScaleFactor: 3 });
  await m.goto(`${BASE}/index.html`, { waitUntil: 'load' });
  await m.waitForTimeout(1800);
  const mob = await m.evaluate(() => ({
    scrollW: document.documentElement.scrollWidth,
    inner: window.innerWidth,
    // The links are not in the bar on a phone — they are one tap away in the
    // sheet, which is a different thing from the display:none they used to be.
    menuButton: getComputedStyle(document.getElementById('navtoggle')).display !== 'none',
    sheetShut: !document.getElementById('navlinks').classList.contains('open'),
    h1: parseFloat(getComputedStyle(document.querySelector('h1')).fontSize),
    heroSrc: !!document.getElementById('heroFilm').getAttribute('src'),
    heroPlaying: (() => { const v = document.getElementById('heroFilm'); return !v.paused && v.currentTime > 0; })(),
    poster: !!document.getElementById('heroFilm').poster,
    // Nothing has been reached yet, so nothing but the hero may have fetched.
    actSrcs: [...document.querySelectorAll('.act-fig video[data-film]')].filter((v) => v.getAttribute('src')).length,
  }));
  check('mobile viewport honoured', mob.inner <= 400, `innerWidth=${mob.inner}`);
  check('no horizontal overflow', mob.scrollW <= mob.inner + 1, `scrollW=${mob.scrollW} vs ${mob.inner}`);
  check('mobile breakpoint active', mob.menuButton && mob.sheetShut,
    `menu button=${mob.menuButton} sheet shut=${mob.sheetShut}`);

  // Every nav link must be REACHABLE on a phone, not just present in the DOM.
  // They were display:none below 820px, which took the viewer sign-in and both
  // desks off the page entirely — the thing Phil reported.
  const menu = await m.evaluate(async () => {
    document.getElementById('navtoggle').click();
    await new Promise((r) => setTimeout(r, 500));
    const shown = [...document.querySelectorAll('#navlinks a')].filter((a) => {
      const r = a.getBoundingClientRect();
      return r.width > 0 && r.height > 0 && getComputedStyle(a).display !== 'none';
    });
    const viewer = document.querySelector('.navviewer');
    const vr = viewer.getBoundingClientRect();
    const out = {
      expanded: document.getElementById('navtoggle').getAttribute('aria-expanded'),
      labels: shown.map((a) => a.textContent.trim()),
      viewerVisible: vr.width > 0 && vr.height > 0,
      viewerHref: viewer.href,
      // A thumb target, not a 13px desktop row.
      viewerTall: Math.round(vr.height),
      insideViewport: vr.left >= 0 && vr.right <= innerWidth,
    };
    // A link closes the sheet behind it.
    document.querySelector('#navlinks a[href="#method"]').click();
    await new Promise((r) => setTimeout(r, 500));
    out.closesAfterTap = !document.getElementById('navlinks').classList.contains('open');
    return out;
  });
  check('the phone menu opens every link', menu.labels.length === 7 && menu.expanded === 'true',
    `${menu.labels.length} links: ${menu.labels.join(', ')}`);
  check('viewer sign-in is reachable on a phone', menu.viewerVisible && menu.insideViewport
    && /#viewerpick$/.test(menu.viewerHref) && menu.viewerTall >= 40,
    `visible=${menu.viewerVisible} height=${menu.viewerTall}px in-viewport=${menu.insideViewport}`);
  check('the sheet closes behind a link', menu.closesAfterTap, `still open=${!menu.closesAfterTap}`);
  // The nav sign-in on a phone opens the picker, closes the sheet behind it,
  // and the two desk cards both fit inside a 390px viewport.
  const phonePick = await m.evaluate(async () => {
    document.getElementById('navtoggle').click();
    await new Promise((r) => setTimeout(r, 400));
    document.querySelector('.navviewer').click();
    await new Promise((r) => setTimeout(r, 300));
    const pick = document.getElementById('viewerpick');
    const card = document.querySelector('.viewerpick-card').getBoundingClientRect();
    const eq = document.getElementById('viewerpick-equities').getBoundingClientRect();
    const cr = document.getElementById('viewerpick-crypto').getBoundingClientRect();
    const out = {
      shown: getComputedStyle(pick).display,
      sheetShut: !document.getElementById('navlinks').classList.contains('open'),
      cardFits: card.left >= 0 && card.right <= innerWidth && card.top >= 0 && card.bottom <= innerHeight,
      eqTall: Math.round(eq.height), crTall: Math.round(cr.height),
      stacked: cr.top >= eq.bottom - 1,
    };
    dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' }));
    await new Promise((r) => setTimeout(r, 200));
    out.escClosed = getComputedStyle(pick).display === 'none';
    return out;
  });
  check('the phone sign-in opens the picker and it fits', phonePick.shown === 'flex' && phonePick.sheetShut
    && phonePick.cardFits && phonePick.stacked && phonePick.eqTall >= 44 && phonePick.crTall >= 44 && phonePick.escClosed,
    `shown=${phonePick.shown} sheet-shut=${phonePick.sheetShut} fits=${phonePick.cardFits} stacked=${phonePick.stacked} h=${phonePick.eqTall}/${phonePick.crTall} esc=${phonePick.escClosed}`);
  check('headline scaled down', mob.h1 < 48, `${mob.h1}px`);
  // The performance contract on a phone: the hero runs, and ONLY the hero.
  // It used to fetch nothing at all, which left mobile looking at stills.
  check('the hero film runs on mobile', mob.heroSrc && mob.heroPlaying && mob.poster,
    `src=${mob.heroSrc} playing=${mob.heroPlaying} poster=${mob.poster}`);
  check('mobile fetches nothing it has not reached', mob.actSrcs === 0,
    `${mob.actSrcs} act clips fetched before scrolling`);

  // A phone used to get the hero and nothing else, so a re-shot act clip could
  // never show up there. It plays now — and the contract that makes that safe
  // is the one asserted here: scrolling to a figure starts THAT clip, and
  // leaves exactly one playing on the whole page.
  const act = await m.evaluate(async () => {
    const fig = document.querySelector('.act-fig video[data-film]');
    fig.scrollIntoView({ behavior: 'instant', block: 'center' });
    await new Promise((r) => setTimeout(r, 2200));
    const all = [...document.querySelectorAll('video[data-film]')];
    return {
      film: (fig.getAttribute('src') || '').split('/').pop(),
      playing: !fig.paused && fig.currentTime > 0,
      concurrent: all.filter((v) => !v.paused).length,
    };
  });
  check('an act film plays on mobile', act.playing, `${act.film || 'no src'} playing=${act.playing}`);
  check('only one clip decodes at a time', act.concurrent === 1, `${act.concurrent} playing at once`);

  // Old bookmarks. This page replaced TWO landing scripts with two different
  // tab vocabularies, and shipping only one of them stranded /#market on the
  // marketing page — caught by CryptoForge's own app-shell spec, not here.
  // /app does not exist in this static tree; the redirect is what is asserted,
  // so a 404 landing at the right URL is a pass.
  const rescue = [];
  for (const [hash, why] of [['market', 'CryptoForge bare tab'], ['portfolio-page', 'PhilForge suffixed tab'],
                             ['results-page/42', 'run id survives'], ['markets', 'own anchor must NOT redirect']]) {
    const r = await browser.newPage();
    await r.goto(`${BASE}/index.html#${hash}`, { waitUntil: 'load' }).catch(() => {});
    await r.waitForTimeout(400);
    rescue.push({ hash, why, url: r.url().replace(BASE, '') });
    await r.close();
  }
  const wrong = rescue.filter((x) => (x.hash === 'markets' ? /\/app#/.test(x.url) : x.url !== `/app#${x.hash}`));
  check('old terminal bookmarks are handed to /app', wrong.length === 0,
    wrong.length ? wrong.map((w) => `#${w.hash} (${w.why}) → ${w.url}`).join(', ')
                 : rescue.map((r) => `#${r.hash}→${r.url}`).join(', '));

  await browser.close();

  const failed = results.filter((r) => !r.pass);
  console.log(`\n${results.length - failed.length}/${results.length} checks passed`);
  process.exit(failed.length ? 1 : 0);
})();
