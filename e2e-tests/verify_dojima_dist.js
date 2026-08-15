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
    navHidden: getComputedStyle(document.querySelector('.navlinks a:not(.navcta)')).display === 'none',
    h1: parseFloat(getComputedStyle(document.querySelector('h1')).fontSize),
    heroSrc: !!document.getElementById('heroFilm').getAttribute('src'),
    poster: !!document.getElementById('heroFilm').poster,
  }));
  check('mobile viewport honoured', mob.inner <= 400, `innerWidth=${mob.inner}`);
  check('no horizontal overflow', mob.scrollW <= mob.inner + 1, `scrollW=${mob.scrollW} vs ${mob.inner}`);
  check('mobile breakpoint active', mob.navHidden, `navlinks hidden=${mob.navHidden}`);
  check('headline scaled down', mob.h1 < 48, `${mob.h1}px`);
  // The performance contract: below 880px no video bytes are requested at all.
  check('no film fetched on mobile', !mob.heroSrc && mob.poster, `src=${mob.heroSrc} poster=${mob.poster}`);

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
