/**
 * Check the Ecclesiastes epigraph and the PhilForge's Dōjima brand marks.
 *
 * Hebrew fails quietly: a missing glyph renders as .notdef boxes at the same
 * layout size, and a cantillation mark can be dropped or collide with the
 * letter without changing anything measurable. So this asserts the codepoints
 * survived intact, the text really is laid out right-to-left, and it also
 * writes a crop of the band for a human to look at.
 */
const { chromium } = require('playwright');

const BASE = process.argv[2] || 'http://localhost:8096';
const OUT = process.argv[3] || '/tmp/epigraph.png';
const results = [];
const check = (name, pass, detail) => {
  results.push({ name, pass });
  console.log(`${pass ? '  ok  ' : ' FAIL '} ${name}${detail ? ' — ' + detail : ''}`);
};

const HEB = 'וְהַכֶּסֶף'; // וְהַכֶּסֶף, sans te'amim
const TIPCHA = '֖'; // the cantillation mark under the kaf

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: 2 });
  await page.goto(`${BASE}/index.html`, { waitUntil: 'load' });
  await page.evaluate(() => document.querySelector('.epi').scrollIntoView({ behavior: 'instant', block: 'center' }));
  await page.waitForTimeout(1600);

  const e = await page.evaluate(() => {
    const heb = document.querySelector('.heb');
    const verse = document.querySelector('.epi-v');
    const cs = getComputedStyle(heb);
    const r = heb.getBoundingClientRect();
    return {
      text: heb.textContent,
      dir: cs.direction,
      lang: heb.getAttribute('lang'),
      font: cs.fontFamily.split(',')[0].replace(/"/g, ''),
      size: parseFloat(cs.fontSize),
      color: cs.color,
      w: Math.round(r.width), h: Math.round(r.height),
      verse: verse.textContent.replace(/\s+/g, ' ').trim(),
      cite: document.querySelector('.epi-cite').textContent.trim(),
      ...(() => {
        const c = document.querySelector('.epi-cite');
        const band = c.closest('.epi');
        const cr = c.getBoundingClientRect(), br = band.getBoundingClientRect();
        const op = parseFloat(getComputedStyle(c).opacity);
        return {
          citeOpacity: op,
          citeIn: c.classList.contains('in'),
          // the painted box must sit inside the band that may be clipping it
          citeClipped: getComputedStyle(band).overflow !== 'visible' && cr.bottom > br.bottom,
          citeShown: op > 0.9 && cr.height > 0,
        };
      })(),
      revealed: document.querySelectorAll('.epi .rv.in').length,
      navBrand: document.querySelector('nav .brand').textContent.trim(),
      footBrand: document.querySelector('footer .brand').textContent.trim(),
      bw: (() => {
        const a = document.querySelector('nav .brand .bw-1');
        const b = document.querySelector('nav .brand .bw-2');
        if (!a || !b) return { faces: 0, tracked: 0, colour: 'missing' };
        const ca = getComputedStyle(a), cb = getComputedStyle(b);
        const face = (c) => c.fontFamily.split(',')[0].replace(/"/g, '');
        return {
          faces: new Set([face(ca), face(cb)]).size,
          tracked: parseFloat(ca.letterSpacing) / parseFloat(ca.fontSize),
          colour: cb.color,
          possessiveColour: ca.color,
        };
      })(),
      desks: [...document.querySelectorAll('nav .desks a')].map((a) => a.href),
      markFit: getComputedStyle(document.querySelector('nav .brand .mark')).objectFit,
      markRatio: (() => {
        const r = document.querySelector('nav .brand .mark').getBoundingClientRect();
        return r.height / r.width;
      })(),
      title: document.title,
    };
  });

  // Codepoints, not appearance: this is what proves nothing was mangled by an
  // encoding step or dropped by the build.
  check('Hebrew letters intact', e.text.normalize('NFC').includes(HEB.normalize('NFC').slice(0, 3)) && e.text.length >= 10,
    JSON.stringify(e.text) + ` (${e.text.length} cp)`);
  check('cantillation mark kept', e.text.includes(TIPCHA), 'U+0596 tipcha present');
  check('laid out right-to-left', e.dir === 'rtl' && e.lang === 'he', `dir=${e.dir} lang=${e.lang}`);
  check('rendered at display size', e.size > 40 && e.w > 100 && e.h > 40, `${e.size}px, box ${e.w}x${e.h}`);
  check('set in brass', /rgb\(232, 204, 106\)|rgb\(201, 162, 39\)/.test(e.color), e.color);
  check('verse text exact', /A feast is made for laughter, and wine makes life merry, but money is the answer for everything\./.test(e.verse), e.verse.slice(0, 60) + '…');
  check('cited', /Ecclesiastes 10:19/.test(e.cite), e.cite);
  // "Cited" was passing on text content alone while the line was INVISIBLE:
  // an un-revealed .rv is translated down 26px, the band clipped it, so the
  // observer saw zero of it and never revealed it — a deadlock no scroll could
  // break. Assert it is actually painted, not merely present.
  check('citation is visible, not just present', e.citeShown,
    `opacity ${e.citeOpacity}, revealed=${e.citeIn}, ${e.citeClipped ? 'CLIPPED by an ancestor' : 'inside its band'}`);
  check('epigraph reveals', e.revealed > 0, `${e.revealed} revealed`);

  // The epigraph opens the page, above the hero, and carries the #top anchor.
  const order = await page.evaluate(() => {
    const epi = document.querySelector('.epi');
    const hero = document.querySelector('.hero');
    return {
      before: !!(epi.compareDocumentPosition(hero) & Node.DOCUMENT_POSITION_FOLLOWING),
      isTop: epi.id === 'top',
      clearsNav: epi.getBoundingClientRect().top + parseFloat(getComputedStyle(epi).paddingTop) >= 74,
      heroVisible: hero.getBoundingClientRect().top < window.innerHeight,
      epiH: Math.round(epi.getBoundingClientRect().height),
      vh: window.innerHeight,
    };
  });
  check('epigraph opens the page', order.before && order.isTop, `aboveHero=${order.before} #top=${order.isTop}`);
  check('clears the fixed nav', order.clearsNav);
  // It should not eat the whole screen — the film has to hint at itself.
  check('hero still shows below it', order.heroVisible);
  // The regression this guards: the band grew to 74svh and buried the port
  // film, which is the picture the whole page is built on. Half the first
  // screen belongs to the picture.
  const filmShare = Math.round(((order.vh - order.epiH) / order.vh) * 100);
  check('film keeps ~half the first screen', filmShare >= 33,
    `epigraph ${order.epiH}px of ${order.vh}px — film gets ${filmShare}%`);

  // The whole opening beat has to land on one screen: the verse, the headline
  // down to its last line, and the proof tape under it. Each of these slid
  // below the fold at some point, so all three are pinned here — and at more
  // than one screen shape, because the hero's min-height is a calc().
  const opening = [];
  for (const vp of [{ width: 1440, height: 900 }, { width: 1512, height: 830 }, { width: 1280, height: 800 }]) {
    const q = await browser.newPage({ viewport: vp });
    await q.goto(`${BASE}/index.html`, { waitUntil: 'load' });
    await q.waitForTimeout(700);
    opening.push(await q.evaluate((label) => {
      const last = document.querySelector('.hero h1').lastElementChild.getBoundingClientRect();
      const tape = document.querySelector('.tape').getBoundingClientRect();
      return {
        label,
        headline: Math.round(innerHeight - last.bottom),
        // How much of the tape is actually on screen. It does not have to be
        // whole — it has to be *seen* from the top, and its type sits on the
        // first line — but a sliver of border does not count.
        tapeSeen: Math.round(Math.min(tape.bottom, innerHeight) - tape.top),
        tapeH: Math.round(tape.height),
      };
    }, `${vp.width}x${vp.height}`));
    await q.close();
  }
  const cut = opening.filter((o) => o.headline < 0 || o.tapeSeen < Math.min(20, o.tapeH));
  check('verse, headline and tape share the first screen', cut.length === 0,
    opening.map((o) => `${o.label}: ${o.tapeSeen}/${o.tapeH}px of tape`).join(', '));

  // Exactly two lines, counted by distinct baselines: a Range's rect count is
  // not a line count, because the <b> splits every line into several boxes.
  const lines = await page.evaluate(() => {
    const v = document.querySelector('.epi-v');
    const r = document.createRange();
    r.selectNodeContents(v);
    const tops = [...r.getClientRects()].map((b) => Math.round(b.top));
    return [...new Set(tops)].length;
  });
  check('verse sets in two lines', lines === 2, `${lines} lines at 1440px`);
  // Phil's mark: a broken <img> still occupies its box and still passes a
  // "is it there" test, so assert the bytes actually decoded and that it is
  // fed by artwork bigger than the box it is drawn in.
  const mark = await page.evaluate(() => ['nav', 'footer'].map((where) => {
    const i = document.querySelector(`${where} .brand .mark`);
    if (!i) return { where, missing: true };
    const r = i.getBoundingClientRect();
    const brand = i.closest('.brand').getBoundingClientRect();
    return {
      where,
      decoded: i.complete && i.naturalWidth > 0,
      nat: i.naturalWidth,
      box: Math.round(r.width),
      // "before the words": it must sit at the leading edge of the brand.
      first: Math.round(r.left) <= Math.round(brand.left) + 1,
    };
  }));
  const badMark = mark.filter((m) => m.missing || !m.decoded || !m.first || m.nat < m.box * 2);
  check('brand mark loads before the words', badMark.length === 0,
    badMark.length ? JSON.stringify(badMark) : mark.map((m) => `${m.where} ${m.box}px from ${m.nat}px art`).join(', '));

  // The tab icon: a 404 here is invisible on the page itself, and browsers
  // cache favicons hard enough that "it still looks old" is usually the cache
  // rather than the build — so assert the bytes are actually served.
  const fav = await page.evaluate(async () => {
    const l = document.querySelector('link[rel="icon"]');
    if (!l) return { missing: true };
    const res = await fetch(l.getAttribute('href'));
    const blob = await res.blob();
    const bmp = await createImageBitmap(blob).catch(() => null);
    return { href: l.getAttribute('href'), status: res.status, type: res.headers.get('content-type'),
             bytes: blob.size, w: bmp ? bmp.width : 0 };
  });
  check('favicon is the PhilForge mark', !fav.missing && fav.status === 200 && /png/.test(fav.type || '') && fav.w >= 32,
    fav.missing ? 'no <link rel=icon>' : `${fav.href} ${fav.status} ${fav.type} ${fav.w}px ${fav.bytes}B`);

  // The wordmark is two elements now (a letterspaced possessive over the name),
  // so match the parts rather than one flat string with a space in it.
  const wordmark = (s) => /PhilForge.s/.test(s) && /Dōjima/.test(s);
  check('nav brand renamed', wordmark(e.navBrand), e.navBrand);
  check('footer brand renamed', wordmark(e.footBrand), e.footBrand);
  // Both parts must actually be SET — one flat fallback serif is what made it
  // read as unstyled — and the name has to carry PhilForge's green.
  // Two faces AND two colours: the possessive in the page's display brass,
  // the name in PhilForge's green.
  check('wordmark is typeset, not just typed',
    e.bw.faces === 2 && e.bw.tracked > 0.2
    && /rgb\(232, 204, 106\)/.test(e.bw.possessiveColour)
    && /rgb\(52, 211, 153\)/.test(e.bw.colour),
    `${e.bw.faces} faces, tracked ${e.bw.tracked.toFixed(2)}em, ` +
    `PhilForge's ${e.bw.possessiveColour} / Dōjima ${e.bw.colour}`);
  // The front door forks to two desks; a single "Enter" threw that away once.
  check('both desks are reachable from the nav', e.desks.length === 2 &&
    /philforge\.in\/app$/.test(e.desks[0]) && /crypto\.philforge\.in\/app$/.test(e.desks[1]),
    e.desks.join(' , ') || 'no desk links in nav');
  // The shield is not square. A square box cropped its point off.
  check('mark shows the whole shield', e.markFit === 'contain' && e.markRatio > 1.05 && e.markRatio < 1.35,
    `object-fit:${e.markFit}, drawn ${e.markRatio.toFixed(2)}:1 tall`);
  check('page title renamed', /PhilForge.s Dōjima/.test(e.title), e.title);

  // Centring is not free here: `p{max-width:62ch}` with no auto margins makes
  // any short paragraph hug the left, which looks like a bug in the design.
  const centred = await page.evaluate(() => {
    const wrap = document.querySelector('.epi .wrap');
    const cs = getComputedStyle(wrap);
    const box = wrap.getBoundingClientRect();
    const mid = box.left + parseFloat(cs.paddingLeft) +
      (box.width - parseFloat(cs.paddingLeft) - parseFloat(cs.paddingRight)) / 2;
    return ['.heb', '.epi-tr', '.epi-rule', '.epi-v', '.epi-cite'].map((s) => {
      const r = document.querySelector(s).getBoundingClientRect();
      return { s, off: Math.round(Math.abs((r.left + r.right) / 2 - mid)) };
    });
  });
  const skewed = centred.filter((c) => c.off > 2);
  check('epigraph is centred', skewed.length === 0,
    skewed.map((c) => `${c.s} off by ${c.off}px`).join(', ') || 'all lines on the centre line');

  // A .notdef box is typically a plain rectangle; a real Hebrew string with a
  // mark under it is not. Compare ink against a deliberately broken font.
  const ink = await page.evaluate(() => {
    const el = document.querySelector('.heb');
    const r = el.getBoundingClientRect();
    return { top: r.top, left: r.left, w: r.width, h: r.height };
  });
  const shot = await page.screenshot({ clip: { x: Math.max(0, ink.left - 40), y: Math.max(0, ink.top - 30), width: ink.w + 80, height: ink.h + 60 } });
  // The <p> is full-width, so its box tells us nothing about the glyphs —
  // measure the text run itself with a Range.
  const glyphs = await page.evaluate(() => {
    const el = document.querySelector('.heb');
    const measure = () => {
      const r = document.createRange();
      r.selectNodeContents(el);
      const b = r.getBoundingClientRect();
      return { w: b.width, h: b.height };
    };
    const chosen = measure();
    const prev = el.style.fontFamily;
    el.style.fontFamily = 'monospace';
    const mono = measure();
    el.style.fontFamily = prev;
    return { chosen, mono };
  });
  check('a real Hebrew face is used',
    glyphs.chosen.w > 40 && Math.abs(glyphs.chosen.w - glyphs.mono.w) > 1,
    `"${e.font}" run ${Math.round(glyphs.chosen.w)}x${Math.round(glyphs.chosen.h)}px vs monospace ${Math.round(glyphs.mono.w)}px`);

  require('fs').writeFileSync(OUT, shot);
  await page.screenshot({ path: OUT.replace('.png', '_band.png'), clip: { x: 0, y: Math.max(0, ink.top - 190), width: 1440, height: 560 } });
  console.log(`\nwrote ${OUT} and ${OUT.replace('.png', '_band.png')}`);

  await browser.close();
  const failed = results.filter((r) => !r.pass);
  console.log(`${results.length - failed.length}/${results.length} checks passed`);
  process.exit(failed.length ? 1 : 0);
})();
