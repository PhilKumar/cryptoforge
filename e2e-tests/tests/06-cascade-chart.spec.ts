import { test, expect, Page } from '@playwright/test';

/**
 * Cascade chart coverage.
 *
 * The chart had none. A typo in the renderer (`pal is not defined`) blanked the
 * whole drawing and nothing in CI noticed — it was found by hand, on a
 * real-money campaign. This is the net that would have caught it.
 *
 * Two layers, for two different failure modes:
 *
 *  1. RENDERER — the chart API is intercepted and a fixture is served, so every
 *     shape the renderer must survive (crowded, bare, single-candle, frozen,
 *     empty) is exercised on every run without needing a campaign to exist.
 *     This is what catches a typo or a crash in the drawing code.
 *
 *  2. PAYLOAD — if a campaign happens to exist, its chart is opened for real,
 *     with no interception. Fixtures can drift from what the server actually
 *     sends; this is what catches that. It skips when there is nothing to open.
 *
 * This suite CREATES NOTHING. Cascade runs against mainnet, so a test that
 * starts a campaign to have something to chart would be placing real orders.
 * Every campaign it touches is one that was already there, and it only reads.
 */

const PIN = process.env.E2E_PIN || '123456';

// Fixed epoch: the chart formats times to IST, and a moving clock would make
// the rendered labels differ run to run for no useful reason.
const T0 = 1750000000;
const STEP = 300;

async function login(page: Page) {
  await page.goto('/');
  for (const digit of PIN.split('')) {
    await page.click(`button.key[data-val="${digit}"]`);
  }
  await page.waitForSelector('.nav-tab', { timeout: 10_000 });
  await page.waitForFunction(() => typeof (window as any).cfCascadeShowChart === 'function');
}

// ── Fixtures, built to the shape engine/cascade.py get_chart_data returns ──

function candles(n: number, motherAt = 0) {
  return Array.from({ length: n }, (_, i) => {
    const base = 61000 - i * 8;
    return {
      t: T0 + i * STEP,
      o: base, h: base + 60, l: base - 60, c: base - 20,
      is_mother: i === motherAt,
    };
  });
}

function leg(id: number, high: number, low: number, funded = true) {
  const span = high - low;
  return {
    leg_id: id, trendline_id: id,
    touch_high: high, touch_timestamp: T0 + id * 1200,
    low, finalized: true, escalated: false,
    pool_usd: 250 * id,
    fall_pct_from_mother: 0.8 * id,
    allocation_pct: 0.5 * id,
    netted_pct: 0,
    levels: {
      '0': high, '1': low,
      '2': high - span * 0.236, '4': high - span * 0.5, '8': high - span * 0.786,
    },
    orders: funded
      ? [2, 4, 8].map((lv) => ({
          level: lv, price: high - span * (lv / 10), usd_notional: 40 * lv,
          status: 'open', fill_price: null, own_usd: 40 * lv,
          received: 0, moved_usd: 0, moved_to_level: null,
        }))
      : [],
  };
}

// A trendline is a ray extrapolated across the whole window, and the renderer
// only labels it where it is still on screen. The second anchor is kept close
// to the first on purpose: a steeper line leaves the price range long before
// the right edge, and then there is legitimately no label to assert on.
function trendline(id: number, active: boolean, bearsFib = true) {
  return {
    id,
    a1: { t: T0, p: 61060 },
    a2: { t: T0 + 20000, p: 60800 - id * 40 },
    active, bears_fib: bearsFib,
  };
}

function chartPayload(over: Record<string, unknown> = {}) {
  return {
    status: 'ok',
    campaign_id: 'e2e-chart-fixture',
    symbol: 'BTCUSDT',
    state: 'ARMED',
    mode: 'paper',
    mother: { t: T0, high: 61060, low: 60940 },
    timeframe: '5m',
    timeframe_auto: true,
    mother_forced_visible: false,
    timeframe_options: ['5m', '15m', '1h'],
    campaign_timeframe: '5m',
    candles: candles(80),
    trendlines: [trendline(1, false), trendline(2, true)],
    legs: [leg(1, 60900, 60300), leg(2, 60600, 60000)],
    fills: [],
    entries: [{ t: T0 + 1500, price: 60500, round: 1 }],
    exits: [],
    avg_entry_price: 60500,
    tp_price: 60800,
    last_price: 60450,
    frozen: false,
    ...over,
  };
}

// Every shape the renderer has to survive. The degenerate ones are here on
// purpose: a single candle makes the time scale's t1-t0 zero, and a campaign
// with no structures yet is what every campaign looks like in its first minute.
const SCENARIOS: Array<{ name: string; payload: Record<string, unknown>; drawsGeometry: boolean }> = [
  {
    name: 'live campaign',
    payload: chartPayload(),
    drawsGeometry: true,
  },
  {
    name: 'crowded — more structures than the draw cap',
    payload: chartPayload({
      trendlines: [1, 2, 3, 4, 5].map((i) => trendline(i, i === 5)),
      legs: [1, 2, 3, 4, 5].map((i) => leg(i, 60900 - i * 90, 60300 - i * 90)),
    }),
    drawsGeometry: true,
  },
  {
    name: 'same-shelf trendline that bears no fib',
    payload: chartPayload({ trendlines: [trendline(1, true, false)] }),
    drawsGeometry: true,
  },
  {
    name: 'bare — armed, nothing marked yet',
    payload: chartPayload({
      trendlines: [], legs: [], entries: [], exits: [],
      avg_entry_price: null, tp_price: null,
    }),
    drawsGeometry: true,
  },
  {
    name: 'single candle — zero-width time scale',
    payload: chartPayload({ candles: candles(1), trendlines: [], legs: [] }),
    drawsGeometry: true,
  },
  {
    name: 'frozen record with a closed round',
    payload: chartPayload({
      state: 'ENDED', frozen: true, snapshot: true,
      exits: [{ t: T0 + 12000, price: 60800, round: 1, pnl: 42.5, avg_entry: 60500 }],
      entries: [
        { t: T0 + 1500, price: 60600, round: 1 },
        { t: T0 + 4500, price: 60400, round: 1 },
      ],
    }),
    drawsGeometry: true,
  },
  {
    name: 'no candles replayed yet',
    payload: chartPayload({ candles: [], trendlines: [], legs: [] }),
    drawsGeometry: false,
  },
];

// An uncaught exception is never filtered — a renderer crash is precisely what
// this suite exists to catch, and `pal is not defined` arrived as one.
//
// Console lines are filtered down to JS errors. A "Failed to load resource" is
// the browser reporting a status code, not the chart failing: this suite
// deliberately provokes a 404 in the error-handling test, and the app's own
// background polling trips the server's rate limiter under a fast test loop.
// Neither says anything about whether the chart drew, and leaving them in
// makes this the suite everyone learns to ignore.
const NOISE = /Failed to load resource|favicon|fonts\.googleapis|manifest|service worker|WebSocket|net::ERR_/i;

function watchErrors(page: Page) {
  const errors: string[] = [];
  page.on('pageerror', (err) => errors.push(`pageerror: ${err.message}`));
  page.on('console', (msg) => {
    if (msg.type() === 'error') errors.push(`console: ${msg.text()}`);
  });
  return {
    take() {
      const out = errors.filter((e) => !NOISE.test(e));
      errors.length = 0;
      return out;
    },
  };
}

// One route handler for the whole test, swapping what it serves. Registering a
// fresh handler per payload would stack them, and this suite walks several
// payloads inside a single test to keep its login count down — see the note on
// the sweeps below.
async function serveFixture(page: Page, initial: Record<string, unknown>) {
  let current = initial;
  await page.route('**/api/cascade/campaigns/*/chart*', (route) =>
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(current) }),
  );
  return (next: Record<string, unknown>) => { current = next; };
}

async function openChart(page: Page, mode?: string) {
  await page.evaluate(
    ([m]) => (window as any).cfCascadeShowChart('e2e-chart-fixture', m || undefined),
    [mode || ''],
  );
  await expect(page.locator('#cf-cascade-chart-overlay')).toBeVisible();
}

// Did the chart actually PAINT? Each renderer gets the strongest check its
// medium allows: structural for the SVG (a blank chart has no candle bodies),
// and a real pixel sample for the canvas.
async function svgIsPainted(page: Page, minCandles: number, where = 'svg') {
  const svg = page.locator('#cf-cascade-chart-body svg');
  await expect(svg, where).toBeVisible();
  // One <rect> per candle body. `pal is not defined` threw before the first
  // one was appended, and this is the assertion that would have said so.
  expect(await svg.locator('rect').count(), `${where}: candle bodies`).toBeGreaterThanOrEqual(minCandles);
  expect(await svg.locator('text').count(), `${where}: labels`).toBeGreaterThan(0);
}

async function canvasIsPainted(page: Page, where = 'canvas', minCandles = 1) {
  await expect(page.locator('#cf-chart-canvas-host'), where).toBeVisible();
  const info = await page.evaluate(() => {
    const cv = document.getElementById('cf-chart-canvas-main') as HTMLCanvasElement | null;
    if (!cv || !cv.width) return null;
    const px = cv.getContext('2d')!.getImageData(0, 0, cv.width, cv.height).data;
    let painted = 0;
    for (let i = 3; i < px.length; i += 4) if (px[i] > 0) painted++;
    const dpr = window.devicePixelRatio || 1;
    const chart = (window as any)._cfChartCanvas;
    return {
      painted, backingW: cv.width, cssW: Math.round(cv.getBoundingClientRect().width), dpr,
      paint: chart && chart.paint,
    };
  });
  expect(info, `${where}: canvas must be mounted and sized`).not.toBeNull();
  expect(info!.painted, `${where}: canvas must not be blank`).toBeGreaterThan(0);
  // The whole point of the canvas engine: a device-pixel backing store.
  expect(info!.backingW, `${where}: backing store must be DPR-scaled`)
    .toBe(Math.round(info!.cssW * info!.dpr));
  // The Phase 1 notice made pixels too. Phase 2 must prove that the real
  // layers painted: a frame alone is not a candle chart.
  expect(info!.paint, `${where}: canvas must report its paint layers`).toBeTruthy();
  expect(info!.paint.candles, `${where}: candle layer`).toBeGreaterThanOrEqual(minCandles);
  expect(info!.paint.labels, `${where}: axis and chart labels`).toBeGreaterThan(0);
}

test.describe('Cascade chart', () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  // Every payload shape is walked inside ONE test per engine rather than one
  // test each. Each test logs in, and at a test per scenario this suite's login
  // volume alone tripped the server's rate limiter hard enough to fail
  // unrelated specs running beside it. The coverage is identical; the scenario
  // name rides on each assertion so a failure still says which shape broke.
  for (const engine of ['classic', 'canvas'] as const) {
    test(`${engine}: renders every payload shape without errors`, async ({ page }) => {
      const watch = watchErrors(page);
      const serve = await serveFixture(page, SCENARIOS[0].payload);
      await page.evaluate((e) => (window as any).cfCascadeSetEngine(e), engine);

      for (const scenario of SCENARIOS) {
        serve(scenario.payload);
        await openChart(page);

        const where = `${engine} · ${scenario.name}`;
        const n = (scenario.payload.candles as unknown[]).length;
        if (!scenario.drawsGeometry) {
          // No candles is a legitimate state, not a failure — it must say so in
          // words rather than leaving an empty panel.
          await expect(page.locator('#cf-cascade-chart-body'), where)
            .toContainText('No candles replayed yet');
        } else {
          // The legend is part of the chart: without its colour key the drawing
          // cannot be read at all.
          await expect(page.locator('.cf-cascade-chart-legend'), where).toBeVisible();
          if (engine === 'canvas') await canvasIsPainted(page, where, n);
          else await svgIsPainted(page, n, where);
        }

        expect(watch.take(), `console/page errors while rendering ${where}`).toEqual([]);
        await page.evaluate(() => (window as any).cfCascadeHideChart());
      }
    });
  }

  // The things the chart exists to SAY, rather than merely that it drew. One
  // test, four subjects, for the same login-volume reason as the sweeps.
  test('classic: says what the drawing means', async ({ page }) => {
    const watch = watchErrors(page);
    const serve = await serveFixture(page, chartPayload());
    await page.evaluate(() => (window as any).cfCascadeSetEngine('classic'));
    await openChart(page);

    const body = page.locator('#cf-cascade-chart-body');
    // The left gutter carries the levels. These are the labels the chart exists
    // to show — the mother high, both fib anchors, and each funded buy level
    // with the dollars resting on it.
    await expect(body, 'gutter labels').toContainText('MOTHER');
    await expect(body, 'gutter labels').toContainText('AVG ENTRY');
    await expect(body, 'gutter labels').toContainText('TARGET');
    await expect(body.locator('svg text', { hasText: /^\d+ \(/ }).first(), 'fib level labels').toBeVisible();
    await expect(body.locator('svg text', { hasText: /\$/ }).first(), 'dollars on funded levels').toBeVisible();
    // The active trendline is starred so it can be told from the retired ones.
    await expect(body.locator('svg text', { hasText: '★' }), 'active trendline mark').toHaveCount(1);

    // Journal mode: the picture of the trade, without the live detail tables.
    await page.evaluate(() => (window as any).cfCascadeHideChart());
    await openChart(page, 'journal');
    await expect(page.locator('#cf-cascade-chart-overlay')).toHaveClass(/cf-chart-journal/);
    await expect(page.locator('.cf-cascade-chart-tables'), 'journal drops the tables').toHaveCount(0);
    await svgIsPainted(page, 80, 'journal');
    await page.evaluate(() => (window as any).cfCascadeHideChart());

    // A chart that quietly drops structures is worse than a busy one, because
    // there is no way to tell that it happened.
    serve(chartPayload({
      trendlines: [1, 2, 3, 4, 5].map((i) => trendline(i, i === 5)),
      legs: [1, 2, 3, 4, 5].map((i) => leg(i, 60900 - i * 90, 60300 - i * 90)),
    }));
    await openChart(page);
    await expect(page.locator('.cf-cascade-chart-legend'), 'draw cap is declared').toContainText('older hidden');
    // Everything is still listed below, drawn or not.
    await expect(page.locator('.cf-cascade-chart-tables'), 'hidden structures still listed')
      .toContainText('Trendline 5');
    await page.evaluate(() => (window as any).cfCascadeHideChart());

    // Without the badge a closed trade reads as a live chart that has stopped
    // updating — the same picture with the opposite meaning.
    serve(chartPayload({
      state: 'ENDED', frozen: true, snapshot: true,
      exits: [{ t: T0 + 12000, price: 60800, round: 1, pnl: 42.5, avg_entry: 60500 }],
    }));
    await openChart(page);
    await expect(page.locator('.cf-cascade-chart-legend'), 'frozen badge').toContainText('FROZEN RECORD');
    await expect(page.locator('#cf-cascade-chart-body'), 'frozen exit label').toContainText('SOLD AT');

    expect(watch.take()).toEqual([]);
  });

  test('canvas: paints the same chart structures and labels', async ({ page }) => {
    const watch = watchErrors(page);
    const serve = await serveFixture(page, chartPayload());
    await page.evaluate(() => (window as any).cfCascadeSetEngine('canvas'));
    await openChart(page);
    await canvasIsPainted(page, 'canvas meaning', 80);

    const paint = await page.evaluate(() => (window as any)._cfChartCanvas.paint);
    expect(paint.trendlines, 'trendline layer').toBe(2);
    expect(paint.fibs, 'mother, fibs, target and entry levels').toBeGreaterThan(10);
    expect(paint.markers, 'entry marker layer').toBe(1);
    expect(paint.labelTexts).toEqual(expect.arrayContaining([
      expect.stringContaining('MOTHER'),
      expect.stringContaining('AVG ENTRY'),
      expect.stringContaining('TARGET'),
      expect.stringContaining('$'),
      expect.stringContaining('TL2 ★'),
    ]));
    await page.evaluate(() => (window as any).cfCascadeHideChart());

    // A same-shelf line has no fib. It remains visible, but has to say that it
    // cannot place an order — otherwise it reads as a tradeable trendline.
    serve(chartPayload({ trendlines: [trendline(1, true, false)] }));
    await openChart(page);
    expect(await page.evaluate(() => (window as any)._cfChartCanvas.paint.labelTexts))
      .toEqual(expect.arrayContaining([expect.stringContaining('TL1 (no fib)')]));
    await page.evaluate(() => (window as any).cfCascadeHideChart());

    serve(chartPayload({
      state: 'ENDED', frozen: true, snapshot: true,
      exits: [{ t: T0 + 12000, price: 60800, round: 1, pnl: 42.5, avg_entry: 60500 }],
    }));
    await openChart(page);
    expect(await page.evaluate(() => (window as any)._cfChartCanvas.paint.labelTexts))
      .toEqual(expect.arrayContaining([
        expect.stringContaining('SOLD AT'),
        expect.stringContaining('SELL'),
      ]));
    expect(watch.take()).toEqual([]);
  });

  test('canvas: carries the Classic chart states through journal, draw-cap, fullscreen and theme changes', async ({ page }) => {
    const watch = watchErrors(page);
    const serve = await serveFixture(page, chartPayload());
    await page.evaluate(() => (window as any).cfCascadeSetEngine('canvas'));

    // Journal is a record: picture only, never the live structure tables.
    await openChart(page, 'journal');
    await expect(page.locator('#cf-cascade-chart-overlay')).toHaveClass(/cf-chart-journal/);
    await expect(page.locator('.cf-cascade-chart-tables')).toHaveCount(0);
    await canvasIsPainted(page, 'canvas journal', 80);
    await page.evaluate(() => (window as any).cfCascadeHideChart());

    // The shared legend makes the three-structure draw cap explicit while the
    // Canvas paint record proves the capped renderer still has its geometry.
    serve(chartPayload({
      trendlines: [1, 2, 3, 4, 5].map((i) => trendline(i, i === 5)),
      legs: [1, 2, 3, 4, 5].map((i) => leg(i, 60900 - i * 90, 60300 - i * 90)),
    }));
    await openChart(page);
    await expect(page.locator('.cf-cascade-chart-legend')).toContainText('older hidden');
    expect(await page.evaluate(() => (window as any)._cfChartCanvas.paint.trendlines)).toBe(3);
    await expect(page.locator('.cf-cascade-chart-tables')).toContainText('Trendline 5');

    // Expand uses the same retained canvas and still paints at its new size.
    await page.locator('#cf-cascade-fullscreen-btn').click();
    await expect(page.locator('#cf-cascade-chart-panel')).toHaveClass(/cf-cascade-chart-fs/);
    await canvasIsPainted(page, 'canvas fullscreen', 80);

    // Unlike SVG, Canvas needs an explicit repaint when the app theme changes.
    await page.evaluate(() => document.documentElement.setAttribute('data-theme', 'light'));
    await page.waitForFunction(() => (window as any)._cfChartCanvas.paint.theme === 'light');
    await canvasIsPainted(page, 'canvas light repaint', 80);
    await page.evaluate(() => document.documentElement.setAttribute('data-theme', 'dark'));
    await page.waitForFunction(() => (window as any)._cfChartCanvas.paint.theme === 'dark');
    await page.evaluate(() => (window as any).cfCascadeHideChart());
    expect(watch.take()).toEqual([]);
  });

  test('Canvas and Classic can be compared on the same fixture in both themes', async ({ page }, testInfo) => {
    const serve = await serveFixture(page, chartPayload());
    const body = page.locator('#cf-cascade-chart-body');
    for (const theme of ['dark', 'light']) {
      // A screenshot is useful only when both renderers received the identical
      // payload. The fixture route remains installed while the engine flips.
      await page.evaluate(([nextTheme]) => document.documentElement.setAttribute('data-theme', nextTheme), [theme]);
      for (const engine of ['classic', 'canvas'] as const) {
        await page.evaluate((nextEngine) => (window as any).cfCascadeSetEngine(nextEngine), engine);
        await openChart(page);
        const path = testInfo.outputPath(`cascade-${theme}-${engine}.png`);
        await body.screenshot({ path });
        await testInfo.attach(`cascade ${theme} ${engine}`, { path, contentType: 'image/png' });
        await page.evaluate(() => (window as any).cfCascadeHideChart());
      }
    }
    // Keep the rest of the suite independent from this visual exercise.
    await page.evaluate(() => {
      document.documentElement.setAttribute('data-theme', 'auto');
      (window as any).cfCascadeSetEngine('classic');
    });
    serve(chartPayload());
  });

  test('canvas: pan, cursor zoom and independent axis drags change only their intended viewport', async ({ page }) => {
    await serveFixture(page, chartPayload());
    await page.evaluate(() => (window as any).cfCascadeSetEngine('canvas'));
    await openChart(page);
    // The compact dialog intentionally scrolls its body when the complete
    // ledger is taller than the window. Expand before testing the bottom time
    // axis so every deliberate hit target is actually visible to the user.
    await page.evaluate(() => (window as any).cfCascadeToggleFullscreen(true));
    const host = page.locator('#cf-chart-canvas-host');
    const box = await host.boundingBox();
    expect(box, 'canvas host bounds').not.toBeNull();
    const state = async () => page.evaluate(() => {
      const c = (window as any)._cfChartCanvas;
      if (!c) return { missing: true, overlay: document.getElementById('cf-cascade-chart-overlay')?.style.display || '' };
      return { viewport: { ...c.viewport }, projection: {
        padL: c.projection.padL, padT: c.projection.padT, plotW: c.projection.plotW, plotH: c.projection.plotH,
      } };
    });
    const start = await state();
    expect(start, 'Canvas remains mounted after chart opens').not.toHaveProperty('missing');
    const plotX = box!.x + start.projection.padL + start.projection.plotW * 0.55;
    const plotY = box!.y + start.projection.padT + start.projection.plotH * 0.45;

    // Wheel zoom is time-only and anchors the bar beneath the pointer.
    await page.mouse.move(plotX, plotY);
    await page.mouse.wheel(0, -120);
    const afterWheel = await state();
    expect(afterWheel, 'Canvas remains mounted after wheel zoom').not.toHaveProperty('missing');
    expect(afterWheel.viewport.tMax - afterWheel.viewport.tMin, 'wheel narrows time').toBeLessThan(start.viewport.tMax - start.viewport.tMin);
    expect(afterWheel.viewport.pMin, 'wheel leaves price minimum').toBe(start.viewport.pMin);
    expect(afterWheel.viewport.pMax, 'wheel leaves price maximum').toBe(start.viewport.pMax);

    // The existing toolbar controls drive the Canvas time viewport as well.
    await page.getByLabel('Zoom in').click();
    const afterToolbar = await state();
    expect(afterToolbar.viewport.tMax - afterToolbar.viewport.tMin, 'toolbar zoom narrows time')
      .toBeLessThan(afterWheel.viewport.tMax - afterWheel.viewport.tMin);
    expect(afterToolbar.viewport.pMin, 'toolbar leaves price minimum').toBe(afterWheel.viewport.pMin);
    expect(afterToolbar.viewport.pMax, 'toolbar leaves price maximum').toBe(afterWheel.viewport.pMax);

    // Dragging the price gutter changes price scale only.
    const priceX = box!.x + afterToolbar.projection.padL + afterToolbar.projection.plotW + 14;
    const axisY = box!.y + afterToolbar.projection.padT + afterToolbar.projection.plotH / 2;
    await page.mouse.move(priceX, axisY);
    await page.mouse.down();
    await page.mouse.move(priceX, axisY - 55);
    await page.mouse.up();
    const afterPrice = await state();
    expect(afterPrice, 'Canvas remains mounted after price-axis drag').not.toHaveProperty('missing');
    expect(afterPrice.viewport.pMax - afterPrice.viewport.pMin, 'upward price-axis drag zooms in')
      .toBeLessThan(afterToolbar.viewport.pMax - afterToolbar.viewport.pMin);
    expect(afterPrice.viewport.tMin, 'price axis leaves time minimum').toBe(afterToolbar.viewport.tMin);
    expect(afterPrice.viewport.tMax, 'price axis leaves time maximum').toBe(afterToolbar.viewport.tMax);

    // The reverse motion must be the inverse operation, not merely a different
    // price scale: dragging down expands the range and still leaves time alone.
    await page.mouse.move(priceX, axisY);
    await page.mouse.down();
    await page.mouse.move(priceX, axisY + 55);
    await page.mouse.up();
    const afterPriceDown = await state();
    expect(afterPriceDown.viewport.pMax - afterPriceDown.viewport.pMin, 'downward price-axis drag zooms out')
      .toBeGreaterThan(afterPrice.viewport.pMax - afterPrice.viewport.pMin);
    expect(afterPriceDown.viewport.tMin, 'price-axis direction leaves time minimum').toBe(afterPrice.viewport.tMin);
    expect(afterPriceDown.viewport.tMax, 'price-axis direction leaves time maximum').toBe(afterPrice.viewport.tMax);

    // Dragging the time gutter changes time scale only.
    const timeX = box!.x + afterPriceDown.projection.padL + afterPriceDown.projection.plotW / 2;
    const timeY = box!.y + afterPriceDown.projection.padT + afterPriceDown.projection.plotH + 2;
    await page.mouse.move(timeX, timeY);
    await page.mouse.down();
    await page.mouse.move(timeX - 55, timeY);
    await page.mouse.up();
    const afterTime = await state();
    expect(afterTime, 'Canvas remains mounted after time-axis drag').not.toHaveProperty('missing');
    expect(afterTime.viewport.tMax - afterTime.viewport.tMin, 'time axis changes time span')
      .not.toBe(afterPrice.viewport.tMax - afterPrice.viewport.tMin);
    expect(afterTime.viewport.pMin, 'time axis leaves price minimum').toBe(afterPriceDown.viewport.pMin);
    expect(afterTime.viewport.pMax, 'time axis leaves price maximum').toBe(afterPriceDown.viewport.pMax);

    // Each axis has an independent double-click reset; the plot reset is full fit.
    const fit = await page.evaluate(() => (window as any)._cfChartCanvasFit((window as any)._cfChartCanvas));
    await page.mouse.dblclick(priceX, axisY);
    const afterPriceReset = await state();
    expect(afterPriceReset.viewport.pMin, 'price reset restores fitted minimum').toBe(fit.pMin);
    expect(afterPriceReset.viewport.pMax, 'price reset restores fitted maximum').toBe(fit.pMax);
    expect(afterPriceReset.viewport.tMin, 'price reset retains time').toBe(afterTime.viewport.tMin);
    expect(afterPriceReset.viewport.tMax, 'price reset retains time').toBe(afterTime.viewport.tMax);

    await page.mouse.dblclick(timeX, timeY);
    const afterTimeReset = await state();
    expect(afterTimeReset.viewport.tMin, 'time reset restores fitted minimum').toBe(fit.tMin);
    expect(afterTimeReset.viewport.tMax, 'time reset restores fitted maximum').toBe(fit.tMax);
    expect(afterTimeReset.viewport.pMin, 'time reset retains price').toBe(afterPriceReset.viewport.pMin);
    expect(afterTimeReset.viewport.pMax, 'time reset retains price').toBe(afterPriceReset.viewport.pMax);

    // Plot drag pans both ranges, unlike either dedicated axis gutter.
    await page.mouse.move(plotX, plotY);
    await page.mouse.down();
    await page.mouse.move(plotX + 45, plotY + 35);
    await page.mouse.up();
    const afterPan = await state();
    expect(afterPan.viewport.tMin, 'pan changes time').not.toBe(afterTimeReset.viewport.tMin);
    expect(afterPan.viewport.pMin, 'pan changes price').not.toBe(afterTimeReset.viewport.pMin);
    await page.mouse.dblclick(plotX, plotY);
    const afterFullReset = await state();
    expect(afterFullReset.viewport).toEqual(fit);

    // The overlay has its own pixels: pointer movement must paint it without
    // redrawing the main surface, and leaving the host clears it again.
    await page.mouse.move(plotX, plotY);
    const overlayPainted = await page.evaluate(() => {
      const cv = document.getElementById('cf-chart-canvas-overlay') as HTMLCanvasElement;
      const px = cv.getContext('2d')!.getImageData(0, 0, cv.width, cv.height).data;
      return px.some((_, i) => i % 4 === 3 && px[i] > 0);
    });
    expect(overlayPainted, 'crosshair paints the overlay').toBe(true);
    await page.mouse.move(box!.x - 10, box!.y - 10);
    const overlayCleared = await page.evaluate(() => {
      const cv = document.getElementById('cf-chart-canvas-overlay') as HTMLCanvasElement;
      const px = cv.getContext('2d')!.getImageData(0, 0, cv.width, cv.height).data;
      return px.every((_, i) => i % 4 !== 3 || px[i] === 0);
    });
    expect(overlayCleared, 'crosshair clears on leave').toBe(true);
  });

  test('canvas refresh keeps a research view, but follows only from the right edge', async ({ page }) => {
    const initial = chartPayload();
    const serve = await serveFixture(page, initial);
    await page.evaluate(() => (window as any).cfCascadeSetEngine('canvas'));
    await openChart(page);

    const extend = (payload: Record<string, unknown>) => {
      const bars = payload.candles as Array<Record<string, number | boolean>>;
      const last = bars[bars.length - 1];
      return {
        ...payload,
        candles: [...bars, { ...last, t: Number(last.t) + STEP, is_mother: false }],
      };
    };
    const view = async () => page.evaluate(() => {
      const c = (window as any)._cfChartCanvas;
      return { viewport: { ...c.viewport }, candleCount: c.data.candles.length };
    });

    // Start from a deliberately panned-back study view, not from the right edge.
    await page.evaluate((step) => {
      const c = (window as any)._cfChartCanvas;
      const v = c.viewport;
      (window as any)._cfChartCanvasSetViewport(c, {
        tMin: v.tMin - step * 4, tMax: v.tMax - step * 4, pMin: v.pMin, pMax: v.pMax,
      });
      (window as any).__canvasBeforeRefresh = c.main;
    }, STEP);
    const panned = await view();
    const onceExtended = extend(initial);
    serve(onceExtended);
    await page.evaluate(() => (window as any).cfCascadeRefreshChart());
    const afterPannedRefresh = await view();
    expect(afterPannedRefresh.viewport, 'a panned-back viewport is never yanked forward').toEqual(panned.viewport);
    expect(afterPannedRefresh.candleCount, 'the refreshed payload still reached Canvas').toBe(81);
    expect(await page.evaluate(() => (window as any).__canvasBeforeRefresh === (window as any)._cfChartCanvas.main),
      'refresh retains the existing Canvas surface').toBe(true);

    // Resetting to fit puts us back at the latest bar. The next refresh keeps
    // the current zoom span but advances its right edge to the new latest bar.
    await page.evaluate(() => (window as any).cfCascadeZoomReset());
    const edgeBefore = await view();
    const twiceExtended = extend(onceExtended);
    serve(twiceExtended);
    await page.evaluate(() => (window as any).cfCascadeRefreshChart());
    const afterEdgeRefresh = await view();
    const fit = await page.evaluate(() => (window as any)._cfChartCanvasFit((window as any)._cfChartCanvas));
    expect(afterEdgeRefresh.viewport.tMax, 'right-edge viewport follows the latest candle').toBe(fit.tMax);
    expect(afterEdgeRefresh.viewport.tMax - afterEdgeRefresh.viewport.tMin, 'follow retains zoom span')
      .toBe(edgeBefore.viewport.tMax - edgeBefore.viewport.tMin);
    expect(afterEdgeRefresh.viewport.pMin, 'follow leaves price minimum alone').toBe(edgeBefore.viewport.pMin);
    expect(afterEdgeRefresh.viewport.pMax, 'follow leaves price maximum alone').toBe(edgeBefore.viewport.pMax);
  });

  test('the engine toggle switches renderers and sticks', async ({ page }) => {
    const watch = watchErrors(page);
    await serveFixture(page, chartPayload());
    await page.evaluate(() => (window as any).cfCascadeSetEngine('classic'));
    await openChart(page);

    await expect(page.locator('#cf-cascade-chart-body svg')).toBeVisible();
    await expect(page.locator('#cf-cascade-zoom-group')).toBeVisible();

    await page.locator('#cf-cascade-chart-engine [data-engine="canvas"]').click();
    await canvasIsPainted(page);
    await expect(page.locator('#cf-cascade-chart-body svg')).toHaveCount(0);
    // Canvas owns compatible centre zoom and fit actions, so the shared
    // controls stay available rather than sitting there dead.
    await expect(page.locator('#cf-cascade-zoom-group')).toBeVisible();
    expect(await page.evaluate(() => localStorage.getItem('cf-chart-engine'))).toBe('canvas');

    // Closing must release the canvas, or every open leaks a ResizeObserver
    // onto a detached node.
    await page.evaluate(() => (window as any).cfCascadeHideChart());
    expect(await page.evaluate(() => (window as any)._cfChartCanvas === null)).toBe(true);

    // Errors are checked HERE, before the reload below: navigating away aborts
    // whatever the app had in flight, and its loaders log those rejections.
    // That is this test moving the page, not the chart failing.
    expect(watch.take()).toEqual([]);

    // The choice survives a reload — that is what makes flipping back instant.
    await page.reload();
    await page.waitForFunction(() => typeof (window as any).cfCascadeShowChart === 'function');
    expect(await page.evaluate(() => (window as any)._CF_CHART_ENGINE)).toBe('canvas');
    await page.evaluate(() => (window as any).cfCascadeSetEngine('classic'));
  });

  test('refresh and timeframe changes redraw cleanly', async ({ page }) => {
    const watch = watchErrors(page);
    await serveFixture(page, chartPayload());
    await page.evaluate(() => (window as any).cfCascadeSetEngine('classic'));
    await openChart(page);

    // The buttons are rebuilt from what the campaign can actually be drawn at.
    await expect(page.locator('#cf-cascade-chart-tf [data-tf="15m"]')).toBeVisible();

    // Repeated redraws are the live case — the chart polls. Whatever the
    // renderer leaves behind must not accumulate.
    for (let i = 0; i < 3; i++) {
      await page.evaluate(() => (window as any).cfCascadeRefreshChart());
      await svgIsPainted(page, 80);
    }
    expect(await page.locator('#cf-cascade-chart-body svg').count()).toBe(1);

    await page.locator('#cf-cascade-chart-tf [data-tf="15m"]').click();
    await svgIsPainted(page, 80);
    await page.locator('#cf-cascade-chart-tf [data-tf="auto"]').click();
    await svgIsPainted(page, 80);

    // Expanding must not throw, and must leave a chart on screen.
    await page.locator('#cf-cascade-fullscreen-btn').click();
    await expect(page.locator('#cf-cascade-chart-panel')).toHaveClass(/cf-cascade-chart-fs/);
    await svgIsPainted(page, 80);
    await page.locator('#cf-cascade-fullscreen-btn').click();

    expect(watch.take()).toEqual([]);
  });

  test('a chart error is reported, not left as a blank panel', async ({ page }) => {
    const watch = watchErrors(page);
    await page.route('**/api/cascade/campaigns/*/chart*', (route) =>
      route.fulfill({
        status: 404,
        contentType: 'application/json',
        body: JSON.stringify({ status: 'error', message: 'Campaign gone not found' }),
      }),
    );
    await page.evaluate(() => (window as any).cfCascadeSetEngine('classic'));
    await openChart(page);
    await expect(page.locator('#cf-cascade-chart-body')).toContainText('not found');
    expect(watch.take()).toEqual([]);
  });

  // ── Layer 2: the real payload ──
  // Fixtures prove the renderer. Only a real campaign proves the fixtures still
  // match what the server sends. Read-only, and skipped when there is nothing
  // to open — this suite never creates a campaign.
  test('renders a real campaign chart, when one exists', async ({ page }) => {
    const watch = watchErrors(page);
    const resp = await page.request.get('/api/cascade/status');
    expect(resp.status()).toBe(200);
    const status: {
      campaigns?: Array<{ campaign_id: string }>;
      closed_campaigns?: Array<{ campaign_id: string }>;
    } = await resp.json();

    const id = (status.campaigns || [])[0]?.campaign_id
      || (status.closed_campaigns || [])[0]?.campaign_id;
    test.skip(!id, 'no cascade campaign on this box to chart');

    for (const engine of ['classic', 'canvas'] as const) {
      await page.evaluate((e) => (window as any).cfCascadeSetEngine(e), engine);
      await page.evaluate((cid) => (window as any).cfCascadeShowChart(cid), id!);
      await expect(page.locator('#cf-cascade-chart-overlay')).toBeVisible();

      const body = page.locator('#cf-cascade-chart-body');
      // A campaign with no candles yet is a legitimate answer; anything else
      // must be a drawn chart.
      if (!(await body.innerText()).includes('No candles replayed yet')) {
        await expect(page.locator('.cf-cascade-chart-legend')).toBeVisible();
        if (engine === 'canvas') await canvasIsPainted(page);
        else await svgIsPainted(page, 1);
      }
      await page.evaluate(() => (window as any).cfCascadeHideChart());
    }
    await page.evaluate(() => (window as any).cfCascadeSetEngine('classic'));
    expect(watch.take()).toEqual([]);
  });
});
