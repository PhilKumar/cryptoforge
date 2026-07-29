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

async function canvasIsPainted(page: Page, where = 'canvas') {
  await expect(page.locator('#cf-chart-canvas-host'), where).toBeVisible();
  const info = await page.evaluate(() => {
    const cv = document.getElementById('cf-chart-canvas-main') as HTMLCanvasElement | null;
    if (!cv || !cv.width) return null;
    const px = cv.getContext('2d')!.getImageData(0, 0, cv.width, cv.height).data;
    let painted = 0;
    for (let i = 3; i < px.length; i += 4) if (px[i] > 0) painted++;
    const dpr = window.devicePixelRatio || 1;
    return { painted, backingW: cv.width, cssW: Math.round(cv.getBoundingClientRect().width), dpr };
  });
  expect(info, `${where}: canvas must be mounted and sized`).not.toBeNull();
  expect(info!.painted, `${where}: canvas must not be blank`).toBeGreaterThan(0);
  // The whole point of the canvas engine: a device-pixel backing store.
  expect(info!.backingW, `${where}: backing store must be DPR-scaled`)
    .toBe(Math.round(info!.cssW * info!.dpr));
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
          if (engine === 'canvas') await canvasIsPainted(page, where);
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
    // The zoom buttons drive the SVG viewBox; they must not sit there dead.
    await expect(page.locator('#cf-cascade-zoom-group')).toBeHidden();
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
