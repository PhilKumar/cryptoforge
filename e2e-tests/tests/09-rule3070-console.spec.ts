import { test, expect, Page } from '@playwright/test';

/**
 * The 30-70 paper console.
 *
 * Everything here is served from fixtures. The real service replays 30 days of
 * Binance candles on demand, which is neither deterministic nor available from
 * CI — and the failures worth catching are all in the page: a renderer typo
 * that blanks the chart, a table that stops rendering, a control that shows
 * when it should not.
 *
 * The chart assertions matter most. The 30-70 does not own a renderer; it
 * hands Cascade's renderer a payload in Cascade's shape. If either side of
 * that contract drifts, the chart silently draws nothing — the exact failure
 * this suite was started for.
 */

const PIN = process.env.E2E_PIN || '123456';
const T0 = 1750000000;
const STEP = 300;

function candles(n: number) {
  const out: any[] = [];
  let price = 64000;
  for (let i = 0; i < n; i++) {
    const open = price;
    const close = price + (i % 3 === 0 ? -18 : 12);
    out.push({
      t: T0 + i * STEP,
      o: open,
      h: Math.max(open, close) + 9,
      l: Math.min(open, close) - 9,
      c: close,
      is_mother: i === 4
    });
    price = close;
  }
  return out;
}

const CHART = {
  status: 'ok',
  campaign_id: '1750001200-1750002400',
  symbol: 'BTCUSDT',
  state: 'OPEN (70% of band 1 pending)',
  mode: 'paper',
  mother: { t: T0 + 4 * STEP, high: 64120 },
  timeframe: '5m',
  timeframe_auto: true,
  timeframe_options: ['5m', '15m', '1h', '4h', '1d'],
  campaign_timeframe: '5m',
  candles: candles(60),
  trendlines: [],
  legs: [
    { leg_id: 1, touch_high: 64090, touch_timestamp: T0 + 6 * STEP, low: 63960,
      levels: { '2': 63830, '4': 63570 }, orders: [] },
    // The mother's own fib: its 0 is the mother line and its 1 the same low,
    // so both are deliberately null and only 2/4 are drawn.
    { leg_id: 2, touch_high: null, touch_timestamp: T0 + 4 * STEP, low: null,
      levels: { '2': 63800, '4': 63480 }, orders: [] }
  ],
  fills: [{ timestamp: T0 + 20 * STEP, price: 63990 }],
  entries: [{ t: T0 + 20 * STEP, price: 63990, usd: 12.5 }],
  exits: [],
  avg_entry_price: 63990,
  tp_price: 64050,
  entry_price: 63940,
  last_price: 64010,
  frozen: false,
  trade_end_ts: 0,
  close_reason: '',
  r37: {
    v_type: 'failed V', fall_pct: 0.42, pot_usd: 8.4, minor: true, cost: 12.5, paper: true,
    mother_when: '2025-06-15 18:30', touch_when: '15 Jun 19:05', target_when: null,
    buys: [{ when: '15 Jun 20:10', price: 63990, usd: 12.5, label: '30% b1' }]
  }
};

function status(running: boolean) {
  const now = Math.floor(Date.now() / 1000);
  return {
    running,
    symbol: 'BTCUSDT',
    capital: 2000,
    purse: 2000,
    start_ts: now - 7200,
    last_tick_ts: now - 60,
    last_close: 64010,
    bars: 8600,
    next_tick_ts: now + 120,
    watch: {
      price: 64010,
      bar_when: '2025-06-15 21:00',
      stage: 'dip in — 1 green so far, needs 2',
      greens: 1,
      armed_count: 3,
      armed_near: 3,
      nearest_pct: 0.11,
      mother: { price: 64120, when: '2025-06-15 18:30', below_pct: 0.17 },
      dip: { price: 63975, when: '2025-06-15 20:45' },
      armed: [
        { mother: '2025-06-15 18:30', mts: T0 + 4 * STEP, cid: '1750001200-1750002400',
          mother_high: 64120, entry: 63940, away_pct: 0.11, pending: '30% of band 1',
          minor: true, fall_pct: 0.42, pot: 8.4 },
        { mother: '2025-06-15 17:55', mts: T0 + 2 * STEP, cid: '1750000600-1750001800',
          mother_high: 64200, entry: 63880, away_pct: 0.2, pending: '70% of band 1',
          minor: false, fall_pct: 0.9, pot: 18 }
      ]
    },
    activity: [
      { ts: now - 60, kind: 'tick', text: 'scanned to $64,010.00 — 3 orders armed' },
      { ts: now - 360, kind: 'buy', text: 'BUY 30% b1 at $63,990.00 ($12.50)' }
    ],
    opens: {
      count: 1, cost: 12.5, unrealised: 0.25,
      rows: [{ mother: '2025-06-15 18:30', mts: T0 + 4 * STEP, cid: '1750001200-1750002400',
               cost: 12.5, unrealised: 0.25, target: 64050, buys: 1, minor: true,
               status: 'OPEN (70% of band 1 pending)', paper: true }],
      warmup_holding: 1, warmup_cost: 9.4, warmup_unrealised: -0.1,
      warmup_rows: [{ mother: '2025-06-14 11:05', mts: T0 - 4000, cid: '1749996000-1749997000',
                      cost: 9.4, unrealised: -0.1, target: 64380, buys: 1, minor: true,
                      status: 'OPEN (70% of band 1 pending)', paper: false }]
    },
    closed: { count: 1, net: 1.23 },
    last_error: '',
    writer_conflict: ''
  };
}

async function open3070(page: Page, running: boolean) {
  await page.route('**/api/rule3070/status**', (route) =>
    route.fulfill({ json: status(running) }));
  await page.route('**/api/rule3070/journal**', (route) =>
    route.fulfill({ json: { events: [
      { kind: 'BUY', ts: T0 + 20 * STEP, mts: T0 + 4 * STEP, cid: '1750001200-1750002400',
        when: '2025-06-15 20:10', mother: '2025-06-15 18:30', label: '30% b1',
        price: 63990, usd: 12.5, minor: true, fall_pct: 0.42, target: 64050 }
    ] } }));
  await page.route('**/api/rule3070/chart**', (route) => route.fulfill({ json: CHART }));

  await page.goto('/app');
  for (const digit of PIN.split('')) await page.click(`button.key[data-val="${digit}"]`);
  await page.waitForSelector('.nav-tab', { timeout: 15_000 });
  await page.click('#nav-rule3070');
  await expect(page.locator('#rule3070-page')).toBeVisible();
  await expect(page.locator('#cf-r37-watch-price')).toContainText('64,010');
}

test.describe('30-70 console', () => {
  test('the wait is on screen: mother, V stage, armed orders and activity', async ({ page }) => {
    const errors: string[] = [];
    page.on('pageerror', (e) => errors.push(String(e)));

    await open3070(page, true);

    await expect(page.locator('#cf-r37-watch-mother')).toContainText('64,120');
    await expect(page.locator('#cf-r37-watch-below')).toContainText('0.17% below');
    await expect(page.locator('#cf-r37-watch-stage')).toContainText('needs 2');
    await expect(page.locator('#cf-r37-watch-armed')).toHaveText('3');
    await expect(page.locator('#cf-r37-armed-body tr')).toHaveCount(2);
    await expect(page.locator('#cf-r37-armed-body')).toContainText('30% of band 1');
    await expect(page.locator('#cf-r37-activity')).toContainText('3 orders armed');
    // A running engine counts down to its next scan rather than sitting still.
    await expect(page.locator('#cf-r37-countdown')).toHaveText(/^\d+:\d\d$/);
    expect(errors).toEqual([]);
  });

  test('Stop shows only while running — [hidden] must beat .btn display', async ({ page }) => {
    await open3070(page, true);
    await expect(page.locator('#cf-r37-stop-btn')).toBeVisible();
    await expect(page.locator('#cf-r37-start-btn')).toBeHidden();

    await page.unroute('**/api/rule3070/status**');
    await page.route('**/api/rule3070/status**', (route) => route.fulfill({ json: status(false) }));
    await expect(page.locator('#cf-r37-stop-btn')).toBeHidden({ timeout: 10_000 });
    await expect(page.locator('#cf-r37-start-btn')).toBeVisible();
  });

  test('open, warm-up and journal rows all render with their charts', async ({ page }) => {
    await open3070(page, true);
    await expect(page.locator('#cf-r37-opens-body tr')).toHaveCount(1);
    await expect(page.locator('#cf-r37-warmup-card')).toBeVisible();
    await expect(page.locator('#cf-r37-warmup-body tr')).toHaveCount(1);
    await expect(page.locator('#cf-r37-journal-body')).toContainText('BUY');
    await expect(page.locator('#cf-r37-opens-body button')).toHaveText('Chart');
  });

  test('a trade charts through the Cascade renderer, not a second one', async ({ page }) => {
    const errors: string[] = [];
    page.on('pageerror', (e) => errors.push(String(e)));

    await open3070(page, true);
    await page.locator('#cf-r37-armed-body button').first().click();

    await expect(page.locator('#cf-cascade-chart-overlay')).toBeVisible();
    await expect(page.locator('#cf-cascade-chart-title')).toHaveText('30-70 Trade Chart');
    await expect(page.locator('#cf-cascade-chart-meta')).toContainText('failed V');
    // The shared renderer's own furniture: timeframe buttons and a live canvas.
    await expect(page.locator('#cf-cascade-chart-tf .cf-tf-option').first()).toBeVisible();
    expect(await page.locator('#cf-cascade-chart-body canvas').count()).toBeGreaterThan(0);
    // The armed buy line is 30-70-only and must survive the payload round trip.
    expect(await page.evaluate(() => (window as any)._cfCascadeChartData?.entry_price)).toBe(63940);
    // Something was actually painted — a blank canvas is the failure mode here.
    const painted = await page.evaluate(() => {
      const c = document.querySelector('#cf-cascade-chart-body canvas') as HTMLCanvasElement;
      if (!c) return 0;
      const ctx = c.getContext('2d');
      const data = ctx!.getImageData(0, 0, c.width, c.height).data;
      let lit = 0;
      for (let i = 3; i < data.length; i += 4000) if (data[i] > 0) lit++;
      return lit;
    });
    expect(painted).toBeGreaterThan(0);

    await page.evaluate(() => (window as any).cfCascadeHideChart());
    // Closing must hand the chart back to Cascade, or the next campaign chart
    // would fetch from the 30-70 endpoint.
    expect(await page.evaluate(() => (window as any)._cfChartSource)).toBe('cascade');
    expect(errors).toEqual([]);
  });

  test('two fingers zoom the chart', async ({ page }) => {
    await open3070(page, true);
    await page.locator('#cf-r37-armed-body button').first().click();
    await expect(page.locator('#cf-cascade-chart-overlay')).toBeVisible();
    await page.waitForFunction(() => !!(window as any)._cfChartCanvas?.viewport);

    const result = await page.evaluate(() => {
      const c = (window as any)._cfChartCanvas;
      const before = c.viewport.tMax - c.viewport.tMin;
      const box = c.host.getBoundingClientRect();
      const send = (type: string, id: number, x: number) =>
        c.host.dispatchEvent(new PointerEvent(type, {
          pointerId: id, clientX: box.left + x, clientY: box.top + box.height * 0.5,
          bubbles: true, pointerType: 'touch'
        }));
      send('pointerdown', 1, box.width * 0.35);
      send('pointerdown', 2, box.width * 0.65);
      send('pointermove', 1, box.width * 0.1);
      send('pointermove', 2, box.width * 0.9);
      const after = c.viewport.tMax - c.viewport.tMin;
      send('pointerup', 1, box.width * 0.1);
      send('pointerup', 2, box.width * 0.9);
      return { before, after };
    });
    expect(result.after).toBeLessThan(result.before);
  });
});
