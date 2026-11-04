import { test, expect, Page } from '@playwright/test';

/**
 * THE SCALP DESK — the form's coin charted beside the form.
 *
 * Phil, 2026-11-04: "Please create the same kinda scalp page for cryptoforge
 * as well same as that of philforge", everything in one go: the chart with a
 * live forming bar, B and S on it, the Studio (Supertrend, CPR, EMA, RSI, Fib,
 * lines), and right-click price alarms.
 *
 * The candles are mocked: CI cannot count on reaching an exchange, and what is
 * being guarded is the page, not Binance.
 */

const PIN = process.env.E2E_PIN || '123456';
const USER = process.env.E2E_USER || 'admin';

function chart(symbol: string, timeframe: string) {
  const step = ({ '1m': 60, '3m': 180, '5m': 300, '15m': 900, '1h': 3600 } as Record<string, number>)[timeframe] || 300;
  const now = Math.floor(Date.now() / 1000);
  const last = Math.floor(now / step) * step - step;
  const candles = [];
  for (let i = 199; i >= 0; i--) {
    const t = last - i * step;
    const p = 78000 + Math.sin(i / 9) * 400 + (199 - i) * 2;
    candles.push({ t, o: p, h: p + 60, l: p - 60, c: p + 15 });
  }
  return {
    status: 'ok', symbol, timeframe, broker: 'binance', instrument: { symbol },
    candles, lines: [], entries: [], exits: [], cpr_day: 'UTC',
    overlays: [{ label: 'CPR P (78,000.00)', color: '#d946ef', group: 'cpr', points: [{ t: candles[0].t, price: 78000 }, { t: last, price: 78000 }] }],
  };
}

async function openDesk(page: Page, asked: string[]) {
  await page.route('**/api/scalp/chart?**', (route) => {
    const url = new URL(route.request().url());
    const symbol = url.searchParams.get('symbol') || 'BTCUSDT';
    const timeframe = url.searchParams.get('timeframe') || '5m';
    asked.push(`${symbol}|${timeframe}`);
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(chart(symbol, timeframe)) });
  });
  await page.route('**/api/scalp/alarms', (route) =>
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ status: 'ok', alarms: [], fired: [] }) }),
  );
  await page.setViewportSize({ width: 1500, height: 950 });
  await page.goto('/app');
  await page.fill('#username-input', USER);
  await page.fill('#password-input', PIN);
  await page.click('#unlock-btn');
  await page.waitForSelector('.nav-tab', { timeout: 15_000 });
  await page.waitForFunction(() => typeof (window as any).showPage === 'function' && !!(window as any).cfScalpDesk);
  await page.evaluate(() => (window as any).showPage('scalp-page', document.getElementById('nav-scalp')));
  await expect(page.locator('#cf-scalp-dock #pf-bench-canvas-host')).toBeVisible({ timeout: 15_000 });
}

test.describe('The Scalp desk', () => {
  test('charts the form\'s coin beside the form, with B and S on it', async ({ page }) => {
    const errors: string[] = [];
    page.on('pageerror', (e) => errors.push(e.message));
    const asked: string[] = [];
    await openDesk(page, asked);

    await expect(page.locator('#cf-scalp-dock-title')).toHaveText('BTCUSDT');
    await expect(page.locator('#cf-desk-ticket-buy')).toBeVisible();
    await expect(page.locator('#cf-desk-ticket-sell')).toBeVisible();
    // The ticket's size IS the form's size.
    await expect(page.locator('#cf-desk-ticket-size')).toHaveValue(await page.locator('#cf-scalp-qty').inputValue());

    // Beside it, not under it: the chart's right edge is left of the form's.
    const dock = await page.locator('#cf-scalp-dock').boundingBox();
    const form = await page.locator('.cf-desk-rail .cf-scalp-form-card').boundingBox();
    expect(dock && form && dock.x + dock.width <= form.x + 1).toBeTruthy();
    expect(errors).toEqual([]);
  });

  test('follows the form to another coin and to another timeframe', async ({ page }) => {
    const asked: string[] = [];
    await openDesk(page, asked);
    await page.selectOption('#cf-scalp-symbol', 'ETHUSDT');
    await expect(page.locator('#cf-scalp-dock-title')).toHaveText('ETHUSDT', { timeout: 10_000 });
    await page.click('#cf-scalp-dock [data-strip-tf="15m"]');
    await expect.poll(() => asked[asked.length - 1]).toBe('ETHUSDT|15m');
    await expect(page.locator('#cf-scalp-dock-meta')).toContainText('15M');
  });

  test('the Studio carries the studies and the drawing tools', async ({ page }) => {
    await openDesk(page, []);
    await page.click('#cf-scalp-dock .cf-desk-studio-tab');
    for (const name of ['ST 10,2', 'CPR', 'EMA', 'RSI', 'Fib ▾', 'Line', 'Clear']) {
      await expect(page.locator('#cf-desk-studio-tray .cf-desk-control', { hasText: name }).first()).toBeVisible();
    }
    // Switching a study off redraws without it.
    await page.click('#cf-desk-studio-tray .cf-desk-cpr');
    await expect(page.locator('#cf-desk-studio-tray .cf-desk-cpr')).toHaveAttribute('aria-pressed', 'false');
    const cprShown = await page.evaluate(() =>
      (window as any).cfScalpDesk.state().shown.data.overlays.some((o: any) => o.group === 'cpr'));
    expect(cprShown).toBe(false);
  });

  test('right-click offers an alarm at that price', async ({ page }) => {
    await openDesk(page, []);
    const host = page.locator('#cf-scalp-dock #pf-bench-canvas-host');
    // page.mouse does not scroll: bring the plot on screen first.
    await host.scrollIntoViewIfNeeded();
    const box = await host.boundingBox();
    await page.mouse.click(box!.x + box!.width / 2, box!.y + box!.height / 3, { button: 'right' });
    await expect(page.locator('#cf-desk-alarm-menu .is-add')).toContainText('Alarm at $');
    await page.keyboard.press('Escape');
    await expect(page.locator('#cf-desk-alarm-menu')).toHaveCount(0);
  });
});
