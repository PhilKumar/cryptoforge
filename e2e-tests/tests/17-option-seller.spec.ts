import { test, expect, Page } from '@playwright/test';

/**
 * The 4 PM Delta option seller — PAPER ONLY (17-Sep-2026).
 *
 * The page has one job beyond on/off: show every trade twice, at the MARK the
 * backtest used and at the real QUOTES an order would have got, so the gap
 * between test and reality is on screen. Status and settings are mocked here,
 * so this never switches the server's paper book on.
 */

const PIN = process.env.E2E_PIN || '123456';
const USER = process.env.E2E_USER || 'admin';

function status(over: Record<string, unknown> = {}) {
  const closed = {
    date: '2026-09-16', status: 'closed', side: 'C', votes: { C: 6, P: 0 },
    symbol: 'C-BTC-77000-160926', contracts: 100, size_btc: 0.1,
    entry_mark: 455, entry_bid: 447, stop_px: 910, exit_why: 'time', exit_mark: 60, exit_ask: 70,
    pnl_usd_mark: 37.67, pnl_usd_quote: 35.56, stop_touched_by_candle: false,
  };
  const stopped = {
    ...closed, date: '2026-09-15', side: 'P', symbol: 'P-BTC-76000-150926', votes: { C: 0, P: 5 },
    exit_why: 'stop', exit_mark: 772, pnl_usd_mark: -43.48, pnl_usd_quote: -46.14,
  };
  const skipped = {
    date: '2026-09-17', status: 'skipped', side: '', votes: { C: 0, P: 0 },
    reason: 'no agreement — 0 windows up, 0 down, 5 needed',
  };
  return {
    strategy: 'option-seller', paper_only: true, enabled: false, size_btc: 0.1, contracts: 100,
    rule: { min_votes: 5 }, next_decision_ist: '2026-09-18 16:00 IST',
    today: skipped, open: null,
    totals: {
      trades: 2, wins: 1, stops: 1, pnl_usd_mark: -5.81, pnl_usd_quote: -10.58,
      worst_run_usd_mark: 43.48, poll_missed_stops: 0,
    },
    days: [skipped, closed, stopped],
    events: [{ time: '2026-09-17 16:00:31 IST', level: 'info', message: '2026-09-17: no trade — 0 up / 0 down, 5 needed' }],
    writer: true,
    ...over,
  };
}

async function openOptionSeller(page: Page) {
  let state = status();
  const posted: unknown[] = [];
  await page.route('**/api/option-seller/status', (route) =>
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(state) }),
  );
  await page.route('**/api/option-seller/settings', async (route) => {
    const body = route.request().postDataJSON();
    posted.push(body);
    if (typeof body.enabled === 'boolean') state = status({ enabled: body.enabled, size_btc: body.size_btc });
    await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(state) });
  });
  await page.goto('/app');
  await page.fill('#username-input', USER);
  await page.fill('#password-input', PIN);
  await page.click('#unlock-btn');
  await page.waitForSelector('.nav-tab', { timeout: 15_000 });
  await page.waitForFunction(() => typeof (window as any).showPage === 'function');
  await page.evaluate(() => (window as any).showPage('optsell-page', document.getElementById('nav-strategies')));
  await expect(page.locator('#optsell-page')).toBeVisible();
  await expect(page.locator('#cf-os-stat-trades')).toHaveText('2', { timeout: 10_000 });
  return posted;
}

test.describe('Option Seller — paper', () => {
  test('the tab is in the strategy switcher and marked active', async ({ page }) => {
    await openOptionSeller(page);
    const tab = page.locator('#cf-strat-subnav .cf-strat-tab[data-cf-strat-page="optsell-page"]');
    await expect(tab).toBeVisible();
    await expect(tab).toHaveClass(/is-active/);
    await expect(tab.locator('small')).toContainText('Paper');
  });

  test('the headline shows both results: at the mark and at the real quotes', async ({ page }) => {
    await openOptionSeller(page);
    await expect(page.locator('#cf-os-stat-pnl')).toHaveText('−$5.81');
    await expect(page.locator('#cf-os-stat-pnl-sub')).toContainText('−$10.58');
    await expect(page.locator('#cf-os-stat-trades-sub')).toContainText('1 won');
    await expect(page.locator('#cf-os-stat-dd')).toHaveText('−$43.48');
  });

  test('every day is listed with both results', async ({ page }) => {
    await openOptionSeller(page);
    const rows = page.locator('#cf-os-days tbody tr');
    await expect(rows).toHaveCount(3);
    await expect(rows.nth(0)).toContainText('No trade');
    await expect(rows.nth(1)).toContainText('Closed 17:25');
    await expect(rows.nth(1)).toContainText('+$37.67');
    await expect(rows.nth(1)).toContainText('+$35.56');
    await expect(rows.nth(2)).toContainText('Stopped');
    await expect(page.locator('#cf-os-today')).toContainText('no agreement');
  });

  test('turning it on and off sends a real boolean and flips the controls', async ({ page }) => {
    const posted = await openOptionSeller(page);
    await expect(page.locator('#cf-os-state-badge')).toHaveText('Off');
    await expect(page.locator('#cf-os-off-btn')).toBeHidden();
    await page.click('#cf-os-on-btn');
    await expect(page.locator('#cf-os-state-badge')).toHaveText('On · paper');
    await expect(page.locator('#cf-os-on-btn')).toBeHidden();
    await expect(page.locator('#cf-os-off-btn')).toBeVisible();
    await page.click('#cf-os-off-btn');
    await expect(page.locator('#cf-os-state-badge')).toHaveText('Off');
    expect(posted).toEqual([
      { size_btc: 0.1, enabled: true },
      { size_btc: 0.1, enabled: false },
    ]);
  });

  test('a size outside the limits is refused before it is sent', async ({ page }) => {
    const posted = await openOptionSeller(page);
    await page.fill('#cf-os-size', '25');
    await page.click('#cf-os-on-btn');
    await expect(page.locator('#cf-os-error')).toContainText('between 0.001 and 10 BTC');
    expect(posted).toEqual([]);
  });

  test('contracts follow the size as it is typed', async ({ page }) => {
    await openOptionSeller(page);
    await page.fill('#cf-os-size', '0.25');
    await expect(page.locator('#cf-os-contracts')).toHaveText('250');
  });

  test('the manual opens in English and Tamil', async ({ page }) => {
    await openOptionSeller(page);
    await page.locator('#optsell-page .allocator-header h2 .cf-info').click();
    const panel = page.locator('#cf-os-strategy-info');
    await expect(panel).toBeVisible();
    await expect(panel.locator('[data-cf-info-language="en"]')).toContainText('paper only');
    await panel.getByRole('tab', { name: 'தமிழ்' }).click();
    await expect(panel.locator('[data-cf-info-language="ta"]')).toHaveClass(/is-active/);
  });
});
