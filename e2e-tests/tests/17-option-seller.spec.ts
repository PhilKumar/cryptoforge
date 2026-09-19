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

// An open trade, as the server reports it at 16:30 IST with the money block.
function holding() {
  const open = {
    date: '2026-09-18', status: 'open', side: 'C', votes: { C: 5, P: 0 }, strike: 78200,
    symbol: 'C-BTC-78200-180926', contracts: 100, size_btc: 0.1, spot: 78150,
    entry_ts: 1789727400, entry_seen_ts: 1789727407, entry_mark: 155.39, entry_bid: 154, stop_px: 310.78,
    last_mark: 130, last_bid: 128, last_ask: 132, last_spot: 78260, last_seen_ts: 1789729200,
    view: {
      notional_usd: 7815, premium_usd: 15.54, margin_usd: 39.08, capital_usd: 54.62, im_pct: 0.5,
      im_pct_is_default: false, max_loss_usd: 17.46, pnl_usd_mark_now: 1.35, pnl_usd_quote_now: 0.97,
      mark_change_pct: -16.34, stop_distance_pct: 139.06, minutes_left: 55, seen_sec_ago: 12,
      btc_past_strike_usd: 60,
    },
  };
  return status({ enabled: true, open, today: open, days: [open] });
}

function chart() {
  const t0 = 1789727400;
  return {
    date: '2026-09-18',
    trade: {
      date: '2026-09-18', status: 'open', symbol: 'C-BTC-78200-180926', side: 'C', strike: 78200,
      entry_ts: t0, entry_mark: 155.39, stop_px: 310.78, exit_ts: null, exit_mark: null,
    },
    option: [0, 1, 2, 3].map((i) => [t0 + i * 60, 155 - i * 8]),
    index: [-2, -1, 0, 1, 2, 3].map((i) => [t0 + i * 60, 78150 + i * 30]),
  };
}

async function openOptionSeller(page: Page, initial = status()) {
  let state = initial;
  const charts: string[] = [];
  await page.route('**/api/option-seller/chart**', (route) => {
    charts.push(new URL(route.request().url()).searchParams.get('date') || '');
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(chart()) });
  });
  (page as any).__osCharts = charts;
  const posted: unknown[] = [];
  await page.route('**/api/option-seller/status', (route) =>
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(state) }),
  );
  await page.route('**/api/option-seller/settings', async (route) => {
    const body = route.request().postDataJSON();
    posted.push(body);
    if (typeof body.enabled === 'boolean') state = status({ enabled: body.enabled, size_btc: body.size_btc });
    if (typeof body.weekend_calm === 'boolean') state = { ...state, weekend_calm: body.weekend_calm };
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
  await expect(page.locator('#cf-os-stat-trades')).toHaveText(String((initial as any).totals.trades), { timeout: 10_000 });
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

  test('an open trade shows its price now, P&L, risk and capital', async ({ page }) => {
    await openOptionSeller(page, holding());
    await expect(page.locator('#cf-os-stat-open')).toHaveText('+$1.35');
    await expect(page.locator('#cf-os-stat-open-sub')).toContainText('$54.62');
    const today = page.locator('#cf-os-today');
    await expect(today).toContainText('Price now');
    await expect(today).toContainText('130.00');
    await expect(today).toContainText('−16.3%');
    await expect(today).toContainText('+$0.97');
    await expect(today).toContainText('above the strike');
    await expect(today).toContainText('−$17.46');
    await expect(today).toContainText('55 min left');
    await expect(today).toContainText('≈ $54.62');
    await expect(page.locator('#cf-os-days tbody tr').first()).toContainText('130.00 now');
  });

  test('the trade chart draws the option and Bitcoin, with the stop and the entry', async ({ page }) => {
    await openOptionSeller(page, holding());
    const panel = page.locator('#cf-os-chart-panel');
    await expect(panel).toBeVisible();
    await expect(panel.locator('.cf-os-line-option')).toHaveCount(1);
    await expect(panel.locator('.cf-os-line-index')).toHaveCount(1);
    await expect(panel.locator('.cf-os-stop-text')).toContainText('310.78');
    await expect(panel.locator('.cf-os-strike-text')).toContainText('78,200');
    await expect(panel.locator('#cf-os-readout')).toContainText('Bitcoin');
  });

  test("a day's Chart button draws that day", async ({ page }) => {
    await openOptionSeller(page);
    await expect(page.locator('#cf-os-chart-panel')).toBeVisible();
    await page.locator('#cf-os-days tbody tr').nth(2).getByRole('button', { name: 'Chart' }).click();
    await expect.poll(() => (page as any).__osCharts.slice(-1)[0]).toBe('2026-09-15');
    await expect(page.locator('#cf-os-days tbody tr').nth(2)).toHaveClass(/cf-os-row-shown/);
    await expect(page.locator('#cf-os-days tbody tr').nth(2).getByRole('button')).toHaveText('Chart ↑');
  });

  test('the Chart button brings the chart title into view, below the pinned menu', async ({ page }) => {
    await openOptionSeller(page, holding());
    await expect(page.locator('#cf-os-chart-panel .cf-os-line-option')).toHaveCount(1);
    await page.locator('#cf-os-events-panel').scrollIntoViewIfNeeded();
    await page.locator('#cf-os-days').getByRole('button', { name: /Chart/ }).first().click();
    const title = page.locator('#cf-os-chart-panel .table-title');
    await expect.poll(async () => {
      const box = await title.boundingBox();
      if (!box) return 'none';
      const hit = await page.evaluate(
        ([x, y]) => document.elementFromPoint(x, y)?.closest('#cf-os-chart-panel') ? 'visible' : 'covered',
        [box.x + 5, box.y + box.height / 2],
      );
      return hit;
    }, { timeout: 5000 }).toBe('visible');
  });

  test('the calm-weekend switch sends a real boolean and stays where it was put', async ({ page }) => {
    const posted = await openOptionSeller(page);
    const box = page.locator('#cf-os-weekend');
    await expect(box).toBeChecked();
    await box.uncheck();
    await expect.poll(() => posted.length).toBe(1);
    expect(posted[0]).toEqual({ weekend_calm: false });
    await expect(box).not.toBeChecked();
    await expect(page.locator('#optsell-page [data-cf-info-language="en"]')).toContainText('Calm weekends');
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
