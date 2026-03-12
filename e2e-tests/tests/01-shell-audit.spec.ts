import { test, expect, Page } from '@playwright/test';

const PIN = process.env.E2E_PIN || '123456';

async function login(page: Page) {
  await page.goto('/');
  for (const digit of PIN.split('')) {
    await page.click(`button.key[data-val="${digit}"]`);
  }
  await page.waitForSelector('.nav-tab', { timeout: 10_000 });
}

test.describe('Shell Audit', () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  test('primary navigation renders every core page shell', async ({ page }) => {
    const pageErrors: string[] = [];
    page.on('pageerror', (err) => pageErrors.push(err.message));

    const checks = [
      { nav: '#nav-dashboard', section: '#dashboard-page', probe: '#dash-runs-table' },
      { nav: '#nav-live', section: '#live-page', probe: '#live-panels-container' },
      { nav: '#nav-scalp', section: '#scalp-page', probe: '#cf-scalp-active-table' },
      { nav: '#nav-portfolio', section: '#portfolio-page', probe: '#pf-positions-table' },
      { nav: '#nav-market', section: '#market-page', probe: '#market-table' },
      { nav: '#nav-builder', section: '#builder-page', probe: '#backtest-run-btn' },
      { nav: '#nav-results', section: '#results-page', probe: '#runs-table' },
    ];

    for (const check of checks) {
      await page.click(check.nav);
      await expect(page.locator(check.section)).toHaveClass(/active-page/, { timeout: 10_000 });
      await expect(page.locator(check.probe)).toBeVisible({ timeout: 10_000 });
    }

    expect(pageErrors).toEqual([]);
  });

  test('builder buttons, save flow, and deploy modal work from the UI', async ({ page }) => {
    const runName = `E2E-Builder-${Date.now()}`;

    await page.click('#nav-market');
    const marketTradeBtn = page.locator('.mkt-trade-btn').first();
    await expect(marketTradeBtn).toBeVisible({ timeout: 15_000 });
    await marketTradeBtn.click();

    await expect(page.locator('#builder-page')).toHaveClass(/active-page/, { timeout: 10_000 });
    await page.fill('#b-name', runName);

    const entryBefore = await page.locator('#entry-conditions .condition-row').count();
    const exitBefore = await page.locator('#exit-conditions .condition-row').count();

    await page.click('button:has-text("MACD")');
    await page.click('button:has-text("Bollinger")');
    await expect(page.locator('#indicator-list')).toContainText('MACD');
    await expect(page.locator('#indicator-list')).toContainText('BB');

    const addBtns = page.locator('#builder-page button:has-text("+ Add")');
    await addBtns.nth(0).click();
    await addBtns.nth(1).click();
    await expect(page.locator('#entry-conditions .condition-row')).toHaveCount(entryBefore + 1);
    await expect(page.locator('#exit-conditions .condition-row')).toHaveCount(exitBefore + 1);

    await page.click('#side-short');
    await expect(page.locator('#side-short')).toHaveClass(/short-active/);
    await page.click('#side-long');
    await expect(page.locator('#side-long')).toHaveClass(/long-active/);

    await page.click('#builder-page button:has-text("Save")');
    await expect(page.locator('#toast-container .cf-toast').last()).toContainText('Strategy saved', { timeout: 10_000 });

    const savedResp = await page.request.get('/api/strategies');
    const savedList: Array<{ id: number; run_name: string }> = await savedResp.json();
    const saved = savedList.find((s) => s.run_name === runName);
    expect(saved, 'Saved strategy must be persisted via the Save button').toBeTruthy();

    await page.click('#builder-page button:has-text("Deploy")');
    await expect(page.locator('.deploy-modal')).toBeVisible();
    await page.click('#deploy-tab-live');
    await expect(page.locator('#deploy-confirm-btn')).toContainText(/Deploy LIVE/);
    await page.click('#deploy-tab-paper');
    await expect(page.locator('#deploy-confirm-btn')).toContainText(/Deploy Paper/i);
    await page.click('.deploy-modal button:has-text("Cancel")');
    await expect(page.locator('.deploy-modal')).toBeHidden();

    if (saved) {
      await page.request.delete(`/api/strategies/${saved.id}`);
    }
  });

  test('backtest engine, results view, and scalp status endpoint respond', async ({ page }) => {
    const runName = `E2E-Backtest-${Date.now()}`;
    const btResp = await page.request.post('/api/backtest', {
      data: {
        run_name: runName,
        symbol: 'BTCUSDT',
        from_date: '2026-03-01',
        to_date: '2026-03-03',
        initial_capital: 10000,
        leverage: 5,
        trade_side: 'LONG',
        position_size_pct: 100,
        stoploss_pct: 5,
        target_profit_pct: 10,
        trailing_sl_pct: 0,
        fee_pct: 0.05,
        max_trades_per_day: 3,
        indicators: ['EMA_20_5m'],
        entry_conditions: [],
        exit_conditions: [],
        candle_interval: '5m',
      },
    });

    expect(btResp.status()).toBe(200);
    const btBody: { status: string; run_id?: number; stats?: { total_trades: number } } = await btResp.json();
    expect(btBody.status).toBe('success');
    expect(btBody.run_id).toBeTruthy();
    expect(btBody.stats).toBeTruthy();

    await page.click('#nav-results');
    await page.evaluate(() => typeof loadRuns === 'function' && loadRuns());
    const runRow = page.locator('#runs-table tbody tr').filter({ hasText: runName }).first();
    await expect(runRow).toBeVisible({ timeout: 20_000 });
    await runRow.click();
    await expect(page.locator('#run-detail-modal')).toBeVisible({ timeout: 10_000 });
    await expect(page.locator('#rd-trades')).toBeVisible();

    const scalpResp = await page.request.get('/api/scalp/status');
    expect(scalpResp.status()).toBe(200);
    const scalpStatus: { running?: boolean; mode?: string; closed_trades?: unknown[] } = await scalpResp.json();
    expect(typeof scalpStatus.running).toBe('boolean');
    expect(scalpStatus).toHaveProperty('closed_trades');

    if (btBody.run_id) {
      await page.request.delete(`/api/runs/${btBody.run_id}`);
    }
  });
});
