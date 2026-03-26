import { expect, Page, test } from '@playwright/test';

const PIN = process.env.E2E_PIN || '123456';

async function login(page: Page) {
  await page.goto('/');
  for (const digit of PIN.split('')) {
    await page.click(`button.key[data-val="${digit}"]`);
  }
  await page.waitForSelector('.nav-tab', { timeout: 10_000 });
}

async function startPaper(page: Page, runName: string) {
  const resp = await page.request.post('/api/paper/start', {
    data: {
      run_name: runName,
      symbol: 'BTCUSDT',
      leverage: 10,
      trade_side: 'LONG',
      indicators: ['EMA_20_1m'],
      entry_conditions: [],
      exit_conditions: [],
      max_trades_per_day: 1,
      stoploss_pct: 5,
      target_profit_pct: 10,
      trailing_sl_pct: 0,
      initial_capital: 1000,
      position_size_pct: 100,
      fee_pct: 0,
      compounding: false,
      candle_interval: '1m',
    },
  });
  expect(resp.status()).toBe(200);
  const body = await resp.json();
  expect(['started', 'already_running']).toContain(body.status);
  return body.run_id ?? runName;
}

test.describe('Status Routes And Portfolio', () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  test('paper status for a missing run_id does not bleed active engine state', async ({ page }) => {
    const runName = `E2E-Paper-Isolation-${Date.now()}`;
    await page.request.post('/api/paper/stop', { data: { run_id: runName } });
    await startPaper(page, runName);

    const resp = await page.request.get(`/api/paper/status?run_id=${encodeURIComponent('missing-run')}`);
    expect(resp.status()).toBe(200);
    const body = await resp.json();

    expect(body.run_id).toBe('missing-run');
    expect(body.running).toBe(false);
    expect(body.strategy_name || '').not.toContain(runName);

    await page.request.post('/api/paper/stop', { data: { run_id: runName } });
  });

  test('paper status keeps the stopped snapshot for the requested run_id', async ({ page }) => {
    const runName = `E2E-Paper-Stopped-${Date.now()}`;
    await page.request.post('/api/paper/stop', { data: { run_id: runName } });
    await startPaper(page, runName);
    await page.request.post('/api/paper/stop', { data: { run_id: runName } });

    const resp = await page.request.get(`/api/paper/status?run_id=${encodeURIComponent(runName)}`);
    expect(resp.status()).toBe(200);
    const body = await resp.json();

    expect(body.run_id).toBe(runName);
    expect(body.running).toBe(false);
    expect(body.mode).toBe('paper');
    expect(body.strategy_name).toBe(runName);
  });

  test('portfolio page renders empty broker states without breaking the shell', async ({ page }) => {
    await page.click('#nav-portfolio');
    await expect(page.locator('#portfolio-page')).toHaveClass(/active-page/, { timeout: 10_000 });
    await expect(page.locator('#pf-positions-table')).toBeVisible({ timeout: 10_000 });
    await expect(page.locator('#pf-orders-table')).toBeVisible({ timeout: 10_000 });
    await expect(page.locator('#pf-positions-body')).toContainText(/No open positions|Loading/i, { timeout: 10_000 });
  });
});
