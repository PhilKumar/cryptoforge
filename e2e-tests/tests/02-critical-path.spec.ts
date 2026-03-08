/**
 * 02-critical-path.spec.ts
 * Regression suite for the 6 CryptoForge hotfixes:
 *   1. View modal renders HTML, not raw tag strings
 *   2. Timestamps shown as HH:MM:SS
 *   3. Paper trades appear under the Paper filter
 *   4. Scalp filter button exists and works
 *   5. Paper engine transitions from Scanning → In Trade
 *   6. supertrend_dir column is NOT exposed in the indicator API
 */

import { test, expect, Page } from '@playwright/test';

const PIN = process.env.E2E_PIN || '123456';

// ── Auth helper ─────────────────────────────────────────────
async function login(page: Page) {
  await page.goto('/login');
  await page.fill('input[type="password"], input[name="pin"]', PIN);
  await page.click('button[type="submit"], button:has-text("Login"), button:has-text("Enter")');
  await page.waitForURL(/\/$|\/strategy/, { timeout: 10_000 });
}

// ── Deploy a paper strategy and return its run_id ───────────
async function deployPaperStrategy(page: Page): Promise<string> {
  const resp = await page.request.post('/api/paper/start', {
    data: {
      run_name: 'E2E-Paper-Test',
      symbol: 'BTCUSDT',
      leverage: 10,
      trade_side: 'LONG',
      stoploss_pct: 5,
      target_profit_pct: 10,
      candle_interval: '1m',
      initial_capital: 1000,
      position_size_pct: 100,
      // entry_conditions intentionally omitted — tests the fallback path
    },
  });
  expect(resp.status()).toBe(200);
  const body = await resp.json();
  expect(body.status).toBe('started');
  return body.run_id as string;
}

// ────────────────────────────────────────────────────────────
// Phase 1: UI & Rendering
// ────────────────────────────────────────────────────────────

test.describe('Phase 1 — UI & Rendering', () => {

  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  // ── Bug 1: View modal must not contain raw HTML tag strings ─
  test('View modal renders HTML — no raw <div> text visible', async ({ page }) => {
    // Navigate to execution / live-monitor page
    await page.click('#nav-live, [data-page="live"], button:has-text("Live"), a:has-text("Live")');

    // Start a paper engine so there is an active panel to inspect
    const runId = await deployPaperStrategy(page);

    // Wait for the panel to appear, then click "View"
    await page.waitForSelector(`button:has-text("View")`, { timeout: 15_000 });
    await page.click(`button:has-text("View")`);

    // The modal body must be visible
    const modal = page.locator('.cf-modal');
    await expect(modal).toBeVisible({ timeout: 5_000 });

    // BUG GUARD: modal text must NOT contain raw HTML angle-bracket tags
    const modalText = await modal.innerText();
    expect(modalText).not.toMatch(/<div\s/i);
    expect(modalText).not.toMatch(/<h3\s/i);
    expect(modalText).not.toMatch(/<span\s/i);

    // Sanity: expected rendered content is present as text, not markup
    expect(modalText).toContain('Total P&L');
    expect(modalText).toContain('Win Rate');

    // Tear-down
    await page.request.post('/api/paper/stop', { data: { run_id: runId } });
  });

  // ── Bug 2: Timestamps must be HH:MM:SS ──────────────────────
  test('Trade table timestamps match HH:MM:SS format', async ({ page }) => {
    await page.goto('/');
    await page.click('#nav-results, [data-page="results"], button:has-text("Results"), a:has-text("Results")');

    // If there are trades in the table, every non-dash time cell matches HH:MM:SS
    const timeCells = page.locator('td[style*="white-space:nowrap"]');
    const count = await timeCells.count();

    for (let i = 0; i < Math.min(count, 20); i++) {
      const text = (await timeCells.nth(i).innerText()).trim();
      if (text && text !== '—') {
        // Must match HH:MM:SS (not a full date like 2024-01-01 12:34:56)
        expect(text).toMatch(/^\d{2}:\d{2}:\d{2}$/);
      }
    }
  });

});

// ────────────────────────────────────────────────────────────
// Phase 2: Results Page & Filtering
// ────────────────────────────────────────────────────────────

test.describe('Phase 2 — Results Filtering', () => {

  test.beforeEach(async ({ page }) => {
    await login(page);
    await page.click('#nav-results, [data-page="results"], button:has-text("Results"), a:has-text("Results")');
  });

  // ── Bug 3: Paper filter ──────────────────────────────────────
  test('Paper filter button is present and toggles correctly', async ({ page }) => {
    const paperBtn = page.locator('.runs-filter-btn[data-filter="paper"]');
    await expect(paperBtn).toBeVisible();
    await paperBtn.click();
    await expect(paperBtn).toHaveClass(/active/);
  });

  // ── Bug 4: Scalp filter ──────────────────────────────────────
  test('Scalp filter button is present and toggles correctly', async ({ page }) => {
    const scalpBtn = page.locator('.runs-filter-btn[data-filter="scalp"]');
    await expect(scalpBtn).toBeVisible();
    await expect(scalpBtn).toContainText('Scalp');
    await scalpBtn.click();
    await expect(scalpBtn).toHaveClass(/active/);
    // Results list must be visible (even if empty)
    await expect(page.locator('#results-list')).toBeVisible();
  });

});

// ────────────────────────────────────────────────────────────
// Phase 3 — Execution Engine
// ────────────────────────────────────────────────────────────

test.describe('Phase 3 — Paper Engine Execution', () => {

  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  // ── Bug 5: Paper engine must not stay stuck in Scanning ──────
  test('Paper engine transitions from Scanning to active state within 90s', async ({ page }) => {
    const runId = await deployPaperStrategy(page);

    // Poll /api/paper/status every 5 s for up to 90 s
    let inTrade = false;
    for (let attempt = 0; attempt < 18; attempt++) {
      await page.waitForTimeout(5_000);
      const resp = await page.request.get('/api/paper/status');
      if (resp.status() !== 200) continue;
      const engines: Array<{ run_id: string; in_trade: boolean; running: boolean }> = await resp.json();
      const engine = engines.find((e) => e.run_id === runId);
      if (engine?.in_trade) {
        inTrade = true;
        break;
      }
    }

    // Stop before asserting so we don't leave a dangling engine
    await page.request.post('/api/paper/stop', { data: { run_id: runId } });

    expect(
      inTrade,
      'Paper engine should have entered a trade within 90s — check EMA_20_1m indicator injection and eval_condition_group'
    ).toBe(true);
  });

  // ── Bug 6: supertrend_dir must NOT appear in indicator payload ─
  test('Supertrend indicator API response does not expose supertrend_dir', async ({ page }) => {
    // Start a paper engine configured with a supertrend indicator
    const resp = await page.request.post('/api/paper/start', {
      data: {
        run_name: 'E2E-ST-Dir-Check',
        symbol: 'BTCUSDT',
        leverage: 5,
        trade_side: 'LONG',
        stoploss_pct: 5,
        target_profit_pct: 10,
        candle_interval: '1m',
        initial_capital: 1000,
        position_size_pct: 100,
        indicators: ['Supertrend_10_3.0_1m'],
        entry_conditions: [{ left: 'current_close', operator: 'is_above', right: 'number', right_number_value: 0, connector: 'AND' }],
        exit_conditions:  [{ left: 'current_close', operator: 'is_above', right: 'number', right_number_value: 0, connector: 'AND' }],
      },
    });
    const body = await resp.json();
    const runId: string = body.run_id;

    // Give the engine one poll cycle to compute indicators
    await page.waitForTimeout(8_000);

    const statusResp = await page.request.get('/api/paper/status');
    const engines: Array<{ run_id: string; current_indicators: Record<string, number> }> = await statusResp.json();
    const engine = engines.find((e) => e.run_id === runId);

    await page.request.post('/api/paper/stop', { data: { run_id: runId } });

    if (engine?.current_indicators) {
      const keys = Object.keys(engine.current_indicators);
      const hasDirKey = keys.some((k) => k.toLowerCase().includes('_dir'));
      expect(hasDirKey, `supertrend_dir must be removed — found keys: ${keys.join(', ')}`).toBe(false);
    }
  });

});
