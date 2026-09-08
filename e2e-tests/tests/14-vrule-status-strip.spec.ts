import { test, expect, Page } from '@playwright/test';

/**
 * The V-Rule page says what the book is worth, and what each ladder is doing.
 *
 * Two things reported together on 2026-09-08, looking at a screen of fifteen
 * ladder cards:
 *
 *   "why so many ladders? ... only the armed or the current ones"
 *   "I am not seeing any status of the live trade like capital, profit booked"
 *
 * The filter was already right — 15 of 42 live ladders, the other 27 merely
 * watching and correctly hidden. What was wrong is that every one of the 13
 * ladders HOLDING coin displayed the pill "Waiting", because the driver fills
 * its campaigns itself and the cascade state machine underneath never leaves
 * WAITING_FIRST_DEPTH. A list of ladders all labelled "Waiting" reads as a list
 * of ladders doing nothing.
 *
 * And the page had every table and no headline: no purse, nothing committed,
 * nothing booked — the numbers Cascade-Auto has carried all along.
 */

const PIN = process.env.E2E_PIN || '123456';
const USER = process.env.E2E_USER || 'admin';

function campaign(seq: number, over: Record<string, unknown> = {}) {
  return {
    campaign_id: 'c' + seq,
    seq,
    symbol: 'BTCUSDT',
    mode: 'paper',
    state: 'WAITING_FIRST_DEPTH', // the driver never advances it — that is the point
    closed_at: '',
    filled_base_qty: 0,
    spent_usd: 0,
    pending_usd: 0,
    timeframe: '5m',
    mc_kind: 'major',
    rounds: [],
    all_fills: [],
    ...over,
  };
}

const STATUS = {
  books: [
    {
      symbol: 'BTCUSDT', exchange: 'binance', mode: 'paper', enabled: true,
      purse_usd: 2000, pocket_usd: 37.5, wallet_cap_usd: 1000,
      fold_threshold_usd: 500, in_coin_usd: 121, campaigns: 3,
    },
  ],
  exchanges: [], rules: {}, closed_campaigns: [],
  campaigns: [
    campaign(602, { filled_base_qty: 0.004, spent_usd: 55, all_fills: [{ price: 1, quantity: 1 }] }),
    campaign(636, { pending_usd: 12 }),
    campaign(700), // pure watcher — must not be listed
  ],
};

async function openVRule(page: Page) {
  await page.route('**/api/vrule/live/status', (route) =>
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(STATUS) }),
  );
  await page.goto('/app');
  await page.fill('#username-input', USER);
  await page.fill('#password-input', PIN);
  await page.click('#unlock-btn');
  await page.waitForSelector('.nav-tab', { timeout: 15_000 });
  // The nav exists before the app script finishes: showPage is not defined yet
  // and the evaluate throws. Wait for the function, not the markup.
  await page.waitForFunction(() => typeof (window as any).showPage === 'function');
  await page.evaluate(() => (window as any).showPage('rule3070-page', document.getElementById('nav-strategies')));
  await expect(page.locator('#cf-vr-campaigns .cf-cascade-card')).toHaveCount(2, { timeout: 10_000 });
}

test.describe('V-Rule — the headline and the pills', () => {
  test('the strip reports purse, committed, booked and working', async ({ page }) => {
    await openVRule(page);
    await expect(page.locator('#cf-vr-stat-purse')).toHaveText('$2,000.00');
    await expect(page.locator('#cf-vr-stat-incoin')).toHaveText('$121.00');
    await expect(page.locator('#cf-vr-stat-incoin-sub')).toContainText('$1,000.00 limit');
    await expect(page.locator('#cf-vr-stat-pocket')).toHaveText('$37.50');
    await expect(page.locator('#cf-vr-stat-pocket-sub')).toContainText('folds at $500.00');
    await expect(page.locator('#cf-vr-stat-lines')).toHaveText('2');
    await expect(page.locator('#cf-vr-stat-lines-sub')).toContainText('1 holding coin');
  });

  test('a ladder holding coin does NOT say Waiting', async ({ page }) => {
    await openVRule(page);
    const held = page.locator('#cf-vr-campaigns .cf-cascade-card').first();
    await expect(held).toContainText('Holding');
    await expect(held).not.toContainText('Waiting');
  });

  test('a ladder with money committed reads Armed', async ({ page }) => {
    await openVRule(page);
    const armed = page.locator('#cf-vr-campaigns .cf-cascade-card').nth(1);
    await expect(armed).toContainText('Armed');
    await expect(armed).not.toContainText('Waiting');
  });

  test('a ladder merely watching is still not listed', async ({ page }) => {
    await openVRule(page);
    await expect(page.locator('#cf-vr-campaigns')).not.toContainText('#700');
    await expect(page.locator('#cf-vr-watching')).toContainText('1 more');
  });
});
