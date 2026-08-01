import { test, expect, Page } from '@playwright/test';

/**
 * The closed-rounds table on a live campaign card.
 *
 * This had no coverage, and a bug shipped straight through: a hand-built pager
 * added under the table read `rFrom`/`rPage`/`rPages`, none of which were ever
 * declared. `_cfCascadePositionPanel` therefore threw ReferenceError for any
 * campaign holding a closed round, and the `mount.innerHTML` that follows it
 * never ran — the entire live campaign list rendered blank. It stayed invisible
 * only because no active campaign had a closed round at the time, so the first
 * take-profit to land would have blanked the panel.
 *
 * Two things are asserted, because they fail differently:
 *
 *  1. The panel RENDERS at all with rounds present — a page error here means
 *     the card list is gone, not merely that one number is wrong.
 *  2. The round is reported NET of commission, with the fee shown beside it.
 *     Cascade booked gross P&L until fees were modelled (AUDIT §1.2), so a
 *     round that silently loses its fee column has regressed to the old number.
 *
 * Like 06, this suite CREATES NOTHING. Cascade trades mainnet; the campaign
 * here is a fixture served over an intercepted status call.
 */

const PIN = process.env.E2E_PIN || '123456';
const T0 = 1750000000;

async function login(page: Page) {
  await page.goto('/');
  for (const digit of PIN.split('')) {
    await page.click(`button.key[data-val="${digit}"]`);
  }
  await page.waitForSelector('.nav-tab', { timeout: 10_000 });
  await page.waitForFunction(() => typeof (window as any).cfLoadCascadeStatus === 'function');
}

const NOISE = /Failed to load resource|favicon|fonts\.googleapis|manifest|service worker|WebSocket|net::ERR_/i;

function watchErrors(page: Page) {
  const errors: string[] = [];
  page.on('pageerror', (err) => errors.push(`pageerror: ${err.message}`));
  page.on('console', (msg) => {
    if (msg.type() === 'error') errors.push(`console: ${msg.text()}`);
  });
  return () => errors.filter((e) => !NOISE.test(e));
}

/** One round, in the shape engine/cascade.py Round.to_dict() returns. */
function round_(id: number, opts: { fees?: number } = {}) {
  const gross = 10.0;
  const fees = opts.fees ?? 0.21;
  return {
    round_id: id,
    leg_id: 1,
    avg_entry: 100.0,
    quantity: 1.0,
    invested_usd: 100.0,
    exit_price: 110.0,
    pnl: Number((gross - fees).toFixed(8)),
    pnl_gross: gross,
    fees_usd: fees,
    closed_at: '2026-08-01 12:00:00',
    opened_ts: T0,
    closed_ts: T0 + 3600,
    fills: [{ timestamp: T0, price: 100.0, quantity: 1.0, usd: 100.0, level: 2, leg_id: 1, order_id: 'x' }],
  };
}

function statusWith(rounds: Array<Record<string, unknown>>) {
  const fees = rounds.reduce((s, r) => s + Number(r.fees_usd || 0), 0);
  const pnl = rounds.reduce((s, r) => s + Number(r.pnl || 0), 0);
  return {
    status: 'ok',
    running: true,
    active_count: 1,
    campaigns: [
      {
        campaign_id: 'e2e-rounds-fixture',
        seq: 1,
        symbol: 'SOLUSDT',
        mode: 'paper',
        state: 'TRENDLINE_ACTIVE',
        mc_kind: 'major',
        timeframe: '5m',
        capital_usd: 1000,
        mother_high: 120.0,
        mother_low: 95.0,
        mother_timestamp: T0,
        legs: [],
        all_fills: [],
        rounds,
        rounds_closed: rounds.length,
        realized_pnl_total: Number(pnl.toFixed(2)),
        fees_total: Number(fees.toFixed(4)),
        spent_usd: 0,
        resting_usd: 0,
        pending_usd: 0,
        last_price: 105.0,
        model_version: 21,
        stale_model: false,
      },
    ],
    closed_campaigns: [],
    instruments: {},
    capital_groups: {},
  };
}

async function serveStatus(page: Page, payload: Record<string, unknown>) {
  await page.route('**/api/cascade/status', (route) =>
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(payload) }),
  );
}

/**
 * Put the fixture on screen for real: the Cascade page has to be the visible
 * section, and cards are collapsed until clicked. Asserting against a card that
 * is merely in the DOM would pass on markup nobody can see, which is most of
 * what went wrong here in the first place.
 */
async function showCampaign(page: Page) {
  await page.evaluate(() => (window as any).showPage(
    'cascade-page', (window as any).cfNavButtonForPage('cascade-page'), { skipHistory: true },
  ));
  await page.evaluate(() => (window as any).cfLoadCascadeStatus(false));
  await expect(page.locator('#cf-cascade-campaigns .cf-cascade-card')).toHaveCount(1);
  // Toggled through the function rather than a click on the header: the header
  // is mostly taken up by the scrolling title strip, which swallows the event.
  // Whether that strip should be clickable is 06's business, not this file's.
  await page.evaluate(() => (window as any).cfCascadeToggleCard('e2e-rounds-fixture'));
  await expect(page.locator('#cf-cascade-campaigns .cf-cascade-card.is-open')).toHaveCount(1);
}

test.describe('Cascade closed-rounds table', () => {
  test('a campaign holding closed rounds still renders its card', async ({ page }) => {
    const errors = watchErrors(page);
    await login(page);
    await serveStatus(page, statusWith([round_(1), round_(2)]));

    // The whole point: the mount has content. Before the fix this was empty,
    // because the render threw before ever assigning innerHTML.
    await showCampaign(page);
    await expect(page.locator('#cf-cascade-campaigns table.cf-cascade-rounds')).toBeVisible();
    expect(errors(), 'rendering a card with rounds raises nothing').toEqual([]);
  });

  test('rounds report P&L net of commission, with the fee beside it', async ({ page }) => {
    await login(page);
    await serveStatus(page, statusWith([round_(1)]));
    await showCampaign(page);

    const table = page.locator('#cf-cascade-campaigns table.cf-cascade-rounds');
    await expect(table.locator('thead th', { hasText: 'Fees' })).toHaveCount(1);
    // $10.00 gross less $0.21 — the net figure is what the row must show.
    await expect(table.locator('tbody tr').first()).toContainText('+$9.79');
    await expect(table.locator('tbody tr').first()).toContainText('-$0.21');
    await expect(page.locator('#cf-cascade-campaigns .cf-cascade-position.is-closed'))
      .toContainText('after $0.21 fees');
  });

  test('a round closed before fees were recorded reads as unknown, not as free', async ({ page }) => {
    await login(page);
    // No fees_usd/pnl_gross at all — exactly how rounds written by the older
    // engine restore. Showing "$0.00" would claim it paid no commission.
    const legacy = { ...round_(1), pnl: 10.0 };
    delete (legacy as Record<string, unknown>).fees_usd;
    delete (legacy as Record<string, unknown>).pnl_gross;
    await serveStatus(page, statusWith([legacy]));
    await showCampaign(page);

    const row = page.locator('#cf-cascade-campaigns table.cf-cascade-rounds tbody tr').first();
    await expect(row).toContainText('+$10.00');
    await expect(row).not.toContainText('-$0.00');
    await expect(page.locator('#cf-cascade-campaigns .cf-cascade-position.is-closed'))
      .not.toContainText('fees');
  });
});
