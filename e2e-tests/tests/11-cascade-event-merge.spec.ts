import { test, expect, Page } from '@playwright/test';

/**
 * The Cascade Event Log after ended campaigns stopped carrying their own log.
 *
 * `/api/cascade/status` was shipping 3.75 MB every 3 seconds — 459 of its 463
 * campaigns had already ended and each still carried its full `event_log`,
 * 5,567 of the 5,721 lines. Those are gone from the payload now, and the page
 * reads the engine's persisted log from a new top-level `events` key instead.
 *
 * That makes the panel a MERGE of two sources, which is exactly where it can
 * go wrong, so all three failure modes are covered here:
 *
 *  1. Persisted lines must actually render — dropping event_log without
 *     reading `events` would leave the panel empty, which is the whole risk.
 *  2. A line in BOTH sources must appear once. Working campaigns still carry
 *     their event_log and those same lines are also in the persisted log.
 *  3. Turning the page must not lose them. The pager re-renders from remembered
 *     state, and that state held only `campaigns` — so paging would have
 *     silently dropped every persisted line on the second page.
 *
 * Like 06 and 08, this suite CREATES NOTHING: Cascade trades mainnet, and the
 * payload here is a fixture served over an intercepted status call.
 */

const PIN = process.env.E2E_PIN || '123456';
const USER = process.env.E2E_USER || 'admin';

async function login(page: Page) {
  await page.goto('/app');
  await page.fill('#username-input', USER);
  await page.fill('#password-input', PIN);
  await page.click('#unlock-btn');
  await page.waitForSelector('.nav-tab', { timeout: 10_000 });
  await page.waitForFunction(() => typeof (window as any).cfLoadCascadeStatus === 'function');
}

function evt(ts: string, message: string, campaignId = 'c-ended') {
  return { timestamp: ts, campaign_id: campaignId, level: 'info', message, symbol: 'BTCUSDT' };
}

/** An ended campaign, arriving the way the server now sends one: no event_log. */
function endedCampaign() {
  return {
    campaign_id: 'c-ended',
    symbol: 'BTCUSDT',
    seq: 1,
    mode: 'paper',
    state: 'COMPLETED',
    closed_at: '2026-09-08 10:00:00',
    rounds: [],
    all_fills: [],
    rounds_closed: 0,
    realized_pnl_total: 0,
    fees_total: 0,
    spent_usd: 0,
    resting_usd: 0,
    pending_usd: 0,
    last_price: 100.0,
    mother_high: 110.0,
    mother_low: 90.0,
    model_version: 21,
    stale_model: false,
  };
}

async function serve(page: Page, payload: Record<string, unknown>) {
  await page.route('**/api/cascade/status', (route) =>
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(payload) }),
  );
}

async function showCascade(page: Page) {
  await page.evaluate(() => (window as any).showPage(
    'cascade-page', (window as any).cfNavButtonForPage('cascade-page'), { skipHistory: true },
  ));
  await page.evaluate(() => (window as any).cfLoadCascadeStatus(false));
}

test.describe('Cascade event log — two sources, one list', () => {
  test('a persisted line renders even though no campaign carries it', async ({ page }) => {
    await login(page);
    await serve(page, {
      status: 'ok',
      running: true,
      campaigns: [endedCampaign()],
      closed_campaigns: [],
      instruments: {},
      capital_groups: {},
      events: [evt('2026-09-08 09:00:00', 'persisted-line-one')],
    });
    await showCascade(page);
    await expect(page.locator('#cf-cascade-events')).toContainText('persisted-line-one');
  });

  test('a line in both sources is listed once', async ({ page }) => {
    await login(page);
    const shared = evt('2026-09-08 09:30:00', 'shared-line', 'c-working');
    await serve(page, {
      status: 'ok',
      running: true,
      // A working campaign still carries its own log, and the persisted log
      // holds the same line — the overlap this dedupe exists for.
      campaigns: [{ ...endedCampaign(), campaign_id: 'c-working', state: 'TRENDLINE_ACTIVE', closed_at: '', event_log: [shared] }],
      closed_campaigns: [],
      instruments: {},
      capital_groups: {},
      events: [shared],
    });
    await showCascade(page);
    const mount = page.locator('#cf-cascade-events');
    await expect(mount).toContainText('shared-line');
    const count = await mount.evaluate(
      (el) => (el.textContent || '').split('shared-line').length - 1,
    );
    expect(count).toBe(1);
  });

  test('turning the page keeps the persisted lines', async ({ page }) => {
    await login(page);
    // 50 persisted lines over a page size of 40, so page 2 exists and is made
    // ENTIRELY of persisted lines — the exact thing the old pager dropped.
    const many = Array.from({ length: 50 }, (_, i) =>
      evt(`2026-09-08 09:${String(i).padStart(2, '0')}:00`, `persisted-${i}`),
    );
    await serve(page, {
      status: 'ok',
      running: true,
      campaigns: [endedCampaign()],
      closed_campaigns: [],
      instruments: {},
      capital_groups: {},
      events: many,
    });
    await showCascade(page);
    await expect(page.locator('#cf-cascade-events')).toContainText('persisted-49');

    // Turn the page and read the DOM in the SAME evaluate. The status poll
    // re-renders every 3 seconds WITH the events, so asserting afterwards lets
    // the poll repair the very thing under test — checked, and it did: this
    // test passed against the unfixed code until it was made synchronous.
    const afterPaging = await page.evaluate(() => {
      (window as any).cfCascadeEventsPage(1, 'cf-cascade-events');
      return document.getElementById('cf-cascade-events')?.textContent || '';
    });
    expect(afterPaging).toContain('persisted-0');
    expect(afterPaging).not.toContain('No events yet');
  });
});
