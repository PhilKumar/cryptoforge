import { test, expect, Page } from '@playwright/test';

/**
 * Trade alerts — the pop-up that has to still be there when you get back.
 *
 * The whole promise of this stack is the opposite of a toast: an entry, a
 * target or a stalled engine stays on screen until it is dismissed by hand,
 * and comes back on the next page load if it never was. A renderer typo or an
 * ack that silently fails would break exactly that promise, quietly, and the
 * only symptom is an alert you never saw — which is invisible by definition.
 *
 * The inbox API is intercepted throughout. Cascade runs against mainnet, so
 * this suite creates no campaign and no order; the alerts here are fixtures.
 */

const PIN = process.env.E2E_PIN || '123456';
const USER = process.env.E2E_USER || 'admin';

function alert(id: number, over: Record<string, unknown> = {}) {
  return {
    id,
    ts: '2026-07-31 15:42:10',
    epoch: 1785484330,
    kind: 'cascade_fill',
    title: `BTCUSDT — Entry filled #${id}`,
    body: 'Bought $412.50 at 61,240.00 on the turn (avg 61,105.20, TP 62,320.00)',
    level: 'info',
    symbol: 'BTCUSDT',
    mode: 'cascade',
    seen: false,
    ...over,
  };
}

/** Serve a fixed inbox and record every acknowledgement the page sends. */
async function stubInbox(page: Page, items: Record<string, unknown>[]) {
  const acks: Record<string, unknown>[] = [];
  // The suite shares one server, and a paper or scalp trade in a neighbouring
  // spec pushes a real alert down this socket. Left connected, the stack under
  // test would gain cards nothing here asked for.
  await page.routeWebSocket('**/ws', () => {});
  await page.route('**/api/notifications/ack', async (route) => {
    acks.push(JSON.parse(route.request().postData() || '{}'));
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ acknowledged: 1, unseen: 0 }),
    });
  });
  await page.route('**/api/notifications', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ items, unseen: items.length, total: items.length }),
    });
  });
  return acks;
}

async function login(page: Page) {
  await page.goto('/app');
  // Accounts, since 2026-08-17: username + password (the seeded admin's
  // password is CRYPTOFORGE_PIN, which is what E2E_PIN carries).
  await page.fill('#username-input', USER);
  await page.fill('#password-input', PIN);
  await page.click('#unlock-btn');
  await page.waitForSelector('.nav-tab', { timeout: 10_000 });
}

test.describe('Trade alerts', () => {
  test('unseen alerts render on load and do not time out', async ({ page }) => {
    await stubInbox(page, [alert(1, { level: 'success', title: 'SOLUSDT — Target hit' }), alert(2)]);
    await login(page);

    const stack = page.locator('#cf-alert-stack');
    await expect(stack).toBeVisible({ timeout: 10_000 });
    await expect(page.locator('.cf-alert-card')).toHaveCount(2);
    await expect(page.locator('#cf-alert-count')).toHaveText('2');
    await expect(page.locator('.cf-alert-card-title').first()).toHaveText('SOLUSDT — Target hit');

    // Well past the 4.2s a toast lives. Nothing here may disappear on its own.
    await page.waitForTimeout(6000);
    await expect(stack).toBeVisible();
    await expect(page.locator('.cf-alert-card')).toHaveCount(2);
  });

  test('"Got it" acknowledges one alert and leaves the rest standing', async ({ page }) => {
    const acks = await stubInbox(page, [alert(7), alert(8)]);
    await login(page);
    await expect(page.locator('.cf-alert-card')).toHaveCount(2);

    await page.locator('.cf-alert-card-ack').first().click();

    await expect(page.locator('.cf-alert-card')).toHaveCount(1);
    await expect(page.locator('#cf-alert-stack')).toBeVisible();
    expect(acks).toEqual([{ ids: [7] }]);
  });

  test('"Dismiss all" clears the stack', async ({ page }) => {
    const acks = await stubInbox(page, [alert(1), alert(2), alert(3)]);
    await login(page);
    await expect(page.locator('.cf-alert-card')).toHaveCount(3);

    await page.click('#cf-alert-dismiss-all');

    await expect(page.locator('#cf-alert-stack')).toBeHidden();
    expect(acks).toEqual([{ all: true }]);
  });

  test('a standing alert never blocks the site underneath it', async ({ page }) => {
    // The first cut of this panel sat over the nav row and swallowed clicks on
    // it. Because these alerts do not dismiss themselves, that made the whole
    // site unusable until each one was acknowledged — the kill switch too.
    await stubInbox(page, [alert(1), alert(2), alert(3)]);
    await login(page);
    await expect(page.locator('.cf-alert-card')).toHaveCount(3);

    for (const nav of ['#nav-builder', '#nav-market', '#nav-dashboard']) {
      await page.click(nav, { timeout: 5_000 });
    }
    await expect(page.locator('#dashboard-page')).toHaveClass(/active-page/);
    // Still standing: navigating away is not the same as having seen them.
    await expect(page.locator('.cf-alert-card')).toHaveCount(3);
  });

  test('a dismissed alert does not come back on the next inbox read', async ({ page }) => {
    // The inbox is re-read on every socket connect, on tab focus and every 45s,
    // and the server here keeps answering with the alert you just cleared —
    // exactly what a read already in flight when you clicked would return. The
    // dismissal is the deliberate act; a stale read must not undo it. This
    // raced in CI as "Got it leaves 2 cards standing" before it was fixed.
    await stubInbox(page, [alert(7), alert(8)]);
    await login(page);
    await expect(page.locator('.cf-alert-card')).toHaveCount(2);

    await page.locator('.cf-alert-card-ack').first().click();
    await expect(page.locator('.cf-alert-card')).toHaveCount(1);

    // Two more reads of the unchanged server inbox, the way a reconnect does.
    await page.evaluate(() => (window as any).cfLoadAlerts());
    await page.evaluate(() => (window as any).cfLoadAlerts());
    await expect(page.locator('.cf-alert-card')).toHaveCount(1);
    await expect(page.locator('.cf-alert-card-title')).toHaveText(
      'BTCUSDT — Entry filled #8');

    // Same promise for Dismiss all: the whole stack stays down.
    await page.click('#cf-alert-dismiss-all');
    await expect(page.locator('#cf-alert-stack')).toBeHidden();
    await page.evaluate(() => (window as any).cfLoadAlerts());
    await expect(page.locator('#cf-alert-stack')).toBeHidden();
  });

  test('an empty inbox shows nothing at all', async ({ page }) => {
    await stubInbox(page, []);
    await login(page);
    await expect(page.locator('#cf-alert-stack')).toBeHidden();
  });

  test('a live push adds a card without a reload, and cannot inject markup', async ({ page }) => {
    await stubInbox(page, []);
    // The first inbox poll fires 1.2s after boot and repaints the stack from
    // the (stubbed, empty) server. Push only after it has been and gone, or
    // the poll wipes the card this test is about — sign-in used to be six
    // keypad clicks slow enough to hide that; a username/password sign-in is not.
    const firstPoll = page.waitForResponse((r) => r.url().includes('/api/notifications') && r.request().method() === 'GET');
    await login(page);
    await firstPoll;

    await expect(page.locator('#cf-alert-stack')).toBeHidden();

    // What the WebSocket handler does when the engine raises an alert.
    const injected = '<img src=x onerror=alert(1)>ETHUSDT — Target hit';
    await page.evaluate((title) => {
      (window as any).cfPushAlert({
        id: 999999,
        ts: '2026-07-31 16:01:44',
        title,
        body: 'Round 3 closed at TP 3,180.00',
        level: 'success',
        mode: 'cascade',
        seen: false,
      });
    }, injected);

    await expect(page.locator('#cf-alert-stack')).toBeVisible();
    await expect(page.locator('.cf-alert-card')).toHaveCount(1);
    // Rendered as text, not parsed: the engine's strings are data.
    const card = page.locator('.cf-alert-card-title', { hasText: 'ETHUSDT — Target hit' });
    await expect(card).toHaveText(injected);
    expect(await page.locator('.cf-alert-card-title img').count()).toBe(0);
  });
});
