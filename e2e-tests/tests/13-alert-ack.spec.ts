import { test, expect, Page } from '@playwright/test';

/**
 * "Got it" on a trade alert must never look dead.
 *
 * Reported 2026-09-08: three clicks, nothing happened. The nginx log settled
 * it — at that moment there was NO ack request at all, and the two acks that
 * did leave the browser during the same period came back 499, the client
 * giving up mid-flight. The site was stalling for other reasons that day, and
 * `cfAckAlerts` had no deadline: a fetch that never resolves leaves the handler
 * awaiting forever, the card in place, and the button indistinguishable from
 * one that was never wired up.
 *
 * The card is SUPPOSED to survive a failed ack — seen-state belongs to the
 * server and an alert must not vanish on a promise that was never kept. So the
 * fix is not optimistic removal; it is that the button always answers.
 */

const PIN = process.env.E2E_PIN || '123456';
const USER = process.env.E2E_USER || 'admin';

const ALERTS = [1, 2, 3].map((id) => ({
  id,
  ts: '2026-09-08 09:45:08',
  epoch: 1788855236,
  kind: 'cascade_start',
  title: `Cascade-Hybrid · PAXGUSDT #52${id}`,
  body: 'mother candle broke above.',
  level: 'warn',
  symbol: 'PAXGUSDT',
  mode: 'CASCADE',
  seen: false,
}));

async function login(page: Page) {
  // Serve the inbox ourselves: this suite is about the button, not the engine,
  // and it must never depend on a real alert existing.
  await page.route('**/api/notifications', (route) =>
    route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ items: ALERTS, unseen: ALERTS.length, total: ALERTS.length }),
    }),
  );
  await page.goto('/app');
  await page.fill('#username-input', USER);
  await page.fill('#password-input', PIN);
  await page.click('#unlock-btn');
  await page.waitForSelector('.nav-tab', { timeout: 15_000 });
  await expect(page.locator('.cf-alert-card')).toHaveCount(3, { timeout: 10_000 });
}

test.describe('Trade alerts — Got it', () => {
  test('a good ack clears the card', async ({ page }) => {
    await login(page);
    await page.route('**/api/notifications/ack', (route) =>
      route.fulfill({ status: 200, contentType: 'application/json', body: '{"acknowledged":1,"unseen":2}' }),
    );
    await page.locator('.cf-alert-card-ack').first().click();
    await expect(page.locator('.cf-alert-card')).toHaveCount(2);
  });

  test('a refused ack keeps the card AND revives the button', async ({ page }) => {
    await login(page);
    await page.route('**/api/notifications/ack', (route) => route.fulfill({ status: 500, body: '{}' }));
    await page.locator('.cf-alert-card-ack').first().click();

    // The card must stay: seen-state is the server's, not the browser's.
    await expect(page.locator('.cf-alert-card')).toHaveCount(3);
    // But the button must be usable again, or the card is stuck forever.
    const btn = page.locator('.cf-alert-card-ack').first();
    await expect(btn).toHaveText('Got it');
    await expect(btn).toBeEnabled();
  });

  test('a hung server does not leave the button dead', async ({ page }) => {
    test.setTimeout(60_000);
    await login(page);
    // Never respond — the exact condition that produced the report.
    await page.route('**/api/notifications/ack', () => {});
    const btn = page.locator('.cf-alert-card-ack').first();
    await btn.click();

    // Immediate feedback, so it never reads as an unwired button.
    await expect(btn).toHaveText('...');

    // And it comes back on its own rather than waiting for a reload.
    await expect(page.locator('.cf-alert-card-ack').first()).toHaveText('Got it', { timeout: 20_000 });
    await expect(page.locator('.cf-alert-card-ack').first()).toBeEnabled();
    await expect(page.locator('.cf-alert-card')).toHaveCount(3);
  });
});
