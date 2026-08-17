import { test, expect, Page } from '@playwright/test';

const PIN = process.env.E2E_PIN || '123456';
const USER = process.env.E2E_USER || 'admin';

async function login(page: Page) {
  await page.goto('/app');
  // Accounts, since 2026-08-17: username + password (the seeded admin's
  // password is CRYPTOFORGE_PIN, which is what E2E_PIN carries).
  await page.fill('#username-input', USER);
  await page.fill('#password-input', PIN);
  await page.click('#unlock-btn');
  await page.waitForSelector('.nav-tab', { timeout: 10_000 });
}

/**
 * Signing out puts you at the front door, never back at the keypad.
 *
 * This broke silently once and would again: the moment the cookie is dropped,
 * every poller still in flight comes back 401, and the expired-session handler
 * used to win the race and send the browser to /app — the unlock screen the
 * person had just chosen to leave. The destination is the assertion.
 */
test('signing out lands on the landing page, not the unlock screen', async ({ page }) => {
  await login(page);
  await page.click('[data-cf-click="doLogout()"]');

  await page.waitForURL((url) => new URL(url).pathname === '/', { timeout: 15_000 });

  const landed = await page.evaluate(() => ({
    path: location.pathname,
    epigraph: !!document.querySelector('.epi'),
    keypad: !!document.querySelector('#unlock-btn'),
    navTabs: document.querySelectorAll('.nav-tab').length,
  }));
  expect(landed.path).toBe('/');
  expect(landed.epigraph).toBe(true);
  expect(landed.keypad).toBe(false);
  expect(landed.navTabs).toBe(0);

  // The session is genuinely gone — the landing is not just a page we painted
  // over a live session.
  const res = await page.request.get('/api/cascade/status');
  expect([401, 403]).toContain(res.status());
});
