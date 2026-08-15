import { test, expect, Page } from '@playwright/test';

const PIN = process.env.E2E_PIN || '123456';
const VIEWER_PIN = process.env.E2E_VIEWER_PIN || '777777';

/**
 * The viewer door. Phil, 2026-08-17: "Portfolio numbers can be visible here...
 * Only nothing can be started or stopped or no admin activities."
 *
 * A second PIN opens a read-only session. The server refuses every write by
 * HTTP method and the admin reads by path (app.py _viewer_may_call); the page
 * arrives with `read-only-account` on <html>, hides the controls that cannot
 * work, and says "View only" in the top bar. These tests check both halves —
 * the server's refusal and the page's honesty — because either alone would
 * be theatre.
 *
 * Needs CRYPTOFORGE_VIEWER_PIN set on the server (the workflow sets 777777);
 * without it the switch never appears and the whole file skips itself.
 */

async function keypad(page: Page, digits: string) {
  for (const digit of digits.split('')) await page.click(`button.key[data-val="${digit}"]`);
}

async function loginAsViewer(page: Page) {
  await page.goto('/app');
  const mode = page.locator('#unlock-mode');
  await expect(mode, 'the viewer switch must appear when the server has a viewer PIN').toBeVisible({ timeout: 10_000 });
  await mode.click();
  await expect(page.locator('.unlock-sub')).toHaveText(/view-only/i);
  await keypad(page, VIEWER_PIN);
  await page.waitForSelector('.nav-tab', { timeout: 10_000 });
}

test.beforeEach(async ({ request }) => {
  const status = await (await request.get('/api/auth/status')).json();
  test.skip(!status.viewer_login_enabled, 'server has no CRYPTOFORGE_VIEWER_PIN — the door is closed');
});

test('the switch is offered only when a viewer PIN exists, and takes the viewer PIN without a code', async ({ page }) => {
  await loginAsViewer(page);
  const seen = await page.evaluate(() => ({
    readOnly: document.documentElement.classList.contains('read-only-account'),
    chip: getComputedStyle(document.getElementById('topbar-viewer-chip')!).display,
    chipText: document.getElementById('topbar-viewer-chip')!.textContent!.trim(),
    admin: getComputedStyle(document.getElementById('topbar-admin-btn')!).display,
    kill: getComputedStyle(document.getElementById('kill-switch-btn')!).display,
    hidden: [...document.querySelectorAll('.read-only-hide')].filter((e) => getComputedStyle(e).display === 'none').length,
    total: document.querySelectorAll('.read-only-hide').length,
  }));
  expect(seen.readOnly, 'html.read-only-account is set by the server in the shell').toBe(true);
  expect(seen.chip).not.toBe('none');
  expect(seen.chipText).toContain('View');
  expect(seen.admin, 'the admin console button is not offered').toBe('none');
  expect(seen.kill, 'the kill switch is not offered').toBe('none');
  expect(seen.total).toBeGreaterThan(10);
  expect(seen.hidden).toBe(seen.total);
});

test('a viewer sees the portfolio numbers and is refused every write', async ({ page }) => {
  await loginAsViewer(page);

  // Reads pass — the balances are Phil's call to show, and he chose to.
  for (const path of ['/api/portfolio/summary', '/api/strategies', '/api/live/status', '/api/notifications']) {
    const res = await page.request.get(path);
    expect(res.status(), `${path} must be readable`).not.toBe(403);
    expect(res.status()).not.toBe(401);
  }

  // Writes are refused by METHOD, with one sentence and one code.
  const csrf = (await page.context().cookies()).find((c) => c.name === 'cryptoforge_csrf')?.value || '';
  const headers = { 'X-CSRF-Token': csrf, 'X-Requested-With': 'XMLHttpRequest', 'Content-Type': 'application/json' };
  for (const [method, path] of [
    ['post', '/api/live/start'],
    ['post', '/api/paper/start'],
    ['post', '/api/emergency-stop'],
    ['post', '/api/orders/place'],
    ['put', '/api/admin/config'],
  ] as const) {
    const res = await page.request[method](path, { headers, data: {} });
    expect(res.status(), `${method.toUpperCase()} ${path}`).toBe(403);
    expect((await res.json()).code).toBe('viewer_read_only');
  }

  // Admin reads are refused by PATH.
  for (const path of ['/api/admin/config', '/api/ops/state/backup', '/api/audit/production-readiness']) {
    const res = await page.request.get(path);
    expect(res.status(), `GET ${path}`).toBe(403);
  }
});

test('a refused action is said in the page, once', async ({ page }) => {
  await loginAsViewer(page);
  // Through the dispatcher, the way a JS-rendered Stop button would arrive.
  await page.evaluate(() => (window as any)._cfInvokeNamedFunction('stopEngine', ['none', 'paper']));
  const toast = page.locator('#toast-container .cf-toast-msg').first();
  await expect(toast).toContainText('View-only access');
});

test('the unlock PIN still opens a full session with no viewer marks', async ({ page }) => {
  await page.goto('/app');
  await keypad(page, PIN);
  await page.waitForSelector('.nav-tab', { timeout: 10_000 });
  const seen = await page.evaluate(() => ({
    readOnly: document.documentElement.classList.contains('read-only-account'),
    chip: getComputedStyle(document.getElementById('topbar-viewer-chip')!).display,
    admin: getComputedStyle(document.getElementById('topbar-admin-btn')!).display,
  }));
  expect(seen.readOnly).toBe(false);
  expect(seen.chip).toBe('none');
  expect(seen.admin).not.toBe('none');
  const res = await page.request.get('/api/admin/health');
  expect(res.status()).toBe(200);
});
