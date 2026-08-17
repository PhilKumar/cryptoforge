import { test, expect, Page } from '@playwright/test';

const PIN = process.env.E2E_PIN || '123456';
const USER = process.env.E2E_USER || 'admin';
const VIEWER_USER = 'e2e-viewer';
const VIEWER_PASSWORD = 'lookonly1';

/**
 * Accounts and the viewer role. Phil, 2026-08-17: "I need the same kinda
 * authentication for cryptoforge as well with username and password and
 * authentication... Also I want to add user from admin console." And earlier
 * the same day, for viewers: "Portfolio numbers can be visible here... Only
 * nothing can be started or stopped or no admin activities."
 *
 * The admin creates a viewer account from the Admin Console; the viewer signs
 * in with a username and password and gets a read-only session. The server
 * refuses every write by HTTP method and the admin reads by path (app.py
 * _viewer_may_call); the page arrives with `read-only-account` on <html>,
 * hides the controls that cannot work, and says "View only" in the top bar.
 * These tests check both halves, because either alone would be theatre.
 */

async function signIn(page: Page, username: string, password: string) {
  await page.goto('/app');
  await page.fill('#username-input', username);
  await page.fill('#password-input', password);
  await page.click('#unlock-btn');
  await page.waitForSelector('.nav-tab', { timeout: 10_000 });
  // The chip and the admin gear fill in from /api/auth/status after first
  // paint; read them only once that has landed, or the test races the page.
  await page.waitForFunction(() => {
    const role = document.getElementById('topbar-user-role');
    return !!role && role.textContent !== '\u2014' && role.textContent!.trim() !== '';
  }, undefined, { timeout: 10_000 });
}

async function csrfHeaders(page: Page) {
  const csrf = (await page.context().cookies()).find((c) => c.name === 'cryptoforge_csrf')?.value || '';
  return { 'X-CSRF-Token': csrf, 'X-Requested-With': 'XMLHttpRequest', 'Content-Type': 'application/json' };
}

// The admin creates (or re-creates) the viewer account through the console's
// own API, so the spec never depends on state left by an earlier run.
async function ensureViewerAccount(page: Page) {
  await signIn(page, USER, PIN);
  const headers = await csrfHeaders(page);
  const list = await (await page.request.get('/api/admin/users')).json();
  const existing = (list.users || []).find((u: any) => u.username === VIEWER_USER);
  if (existing) {
    const reset = await page.request.put(`/api/admin/users/${existing.id}/password`, { headers, data: { password: VIEWER_PASSWORD } });
    expect(reset.status(), 'reset the viewer password').toBe(200);
    if (!existing.is_active) {
      const toggle = await page.request.put(`/api/admin/users/${existing.id}/toggle`, { headers, data: {} });
      expect(toggle.status()).toBe(200);
    }
  } else {
    const created = await page.request.post('/api/admin/users', { headers, data: { username: VIEWER_USER, password: VIEWER_PASSWORD, role: 'viewer' } });
    expect(created.status(), 'create the viewer account').toBe(200);
  }
  await page.request.post('/api/auth/logout', { headers, data: {} });
}

test.beforeEach(async ({ page }) => {
  await ensureViewerAccount(page);
});

test('an account the admin created signs in as a viewer and the page says so', async ({ page }) => {
  await signIn(page, VIEWER_USER, VIEWER_PASSWORD);
  const seen = await page.evaluate(() => ({
    readOnly: document.documentElement.classList.contains('read-only-account'),
    chip: getComputedStyle(document.getElementById('topbar-viewer-chip')!).display,
    chipText: document.getElementById('topbar-viewer-chip')!.textContent!.trim(),
    who: document.getElementById('topbar-username')!.textContent + ' ' + document.getElementById('topbar-user-role')!.textContent,
    admin: (document.getElementById('topbar-admin-btn') as HTMLElement).hidden,
    kill: getComputedStyle(document.getElementById('kill-switch-btn')!).display,
    hidden: [...document.querySelectorAll('.read-only-hide')].filter((e) => getComputedStyle(e).display === 'none').length,
    total: document.querySelectorAll('.read-only-hide').length,
  }));
  expect(seen.readOnly, 'html.read-only-account is set by the server in the shell').toBe(true);
  expect(seen.chip).not.toBe('none');
  expect(seen.chipText).toContain('View');
  expect(seen.who).toBe(`${VIEWER_USER} VIEWER`);
  expect(seen.admin, 'the admin console button is not offered').toBe(true);
  expect(seen.kill, 'the kill switch is not offered').toBe('none');
  expect(seen.total).toBeGreaterThan(10);
  expect(seen.hidden).toBe(seen.total);
});

test('a viewer sees the portfolio numbers and is refused every write', async ({ page }) => {
  await signIn(page, VIEWER_USER, VIEWER_PASSWORD);

  // Reads pass — the balances are Phil's call to show, and he chose to.
  for (const path of ['/api/portfolio/summary', '/api/strategies', '/api/live/status', '/api/notifications', '/api/user/profile']) {
    const res = await page.request.get(path);
    expect(res.status(), `${path} must be readable`).not.toBe(403);
    expect(res.status()).not.toBe(401);
  }

  // Writes are refused by METHOD, with one sentence and one code.
  const headers = await csrfHeaders(page);
  for (const [method, path] of [
    ['post', '/api/live/start'],
    ['post', '/api/paper/start'],
    ['post', '/api/emergency-stop'],
    ['post', '/api/orders/place'],
    ['put', '/api/admin/config'],
    ['post', '/api/admin/users'],
  ] as const) {
    const res = await page.request[method](path, { headers, data: {} });
    expect(res.status(), `${method.toUpperCase()} ${path}`).toBe(403);
    expect((await res.json()).code).toBe('viewer_read_only');
  }

  // Admin reads are refused by PATH.
  for (const path of ['/api/admin/config', '/api/admin/users', '/api/ops/state/backup', '/api/audit/production-readiness']) {
    const res = await page.request.get(path);
    expect(res.status(), `GET ${path}`).toBe(403);
  }
});

test('a viewer may still change their own password and it takes effect', async ({ page }) => {
  await signIn(page, VIEWER_USER, VIEWER_PASSWORD);
  const headers = await csrfHeaders(page);
  const changed = await page.request.put('/api/user/password', { headers, data: { current_password: VIEWER_PASSWORD, new_password: 'lookonly2' } });
  expect(changed.status(), 'own password is the one write a viewer owns').toBe(200);
  await page.request.post('/api/auth/logout', { headers, data: {} });
  await page.goto('/app');
  await page.fill('#username-input', VIEWER_USER);
  await page.fill('#password-input', VIEWER_PASSWORD);
  await page.click('#unlock-btn');
  await expect(page.locator('#unlock-status')).toContainText('Invalid');
  await signIn(page, VIEWER_USER, 'lookonly2');
  // Put it back for the next test in this file.
  const back = await page.request.put('/api/user/password', { headers: await csrfHeaders(page), data: { current_password: 'lookonly2', new_password: VIEWER_PASSWORD } });
  expect(back.status()).toBe(200);
});

test('a refused action is said in the page, once', async ({ page }) => {
  await signIn(page, VIEWER_USER, VIEWER_PASSWORD);
  // Through the dispatcher, the way a JS-rendered Stop button would arrive.
  await page.evaluate(() => (window as any)._cfInvokeNamedFunction('stopEngine', ['none', 'paper']));
  const toast = page.locator('#toast-container .cf-toast-msg').first();
  await expect(toast).toContainText('View-only access');
});

test('the admin still opens a full session, and a disabled viewer cannot sign in', async ({ page }) => {
  await signIn(page, USER, PIN);
  const seen = await page.evaluate(() => ({
    readOnly: document.documentElement.classList.contains('read-only-account'),
    chip: getComputedStyle(document.getElementById('topbar-viewer-chip')!).display,
    admin: (document.getElementById('topbar-admin-btn') as HTMLElement).hidden,
    who: document.getElementById('topbar-user-role')!.textContent,
  }));
  expect(seen.readOnly).toBe(false);
  expect(seen.chip).toBe('none');
  expect(seen.admin).toBe(false);
  expect(seen.who).toBe('ADMIN');
  const users = await (await page.request.get('/api/admin/users')).json();
  const viewer = users.users.find((u: any) => u.username === VIEWER_USER);
  const headers = await csrfHeaders(page);
  expect((await page.request.put(`/api/admin/users/${viewer.id}/toggle`, { headers, data: {} })).status()).toBe(200);
  await page.request.post('/api/auth/logout', { headers, data: {} });
  await page.goto('/app');
  await page.fill('#username-input', VIEWER_USER);
  await page.fill('#password-input', VIEWER_PASSWORD);
  await page.click('#unlock-btn');
  await expect(page.locator('#unlock-status')).toContainText('disabled');
});
