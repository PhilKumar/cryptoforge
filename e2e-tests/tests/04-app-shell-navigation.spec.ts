import { expect, Page, test } from '@playwright/test';

const PIN = process.env.E2E_PIN || '123456';

async function login(page: Page) {
  await page.goto('/');
  for (const digit of PIN.split('')) {
    await page.click(`button.key[data-val="${digit}"]`);
  }
  await page.waitForSelector('.nav-tab', { timeout: 10_000 });
}

async function expectActivePage(page: Page, pageId: string, navId: string) {
  await expect(page.locator('#' + pageId)).toHaveClass(/active-page/, { timeout: 10_000 });
  await expect(page.locator('#' + navId)).toHaveClass(/active/, { timeout: 10_000 });
}

test.describe('App Shell Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  test('header console opens broker settings as a modal, not a page', async ({ page }) => {
    await expect(page.locator('#topbar-refresh-btn')).toBeVisible();
    await expect(page.locator('#topbar-admin-btn')).toBeVisible();

    await page.click('#topbar-admin-btn');
    await expect(page.locator('#admin-console-modal')).toBeVisible({ timeout: 10_000 });
    await expect(page.locator('#admin-active-broker-select')).toBeVisible({ timeout: 10_000 });
    await expectActivePage(page, 'dashboard-page', 'nav-dashboard');

    await page.click('#admin-console-close');
    await expect(page.locator('#admin-console-modal')).toBeHidden();
  });

  test('browser history tracks shell navigation for the installed-app back button path', async ({ page }) => {
    await page.click('#nav-builder');
    await expectActivePage(page, 'builder-page', 'nav-builder');
    await expect.poll(() => page.evaluate(() => location.hash)).toBe('#builder');

    await page.click('#nav-results');
    await expectActivePage(page, 'results-page', 'nav-results');
    const beforeBack = await page.evaluate(() => ({ hash: location.hash, historyLength: history.length }));
    expect(beforeBack.hash).toBe('#results');
    expect(beforeBack.historyLength).toBeGreaterThan(2);

    await page.goBack();
    await expectActivePage(page, 'builder-page', 'nav-builder');
    await expect.poll(() => page.evaluate(() => location.hash)).toBe('#builder');

    await page.goBack();
    await expectActivePage(page, 'dashboard-page', 'nav-dashboard');
    await expect.poll(() => page.evaluate(() => location.hash)).toBe('#dashboard');
  });

  test('deep links, reloads, hash changes, and saved-tab restore stay deterministic', async ({ page }) => {
    await page.goto('/#market');
    await expectActivePage(page, 'market-page', 'nav-market');
    await expect.poll(() => page.evaluate(() => location.hash)).toBe('#market');

    await page.reload();
    await expectActivePage(page, 'market-page', 'nav-market');
    await expect.poll(() => page.evaluate(() => location.hash)).toBe('#market');

    await page.evaluate(() => { location.hash = '#live'; });
    await expectActivePage(page, 'live-page', 'nav-live');
    await expect.poll(() => page.evaluate(() => location.hash)).toBe('#live');

    await page.evaluate(() => localStorage.setItem('cf_active_tab', 'portfolio'));
    await page.goto('/');
    await expectActivePage(page, 'portfolio-page', 'nav-portfolio');
    await expect.poll(() => page.evaluate(() => location.hash)).toBe('#portfolio');
  });

  test('persisted pageshow restore and same-tab clicks do not rerun active-page loaders', async ({ page }) => {
    await page.click('#nav-results');
    await expectActivePage(page, 'results-page', 'nav-results');

    await page.evaluate(() => {
      (window as any).__cfShellCounts = { loadRuns: 0, fetchStrategies: 0 };
      const origLoadRuns = (window as any).loadRuns;
      const origFetchStrategies = (window as any).fetchStrategies;
      (window as any).loadRuns = function (...args: any[]) {
        (window as any).__cfShellCounts.loadRuns += 1;
        return origLoadRuns.apply(this, args);
      };
      (window as any).fetchStrategies = function (...args: any[]) {
        (window as any).__cfShellCounts.fetchStrategies += 1;
        return origFetchStrategies.apply(this, args);
      };
    });

    const historyBefore = await page.evaluate(() => history.length);

    await page.click('#nav-results');
    await expectActivePage(page, 'results-page', 'nav-results');

    await page.evaluate(() => {
      const evt = new Event('pageshow');
      Object.defineProperty(evt, 'persisted', { value: true });
      window.dispatchEvent(evt);
    });

    const counts = await page.evaluate(() => ({
      ...(window as any).__cfShellCounts,
      hash: location.hash,
      historyLength: history.length,
      activePage: document.querySelector('.page-section.active-page')?.id || '',
    }));

    expect(counts.loadRuns).toBe(0);
    expect(counts.fetchStrategies).toBe(0);
    expect(counts.hash).toBe('#results');
    expect(counts.historyLength).toBe(historyBefore);
    expect(counts.activePage).toBe('results-page');
  });
});
