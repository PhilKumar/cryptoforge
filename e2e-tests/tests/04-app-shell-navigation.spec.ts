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
});
