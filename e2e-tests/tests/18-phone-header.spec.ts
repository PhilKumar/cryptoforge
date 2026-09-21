import { test, expect, Page } from '@playwright/test';

/**
 * The phone header (22-Sep-2026).
 *
 * Phil: "all the headings and tab heading completely occupy the page and I am
 * viewing the main content only on the last 20% of the screen". On a 375x812
 * phone the pinned header measured 549px. It is now compact, and it slides
 * away while reading down and returns on the first scroll up, like an app.
 */

const PIN = process.env.E2E_PIN || '123456';
const USER = process.env.E2E_USER || 'admin';

async function login(page: Page) {
  await page.goto('/app');
  await page.fill('#username-input', USER);
  await page.fill('#password-input', PIN);
  await page.click('#unlock-btn');
  await page.waitForSelector('.nav-tab', { timeout: 15_000 });
  await page.waitForFunction(() => typeof (window as any).showPage === 'function');
}

const shellHeight = (page: Page) =>
  page.evaluate(() => Math.round(document.querySelector('.sticky-shell')!.getBoundingClientRect().height));
const shellBottom = (page: Page) =>
  page.evaluate(() => Math.round(document.querySelector('.sticky-shell')!.getBoundingClientRect().bottom));

test.describe('Phone header', () => {
  test.use({ viewport: { width: 375, height: 812 } });

  test('the pinned header is at most a third of a phone screen', async ({ page }) => {
    await login(page);
    await page.evaluate(() => (window as any).showPage('optsell-page', document.getElementById('nav-strategies')));
    // It was 549px, two-thirds of the screen.
    expect(await shellHeight(page)).toBeLessThanOrEqual(270);
    // The strategy switcher is one swipeable row, not four stacked cards.
    const tabs = page.locator('#cf-strat-subnav .cf-strat-tab');
    const tops = await tabs.evaluateAll((els) => els.map((e) => Math.round(e.getBoundingClientRect().top)));
    expect(new Set(tops).size).toBe(1);
    expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(375);
  });

  test('it slides away reading down and returns on the first scroll up', async ({ page }) => {
    await login(page);
    await page.evaluate(() => (window as any).showPage('optsell-page', document.getElementById('nav-strategies')));
    await page.mouse.move(187, 600);
    await page.mouse.wheel(0, 700);
    await expect.poll(() => shellBottom(page)).toBeLessThanOrEqual(0);
    await page.mouse.wheel(0, -120);
    await expect.poll(() => shellBottom(page)).toBeGreaterThan(150);
  });

  test('a new page opens with its header showing', async ({ page }) => {
    await login(page);
    await page.evaluate(() => (window as any).showPage('optsell-page', document.getElementById('nav-strategies')));
    await page.mouse.move(187, 600);
    await page.mouse.wheel(0, 700);
    await expect.poll(() => shellBottom(page)).toBeLessThanOrEqual(0);
    await page.evaluate(() => (window as any).showPage('portfolio-page', document.getElementById('nav-portfolio')));
    await expect.poll(() => page.evaluate(() => document.documentElement.classList.contains('cf-shell-tucked'))).toBe(false);
  });
});

test.describe('Desktop header', () => {
  test('never slides away on a wide screen', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 });
    await login(page);
    await page.evaluate(() => (window as any).showPage('optsell-page', document.getElementById('nav-strategies')));
    await page.mouse.move(700, 600);
    await page.mouse.wheel(0, 700);
    await page.waitForTimeout(400);
    expect(await page.evaluate(() => document.documentElement.classList.contains('cf-shell-tucked'))).toBe(false);
    expect(await shellBottom(page)).toBeGreaterThan(100);
  });
});
