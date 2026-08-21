import { test, expect, Page } from '@playwright/test';

/**
 * Auto-Cascade_Fib pins the chart into its page rather than opening it over
 * the screen. It does that by MOVING the one chart panel — the real one, with
 * its toolbar, engines and canvas — out of its overlay and into a page
 * container, so both pages keep sharing a single renderer.
 *
 * That is cheap and it is also the whole risk: a panel left behind in the
 * Auto-Cascade_Fib page hands the Cascade page an empty overlay the next time
 * it opens a chart, and the fault looks like the renderer's rather than a
 * misplaced element. Worse, the Auto-Cascade_Fib page is display:none from
 * everywhere else, so the borrowed panel would be invisible with no error.
 *
 * These tests drive the move directly, with no campaign and no orders, so they
 * run on every push regardless of what the market is doing.
 */

const PIN = process.env.E2E_PIN || '123456';
const USER = process.env.E2E_USER || 'admin';

async function login(page: Page) {
  await page.goto('/app');
  await page.fill('#username-input', USER);
  await page.fill('#password-input', PIN);
  await page.click('#unlock-btn');
  await page.waitForSelector('.nav-tab', { timeout: 10_000 });
  await page.waitForFunction(() => typeof (window as any).cfCascadeChartPinTo === 'function');
}

const where = (page: Page) =>
  page.evaluate(() => {
    const panel = document.getElementById('cf-cascade-chart-panel');
    const host = document.getElementById('cf-af-chart-host');
    const overlay = document.getElementById('cf-cascade-chart-overlay');
    return {
      inHost: !!(panel && host && host.contains(panel)),
      inOverlay: !!(panel && overlay && overlay.contains(panel)),
      pinnedClass: !!panel?.classList.contains('cf-chart-pinned'),
      bodyFullscreen: document.body.classList.contains('cf-chart-fs-open'),
    };
  });

test.describe('Auto-Cascade_Fib chart pinning', () => {
  test('the page exists with a host for the chart and a line picker', async ({ page }) => {
    await login(page);
    await page.evaluate(() => (window as any).showPage('autofib-page', document.getElementById('nav-strategies')));
    await expect(page.locator('#autofib-page')).toBeVisible();
    await expect(page.locator('#cf-af-chart-host')).toHaveCount(1);
    await expect(page.locator('#cf-af-chart-pick')).toHaveCount(1);
  });

  test('the panel moves into the page and all the way back', async ({ page }) => {
    await login(page);
    await expect.poll(() => where(page).then((w) => w.inOverlay)).toBe(true);

    await page.evaluate(() => (window as any).cfCascadeChartPinTo('cf-af-chart-host'));
    const pinned = await where(page);
    expect(pinned.inHost, 'panel did not move into the page').toBe(true);
    expect(pinned.inOverlay).toBe(false);
    expect(pinned.pinnedClass, 'pinned panel keeps its modal styling').toBe(true);
    // Pinning must never leave the page locked behind a full-screen chart.
    expect(pinned.bodyFullscreen).toBe(false);

    await page.evaluate(() => (window as any).cfCascadeChartPinTo(''));
    const back = await where(page);
    expect(back.inOverlay, 'panel was stranded outside its overlay').toBe(true);
    expect(back.inHost).toBe(false);
    expect(back.pinnedClass).toBe(false);
  });

  test('closing the chart returns the panel even when it was pinned', async ({ page }) => {
    await login(page);
    await page.evaluate(() => (window as any).cfCascadeChartPinTo('cf-af-chart-host'));
    expect((await where(page)).inHost).toBe(true);

    // cfCascadeHideChart is what every close path runs — the button, the
    // backdrop, and leaving the page. It has to undo the pin too.
    await page.evaluate(() => (window as any).cfCascadeHideChart());
    const after = await where(page);
    expect(after.inOverlay, 'closing left the panel in the page').toBe(true);
    expect(after.bodyFullscreen).toBe(false);
    await expect(page.locator('#cf-cascade-chart-overlay')).toBeHidden();
  });

  test('leaving the page hands the panel back before it hides', async ({ page }) => {
    await login(page);
    await page.evaluate(() => (window as any).showPage('autofib-page', document.getElementById('nav-strategies')));
    await page.evaluate(() => (window as any).cfCascadeChartPinTo('cf-af-chart-host'));
    expect((await where(page)).inHost).toBe(true);

    await page.evaluate(() => (window as any).showPage('cascade-page', document.getElementById('nav-strategies')));
    const after = await where(page);
    expect(after.inOverlay, 'the panel went invisible with the page that borrowed it').toBe(true);
  });

  test('the Cascade page can still open a chart after the page borrowed it', async ({ page }) => {
    await login(page);
    // Borrow it, give it back the way leaving the page does, then use the
    // Cascade page's own opener against a fixture.
    await page.evaluate(() => (window as any).cfCascadeChartPinTo('cf-af-chart-host'));
    await page.evaluate(() => (window as any).cfCascadeHideChart());

    await page.route('**/api/cascade/campaigns/**/chart**', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          status: 'ok', symbol: 'BTCUSDT', timeframe: '5m', campaign_seq: 1,
          candles: Array.from({ length: 40 }, (_, i) => ({
            t: 1750000000 + i * 300, o: 61000, h: 61060, l: 60940, c: 60980, is_mother: i === 0,
          })),
          legs: [], trendlines: [], fills: [], rounds: [],
        }),
      });
    });
    await page.evaluate(() => (window as any).cfCascadeShowChart('anything'));
    await expect(page.locator('#cf-cascade-chart-overlay')).toBeVisible();
    // The panel has to be IN the overlay for the chart to be seen at all.
    expect((await where(page)).inOverlay).toBe(true);
    await expect(page.locator('#cf-cascade-chart-body')).not.toBeEmpty();
  });
});
