import { test, expect, Page } from '@playwright/test';

/**
 * The Assets tab and its three framed tearsheets.
 *
 * This suite exists because the failure mode here is SILENT. The sheets are
 * framed, and a Content-Security-Policy that forbids the frame renders a blank
 * white box with nothing but one console line — every unit test still passes,
 * the page still "loads", and the tab looks finished until someone opens it.
 *
 * It happened twice while this was built, and the two halves look identical:
 *
 *   1. `frame-src 'none'` — the parent may not frame anything.
 *   2. `frame-ancestors 'none'` — the DOCUMENT may not be framed, by anyone,
 *      including a same-origin parent. Fixing only the first leaves it blank.
 *
 * So these tests assert on what is INSIDE the frame, never merely that the
 * element exists.
 */

const PIN = process.env.E2E_PIN || '123456';
const USER = process.env.E2E_USER || 'admin';

async function login(page: Page) {
  await page.goto('/app');
  await page.fill('#username-input', USER);
  await page.fill('#password-input', PIN);
  await page.click('#unlock-btn');
  await page.waitForSelector('.nav-tab', { timeout: 15_000 });
}

async function openAssets(page: Page) {
  await page.click('#nav-assets');
  await expect(page.locator('#assets-page')).toHaveClass(/active-page/);
}

test.describe('Assets — the strategy tearsheets', () => {
  test('the tab opens and carries all three sheets', async ({ page }) => {
    await login(page);
    await openAssets(page);
    await expect(page.locator('#cf-assets-subnav [data-cf-assets-doc]')).toHaveCount(3);
    for (const doc of ['hybrid', 'vrule', 'auto']) {
      await expect(page.locator(`[data-cf-assets-doc="${doc}"]`)).toHaveCount(1);
    }
  });

  test('the framed document actually renders — not a CSP-blocked blank', async ({ page }) => {
    const blocked: string[] = [];
    page.on('console', (m) => {
      if (m.type() === 'error' && /Content Security Policy|frame-src|frame-ancestors/i.test(m.text())) {
        blocked.push(m.text());
      }
    });
    await login(page);
    await openAssets(page);

    // Content, not the element: an empty frame satisfies every selector.
    const heading = page.frameLocator('#cf-assets-frame').locator('h1').first();
    await expect(heading).toContainText(/Cascade Hybrid/i, { timeout: 20_000 });
    expect(blocked, `CSP blocked the frame:\n${blocked.join('\n')}`).toEqual([]);
  });

  test('each sub-tab swaps the document', async ({ page }) => {
    await login(page);
    await openAssets(page);

    await page.click('[data-cf-assets-doc="vrule"]');
    await expect(page.frameLocator('#cf-assets-frame').locator('h1').first())
      .toContainText(/V-Rule/i, { timeout: 20_000 });

    await page.click('[data-cf-assets-doc="auto"]');
    await expect(page.frameLocator('#cf-assets-frame').locator('h1').first())
      .toContainText(/Cascade-Auto/i, { timeout: 20_000 });  // hyphen is the enforced name
  });

  test('the workspace theme reaches the document', async ({ page }) => {
    await login(page);
    await openAssets(page);
    const frame = page.locator('#cf-assets-frame');

    await page.evaluate(() => document.documentElement.setAttribute('data-theme', 'dark'));
    await expect(frame).toHaveAttribute('src', /theme=dark/, { timeout: 10_000 });

    await page.evaluate(() => document.documentElement.setAttribute('data-theme', 'light'));
    await expect(frame).toHaveAttribute('src', /theme=light/, { timeout: 10_000 });
  });

  test('the chosen sheet survives leaving and coming back', async ({ page }) => {
    await login(page);
    await openAssets(page);
    await page.click('[data-cf-assets-doc="auto"]');
    await expect(page.locator('#cf-assets-frame')).toHaveAttribute('data-cf-doc', 'auto');

    await page.click('#nav-journal');
    await page.click('#nav-assets');
    await expect(page.locator('#cf-assets-frame')).toHaveAttribute('data-cf-doc', 'auto');
    await expect(page.locator('[data-cf-assets-doc="auto"]')).toHaveAttribute('aria-selected', 'true');
  });
});
