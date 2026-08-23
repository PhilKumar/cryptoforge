import { expect, Page, test } from '@playwright/test';

const PIN = process.env.E2E_PIN || '123456';
const USER = process.env.E2E_USER || 'admin';

const shellPages = [
  { hash: '#journal', section: 'journal-page' },
  { hash: '#dashboard', section: 'dashboard-page' },
  { hash: '#portfolio', section: 'portfolio-page' },
  { hash: '#cascade', section: 'cascade-page' },
  { hash: '#builder', section: 'builder-page' },
  { hash: '#live', section: 'live-page' },
  { hash: '#scalp', section: 'scalp-page' },
  { hash: '#market', section: 'market-page' },
  { hash: '#results', section: 'results-page' },
];

async function login(page: Page) {
  await page.goto('/app');
  await page.fill('#username-input', USER);
  await page.fill('#password-input', PIN);
  await page.click('#unlock-btn');
  await page.waitForSelector('.nav-tab', { timeout: 10_000 });
}

async function setTheme(page: Page, theme: 'dark' | 'light') {
  await page.evaluate((nextTheme) => {
    document.documentElement.dataset.theme = nextTheme;
    document.documentElement.style.colorScheme = nextTheme;
    localStorage.setItem('cf-theme', nextTheme);
  }, theme);
}

async function activePageLayoutIssues(page: Page, pageId: string) {
  return page.evaluate((activePageId) => {
    const viewportWidth = document.documentElement.clientWidth;
    const active = document.getElementById(activePageId);
    if (!active) return [`${activePageId}: missing active page`];

    const visible = (el: Element) => {
      const style = getComputedStyle(el as HTMLElement);
      const rect = el.getBoundingClientRect();
      return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
    };
    const label = (el: Element) => {
      const html = el as HTMLElement;
      return (el.id || html.innerText || el.getAttribute('aria-label') || el.tagName).replace(/\s+/g, ' ').trim().slice(0, 52);
    };
    const hasHorizontalScrollOwner = (el: Element) => {
      let parent = el.parentElement;
      while (parent && parent !== document.body) {
        const style = getComputedStyle(parent);
        if ((style.overflowX === 'auto' || style.overflowX === 'scroll') && parent.scrollWidth > parent.clientWidth + 1) {
          return true;
        }
        parent = parent.parentElement;
      }
      return false;
    };

    const issues: string[] = [];
    const overflow = document.documentElement.scrollWidth - viewportWidth;
    if (overflow > 1) issues.push(`${activePageId}: document overflow ${overflow}px`);

    active.querySelectorAll('button, a[href], input, select, textarea').forEach((el) => {
      if (!visible(el)) return;
      const rect = el.getBoundingClientRect();
      if ((rect.left < -1 || rect.right > viewportWidth + 1) && !hasHorizontalScrollOwner(el)) {
        issues.push(`${activePageId}: control clipped ${label(el)} [${Math.round(rect.left)}, ${Math.round(rect.right)}]`);
      }
    });

    active.querySelectorAll('table').forEach((table) => {
      if (!visible(table)) return;
      const rect = table.getBoundingClientRect();
      if (rect.width > viewportWidth + 1 && !hasHorizontalScrollOwner(table)) {
        issues.push(`${activePageId}: wide table has no horizontal scroll owner ${label(table)} (${Math.round(rect.width)}px)`);
      }
    });

    return issues;
  }, pageId);
}

test.describe('PWA, responsive and light-mode layout', () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  for (const viewport of [
    { name: 'phone', width: 390, height: 844 },
    { name: 'tablet', width: 768, height: 1024 },
  ]) {
    for (const theme of ['dark', 'light'] as const) {
      test(`${viewport.name} ${theme}: routes keep controls and tables viewable`, async ({ page }) => {
        await page.setViewportSize({ width: viewport.width, height: viewport.height });
        await setTheme(page, theme);
        const issues: string[] = [];

        for (const shellPage of shellPages) {
          await page.goto('/app' + shellPage.hash, { waitUntil: 'domcontentloaded' });
          await expect(page.locator('#' + shellPage.section)).toHaveClass(/active-page/, { timeout: 10_000 });
          await page.waitForTimeout(180);
          issues.push(...await activePageLayoutIssues(page, shellPage.section));
        }

        expect(issues).toEqual([]);
      });
    }
  }

  test('phone light: global dialogs fit and retain reachable close/actions', async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 640 });
    await setTheme(page, 'light');

    const expectDialogFits = async (selector: string) => {
      const viewport = page.viewportSize();
      expect(viewport).not.toBeNull();
      const box = await page.locator(selector).boundingBox();
      expect(box, `${selector} has no box`).not.toBeNull();
      expect(box!.x, `${selector} begins off screen`).toBeGreaterThanOrEqual(0);
      expect(box!.x + box!.width, `${selector} ends off screen`).toBeLessThanOrEqual(viewport!.width);
      expect(box!.y, `${selector} begins above the viewport`).toBeGreaterThanOrEqual(0);
      expect(box!.y + box!.height, `${selector} ends below the viewport`).toBeLessThanOrEqual(viewport!.height);
    };

    await page.click('#topbar-appearance-btn');
    await expect(page.locator('#appearance-modal')).toBeVisible();
    await expectDialogFits('.appearance-panel');
    await expect(page.locator('#appearance-modal .admin-modal-close')).toBeInViewport();
    await page.keyboard.press('Escape');

    await page.click('#topbar-admin-btn');
    await expect(page.locator('#admin-console-modal')).toBeVisible({ timeout: 15_000 });
    await expectDialogFits('#admin-console-modal .cf-admin-modal-panel');
    await expect(page.locator('#admin-console-modal .admin-modal-close')).toBeInViewport();
    await page.keyboard.press('Escape');

    await page.evaluate(() => {
      document.body.classList.add('kill-switch-on');
      document.getElementById('kill-switch-btn')?.classList.remove('hidden');
      const banner = document.createElement('div');
      banner.className = 'cf-pwa-update-banner';
      banner.dataset.auditBanner = '1';
      banner.innerHTML = '<div><strong>CryptoForge update ready</strong><span>Audit update</span></div><button class="cf-pwa-update-btn">Reload</button>';
      document.body.appendChild(banner);
    });
    const updateBox = await page.locator('[data-audit-banner="1"]').boundingBox();
    const killBox = await page.locator('#kill-switch-btn').boundingBox();
    expect(updateBox).not.toBeNull();
    expect(killBox).not.toBeNull();
    expect(updateBox!.y + updateBox!.height, 'PWA update banner overlaps the emergency control').toBeLessThanOrEqual(killBox!.y - 8);
    await page.locator('[data-audit-banner="1"]').evaluate((banner) => banner.remove());

    await page.evaluate(() => { void (window as any).CryptoForgePWA.openInstallPrompt(); });
    const pwaDialog = page.locator('.cf-pwa-sheet');
    await expect(pwaDialog).toBeVisible();
    await expectDialogFits('.cf-pwa-sheet');
    const close = pwaDialog.locator('.cf-pwa-close');
    await expect(close).toBeInViewport();
    const closeBox = await close.boundingBox();
    expect(closeBox!.width, 'PWA close control stretched like an action button').toBeLessThanOrEqual(48);
    const primary = pwaDialog.locator('[data-pwa-action="primary"]');
    await expect(primary).toBeInViewport();
    const modalAboveKill = await page.evaluate(() => {
      const overlay = document.querySelector('.cf-pwa-overlay') as HTMLElement | null;
      const kill = document.getElementById('kill-switch-btn');
      return !overlay || !kill || getComputedStyle(kill).display === 'none'
        || Number(getComputedStyle(overlay).zIndex) > Number(getComputedStyle(kill).zIndex);
    });
    expect(modalAboveKill, 'emergency control painted over the PWA modal').toBe(true);

    await page.setViewportSize({ width: 844, height: 390 });
    await expectDialogFits('.cf-pwa-sheet');
    await expect(close).toBeInViewport();
    await pwaDialog.evaluate((dialog) => { dialog.scrollTop = dialog.scrollHeight; });
    await expect(primary).toBeInViewport();
  });

  test('manifest exposes a scoped app entry and complete install artwork', async ({ request }) => {
    const response = await request.get('/manifest.webmanifest');
    expect(response.status()).toBe(200);
    const manifest = await response.json();
    expect(manifest.id).toBe('/app');
    expect(manifest.start_url).toBe('/app');
    expect(manifest.scope).toBe('/');
    expect(['standalone', 'minimal-ui']).toContain(manifest.display);
    expect(manifest.icons).toEqual(expect.arrayContaining([
      expect.objectContaining({ sizes: '192x192', purpose: 'any' }),
      expect.objectContaining({ sizes: '512x512', purpose: 'any' }),
      expect.objectContaining({ sizes: '192x192', purpose: 'maskable' }),
      expect.objectContaining({ sizes: '512x512', purpose: 'maskable' }),
    ]));
    expect(manifest.screenshots).toEqual(expect.arrayContaining([
      expect.objectContaining({ form_factor: 'wide' }),
      expect.objectContaining({ form_factor: 'narrow' }),
    ]));
  });
});
