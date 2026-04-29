import { expect, Page, test } from '@playwright/test';

const PIN = process.env.E2E_PIN || '123456';

async function login(page: Page) {
  await page.goto('/');
  for (const digit of PIN.split('')) {
    await page.click(`[data-val="${digit}"]`);
  }
  await page.waitForSelector('.nav-tab', { timeout: 10_000 });
}

async function expectActivePage(page: Page, pageId: string, navId: string) {
  await expect(page.locator('#' + pageId)).toHaveClass(/active-page/, { timeout: 10_000 });
  await expect(page.locator('#' + navId)).toHaveClass(/active/, { timeout: 10_000 });
}

const shellPages = [
  { hash: '#dashboard', nav: '#nav-dashboard', section: 'dashboard-page', probe: '#dash-runs-table' },
  { hash: '#portfolio', nav: '#nav-portfolio', section: 'portfolio-page', probe: '#pf-positions-table' },
  { hash: '#builder', nav: '#nav-builder', section: 'builder-page', probe: '#backtest-run-btn' },
  { hash: '#live', nav: '#nav-live', section: 'live-page', probe: '#live-panels-container' },
  { hash: '#scalp', nav: '#nav-scalp', section: 'scalp-page', probe: '#cf-scalp-active-table' },
  { hash: '#market', nav: '#nav-market', section: 'market-page', probe: '#market-table' },
  { hash: '#results', nav: '#nav-results', section: 'results-page', probe: '#runs-table' },
];

const getRoutes = [
  '/api/auth/status',
  '/api/health',
  '/api/ready',
  '/api/audit/production-readiness',
  '/api/ops/state/summary',
  '/api/ops/state/backup',
  '/api/dashboard/summary',
  '/api/admin/config',
  '/api/broker/settings',
  '/api/products',
  '/api/leverage/BTCUSDT',
  '/api/cryptos',
  '/api/market/top25',
  '/api/ticker',
  '/api/ticker/BTCUSDT',
  '/api/live/status',
  '/api/paper/status',
  '/api/paper/status?run_id=audit-missing',
  '/api/orders',
  '/api/positions',
  '/api/wallet',
  '/api/broker/trades',
  '/api/portfolio/summary',
  '/api/engines/all',
  '/api/portfolio/history',
  '/api/strategies',
  '/api/strategies/0/versions',
  '/api/runs',
  '/api/runs/0',
  '/api/runs/0/csv',
  '/api/cache/status',
  '/api/funding/BTCUSDT',
  '/api/paper/trades/csv',
  '/api/live/trades/csv',
  '/api/scalp/status',
  '/api/scalp/diagnostics?symbol=BTCUSDT',
  '/api/scalp/trades',
  '/api/scalp/activity',
];

test.describe('Comprehensive Site Audit', () => {
  test.beforeEach(async ({ page }) => {
    await login(page);
  });

  test('authenticated GET routes do not return server errors', async ({ page }) => {
    const failures: string[] = [];

    for (const url of getRoutes) {
      const response = await page.request.get(url, { timeout: 35_000 });
      if (response.status() >= 500) {
        const body = await response.text().catch(() => '');
        failures.push(`${url} -> ${response.status()} ${body.slice(0, 240)}`);
      }
    }

    expect(failures).toEqual([]);
  });

  test('all shell routes load without document, script, or stylesheet failures', async ({ page }) => {
    const pageErrors: string[] = [];
    const resourceFailures: string[] = [];

    page.on('pageerror', (error) => pageErrors.push(error.message));
    page.on('requestfailed', (request) => {
      if (['document', 'script', 'stylesheet'].includes(request.resourceType())) {
        resourceFailures.push(`${request.resourceType()} failed: ${request.url()} ${request.failure()?.errorText || ''}`);
      }
    });
    page.on('response', (response) => {
      const type = response.request().resourceType();
      if (['document', 'script', 'stylesheet'].includes(type) && response.status() >= 400) {
        resourceFailures.push(`${type} ${response.status()}: ${response.url()}`);
      }
    });

    for (const shellPage of shellPages) {
      await page.goto('/' + shellPage.hash, { waitUntil: 'domcontentloaded' });
      await expectActivePage(page, shellPage.section, shellPage.nav.slice(1));
      await expect(page.locator(shellPage.probe)).toBeVisible({ timeout: 15_000 });
      await page.waitForTimeout(250);
    }

    expect(pageErrors).toEqual([]);
    expect(resourceFailures).toEqual([]);
  });

  test('visible links and controls have usable labels and no dead local links', async ({ page }) => {
    const issues: string[] = [];

    for (const shellPage of shellPages) {
      await page.click(shellPage.nav);
      await expectActivePage(page, shellPage.section, shellPage.nav.slice(1));
      await expect(page.locator(shellPage.probe)).toBeVisible({ timeout: 15_000 });

      const pageIssues = await page.evaluate((pageId) => {
        const visible = (el: Element) => {
          const htmlEl = el as HTMLElement;
          const style = window.getComputedStyle(htmlEl);
          const rect = htmlEl.getBoundingClientRect();
          return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
        };
        const labelFor = (el: Element) => {
          const htmlEl = el as HTMLElement;
          return (htmlEl.innerText || el.getAttribute('aria-label') || el.getAttribute('title') || el.getAttribute('value') || '').trim();
        };
        const selectorFor = (el: Element) => {
          const tag = el.tagName.toLowerCase();
          const id = el.id ? '#' + el.id : '';
          const classes = Array.from(el.classList).slice(0, 3).map((name) => '.' + name).join('');
          const label = labelFor(el).replace(/\s+/g, ' ').slice(0, 48);
          return `${tag}${id}${classes}${label ? ` "${label}"` : ''}`;
        };
        const active = document.getElementById(pageId);
        if (!active) return [];
        const found: string[] = [];
        active.querySelectorAll('button, a[href], select, input, textarea').forEach((el) => {
          if (!visible(el)) return;
          const tag = el.tagName.toLowerCase();
          const type = (el.getAttribute('type') || '').toLowerCase();
          if (tag === 'input' && ['hidden', 'checkbox', 'radio'].includes(type)) return;
          const label = labelFor(el);
          const id = el.id;
          const hasLabelElement = !!(id && Array.from(active.querySelectorAll('label')).some((labelNode) => labelNode.getAttribute('for') === id));
          if (!label && !hasLabelElement) found.push(`${pageId}: unlabeled ${selectorFor(el)}`);
          if (tag === 'a') {
            const href = el.getAttribute('href') || '';
            if (!href || href === '#') found.push(`${pageId}: dead local link ${selectorFor(el)}`);
            if (/^javascript:/i.test(href)) found.push(`${pageId}: javascript href ${selectorFor(el)}`);
          }
        });
        active.querySelectorAll('[data-cf-click]').forEach((el) => {
          const code = el.getAttribute('data-cf-click') || '';
          try {
            new Function('event', code);
          } catch (error) {
            found.push(`${pageId}: invalid data-cf-click on ${selectorFor(el)}: ${String(error)}`);
          }
        });
        return found;
      }, shellPage.section);

      issues.push(...pageIssues);
    }

    expect(issues).toEqual([]);
  });

  test('admin console modal lists both brokers and masks secret values', async ({ page }) => {
    await page.click('#topbar-admin-btn');
    await expect(page.locator('#admin-console-modal')).toBeVisible({ timeout: 15_000 });
    await expect(page.locator('#admin-active-broker-select')).toBeVisible({ timeout: 15_000 });

    await expect.poll(async () => page.locator('#admin-active-broker-select option').evaluateAll((nodes) => nodes.map((node) => (node as HTMLOptionElement).value)), {
      timeout: 15_000,
    }).toEqual(expect.arrayContaining(['delta', 'coindcx']));

    await expect(page.locator('#admin-fields-delta [data-admin-env-key=DELTA_API_KEY]')).toBeVisible();
    await expect(page.locator('#admin-fields-coindcx [data-admin-env-key=COINDCX_API_KEY]')).toBeVisible();

    const response = await page.request.get('/api/admin/config');
    expect(response.status()).toBe(200);
    const data: { fields: Array<{ key: string; secret: boolean; value: string; masked: string }> } = await response.json();
    const secretLeaks = data.fields.filter((field) => field.secret && field.value);
    expect(secretLeaks).toEqual([]);
    expect(data.fields.some((field) => field.key === 'CRYPTOFORGE_BROKER')).toBe(true);
  });
});
