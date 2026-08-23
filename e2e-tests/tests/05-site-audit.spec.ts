import { expect, Page, test } from '@playwright/test';

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

async function expectActivePage(page: Page, pageId: string, navId: string) {
  await expect(page.locator('#' + pageId)).toHaveClass(/active-page/, { timeout: 10_000 });
  await expect(page.locator('#' + navId)).toHaveClass(/active/, { timeout: 10_000 });
}

const shellPages = [
  { hash: '#journal', nav: '#nav-journal', section: 'journal-page', probe: '#cf-journal-body' },
  { hash: '#dashboard', nav: '#nav-dashboard', section: 'dashboard-page', probe: '#dash-runs-table' },
  { hash: '#portfolio', nav: '#nav-portfolio', section: 'portfolio-page', probe: '#pf-positions-table' },
  { hash: '#cascade', nav: '#nav-strategies', section: 'cascade-page', probe: '#cf-cascade-trades' },
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
      await page.goto('/app' + shellPage.hash, { waitUntil: 'domcontentloaded' });
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

  test('core pages do not create document-level overflow at phone and tablet widths', async ({ page }) => {
    for (const viewport of [{ width: 390, height: 844 }, { width: 768, height: 1024 }]) {
      await page.setViewportSize(viewport);
      for (const shellPage of shellPages) {
        await page.goto('/app' + shellPage.hash, { waitUntil: 'domcontentloaded' });
        await expectActivePage(page, shellPage.section, shellPage.nav.slice(1));
        const overflow = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
        expect(overflow, `${shellPage.section} overflows at ${viewport.width}px`).toBeLessThanOrEqual(1);
      }
    }
  });

  test('dialogs are named and keyboard-operable', async ({ page }) => {
    await page.click('#topbar-appearance-btn');
    const appearance = page.getByRole('dialog', { name: 'Appearance' });
    await expect(appearance).toBeVisible();
    await page.keyboard.press('Escape');
    await expect(appearance).toBeHidden();

    await page.evaluate(() => { void (window as any).cfAlert('Keyboard audit', 'Audit dialog', 'ℹ️'); });
    const alert = page.getByRole('dialog', { name: 'Audit dialog' });
    await expect(alert).toBeVisible();
    await expect(alert.getByRole('button', { name: 'OK' })).toBeFocused();
    await page.keyboard.press('Escape');
    await expect(alert).toBeHidden();
  });

  // Adding .cf-strat-subnav INSIDE the `.cf-mode-option, .cf-tf-option` selector
  // list silently handed Paper/Live the sub-nav's rule instead of the button
  // rule, and they rendered as raw browser buttons on a live page. No Python
  // test can see that, and nothing else here reads a computed style, so the
  // segmented toggles get their own guard.
  test('segmented toggles are styled, not raw browser buttons', async ({ page }) => {
    await page.click('#nav-strategies');
    await expectActivePage(page, 'cascade-page', 'nav-strategies');
    await expect(page.locator('#nav-strategies .live-dot'), 'one aggregate strategy status badge').toHaveCount(1);
    await expect(page.locator('#strategies-tab-dot')).toHaveAttribute('role', 'status');
    const aggregateStatus = await page.evaluate(() => {
      const app = window as any;
      app._cfUpdateStrategiesTabDot('cascade', true);
      app._cfUpdateStrategiesTabDot('vrule', true);
      const dot = document.getElementById('strategies-tab-dot')!;
      const result = {
        active: dot.classList.contains('active'),
        label: dot.getAttribute('aria-label'),
      };
      app._cfUpdateStrategiesTabDot('cascade', false);
      app._cfUpdateStrategiesTabDot('vrule', false);
      app._cfUpdateStrategiesTabDot('auto', false);
      return result;
    });
    expect(aggregateStatus).toEqual({ active: true, label: 'Cascade-Hybrid, V-Rule active' });

    // The Cascade/V-Rule selector is a desk CARD now (PhilForge's shape: icon
    // tile, name, one line) — it keeps a deliberate 1px border, so it is
    // checked for the card look rather than the flat segmented one.
    const toggles = [
      { selector: '.cf-mode-option', label: 'Paper/Live mode', border: 0 },
      { selector: '.cf-strat-subnav .cf-strat-tab', label: 'Cascade/V-Rule selector', border: 1 },
    ];

    for (const toggle of toggles) {
      const painted = await page.locator(toggle.selector).first().evaluate((el) => {
        const style = window.getComputedStyle(el as HTMLElement);
        return {
          appearance: style.appearance,
          borderRadius: parseFloat(style.borderRadius) || 0,
          paddingLeft: parseFloat(style.paddingLeft) || 0,
          borderTopWidth: parseFloat(style.borderTopWidth) || 0,
        };
      });
      // A default <button> keeps appearance:auto, a hairline border and no
      // radius; every one of ours is flattened and re-padded by the rule.
      expect(painted.appearance, `${toggle.label} kept the native button look`).toBe('none');
      expect(painted.borderRadius, `${toggle.label} lost its rounded corners`).toBeGreaterThan(0);
      expect(painted.paddingLeft, `${toggle.label} lost its padding`).toBeGreaterThan(4);
      expect(painted.borderTopWidth, `${toggle.label} has the wrong border`).toBe(toggle.border);
    }

    // The desk card carries a name AND a one-line description. There is ONE
    // selector now, in the header inside .sticky-shell, so it holds still with
    // the nav instead of scrolling away with the page — and it must read the
    // same, and stay visible, wherever among the three you are standing.
    const STRATEGIES = ['Cascade-Hybrid', 'V-Rule', 'Cascade_Auto'];
    await expect(page.locator('.cf-strat-subnav'), 'one switcher, not one per page').toHaveCount(1);
    await expect(
      page.locator('.sticky-shell .cf-strat-subnav'),
      'the switcher must live in the header shell',
    ).toHaveCount(1);
    for (const pageId of ['cascade-page', 'rule3070-page', 'autofib-page']) {
      await page.evaluate((id) => (window as any).showPage(id), pageId);
      const bar = page.locator('#cf-strat-subnav');
      await expect(bar, `${pageId} selector visible`).toBeVisible();
      const cards = bar.locator('.cf-strat-tab');
      await expect(cards, `${pageId} selector`).toHaveCount(STRATEGIES.length);
      await expect(cards.locator('strong')).toHaveText(STRATEGIES);
      for (let i = 0; i < STRATEGIES.length; i++) {
        await expect(cards.nth(i).locator('small')).not.toBeEmpty();
      }
      // The card for the page you are on is the marked one.
      await expect(
        bar.locator(`.cf-strat-tab[data-cf-strat-page="${pageId}"]`),
        `${pageId} card marked active`,
      ).toHaveClass(/is-active/);
    }
    // And it is gone on a page that is not a strategy.
    await page.evaluate(() => (window as any).showPage('journal-page'));
    await expect(page.locator('#cf-strat-subnav'), 'hidden off the strategy pages').toBeHidden();
    await page.evaluate(() => (window as any).showPage('cascade-page'));

    // Strategy cards stay compact and start at the left edge, leaving room in
    // the same row for future strategies instead of stretching two cards to
    // consume the whole page.
    const layout = await page.locator('#cf-strat-subnav').evaluate((tabs) => {
      const host = tabs.getBoundingClientRect();
      const cards = Array.from(tabs.querySelectorAll('.cf-strat-tab'))
        .map((card) => card.getBoundingClientRect());
      return {
        host: { left: host.left, right: host.right },
        cards: cards.map((card) => ({ left: card.left, right: card.right, width: card.width })),
      };
    });
    expect(layout.cards[0].left).toBeCloseTo(layout.host.left, 0);
    // Every card compact, and each one starting after the last — a row that
    // reads left to right, not a set stretched to fill the page.
    for (let i = 0; i < layout.cards.length; i++) {
      expect(layout.cards[i].width, `card ${i} is too wide`).toBeLessThanOrEqual(320);
      if (i > 0) {
        expect(layout.cards[i].left, `card ${i} overlaps the one before it`)
          .toBeGreaterThan(layout.cards[i - 1].right);
      }
    }
    // Room still left after the LAST card, so the next strategy has somewhere
    // to go. Checking the second card would stop meaning anything the moment
    // a third one exists.
    const lastCard = layout.cards[layout.cards.length - 1];
    expect(layout.host.right - lastCard.right).toBeGreaterThan(100);
  });
});
