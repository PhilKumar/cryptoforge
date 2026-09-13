import { test, expect } from '@playwright/test';

for (const theme of ['dark', 'light']) {
  test(`populated Live monitor stays inside a phone in ${theme}`, async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 740 });
    await page.addInitScript(t => localStorage.setItem('cf-theme', t), theme);
    await page.route('**/api/engines/all', route => route.fulfill({ json: { engines: [{
      run_id: 'visual-fixture', strategy_name: 'Long strategy name for mobile verification',
      mode: 'paper', symbol: 'BTCUSDT', running: true, in_trade: true,
      current_candle: { close: 65000, open: 64000, high: 66000, low: 63000, volume: 100 },
      open_trades: [{ symbol: 'BTCUSDT', side: 'long', entry_price: 64000, notional: 1000 }],
      event_log: [{ time: '12:00', type: 'warning', message: 'Visual fixture only' }],
    }] } }));
    await page.goto('/app');
    await page.fill('#username-input', process.env.E2E_USER || 'admin');
    await page.fill('#password-input', process.env.E2E_PIN || '123456');
    await page.click('#unlock-btn');
    await page.waitForSelector('.nav-tab');
    await page.waitForFunction(() => typeof (window as any).showPage === 'function');
    await page.goto('/app#live');
    await expect(page.locator('#live-open-table-visual-fixture')).toBeVisible();
    const issues = await page.evaluate(() => {
      const issues: string[] = [];
      if (document.documentElement.scrollWidth > innerWidth + 1) issues.push('page overflow');
      document.querySelectorAll('#live-panels-container .live-monitor-table').forEach(table => {
        const parent = table.parentElement!;
        if (getComputedStyle(parent).overflowX !== 'auto') issues.push('table cannot scroll');
        if (parent.getBoundingClientRect().right > innerWidth + 1) issues.push('table wrapper overflow');
      });
      return issues;
    });
    expect(issues).toEqual([]);
    await page.screenshot({ path: `visual-evidence/live-${theme}-320.png`, fullPage: true });
  });
}

for (const theme of ['dark', 'light']) {
  test(`login remains reachable on a short phone in ${theme}`, async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 360 });
    await page.addInitScript(t => localStorage.setItem('cf-theme', t), theme);
    await page.goto('/app');
    await expect(page.locator('#username-input')).toBeVisible();
    const layout = await page.evaluate(() => ({
      top: document.querySelector('.unlock-card')!.getBoundingClientRect().top,
      overflow: getComputedStyle(document.body).overflowY,
      width: document.documentElement.scrollWidth,
    }));
    expect(layout.top).toBeGreaterThanOrEqual(0);
    expect(layout.overflow).not.toBe('hidden');
    expect(layout.width).toBeLessThanOrEqual(320);
    await page.locator('#unlock-btn').scrollIntoViewIfNeeded();
    await expect(page.locator('#unlock-btn')).toBeInViewport();
    await page.screenshot({ path: `visual-evidence/login-${theme}-320.png`, fullPage: true });
  });
}

test('light warning and danger buttons retain readable text across their gradients', async ({ page }) => {
  await page.goto('/app');
  await page.fill('#username-input', process.env.E2E_USER || 'admin');
  await page.fill('#password-input', process.env.E2E_PIN || '123456');
  await page.click('#unlock-btn');
  await page.waitForSelector('.nav-tab');
  await page.waitForFunction(() => typeof (window as any).showPage === 'function');
  await expect(page.locator('.nav-tab').first()).toHaveCSS('font-weight', '700');
  const ratios = await page.evaluate(() => {
    document.documentElement.dataset.theme = 'light';
    function luminance(rgb: number[]) {
      const c = rgb.map(v => { v /= 255; return v <= 0.04045 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4; });
      return c[0] * 0.2126 + c[1] * 0.7152 + c[2] * 0.0722;
    }
    return ['arctic', 'gold', 'magenta', 'citrus', 'graphite', 'bronze'].flatMap(tint => {
      document.documentElement.dataset.tint = tint;
      return ['btn-primary', 'btn-warning', 'btn-danger', ...['all', 'backtest', 'paper', 'live', 'scalp', 'long', 'short', 'wins', 'losses'].map(kind => `filter-pill filter-pill-${kind} active`)].map(cls => {
        const button = document.createElement('button');
        button.className = cls.startsWith('btn-') ? `btn ${cls}` : cls;
        document.body.append(button);
        const s = getComputedStyle(button);
        const fg = luminance(s.color.match(/[\d.]+/g)!.slice(0, 3).map(Number));
        const background = s.backgroundImage;
        const stops = [...background.matchAll(/rgb\(([^)]+)\)/g)].map(m => luminance(m[1].split(',').map(Number)));
        button.remove();
        return { cls: `${tint} ${cls}`, background, ratios: stops.map(bg => (Math.max(fg, bg) + 0.05) / (Math.min(fg, bg) + 0.05)) };
      });
    });
  });
  for (const result of ratios) {
    expect(result.ratios.length, JSON.stringify(result)).toBe(2);
    for (const ratio of result.ratios) expect(ratio, result.cls).toBeGreaterThanOrEqual(4.5);
  }
  await page.goto('/app#cascade');
  for (const theme of ['dark', 'light']) {
    await page.evaluate(t => document.documentElement.dataset.theme = t, theme);
    await page.screenshot({ path: `visual-evidence/cascade-${theme}-desktop.png`, fullPage: true });
  }
});
