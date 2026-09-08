import { test, expect, Page } from '@playwright/test';

/**
 * The four numbers, small, beside the heading, and coloured by what they mean.
 *
 * Phil, 2026-09-08: "make these 4 tiles decreasing the size and place them next
 * to the heading with giving the values the appropriate colours".
 *
 * The colour is the part worth guarding. It is set from the NUMBERS in JS, not
 * from the markup, so a tile cannot end up permanently green because someone
 * typed a class. And only the tiles with a verdict are coloured: purse is the
 * size of the book and has no good or bad, so it keeps the ordinary text
 * colour. Colouring all four would be decoration — if everything is coloured,
 * nothing is being said.
 */

const PIN = process.env.E2E_PIN || '123456';
const USER = process.env.E2E_USER || 'admin';

function autoBooks(inCoin: number, pocket: number) {
  return {
    armed: false,
    campaigns: [],
    closed_campaigns: [],
    exchanges: [],
    books: [
      {
        symbol: 'BTCUSDT', enabled: true, purse_usd: 2000, pocket_usd: pocket,
        wallet_cap_usd: 1000, fold_threshold_usd: 500, in_coin_usd: inCoin,
        campaigns: 3, working_line: true,
      },
    ],
  };
}

async function openAuto(page: Page, payload: unknown) {
  await page.route('**/api/auto-fib/status', (route) =>
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(payload) }),
  );
  await page.goto('/app');
  await page.fill('#username-input', USER);
  await page.fill('#password-input', PIN);
  await page.click('#unlock-btn');
  await page.waitForSelector('.nav-tab', { timeout: 15_000 });
  // The nav exists before the app script finishes defining showPage.
  await page.waitForFunction(() => typeof (window as any).showPage === 'function');
  await page.evaluate(() =>
    (window as any).showPage('autofib-page', document.getElementById('nav-strategies')),
  );
  await expect(page.locator('#cf-af-stats')).toBeVisible();
}

test.describe('The header stat strip', () => {
  test('sits BESIDE the heading, on the same row', async ({ page }) => {
    await openAuto(page, autoBooks(155.2, 8.5));
    await expect(page.locator('#cf-af-stats .stat-box')).toHaveCount(4);
    // Phil, twice: not under the heading. Level with it.
    const m = await page.evaluate(() => {
      const h = document.querySelector('#autofib-page .allocator-header h2')!.getBoundingClientRect();
      const s = document.getElementById('cf-af-stats')!.getBoundingClientRect();
      return { drop: Math.abs(s.top - h.top), left: s.left - h.right };
    });
    expect(m.drop, 'strip is level with the heading, not below it').toBeLessThan(60);
    expect(m.left, 'strip starts to the RIGHT of the heading').toBeGreaterThan(0);
  });

  test('stretches to fill the row rather than hugging its content', async ({ page }) => {
    await openAuto(page, autoBooks(155.2, 8.5));
    const m = await page.evaluate(() => {
      const strip = document.getElementById('cf-af-stats')!;
      const header = strip.parentElement!.getBoundingClientRect();
      return { strip: strip.getBoundingClientRect().width, header: header.width };
    });
    // "Don't make bigger panels and leave space on both sides" — the strip
    // takes what the title and the chip do not, which is most of the row.
    expect(m.strip / m.header).toBeGreaterThan(0.55);
  });

  test('four equal tiles, all on ONE row', async ({ page }) => {
    await openAuto(page, autoBooks(155.2, 8.5));
    const boxes = await page.locator('#cf-af-stats .stat-box').evaluateAll((els) =>
      els.map((e) => { const r = e.getBoundingClientRect(); return { w: Math.round(r.width), top: Math.round(r.top) }; }),
    );
    expect(new Set(boxes.map((b) => b.top)).size, 'all tiles share one row').toBe(1);
    const widths = new Set(boxes.map((b) => b.w));
    expect(widths.size, 'tiles are equal width').toBe(1);
  });

  test('the tile is one line, not three', async ({ page }) => {
    await openAuto(page, autoBooks(155.2, 8.5));
    const h = (await page.locator('#cf-af-stats .stat-box').first().boundingBox())!.height;
    // The stacked card this replaces was ~90px. One line of 15px text with
    // 8px padding cannot exceed ~45px, so this fails the moment it re-stacks.
    expect(h).toBeLessThan(50);
  });

  test('comfortable exposure and earned profit read good', async ({ page }) => {
    await openAuto(page, autoBooks(155.2, 8.5));   // $155 of a $1,000 limit
    await expect(page.locator('#cf-af-stat-incoin')).toHaveClass(/is-good/);
    await expect(page.locator('#cf-af-stat-pocket')).toHaveClass(/is-good/);
  });

  test('exposure near its limit turns to a warning', async ({ page }) => {
    await openAuto(page, autoBooks(900, 0));       // $900 of a $1,000 limit
    await expect(page.locator('#cf-af-stat-incoin')).toHaveClass(/is-watch/);
  });

  test('a book holding nothing reads idle, not good', async ({ page }) => {
    await openAuto(page, autoBooks(0, 0));
    await expect(page.locator('#cf-af-stat-incoin')).toHaveClass(/is-idle/);
    await expect(page.locator('#cf-af-stat-pocket')).toHaveClass(/is-idle/);
  });

  test('purse is never coloured — it has no good or bad', async ({ page }) => {
    await openAuto(page, autoBooks(155.2, 8.5));
    const cls = (await page.locator('#cf-af-stat-purse').getAttribute('class')) || '';
    expect(cls).not.toMatch(/is-good|is-watch|is-idle/);
  });

  test('the tones are legible in BOTH themes', async ({ page }) => {
    await openAuto(page, autoBooks(900, 8.5));
    for (const theme of ['dark', 'light']) {
      await page.evaluate((t) => document.documentElement.setAttribute('data-theme', t), theme);
      await page.waitForTimeout(200);
      for (const id of ['cf-af-stat-incoin', 'cf-af-stat-pocket']) {
        const ratio = await page.locator('#' + id).evaluate((el, dark) => {
          const box = el.closest('.stat-box') as HTMLElement;
          const lum = (c: string) => {
            const m = (c.match(/\d+/g) || []).map(Number).slice(0, 3);
            const s = m.map((v) => { v /= 255; return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4); });
            return 0.2126 * s[0] + 0.7152 * s[1] + 0.0722 * s[2];
          };
          let bg = getComputedStyle(box).backgroundColor;
          if (/rgba\(0, 0, 0, 0\)|transparent/.test(bg)) bg = dark ? 'rgb(11,15,20)' : 'rgb(255,255,255)';
          const a = lum(getComputedStyle(el).color), b = lum(bg);
          return (Math.max(a, b) + 0.05) / (Math.min(a, b) + 0.05);
        }, theme === 'dark');
        expect(ratio, `${id} in ${theme}`).toBeGreaterThanOrEqual(4.5);
      }
    }
  });
});
