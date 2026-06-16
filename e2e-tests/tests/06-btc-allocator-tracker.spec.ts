import { expect, Page, test } from '@playwright/test';

const PIN = process.env.E2E_PIN || '123456';

async function login(page: Page) {
  await page.goto('/');
  for (const digit of PIN.split('')) {
    await page.click(`button.key[data-val="${digit}"]`);
  }
  await page.waitForSelector('.nav-tab', { timeout: 10_000 });
}

async function addBuyRow(page: Page, buyPrice: string, buyAmount: string) {
  await page.fill('#btc-buy-price', buyPrice);
  await page.fill('#btc-buy-value', buyAmount);
  await page.click('.allocator-buy-actions button:has-text("Add Row")');
}

test.describe('BTC Allocator And Buy Tracker', () => {
  test('latest 20% allocation seeds buy tracker and keeps weighted totals correct', async ({ page }) => {
    test.setTimeout(60_000);
    const pageErrors: string[] = [];
    page.on('pageerror', (err) => pageErrors.push(err.message));

    await login(page);
    await page.click('#nav-allocator');
    await expect(page.locator('#allocator-page')).toHaveClass(/active-page/);

    await page.fill('#btc-alloc-high', '66992');
    await page.fill('#btc-alloc-low', '66560');
    await page.click('.allocator-input-panel button:has-text("Calculate")');

    await expect(page.locator('#btc-alloc-result-body')).toContainText('0.64%');
    await expect(page.locator('#btc-alloc-result-body')).toContainText('₹640');
    await expect(page.locator('#btc-alloc-result-body')).toContainText('₹128');
    await expect(page.locator('#btc-alloc-track-hint')).toContainText('Ready: 20% ₹128');
    await expect(page.locator('#btc-buy-value')).toHaveValue('128');

    await page.click('.allocator-buy-actions button:has-text("Fill 30%")');
    await expect(page.locator('#btc-buy-value')).toHaveValue('192');
    await page.click('.allocator-buy-actions button:has-text("Fill 50%")');
    await expect(page.locator('#btc-buy-value')).toHaveValue('320');
    await page.click('.allocator-buy-actions button:has-text("Fill 20%")');
    await expect(page.locator('#btc-buy-value')).toHaveValue('128');

    await page.click('#btc-alloc-track-latest-20');
    await expect(page.locator('#btc-buy-tracker-body tr')).toHaveCount(1);
    await expect(page.locator('#btc-buy-tracker-body')).toContainText('₹66,560');
    await expect(page.locator('#btc-buy-tracker-body')).toContainText('₹128');

    await addBuyRow(page, '66474', '26');
    await addBuyRow(page, '66368', '32');

    await expect(page.locator('#btc-buy-tracker-body tr')).toHaveCount(3);
    await expect(page.locator('#btc-buy-tracker-body')).toContainText('₹66,545');
    await expect(page.locator('#btc-buy-tracker-body')).toContainText('₹66,515');
    await expect(page.locator('#btc-buy-total-value')).toHaveText('₹186');
    await expect(page.locator('#btc-buy-average-price')).toHaveText('₹66,515');
    await expect(page.locator('#btc-buy-average-fund')).toHaveText('₹62');

    await addBuyRow(page, '100', '0');
    await expect(page.locator('#btc-buy-error')).toHaveText('Buy Amount must be greater than 0.');
    await expect(page.locator('#btc-buy-tracker-body tr')).toHaveCount(3);

    await page.locator('#btc-buy-tracker-body button:has-text("Delete")').nth(1).click();
    await expect(page.locator('#btc-buy-tracker-body tr')).toHaveCount(2);

    await page.click('.allocator-buy-actions button:has-text("Reset Tracker")');
    await expect(page.locator('#btc-buy-tracker-body')).toContainText('No BTC buy rows yet');
    await expect(page.locator('#btc-buy-total-value')).toHaveText('₹0');

    await addBuyRow(page, '26000', '100');
    await addBuyRow(page, '26500', '120');
    await addBuyRow(page, '26000', '300');
    await addBuyRow(page, '26000', '2000');
    await expect(page.locator('#btc-buy-tracker-body tr')).toHaveCount(4);
    await expect(page.locator('#btc-buy-average-price')).toHaveText('₹26,024');
    await expect(page.locator('#btc-buy-average-fund')).toHaveText('₹630');

    await page.click('#btc-alloc-clear-history');
    await expect(page.locator('#btc-alloc-history-body')).toContainText('No allocation history yet');

    expect(pageErrors).toEqual([]);
  });
});
