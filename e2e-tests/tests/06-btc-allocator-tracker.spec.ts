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

    await page.evaluate(() => {
      localStorage.setItem('cf_btc_allocation_state_v1', JSON.stringify({
        previousTotalAllocation: 2440,
        previousHigh: 65725,
        lastResult: {
          bitcoinHigh: 65725,
          bitcoinLow: 65635,
          fallPercent: 0.137,
          totalAllocationRequired: 137,
          previousAllocation: 2440,
          freshAllocation: 0,
          split20: 0,
          split30: 0,
          split50: 0,
        },
        history: [
          {
            createdAt: new Date().toISOString(),
            bitcoinHigh: 65725,
            bitcoinLow: 65635,
            fallPercent: 0.137,
            totalAllocationRequired: 137,
            previousAllocation: 2440,
            freshAllocation: 0,
            split20: 0,
            split30: 0,
            split50: 0,
          },
          {
            createdAt: new Date().toISOString(),
            bitcoinHigh: 66992,
            bitcoinLow: 65360,
            fallPercent: 2.437,
            totalAllocationRequired: 2436,
            previousAllocation: 930,
            freshAllocation: 1506,
            split20: 301,
            split30: 452,
            split50: 753,
          },
        ],
        buyRows: [],
      }));
      (window as any)._btcAllocationLoadState();
      (window as any).renderBtcAllocationCalculator();
    });
    await expect(page.locator('#btc-alloc-memory')).toHaveText('₹0');
    await expect(page.locator('#btc-alloc-result-body')).toContainText('No calculation yet');

    await page.fill('#btc-alloc-high', '65725');
    await page.fill('#btc-alloc-low', '65635');
    await page.click('.allocator-input-panel button:has-text("Calculate")');

    const resultCells = page.locator('#btc-alloc-result-body td');
    await expect(resultCells.nth(0)).toHaveText('0.137%');
    await expect(resultCells.nth(1)).toHaveText('₹137');
    await expect(resultCells.nth(2)).toHaveText('₹0');
    await expect(resultCells.nth(3)).toHaveText('₹137');
    await expect(resultCells.nth(4)).toHaveText('₹27');
    await expect(resultCells.nth(5)).toHaveText('₹41');
    await expect(resultCells.nth(6)).toHaveText('₹68');
    await expect(page.locator('#btc-alloc-track-hint')).toContainText('Ready: 20% ₹27');
    await expect(page.locator('#btc-buy-value')).toHaveValue('27');

    await page.click('.allocator-buy-actions button:has-text("Fill 30%")');
    await expect(page.locator('#btc-buy-value')).toHaveValue('41');
    await page.click('.allocator-buy-actions button:has-text("Fill 50%")');
    await expect(page.locator('#btc-buy-value')).toHaveValue('68');
    await page.click('.allocator-buy-actions button:has-text("Fill 20%")');
    await expect(page.locator('#btc-buy-value')).toHaveValue('27');

    await page.click('#btc-alloc-track-latest-20');
    await expect(page.locator('#btc-buy-tracker-body tr')).toHaveCount(1);
    await expect(page.locator('#btc-buy-tracker-body')).toContainText('₹65,635');
    await expect(page.locator('#btc-buy-tracker-body')).toContainText('₹27');

    await page.fill('#btc-alloc-low', '65623');
    await page.click('.allocator-input-panel button:has-text("Calculate")');
    await expect(resultCells.nth(0)).toHaveText('0.155%');
    await expect(resultCells.nth(1)).toHaveText('₹155');
    await expect(resultCells.nth(2)).toHaveText('₹137');
    await expect(resultCells.nth(3)).toHaveText('₹18');
    await expect(resultCells.nth(4)).toHaveText('₹4');
    await expect(resultCells.nth(5)).toHaveText('₹5');
    await expect(resultCells.nth(6)).toHaveText('₹9');
    await expect(page.locator('#btc-buy-value')).toHaveValue('4');

    await page.fill('#btc-alloc-high', '66992');
    await page.fill('#btc-alloc-low', '66560');
    await page.click('.allocator-input-panel button:has-text("Calculate")');
    await expect(resultCells.nth(0)).toHaveText('0.645%');
    await expect(resultCells.nth(1)).toHaveText('₹645');
    await expect(resultCells.nth(2)).toHaveText('₹2,436');
    await expect(resultCells.nth(3)).toHaveText('₹0');

    await page.click('.allocator-buy-actions button:has-text("Reset Tracker")');
    await expect(page.locator('#btc-buy-tracker-body')).toContainText('No BTC buy rows yet');
    await expect(page.locator('#btc-buy-total-value')).toHaveText('₹0');

    await addBuyRow(page, '100', '0');
    await expect(page.locator('#btc-buy-error')).toHaveText('Buy Amount must be greater than 0.');
    await expect(page.locator('#btc-buy-tracker-body')).toContainText('No BTC buy rows yet');

    await addBuyRow(page, '26000', '100');
    await addBuyRow(page, '26500', '120');
    await addBuyRow(page, '26000', '300');
    await addBuyRow(page, '26000', '2000');
    await expect(page.locator('#btc-buy-tracker-body tr')).toHaveCount(4);
    await expect(page.locator('#btc-buy-average-price')).toHaveText('₹26,024');
    await expect(page.locator('#btc-buy-average-fund')).toHaveText('₹630');

    await page.locator('#btc-buy-tracker-body button:has-text("Delete")').nth(1).click();
    await expect(page.locator('#btc-buy-tracker-body tr')).toHaveCount(3);

    await page.click('#btc-alloc-clear-history');
    await expect(page.locator('#btc-alloc-history-body')).toContainText('No allocation history yet');
    await expect(page.locator('#btc-alloc-result-body')).toContainText('No calculation yet');
    await expect(page.locator('#btc-alloc-memory')).toHaveText('₹0');

    expect(pageErrors).toEqual([]);
  });
});
