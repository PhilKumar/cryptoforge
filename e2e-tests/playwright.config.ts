import { defineConfig, devices } from '@playwright/test';

const requestedBrowser = process.env.E2E_BROWSER;
if (requestedBrowser && !['chromium', 'firefox', 'webkit'].includes(requestedBrowser)) {
  throw new Error(`Unsupported E2E_BROWSER: ${requestedBrowser}`);
}
const browserName = requestedBrowser as 'chromium' | 'firefox' | 'webkit' | undefined;
const crossBrowserUse = browserName
  ? { ...devices['Desktop Chrome'], browserName, serviceWorkers: 'block' as const }
  : null;
const projects = browserName
  ? [{ name: `cryptoforge-${browserName}`, use: crossBrowserUse }]
  : [
      {
        name: 'cryptoforge',
        use: { ...devices['Desktop Chrome'] },
      },
    ];

export default defineConfig({
  testDir: './tests',
  fullyParallel: false,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  reporter: [['html', { open: 'never' }], ['list']],
  use: {
    baseURL: process.env.E2E_BASE_URL || 'http://localhost:8001',
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
  },
  projects,
});
