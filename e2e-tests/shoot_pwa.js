/* PWA install screenshots, taken from the page as it actually renders.
   Wide gets the film (>=880px); narrow deliberately does not — below 880 the
   performance contract stands the posters in, and the screenshot should show
   what a phone really gets. */
const { chromium } = require('playwright');
const OUT = process.argv[2] || '/tmp';
const BASE = process.argv[3] || 'http://127.0.0.1:8123';

(async () => {
  const b = await chromium.launch();
  for (const [name, vp, dsf] of [['wide', { width: 1280, height: 800 }, 1],
                                 ['narrow', { width: 375, height: 667 }, 2]]) {
    const p = await b.newPage({ viewport: vp, deviceScaleFactor: dsf });
    await p.goto(`${BASE}/index.html`, { waitUntil: 'load' });
    await p.waitForTimeout(name === 'wide' ? 4000 : 1500);
    await p.screenshot({ path: `${OUT}/${name}.jpg`, quality: 88, type: 'jpeg' });
    const size = await p.evaluate(() => ({ w: innerWidth, h: innerHeight }));
    console.log(name, JSON.stringify(size));
    await p.close();
  }
  await b.close();
})();
