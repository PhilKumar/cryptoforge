import { test, expect, Page } from '@playwright/test';

/**
 * The closed-rounds table on a live campaign card.
 *
 * This had no coverage, and a bug shipped straight through: a hand-built pager
 * added under the table read `rFrom`/`rPage`/`rPages`, none of which were ever
 * declared. `_cfCascadePositionPanel` therefore threw ReferenceError for any
 * campaign holding a closed round, and the `mount.innerHTML` that follows it
 * never ran — the entire live campaign list rendered blank. It stayed invisible
 * only because no active campaign had a closed round at the time, so the first
 * take-profit to land would have blanked the panel.
 *
 * Two things are asserted, because they fail differently:
 *
 *  1. The panel RENDERS at all with rounds present — a page error here means
 *     the card list is gone, not merely that one number is wrong.
 *  2. The round is reported NET of commission, with the fee shown beside it.
 *     Cascade booked gross P&L until fees were modelled (AUDIT §1.2), so a
 *     round that silently loses its fee column has regressed to the old number.
 *
 * Like 06, this suite CREATES NOTHING. Cascade trades mainnet; the campaign
 * here is a fixture served over an intercepted status call.
 */

const PIN = process.env.E2E_PIN || '123456';
const USER = process.env.E2E_USER || 'admin';
const T0 = 1750000000;

async function login(page: Page) {
  await page.goto('/app');
  // Accounts, since 2026-08-17: username + password (the seeded admin's
  // password is CRYPTOFORGE_PIN, which is what E2E_PIN carries).
  await page.fill('#username-input', USER);
  await page.fill('#password-input', PIN);
  await page.click('#unlock-btn');
  await page.waitForSelector('.nav-tab', { timeout: 10_000 });
  await page.waitForFunction(() => typeof (window as any).cfLoadCascadeStatus === 'function');
}

const NOISE = /Failed to load resource|favicon|fonts\.googleapis|manifest|service worker|WebSocket|net::ERR_/i;

function watchErrors(page: Page) {
  const errors: string[] = [];
  page.on('pageerror', (err) => errors.push(`pageerror: ${err.message}`));
  page.on('console', (msg) => {
    if (msg.type() === 'error') errors.push(`console: ${msg.text()}`);
  });
  return () => errors.filter((e) => !NOISE.test(e));
}

/** One round, in the shape engine/cascade.py Round.to_dict() returns. */
function round_(id: number, opts: { fees?: number } = {}) {
  const gross = 10.0;
  const fees = opts.fees ?? 0.21;
  return {
    round_id: id,
    leg_id: 1,
    avg_entry: 100.0,
    quantity: 1.0,
    invested_usd: 100.0,
    exit_price: 110.0,
    pnl: Number((gross - fees).toFixed(8)),
    pnl_gross: gross,
    fees_usd: fees,
    closed_at: '2026-08-01 12:00:00',
    opened_ts: T0,
    closed_ts: T0 + 3600,
    fills: [{ timestamp: T0, price: 100.0, quantity: 1.0, usd: 100.0, level: 2, leg_id: 1, order_id: 'x' }],
  };
}

// `mode` is a parameter because the closed-round LEDGER is a money ledger and
// skips paper campaigns outright. The card tests below want the paper default
// they have always had; the ledger test has to ask for a live one or it renders
// an empty table and every assertion below it is meaningless.
function statusWith(rounds: Array<Record<string, unknown>>, mode = 'paper') {
  const fees = rounds.reduce((s, r) => s + Number(r.fees_usd || 0), 0);
  const pnl = rounds.reduce((s, r) => s + Number(r.pnl || 0), 0);
  return {
    status: 'ok',
    running: true,
    active_count: 1,
    campaigns: [
      {
        campaign_id: 'e2e-rounds-fixture',
        seq: 1,
        symbol: 'SOLUSDT',
        mode,
        state: 'TRENDLINE_ACTIVE',
        mc_kind: 'major',
        timeframe: '5m',
        capital_usd: 1000,
        mother_high: 120.0,
        mother_low: 95.0,
        mother_timestamp: T0,
        legs: [],
        all_fills: [],
        rounds,
        rounds_closed: rounds.length,
        realized_pnl_total: Number(pnl.toFixed(2)),
        fees_total: Number(fees.toFixed(4)),
        spent_usd: 0,
        resting_usd: 0,
        pending_usd: 0,
        last_price: 105.0,
        model_version: 21,
        stale_model: false,
      },
    ],
    closed_campaigns: [],
    instruments: {},
    capital_groups: {},
  };
}

async function serveStatus(page: Page, payload: Record<string, unknown>) {
  await page.route('**/api/cascade/status', (route) =>
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(payload) }),
  );
}

/**
 * Put the fixture on screen for real: the Cascade page has to be the visible
 * section, and cards are collapsed until clicked. Asserting against a card that
 * is merely in the DOM would pass on markup nobody can see, which is most of
 * what went wrong here in the first place.
 */
async function showCampaign(page: Page) {
  await page.evaluate(() => (window as any).showPage(
    'cascade-page', (window as any).cfNavButtonForPage('cascade-page'), { skipHistory: true },
  ));
  await page.evaluate(() => (window as any).cfLoadCascadeStatus(false));
  await expect(page.locator('#cf-cascade-campaigns .cf-cascade-card')).toHaveCount(1);
  // Toggled through the function rather than a click on the header: the header
  // is mostly taken up by the scrolling title strip, which swallows the event.
  // Whether that strip should be clickable is 06's business, not this file's.
  await page.evaluate(() => (window as any).cfCascadeToggleCard('e2e-rounds-fixture'));
  await expect(page.locator('#cf-cascade-campaigns .cf-cascade-card.is-open')).toHaveCount(1);
}

/**
 * The Closed Rounds LEDGER — the flat table under the campaign list, which is
 * the one actually read day to day. It had no Fees column at all, so the "~"
 * that says a fee was modelled rather than read back from Binance was only
 * visible inside an expanded campaign card. Phil went looking for it here and
 * there was nothing to find.
 */
function ledgerRound(id: number, over: Record<string, unknown> = {}) {
  return {
    round_id: id, leg_id: 6, avg_entry: 72.69, exit_price: 73.09, quantity: 0.357,
    invested_usd: 25.95, pnl: 0.1038, pnl_gross: 0.1428, fees_usd: 0.039, fees_estimated: false,
    closed_ts: T0 + id, closed_at: '2026-08-03 19:15:00', opened_ts: T0, fills: [{}, {}],
    ...over,
  };
}

test.describe('Cascade closed-rounds ledger', () => {
  test('the ledger shows what each round paid, and says which figures are guesses', async ({ page }) => {
    await login(page);
    // Explicit close times: the ledger sorts newest first, so leaving them to
    // fall out of the loop index puts the rows in the opposite order to the
    // one the assertions below read.
    const measured = ledgerRound(1, { closed_ts: T0 + 300 });
    const modelled = ledgerRound(2, { closed_ts: T0 + 200, fees_usd: 0.052, fees_estimated: true });
    const legacy = ledgerRound(3, { closed_ts: T0 + 100 });
    delete (legacy as Record<string, unknown>).fees_usd;
    delete (legacy as Record<string, unknown>).fees_estimated;
    await serveStatus(page, statusWith([measured, modelled, legacy], 'live'));
    await page.evaluate(() => (window as any).showPage(
      'cascade-page', (window as any).cfNavButtonForPage('cascade-page'), { skipHistory: true },
    ));
    await page.evaluate(() => (window as any).cfLoadCascadeStatus(false));

    const rows = page.locator('#cf-cascade-ledger-body tr');
    await expect(rows).toHaveCount(3);
    // Header, body and the empty-state colspan all have to agree, or the table
    // shears sideways the moment a column is added.
    await expect(page.locator('.cf-cascade-ledger thead th')).toHaveCount(14);
    await expect(rows.first().locator('td')).toHaveCount(14);

    // Read back from the exchange: a bare figure.
    await expect(rows.nth(0).locator('td').nth(9)).toHaveText('-$0.04');
    // Modelled at the standard rate: marked, because it cannot see the BNB
    // discount and is therefore an approximation.
    await expect(rows.nth(1).locator('td').nth(9)).toHaveText('~-$0.05');
    // Closed before fees were recorded at all — unknown, never "$0.00", which
    // would claim the round traded free.
    await expect(rows.nth(2).locator('td').nth(9)).toHaveText('--');

    const meta = page.locator('#cf-cascade-ledger-meta');
    await expect(meta).toContainText('after $0.09 fees');
    await expect(meta, 'the total admits how much of itself is modelled')
      .toContainText('$0.05 of it estimated');
  });

  test('paper rounds are listed but never counted in the money', async ({ page }) => {
    await login(page);
    // Three rounds on a PAPER campaign, $77.85 deployed and +$0.31 realised.
    // That used to be added to the live totals, which made the account read
    // bigger than it is. Excluding the ROWS as well was the overcorrection:
    // 26 trades read as 20 and looked like six had gone missing.
    await serveStatus(page, statusWith([ledgerRound(1), ledgerRound(2), ledgerRound(3)], 'paper'));
    await page.evaluate(() => (window as any).showPage(
      'cascade-page', (window as any).cfNavButtonForPage('cascade-page'), { skipHistory: true },
    ));
    await page.evaluate(() => (window as any).cfLoadCascadeStatus(false));

    // Every row still there, and each one says why it is not in the total.
    const rows = page.locator('#cf-cascade-ledger-body tr');
    await expect(rows).toHaveCount(3);
    await expect(rows.first()).toContainText('paper — not counted');

    const meta = page.locator('#cf-cascade-ledger-meta');
    await expect(meta, 'no paper money in the deployed figure').toContainText('0 live rounds closed');
    await expect(meta).toContainText('$0.00 deployed');
    await expect(meta).toContainText('+$0.00 realised');
    await expect(meta, 'the rows below are accounted for').toContainText('plus 3 paper rounds listed below');
    await expect(meta).not.toContainText('$77.85');
  });

  test('a live and a paper campaign side by side: all rows, live-only totals', async ({ page }) => {
    await login(page);
    const status = statusWith([ledgerRound(1, { invested_usd: 10.00, pnl: 0.40, fees_usd: 0.01 })], 'live');
    // A second campaign in the same payload, on paper, with much bigger numbers.
    (status.campaigns as Array<Record<string, unknown>>).push({
      ...(status.campaigns as Array<Record<string, unknown>>)[0],
      campaign_id: 'e2e-paper-fixture', seq: 2, symbol: 'BTCUSDT', mode: 'paper',
      rounds: [ledgerRound(9, { invested_usd: 900.00, pnl: 45.00, fees_usd: 0.90 })],
    });
    await serveStatus(page, status);
    await page.evaluate(() => (window as any).showPage(
      'cascade-page', (window as any).cfNavButtonForPage('cascade-page'), { skipHistory: true },
    ));
    await page.evaluate(() => (window as any).cfLoadCascadeStatus(false));

    await expect(page.locator('#cf-cascade-ledger-body tr')).toHaveCount(2);
    const meta = page.locator('#cf-cascade-ledger-meta');
    await expect(meta).toContainText('1 live round closed at target');
    await expect(meta).toContainText('$10.00 deployed');
    await expect(meta).toContainText('+$0.40 realised');
    await expect(meta).toContainText('plus 1 paper round listed below');
    // The paper campaign's $900 and $45 are visible in its row and in no total.
    await expect(meta).not.toContainText('910.00');
    await expect(meta).not.toContainText('45.40');
  });
});

test.describe('Cascade closed-rounds table', () => {
  test('a campaign holding closed rounds still renders its card', async ({ page }) => {
    const errors = watchErrors(page);
    await login(page);
    await serveStatus(page, statusWith([round_(1), round_(2)]));

    // The whole point: the mount has content. Before the fix this was empty,
    // because the render threw before ever assigning innerHTML.
    await showCampaign(page);
    await expect(page.locator('#cf-cascade-campaigns table.cf-cascade-rounds')).toBeVisible();
    expect(errors(), 'rendering a card with rounds raises nothing').toEqual([]);
  });

  test('rounds report P&L net of commission, with the fee beside it', async ({ page }) => {
    await login(page);
    await serveStatus(page, statusWith([round_(1)]));
    await showCampaign(page);

    const table = page.locator('#cf-cascade-campaigns table.cf-cascade-rounds');
    await expect(table.locator('thead th', { hasText: 'Fees' })).toHaveCount(1);
    // $10.00 gross less $0.21 — the net figure is what the row must show.
    await expect(table.locator('tbody tr').first()).toContainText('+$9.79');
    await expect(table.locator('tbody tr').first()).toContainText('-$0.21');
    await expect(page.locator('#cf-cascade-campaigns .cf-cascade-position.is-closed'))
      .toContainText('after $0.21 fees');
  });

  test('a round closed before fees were recorded reads as unknown, not as free', async ({ page }) => {
    await login(page);
    // No fees_usd/pnl_gross at all — exactly how rounds written by the older
    // engine restore. Showing "$0.00" would claim it paid no commission.
    const legacy = { ...round_(1), pnl: 10.0 };
    delete (legacy as Record<string, unknown>).fees_usd;
    delete (legacy as Record<string, unknown>).pnl_gross;
    await serveStatus(page, statusWith([legacy]));
    await showCampaign(page);

    const row = page.locator('#cf-cascade-campaigns table.cf-cascade-rounds tbody tr').first();
    await expect(row).toContainText('+$10.00');
    await expect(row).not.toContainText('-$0.00');
    await expect(page.locator('#cf-cascade-campaigns .cf-cascade-position.is-closed'))
      .not.toContainText('fees');
  });
});

/**
 * The strip's own controls.
 *
 * These are here because the "⋯" menu shipped to production completely dead
 * and every check I ran said it worked — because every check called
 * cfCascadeToggleMenu() directly. The button is wired through data-cf-click,
 * which is ONE listener on document, and the outside-click handler that closes
 * the menu is another. Calling the function skips both. Only a real click that
 * bubbles to document exercises the order they run in, which is where the bug
 * was: the menu opened and the closer shut it again in the same tick.
 *
 * So: no page.evaluate shortcuts in here. Clicks only.
 */
test.describe('Cascade strip controls', () => {
  test('the ⋯ menu opens on a real click and stays open', async ({ page }) => {
    await login(page);
    await serveStatus(page, statusWith([round_(1)]));
    await showCampaign(page);

    const menu = page.locator('.cf-cascade-card .cf-cascade-menu');
    await expect(menu).toBeHidden();

    await page.click('.cf-cascade-card .cf-cascade-more-btn');
    await expect(menu, 'a click that bubbles to document must leave it open').toBeVisible();

    // It also has to survive the 3s status poll rebuilding the whole panel.
    await page.waitForTimeout(3500);
    await expect(menu, 'the repaint must not close it').toBeVisible();
  });

  test('the menu closes on a second click, on an outside click, and on Escape', async ({ page }) => {
    await login(page);
    await serveStatus(page, statusWith([round_(1)]));
    await showCampaign(page);
    const menu = page.locator('.cf-cascade-card .cf-cascade-menu');
    const more = '.cf-cascade-card .cf-cascade-more-btn';

    await page.click(more);
    await page.click(more);
    await expect(menu, 'second click toggles it shut').toBeHidden();

    await page.click(more);
    // Anywhere that is not the actions strip counts as outside — the stats row
    // inside the same card is the closest such place, so it is the strictest
    // check of where the guard draws the line.
    await page.locator('.cf-cascade-card .cf-cascade-stats').first().click();
    await expect(menu, 'a click elsewhere puts it away').toBeHidden();

    await page.click(more);
    await page.keyboard.press('Escape');
    await expect(menu, 'Escape puts it away').toBeHidden();
  });

  test('only Chart and Stop ride on the strip', async ({ page }) => {
    await login(page);
    await serveStatus(page, statusWith([round_(1)]));
    await showCampaign(page);

    const stripButtons = page.locator('.cf-cascade-card .cf-cascade-actions > button');
    await expect(stripButtons).toHaveCount(2);
    await expect(stripButtons.nth(0)).toHaveText('Chart');
    await expect(stripButtons.nth(1)).toHaveText('Stop');
    // Delete is reachable, but never one stray click away.
    await expect(page.locator('.cf-cascade-card .cf-cascade-menu-item.is-danger')).toHaveText('Delete');
  });

  test('the state pill says a word, not the engine constant', async ({ page }) => {
    await login(page);
    await serveStatus(page, statusWith([round_(1)]));
    await showCampaign(page);

    const pill = page.locator('.cf-cascade-card .cf-cascade-title .admin-pill').first();
    await expect(pill).toHaveText('Active');
    await expect(pill).toHaveAttribute('title', /TRENDLINE_ACTIVE/);
  });

  test('hovering the strip stops it scrolling, and letting go resumes in place', async ({ page }) => {
    await login(page);
    await serveStatus(page, statusWith([round_(1)]));
    await showCampaign(page);

    const view = page.locator('.cf-cascade-card .cf-cascade-title-view');
    // The COMPUTED state of the track, not a CSS variable we set ourselves —
    // the first version asserted its own bookkeeping and passed while the
    // strip visibly lurched on screen.
    const playState = () => view.evaluate((el) =>
      getComputedStyle(el.firstElementChild as Element).animationPlayState);
    const delay = () => view.evaluate((el) => parseFloat((el as HTMLElement).style.getPropertyValue('--cf-marquee-delay')));
    const trackNode = () => view.evaluate((el) => (el.firstElementChild as HTMLElement).dataset.probe);

    // Only a clipped strip scrolls; a short one has nothing to pause.
    const clipped = await view.evaluate((el) => el.classList.contains('is-clipped'));
    test.skip(!clipped, 'this fixture’s strip fits, so there is no marquee to pause');

    // Mark the live track so we can tell whether hovering rebuilt it. It must
    // not: tearing the cloned pill group out to re-measure is what made the
    // strip lurch before it settled.
    await view.evaluate((el) => { (el.firstElementChild as HTMLElement).dataset.probe = 'original'; });
    await view.hover();
    expect(await playState(), 'the pointer arriving must stop it').toBe('paused');
    expect(await trackNode(), 'hovering must not rebuild the track').toBe('original');

    const cycle = await view.evaluate((el) => parseFloat((el as HTMLElement).style.getPropertyValue('--cf-marquee-time')));
    const advanceFrom = (a: number, b: number) => (((a - b) % cycle) + cycle) % cycle;

    // THE symptom this test exists for: the strip stops on screen the instant
    // the pointer lands (CSS), but the phase is only recomputed when the panel
    // repaints. If the hover is not booked the moment it happens, that repaint
    // computes a phase several seconds later and the "stopped" text lurches
    // once before settling. So: read the phase, hover, and check that the
    // first repaint under the cursor advanced it only by the time the strip
    // was genuinely still running — not by the whole time it was held.
    // Take the poll out of the loop so repaints happen only when this test says
    // so. With it running, a rebuild lands somewhere inside every measurement
    // window and the difference between "booked on hover" and "booked at the
    // next repaint" is unobservable — which is exactly how the defect survived.
    await page.evaluate(() => { (window as any).cfLoadCascadeStatus = () => {}; });
    const repaint = () => page.evaluate(() =>
      (window as any).cfRenderCascadeStatus((window as any)._cfCascadeLastStatus));

    // Off the strip first. The hover above already froze it, and measuring the
    // arrival while the pointer is still down compares a frozen phase with
    // itself — which passes whatever the code does, and is how the first two
    // attempts at this test fooled me.
    await page.mouse.move(0, 0);
    await repaint();
    const beforeHover = await delay();
    await view.hover();
    // Held for well over a second with no repaint in between. The next repaint
    // must place the strip where it was when the pointer landed — if the hover
    // is only booked at repaint time, this second and a half counts as running
    // and the "stopped" text lurches forward before settling.
    await page.waitForTimeout(1500);
    await repaint();
    const held = await delay();
    expect(await playState(), 'still held after the panel rebuilt under the cursor').toBe('paused');
    expect(
      advanceFrom(beforeHover, held),
      'the first repaint after hovering must not jump the strip forward',
    ).toBeLessThan(0.8);

    await page.waitForTimeout(1500);
    await repaint();

    // Frozen has to mean frozen across further repaints. The panel destroys the
    // hovered node every 3s and Chrome fires mouseout for it with a null
    // relatedTarget, which used to un-freeze a strip the pointer never left.
    await page.waitForTimeout(3500);
    expect(await playState(), 'two repaints later, still held').toBe('paused');
    expect(await delay(), 'a held strip must not advance').toBeCloseTo(held, 1);

    await page.mouse.move(0, 0);
    expect(await playState(), 'letting go starts it again').toBe('running');
    // It carries on from the frozen frame rather than snapping to wherever the
    // shared clock reached while it was held — that is what the paused-time
    // ledger buys. Without it this jumps by the entire hover duration, which
    // here is over three seconds of a six second cycle.
    await page.waitForTimeout(1200);
    await repaint();
    expect(
      advanceFrom(held, await delay()),
      'on release it carries on from the frozen frame, not from the live clock',
    ).toBeLessThan(2.2);
  });
});

/**
 * The menu against the cards below it.
 *
 * Every .card carries a backdrop-filter, which makes each one its own stacking
 * context — so the menu's z-index cannot lift it above campaigns that come
 * later in the DOM, and they painted straight over it. Phil saw "Restructure"
 * sliced in half by the next card.
 *
 * It needs the GROUPED layout to show up: with a stack header above them the
 * rows sit close enough that the menu reaches well into the next card. Without
 * the header the menu happened to clear it, which is why a first attempt at
 * reproducing this found nothing and pronounced the bug imaginary.
 */
function statusWithStack() {
  const mk = (seq: number, id: string, symbol: string) => ({
    campaign_id: id, seq, symbol, state: 'TRENDLINE_ACTIVE', mode: 'paper',
    capital_usd: 2000, mother_high: 65000, mother_low: 63000, last_price: 63900,
    legs: [], rounds: [], all_fills: [], timeframe: '5m', mc_kind: 'major',
    spent_usd: 34.48, resting_usd: 0, fall_pct_from_mother: 1.2, allocated_pct: 0.4,
  });
  return {
    status: 'ok',
    campaigns: [mk(69, 'c69', 'BTCUSDT'), mk(76, 'c76', 'BTCUSDT'), mk(67, 'c67', 'PAXGUSDT'), mk(74, 'c74', 'SOLUSDT')],
    instruments: {
      BTCUSDT: {
        budget_usd: 2000, available_usd: -2000, in_position_usd: 34.48,
        committed_usd: 4000, live_count: 2, timeframes: ['1h', '5m'], realized_pnl_usd: 0,
      },
    },
    closed_campaigns: [],
    capital_groups: {},
  };
}

test.describe('Cascade strip menu stacking', () => {
  test('the menu paints over the campaigns below it, not under them', async ({ page }) => {
    await login(page);
    await serveStatus(page, statusWithStack());
    await page.evaluate(() => (window as any).showPage(
      'cascade-page', (window as any).cfNavButtonForPage('cascade-page'), { skipHistory: true },
    ));
    await page.evaluate(() => (window as any).cfLoadCascadeStatus(false));
    await expect(page.locator('#cf-cascade-campaigns .cf-cascade-card')).toHaveCount(4);
    await expect(page.locator('.cf-cascade-stack-head'), 'the grouped layout is what exposes it').toHaveCount(1);

    // Second card, as in the report — it has campaigns both above and below.
    await page.locator('.cf-cascade-card').nth(1).scrollIntoViewIfNeeded();
    await page.locator('.cf-cascade-card').nth(1).locator('.cf-cascade-more-btn').click();
    const menu = page.locator('.cf-cascade-card .cf-cascade-menu:not([hidden])');
    await expect(menu).toBeVisible();

    // Hit-test the LAST item: it reaches deepest into the card below, so it is
    // the first thing to disappear under it. Asserted on what is actually on
    // top at that point rather than on any z-index we set ourselves — the
    // reader of this pixel is the only witness that matches what Phil saw.
    const topmost = await menu.evaluate((el) => {
      const items = el.querySelectorAll('.cf-cascade-menu-item');
      const item = items[items.length - 1] as HTMLElement;
      const b = item.getBoundingClientRect();
      const hit = document.elementFromPoint(Math.round(b.x + b.width / 2), Math.round(b.y + b.height / 2));
      return hit ? String((hit as HTMLElement).className) : 'nothing';
    });
    expect(topmost, 'a campaign below must not be painted over the open menu')
      .toContain('cf-cascade-menu-item');
  });

  test('an instrument\u2019s campaigns are drawn inside its group, and others are not', async ({ page }) => {
    await login(page);
    await serveStatus(page, statusWithStack());
    await page.evaluate(() => (window as any).showPage(
      'cascade-page', (window as any).cfNavButtonForPage('cascade-page'), { skipHistory: true },
    ));
    await page.evaluate(() => (window as any).cfLoadCascadeStatus(false));
    await expect(page.locator('#cf-cascade-campaigns .cf-cascade-card')).toHaveCount(4);

    // The header used to be a sibling of the cards: it said where BTCUSDT
    // began and never where it ended, so with PAXG and SOL underneath there was
    // nothing on screen saying which rows the $2,000 budget covered.
    const stack = page.locator('.cf-cascade-stack');
    await expect(stack).toHaveCount(1);
    await expect(stack.locator('> .cf-cascade-stack-head'), 'the header is the lid of the group')
      .toHaveCount(1);
    await expect(stack.locator('.cf-cascade-card'), 'both BTCUSDT campaigns live in it').toHaveCount(2);
    await expect(stack).toContainText('BTCUSDT');
    await expect(stack).toContainText('free of $2,000.00');

    // A lone campaign with no budget is not a group; a box round one card is a
    // box drawn for its own sake.
    await expect(
      page.locator('#cf-cascade-campaigns > .cf-cascade-card'),
      'PAXG and SOL stay outside any wrapper',
    ).toHaveCount(2);

    // And the wrapper must not become a stacking context of its own, or it
    // would bury the open menu exactly the way the cards did.
    expect(await stack.evaluate((el) => getComputedStyle(el).backdropFilter)).toBe('none');
  });
});
