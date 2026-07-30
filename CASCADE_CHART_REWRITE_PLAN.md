# Cascade Chart Rewrite — Status and Plan

**As of 2026-07-30.** All renderer phases are implemented; the final production
acceptance pass remains. The chart you actually use is still the Classic SVG
renderer; nothing about how it draws has changed.

**Scope guarantee, unchanged for every phase below:** no engine, payload or
geometry changes. `get_chart_data` in `engine/cascade.py` is untouched, so
nothing in this work can affect what gets traded.

---

## Where things stand

| Phase | What | Status |
|---|---|---|
| 0 | E2E safety net | **Done** — `7c4a860` |
| 1 | Engine toggle + canvas harness | **Done** — `1690003` |
| 2 | Canvas core (viewport + draw pipeline) | **Done** — `76e636f` |
| 3 | Interaction (pan, zoom, axis-drag, crosshair) | **Done** — `76e636f`, `ebd53c1` |
| 4 | Live updates that keep your viewport | **Done** — manual refresh plus 3s Canvas-poll bridge, pending CI for the latest bridge |
| 5 | Parity with Classic | **Done** — `76e636f` |
| 6 | Verify and hand over | **In progress** — automated checks complete locally; CI and your production visual acceptance remain |

No renderer implementation work remains. The final acceptance needs an
authenticated production session and the next CI/deploy result.

### Note on order

Phase 1 was built before Phase 0, because that was the order you asked for.
Phase 0 then landed before any renderer code was written, which was the point
of putting it first — Phase 2 onward is the code with real crash risk, and it
now has coverage under it.

---

## Done: Phase 0 — the safety net (`7c4a860`)

**File:** `e2e-tests/tests/06-cascade-chart.spec.ts` (new, ~460 lines)

The chart had zero e2e coverage. That is the hole `pal is not defined` walked
through: a one-word typo blanked the entire chart, CI stayed green, and it was
found by hand on a live campaign.

Two layers, for two different failure modes:

- **Renderer** — the chart API is intercepted and a fixture served, so it runs
  on every CI run without a campaign needing to exist. Seven payload shapes ×
  both engines: live, crowded past the draw cap, a same-shelf line bearing no
  fib, bare, a single candle (zero-width time scale), a frozen record with a
  closed round, no candles at all.
- **Payload** — if a campaign exists, its chart opens for real with no
  interception, because fixtures drift from what the server sends. Skips when
  there is nothing to open.

Beyond "it did not throw", it asserts the chart **painted**: candle bodies and
labels in the SVG, a non-blank pixel sample plus a DPR-scaled backing store on
the canvas. Plus what the chart exists to say — gutter labels, dollars on
funded levels, the active-trendline mark, the draw-cap note, the FROZEN RECORD
badge, journal mode.

**It creates nothing.** Cascade runs on mainnet; a test that started a campaign
to have something to chart would be placing real orders.

**Proven, not assumed:** the original typo was reinjected into both renderers.
SVG → four tests red with `pal is not defined` on the panel. Canvas → its sweep
red. Restored, then three consecutive clean runs.

6 tests, ~15s added to CI.

## Done: Phase 1 — toggle and harness (`1690003`)

- `_CF_CHART_ENGINE` + `cfCascadeSetEngine()` — `static/cryptoforge-app.js:9480`,
  persisted in `localStorage` under `cf-chart-engine`. **Classic is the
  default**; Canvas is opt-in, and flipping back is one click.
- Classic/Canvas control in the chart toolbar — `strategy.html`, beside
  Auto/5M/15M/1H.
- `_cfChartMountRenderer()` (`:9514`) — every path that writes the chart body
  goes through one function, so neither engine can be left half-wired.
- `_cfCascadeChartData` — the payload is cached, so flipping engines redraws
  from the *same bars* instead of refetching.
- Canvas harness — `_cfChartCanvasMount/Teardown/Resize/Draw` (`:9818`–`:9876`):
  two stacked surfaces (chart + separate crosshair layer), DPR-correct backing
  store, host that fills the panel instead of a hardcoded 1440×660,
  ResizeObserver-backed lifecycle so refresh and close cannot leak observers
  onto detached nodes.
- CSS — `static/cryptoforge-app.css:7348`.

The canvas draws **no geometry** and says so on its own surface. That is
deliberate: a partial chart at this stage could be mistaken for the real one.

---

## Implementation record

### Phase 2 — the canvas core (~3h)

The first phase that draws. Everything here is new code in
`static/cryptoforge-app.js`, alongside the existing `_cfChartCanvasDraw` stub.

**Activities**

1. **Viewport model** — a `{ tMin, tMax, pMin, pMax }` object: time and price
   ranges held *independently*. Everything else derives from it. This replaces
   viewBox nudging and is what makes independent axis-drag possible in Phase 3.
2. **Projection helpers** — `xOf(t)`, `yOf(p)` and their inverses `tAt(x)`,
   `pAt(y)`, built off the viewport plus plot padding. The inverses are what
   the crosshair and cursor-anchored zoom need.
3. **Fit function** — derive the initial viewport from the payload using the
   same rules Classic uses today (`_cfCascadeChartSvg:9523`): candle high/low,
   mother high, leg touch highs and lows, tp_price, then 6% price padding.
4. **Draw pipeline**, in order: grid + axes → mother column → candles →
   trendlines → fibs → buy/sell markers → labels. Each step its own function,
   so Phase 4 can redraw layers selectively.
5. **Label de-collision** — port the gutter nudge from Classic, including the
   0.5px overshoot that fixed the ETH freeze (see the comment at `:9617`).
   This is a known-sharp edge; it gets copied deliberately, not reinvented.

**How it gets verified**

- `node --check` after each edit (catches syntax only — it is *not* the net).
- The Phase 0 sweep, extended: the canvas branch currently asserts "non-blank
  and DPR-scaled". Once geometry is drawn it gets the same structural
  assertions Classic has. New tests are added here, not deferred to Phase 6.
- Screenshot comparison against Classic on identical payloads, both themes.

**Definition of done:** Canvas draws a campaign that is recognisably the same
chart as Classic on the same data, and the e2e sweep asserts it.

### Phase 3 — interaction (~2h)

The part that makes it feel like a chart rather than a picture.

**Activities**

1. **Drag to pan** — anywhere on the plot, at any zoom. Classic only pans when
   zoomed in past 1× (`_cfChartBindZoom:10163`); this has no such gate.
2. **Wheel zoom about the cursor** — time axis only, the way TradingView does
   it. The bar under the pointer stays under the pointer.
3. **Axis drag — independent, as you chose.** Dragging the price axis stretches
   or compresses price and leaves time alone; dragging the time axis does the
   reverse. This is the specific thing viewBox scaling fundamentally cannot do.
4. **Double-click resets** — on an axis, reset that axis; on the plot, fit
   everything.
5. **Crosshair on the overlay canvas** — already allocated in Phase 1, so
   moving the pointer repaints one cheap surface instead of the whole chart.
6. **Re-show the zoom controls** for Canvas, or replace them with a fit button.
   Phase 1 hides them because they drive the SVG viewBox; that decision gets
   revisited once Canvas has its own zoom.

**How it gets verified**

- Playwright pointer simulation (`mouse.move/down/up`, `mouse.wheel`) asserting
  the viewport object changed in the expected direction and *only* the expected
  axis moved. Independent axis-drag is a behaviour claim, so it gets an
  assertion, not a screenshot.
- Manual pass in the browser on a real campaign.

**Definition of done:** pan, cursor-zoom and both axis drags work and are
covered by tests that would fail if the axes became linked.

### Phase 4 — live updates that do not fight you (~1h)

**Activities**

1. **Keep the viewport across refreshes.** Today `cfCascadeRefreshChart` →
   `cfCascadeShowChart` → `cfCascadeZoomReset()`, so every 3-second poll throws
   away your zoom and pan. That alone is most of why the current chart feels
   un-chartlike.
2. **Auto-follow the right edge only if you were already at it** — TradingView's
   rule. If you have panned back to look at fib 1, a poll must not yank you to
   the present.
3. **Redraw only the layers whose data changed.**

**How it gets verified**

- E2E: pan away from the right edge, fire a refresh with a changed payload,
  assert the viewport is unchanged. Then repeat from the right edge and assert
  it followed. Both directions, because only asserting one would pass on a
  chart that ignored refreshes entirely.

**Definition of done:** a poll never moves the view unless you were at the
right edge.

### Phase 5 — parity (~1.5h)

Everything Classic does, or Canvas is not a replacement. Each item below exists
in Classic today and gets ported and asserted:

- left-gutter labels; dollars on each funded level
- the 3-structure draw cap with the "older hidden" note
- FROZEN RECORD badge and `SOLD AT` exit labels
- journal mode (chart only, no tables)
- Expand / fullscreen
- light and dark themes, including a repaint on theme change — Classic
  re-renders from HTML, Canvas must redraw explicitly
- mother-candle column highlight and `MC` mark
- entry/exit arrows with round P&L

**How it gets verified:** the Phase 0 suite already asserts most of these
against Classic. The parity work is largely *removing the `classic:` prefix*
from those tests and running them against both engines.

**Definition of done:** every Phase 0 assertion that currently runs only on
Classic passes on Canvas too.

### Phase 6 — verify, then hand it over (~1h)

**Activities**

1. Full e2e suite green against both renderers, in CI, not just locally. The
   latest Canvas-poll bridge is covered locally and awaits that CI run.
2. My own read-only pass on a real campaign: 5m / 15m / 1H, dark and light, expanded and
   not, live and frozen.
3. Ship with **Classic still the default**. You compare them side by side on
   live data and flip when you are satisfied.
4. Update `proj_cascade_chart_rewrite_deferred` in memory to reflect reality.

**Definition of done:** you have flipped to Canvas yourself and kept it there.

---

## Tools and skills I will use

**Code**

- `Read` / `Edit` / `Write` — all renderer work is in
  `static/cryptoforge-app.js`, `static/cryptoforge-app.css`, `strategy.html`.
- `Bash` — `node --check` after every JS edit; `git` for commits.

**Testing**

- `Bash` running the repo's own Playwright binary:
  `cd e2e-tests && E2E_PIN=123456 ./node_modules/.bin/playwright test`.
  Its own binary, not `npx` — `npx` pulls a second copy and dies with
  "did not expect test() to be called here".
- A local app on `:8001`, started **isolated**: dummy broker credentials, empty
  state DB, TOTP off. With the repo's real `.env` the app restores campaigns and
  calls `reconcile()`, which re-syncs live orders against Binance mainnet.
  Recipe is saved in memory as `proj_cryptoforge_local_e2e_recipe`.
- Playwright element screenshots for visual comparison between engines.
- **Typo reinjection** after each phase — the Phase 0 net is only worth what it
  catches, so it gets re-proven, not assumed.

**Visual checking**

- `mcp__Claude_Browser__*` (Browser pane) against the local app for
  interaction work in Phase 3, where a static screenshot cannot show whether a
  drag felt right.

**Skills**

- Honestly: none of the installed skills fit this work. `/code-review` and
  `/security-review` exist but are yours to trigger, not mine. I am not going
  to invoke a skill for the sake of listing one.
- I will **not** spawn subagents unless you ask. This is a single coherent
  change to one file; a cold agent would re-derive context I already have.

**What I will not use**

- No new libraries. Nothing is added to the page — the chart stays
  self-contained, as it is today.

---

## Standing risks

1. **Every push to `main` deploys.** The `Deploy — CryptoForge` job in
   `.github/workflows/playwright.yml` is gated on E2E and fires on push. A
   deploy restarts the live trading engine and reconciles orders. That is true
   even for a test-only or docs-only commit. I will say so each time rather
   than let it be a surprise.
2. **The local e2e baseline is flaky, CI is not.** On this box 1–3 unrelated
   tests fail per run with a wandering set. Before blaming a new spec for
   breaking neighbours, run the baseline twice with the new file moved out.
3. **`node --check` proves nothing about the renderer.** It parses syntax; an
   undefined identifier passes it cleanly. The e2e suite is the net.

---

## Immediate next step

Complete the authenticated, read-only production comparison; then you decide
whether to keep Canvas selected. Classic remains the default until that choice.

*This plan is included with the poll-bridge change so the repository record
matches the implementation. The automatic deployment remains E2E-gated.*
