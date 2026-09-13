# CryptoForge visual consistency and security review

Review started 12 September and finalized 13 September 2026 against `e8e14151f6b1440eea10c3bc8fa746ecbd8326f8`.
Review branch: `codex/cryptoforge-ui-audit-20260912`.

## Outcome and boundaries

Implemented targeted visual corrections in an isolated worktree. The original checkout, its local changes, live credentials, trading state, and production service were not modified. Nothing was pushed or deployed.

This is a source-assisted audit with automated scanning, regression tests, and selected browser inspections. It is **not** a claim that every line and word received a complete manual security review, a penetration test, or a guarantee against future failures. Security concerns below remain open because their remediation changes authentication implementation beyond the requested cosmetic scope.

## Completed visual fixes

| Area | Finding | Correction |
| --- | --- | --- |
| Populated Live monitor on phones | Two fixed grid columns and 600px tables expanded the document; measured 870px overflow at 390px. Empty-state testing did not expose this. | Allow grid children to shrink; stack columns below 720px; use two stat columns; give tables their own horizontal scrolling. |
| Live monitor controls | Header actions could squeeze long strategy names; completed-trade search lacked an accessible name. | Wrap header contents and label the search field. No event handlers changed. |
| Light-mode table surfaces | Later `.table-surface .trade-table` dark rules beat the existing light selector. | Make the light selector cover that nesting explicitly. Live wrappers now use theme surfaces and borders. |
| Primary actions and active navigation | Light primary actions and underlines used fixed cyan/purple despite the chosen appearance preset. | Use the selected light accent for primary actions, the All filter, and active navigation. |
| Selected filters and warnings | Several bright gradient endpoints beneath white text had insufficient contrast. | Darken the same semantic hues for backtest, paper, live, scalp, long/short and win/loss filters, and warning actions. Consolidate danger-button fill through its existing token. |
| Status colors | Live state and event text used dark-theme pastels in both themes; light state badges retained pale text. Some components referenced undefined warning/error tokens. | Use existing semantic tokens, provide warning/error aliases, and give light status badges readable foreground/background pairs. |
| Login on short screens | Body overflow was hidden; a tall form or open appearance panel could be unreachable. | Allow vertical scrolling, prevent card shrinking, reserve top-control space, and bound appearance-panel height. |
| Login readability and keyboard use | Footer text was highly transparent and small; light placeholders were faint; generic controls lacked a shared focus treatment. | Increase footer readability, darken light placeholders and status text, and provide a low-specificity keyboard focus outline that preserves component-specific treatment. |
| Narrow header | The clock could shrink until its text overlapped adjacent controls. | Allow the control row to wrap and preserve the clock width. |

The chart renderers, OHLC payloads, axes, trendline geometry, price calculations, trade signals, order routes, polling intervals, and event-handler expressions were preserved. Existing chart regression tests cover Classic and Canvas, fullscreen, theme changes, frozen/journal states, structures and labels. Published strategy tearsheets retain their own document styling.

Only ten string-literal lines changed in the app JavaScript: CSS classes, style values, and one accessibility attribute. A TypeScript scanner comparison found all non-string JavaScript tokens unchanged; manual diff review confirms the changed strings are presentation-only. This structural comparison supports the review but is not a general proof of program equivalence.

Static assets already receive content-derived cache versions in `app.py`; no guessed asset-manifest version or service-worker change was added.

## Open authentication findings

### P2 — Accepted passwords can fail during hashing

`accounts.py:60–73` accepts 8–128 characters and passes UTF-8 bytes directly to bcrypt. Installed bcrypt is 5.0.0. A 73-character ASCII password (`A1` followed by 71 `x` characters) passes `password_policy_error` but raises `ValueError` in `hash_password`.

The affected callers include self-service password changes (`app.py:3105`), account creation (`app.py:3369`) and administrator password reset (`app.py:3405`). Multibyte passwords can exceed 72 bytes with fewer than 73 characters. This can surface as a server error for an apparently valid password. The open-ended `bcrypt>=4.0` requirement also permits different long-password behavior across environments.

Recommended separate change: explicitly design and validate a byte-aware policy or adopt a reviewed versioned password-hashing scheme, with compatibility tests for existing accounts. Do not silently truncate passwords. [bcrypt's documented 72-byte behavior](https://pypi.org/project/bcrypt/) supports this finding. Local reproduction is in `evidence/auth-review-evidence.json`.

### P2 — Password work blocks the application event loop

`app.py:2980–3003` implements login as an async route but calls synchronous bcrypt hashing/checking through `accounts.py:72–78`. Account writes also hash synchronously. A local demonstration of the same hash operation delayed a callback scheduled for 10ms to approximately 294ms. This demonstrates blocking in this environment; it does not measure production load or prove a missed trade.

Login for an unknown username also constructs a new cost-12 dummy hash and then verifies it, whereas a known username uses its existing hash. That introduces additional work and a potential timing distinction despite the route's equal-timing comment. A production timing attack was not tested.

Recommended separate change: move hashing and verification off the event-loop thread with bounded concurrency, precompute a dummy hash, and test unknown/known-user timing and concurrent application responsiveness. Rate limiting reduces abuse but does not remove per-request event-loop blocking.

### Deployment assumptions requiring separate verification

The IP and HTTPS helpers trust forwarding headers. The checked-in service binds to loopback and nginx overwrites the relevant headers, which supports that trust boundary. The currently deployed proxy, firewall and service configuration were not inspected. Direct public exposure of the application would require a separate trust-boundary review.

The checked-in workflow deploys on pushes to `main`, uses `FORCE_DEPLOY=1`, and can change a running trading service. Accordingly this branch has not been pushed or promoted.

## Validation

- Source inventory: tracked baseline paths listed in `evidence/source-inventory.json`; this is an inventory, not a per-file manual-review attestation.
- Bandit: 45,858 Python lines scanned, zero returned medium/high findings under the repository's configured severity/confidence thresholds. No scan errors. The configuration skips B101/B105/B110/B311 and inline suppressions remain in effect; those exclusions limit the result. Full output: `evidence/bandit-results.json`.
- Dependency audit: 40 resolved packages, zero known vulnerabilities, zero skipped packages at the time of the audit. This checks the resolved requirements, not the packages installed on production. Full output: `evidence/dependency-audit.json`.
- Python suite: 2,029 passed and 48 failed in the restricted sandbox, plus 85 passing subtests. All 48 failures were in the executor UI module, whose temporary server bindings were denied. Rerunning that complete module with loopback binding allowed passed 99/99. Thus every collected Python test passed across the full run and the affected-module rerun; there was not a single all-green full-suite invocation.
- Focused audit/live-hardening tests: 165/165 passed.
- Ten static JavaScript files passed syntax checks; the edited app file was rechecked after the final markup-only edit.
- Final browser suite: **110 passed, 1 skipped, 0 failed** in 5.1 minutes using one worker. The skipped case needs a real campaign, deliberately absent from the isolated test account. Full output: `evidence/e2e-final-results.txt`.
- New regression cases cover populated Live tables at 320px in both themes, login at 320×360, and gradient-endpoint text contrast across six tints. Existing mobile checks cover 390px and 768px, dialogs, labels and navigation.
- Two existing test helpers now wait for the deferred application script, rather than acting on shell markup before `showPage` exists. Initial concurrent and sequential runs exposed both startup timing failures and the genuine populated-table overflow above; failures were investigated rather than omitted.

The contrast checks use the 4.5:1 normal-text criterion from [WCAG 2.2](https://www.w3.org/WAI/WCAG22/Understanding/contrast-minimum.html). They measure specified gradient endpoints, not every rendered pixel or every interaction state. They do not establish whole-site WCAG conformance.

## Remaining verification limits

No production penetration test, exchange-failure simulation against live orders, historical secret scan, independent security review, real-device iOS/Android test, or Safari/Firefox matrix was performed. Browser evidence uses Chromium and isolated/fake trading state; it cannot certify real-money safety. Existing business behavior and the two authentication concerns above were intentionally not changed.

## Visual evidence

- [Light mobile Live monitor](evidence/live-light-320.png)
- [Dark mobile Live monitor](evidence/live-dark-320.png)
- [Light desktop Cascade](evidence/cascade-light-desktop.png)
- [Dark desktop Cascade](evidence/cascade-dark-desktop.png)
- [Short-screen light login](evidence/login-light-320.png)
- [Short-screen dark login](evidence/login-dark-320.png)
