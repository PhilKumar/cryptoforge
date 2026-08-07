/**
 * error-handler.js — CryptoForge Frontend Error System
 * =================================================================
 * Drop-in: add one line to strategy.html <head>:
 *   <script src="/static/error-handler.js" defer></script>
 *
 * Provides four layers:
 *   1. Global JS crash catcher    (window.onerror + unhandledrejection)
 *   2. Toast notification system  (replaces the old #toast display:none/block)
 *   3. safeFetch()                (standardised API fetch wrapper)
 *   4. widgetError()              (localised in-widget error state)
 *
 * Compatible with the existing toast(msg, type, duration) call signature
 * used throughout strategy.html — no call sites need to change.
 */

/* ═══════════════════════════════════════════════════════════════════
   0. SHARED HELPERS
   ═══════════════════════════════════════════════════════════════════ */

/** Read a CSS variable from :root, with a fallback. */
function _cssVar(name, fallback = '') {
  return getComputedStyle(document.documentElement)
    .getPropertyValue(name).trim() || fallback;
}

/* ═══════════════════════════════════════════════════════════════════
   1. GLOBAL JS CRASH CATCHER
   ═══════════════════════════════════════════════════════════════════
   Catches:
     - Synchronous JS errors  (window.onerror)
     - Unhandled promise rejections (unhandledrejection)

   On a fatal crash: hides broken UI and injects a full-page
   fallback screen that matches the app's dark glassmorphism theme.
   ═══════════════════════════════════════════════════════════════════ */

let _crashCount = 0;
const _MAX_CRASHES = 3; // suppress fallback after this many (avoid infinite loops)

function _showCrashScreen(message, source) {
  _crashCount++;
  if (_crashCount > _MAX_CRASHES) return;

  // Only inject once
  if (document.getElementById('_af-crash-screen')) return;

  console.error('[CryptoForge] Fatal JS error ->', message, source);

  const screen = document.createElement('div');
  screen.id = '_af-crash-screen';
  screen.style.cssText = `
    position: fixed; inset: 0; z-index: 99999;
    display: flex; align-items: center; justify-content: center;
    background: #060b12;
    font-family: 'Manrope', system-ui, sans-serif;
  `;

  screen.innerHTML = `
    <div style="
      background: linear-gradient(160deg, rgba(20,28,46,0.98), rgba(13,18,32,0.95));
      border: 1px solid rgba(255,255,255,0.10);
      border-top: 1px solid rgba(255,255,255,0.18);
      border-radius: 20px;
      padding: 48px 40px;
      max-width: 480px;
      width: 90vw;
      text-align: center;
      box-shadow: 0 0 0 1px rgba(255,255,255,0.05),
                  0 32px 80px rgba(0,0,0,0.7),
                  inset 0 1px 0 rgba(255,255,255,0.10);
      position: relative;
      overflow: hidden;
    ">
      <!-- top shimmer line -->
      <div style="
        position: absolute; top: 0; left: 0; right: 0; height: 2px;
        background: linear-gradient(90deg, transparent, rgba(248,113,113,0.7), transparent);
      "></div>

      <!-- icon -->
      <div style="
        font-size: 52px; line-height: 1; margin-bottom: 20px;
        filter: drop-shadow(0 0 20px rgba(248,113,113,0.4));
      ">⚡</div>

      <!-- heading -->
      <h2 style="
        font-family: 'Space Grotesk', 'Manrope', system-ui, sans-serif;
        font-size: 22px; font-weight: 800; letter-spacing: -0.3px;
        color: #dde3ee; margin-bottom: 12px;
      ">Something went wrong</h2>

      <!-- message -->
      <p style="
        font-size: 14px; line-height: 1.65;
        color: rgba(221,227,238,0.60);
        margin-bottom: 10px;
      ">
        The page encountered an unexpected error.<br>
        Your positions and strategies are unaffected.
      </p>

      <!-- technical context (collapsed) -->
      <details style="margin-bottom: 28px; text-align: left;">
        <summary style="
          font-family: 'IBM Plex Mono', monospace;
          font-size: 11px; color: rgba(221,227,238,0.35);
          cursor: pointer; letter-spacing: 0.05em;
          list-style: none; display: flex; align-items: center; gap: 6px;
          justify-content: center;
        ">
          <span style="opacity:0.5;">▶</span> Technical details
        </summary>
        <div style="
          margin-top: 10px;
          background: rgba(0,0,0,0.35); border: 1px solid rgba(255,255,255,0.06);
          border-radius: 8px; padding: 12px 14px;
          font-family: 'IBM Plex Mono', monospace;
          font-size: 11px; color: rgba(248,113,113,0.75);
          word-break: break-word; line-height: 1.6;
        ">${_escHtml(String(message || 'Unknown error'))}</div>
      </details>

      <!-- actions -->
      <div style="display: flex; gap: 10px; justify-content: center; flex-wrap: wrap;">
        <button onclick="location.reload()" style="
          padding: 11px 28px; border-radius: 999px; cursor: pointer;
          font-family: 'Manrope', system-ui; font-weight: 700; font-size: 14px;
          background: linear-gradient(180deg, rgba(0,191,165,0.25) 0%, rgba(0,150,130,0.45) 100%);
          color: #34d399; border: 1px solid rgba(0,191,165,0.45);
          box-shadow: 0 4px 16px rgba(0,191,165,0.2), inset 0 1px 0 rgba(255,255,255,0.1);
          transition: filter 0.15s;
        " onmouseover="this.style.filter='brightness(1.12)'"
           onmouseout="this.style.filter='none'">
          ↺ Reload Page
        </button>
        <button onclick="document.getElementById('_af-crash-screen').remove()" style="
          padding: 11px 28px; border-radius: 999px; cursor: pointer;
          font-family: 'Manrope', system-ui; font-weight: 700; font-size: 14px;
          background: transparent; color: rgba(221,227,238,0.45);
          border: 1px solid rgba(255,255,255,0.10); transition: color 0.15s;
        " onmouseover="this.style.color='rgba(221,227,238,0.85)'"
           onmouseout="this.style.color='rgba(221,227,238,0.45)'">
          Dismiss
        </button>
      </div>
    </div>
  `;

  document.body.appendChild(screen);

  // Fade in
  screen.style.opacity = '0';
  requestAnimationFrame(() => {
    screen.style.transition = 'opacity 0.3s ease';
    screen.style.opacity = '1';
  });
}

// Synchronous JS errors
window.onerror = function (message, source, lineno, colno, error) {
  const loc = source ? `${source.split('/').pop()}:${lineno}` : '';
  _showCrashScreen(message, loc);
  return false; // let browser still log it
};

// Unhandled promise rejections
window.addEventListener('unhandledrejection', function (event) {
  const reason = event.reason;
  const message = reason instanceof Error ? reason.message : String(reason ?? 'Unhandled promise rejection');

  // Suppress noisy network/abort errors that aren't real crashes
  const ignored = ['AbortError', 'NetworkError', 'Failed to fetch'];
  if (ignored.some(t => message.includes(t))) return;

  _showCrashScreen(message, 'Promise');
});


/* ═══════════════════════════════════════════════════════════════════
   2. TOAST NOTIFICATION SYSTEM
   ═══════════════════════════════════════════════════════════════════
   Replaces the old single-element #toast with a queued stack.

   API — identical to the original, all call sites unchanged:
     toast(message, type = '', duration = 4000)
     type: 'success' | 'danger' | 'warn' | '' (neutral/info)

   Each toast slides in from the right, stacks vertically,
   auto-dismisses, and can be manually closed.
   ═══════════════════════════════════════════════════════════════════ */

(function _initToastSystem() {
  // The toast CSS lives in /static/error-handler.css. It used to be injected
  // here as a <style> element, which the app's CSP (`style-src-elem 'self'`)
  // silently dropped — the tag reached the DOM but never became a stylesheet,
  // so every toast rendered unstyled: container static instead of fixed, no
  // z-index, transparent background. Do not move it back inline.
  if (document.getElementById('_af-toast-container')) return;

  // Create container
  const container = document.createElement('div');
  container.id = '_af-toast-container';
  document.body.appendChild(container);

  // Hide the legacy #toast element so it doesn't conflict
  const legacy = document.getElementById('toast');
  if (legacy) legacy.style.display = 'none';
})();

const _TOAST_META = {
  success: { icon: '✓',  cls: '_af-success', label: 'Success'     },
  danger:  { icon: '✕',  cls: '_af-danger',  label: 'Error'        },
  warn:    { icon: '⚠',  cls: '_af-warn',    label: 'Warning'      },
  info:    { icon: 'ℹ',  cls: '_af-info',    label: 'Info'         },
  '':      { icon: '⚡', cls: '_af-neutral', label: ''             },
};

/**
 * Show a toast notification.
 * Drop-in replacement for the original toast() — identical signature.
 *
 * @param {string} msg       Message text
 * @param {string} type      'success' | 'danger' | 'warn' | 'info' | ''
 * @param {number} duration  Auto-dismiss delay in ms (default 4000)
 */
function toast(msg, type = '', duration = 4000) {
  if (!msg) return;

  const container = document.getElementById('_af-toast-container');
  if (!container) return;

  const meta = _TOAST_META[type] || _TOAST_META[''];

  const el = document.createElement('div');
  el.className = `_af-toast ${meta.cls}`;
  el.setAttribute('role', 'alert');
  el.setAttribute('aria-live', 'polite');

  el.innerHTML = `
    <span class="_af-toast-icon" aria-hidden="true">${meta.icon}</span>
    <div class="_af-toast-body">
      ${meta.label ? `<div class="_af-toast-title">${_escHtml(meta.label)}</div>` : ''}
      <div class="_af-toast-msg">${_escHtml(String(msg))}</div>
    </div>
    <button class="_af-toast-close" aria-label="Dismiss notification">✕</button>
    <div class="_af-toast-progress" style="width:100%; transform:scaleX(1);"></div>
  `;

  container.appendChild(el);

  // Dismiss handler
  function dismiss() {
    el.classList.add('dismissing');
    setTimeout(() => el.remove(), 420);
  }

  el.querySelector('._af-toast-close').addEventListener('click', dismiss);

  // Trigger enter animation on next frame
  requestAnimationFrame(() => {
    requestAnimationFrame(() => el.classList.add('visible'));
  });

  // Progress bar countdown
  const bar = el.querySelector('._af-toast-progress');
  if (bar && duration > 0) {
    bar.style.transitionDuration = `${duration}ms`;
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        bar.style.transform = 'scaleX(0)';
      });
    });
  }

  // Auto-dismiss
  const timer = duration > 0 ? setTimeout(dismiss, duration) : null;

  // Pause timer on hover
  el.addEventListener('mouseenter', () => {
    if (timer) clearTimeout(timer);
    if (bar) { bar.style.transitionDuration = '0ms'; }
  });
  el.addEventListener('mouseleave', () => {
    const remaining = duration * parseFloat(getComputedStyle(bar).transform.match(/\d*\.?\d+/)?.[0] ?? 1);
    if (remaining > 0) {
      if (bar) { bar.style.transitionDuration = `${remaining}ms`; bar.style.transform = 'scaleX(0)'; }
      setTimeout(dismiss, remaining);
    }
  });

  // Limit stack depth
  const all = container.querySelectorAll('._af-toast');
  if (all.length > 5) all[0].querySelector('._af-toast-close')?.click();
}

// Backwards-compat alias (some older call sites use showToast)
window.showToast = toast;
window.toast = toast;


/* ═══════════════════════════════════════════════════════════════════
   3. SAFE FETCH — Standardised API call wrapper
   ═══════════════════════════════════════════════════════════════════
   Usage:
     const data = await safeFetch('/api/backtest', {
       method: 'POST',
       body: JSON.stringify(payload),
       headers: { 'Content-Type': 'application/json' },
     });
     // data is the parsed JSON body on success.
     // On failure, throws an ApiError — never shows raw {"detail":...}.

   Options (third arg):
     silent:   true  → suppress toast on error (handle manually)
     toastErr: false → same as silent
   ═══════════════════════════════════════════════════════════════════ */

class ApiError extends Error {
  constructor({ code, title, message, detail, debug }) {
    super(message);
    this.name    = 'ApiError';
    this.code    = code;
    this.title   = title;
    this.detail  = detail ?? null;
    this.debug   = debug  ?? null;
  }
}

/**
 * Fetch wrapper that:
 *   - Parses the standardised { success, error } shape
 *   - Falls back gracefully if the backend returns old-style { detail }
 *   - Shows a toast automatically on error (disable with opts.silent)
 *   - Handles network failures (no connection) cleanly
 *   - Respects AbortSignal (no toast on user-cancelled requests)
 *
 * @param {string} url
 * @param {RequestInit} [fetchOpts]
 * @param {{ silent?: boolean }} [opts]
 * @returns {Promise<any>}  Parsed JSON body
 */
async function safeFetch(url, fetchOpts = {}, opts = {}) {
  let res;

  try {
    res = await fetch(url, fetchOpts);
  } catch (networkErr) {
    // AbortError = intentional cancel, stay silent
    if (networkErr.name === 'AbortError') throw networkErr;

    const err = new ApiError({
      code:    0,
      title:   'Network Error',
      message: 'Could not reach the server. Check your connection.',
    });
    if (!opts.silent) toast(err.message, 'danger');
    throw err;
  }

  // Parse body (always attempt JSON)
  let body;
  try {
    body = await res.json();
  } catch {
    body = null;
  }

  if (res.ok) {
    // Success — return the body directly
    return body;
  }

  // ── Error path ──────────────────────────────────────────────────
  // Handle both the new standardised shape and the old FastAPI default
  const errBlock = body?.error ?? {};
  const detail   = body?.detail ?? null; // old FastAPI shape fallback

  const apiErr = new ApiError({
    code:    errBlock.code    ?? res.status,
    title:   errBlock.title   ?? _httpLabel(res.status),
    message: errBlock.message ?? _httpLabel(res.status),
    detail:  errBlock.detail  ?? (typeof detail === 'string' ? detail : null),
    debug:   errBlock.debug   ?? null,
  });

  if (!opts.silent) {
    // Show human-readable message, not the raw detail
    const toastMsg = apiErr.detail
      ? `${apiErr.message} (${apiErr.detail})`
      : apiErr.message;
    const toastType = res.status >= 500 ? 'danger'
                    : res.status === 429 ? 'warn'
                    : res.status === 401 ? 'warn'
                    : 'danger';
    toast(toastMsg, toastType);
  }

  throw apiErr;
}

/** Minimal HTTP status label fallback (used when error body is missing). */
function _httpLabel(status) {
  const labels = {
    400: 'Bad request — please check your inputs.',
    401: 'Session expired. Please log in again.',
    403: 'You don\'t have permission to do that.',
    404: 'Resource not found.',
    408: 'Request timed out. Please try again.',
    409: 'Conflict — please refresh and try again.',
    422: 'Invalid input — please check your fields.',
    429: 'Too many requests. Please wait a moment.',
    500: 'Server error. Your positions are safe.',
    502: 'Broker API unreachable. Please retry shortly.',
    503: 'Service temporarily offline.',
    504: 'Broker timed out. Check your positions before retrying.',
  };
  return labels[status] ?? `Unexpected error (${status}). Please try again.`;
}

window.safeFetch = safeFetch;
window.ApiError  = ApiError;


/* ═══════════════════════════════════════════════════════════════════
   4. WIDGET-LEVEL ERROR STATES
   ═══════════════════════════════════════════════════════════════════
   Usage — basic:
     widgetError(document.getElementById('market-data-container'),
                 'Failed to load market data.');

   Usage — with retry:
     widgetError(el, 'Prices unavailable.', () => loadMarketData());

   Usage — loading state:
     widgetLoading(el);

   Usage — clear error:
     widgetClear(el);

   Sizing:
     The error card respects the container's existing height.
     Pass opts.compact = true for small widgets (e.g. ticker cells).
   ═══════════════════════════════════════════════════════════════════ */

/** Inject localised error state into a widget container. */
function widgetError(container, message = 'Failed to load data.', retryFn = null, opts = {}) {
  if (!container) return;

  const compact = opts.compact ?? (container.clientHeight < 120);
  const retryId = `_retry_${Math.random().toString(36).slice(2)}`;

  container.innerHTML = `
    <div style="
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      gap: ${compact ? '8px' : '12px'};
      padding: ${compact ? '16px 12px' : '28px 20px'};
      min-height: ${compact ? '80px' : '120px'};
      text-align: center;
      background: rgba(30,10,10,0.45);
      border: 1px solid rgba(248,113,113,0.15);
      border-radius: 10px;
      position: relative;
      overflow: hidden;
    ">
      <!-- top accent line -->
      <div style="
        position: absolute; top: 0; left: 0; right: 0; height: 1.5px;
        background: linear-gradient(90deg, transparent, rgba(248,113,113,0.45), transparent);
      "></div>

      <!-- icon -->
      <div style="
        font-size: ${compact ? '20px' : '26px'};
        opacity: 0.65;
        filter: drop-shadow(0 0 8px rgba(248,113,113,0.3));
      ">⚠</div>

      <!-- message -->
      <p style="
        font-family: 'Manrope', system-ui, sans-serif;
        font-size: ${compact ? '12px' : '13.5px'};
        color: rgba(248,113,113,0.80);
        line-height: 1.5;
        margin: 0;
        max-width: 280px;
      ">${_escHtml(message)}</p>

      ${retryFn ? `
        <button id="${retryId}" style="
          padding: ${compact ? '5px 14px' : '7px 18px'};
          border-radius: 999px; cursor: pointer;
          font-family: 'Manrope', system-ui; font-weight: 700;
          font-size: ${compact ? '11px' : '12px'};
          background: rgba(248,113,113,0.12);
          color: #fca5a5;
          border: 1px solid rgba(248,113,113,0.30);
          transition: background 0.15s, border-color 0.15s;
          display: inline-flex; align-items: center; gap: 5px;
        "
        onmouseover="this.style.background='rgba(248,113,113,0.20)'"
        onmouseout="this.style.background='rgba(248,113,113,0.12)'">
          ↺ Retry
        </button>
      ` : ''}
    </div>
  `;

  if (retryFn) {
    document.getElementById(retryId)?.addEventListener('click', () => {
      widgetLoading(container);
      // Small delay so user sees the loading state before the retry fires
      setTimeout(retryFn, 150);
    });
  }
}

/** Inject a skeleton loading state into a widget container. */
function widgetLoading(container, opts = {}) {
  if (!container) return;
  const compact = opts.compact ?? (container.clientHeight < 120);
  const rows    = opts.rows ?? (compact ? 2 : 4);

  const skeletonRows = Array.from({ length: rows }, (_, i) => `
    <div style="
      height: 12px;
      border-radius: 6px;
      background: linear-gradient(90deg,
        rgba(255,255,255,0.04) 0%,
        rgba(255,255,255,0.09) 50%,
        rgba(255,255,255,0.04) 100%);
      background-size: 200% 100%;
      animation: _af-shimmer 1.5s ease-in-out infinite;
      width: ${i === rows - 1 ? '60%' : '100%'};
    "></div>
  `).join('');

  // Inject keyframes once
  // The @keyframes moved to /static/error-handler.css for the same reason as
  // the toast rules above: an injected <style> is blocked by style-src-elem,
  // so the shimmer never animated.

  container.innerHTML = `
    <div style="
      padding: ${compact ? '14px 12px' : '20px 16px'};
      display: flex; flex-direction: column; gap: 10px;
      background: rgba(13,18,32,0.5);
      border: 1px solid rgba(255,255,255,0.06);
      border-radius: 10px;
    ">
      ${skeletonRows}
    </div>
  `;
}

/** Clear any error/loading state by emptying the container. */
function widgetClear(container) {
  if (container) container.innerHTML = '';
}

window.widgetError   = widgetError;
window.widgetLoading = widgetLoading;
window.widgetClear   = widgetClear;


/* ═══════════════════════════════════════════════════════════════════
   INTERNAL UTILITIES
   ═══════════════════════════════════════════════════════════════════ */

/** Escape HTML to prevent XSS in injected error messages. */
function _escHtml(str) {
  return String(str)
    .replace(/&/g,  '&amp;')
    .replace(/</g,  '&lt;')
    .replace(/>/g,  '&gt;')
    .replace(/"/g,  '&quot;')
    .replace(/'/g,  '&#39;');
}

window._escHtml = _escHtml;

console.info('[CryptoForge] error-handler.js loaded - global error handling active.');
