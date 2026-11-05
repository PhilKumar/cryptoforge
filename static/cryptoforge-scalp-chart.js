/* THE SCALP CHART — PhilForge's canvas renderer, brought home.
 *
 * PhilForge's chart began as a port of CryptoForge's own `_cfChart` renderer
 * (2026-07-30) and grew up there: bar-index x-axis, gliding zoom, Supertrend
 * and RSI panes, drawings dragged by their ends, a magnet, a countdown tag.
 * Phil, 2026-11-04: "Please create the same kinda scalp page for cryptoforge
 * as well same as that of philforge". This file is that renderer, copied
 * rather than rewritten -- a second implementation of the same drawing is a
 * second set of bugs -- with the India-specific parts changed:
 *
 *   * money is dollars, not rupees;
 *   * prices keep the decimals a coin needs (XRP is not BTC);
 *   * the bar clock runs 24/7 on the epoch grid, as the exchanges' bars do --
 *     there is no NSE session, no weekend and no 09:15 grid;
 *   * the axis still reads IST, the only clock Phil uses.
 *
 * Its names stay `_pfChart*` / `pfChart*`: they collide with nothing in
 * cryptoforge-app.js (whose own renderer is `_cfChart*`), and keeping them
 * means a fix made in PhilForge can be carried across line for line.
 *
 * Payload: { timeframe, candles:[{t,o,h,l,c}], lines:[{price,label,color}],
 *   overlays:[{label,color,points:[{t,price}],group}], entries, exits, ... }
 * Every `t` is epoch SECONDS.
 */

function _pfChartIst(ts) {
  if (!ts) return '--';
  var d = new Date((Number(ts) + 19800) * 1000);
  if (isNaN(d.getTime())) return '--';
  return d.toISOString().slice(5, 16).replace('T', ' ');
}

// Dollars, US grouping, as every other figure in CryptoForge.
function _pfChartInr(value) {
  var n = Number(value);
  if (!isFinite(n)) return '$0';
  return '$' + n.toLocaleString('en-US', { maximumFractionDigits: 2 });
}

// A price with the decimals its size needs: BTC at 2, XRP at 4, a sub-dollar
// coin at 6. `fixed` pads to that many (the price tag), otherwise trailing
// zeros drop as they did before.
function _pfChartPriceDigits(value) {
  var a = Math.abs(Number(value));
  if (!isFinite(a) || a >= 100) return 2;
  if (a >= 1) return 4;
  return 6;
}
function _pfChartPrice(value, fixed) {
  var n = Number(value), d = _pfChartPriceDigits(n);
  if (!isFinite(n)) return '--';
  return n.toLocaleString('en-US', { minimumFractionDigits: fixed ? d : 0, maximumFractionDigits: d });
}
window.pfChartPrice = _pfChartPrice;

function _pfBenchChartHostHtml() {
  // The zoom cluster is part of the HOST, not any one caller's toolbar, so
  // every chart on the site carries the same − % + controls without each page
  // remembering to add them (Phil's one-strip standard, 2026-08-13).
  return '<div class="pf-bench-canvas-host" id="pf-bench-canvas-host">'
    + '<canvas id="pf-bench-canvas-main"></canvas>'
    + '<canvas id="pf-bench-canvas-overlay"></canvas>'
    + '<div class="pf-bench-zoom" data-bench-zoom>'
    + '<button type="button" data-bench-zoom-out aria-label="Zoom out">&minus;</button>'
    // The readout IS the reset. Reset used to be a double-click on the plot,
    // announced in a title tooltip -- a phone has neither hover nor a reliable
    // double-click, so on mobile the function existed and could not be found
    // (Phil, 2026-08-17: "Chart zoom functions must be clear for charts in
    // mobile"). A button carrying the percentage is the obvious target, and it
    // still shows what it always showed.
    + '<button type="button" class="pf-bench-zoom-reset" data-bench-zoom-level data-bench-zoom-reset'
    + ' aria-label="Reset zoom to fit" title="Reset zoom to fit">100%</button>'
    + '<button type="button" data-bench-zoom-in aria-label="Zoom in">+</button>'
    + '</div>'
    + '</div>';
}

var _PF_CHART_MAX_STRUCTURES = 3;

// buyMark / sellMark are deliberately OFF the candle palette. Green buy arrows
// vanished against green candles and red sell arrows against red ones — the two
// marks you most need to find were camouflaged by the bars they sat on. Amber
// and white appear nowhere else on the chart.
//
// CANDLES AND THE EMA ARE TRADINGVIEW'S (Phil, 2026-10-13: "make the standard
// default chart like trading view colours"). Teal up, crimson down and a blue
// 20-EMA -- the same set the Journal page's own charts are drawn in, so the
// site and the journal read alike. On a light sheet TradingView outlines each
// candle and draws its wick near-black (`wick`); on a dark one the wick takes
// the candle's colour, so the dark palette names none.
var _PF_CHART_DARK = {
  grid: 'rgba(148,163,184,0.12)', axis: 'rgba(148,163,184,0.55)',
  up: '#089981', down: '#e91e63', mother: '#a855f7', tp: '#10b981',
  avg: '#e2e8f0', fill: '#22c55e', fillRing: '#0b1220',
  buyMark: '#ffffff', sellMark: '#fbbf24', markRing: '#0b1220',
  fibs: ['#3b82f6', '#22c55e', '#ef4444'],
  ema: '#5b9bff', rsi: '#7e57c2', rsiGuide: 'rgba(148,163,184,0.55)', rsiBand: 'rgba(126,87,194,0.10)'
};
var _PF_CHART_LIGHT = {
  grid: 'rgba(15,23,42,0.10)', axis: 'rgba(51,65,85,0.75)',
  up: '#089981', down: '#e91e63', wick: '#0f172a', mother: '#7c3aed', tp: '#047857',
  avg: '#334155', fill: '#15803d', fillRing: '#ffffff',
  buyMark: '#1e293b', sellMark: '#b45309', markRing: '#ffffff',
  fibs: ['#1d4ed8', '#15803d', '#be123c'],
  ema: '#2962ff', rsi: '#7e57c2', rsiGuide: 'rgba(51,65,85,0.45)', rsiBand: 'rgba(126,87,194,0.10)'
};

function _pfChartPalette() {
  var theme = document.documentElement.getAttribute('data-theme');
  if (!theme || theme === 'auto') {
    theme = window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
  }
  return theme === 'light' ? _PF_CHART_LIGHT : _PF_CHART_DARK;
}

// API overlays carry literal colours. Resolve their light-theme equivalents
// at paint time so switching theme also updates CPR, Supertrend and quotes.
function _pfChartSeriesColor(color, PAL, fallback) {
  if (!color) return fallback;
  // TradingView's blue is drawn for a white sheet; on the dark one it is
  // lifted a little so the CPR and the EMA do not sink into the background.
  if (PAL !== _PF_CHART_LIGHT) return String(color).toLowerCase() === '#2962ff' ? '#5b9bff' : color;
  var light = {
    '#4ade80': '#15803d', '#6ee7b7': '#047857', '#34d399': '#047857',
    '#f87171': '#b91c1c', '#fca5a5': '#b91c1c',
    '#f59e0b': '#92400e', '#fbbf24': '#92400e',
    '#38bdf8': '#0369a1', '#60a5fa': '#1d4ed8', '#93c5fd': '#1d4ed8',
    '#e2e8f0': '#334155', '#ffffff': '#334155', '#fff': '#334155'
  };
  return light[String(color).toLowerCase()] || color;
}

// THE ONE PALETTE, exported so a second chart cannot invent its own. The
// Terminal campaign chart kept a private copy and it drifted: two of its three
// dark fib colours were written `var(--green)` and `var(--red)`, and Canvas does
// not resolve CSS variables -- assigning one to strokeStyle is silently ignored,
// so the context keeps whatever colour it had. Every leg after the first, every
// trendline after the first and every fill drew in leg 1's blue.
window.pfChartPalette = _pfChartPalette;

// THE SAME CAP, EXPORTED FOR THE SAME REASON. Only the newest three legs and
// three trendlines are drawn; past that the chart is a cat's cradle and the
// structure that matters is the one nearest the price. The Terminal campaign
// chart had its own drawing loop and no cap at all, so it drew seven fibs and
// six trendlines while every other chart on the site drew three.
window.pfChartMaxStructures = _PF_CHART_MAX_STRUCTURES;

/* The newest `pfChartMaxStructures` of a structure list, never dropping the
 * ACTIVE one — Classic's exception, kept: the live trendline stays on the chart
 * even when three retired ones came after it. `isActive` is a predicate because
 * the two payload shapes spell it differently. */
window.pfChartLatestStructures = function (list, isActive) {
  var all = Array.isArray(list) ? list : [];
  var kept = all.slice(-_PF_CHART_MAX_STRUCTURES);
  if (typeof isActive !== 'function') return kept;
  var active = all.filter(isActive)[0];
  if (!active || kept.indexOf(active) !== -1) return kept;
  // THE EXCEPTION NEVER WORKED. It used to prepend the active line and then
  // slice(-MAX) again, which drops exactly what was just prepended -- so the
  // live trendline vanished the moment three retired ones came after it. Make
  // room instead: the active line plus the newest MAX-1 others.
  return [active].concat(kept.slice(-(_PF_CHART_MAX_STRUCTURES - 1)));
};


// ── The bar clock ────────────────────────────────────────
// CRYPTO NEVER CLOSES. Every exchange cuts its bars on the epoch grid (a 1h
// bar opens on the UTC hour), so the bar `nowSec` falls in is simply the grid
// cell it is in; `open` is that UTC day's midnight, which every timeframe up to
// a day divides into evenly. `hours` is accepted and ignored, so a payload
// shaped for PhilForge still draws.
function _pfChartBarWindow(nowSec, tfSec, hours) {
  var now = Number(nowSec), step = Math.max(Number(tfSec) || 60, 1);
  if (!isFinite(now)) return null;
  var start = Math.floor(now / step) * step;
  return { start: start, close: start + step, day: _pfChartIstDay(start), open: start - (start % 86400) };
}

function _pfChartIstDay(epochSec) { return Math.floor((Number(epochSec) + 19800) / 86400); }

window.pfChartBarWindow = _pfChartBarWindow;
window.pfChartIstDay = _pfChartIstDay;

// Live canvas state, or null when no canvas chart is mounted. Holding the
// ResizeObserver here is what makes teardown possible — without it, every
// refresh would leave another observer watching a detached element.
var _pfChartCanvas = null;

// MORE THAN ONE CHART, side by side (Phil, 2026-10-29: two charts on the
// Scalp desk, CE beside PE). Every live chart is in _pfChartCanvases. The
// page talks to ONE of them -- the focused chart -- through the same
// functions as before, so a page with one chart is unchanged. A chart's own
// pointer, wheel, resize and timer work runs with _pfChartCanvas pointed at
// that chart for the length of the handler (_pfChartAs), then back at the
// focused one. Only a mount asked for with {multi: true} leaves the others up.
var _pfChartCanvases = [];
var _pfChartFocused = null;

function _pfChartAs(c, fn) {
  return function () {
    if (!c.live) return undefined;
    _pfChartCanvas = c;
    try { return fn.apply(this, arguments); }
    finally { _pfChartCanvas = _pfChartFocused && _pfChartFocused.live ? _pfChartFocused : null; }
  };
}

// The live chart drawn in (or around) `el`.
function _pfChartFind(el) {
  if (!el) return null;
  for (var i = 0; i < _pfChartCanvases.length; i += 1) {
    var c = _pfChartCanvases[i];
    if (c.host === el || (el.contains && el.contains(c.host)) || c.host.contains(el)) return c;
  }
  return null;
}

// Point the page at the chart in `el`; false when there is none.
function pfChartFocus(el) {
  var c = _pfChartFind(el);
  if (!c) return false;
  _pfChartFocused = c;
  _pfChartCanvas = c;
  return true;
}

// Run fn with the chart in `el` as the one addressed, without focusing it:
// how a page refreshes the chart beside the focused one.
function pfChartWith(el, fn) {
  var c = _pfChartFind(el);
  return c ? _pfChartAs(c, fn)() : undefined;
}

function pfChartFocusedHost() {
  return _pfChartFocused && _pfChartFocused.live ? _pfChartFocused.host : null;
}

window.pfChartFocus = pfChartFocus;
window.pfChartWith = pfChartWith;
window.pfChartFocusedHost = pfChartFocusedHost;

/* Retire every OTHER canvas host in the document.
 *
 * The renderer addresses its surfaces by fixed ids, so only one chart can be
 * live at a time -- that has always been true. What made it bite is that the
 * Equity page grew charts that stay in the DOM after you navigate away (an
 * expanded scanner row, the ladder overlay). With two hosts present,
 * getElementById returns whichever comes FIRST in document order, so opening
 * the scalp option chart painted into the scanner's canvas and left the scalp
 * dialog blank -- no error, nothing in the console, just an empty box.
 *
 * Retiring the strays here keeps the ids unique by construction, which is the
 * only way this stays fixed as more charts are added.
 */
function _pfChartCanvasRetireOthers(keep) {
  var hosts = document.querySelectorAll('#pf-bench-canvas-host');
  for (var i = 0; i < hosts.length; i += 1) {
    var node = hosts[i];
    if (node === keep || !node.parentNode) continue;
    var note = document.createElement('p');
    note.className = 'pf-bench-empty';
    note.textContent = 'Chart closed — only one chart can be open at a time.';
    node.parentNode.replaceChild(note, node);
  }
}

function _pfChartCanvasMount(d, scope, opts) {
  opts = opts || {};
  // One of several ({multi}): only the chart this container held, and any
  // whose host has left the page, go. Otherwise every chart does.
  if (opts.multi) {
    _pfChartCanvases.slice().forEach(function (old) {
      if (!old.host.isConnected || (scope && scope.contains(old.host))) _pfChartCanvasTeardown(old);
    });
  } else {
    _pfChartCanvasTeardown();
  }
  // Scoped to the container the caller just wrote the host into. A global
  // lookup finds the first host in the document, which is not necessarily the
  // one being drawn -- see _pfChartCanvasRetireOthers.
  var host = (scope && scope.querySelector('#pf-bench-canvas-host'))
    || document.getElementById('pf-bench-canvas-host');
  if (!host) return;
  if (!opts.multi) _pfChartCanvasRetireOthers(host);
  var main = host.querySelector('#pf-bench-canvas-main');
  var overlay = host.querySelector('#pf-bench-canvas-overlay');
  if (!main || !overlay) return;
  _pfChartCanvas = {
    live: true, multi: !!opts.multi,
    host: host, main: main, overlay: overlay,
    ctx: main.getContext('2d'), octx: overlay.getContext('2d'),
    data: d, w: 0, h: 0, dpr: 0, ro: null, themeObserver: null,
    // Phase 2 owns this model. Phase 3 changes it through pan/zoom/axis-drag;
    // Phase 4 preserves it across a live refresh.
    viewport: null, projection: null, paint: null, paintKey: '', handlers: [], drag: null,
    // Pending requestAnimationFrame id for a coalesced resize; see the
    // ResizeObserver below for why the work cannot run inline.
    resizeFrame: 0
  };
  var c = _pfChartCanvas;
  _pfChartCanvases.push(c);
  // A chart drawn beside the focused one ({background}) does not take focus.
  if (!(opts.background && _pfChartFocused && _pfChartFocused.live)) _pfChartFocused = c;
  _pfChartAs(c, function () {
    _pfChartCanvasResize(c);
    _pfChartCanvasBindInteraction(c);
  })();
  // The bar-close countdown ticks once a second, on a chart that asks for it
  // (payload.countdown) and only while it can be seen.
  c.clock = setInterval(_pfChartAs(c, function () {
    if (!c.data || !c.data.countdown) return;
    if (document.hidden || !c.host.isConnected || !c.host.getClientRects().length) return;
    _pfChartCanvasDraw(c);
  }), 1000);
  // The host's own zoom cluster. Bound per-mount; teardown removes the host.
  var zoomBox = host.querySelector('[data-bench-zoom]');
  if (zoomBox) {
    var paintLevel = function () {
      var level = zoomBox.querySelector('[data-bench-zoom-level]');
      if (level && typeof pfChartZoomPercent === 'function') level.textContent = pfChartZoomPercent() + '%';
    };
    zoomBox.addEventListener('click', _pfChartAs(c, function (event) {
      if (event.target.closest('[data-bench-zoom-in]')) _pfChartCanvasZoom(0.8);
      else if (event.target.closest('[data-bench-zoom-out]')) _pfChartCanvasZoom(1.25);
      else if (event.target.closest('[data-bench-zoom-reset]')) _pfChartCanvasZoom(0, true);
      else return;
      paintLevel();
    }));
    // Wheel-zoom and double-click reset land on the canvas; keep the readout
    // honest without a caller having to wire anything.
    var paintLater = function () { requestAnimationFrame(_pfChartAs(c, paintLevel)); };
    host.addEventListener('wheel', paintLater, { passive: true });
    host.addEventListener('dblclick', paintLater);
  }
  // Canvas pixels do not inherit CSS colours. Watch the single theme attribute
  // and repaint from the same payload/viewport when it changes; SVG gets this
  // for free through a fresh DOM render, Canvas must do it explicitly.
  if (window.MutationObserver) {
    c.themeObserver = new MutationObserver(_pfChartAs(c, function () { _pfChartCanvasDraw(c); }));
    c.themeObserver.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
  }
  // The host is sized entirely by CSS, and the things that change it — going
  // fullscreen, rotating a phone, dragging the window edge — do not all fire a
  // resize event on window. Observe the element itself.
  if (window.ResizeObserver) {
    // Deferred to the next frame, deliberately. Resizing writes canvas
    // width/height and inline styles, which mutates layout — do that INSIDE the
    // observer callback and the browser cannot finish delivering the batch it
    // is in the middle of, so it aborts with "ResizeObserver loop completed
    // with undelivered notifications". That is fired at window.onerror, which
    // on this site puts a full "Something went wrong" screen over the app the
    // moment a chart opens. Coalescing to one frame keeps the layout write out
    // of the delivery cycle, and collapses a burst of observations into a
    // single redraw.
    c.ro = new ResizeObserver(function () {
      if (c.resizeFrame) return;
      // Torn down, or replaced by another chart, between frames: _pfChartAs
      // does nothing for a chart that is no longer live.
      c.resizeFrame = requestAnimationFrame(_pfChartAs(c, function () {
        c.resizeFrame = 0;
        _pfChartCanvasResize(c);
      }));
    });
    c.ro.observe(host);
  } else {
    c.onWindowResize = _pfChartAs(c, function () { _pfChartCanvasResize(c); });
    window.addEventListener('resize', c.onWindowResize);
  }
  _pfChartCanvas = _pfChartFocused;
}

// Every chart, or only `only`.
function _pfChartCanvasTeardown(only) {
  var list = only ? [only] : _pfChartCanvases.slice();
  if (!only && _pfChartCanvas && list.indexOf(_pfChartCanvas) < 0) list.push(_pfChartCanvas);
  list.forEach(function (c) {
    c.live = false;
    var at = _pfChartCanvases.indexOf(c);
    if (at >= 0) _pfChartCanvases.splice(at, 1);
    if (_pfChartFocused === c) _pfChartFocused = null;
    if (_pfChartCanvas === c) _pfChartCanvas = null;
    (c.handlers || []).forEach(function (row) { c.host.removeEventListener(row[0], row[1], row[2]); });
    // A frame already queued would otherwise resize a chart that is gone.
    if (c.resizeFrame) { cancelAnimationFrame(c.resizeFrame); c.resizeFrame = 0; }
    if (c.clock) { clearInterval(c.clock); c.clock = 0; }
    _pfChartCanvasStopMotion(c);
    if (c.ro) { try { c.ro.disconnect(); } catch (err) {} }
    else if (c.onWindowResize) window.removeEventListener('resize', c.onWindowResize);
    if (c.themeObserver) { try { c.themeObserver.disconnect(); } catch (err) {} }
  });
}

// The actual "high definition" fix. The backing store is sized in DEVICE
// pixels and the context scaled by the ratio, so a 1px line is one real pixel
// on a retina screen. The SVG cannot do this: it is laid out once at a fixed
// 1440×660 and then stretched to whatever width the panel happens to be.
function _pfChartCanvasResize(cc) {
  var c = cc || _pfChartCanvas;
  if (!c || !c.host.isConnected) return;
  var box = c.host.getBoundingClientRect();
  var cssW = Math.max(Math.round(box.width), 1);
  var cssH = Math.max(Math.round(box.height), 1);
  var dpr = Math.max(window.devicePixelRatio || 1, 1);
  if (cssW === c.w && cssH === c.h && dpr === c.dpr) return;
  // Ignore a wobble of a pixel or two once the chart is up. Sub-pixel layout
  // rounding and a scrollbar arriving and leaving can nudge the box back and
  // forth forever; redrawing on each nudge is a visible flicker and buys
  // nothing, since a 1px change is not perceptible in the drawing. The first
  // paint (c.w === 0) and any DPR change always go through.
  if (c.w && dpr === c.dpr && Math.abs(cssW - c.w) <= 2 && Math.abs(cssH - c.h) <= 2) return;
  c.w = cssW; c.h = cssH; c.dpr = dpr;
  [c.main, c.overlay].forEach(function (cv) {
    cv.width = Math.round(cssW * dpr);
    cv.height = Math.round(cssH * dpr);
    cv.style.width = cssW + 'px';
    cv.style.height = cssH + 'px';
  });
  // Setting width/height resets the context, so the DPR transform has to be
  // reapplied every time — after which everything downstream draws in plain
  // CSS pixels and never has to think about the ratio again.
  c.ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  c.octx.setTransform(dpr, 0, 0, dpr, 0, 0);
  _pfChartCanvasDraw(c);
}

function _pfChartCanvasStructures(d) {
  // Same helper the Terminal campaign chart uses, so the cap cannot be three
  // in one chart and seven in another.
  return {
    legs: window.pfChartLatestStructures(d.legs),
    trendlines: window.pfChartLatestStructures(d.trendlines, function (tl) { return tl && tl.active; }),
  };
}

function _pfChartCanvasTimeframeSeconds(d) {
  var tf = String(d.timeframe || d.campaign_timeframe || '5m').toLowerCase();
  var match = tf.match(/^(\d+)(m|h|d|w)$/);
  if (!match) return 300;
  var units = { m: 60, h: 3600, d: 86400, w: 604800 };
  return Math.max(Number(match[1]) * units[match[2]], 1);
}

// ── Bar-index axis ───────────────────────────────────────────
// The x-axis walks BARS, not the clock.  NSE trades 09:15–15:30 and sleeps
// nights and weekends; projected on wall time, five sessions of candles bunch
// into thin stripes separated by voids (Phil: "connect the candles without any
// gaps").  So x is the candle's position in the array, times are interpolated
// only for labels, and anything between two sessions simply does not exist —
// the same convention TradingView draws with.
function _pfChartCanvasAxisBuild(d) {
  var candles = d.candles || [];
  var times = [];
  for (var i = 0; i < candles.length; i++) times.push(Number(candles[i].t));
  var diffs = [];
  for (var j = 1; j < times.length; j++) {
    var step = times[j] - times[j - 1];
    if (step > 0) diffs.push(step);
  }
  diffs.sort(function (a, b) { return a - b; });
  // Median, not mean: one weekend in the diffs would drag a mean to hours.
  var barSec = diffs.length ? diffs[Math.floor(diffs.length / 2)] : _pfChartCanvasTimeframeSeconds(d);
  // future_t0: the next session's open, where the bars drawn ahead of the
  // price (the Scalp chart's next-day CPR) begin, future_gap bars past the last.
  var futureT0 = Number(d.future_bars) > 0 && Number(d.future_t0) > 0 ? Number(d.future_t0) : 0;
  return { source: candles, times: times, barSec: Math.max(barSec, 1), futureT0: futureT0, futureGap: Math.max(0, Number(d.future_gap) || 0) };
}

function _pfChartCanvasAxisOf(c) {
  var d = c.data || {};
  var candles = d.candles || [];
  var futureT0 = Number(d.future_bars) > 0 && Number(d.future_t0) > 0 ? Number(d.future_t0) : 0;
  if (!c.axis || c.axis.source !== candles || c.axis.futureT0 !== futureT0) c.axis = _pfChartCanvasAxisBuild(d);
  return c.axis;
}

// Epoch seconds -> fractional bar index. Piecewise-linear inside the data,
// extrapolated at one bar per barSec beyond either edge.
function _pfChartCanvasIdxOf(axis, t) {
  var times = axis.times;
  if (!times.length) return 0;
  t = Number(t);
  if (t <= times[0]) return (t - times[0]) / axis.barSec;
  var last = times.length - 1;
  if (axis.futureT0 && t >= axis.futureT0) return last + axis.futureGap + (t - axis.futureT0) / axis.barSec;
  if (t >= times[last]) return last + (t - times[last]) / axis.barSec;
  var lo = 0, hi = last;
  while (hi - lo > 1) {
    var mid = (lo + hi) >> 1;
    if (times[mid] <= t) lo = mid; else hi = mid;
  }
  return lo + (t - times[lo]) / Math.max(times[hi] - times[lo], 1);
}

// Fractional bar index -> epoch seconds, for axis and crosshair labels only.
function _pfChartCanvasTimeAt(axis, idx) {
  var times = axis.times;
  if (!times.length) return 0;
  idx = Number(idx);
  if (idx <= 0) return times[0] + idx * axis.barSec;
  var last = times.length - 1;
  if (axis.futureT0 && idx >= last + axis.futureGap) return axis.futureT0 + (idx - last - axis.futureGap) * axis.barSec;
  if (idx >= last) return times[last] + (idx - last) * axis.barSec;
  var lo = Math.floor(idx);
  return times[lo] + (idx - lo) * (times[lo + 1] - times[lo]);
}

// How many bars of context frame the trade on either side.
// How far past the candles a level may drag the y-axis, in multiples of the
// visible candle range. ONE: the candles are the subject of the chart, and a
// rung a full range away is already the furthest thing worth seeing beside
// them. Deeper ones stay drawn and return on zoom-out.
var _PF_CHART_LEVEL_FIT_SPANS = 1;
var _PF_CHART_TRADE_PAD_BARS = 25;

// Initial fit: the TRADE, not the whole fetch.  With entries the window is 25
// bars before the first buy (the mother always kept in view) to 25 past the
// exit; without any it falls back to every candle.  The price range then fits
// what is actually visible plus the drawn lines — fitting the whole fetch
// squashed the trade into a sliver whenever later days fell far below it.
// tMin/tMax are BAR INDICES; only the names are inherited.
function _pfChartCanvasFit(c, initial) {
  var d = c.data || {};
  var candles = d.candles || [];
  if (!candles.length) return null;
  var axis = _pfChartCanvasAxisOf(c);
  var last = candles.length - 1;
  // future_bars: empty bars kept past the last candle for levels drawn ahead
  // of the price -- the Scalp chart's next-day CPR (2026-10-29).
  var rightEdge = last + 0.5 + Math.max(0, Number(d.future_bars) || 0);
  var iMin = -0.5, iMax = rightEdge;
  var entries = d.entries || [], exits = d.exits || [];
  if (entries.length) {
    var entryIdx = Infinity;
    entries.forEach(function (fill) {
      if (fill && fill.t != null) entryIdx = Math.min(entryIdx, _pfChartCanvasIdxOf(axis, fill.t));
    });
    var exitIdx = last;
    if (exits.length) {
      exitIdx = 0;
      exits.forEach(function (exit) {
        if (exit && exit.t != null) exitIdx = Math.max(exitIdx, _pfChartCanvasIdxOf(axis, exit.t));
      });
    }
    var motherIdx = 0;
    for (var m = 0; m < candles.length; m++) {
      if (candles[m].is_mother) { motherIdx = m; break; }
    }
    if (isFinite(entryIdx)) {
      iMin = Math.max(-0.5, Math.min(motherIdx - 2, entryIdx - _PF_CHART_TRADE_PAD_BARS) - 0.5);
      iMax = Math.min(last + 0.5, exitIdx + _PF_CHART_TRADE_PAD_BARS + 0.5);
    }
  }
  // fit_bars: N frames the newest N bars, widened only to keep a marked
  // entry in view -- the Scalp option chart's "one day's worth of candles"
  // (Phil, 2026-10-15). initial_bars, when given, replaces it for the FIRST
  // frame of a payload only, so a caller can reopen at the span the viewer
  // last chose while the reset button still returns to fit_bars.
  var barsWanted = Number((initial && d.initial_bars) || d.fit_bars) || 0;
  if (barsWanted > 0) {
    var keepBars = entries.length && isFinite(entryIdx) ? iMin : Infinity;
    // N CANDLES, counted back from the last real one; tomorrow's empty CPR
    // block rides on after them. Counted from the right edge instead, a span
    // narrower than that block (77 bars on 5m once the day has closed) began
    // past the last candle and the fit below read a bar that does not exist:
    // "Cannot read properties of undefined (reading 'l')" (Phil, 2026-11-02).
    iMin = Math.max(-0.5, Math.min(last + 0.5 - barsWanted, keepBars));
    iMax = rightEdge;
  }
  // fit_sessions: N frames the last N trading days (IST dates) up to the
  // latest bar, widened only to keep a marked entry in view.
  var sessions = barsWanted > 0 ? 0 : Number(d.fit_sessions) || 0;
  if (sessions > 0) {
    var seen = 0, day = null, startIdx = 0;
    for (var k = last; k >= 0; k--) {
      var dayNo = Math.floor((Number(candles[k].t) + 19800) / 86400);
      if (dayNo !== day) {
        day = dayNo;
        seen += 1;
        if (seen > sessions) { startIdx = k + 1; break; }
      }
    }
    var keepEntry = entries.length && isFinite(entryIdx) ? iMin : Infinity;
    iMin = Math.max(-0.5, Math.min(startIdx - 0.5, keepEntry));
    iMax = rightEdge;
  }
  // Never past the data: at least the last candle is measured.
  var from = Math.min(Math.max(Math.ceil(iMin), 0), last), to = Math.max(Math.min(Math.floor(iMax), last), from);
  var lo = Number(candles[from].l), hi = Number(candles[from].h);
  for (var i = from; i <= to; i++) {
    lo = Math.min(lo, Number(candles[i].l));
    hi = Math.max(hi, Number(candles[i].h));
  }
  if (d.mother && d.mother.high) hi = Math.max(hi, Number(d.mother.high));
  if (d.mother && d.mother.low) lo = Math.min(lo, Number(d.mother.low));
  var structures = _pfChartCanvasStructures(d);
  structures.legs.forEach(function (leg) {
    if (leg.touch_high) hi = Math.max(hi, Number(leg.touch_high));
    if (leg.low) lo = Math.min(lo, Number(leg.low));
  });
  if (d.tp_price) {
    hi = Math.max(hi, Number(d.tp_price));
    lo = Math.min(lo, Number(d.tp_price));
  }
  // Levels are allowed to PULL the fit only as far as the price action itself.
  // A fib ladder extrapolates: L16 sits sixteen swing-spans below the swing
  // high, which on BANKNIFTY put it 3,600 points under anything that ever
  // traded and squeezed every candle into the top tenth of the pane. So a level
  // joins the fit only if it is already near the market; the deep ones are
  // still DRAWN, and come back into view on zoom-out because hline() skips
  // whatever is off-axis. Same rule CryptoForge's cascade chart has always
  // used -- fit the prices the market printed, never the extrapolation.
  var candleSpan = (hi - lo) || Math.max(Math.abs(hi) * 0.02, 1);
  var nearLo = lo - candleSpan * _PF_CHART_LEVEL_FIT_SPANS;
  var nearHi = hi + candleSpan * _PF_CHART_LEVEL_FIT_SPANS;
  (d.lines || []).forEach(function (line) {
    if (!line || line.price == null) return;
    var price = Number(line.price);
    if (!isFinite(price) || price < nearLo || price > nearHi) return;
    hi = Math.max(hi, price);
    lo = Math.min(lo, price);
  });
  var priceSpan = (hi - lo) || Math.max(Math.abs(hi) * 0.02, 1);
  var padP = priceSpan * 0.06;
  return { tMin: iMin, tMax: iMax, pMin: lo - padP, pMax: hi + padP };
}

// Only fields that can change Canvas pixels belong in this key. A refresh that
// merely changes table/status data can keep the already-painted surface; a new
// candle, level or marker asks the normal draw pipeline to repaint coherently.
function _pfChartCanvasPaintKey(d) {
  d = d || {};
  return JSON.stringify({
    candles: d.candles || [], mother: d.mother || null,
    legs: d.legs || [], trendlines: d.trendlines || [], lines: d.lines || [],
    fills: d.fills || [], entries: d.entries || [], exits: d.exits || [],
    avg_entry_price: d.avg_entry_price, tp_price: d.tp_price,
    tp_label: d.tp_label || '', timeframe: d.timeframe,
    indicators: d.indicators || null,
    // Overlays paint too (EMA, Supertrend): without them here, a payload that
    // changed only an overlay -- the Scalp chart's ST switch -- never repainted.
    overlays: d.overlays || [],
    drawings: d.drawings || []
  });
}

function _pfChartCanvasSameViewport(a, b) {
  return !!a && !!b && a.tMin === b.tMin && a.tMax === b.tMax && a.pMin === b.pMin && a.pMax === b.pMax;
}

// Captured before the chart API refresh begins. At the right edge means the
// view is following the latest bar; a panned-back research view is never
// allowed to jump just because a poll returned a newer payload.
function _pfChartCanvasRefreshState() {
  var c = _pfChartCanvas, fit = c && _pfChartCanvasFit(c);
  if (!c || !c.viewport || !fit) return null;
  var tolerance = Math.max(0.5, (c.viewport.tMax - c.viewport.tMin) * 0.001);
  return {
    viewport: { tMin: c.viewport.tMin, tMax: c.viewport.tMax, pMin: c.viewport.pMin, pMax: c.viewport.pMax },
    followRight: Math.abs(c.viewport.tMax - fit.tMax) <= tolerance
  };
}

// Update the existing two Canvas surfaces instead of destroying/recreating
// them. This retains pointer bindings and DPR backing stores, and it gives the
// renderer a chance to skip paint work when the chart-specific payload did not
// actually change.
function _pfChartCanvasRefresh(d, saved) {
  var c = _pfChartCanvas;
  if (!c || !c.host || !c.host.isConnected) return false;
  var oldKey = c.paintKey, oldViewport = c.viewport;
  // A DRAWING IN HAND KEEPS ITS PLACE. The Scalp chart refreshes every few
  // seconds and rebuilds its drawings from storage; one landing between the
  // last move of a drag and the release put back the stored (unmoved) line,
  // and the release saved that -- a dragged trendline snapped back (found by
  // CI, 2026-10-21). While a drawing is being made or moved, the one on
  // screen is kept.
  var drawing = (c.drag && c.drag.kind === 'draw') || c.drawPending;
  if (drawing && c.data && Array.isArray(c.data.drawings) && d && d !== c.data) d.drawings = c.data.drawings;
  c.data = d;
  var fit = _pfChartCanvasFit(c, !(saved && saved.viewport));
  if (!fit) return false;
  var next = saved && saved.viewport ? {
    tMin: saved.viewport.tMin, tMax: saved.viewport.tMax,
    pMin: saved.viewport.pMin, pMax: saved.viewport.pMax
  } : fit;
  if (saved && saved.followRight) {
    var span = saved.viewport.tMax - saved.viewport.tMin;
    next.tMax = fit.tMax;
    next.tMin = fit.tMax - span;
  }
  c.viewport = next;
  var nextKey = _pfChartCanvasPaintKey(d);
  // A changed viewport moves every projected pixel, so it must repaint as a
  // whole. With an unchanged viewport, this intentionally skips paint if none
  // of the independent draw layers received new data.
  if (!_pfChartCanvasSameViewport(oldViewport, next) || oldKey !== nextKey) _pfChartCanvasDraw(c);
  return true;
}

// xOf/yOf and their inverses are kept together and exposed on canvas state.
// Phase 3 uses the inverse pair for cursor-anchored zoom and the crosshair.
// The horizontal domain is bar indices; xOf accepts epoch seconds and converts
// through the axis, so every draw layer keeps passing timestamps unchanged.
// THE WINDOW IN TIME, carried from one chart to the next (Phil, 2026-10-19:
// "the chart resets again and again if I move to other strike price or CE to
// PE ... make it stay what I have viewed before"). Read as clock times, so it
// survives a new contract or a new timeframe: the same stretch of the day,
// still riding the live edge if it was. Only the price axis is fitted afresh,
// to the new contract's own candles in that stretch.
function pfChartCurrentWindow() {
  var c = _pfChartCanvas;
  if (!c || !c.viewport || !((c.data || {}).candles || []).length) return null;
  var axis = _pfChartCanvasAxisOf(c), fit = _pfChartCanvasFit(c);
  var tolerance = Math.max(0.5, (c.viewport.tMax - c.viewport.tMin) * 0.001);
  return {
    from: _pfChartCanvasTimeAt(axis, c.viewport.tMin),
    to: _pfChartCanvasTimeAt(axis, c.viewport.tMax),
    followRight: !!fit && Math.abs(c.viewport.tMax - fit.tMax) <= tolerance
  };
}

function pfChartApplyWindow(win) {
  var c = _pfChartCanvas, bars = ((c && c.data) || {}).candles || [];
  if (!c || !win || !bars.length) return false;
  var axis = _pfChartCanvasAxisOf(c), fit = _pfChartCanvasFit(c);
  if (!fit) return false;
  var a = _pfChartCanvasIdxOf(axis, win.from), b = _pfChartCanvasIdxOf(axis, win.to);
  if (!isFinite(a) || !isFinite(b) || b <= a) return false;
  if (win.followRight) { var span = b - a; b = fit.tMax; a = b - span; }
  var from = Math.max(0, Math.ceil(a)), to = Math.min(bars.length - 1, Math.floor(b));
  if (to < from) { from = Math.max(0, Math.min(bars.length - 1, Math.round(a))); to = from; }
  var lo = Infinity, hi = -Infinity;
  for (var i = from; i <= to; i++) {
    lo = Math.min(lo, Number(bars[i].l));
    hi = Math.max(hi, Number(bars[i].h));
  }
  if (!isFinite(lo) || !isFinite(hi)) { lo = fit.pMin; hi = fit.pMax; }
  else { var pad = ((hi - lo) || Math.max(Math.abs(hi) * 0.02, 1)) * 0.06; lo -= pad; hi += pad; }
  _pfChartCanvasSetViewport(c, { tMin: a, tMax: b, pMin: lo, pMax: hi });
  return true;
}

function _pfChartCanvasProjection(c) {
  var v = c.viewport;
  if (!v) return null;
  var axis = _pfChartCanvasAxisOf(c);
  // Classic's 150/62/26/34 gutters live in a 1440×660 viewBox. Scale that
  // exact geometry to the real Canvas surface (rather than retaining 150 CSS
  // pixels at every width), so both engines put the same payload in the same
  // horizontal lane. The floors keep labels readable on a narrow screen.
  // payload.padLeft: a phone's chart gives its candles the room the 86px
  // label gutter would take (the Scalp desk, 2026-11-05: on a 390px screen
  // that gutter left the candles a 200px strip).
  var padL = Number(c.data && c.data.padLeft) > 0 ? Number(c.data.padLeft) : Math.max(86, 150 * c.w / 1440);
  var padR = Number(c.data && c.data.padRight) > 0 ? Number(c.data.padRight) : Math.max(42, 62 * c.w / 1440);
  // A chart that lays a row of controls over its top asks for room for it
  // (payload.padTop), so the plot's first line starts below them.
  var padT = Math.max(18, 26 * c.h / 660, Number((c.data && c.data.padTop) || 0) || 0);
  var padB = Math.max(26, 34 * c.h / 660);
  var plotW = Math.max(c.w - padL - padR, 1);
  var fullH = Math.max(c.h - padT - padB, 1);
  // A SUB-PANE IS OPT-IN. Reserving the band unconditionally would move every
  // pixel of the four charts that already exist; keyed off the payload, a chart
  // that sends no RSI is byte-identical to before this pane existed.
  var rsi = (c.data && c.data.indicators && c.data.indicators.rsi) || null;
  // A FLOOR WOULD EAT THE PRICE. `Math.max(46, ...)` took 46px of a 130px plot
  // in testing and left the candles 71px tall. The band is a SHARE of the
  // height, capped, and simply not drawn when the surface is too short for a
  // readable one -- the price pane always keeps at least ~three quarters.
  // Its height can be dragged by the line between the panes (Phil,
  // 2026-10-28: "I am unable to drag the RSI... Make it dragable"); a height
  // chosen here (c.rsiPaneH) or kept by the page (payload.rsiPaneH) holds,
  // never under 50px and never past 60% of the chart.
  var rsiWant = Number(c.rsiPaneH || (c.data && c.data.rsiPaneH) || 0);
  var rsiH = rsi && rsi.length && fullH >= 190
    ? (rsiWant > 0 ? Math.max(50, Math.min(fullH * 0.6, rsiWant)) : Math.min(120, fullH * 0.22)) : 0;
  var rsiGap = rsiH ? Math.max(10, padT * 0.5) : 0;
  var plotH = Math.max(fullH - rsiH - rsiGap, 40);
  var priceTop = padT;
  var rsiTop = padT + plotH + rsiGap;
  // The RSI axis stretches like the price axis (drag it; double-click puts it
  // back to 0-100). Phil, 2026-10-20: "the chart Y axis when we hover we get
  // a up and down arrow right? the same way RSI Y axis has to have it".
  var rr = c.rsiRange || { lo: 0, hi: 100 };
  var rSpan = Math.max(rr.hi - rr.lo, 1);
  var tSpan = Math.max(v.tMax - v.tMin, 1);
  var pSpan = Math.max(v.pMax - v.pMin, Number.EPSILON);
  var p = {
    padL: padL, padR: padR, padT: priceTop, padB: padB, plotW: plotW, plotH: plotH,
    // A chart may ask for a larger floor (payload.fontFloor): the docked
    // Scalp chart's ~1000px width put its axis numbers at ~7px.
    fontScale: Math.max(Number((c.data && c.data.fontFloor) || 0) || 0.75, Math.min(1, c.w / 1440, c.h / 660)),
    xOf: function (t) { return padL + ((_pfChartCanvasIdxOf(axis, t) - v.tMin) / tSpan) * plotW; },
    yOf: function (price) { return priceTop + ((v.pMax - Number(price)) / pSpan) * plotH; },
    tAt: function (x) { return v.tMin + ((Number(x) - padL) / plotW) * tSpan; },
    timeAt: function (idx) { return _pfChartCanvasTimeAt(axis, idx); },
    pAt: function (y) { return v.pMax - ((Number(y) - priceTop) / plotH) * pSpan; },
    inPrice: function (price) { return Number(price) >= v.pMin && Number(price) <= v.pMax; },
    rsiH: rsiH, rsiGap: rsiGap, rsiTop: rsiTop, rsiLo: rr.lo, rsiHi: rr.hi,
    // RSI is bounded 0-100 by construction, so this axis is never fitted to
    // the data -- a fitted one would silently rescale the 70/30 lines that
    // are the whole point of looking at it. Only a hand on the axis moves it.
    rsiYOf: function (value) {
      return rsiTop + ((rr.hi - Number(value)) / rSpan) * rsiH;
    },
    rsiAt: function (y) { return rr.hi - ((Number(y) - rsiTop) / rsiH) * rSpan; }
  };
  c.projection = p;
  return p;
}

function _pfChartCanvasText(ctx, text, x, y, color, size, align, weight, scale) {
  ctx.fillStyle = color;
  ctx.font = (weight || '400') + ' ' + ((size || 10) * (scale || 1)) + 'px monospace';
  ctx.textAlign = align || 'left';
  ctx.textBaseline = 'middle';
  ctx.fillText(String(text), x, y);
}

function _pfChartCanvasClip(ctx, p, draw) {
  ctx.save();
  ctx.beginPath();
  ctx.rect(p.padL, p.padT, p.plotW, p.plotH);
  ctx.clip();
  draw();
  ctx.restore();
}

function _pfChartCanvasGridAxes(c, p, PAL) {
  var ctx = c.ctx, d = c.data || {}, candles = d.candles || [];
  var labels = 0;
  ctx.lineWidth = 1;
  for (var g = 0; g <= 4; g++) {
    var price = c.viewport.pMin + (c.viewport.pMax - c.viewport.pMin) * (g / 4);
    var y = p.yOf(price);
    ctx.strokeStyle = PAL.grid;
    ctx.beginPath(); ctx.moveTo(p.padL, y); ctx.lineTo(p.padL + p.plotW, y); ctx.stroke();
    // Gridline prices are round numbers on a big coin, a coin's own fixed
    // decimals on a small one -- never a mix of "79,523.5" and "78,591.38".
    var axisText = Math.abs(price) >= 1000 ? Math.round(price).toLocaleString('en-US') : _pfChartPrice(price, true);
    _pfChartCanvasText(ctx, axisText,
      p.padL + p.plotW + 6, y, PAL.axis, 9.5, 'left', null, p.fontScale);
    labels++;
  }
  // Ticks come from the candles actually inside the viewport — the window is
  // usually a clipped slice of the fetch, and a tick from outside it would be
  // painted into the gutters.
  var from = Math.max(Math.ceil(c.viewport.tMin), 0);
  var to = Math.min(Math.floor(c.viewport.tMax), candles.length - 1);
  if (to >= from) {
    // As many as fit: six labels on a phone's plot overprinted each other.
    var ticks = Math.min(6, Math.max(2, Math.floor(p.plotW / 95)), to - from + 1);
    for (var i = 0; i < ticks; i++) {
      var ci = Math.round(from + (to - from) * (i / Math.max(ticks - 1, 1)));
      // Kept whole inside the canvas: on a narrow gutter the first and last
      // labels, centred on their bars, ran off the edges.
      ctx.font = '400 ' + (9.5 * (p.fontScale || 1)) + 'px monospace';
      var tickText = _pfChartIst(candles[ci].t), half = ctx.measureText(tickText).width / 2 + 2;
      var tickX = Math.min(Math.max(p.xOf(candles[ci].t), half), c.w - half);
      _pfChartCanvasText(ctx, tickText, tickX, c.h - 8, PAL.axis, 9.5, 'center', null, p.fontScale);
      labels++;
    }
  }
  return labels;
}

function _pfChartCanvasCandleWidth(d, p) {
  var candles = d.candles || [];
  if (candles.length < 2) return p.plotW;
  return Math.abs(p.xOf(candles[1].t) - p.xOf(candles[0].t));
}

function _pfChartCanvasMotherColumn(c, p, PAL) {
  var ctx = c.ctx, d = c.data || {}, bodyW = Math.max(Math.min(_pfChartCanvasCandleWidth(d, p) * 0.65, 9), 1);
  _pfChartCanvasClip(ctx, p, function () {
    (d.candles || []).forEach(function (bar) {
      if (!bar.is_mother) return;
      var x = p.xOf(bar.t);
      ctx.fillStyle = PAL.mother;
      ctx.globalAlpha = 0.09;
      ctx.fillRect(x - Math.max(bodyW, 6) / 2 - 3, p.padT + 1, Math.max(bodyW, 6) + 6, p.plotH - 2);
      ctx.globalAlpha = 1;
    });
  });
}

// Session-open gaps, drawn the way Phil's Pine reference draws them: a
// translucent synthetic candle spanning the previous close to the new open,
// sitting on the bar where the gap happened.  With the bar axis there is no
// blank space to look at, so this block is the only trace a gap leaves.
function _pfChartCanvasGapCandles(c, p, PAL) {
  var ctx = c.ctx, d = c.data || {}, candles = d.candles || [];
  var bodyW = Math.max(Math.min(_pfChartCanvasCandleWidth(d, p) * 0.65, 9), 1);
  var count = 0;
  _pfChartCanvasClip(ctx, p, function () {
    for (var i = 1; i < candles.length; i++) {
      var prev = candles[i - 1], bar = candles[i];
      // A hole in the exchange's history (maintenance, a delisting pause):
      // crypto has no sessions, so this is the only gap there can be.
      if (Number(bar.t) - Number(prev.t) < 4 * 3600) continue;
      var prevClose = Number(prev.c), open = Number(bar.o);
      if (!isFinite(prevClose) || !isFinite(open) || prevClose === open) continue;
      var x = p.xOf(bar.t), up = open > prevClose;
      var top = p.yOf(Math.max(prevClose, open)), bottom = p.yOf(Math.min(prevClose, open));
      ctx.fillStyle = up ? PAL.up : PAL.down;
      ctx.globalAlpha = 0.22;
      ctx.fillRect(x - (bodyW + 4) / 2, top, bodyW + 4, Math.max(bottom - top, 1));
      ctx.globalAlpha = 0.6;
      ctx.strokeStyle = up ? PAL.up : PAL.down;
      ctx.lineWidth = 0.8;
      ctx.strokeRect(x - (bodyW + 4) / 2, top, bodyW + 4, Math.max(bottom - top, 1));
      ctx.globalAlpha = 1;
      count++;
    }
  });
  return count;
}

function _pfChartCanvasCandles(c, p, PAL) {
  var ctx = c.ctx, d = c.data || {}, bodyW = Math.max(Math.min(_pfChartCanvasCandleWidth(d, p) * 0.65, 9), 1);
  var count = 0;
  // Only the bars in view (x is the bar index), one either side for the edge.
  var all = d.candles || [], v = c.viewport || {};
  var first = Math.max(0, Math.floor(Number(v.tMin)) - 1), last = Math.min(all.length - 1, Math.ceil(Number(v.tMax)) + 1);
  if (!isFinite(first) || !isFinite(last)) { first = 0; last = all.length - 1; }
  _pfChartCanvasClip(ctx, p, function () {
    all.slice(first, last + 1).forEach(function (bar) {
      var x = p.xOf(bar.t), up = Number(bar.c) >= Number(bar.o), color = up ? PAL.up : PAL.down;
      ctx.strokeStyle = PAL.wick || color; ctx.lineWidth = 1;
      ctx.beginPath(); ctx.moveTo(x, p.yOf(bar.h)); ctx.lineTo(x, p.yOf(bar.l)); ctx.stroke();
      var top = p.yOf(Math.max(Number(bar.o), Number(bar.c)));
      var bottom = p.yOf(Math.min(Number(bar.o), Number(bar.c)));
      ctx.fillStyle = color;
      ctx.fillRect(x - bodyW / 2, top, bodyW, Math.max(bottom - top, 1));
      // The outline TradingView gives a candle on a light sheet. Skipped on
      // hairline bodies, where it would paint the whole candle black.
      if (PAL.wick && bodyW >= 4) {
        ctx.lineWidth = 0.6;
        ctx.strokeRect(x - bodyW / 2, top, bodyW, Math.max(bottom - top, 1));
      }
      if (bar.is_mother) {
        ctx.strokeStyle = PAL.mother; ctx.lineWidth = 1.4;
        ctx.strokeRect(x - bodyW / 2 - 1, p.yOf(bar.h) - 1, bodyW + 2,
          Math.max(p.yOf(bar.l) - p.yOf(bar.h) + 2, 4));
        _pfChartCanvasText(ctx, 'MC', x, Math.max(p.yOf(bar.h) - 8, p.padT + 10), PAL.mother, 9.5, 'center', '700', p.fontScale);
      }
      count++;
    });
  });
  return count;
}

function _pfChartCanvasTrendlines(c, p, PAL, labels) {
  var d = c.data || {}, ctx = c.ctx, drawn = 0;
  _pfChartCanvasClip(ctx, p, function () {
    _pfChartCanvasStructures(d).trendlines.forEach(function (tl) {
      var a1 = tl.a1, a2 = tl.a2;
      if (!a1 || !a2 || Number(a2.t) === Number(a1.t)) return;
      // The line is straight through its two anchors in BAR space (the same
      // convention TradingView extends drawings with), so it is extended in
      // pixels rather than re-derived from a wall-clock slope.
      var x1 = p.xOf(a1.t), y1 = p.yOf(a1.p);
      var x2 = p.xOf(a2.t), y2 = p.yOf(a2.p);
      if (x2 === x1) return;
      var slope = (y2 - y1) / (x2 - x1);
      // FROM the first anchor, not through it. Every trendline on this site is
      // carried down from a mother candle's high; drawing it back past the
      // mother put a line over bars that existed before the structure did
      // (Phil, 2026-08-18: "just start from that mother candle high"). It still
      // extends to the right edge -- a standing line is live until broken.
      var xLeft = Math.max(p.padL, Math.min(x1, x2)), xRight = p.padL + p.plotW;
      var yLeft = y1 + slope * (xLeft - x1), yRight = y1 + slope * (xRight - x1);
      var color = PAL.fibs[(Math.max(1, Number(tl.id) || 1) - 1) % PAL.fibs.length];
      var noFib = tl.bears_fib === false;
      ctx.strokeStyle = color; ctx.lineWidth = tl.active ? 1.3 : 0.9;
      ctx.globalAlpha = noFib ? 0.35 : (tl.active ? 0.95 : 0.5);
      ctx.setLineDash(noFib ? [6, 4] : []);
      ctx.beginPath(); ctx.moveTo(xLeft, yLeft); ctx.lineTo(xRight, yRight); ctx.stroke();
      ctx.setLineDash([]); ctx.globalAlpha = 1;
      if (yRight >= p.padT && yRight <= p.padT + p.plotH) labels.push({ kind: 'right', x: xRight - 4, y: yRight - 5,
        text: 'TL' + tl.id + (noFib ? ' (no fib)' : (tl.active ? ' ★' : '')), color: color });
      drawn++;
    });
  });
  return drawn;
}

function _pfChartCanvasHline(c, p, labels, price, color, text, dash, width, opacity) {
  if (!p.inPrice(price)) return false;
  var ctx = c.ctx, y = p.yOf(price);
  _pfChartCanvasClip(ctx, p, function () {
    ctx.strokeStyle = color; ctx.lineWidth = width || 0.9; ctx.globalAlpha = opacity || 1;
    ctx.setLineDash(dash || []);
    ctx.beginPath(); ctx.moveTo(p.padL, y); ctx.lineTo(p.padL + p.plotW, y); ctx.stroke();
    ctx.setLineDash([]); ctx.globalAlpha = 1;
  });
  if (text) labels.push({ kind: 'gutter', y: y, text: text, color: color });
  return true;
}

// Plain labelled price lines, for strategies that have no fib ladder to draw.
// The two-red ladder's rungs are buy-stops read off candle closes, not levels
// measured from a mother — same picture, different arithmetic behind it.
function _pfChartCanvasLines(c, p, PAL, labels) {
  var d = c.data || {}, count = 0;
  (d.lines || []).forEach(function (line, index) {
    if (line == null || line.price == null) return;
    // A line may name its own colour/dash (CPR, R1-R4, S1-S5, the dotted LIVE
    // line). Literal hex only -- Canvas silently ignores var(--x). Lines that
    // name nothing keep the old palette-by-index behaviour.
    var color = _pfChartSeriesColor(line.color, PAL, PAL.fibs[index % PAL.fibs.length]);
    var spent = Number(line.inr_notional) || 0;
    // A line with NO label is drawn silent: the line only, no price in the
    // gutter. Under "where two meet" the fib levels are ghosted structure
    // behind the zones, and printing "(24,093.15)" for every one of them put
    // 35 numbers down the left edge over two zones that mattered.
    var text = line.label === '' && spent <= 0
      ? ''
      : String(line.label || '') + ' (' + _pfChartPrice(line.price) + ')'
        + (spent > 0 ? '  ' + _pfChartInr(spent) : '');
    var dash = Array.isArray(line.dash) ? line.dash : (line.filled ? [] : [4, 3]);
    var width = Number(line.width) || (line.filled ? 1.1 : 0.8);
    var opacity = Number(line.opacity) || (line.filled ? 0.9 : 0.45);
    count += _pfChartCanvasHline(c, p, labels, Number(line.price), color, text, dash, width, opacity) ? 1 : 0;
  });
  return count;
}

// SUPERTREND, SHADED THE WAY TRADINGVIEW SHADES IT (Phil, 2026-10-14): green
// under the price while the trend is up, red over it while it is down, and the
// band between the line and each candle's body middle ((open+close)/2) filled
// faintly in the same colour. One call per same-direction run, so a flip is a
// break in the line and never a diagonal joining the two sides.
var PF_ST_UP = '#4caf50', PF_ST_DOWN = '#ff5252';

function _pfChartBodyMids(d) {
  var mids = {};
  (d.candles || []).forEach(function (bar) { mids[bar.t] = (Number(bar.o) + Number(bar.c)) / 2; });
  return mids;
}

function _pfChartShadeToBody(ctx, p, pts, color, mids) {
  var shaded = pts.filter(function (pt) { return mids[pt.t] != null && isFinite(mids[pt.t]); });
  if (shaded.length >= 2) {
    ctx.save();
    ctx.globalAlpha = 0.13;
    ctx.fillStyle = color;
    ctx.beginPath();
    shaded.forEach(function (pt, i) {
      var x = p.xOf(pt.t), y = p.yOf(pt.price);
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    for (var j = shaded.length - 1; j >= 0; j--) ctx.lineTo(p.xOf(shaded[j].t), p.yOf(mids[shaded[j].t]));
    ctx.closePath();
    ctx.fill();
    ctx.restore();
  }
  ctx.save();
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.6;
  ctx.setLineDash([]);
  ctx.lineJoin = 'round';
  ctx.beginPath();
  pts.forEach(function (pt, i) {
    var x = p.xOf(pt.t), y = p.yOf(pt.price);
    if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
  });
  ctx.stroke();
  ctx.restore();
}

// Indicator polylines -- the 20-EMA and anything else that is a running series
// rather than a level. overlays: [{label, color, dash, width, points:[{t,price}]}]
function _pfChartCanvasOverlays(c, p, PAL, labels) {
  var d = c.data || {}, ctx = c.ctx, count = 0;
  (d.overlays || []).forEach(function (ov) {
    var pts = (ov && ov.points || []).filter(function (pt) { return pt && pt.t != null && pt.price != null; });
    if (pts.length < 2) return;
    var color = _pfChartSeriesColor(ov.color, PAL, PAL.avg);
    if (ov.supertrend) {
      // A run of the book's Supertrend: TradingView's colours, shaded to the candles.
      color = Number(ov.supertrend) > 0 ? PF_ST_UP : PF_ST_DOWN;
      var mids = _pfChartBodyMids(d);
      _pfChartCanvasClip(ctx, p, function () { _pfChartShadeToBody(ctx, p, pts, color, mids); });
      var tip = pts[pts.length - 1];
      if (ov.label && p.inPrice(tip.price)) labels.push({ kind: 'gutter', y: p.yOf(tip.price), text: String(ov.label), color: color });
      count += 1;
      return;
    }
    _pfChartCanvasClip(ctx, p, function () {
      ctx.strokeStyle = color;
      ctx.lineWidth = Number(ov.width) || 1.3;
      ctx.setLineDash(Array.isArray(ov.dash) ? ov.dash : []);
      ctx.globalAlpha = Number(ov.opacity) || 0.9;
      ctx.beginPath();
      pts.forEach(function (pt, i) {
        var x = p.xOf(pt.t), y = p.yOf(pt.price);
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
      });
      ctx.stroke();
      ctx.setLineDash([]); ctx.globalAlpha = 1;
    });
    var last = pts[pts.length - 1];
    if (ov.label && p.inPrice(last.price)) labels.push({ kind: 'gutter', y: p.yOf(last.price), text: String(ov.label), color: color });
    count += 1;
  });
  return count;
}

function _pfChartCanvasFibs(c, p, PAL, labels) {
  var d = c.data || {}, count = 0;
  function fmt(v) { return _pfChartPrice(v); }
  if (d.mother && d.mother.high) count += _pfChartCanvasHline(c, p, labels, Number(d.mother.high), PAL.mother,
    'MOTHER (' + fmt(d.mother.high) + ')', [5, 3], 1.1) ? 1 : 0;
  // The low half of the mother band. Every caller already sends it and the
  // renderer used to drop it on the floor, which on a fib chart hides the
  // measuring stick the whole ladder is stepped off.
  if (d.mother && d.mother.low) count += _pfChartCanvasHline(c, p, labels, Number(d.mother.low), PAL.mother,
    'MOTHER LOW (' + fmt(d.mother.low) + ')', [5, 3], 1.1, 0.7) ? 1 : 0;
  count += _pfChartCanvasLines(c, p, PAL, labels);
  _pfChartCanvasStructures(d).legs.forEach(function (leg) {
    var color = PAL.fibs[(Math.max(1, Number(leg.leg_id) || 1) - 1) % PAL.fibs.length];
    count += _pfChartCanvasHline(c, p, labels, Number(leg.touch_high), color,
      '0 (' + fmt(leg.touch_high) + ')', [], 0.8, 0.4) ? 1 : 0;
    count += _pfChartCanvasHline(c, p, labels, Number(leg.low), color,
      '1 (' + fmt(leg.low) + ')', [], 0.8, 0.4) ? 1 : 0;
    [2, 4, 8].forEach(function (level) {
      var price = leg.levels ? leg.levels[String(level)] : null;
      if (price == null) return;
      var order = (leg.orders || []).find(function (o) { return o.level === level; }) || {};
      var spent = Number(order.inr_notional) || 0;
      count += _pfChartCanvasHline(c, p, labels, Number(price), color,
        level + ' (' + fmt(price) + ')' + (spent > 0 ? '  ' + _pfChartInr(spent) : ''), [], 1.1, 0.9) ? 1 : 0;
    });
    if (leg.touch_timestamp && p.inPrice(leg.touch_high)) {
      var ctx = c.ctx;
      _pfChartCanvasClip(ctx, p, function () {
        ctx.strokeStyle = color; ctx.lineWidth = 1.5;
        ctx.beginPath(); ctx.arc(p.xOf(leg.touch_timestamp), p.yOf(leg.touch_high), 3.5, 0, Math.PI * 2); ctx.stroke();
      });
    }
  });
  // Upstream labelled this line "SOLD AT" whenever the campaign had ended, which
  // is only true when the target was the thing that ended it. A mother held to
  // expiry never traded here, so the caller names the line and this draws it.
  var hit = String(d.tp_label || '') === 'TARGET HIT';
  if (d.tp_price) count += _pfChartCanvasHline(c, p, labels, Number(d.tp_price), hit ? PAL.sellMark : PAL.tp,
    (d.tp_label || 'TARGET') + ' (' + fmt(d.tp_price) + ')', [6, 3], 1.2) ? 1 : 0;
  if (d.avg_entry_price) count += _pfChartCanvasHline(c, p, labels, Number(d.avg_entry_price), PAL.avg,
    'AVG ENTRY (' + fmt(d.avg_entry_price) + ')', [4, 4], 1.1) ? 1 : 0;
  return count;
}

// The candle a fill at `t` traded in: the bar that opened at or before it
// (13:13 is in the 13:10 five-minute bar, not the 13:15 one). Null when the
// fill lies outside the candles drawn.
function _pfChartCanvasBarAt(c, t) {
  var bars = ((c.data || {}).candles) || [];
  if (!bars.length || t == null) return null;
  var axis = _pfChartCanvasAxisOf(c);
  var idx = Math.floor(_pfChartCanvasIdxOf(axis, Number(t)) + 1e-6);
  if (idx < 0 || idx >= bars.length) return null;
  if (idx === bars.length - 1 && Number(t) >= Number(bars[idx].t) + axis.barSec) return null;
  return bars[idx];
}

function _pfChartCanvasMarkers(c, p, PAL, labels) {
  var d = c.data || {}, ctx = c.ctx, count = 0;
  var entries = (d.entries && d.entries.length) ? d.entries : (d.fills || []).map(function (fill) {
    return { t: fill && fill.timestamp, price: fill && fill.price };
  });
  _pfChartCanvasClip(ctx, p, function () {
    // THE ARROW SITS CLEAR OF ITS CANDLE, as TradingView's do (Phil,
    // 2026-10-16: "Make this buy arrow below the bought candle and sell above
    // the sold candle"): a buy points up from under the low of the candle it
    // filled in, a sell points down from over the high. Drawn at the fill
    // price it sat inside the candle's body and read as either.
    entries.forEach(function (fill) {
      if (!fill || !fill.price || !p.inPrice(fill.price)) return;
      var bar = _pfChartCanvasBarAt(c, fill.t);
      var x = bar ? p.xOf(bar.t) : p.xOf(fill.t);
      var tip = (bar ? p.yOf(bar.l) : p.yOf(fill.price)) + 4;
      ctx.fillStyle = PAL.buyMark; ctx.strokeStyle = PAL.markRing; ctx.lineWidth = 0.9;
      ctx.beginPath(); ctx.moveTo(x, tip); ctx.lineTo(x - 5, tip + 9); ctx.lineTo(x - 2, tip + 9); ctx.lineTo(x - 2, tip + 15);
      ctx.lineTo(x + 2, tip + 15); ctx.lineTo(x + 2, tip + 9); ctx.lineTo(x + 5, tip + 9); ctx.closePath(); ctx.fill(); ctx.stroke();
      count++;
    });
    (d.exits || []).forEach(function (exit) {
      if (!exit || !exit.price || !p.inPrice(exit.price)) return;
      var bar = _pfChartCanvasBarAt(c, exit.t);
      var x = bar ? p.xOf(bar.t) : p.xOf(exit.t);
      var tip = (bar ? p.yOf(bar.h) : p.yOf(exit.price)) - 4, y = tip - 15;
      ctx.fillStyle = PAL.sellMark; ctx.strokeStyle = PAL.markRing; ctx.lineWidth = 0.9;
      ctx.beginPath(); ctx.moveTo(x, tip); ctx.lineTo(x - 5, tip - 9); ctx.lineTo(x - 2, tip - 9); ctx.lineTo(x - 2, tip - 15);
      ctx.lineTo(x + 2, tip - 15); ctx.lineTo(x + 2, tip - 9); ctx.lineTo(x + 5, tip - 9); ctx.closePath(); ctx.fill(); ctx.stroke();
      // A null P&L is UNKNOWN, not zero. `Number(x) || 0` printed "+Rs 0" on
      // every unpriced exit; a real 0 still shows, an absent one says nothing.
      var known = exit.pnl !== null && exit.pnl !== undefined && isFinite(Number(exit.pnl));
      var pnl = Number(exit.pnl);
      labels.push({ kind: 'marker', x: x, y: y - 2, text: 'SELL ' + _pfChartPrice(exit.price)
        + (known ? '  ' + (pnl >= 0 ? '+' : '−') + _pfChartInr(Math.abs(pnl)) : '  unpriced'), color: PAL.sellMark });
      count++;
    });

    // THE RULE THAT ENDED THE TRADE, marked in TIME.
    // Phil, 2026-09-03: "I need the super trend indicator that cuts today's
    // trade to an exit.. I want that in the chart as well". CE_SL15 exits on
    // Supertrend(10, 2.7) over 3m NIFTY -- it turned down at 10:36 and the
    // engine acted at the next 5m boundary, 10:45.
    //
    // A VERTICAL LINE, NOT A PRICE LINE. This chart's candles are the OPTION's
    // premium and the supertrend is an INDEX level: on an axis running 250-300
    // a value of 23,996 is off-screen at best, and read as a premium at worst.
    // The two series share a time axis and nothing else, so time is what is
    // drawn -- which is also the only part that matters: WHEN it turned.
    var st = d.supertrend;
    ((st && Array.isArray(st.flips)) ? st.flips : []).forEach(function (flip) {
      if (!flip || flip.t == null) return;
      var x = p.xOf(flip.t);
      if (!isFinite(x) || x < p.padL || x > p.padL + p.plotW) return;
      var down = String(flip.dir) === 'down';
      ctx.save();
      ctx.strokeStyle = down ? PAL.sellMark : PAL.buyMark;
      ctx.globalAlpha = 0.55;
      ctx.lineWidth = 1;
      ctx.setLineDash([4, 4]);
      ctx.beginPath();
      ctx.moveTo(x, p.padT);
      ctx.lineTo(x, p.padT + p.plotH);
      ctx.stroke();
      ctx.restore();
      labels.push({
        kind: 'marker',
        x: x,
        y: p.padT + (down ? 12 : 26),
        text: 'ST ' + (down ? 'DOWN' : 'UP') + ' ' + _pfChartIst(flip.t).slice(6),
        color: down ? PAL.sellMark : PAL.buyMark,
      });
    });
  });
  return count;
}

function _pfChartCanvasLabels(c, p, labels) {
  var ctx = c.ctx, slots = [], count = 0;
  labels.forEach(function (label) {
    if (label.kind === 'gutter') {
      var y = label.y;
      // Do not "clean this up" into an exact 10px shift. The 0.5px overshoot
      // is the Classic ETH-freeze fix: exact 10 can remain 9.9999999998 apart
      // in floating point and loop forever in the next collision check.
      for (var pass = 0, moved = true; moved && pass <= slots.length; pass++) {
        moved = false;
        for (var k = 0; k < slots.length; k++) {
          if (Math.abs(slots[k] - y) < 10) { y = slots[k] + 10.5; moved = true; break; }
        }
      }
      slots.push(y);
      // Gutter labels are right-aligned against the price lane, so a long one
      // ("TARGET (not reached) (24,412.7)") runs off the left edge and loses its
      // first word. When it cannot fit the gutter, start it just inside the plot
      // instead: overlapping a candle is recoverable, a cut-off label is not.
      ctx.font = '400 ' + (10 * (p.fontScale || 1)) + 'px monospace';
      var fits = ctx.measureText(String(label.text)).width <= p.padL - 8;
      _pfChartCanvasText(ctx, label.text, fits ? p.padL - 6 : p.padL + 6, y + 3,
        label.color, 10, fits ? 'right' : 'left', null, p.fontScale);
    } else if (label.kind === 'right') {
      _pfChartCanvasText(ctx, label.text, label.x, label.y, label.color, 9.5, 'right', null, p.fontScale);
    } else {
      _pfChartCanvasText(ctx, label.text, label.x, label.y, label.color, 9.5, 'center', null, p.fontScale);
    }
    count++;
  });
  return count;
}

/* ── Indicator layers ────────────────────────────────────────────────────
 *
 * Added 2026-08-24 for Gap Carry, whose rule IS an EMA and an RSI: a chart of
 * that rule without them shows the night's candle and no reason for it.
 *
 * Both layers are driven by `payload.indicators`:
 *   { ema: [{t, v}], ema_period: 20,
 *     rsi: [{t, v}], rsi_period: 14, rsi_upper: 70, rsi_lower: 30 }
 * `t` is epoch SECONDS like every other timestamp here. A payload without
 * `indicators` draws nothing and reserves no space, which is why the four
 * charts that predate this are unaffected.
 */

// The Supertrend strategy's own line. Each point says its direction (`d`: 1
// up, -1 down); a payload from before that was added is read off the candle,
// since an up-trend line sits under the close and a down-trend line over it.
function _pfChartCanvasSupertrend(c, p, labels, ind) {
  var d = c.data || {}, ctx = c.ctx, mids = _pfChartBodyMids(d), closes = {};
  (d.candles || []).forEach(function (bar) { closes[bar.t] = Number(bar.c); });
  var runs = [], run = null, drawn = 0;
  ind.supertrend.forEach(function (point) {
    if (!point || point.v === null || point.v === undefined || !isFinite(Number(point.v))) return;
    var dir = point.d != null ? (Number(point.d) > 0 ? 1 : -1)
      : (closes[point.t] != null ? (Number(point.v) <= closes[point.t] ? 1 : -1) : (run ? run.dir : 1));
    if (!run || run.dir !== dir) { run = { dir: dir, pts: [] }; runs.push(run); }
    run.pts.push({ t: point.t, price: Number(point.v) });
    drawn++;
  });
  _pfChartCanvasClip(ctx, p, function () {
    runs.forEach(function (r) {
      if (r.pts.length >= 2) _pfChartShadeToBody(ctx, p, r.pts, r.dir > 0 ? PF_ST_UP : PF_ST_DOWN, mids);
    });
  });
  var lastRun = runs[runs.length - 1];
  if (lastRun && lastRun.pts.length) {
    var tip = lastRun.pts[lastRun.pts.length - 1];
    labels.push({
      y: p.yOf(tip.price), color: lastRun.dir > 0 ? PF_ST_UP : PF_ST_DOWN, align: 'right',
      text: 'ST ' + (ind.atr_period || 10) + ',' + (ind.multiplier != null ? ind.multiplier : '') + ' ' + Math.round(tip.price)
    });
  }
  return drawn;
}

// An EMA is a CURVE, so it cannot be expressed as a line/trendline: `lines`
// are horizontal and a trendline has exactly two anchors. Hence a real path.
function _pfChartCanvasIndicators(c, p, PAL, labels) {
  var ind = (c.data || {}).indicators;
  // EMA or SUPERTREND -- both are curves and draw identically. Supertrend has
  // sent its series as `indicators.supertrend` since the console was built,
  // and this only ever looked at `ema`, so the one strategy NAMED after an
  // indicator was the one chart that never showed it (Phil, 2026-09-01).
  var series = (ind && (ind.ema || ind.supertrend)) || null;
  if (!series || !series.length) return 0;
  if (!ind.ema && ind.supertrend) return _pfChartCanvasSupertrend(c, p, labels, ind);
  var ctx = c.ctx, drawn = 0, last = null;
  _pfChartCanvasClip(ctx, p, function () {
    ctx.strokeStyle = PAL.ema;
    ctx.lineWidth = 1.6;
    ctx.setLineDash([]);          // Phil's standard: no dashed indicator lines.
    ctx.lineJoin = 'round';
    ctx.beginPath();
    var started = false;
    for (var i = 0; i < series.length; i++) {
      var point = series[i];
      if (!point || point.v === null || point.v === undefined || !isFinite(Number(point.v))) continue;
      var x = p.xOf(point.t), y = p.yOf(point.v);
      if (!isFinite(x) || !isFinite(y)) continue;
      if (started) ctx.lineTo(x, y); else { ctx.moveTo(x, y); started = true; }
      drawn++;
      last = { x: x, y: y, v: point.v };
    }
    if (started) ctx.stroke();
  });
  if (last) {
    labels.push({
      y: last.y, text: 'EMA' + (ind.ema_period || 20) + ' ' + Math.round(Number(last.v)),
      color: PAL.ema, align: 'right'
    });
  }
  return drawn;
}

// Its own pane under the price, on a FIXED 0-100 axis with the rule's two
// thresholds drawn: the whole reason to look at it is where 70 and 30 sit.
function _pfChartCanvasRsiDivider(ctx, p, PAL, hot) {
  var y = Math.round((p.padT + p.plotH + p.rsiTop) / 2) + 0.5;
  var mid = p.padL + p.plotW / 2;
  ctx.save();
  ctx.strokeStyle = hot ? '#fbbf24' : PAL.rsiGuide;
  ctx.globalAlpha = hot ? 0.95 : 0.55;
  ctx.lineWidth = hot ? 2 : 1;
  ctx.beginPath(); ctx.moveTo(p.padL, y); ctx.lineTo(p.padL + p.plotW, y); ctx.stroke();
  // The grip: a short pill with three dots, TradingView's pane handle.
  ctx.globalAlpha = hot ? 1 : 0.8;
  ctx.fillStyle = hot ? '#fbbf24' : PAL.rsiGuide;
  var w = 34, h = 8;
  ctx.beginPath();
  if (ctx.roundRect) ctx.roundRect(mid - w / 2, y - h / 2, w, h, 4); else ctx.rect(mid - w / 2, y - h / 2, w, h);
  ctx.fill();
  ctx.fillStyle = PAL.fillRing || '#0b1220';
  [-8, 0, 8].forEach(function (dx) { ctx.beginPath(); ctx.arc(mid + dx, y, 1.4, 0, Math.PI * 2); ctx.fill(); });
  ctx.restore();
}

function _pfChartCanvasRsiPane(c, p, PAL) {
  var ind = (c.data || {}).indicators;
  var series = ind && ind.rsi;
  if (!series || !series.length || !p.rsiH) return 0;
  var ctx = c.ctx, scale = p.fontScale;
  // The handle between the panes, drawn so it can be found (Phil,
  // 2026-10-28: "The RSI drag is still not working"): a line across the gap
  // and a grip at its middle. It lights up under the pointer (the overlay).
  _pfChartCanvasRsiDivider(ctx, p, PAL, false);
  var upper = Number(ind.rsi_upper);
  var lower = Number(ind.rsi_lower);
  if (!isFinite(upper)) upper = 70;
  if (!isFinite(lower)) lower = 30;

  // The band between the two thresholds is the "nothing happens" zone.
  // Clipped to the pane: a stretched axis can push it past the edges.
  ctx.save();
  ctx.beginPath();
  ctx.rect(p.padL, p.rsiTop, p.plotW, p.rsiH);
  ctx.clip();
  ctx.fillStyle = PAL.rsiBand;
  ctx.fillRect(p.padL, p.rsiYOf(upper), p.plotW, Math.max(1, p.rsiYOf(lower) - p.rsiYOf(upper)));
  ctx.restore();

  // As TradingView draws it: the 70 and 30 bands dashed, the middle 50 dotted.
  ctx.strokeStyle = PAL.rsiGuide;
  ctx.lineWidth = 1;
  [[upper, [4, 4]], [50, [1, 3]], [lower, [4, 4]]].forEach(function (band) {
    var y = p.rsiYOf(band[0]);
    if (y < p.rsiTop - 0.5 || y > p.rsiTop + p.rsiH + 0.5) return;
    ctx.setLineDash(band[1]);
    ctx.beginPath();
    ctx.moveTo(p.padL, y);
    ctx.lineTo(p.padL + p.plotW, y);
    ctx.stroke();
    _pfChartCanvasText(ctx, String(Math.round(band[0])), p.padL - 6, y, PAL.axis, 9, 'right', '400', scale);
  });
  ctx.setLineDash([]);

  ctx.save();
  ctx.beginPath();
  ctx.rect(p.padL, p.rsiTop, p.plotW, p.rsiH);
  ctx.clip();
  ctx.strokeStyle = PAL.rsi;
  ctx.lineWidth = 1.6;
  ctx.lineJoin = 'round';
  ctx.beginPath();
  var started = false, drawn = 0;
  for (var i = 0; i < series.length; i++) {
    var point = series[i];
    if (!point || point.v === null || point.v === undefined || !isFinite(Number(point.v))) continue;
    var x = p.xOf(point.t), y = p.rsiYOf(point.v);
    if (!isFinite(x) || !isFinite(y)) continue;
    if (started) ctx.lineTo(x, y); else { ctx.moveTo(x, y); started = true; }
    drawn++;
  }
  if (started) ctx.stroke();
  ctx.restore();

  // The latest value, beside the name and as a tag on the right axis.
  var last = null;
  for (var j = series.length - 1; j >= 0; j--) {
    if (series[j] && isFinite(Number(series[j].v)) && series[j].v !== null) { last = Number(series[j].v); break; }
  }
  var name = 'RSI ' + (ind.rsi_period || 14) + (last !== null ? '  ' + last.toFixed(2) : '');
  _pfChartCanvasText(ctx, name, p.padL + 6, p.rsiTop + 8 * scale, PAL.rsi, 9, 'left', '700', scale);
  if (last !== null) {
    var ty = Math.max(p.rsiTop + 7, Math.min(p.rsiTop + p.rsiH - 7, p.rsiYOf(last))), tw = 38;
    ctx.fillStyle = PAL.rsi;
    ctx.fillRect(p.padL + p.plotW + 2, ty - 7, tw, 14);
    _pfChartCanvasText(ctx, last.toFixed(2), p.padL + p.plotW + 2 + tw / 2, ty + 0.5, '#ffffff', 9, 'center', '700', scale);
  }
  return drawn;
}

// TradingView's last-price tag: the latest close on the price axis in the
// bar's colour, with the time left until that bar closes underneath (Phil,
// 2026-10-15: "add the countdown to bar close"). Opt-in by payload.countdown.
// The countdown shows only while the session is open AND the chart holds a
// bar from today -- on a holiday or after the close the tag carries the price
// alone, never a clock counting a bar that is not forming.
function _pfChartCanvasCountdownText(seconds) {
  var s = Math.max(0, Math.floor(seconds)), h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), r = s % 60;
  var pad = function (n) { return (n < 10 ? '0' : '') + n; };
  return (h ? h + ':' + pad(m) : pad(m)) + ':' + pad(r);
}

function _pfChartCanvasLastPriceTag(c, p, PAL) {
  var d = c.data || {}, candles = d.candles || [];
  if (!d.countdown || !candles.length) return { price: null, countdown: '' };
  var bar = candles[candles.length - 1], price = Number(bar.c);
  if (!isFinite(price)) return { price: null, countdown: '' };
  var now = Date.now() / 1000;
  var tfSec = _pfChartCanvasTimeframeSeconds(d), win = _pfChartBarWindow(now, tfSec, d.session);
  // A bar off the 09:15 grid (a broker's own hourly bars, say) would make the
  // grid's clock wrong for it, so it gets no countdown.
  // On crypto's 24/7 grid the forming bar IS the window's, or it is not live.
  var onGrid = win && (Number(bar.t) - win.open) % tfSec === 0;
  var countdown = win && onGrid && Number(bar.t) === win.start ? _pfChartCanvasCountdownText(win.close - now) : '';
  var ctx = c.ctx, scale = p.fontScale || 1;
  var color = price >= Number(bar.o) ? PAL.up : PAL.down;
  var priceText = _pfChartPrice(price, true);
  ctx.save();
  ctx.font = '700 ' + (9.5 * scale) + 'px monospace';
  var w = Math.max(ctx.measureText(priceText).width, countdown ? ctx.measureText(countdown).width : 0) + 10;
  var h = countdown ? 28 * scale : 15 * scale;
  var x = p.padL + p.plotW + 1;
  // Pinned to the plot's edge when the price is scrolled out of view.
  var y = Math.max(p.padT, Math.min(p.padT + p.plotH - h, p.yOf(price) - 7.5 * scale));
  ctx.fillStyle = color;
  ctx.fillRect(x, y, w, h);
  _pfChartCanvasText(ctx, priceText, x + 5, y + 7.5 * scale, '#ffffff', 9.5, 'left', '700', scale);
  if (countdown) _pfChartCanvasText(ctx, countdown, x + 5, y + 20.5 * scale, 'rgba(255,255,255,0.85)', 9.5, 'left', '400', scale);
  ctx.restore();
  return { price: price, countdown: countdown };
}

function _pfChartCanvasDraw(cc) {
  var c = cc || _pfChartCanvas;
  if (!c || !c.ctx) return;
  var d = c.data || {}, candles = d.candles || [];
  var ctx = c.ctx, PAL = _pfChartPalette();
  ctx.clearRect(0, 0, c.w, c.h);
  if (!candles.length) return;
  if (!c.viewport) c.viewport = _pfChartCanvasFit(c, true);
  var p = _pfChartCanvasProjection(c);
  if (!p) return;
  var labels = [];
  // Keep these independently callable layers in this exact order. Later live
  // refreshes can redraw only changed layers without changing the model.
  var axisLabelCount = _pfChartCanvasGridAxes(c, p, PAL);
  _pfChartCanvasMotherColumn(c, p, PAL);
  var gapCount = _pfChartCanvasGapCandles(c, p, PAL);
  var candleCount = _pfChartCanvasCandles(c, p, PAL);
  var trendlineCount = _pfChartCanvasTrendlines(c, p, PAL, labels);
  var fibCount = _pfChartCanvasFibs(c, p, PAL, labels);
  var overlayCount = _pfChartCanvasOverlays(c, p, PAL, labels);
  var drawingCount = _pfChartCanvasDrawings(c, p, PAL);
  var emaCount = _pfChartCanvasIndicators(c, p, PAL, labels);
  var rsiCount = _pfChartCanvasRsiPane(c, p, PAL);
  var markerCount = _pfChartCanvasMarkers(c, p, PAL, labels);
  var labelCount = _pfChartCanvasLabels(c, p, labels);
  var tag = _pfChartCanvasLastPriceTag(c, p, PAL);
  // E2E reads this small semantic paint record in addition to real pixels. It
  // is a test seam, not payload state, and catches a canvas that draws a frame
  // but silently loses candles/labels/geometry.
  c.paint = {
    candles: candleCount, trendlines: trendlineCount, fibs: fibCount, markers: markerCount,
    overlays: overlayCount, drawings: drawingCount, ema: emaCount, rsi: rsiCount,
    rsiPane: p.rsiH ? { top: p.rsiTop, h: p.rsiH, lo: p.rsiLo, hi: p.rsiHi } : null,
    gaps: gapCount, labels: axisLabelCount + labelCount,
    labelTexts: labels.map(function (label) { return label.text; }),
    lastPrice: tag.price, countdown: tag.countdown,
    theme: document.documentElement.getAttribute('data-theme') || 'auto'
  };
  c.paintKey = _pfChartCanvasPaintKey(d);
  // Opt-in, like onDrawingsChange: a page that lays HTML over the chart
  // (the Scalp desk's trade lines) re-places it after every pan, zoom and
  // tick. A fault in it must never stop the chart drawing.
  if (typeof d.onDraw === 'function') {
    try { d.onDraw(p, c); } catch (err) { if (window.console) console.warn('chart onDraw', err); }
  }
}

function _pfChartCanvasPoint(c, event) {
  var p = c && c.projection;
  if (!p) return null;
  var box = c.host.getBoundingClientRect();
  if (!box.width || !box.height) return null;
  var x = event.clientX - box.left, y = event.clientY - box.top;
  return {
    x: x, y: y,
    plot: x >= p.padL && x <= p.padL + p.plotW && y >= p.padT && y <= p.padT + p.plotH,
    priceAxis: x > p.padL + p.plotW && y >= p.padT && y <= p.padT + p.plotH,
    // The RSI pane's axis: its scale on the right (where its value tag sits)
    // and its 70/50/30 labels on the left.
    rsiAxis: !!p.rsiH && (x > p.padL + p.plotW || x < p.padL) && y >= p.rsiTop && y <= p.rsiTop + p.rsiH,
    rsiPlot: !!p.rsiH && x >= p.padL && x <= p.padL + p.plotW && y >= p.rsiTop && y <= p.rsiTop + p.rsiH,
    // The gap between the price pane and RSI: its height's handle.
    rsiDivider: !!p.rsiH && x >= p.padL && x <= p.padL + p.plotW && y >= p.padT + p.plotH - 6 && y <= p.rsiTop + 6,
    timeAxis: y > p.padT + p.plotH && x >= p.padL && x <= p.padL + p.plotW
      && !(p.rsiH && y >= p.rsiTop && y <= p.rsiTop + p.rsiH)
  };
}

function _pfChartCanvasSetViewport(c, next) {
  if (!c || !next) return;
  var v = c.viewport || {};
  var tSpan = Number(next.tMax) - Number(next.tMin);
  var pSpan = Number(next.pMax) - Number(next.pMin);
  if (!isFinite(tSpan) || !isFinite(pSpan) || tSpan <= 0 || pSpan <= 0) return;
  c.viewport = { tMin: Number(next.tMin), tMax: Number(next.tMax), pMin: Number(next.pMin), pMax: Number(next.pMax) };
  // Every readout on the page shows the SAME number: the Test Bench toolbar's
  // (by id), the host cluster's, and any chart-strip's. Updating only the id
  // left the dialog's own readout frozen at 100% while the chart zoomed.
  var fit = _pfChartCanvasFit(c);
  // The readouts speak for the focused chart; one panned beside it is its own.
  if (fit && (!c.multi || c === _pfChartFocused)) {
    var pct = Math.round((fit.tMax - fit.tMin) / tSpan * 100) + '%';
    var legacy = document.getElementById('pf-bench-zoom-level');
    if (legacy) legacy.textContent = pct;
    document.querySelectorAll('[data-pf-zoom-readout], [data-bench-zoom-level]')
      .forEach(function (node) { node.textContent = pct; });
  }
  _pfChartCanvasDraw(c);
  // The page may keep what the viewer zoomed to (payload.onViewport).
  if (c.data && typeof c.data.onViewport === 'function') {
    try { c.data.onViewport(c.viewport); } catch (err) {}
  }
}

function _pfChartCanvasZoom(factor, reset, anchor) {
  var c = _pfChartCanvas;
  if (!c || !c.viewport) return;
  if (reset || factor === 0) {
    _pfChartCanvasSetViewport(c, _pfChartCanvasFit(c));
    return;
  }
  var step = Number(factor);
  if (!isFinite(step) || step <= 0) return;
  var p = c.projection || _pfChartCanvasProjection(c);
  if (!p) return;
  var oldSpan = c.viewport.tMax - c.viewport.tMin;
  var newSpan = Math.max(0.5, Math.min(oldSpan * 60, oldSpan * step));
  var x = anchor && isFinite(anchor.x) ? anchor.x : p.padL + p.plotW / 2;
  var at = p.tAt(x), ratio = (at - c.viewport.tMin) / oldSpan;
  _pfChartCanvasSetViewport(c, {
    tMin: at - ratio * newSpan,
    tMax: at + (1 - ratio) * newSpan,
    pMin: c.viewport.pMin, pMax: c.viewport.pMax
  });
}

// TradingView's magnet, while a drawing tool waits for a point (Phil,
// 2026-10-16: "It is not sticking to the candle like we use magnet in TV"):
// the price is the candle's open, high, low or close nearest the pointer.
function _pfChartCanvasMagnet(c, idx, y) {
  var p = c.projection, bars = ((c.data || {}).candles || []);
  if (!p || idx < 0 || idx >= bars.length) return null;
  var bar = bars[idx], best = null;
  [bar.o, bar.h, bar.l, bar.c].forEach(function (v) {
    v = Number(v);
    if (!isFinite(v)) return;
    var dist = Math.abs(p.yOf(v) - y);
    if (best === null || dist < best.dist) best = { price: v, y: p.yOf(v), dist: dist };
  });
  return best;
}

function _pfChartCanvasDrawCrosshair(c, point) {
  var ctx = c.octx, p = c.projection;
  if (!ctx || !p) return;
  ctx.clearRect(0, 0, c.w, c.h);
  // On the handle between the panes: it lights up, and no crosshair.
  if (point && point.rsiDivider && !c.drag) {
    _pfChartCanvasRsiDivider(ctx, p, _pfChartPalette(), true);
    c.crosshair = null;
    if (typeof (c.data || {}).onCrosshair === 'function') { try { c.data.onCrosshair(null); } catch (err) {} }
    return;
  }
  if (!point || !(point.plot || point.rsiPlot)) {
    if (typeof (c.data || {}).onCrosshair === 'function') { try { c.data.onCrosshair(null); } catch (err) {} }
    return;
  }
  var PAL = _pfChartPalette(), price = p.pAt(point.y);
  // Over the RSI pane the line across reads the RSI, not a price (Phil,
  // 2026-10-20: "Make the cross-hair flow till RSI").
  var inRsi = !point.plot && point.rsiPlot;
  // THE VERTICAL LINE SITS ON A CANDLE, never between two (Phil, 2026-10-14:
  // "the cross hair has to jump ... on the minute and not all the places").
  // As on TradingView it steps from bar to bar as the pointer moves, and the
  // time beneath it is that candle's own time.
  // Past the last candle it carries on into the empty space at the bar's
  // pace (Phil, 2026-10-28: "make it move without any end"); the time axis
  // already extrapolates there.
  var bars = ((c.data || {}).candles || []).length;
  var idx = Math.round(p.tAt(point.x));
  var barTime = p.timeAt(idx), lineX = p.xOf(barTime);
  if (!isFinite(lineX)) lineX = point.x;
  // The magnet: always while a drawing point is placed, and whenever the
  // chart has it switched on (payload.magnet) -- the price read is then the
  // candle's own open, high, low or close.
  if (!inRsi && (typeof window.pfChartPickHandler === 'function' || _pfChartDrawSnapping(c) || !!(c.data && c.data.magnet))) {
    var snap = _pfChartCanvasMagnet(c, idx, point.y);
    if (snap) { price = snap.price; point = { x: point.x, y: snap.y, plot: point.plot }; }
  }
  ctx.save();
  ctx.strokeStyle = PAL.axis; ctx.lineWidth = 0.7; ctx.globalAlpha = 0.8; ctx.setLineDash([4, 3]);
  ctx.beginPath(); ctx.moveTo(p.padL, point.y); ctx.lineTo(p.padL + p.plotW, point.y); ctx.stroke();
  // The upright runs down through the RSI pane too, as on TradingView.
  var lineBottom = p.rsiH ? p.rsiTop + p.rsiH : p.padT + p.plotH;
  ctx.beginPath(); ctx.moveTo(lineX, p.padT); ctx.lineTo(lineX, lineBottom); ctx.stroke();
  ctx.setLineDash([]); ctx.globalAlpha = 1;
  var priceText = inRsi ? Number(p.rsiAt(point.y)).toFixed(2)
    : _pfChartPrice(price);
  ctx.font = '9.5px monospace';
  var priceW = Math.max(ctx.measureText(priceText).width + 10, 42), timeText = _pfChartIst(Math.round(barTime)) + ' IST';
  var timeW = Math.max(ctx.measureText(timeText).width + 10, 84);
  ctx.fillStyle = PAL.axis;
  ctx.fillRect(p.padL + p.plotW + 2, point.y - 8, priceW, 16);
  ctx.fillRect(lineX - timeW / 2, c.h - 20, timeW, 15);
  _pfChartCanvasText(ctx, priceText, p.padL + p.plotW + 6, point.y + 0.5, PAL.fillRing, 9.5, 'left');
  _pfChartCanvasText(ctx, timeText, lineX, c.h - 12, PAL.fillRing, 9, 'center');
  ctx.restore();
  // A test seam, like c.paint: where the crosshair was last drawn.
  c.crosshair = { x: lineX, y: point.y, top: p.padT, bottom: lineBottom, label: priceText, pane: inRsi ? 'rsi' : 'price', idx: idx };
  // Opt-in, like onDraw: the candle under the line (null past the candles),
  // for a page that shows its open, high, low and close.
  if (typeof (c.data || {}).onCrosshair === 'function') {
    try { c.data.onCrosshair(idx >= 0 && idx < bars ? idx : null); } catch (err) {}
  }
  // The drawing being made follows the pointer, here on the overlay.
  if (c.drawPending) _pfChartCanvasPaintDrawing(ctx, c, p, c.drawPending, true, PAL);
}

function _pfChartCanvasClearCrosshair(c) {
  if (c) c.crosshair = null;
  if (c && c.data && typeof c.data.onCrosshair === 'function') {
    try { c.data.onCrosshair(null); } catch (err) {}
  }
  if (c && c.octx) c.octx.clearRect(0, 0, c.w, c.h);
}

// ── Motion ───────────────────────────────────────────────────
// Phil, 2026-10-15: "why the chart painting is not smooth like Trading view?"
// A drag drew once per pointer event (a fast mouse sends several a frame), a
// wheel notch jumped 8% at once whatever the device -- a trackpad sends dozens
// of tiny wheel events, so it lurched -- and a released drag stopped dead.
// Now a drag paints once per frame, the wheel steers a target the view eases
// toward, and a flicked pan coasts to a stop.
function _pfChartCanvasStopMotion(c) {
  if (!c) return;
  if (c.moveFrame) { cancelAnimationFrame(c.moveFrame); c.moveFrame = 0; }
  if (c.glideFrame) { cancelAnimationFrame(c.glideFrame); c.glideFrame = 0; }
  if (c.coastFrame) { cancelAnimationFrame(c.coastFrame); c.coastFrame = 0; }
  c.pendingView = null;
  c.glideTarget = null;
}

// Paint a dragged viewport at the next frame; later moves in the same frame
// replace it.
function _pfChartCanvasQueueView(c, next) {
  c.pendingView = next;
  if (c.moveFrame) return;
  c.moveFrame = requestAnimationFrame(_pfChartAs(c, function () {
    c.moveFrame = 0;
    var view = c.pendingView;
    c.pendingView = null;
    if (view) _pfChartCanvasSetViewport(c, view);
  }));
}

function _pfChartCanvasFlushView(c) {
  if (c.moveFrame) { cancelAnimationFrame(c.moveFrame); c.moveFrame = 0; }
  var view = c.pendingView;
  c.pendingView = null;
  if (view) _pfChartCanvasSetViewport(c, view);
}

function _pfChartCanvasMaxSpan(c) {
  var bars = ((c.data || {}).candles || []).length;
  return Math.max(bars * 2 + 20, 10);
}

// Ease the time window toward c.glideTarget, about a third of the way each
// frame, so a wheel notch glides and a burst of trackpad events blends.
function _pfChartCanvasGlide(c) {
  if (c.glideFrame) return;
  var last = performance.now();
  var step = _pfChartAs(c, function (now) {
    c.glideFrame = 0;
    var target = c.glideTarget, v = c.viewport;
    if (!target || !v) { c.glideTarget = null; return; }
    var k = 1 - Math.pow(0.65, Math.max(now - last, 1) / 16.7);
    last = now;
    var tMin = v.tMin + (target.tMin - v.tMin) * k, tMax = v.tMax + (target.tMax - v.tMax) * k;
    var done = Math.abs(target.tMin - tMin) < 0.01 && Math.abs(target.tMax - tMax) < 0.01;
    if (done) { tMin = target.tMin; tMax = target.tMax; c.glideTarget = null; }
    _pfChartCanvasSetViewport(c, { tMin: tMin, tMax: tMax, pMin: v.pMin, pMax: v.pMax });
    if (!done) c.glideFrame = requestAnimationFrame(step);
  });
  c.glideFrame = requestAnimationFrame(step);
}

// After a flicked pan the chart keeps moving in time and slows to a stop.
function _pfChartCanvasCoast(c, pxPerMs) {
  var p = c.projection;
  if (!p || Math.abs(pxPerMs) < 0.25) return;
  var velocity = Math.max(-6, Math.min(6, pxPerMs)), last = performance.now();
  var step = _pfChartAs(c, function (now) {
    c.coastFrame = 0;
    var v = c.viewport, proj = c.projection;
    if (!v || !proj) return;
    var dt = Math.max(now - last, 1);
    last = now;
    var shift = -(velocity * dt) / proj.plotW * (v.tMax - v.tMin);
    _pfChartCanvasSetViewport(c, { tMin: v.tMin + shift, tMax: v.tMax + shift, pMin: v.pMin, pMax: v.pMax });
    velocity *= Math.pow(0.92, dt / 16.7);
    if (Math.abs(velocity) > 0.02) c.coastFrame = requestAnimationFrame(step);
  });
  c.coastFrame = requestAnimationFrame(step);
}

// Phase 3 interaction model. Plot drag pans both axes; the axis gutters are
// deliberately separate so price stretching can never move time and vice versa.
// ── Hand-drawn trendlines and Fibs, TradingView's way ────────────────────
// Phil, 2026-10-16: "I want same like the trading view". The first click
// starts the drawing and it follows the pointer; a second click -- or letting
// go after a drag -- finishes it. A finished drawing is taken by an end (to
// move that end) or by its body (to move it whole), and Delete removes the
// one selected. The page owns the list: payload.drawings is
// [{id, kind: 'line'|'fib', a: {t, price}, b: {t, price}, levels, colors}]
// and payload.onDrawingsChange(list) keeps it. Arming a tool changes nothing
// on the chart: the view stays exactly where it was.
var _pfDrawArmed = null; // {kind, levels, colors, extra, onEnd}
var _PF_DRAW_LINE = '#2962ff';

function pfChartArmDrawing(kind, opts) {
  opts = opts || {};
  var c = _pfChartCanvas;
  if (c) { c.drawPending = null; c.drawSel = null; }
  _pfDrawArmed = {
    kind: kind, levels: opts.levels || null, colors: opts.colors || null,
    extra: opts.extra || null, onEnd: opts.onEnd || null
  };
  if (c) _pfChartCanvasDraw(c);
}

function pfChartDisarmDrawing() {
  var armed = _pfDrawArmed, c = _pfChartCanvas;
  _pfDrawArmed = null;
  if (c && c.drawPending) { c.drawPending = null; _pfChartCanvasClearCrosshair(c); }
  if (armed && typeof armed.onEnd === 'function') armed.onEnd();
}

function pfChartDrawingArmed() {
  return _pfDrawArmed ? _pfDrawArmed.kind : '';
}

// While a point is being placed or an end dragged, the crosshair sticks to
// the candle as TradingView's magnet does.
function _pfChartDrawSnapping(c) {
  return !!(_pfDrawArmed || (c && c.drawPending) || (c && c.drag && c.drag.kind === 'draw' && c.drag.part !== 'body'));
}

function _pfChartDrawSnap(c, x, y) {
  var p = c.projection;
  var idx = Math.round(p.tAt(x));
  var magnet = _pfChartCanvasMagnet(c, idx, y);
  return { t: Math.round(p.timeAt(idx)), price: magnet ? magnet.price : Math.round(p.pAt(y) * 100) / 100 };
}

function _pfChartDrawGeom(p, dr) {
  return { xa: p.xOf(dr.a.t), ya: p.yOf(dr.a.price), xb: p.xOf(dr.b.t), yb: p.yOf(dr.b.price) };
}

// A trendline marked `extend: 'right'` carries on past its later point to the
// plot's right edge, TradingView's "Extend right" (Phil, 2026-10-16: "Trendline
// has to extend right ... has to be the setting"). Returns the far end drawn.
function _pfChartLineEnd(p, dr, g) {
  if (dr.kind !== 'line' || dr.extend !== 'right' || Math.abs(g.xb - g.xa) < 0.5) return null;
  var from = g.xa < g.xb ? { x: g.xa, y: g.ya } : { x: g.xb, y: g.yb };
  var to = g.xa < g.xb ? { x: g.xb, y: g.yb } : { x: g.xa, y: g.ya };
  var right = p.padL + p.plotW;
  if (to.x >= right) return null;
  return { x: right, y: from.y + (to.y - from.y) * (right - from.x) / (to.x - from.x), fromX: to.x, fromY: to.y };
}

function _pfChartFibLevels(dr) {
  var a = Number(dr.a.price), span = Number(dr.b.price) - a;
  return (dr.levels || [0, 1]).map(function (k) { return { k: k, price: a + span * k }; });
}

function _pfChartCanvasPaintDrawing(ctx, c, p, dr, selected, PAL) {
  if (!dr || !dr.a || !dr.b) return;
  var g = _pfChartDrawGeom(p, dr);
  ctx.save();
  ctx.beginPath();
  ctx.rect(p.padL, p.padT, p.plotW, p.plotH);
  ctx.clip();
  if (dr.kind === 'fib') {
    // Each level from the earlier pick to the right edge, labelled at its
    // left end; the two picks joined by a dashed diagonal.
    var left = Math.min(g.xa, g.xb), right = p.padL + p.plotW;
    ctx.font = '10px monospace';
    ctx.textBaseline = 'bottom';
    ctx.textAlign = 'left';
    _pfChartFibLevels(dr).forEach(function (lv) {
      var y = p.yOf(lv.price), color = (dr.colors && dr.colors[lv.k]) || '#94a3b8';
      ctx.strokeStyle = color; ctx.lineWidth = 1; ctx.globalAlpha = 0.95;
      ctx.beginPath(); ctx.moveTo(left, y); ctx.lineTo(right, y); ctx.stroke();
      ctx.fillStyle = color;
      ctx.fillText(lv.k + ' (' + _pfChartPrice(lv.price) + ')', left + 4, y - 2);
    });
    ctx.globalAlpha = 0.7; ctx.strokeStyle = '#94a3b8'; ctx.setLineDash([4, 4]);
    ctx.beginPath(); ctx.moveTo(g.xa, g.ya); ctx.lineTo(g.xb, g.yb); ctx.stroke();
  } else {
    ctx.strokeStyle = dr.color || _PF_DRAW_LINE; ctx.lineWidth = 2;
    ctx.beginPath(); ctx.moveTo(g.xa, g.ya); ctx.lineTo(g.xb, g.yb);
    var far = _pfChartLineEnd(p, dr, g);
    if (far) { ctx.moveTo(far.fromX, far.fromY); ctx.lineTo(far.x, far.y); }
    ctx.stroke();
  }
  ctx.restore();
  if (selected) {
    ctx.save();
    [[g.xa, g.ya], [g.xb, g.yb]].forEach(function (pt) {
      ctx.beginPath(); ctx.arc(pt[0], pt[1], 4.5, 0, Math.PI * 2);
      ctx.fillStyle = PAL.markRing; ctx.fill();
      ctx.lineWidth = 1.6; ctx.strokeStyle = dr.color || _PF_DRAW_LINE; ctx.stroke();
    });
    ctx.restore();
  }
}

function _pfChartCanvasDrawings(c, p, PAL) {
  var list = ((c.data || {}).drawings) || [];
  list.forEach(function (dr) { _pfChartCanvasPaintDrawing(c.ctx, c, p, dr, !!dr && dr.id === c.drawSel, PAL); });
  return list.length;
}

function _pfChartSegDist(px, py, ax, ay, bx, by) {
  var dx = bx - ax, dy = by - ay, len = dx * dx + dy * dy;
  var t = len ? Math.max(0, Math.min(1, ((px - ax) * dx + (py - ay) * dy) / len)) : 0;
  return Math.hypot(px - (ax + t * dx), py - (ay + t * dy));
}

// The drawing under the pointer: an end within 7px, else its body within 5px.
function _pfChartDrawHit(c, x, y) {
  var p = c.projection, list = ((c.data || {}).drawings) || [];
  if (!p) return null;
  for (var i = list.length - 1; i >= 0; i--) {
    var dr = list[i];
    if (!dr || !dr.a || !dr.b) continue;
    var g = _pfChartDrawGeom(p, dr);
    if (Math.hypot(x - g.xa, y - g.ya) <= 7) return { id: dr.id, part: 'a' };
    if (Math.hypot(x - g.xb, y - g.yb) <= 7) return { id: dr.id, part: 'b' };
    if (_pfChartSegDist(x, y, g.xa, g.ya, g.xb, g.yb) <= 5) return { id: dr.id, part: 'body' };
    var far = _pfChartLineEnd(p, dr, g);
    if (far && _pfChartSegDist(x, y, far.fromX, far.fromY, far.x, far.y) <= 5) return { id: dr.id, part: 'body' };
    if (dr.kind === 'fib' && x >= Math.min(g.xa, g.xb) - 4 && x <= p.padL + p.plotW) {
      var levels = _pfChartFibLevels(dr);
      for (var k = 0; k < levels.length; k++) {
        if (Math.abs(y - p.yOf(levels[k].price)) <= 4) return { id: dr.id, part: 'body' };
      }
    }
  }
  return null;
}

function _pfChartDrawFind(c, id) {
  var list = ((c.data || {}).drawings) || [];
  for (var i = 0; i < list.length; i++) if (list[i] && list[i].id === id) return list[i];
  return null;
}

// `live`: a save made while a drag is still going -- not a finished change.
// The page keeps those for a refresh's sake but must not count each as an
// undo step (one drag filled the history with pixel steps, and Undo never got
// back to where the line started; Phil, 2026-10-21).
function _pfChartDrawCommit(c, live) {
  var d = c.data || {};
  if (typeof d.onDrawingsChange === 'function') d.onDrawingsChange((d.drawings || []).slice(), { live: !!live });
}

function _pfChartDrawFinish(c) {
  var pend = c.drawPending, armed = _pfDrawArmed, d = c.data || {}, p = c.projection;
  c.drawPending = null;
  _pfDrawArmed = null;
  if (pend && p) {
    var g = _pfChartDrawGeom(p, pend);
    // A click that never moved draws nothing.
    if (Math.hypot(g.xa - g.xb, g.ya - g.yb) >= 4) {
      var dr = { id: 'd' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6), kind: pend.kind, a: pend.a, b: pend.b };
      if (pend.levels) dr.levels = pend.levels;
      if (pend.colors) dr.colors = pend.colors;
      if (pend.extra) Object.keys(pend.extra).forEach(function (k) { dr[k] = pend.extra[k]; });
      d.drawings = (d.drawings || []).concat([dr]);
      c.drawSel = dr.id;
      _pfChartDrawCommit(c);
    }
  }
  _pfChartCanvasClearCrosshair(c);
  _pfChartCanvasDraw(c);
  if (armed && typeof armed.onEnd === 'function') armed.onEnd();
}

function _pfChartDrawShift(c, pt, bars, dp) {
  var axis = _pfChartCanvasAxisOf(c);
  return {
    t: Math.round(_pfChartCanvasTimeAt(axis, _pfChartCanvasIdxOf(axis, pt.t) + bars)),
    price: Math.round((Number(pt.price) + dp) * 100) / 100
  };
}

// pointerdown on the plot. True when a drawing took it (no pan follows).
function _pfChartDrawPointerDown(c, point, event) {
  if (!point || !c.projection) return false;
  if (c.drawPending) {
    if (point.plot) c.drawPending.b = _pfChartDrawSnap(c, point.x, point.y);
    _pfChartDrawFinish(c);
    return true;
  }
  if (!point.plot) return false;
  if (_pfDrawArmed) {
    var at = _pfChartDrawSnap(c, point.x, point.y);
    c.drawPending = {
      kind: _pfDrawArmed.kind, levels: _pfDrawArmed.levels, colors: _pfDrawArmed.colors, extra: _pfDrawArmed.extra,
      a: at, b: { t: at.t, price: at.price }, x: point.x, y: point.y, down: true, dragged: false
    };
    // The preview paints as the finished drawing will: a ray extends right
    // while it is being drawn, not only after the second click.
    if (_pfDrawArmed.extra) Object.keys(_pfDrawArmed.extra).forEach(function (k) {
      if (!(k in c.drawPending)) c.drawPending[k] = _pfDrawArmed.extra[k];
    });
    try { c.host.setPointerCapture(event.pointerId); } catch (err) {}
    _pfChartCanvasDrawCrosshair(c, point);
    return true;
  }
  var hit = _pfChartDrawHit(c, point.x, point.y), dr = hit && _pfChartDrawFind(c, hit.id);
  if (dr) {
    c.drawSel = dr.id;
    c.drag = {
      kind: 'draw', id: dr.id, part: hit.part,
      startIdx: c.projection.tAt(point.x), startPrice: c.projection.pAt(point.y),
      a: { t: dr.a.t, price: dr.a.price }, b: { t: dr.b.t, price: dr.b.price }
    };
    c.host.style.cursor = hit.part === 'body' ? 'move' : 'pointer';
    try { c.host.setPointerCapture(event.pointerId); } catch (err) {}
    _pfChartCanvasClearCrosshair(c);
    _pfChartCanvasDraw(c);
    return true;
  }
  if (c.drawSel) { c.drawSel = null; _pfChartCanvasDraw(c); }
  return false;
}

// pointermove. True when a drawing took it.
function _pfChartDrawPointerMove(c, point) {
  if (c.drawPending) {
    c.drawPending.b = _pfChartDrawSnap(c, point.x, point.y);
    if (c.drawPending.down && Math.hypot(point.x - c.drawPending.x, point.y - c.drawPending.y) > 6) c.drawPending.dragged = true;
    c.host.style.cursor = 'crosshair';
    _pfChartCanvasDrawCrosshair(c, point);
    return true;
  }
  if (c.drag && c.drag.kind === 'draw') {
    var drag = c.drag, dr = _pfChartDrawFind(c, drag.id), p = c.projection;
    if (!dr || !p) { c.drag = null; return true; }
    if (drag.part === 'a' || drag.part === 'b') dr[drag.part] = _pfChartDrawSnap(c, point.x, point.y);
    else {
      var bars = Math.round(p.tAt(point.x) - drag.startIdx), dp = p.pAt(point.y) - drag.startPrice;
      dr.a = _pfChartDrawShift(c, drag.a, bars, dp);
      dr.b = _pfChartDrawShift(c, drag.b, bars, dp);
    }
    drag.moved = true;
    _pfChartCanvasDraw(c);
    // Kept as it moves: a refresh mid-drag rebuilds the list from the page.
    _pfChartDrawCommit(c, true);
    return true;
  }
  return false;
}

// pointerup. True when a drawing took it.
function _pfChartDrawPointerUp(c, event) {
  if (c.drawPending) {
    try { c.host.releasePointerCapture(event.pointerId); } catch (err) {}
    if (c.drawPending.down && c.drawPending.dragged) _pfChartDrawFinish(c);
    else c.drawPending.down = false;
    return true;
  }
  if (c.drag && c.drag.kind === 'draw') {
    try { c.host.releasePointerCapture(event.pointerId); } catch (err) {}
    c.drag = null;
    c.host.style.cursor = '';
    _pfChartDrawCommit(c);
    return true;
  }
  return false;
}

// Esc cancels a drawing in progress (or lets go of a selection); Delete and
// Backspace remove the selected drawing. Never while typing in a field.
document.addEventListener('keydown', function (event) {
  var c = _pfChartCanvas;
  if (!c) return;
  var target = event.target || {};
  if (/^(INPUT|TEXTAREA|SELECT)$/.test(target.tagName || '') || target.isContentEditable) return;
  if (event.key === 'Escape') {
    if (c.drawPending || _pfDrawArmed) {
      event.stopImmediatePropagation();
      pfChartDisarmDrawing();
      _pfChartCanvasDraw(c);
    } else if (c.drawSel) {
      event.stopImmediatePropagation();
      c.drawSel = null;
      _pfChartCanvasDraw(c);
    }
    return;
  }
  if ((event.key === 'Delete' || event.key === 'Backspace') && c.drawSel) {
    var d = c.data || {};
    d.drawings = (d.drawings || []).filter(function (dr) { return dr && dr.id !== c.drawSel; });
    c.drawSel = null;
    event.preventDefault();
    _pfChartDrawCommit(c);
    _pfChartCanvasDraw(c);
  }
}, true);

function _pfChartCanvasBindInteraction(c) {
  if (!c || !c.host) return;
  function bind(name, fn, opts) {
    var run = _pfChartAs(c, fn);
    c.host.addEventListener(name, run, opts);
    c.handlers.push([name, run, opts]);
  }
  // Two-finger pinch. Pointer events deliver each finger separately, so
  // without this map a second touch simply restarted the one-pointer drag and
  // the chart jittered instead of zooming — Phil's phones could never zoom.
  c.touches = new Map();
  c.pinch = null;
  function pinchGeom() {
    var pts = Array.from(c.touches.values());
    return {
      mx: (pts[0].x + pts[1].x) / 2, my: (pts[0].y + pts[1].y) / 2,
      dx: Math.abs(pts[0].x - pts[1].x), dy: Math.abs(pts[0].y - pts[1].y),
      d: Math.hypot(pts[0].x - pts[1].x, pts[0].y - pts[1].y)
    };
  }
  function beginPinch() {
    var v = c.viewport, p = c.projection;
    if (!v || !p || c.touches.size < 2) return;
    var g = pinchGeom();
    c.drag = null;
    c.host.style.cursor = '';
    c.pinch = {
      start: g,
      viewport: { tMin: v.tMin, tMax: v.tMax, pMin: v.pMin, pMax: v.pMax },
      // A clearly vertical pinch stretches price; anything else zooms time.
      vertical: g.dy >= 40 && g.dy > 1.5 * g.dx,
      anchorTime: p.tAt(g.mx), anchorPrice: p.pAt(g.my)
    };
    _pfChartCanvasClearCrosshair(c);
  }
  function movePinch() {
    var pin = c.pinch, p = c.projection;
    if (!pin || !p || c.touches.size < 2) return;
    var g = pinchGeom(), s = pin.start, v0 = pin.viewport;
    var tSpan = v0.tMax - v0.tMin, pSpan = v0.pMax - v0.pMin;
    if (pin.vertical) { if (g.dy >= 12) pSpan = pSpan * s.dy / g.dy; }
    else if (g.d >= 12) tSpan = Math.max(0.5, tSpan * s.d / g.d);
    // The data point under the fingers' first midpoint stays under the moving
    // midpoint, so one gesture both zooms and pans.
    var tMin = pin.anchorTime - ((g.mx - p.padL) / p.plotW) * tSpan;
    var pMax = pin.anchorPrice + ((g.my - p.padT) / p.plotH) * pSpan;
    _pfChartCanvasQueueView(c, { tMin: tMin, tMax: tMin + tSpan, pMin: pMax - pSpan, pMax: pMax });
  }
  function endDrag(event) {
    if (event) { try { c.host.releasePointerCapture(event.pointerId); } catch (err) {} }
    if (event && event.pointerType === 'touch') {
      c.touches.delete(event.pointerId);
      _pfChartCanvasClearCrosshair(c);
      if (c.pinch) {
        if (c.touches.size >= 2) { beginPinch(); return; }
        c.pinch = null;
        if (c.touches.size === 1 && c.viewport) {
          // The finger that stayed down keeps panning without a re-touch.
          var rest = c.touches.values().next().value, v = c.viewport;
          c.drag = { kind: 'pan', x: rest.x, y: rest.y, viewport: { tMin: v.tMin, tMax: v.tMax, pMin: v.pMin, pMax: v.pMax } };
          return;
        }
      }
      // A finger is still down (its pan is live); only a fully lifted
      // gesture may fall through and clear the drag.
      if (c.touches.size) return;
    }
    if (!c.drag) return;
    _pfChartCanvasFlushView(c);
    var drag = c.drag;
    c.drag = null;
    c.host.style.cursor = '';
    // The page may keep the RSI height it was dragged to (payload.onRsiResize).
    if (drag.kind === 'rsiPane' && c.projection && typeof (c.data || {}).onRsiResize === 'function') {
      try { c.data.onRsiResize(Math.round(c.projection.rsiH)); } catch (err) {}
    }
    // Released while still moving: coast, at the speed of the drag's last
    // ~100ms. A pointer that came to rest before letting go stays put.
    var samples = drag.samples || [], b = samples[samples.length - 1];
    var recent = b ? samples.filter(function (s) { return b.at - s.at < 100; }) : [];
    if (drag.kind === 'pan' && b && performance.now() - b.at < 100 && recent.length >= 2) {
      var a = recent[0];
      if (b.at > a.at) _pfChartCanvasCoast(c, (b.x - a.x) / (b.at - a.at));
    }
  }
  // RIGHT-CLICK IS THE PAGE'S MENU, at the price under the pointer (Phil,
  // 2026-11-04: "right clicking on the chart I need to set alarm on that
  // particular price"). A chart whose page offers none keeps the browser's.
  bind('contextmenu', function (event) {
    var handler = c.data && c.data.onContextMenu;
    var point = _pfChartCanvasPoint(c, event), p = c.projection;
    if (typeof handler !== 'function' || !point || !p || !(point.plot || point.priceAxis)) return;
    event.preventDefault();
    _pfChartCanvasClearCrosshair(c);
    handler({ price: p.pAt(point.y), clientX: event.clientX, clientY: event.clientY }, c.host);
  });
  bind('pointerdown', function (event) {
    // The right button opens the menu above; it never pans or draws.
    if (event.button === 2) return;
    _pfChartCanvasStopMotion(c);
    var point = _pfChartCanvasPoint(c, event), v = c.viewport;
    // A drawing tool waiting for a point takes the click instead of a pan:
    // the bar under the pointer (snapped, as the crosshair is) and its price.
    if (point && point.plot && c.projection && typeof window.pfChartPickHandler === 'function') {
      var pickIdx = Math.round(c.projection.tAt(point.x));
      // Past the last candle (a trendline's far end) there is nothing to stick to.
      var magnet = _pfChartCanvasMagnet(c, pickIdx, point.y);
      var pickPrice = magnet ? magnet.price : c.projection.pAt(point.y);
      var picked = window.pfChartPickHandler({ t: Math.round(c.projection.timeAt(pickIdx)), price: pickPrice }, c.host);
      if (picked) { event.preventDefault(); return; }
    }
    if (event.pointerType !== 'touch' || !c.touches.size) {
      if (_pfChartDrawPointerDown(c, point, event)) { event.preventDefault(); return; }
    }
    if (event.pointerType === 'touch' && point) {
      c.touches.set(event.pointerId, { x: point.x, y: point.y });
      try { c.host.setPointerCapture(event.pointerId); } catch (err) {}
      if (c.touches.size === 2) { beginPinch(); return; }
      if (c.touches.size > 2) return;
    }
    if (point && point.rsiDivider && c.projection) {
      c.drag = { kind: 'rsiPane', y: point.y, h0: c.projection.rsiH };
      c.host.style.cursor = 'row-resize';
      try { c.host.setPointerCapture(event.pointerId); } catch (err) {}
      _pfChartCanvasClearCrosshair(c);
      return;
    }
    if (point && point.rsiAxis && c.projection) {
      var rr0 = c.rsiRange || { lo: 0, hi: 100 };
      c.drag = { kind: 'rsi', y: point.y, range: { lo: rr0.lo, hi: rr0.hi }, anchor: c.projection.rsiAt(point.y) };
      c.host.style.cursor = 'ns-resize';
      try { c.host.setPointerCapture(event.pointerId); } catch (err) {}
      _pfChartCanvasClearCrosshair(c);
      return;
    }
    if (!point || !v || (!point.plot && !point.rsiPlot && !point.priceAxis && !point.timeAxis)) return;
    var rsiFrom = c.rsiRange || { lo: 0, hi: 100 };
    c.drag = {
      // A press in the RSI pane pans time as the price pane does, and moves
      // the RSI up and down instead of the price (Phil, 2026-11-02: "I am not
      // having this hand icon to drag my RSI up and down... the top goes off").
      kind: point.plot || point.rsiPlot ? 'pan' : (point.priceAxis ? 'price' : 'time'),
      timeOnly: !point.plot && !!point.rsiPlot,
      rsi: !point.plot && point.rsiPlot ? { lo: rsiFrom.lo, hi: rsiFrom.hi } : null,
      x: point.x, y: point.y,
      viewport: { tMin: v.tMin, tMax: v.tMax, pMin: v.pMin, pMax: v.pMax },
      anchorTime: c.projection.tAt(point.x), anchorPrice: c.projection.pAt(point.y)
    };
    c.host.style.cursor = point.plot || point.rsiPlot ? 'grabbing' : 'ns-resize';
    if (point.timeAxis) c.host.style.cursor = 'ew-resize';
    try { c.host.setPointerCapture(event.pointerId); } catch (err) {}
    _pfChartCanvasClearCrosshair(c);
  });
  bind('pointermove', function (event) {
    var point = _pfChartCanvasPoint(c, event);
    if (!point) return;
    if (event.pointerType === 'touch' && c.touches.has(event.pointerId)) {
      c.touches.set(event.pointerId, { x: point.x, y: point.y });
      if (c.pinch) { movePinch(); return; }
    }
    if (_pfChartDrawPointerMove(c, point)) return;
    if (!c.drag) {
      var over = point.plot && !_pfDrawArmed ? _pfChartDrawHit(c, point.x, point.y) : null;
      c.host.style.cursor = over ? (over.part === 'body' ? 'move' : 'pointer')
        : point.rsiDivider ? 'row-resize'
        : point.rsiPlot && !point.plot ? 'grab'
        : (point.plot ? 'crosshair' : (point.priceAxis || point.rsiAxis ? 'ns-resize' : (point.timeAxis ? 'ew-resize' : '')));
      _pfChartCanvasDrawCrosshair(c, point);
      return;
    }
    if (c.drag.kind === 'rsiPane') {
      // Up makes the RSI taller, down shorter; the price pane takes the rest.
      c.rsiPaneH = Math.max(50, c.drag.h0 - (point.y - c.drag.y));
      _pfChartCanvasDraw(c);
      return;
    }
    if (c.drag.kind === 'rsi') {
      // As the price axis: pulling up tightens the range, pulling down widens
      // it, about the value first taken.
      var d0 = c.drag, span0 = d0.range.hi - d0.range.lo;
      var span1 = Math.max(5, Math.min(400, span0 * Math.exp((point.y - d0.y) / 160)));
      var ratio0 = (d0.anchor - d0.range.lo) / span0;
      c.rsiRange = { lo: d0.anchor - ratio0 * span1, hi: d0.anchor + (1 - ratio0) * span1 };
      _pfChartCanvasDraw(c);
      return;
    }
    var drag = c.drag, start = drag.viewport, p = c.projection;
    var tSpan = start.tMax - start.tMin, pSpan = start.pMax - start.pMin;
    drag.samples = (drag.samples || []).concat([{ x: point.x, at: performance.now() }]).slice(-6);
    if (drag.kind === 'pan') {
      var dt = -(point.x - drag.x) / p.plotW * tSpan;
      var dp = drag.timeOnly ? 0 : (point.y - drag.y) / p.plotH * pSpan;
      if (drag.rsi && p.rsiH) {
        // The RSI follows the hand: pulled down, the values above come in.
        var shift = (point.y - drag.y) / p.rsiH * (drag.rsi.hi - drag.rsi.lo);
        c.rsiRange = { lo: drag.rsi.lo + shift, hi: drag.rsi.hi + shift };
      }
      _pfChartCanvasQueueView(c, { tMin: start.tMin + dt, tMax: start.tMax + dt, pMin: start.pMin + dp, pMax: start.pMax + dp });
    } else if (drag.kind === 'price') {
      // Pulling the price axis upward zooms in (a tighter price range); pulling
      // it downward zooms out. This is intentionally opposite to screen Y,
      // which increases downward.
      var priceSpan = pSpan * Math.exp((point.y - drag.y) / 160);
      var priceRatio = (drag.anchorPrice - start.pMin) / pSpan;
      _pfChartCanvasQueueView(c, {
        tMin: start.tMin, tMax: start.tMax,
        pMin: drag.anchorPrice - priceRatio * priceSpan,
        pMax: drag.anchorPrice + (1 - priceRatio) * priceSpan
      });
    } else {
      var timeSpan = Math.max(0.5, tSpan * Math.exp((drag.x - point.x) / 160));
      var timeRatio = (drag.anchorTime - start.tMin) / tSpan;
      _pfChartCanvasQueueView(c, {
        tMin: drag.anchorTime - timeRatio * timeSpan,
        tMax: drag.anchorTime + (1 - timeRatio) * timeSpan,
        pMin: start.pMin, pMax: start.pMax
      });
    }
  });
  bind('pointerup', function (event) {
    if (_pfChartDrawPointerUp(c, event)) return;
    endDrag(event);
  });
  bind('pointercancel', function (event) {
    if (c.drag && c.drag.kind === 'draw') { c.drag = null; _pfChartDrawCommit(c); }
    endDrag(event);
  });
  bind('pointerleave', function () { if (!c.drag) _pfChartCanvasClearCrosshair(c); });
  bind('wheel', function (event) {
    var point = _pfChartCanvasPoint(c, event), p = c.projection, v = c.viewport;
    if (!point || !point.plot || !p || !v) return;
    event.preventDefault();
    if (c.coastFrame) { cancelAnimationFrame(c.coastFrame); c.coastFrame = 0; }
    // Line and page deltas (some mice) are scaled to pixels, so a notch and a
    // trackpad stroke of the same length move the chart the same amount.
    var unit = event.deltaMode === 1 ? 16 : (event.deltaMode === 2 ? 400 : 1);
    var dx = event.deltaX * unit, dy = event.deltaY * unit;
    var base = c.glideTarget || { tMin: v.tMin, tMax: v.tMax };
    var span = base.tMax - base.tMin, next;
    if (Math.abs(dx) > Math.abs(dy)) {
      // A sideways swipe pans time, as on TradingView.
      var shift = dx / p.plotW * span;
      next = { tMin: base.tMin + shift, tMax: base.tMax + shift };
    } else {
      // Time only: vertical scale is reserved for deliberate price-axis drags.
      // Proportional to the stroke: a mouse notch (~100px) is ~16%.
      var factor = Math.exp(Math.max(-0.3, Math.min(0.3, dy * 0.0015)));
      var newSpan = Math.max(0.5, Math.min(_pfChartCanvasMaxSpan(c), span * factor));
      var at = base.tMin + ((point.x - p.padL) / p.plotW) * span, ratio = (at - base.tMin) / span;
      next = { tMin: at - ratio * newSpan, tMax: at + (1 - ratio) * newSpan };
    }
    c.glideTarget = next;
    _pfChartCanvasGlide(c);
  }, { passive: false });
  bind('dblclick', function (event) {
    var point = _pfChartCanvasPoint(c, event), fit = _pfChartCanvasFit(c);
    if (point && (point.rsiAxis || (point.rsiPlot && !point.plot))) { c.rsiRange = null; _pfChartCanvasDraw(c); return; }
    if (!point || !fit || !c.viewport) return;
    if (point.priceAxis) _pfChartCanvasSetViewport(c, { tMin: c.viewport.tMin, tMax: c.viewport.tMax, pMin: fit.pMin, pMax: fit.pMax });
    else if (point.timeAxis) _pfChartCanvasSetViewport(c, { tMin: fit.tMin, tMax: fit.tMax, pMin: c.viewport.pMin, pMax: c.viewport.pMax });
    else if (point.plot) _pfChartCanvasSetViewport(c, fit);
  });
}


// ── The Test Bench's one entry point ─────────────────────────
// Render into whichever container the page hands us. Mounting is idempotent:
// a second run with a fresh result repaints the same surfaces rather than
// leaving a detached canvas and its observers behind.
// opts: {multi: true} keeps the charts in other containers up (a split desk);
// {background: true} draws it without taking the page's focus.
function pfBenchDrawChart(container, payload, opts) {
  if (!container) return false;
  if (!payload || !(payload.candles || []).length) {
    var gone = _pfChartFind(container.querySelector('#pf-bench-canvas-host'));
    if (opts && opts.multi) { if (gone) _pfChartCanvasTeardown(gone); }
    else _pfChartCanvasTeardown();
    container.innerHTML = '<p class="pf-bench-empty">No candles to draw.</p>';
    return false;
  }
  container.innerHTML = _pfBenchChartHostHtml();
  // Pass the container so the mount addresses THIS chart's surfaces, not
  // whichever host happens to come first in the document.
  _pfChartCanvasMount(payload, container, opts);
  return true;
}

// ── The one chart header strip ───────────────────────────────────────
// Phil, 2026-08-13: "Change or modify all the charts in the site to this
// specifications for the header strip" — the Campaign Chart's row of
// timeframes, − zoom% +, refresh and close, on every canvas. This builds
// that strip once so a caller cannot drift from the standard: it owns only
// the callbacks (what a timeframe change or refresh MEANS for its data).
//
// pfChartStrip(host, {
//   timeframes: ['5m','15m','1h'],   // omit for a chart with one timeframe
//   active: '5m',
//   onTimeframe: function (tf) {},
//   onRefresh: function () {},       // omit to hide the refresh button
//   onClose: function () {},         // omit to hide the close button
// })
// Zoom buttons drive the mounted canvas directly; the % readout is the
// fitted span over the visible span, so 100% is "the framed view".
function pfChartZoomPercent() {
  var c = _pfChartCanvas;
  if (!c || !c.viewport) return 100;
  var fit = _pfChartCanvasFit(c);
  if (!fit) return 100;
  var span = c.viewport.tMax - c.viewport.tMin;
  if (!(span > 0)) return 100;
  return Math.max(1, Math.round(((fit.tMax - fit.tMin) / span) * 100));
}

function pfChartStrip(host, opts) {
  if (!host) return null;
  opts = opts || {};
  var strip = document.createElement('div');
  strip.className = 'pf-chart-strip';
  var html = '';
  if (Array.isArray(opts.timeframes) && opts.timeframes.length) {
    html += '<div class="terminal-cascade-tf-toggle pf-chart-strip-tfs" role="radiogroup" aria-label="Chart timeframe">'
      + opts.timeframes.map(function (tf) {
        var active = String(tf) === String(opts.active || opts.timeframes[0]);
        return '<button type="button" class="terminal-cascade-tf-option' + (active ? ' is-active' : '') + '" data-strip-tf="'
          + tf + '" role="radio" aria-checked="' + (active ? 'true' : 'false') + '">' + String(tf).toUpperCase() + '</button>';
      }).join('')
      + '</div>';
  }
  // ONE chart toolbar for the whole site, copied from the Campaign Chart
  // (Equity -> Cash Cascade) because that is the design Phil adjudicated:
  // timeframes, then zoom, then the round controls, in that order. Zoom used
  // to float over the canvas here and sit in the toolbar there, which is
  // exactly the "deviates everywhere a chart exists" complaint. It lives in
  // the toolbar now; the host's floating cluster is hidden by CSS wherever
  // this strip supplies one.
  if (opts.zoom !== false) {
    html += '<div class="terminal-cascade-tf-toggle pf-chart-strip-zoom" role="group" aria-label="Chart zoom">'
      + '<button type="button" class="terminal-cascade-tf-option" data-strip-zoom-out title="Zoom out" aria-label="Zoom out">&minus;</button>'
      + '<button type="button" class="terminal-cascade-tf-option terminal-cascade-zoom-readout" data-pf-zoom-readout data-strip-zoom-reset title="Reset zoom">100%</button>'
      + '<button type="button" class="terminal-cascade-tf-option" data-strip-zoom-in title="Zoom in" aria-label="Zoom in">+</button>'
      + '</div>';
  }
  if (typeof opts.onRefresh === 'function') {
    html += '<button type="button" class="terminal-cascade-chart-control" data-strip-refresh aria-label="Refresh chart" title="Refresh chart">&#x27F3;</button>';
  }
  if (typeof opts.onClose === 'function') {
    html += '<button type="button" class="terminal-cascade-chart-control is-danger pf-chart-strip-close" data-strip-close aria-label="Close chart" title="Close chart">&#x2715;</button>';
  }
  strip.innerHTML = html;

  strip.addEventListener('click', function (event) {
    var tfBtn = event.target.closest('[data-strip-tf]');
    if (tfBtn) {
      strip.querySelectorAll('[data-strip-tf]').forEach(function (b) {
        var on = b === tfBtn;
        b.classList.toggle('is-active', on);
        b.setAttribute('aria-checked', on ? 'true' : 'false');
      });
      if (typeof opts.onTimeframe === 'function') opts.onTimeframe(tfBtn.getAttribute('data-strip-tf'));
      return;
    }
    // Zoom is NOT handled here — one delegated listener below drives it for
    // every toolbar, generated or hand-written, so they cannot drift apart.
    if (event.target.closest('[data-strip-refresh]')) { opts.onRefresh(); return; }
    if (event.target.closest('[data-strip-close]')) { opts.onClose(); }
  });
  host.insertBefore(strip, host.firstChild);
  return strip;
}

// The zoom trio behaves the same wherever it appears: the Campaign Chart's
// hand-written toolbar, a pfChartStrip, or the host's floating cluster. One
// delegated listener is what makes that true -- per-toolbar handlers are how
// the controls drifted apart in the first place. Factors match the host
// cluster's exactly (1.25 out, 0.8 in) so the same click zooms the same amount
// on every chart.
document.addEventListener('click', function (event) {
  var target = event.target;
  if (!target || typeof target.closest !== 'function') return;
  if (target.closest('[data-strip-zoom-out]')) { _pfChartCanvasZoom(1.25); return; }
  if (target.closest('[data-strip-zoom-in]')) { _pfChartCanvasZoom(0.8); return; }
  if (target.closest('[data-strip-zoom-reset]')) _pfChartCanvasZoom(0, true);
});
