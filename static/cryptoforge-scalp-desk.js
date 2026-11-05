/* THE SCALP DESK — the chart beside the order form.
 *
 * Phil, 2026-11-04: "Please create the same kinda scalp page for cryptoforge
 * as well same as that of philforge", everything in one go. PhilForge's Scalp
 * desk, brought across for a single coin:
 *
 *   * the coin in the form, charted beside it (cryptoforge-scalp-chart.js);
 *   * the forming bar grown from the Scalp engine's own live price, with the
 *     time to its close on the price tag;
 *   * O H L C of the hovered or live bar, and each open trade on the coin as a
 *     line with its side, entry, P&L and Exit; its TP and SL as lines;
 *   * B and S on the chart with the size between them -- the FORM's size, the
 *     form's order: one click is exactly the form's BUY or SELL;
 *   * the Studio: Supertrend (10,2), CPR, EMA 20, RSI 14, the magnet, three
 *     Fib sets, trendlines, Undo/Redo/Clear -- drawings kept per coin;
 *   * right-click for a price alarm, which the server keeps and rings once;
 *   * ⤢ for the chart full screen.
 *
 * The CE|PE split and the strike/expiry controls are PhilForge's options
 * desk and have no meaning for one coin, so they did not come across.
 *
 * Everything here is wired by addEventListener: the page's CSP forbids inline
 * handlers (script-src-attr 'none').
 */
(function () {
  'use strict';

  var TFS = ['1m', '3m', '5m', '15m', '1h'];
  var TF_SEC = { '1m': 60, '3m': 180, '5m': 300, '15m': 900, '1h': 3600 };
  var REFRESH_MS = 20000;
  var DEFAULT_BARS = 160;
  var VIEW_KEY = 'cf-scalp-chart-view-v1';
  var ST = { period: 10, multiplier: 2 };
  var RSI = { period: 14, upper: 60, lower: 40 };
  var FIB_PRESETS = {
    x2: { levels: [0, 1, 2, 4, 8], label: '0 · 1 · 2 · 4 · 8' },
    half: { levels: [0, 0.5, 1], label: '0 · 0.5 · 1' },
    quarter: { levels: [0, 0.25, 0.5, 0.75, 1], label: '0 · 0.25 · 0.5 · 0.75 · 1' }
  };
  var FIB_COLORS = { 0: '#94a3b8', 0.25: '#22d3ee', 0.5: '#f59e0b', 0.75: '#22d3ee', 1: '#94a3b8', 2: '#fb923c', 4: '#a78bfa', 8: '#f472b6' };
  var DRAW_KEY = 'cf_scalp_chart_drawings_v1';
  var UNDO_DEPTH = 50;

  // ── small helpers ────────────────────────────────────────────
  function $(id) { return document.getElementById(id); }
  function store(key, value) { try { localStorage.setItem(key, value); } catch (_) { /* private window */ } }
  function stored(key) { try { return localStorage.getItem(key); } catch (_) { return null; } }
  function switchOn(key, dflt) { var v = stored(key); return v === null ? dflt : v === '1'; }
  function esc(text) {
    return String(text == null ? '' : text).replace(/[&<>"']/g, function (ch) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch];
    });
  }
  function price(v, fixed) { return typeof window.pfChartPrice === 'function' ? window.pfChartPrice(v, fixed) : Number(v).toFixed(2); }
  function money(v) {
    var n = Number(v) || 0;
    return (n >= 0 ? '+' : '−') + '$' + Math.abs(n).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }
  // One name per coin across brokers: Delta's BTCUSD is Binance's BTCUSDT.
  function symKey(s) {
    var raw = String(s || '').trim().toUpperCase();
    if (raw === 'GOLD' || raw === 'GOLDUSDT') raw = 'PAXGUSD';
    return /USDT$/.test(raw) ? raw.slice(0, -1) : raw;
  }
  function pageActive() {
    var page = $('scalp-page');
    return !!page && page.classList.contains('active-page');
  }
  function toast(msg, kind) { if (typeof window.cfToast === 'function') window.cfToast(msg, kind); }
  function api(url, options) {
    var opts = Object.assign({ credentials: 'same-origin', cache: 'no-store' }, options || {});
    return typeof window.cfApiFetch === 'function' ? window.cfApiFetch(url, opts) : fetch(url, opts);
  }
  function status() { return (typeof _cfLatestScalpStatus !== 'undefined' && _cfLatestScalpStatus) || {}; }
  function canvas() { return typeof _pfChartCanvas !== 'undefined' ? _pfChartCanvas : null; }

  // ── what the chart shows ─────────────────────────────────────
  function viewLoad() {
    try { var v = JSON.parse(stored(VIEW_KEY) || 'null'); return v && typeof v === 'object' ? v : {}; } catch (_) { return {}; }
  }
  var saved = viewLoad();
  var tf = TFS.indexOf(saved.tf) >= 0 ? saved.tf : '5m';
  var spanMin = Number(saved.spanMin) > 0 ? Number(saved.spanMin) : 0;
  var shown = null;        // {key, symbol, data}: the payload on screen
  var liveBar = null;      // {key, tf, t, o, h, l}: survives the 20s refreshes
  var request = 0;
  var pendingKey = '';
  var failed = { key: '', at: 0 };
  var lastFetch = 0;
  var drawnKey = '';

  function formSymbol() {
    var el = $('cf-scalp-symbol');
    return el && el.value ? String(el.value).toUpperCase() : 'BTCUSDT';
  }
  function targetKey() { return formSymbol() + '|' + tf; }

  function viewSave() {
    store(VIEW_KEY, JSON.stringify({ tf: tf, spanMin: spanMin || 0 }));
  }
  function rememberSpan() {
    var c = canvas();
    if (!c || !c.viewport || !shown || c.data !== shown.data) return;
    var bars = c.viewport.tMax - c.viewport.tMin;
    if (bars > 0 && isFinite(bars)) spanMin = bars * (TF_SEC[shown.data.timeframe] || 300) / 60;
    viewSave();
  }
  var viewTimer = 0;
  function viewSoon() { clearTimeout(viewTimer); viewTimer = setTimeout(rememberSpan, 400); }
  window.addEventListener('pagehide', rememberSpan);

  // The live price of `symbol`: the engine's watched-coin price, else an open
  // trade's mark on that coin. Never another coin's.
  function livePrice(symbol) {
    var st = status(), want = symKey(symbol);
    var ctl = st.entry_controls || {};
    var p = Number(ctl.price) || 0;
    if (p > 0 && symKey(ctl.symbol) === want) return p;
    var trades = Array.isArray(st.open_trades) ? st.open_trades : [];
    for (var i = 0; i < trades.length; i++) {
      var t = trades[i];
      if (symKey(t.symbol) === want) {
        var m = Number(t.current_price || t.mark_price) || 0;
        if (m > 0) return m;
      }
    }
    return 0;
  }

  // Fold the live price into the last bar, or open the next one. Crypto never
  // closes, so the bar the clock is in is always the forming one.
  function applyLiveBar(data, key) {
    var candles = data && data.candles;
    var p = livePrice(data && data.symbol);
    if (!(p > 0) || !Array.isArray(candles) || !candles.length || typeof window.pfChartBarWindow !== 'function') return false;
    var sec = TF_SEC[data.timeframe] || 300;
    var win = window.pfChartBarWindow(Date.now() / 1000, sec);
    var last = candles[candles.length - 1], lastT = Number(last.t);
    if (!win || lastT > win.start) return false;
    if (!liveBar || liveBar.key !== key || liveBar.tf !== sec || liveBar.t !== win.start) {
      liveBar = { key: key, tf: sec, t: win.start, o: p, h: p, l: p };
    }
    liveBar.h = Math.max(liveBar.h, p);
    liveBar.l = Math.min(liveBar.l, p);
    var bar;
    if (lastT === win.start) {
      bar = Object.assign({}, last, { h: Math.max(Number(last.h), liveBar.h), l: Math.min(Number(last.l), liveBar.l), c: p });
      if (bar.h === Number(last.h) && bar.l === Number(last.l) && bar.c === Number(last.c)) return false;
      data.candles = candles.slice(0, -1).concat([bar]);
    } else {
      // The bar before closed at its own last trade; this one opens at the
      // first price seen in it.
      bar = { t: win.start, o: liveBar.o, h: liveBar.h, l: liveBar.l, c: p };
      data.candles = candles.concat([bar]);
    }
    data.live_price = p;
    return true;
  }

  // ── studies ──────────────────────────────────────────────────
  var stOn = switchOn('cf_scalp_chart_supertrend', true);
  var cprOn = switchOn('cf_scalp_chart_cpr', true);
  var emaOn = switchOn('cf_scalp_chart_ema', true);
  var rsiOn = switchOn('cf_scalp_chart_rsi', true);
  var magnetOn = switchOn('cf_scalp_chart_magnet', false);

  function supertrendRuns(candles, period, multiplier) {
    var n = candles.length;
    if (n < period + 1) return [];
    var h = candles.map(function (b) { return Number(b.h); });
    var l = candles.map(function (b) { return Number(b.l); });
    var c = candles.map(function (b) { return Number(b.c); });
    var atr = new Array(n);
    for (var i = 0; i < n; i++) {
      var tr = i === 0 ? h[0] - l[0] : Math.max(h[i] - l[i], Math.abs(h[i] - c[i - 1]), Math.abs(l[i] - c[i - 1]));
      atr[i] = i === 0 ? tr : atr[i - 1] + (tr - atr[i - 1]) / period;
    }
    var upper = 0, lower = 0, dir = 1, runs = [], run = null;
    for (var j = 0; j < n; j++) {
      var mid = (h[j] + l[j]) / 2, up = mid + multiplier * atr[j], dn = mid - multiplier * atr[j];
      if (j === 0) { upper = up; lower = dn; } else {
        lower = (dn > lower || c[j - 1] < lower) ? dn : lower;
        upper = (up < upper || c[j - 1] > upper) ? up : upper;
        if (dir < 0 && c[j] > upper) dir = 1;
        else if (dir > 0 && c[j] < lower) dir = -1;
      }
      if (j < period) continue; // the ATR has not settled yet
      var value = dir > 0 ? lower : upper;
      if (!run || run.dir !== dir) { run = { dir: dir, points: [] }; runs.push(run); }
      run.points.push({ t: candles[j].t, price: value });
    }
    return runs;
  }

  function rsiSeries(candles, period) {
    var out = [];
    if (!Array.isArray(candles) || candles.length <= period) return out;
    var gain = 0, loss = 0;
    for (var i = 1; i < candles.length; i++) {
      var change = Number(candles[i].c) - Number(candles[i - 1].c);
      var up = Math.max(change, 0), down = Math.max(-change, 0);
      if (i <= period) {
        gain += up; loss += down;
        if (i < period) continue;
        gain /= period; loss /= period;
      } else {
        gain = (gain * (period - 1) + up) / period;
        loss = (loss * (period - 1) + down) / period;
      }
      var v = loss === 0 ? 100 : 100 - 100 / (1 + gain / loss);
      out.push({ t: candles[i].t, v: Math.round(v * 100) / 100 });
    }
    return out;
  }

  // A phone: the dock is narrower than the desktop ticket row.
  function narrowDock() {
    var dock = $('cf-scalp-dock');
    return !!dock && dock.getBoundingClientRect().width > 0 && dock.getBoundingClientRect().width < 600;
  }
  // On a phone a level is named, not priced: "R4 (80,810.00)" over a 300px
  // plot covered the candles. The price is on the axis and under the crosshair.
  function shortLabel(label, narrow) {
    return narrow ? String(label || '').replace(/\s*\(.*\)$/, '') : label;
  }

  function isEma(ov) { return !!ov && (ov.group === 'ema' || /^EMA\b/.test(String(ov.label || ''))); }

  // ── drawings, per coin ───────────────────────────────────────
  var drawTool = '';
  function drawKey(data) { return symKey((data && data.symbol) || formSymbol()); }
  function readDrawings(key) {
    try {
      var all = JSON.parse(stored(DRAW_KEY) || '{}'), d = all[key] || {};
      return { fibs: Array.isArray(d.fibs) ? d.fibs : [], lines: Array.isArray(d.lines) ? d.lines : [] };
    } catch (_) { return { fibs: [], lines: [] }; }
  }
  function writeDrawings(key, d) {
    try {
      var all = JSON.parse(stored(DRAW_KEY) || '{}');
      if (!d.fibs.length && !d.lines.length) delete all[key]; else all[key] = d;
      store(DRAW_KEY, JSON.stringify(all));
    } catch (_) { /* private mode: drawings last as long as the page */ }
  }
  function drawingsFor(key) {
    var d = readDrawings(key);
    var lines = d.lines.filter(function (ln) { return ln && ln.a && ln.b; })
      .map(function (ln, i) { return { id: ln.id || ('l' + i), kind: 'line', extend: 'right', a: ln.a, b: ln.b }; });
    var fibs = d.fibs.filter(function (f) { return f && f.a && f.b && FIB_PRESETS[f.preset]; })
      .map(function (f, i) { return { id: f.id || ('f' + i), kind: 'fib', preset: f.preset, levels: FIB_PRESETS[f.preset].levels, colors: FIB_COLORS, a: f.a, b: f.b }; });
    return lines.concat(fibs);
  }
  var dragBefore = {};
  var history = {};
  function historyOf(key) { if (!history[key]) history[key] = { past: [], future: [] }; return history[key]; }
  function recordDrawings(key, next, startedFrom) {
    var before = startedFrom || readDrawings(key);
    if (JSON.stringify(before) === JSON.stringify(next)) { writeDrawings(key, next); return; }
    var h = historyOf(key);
    h.past.push(before);
    if (h.past.length > UNDO_DEPTH) h.past.shift();
    h.future = [];
    writeDrawings(key, next);
    paintUndo();
  }
  function keepDrawings(key, list, meta) {
    var next = {
      lines: list.filter(function (dr) { return dr.kind === 'line'; }).map(function (dr) { return { id: dr.id, a: dr.a, b: dr.b }; }),
      fibs: list.filter(function (dr) { return dr.kind === 'fib'; }).map(function (dr) { return { id: dr.id, preset: dr.preset, a: dr.a, b: dr.b }; })
    };
    if (meta && meta.live) {
      if (!dragBefore[key]) dragBefore[key] = readDrawings(key);
      writeDrawings(key, next);
      return;
    }
    var before = dragBefore[key];
    delete dragBefore[key];
    recordDrawings(key, next, before);
  }
  function stepDrawings(direction) {
    if (!shown) return;
    var key = drawKey(shown.data), h = historyOf(key);
    var from = direction < 0 ? h.past : h.future, to = direction < 0 ? h.future : h.past;
    if (!from.length) return;
    endDraw();
    to.push(readDrawings(key));
    writeDrawings(key, from.pop());
    redecorate();
    paintUndo();
  }
  function paintUndo() {
    var h = shown ? historyOf(drawKey(shown.data)) : { past: [], future: [] };
    document.querySelectorAll('#cf-scalp-dock .cf-desk-undo').forEach(function (b) { b.disabled = !h.past.length; });
    document.querySelectorAll('#cf-scalp-dock .cf-desk-redo').forEach(function (b) { b.disabled = !h.future.length; });
  }
  document.addEventListener('keydown', function (event) {
    if (!(event.ctrlKey || event.metaKey) || event.altKey) return;
    var k = String(event.key || '').toLowerCase();
    if (k !== 'z' && k !== 'y') return;
    var t = event.target;
    if (t && (t.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName))) return;
    if (!shown || !pageActive()) return;
    event.preventDefault();
    stepDrawings(k === 'y' || event.shiftKey ? 1 : -1);
  });

  // ── the open trades on the charted coin ──────────────────────
  function openTrades() {
    var st = status();
    return Array.isArray(st.open_trades) ? st.open_trades : [];
  }
  function tradesOnChart() {
    if (!shown) return [];
    var want = symKey(shown.data.symbol);
    return openTrades().filter(function (t) { return symKey(t.symbol) === want; });
  }
  function tradeTime(t) {
    var raw = String(t.entry_time || '');
    if (!raw) return 0;
    var iso = raw.replace(' ', 'T');
    if (!/[zZ]|[+-]\d\d:?\d\d$/.test(iso)) iso += 'Z';
    var ms = Date.parse(iso);
    return isFinite(ms) ? Math.floor(ms / 1000) : 0;
  }

  // Everything the page lays over the server's chart, rebuilt from the
  // payload as it came (kept in data._base): CPR and EMA when switched on,
  // Supertrend, RSI, each open trade's TP and SL, alarms and the drawings.
  function decorate(data) {
    if (!data) return;
    if (!data._base) data._base = { overlays: (data.overlays || []).slice(), lines: (data.lines || []).slice(), indicators: data.indicators || null };
    if (rsiOn) {
      data.indicators = Object.assign({}, data._base.indicators || {}, {
        rsi: rsiSeries(data.candles || [], RSI.period), rsi_period: RSI.period, rsi_upper: RSI.upper, rsi_lower: RSI.lower
      });
    } else {
      data.indicators = data._base.indicators;
    }
    var narrow = narrowDock();
    var valued = function (label, pts) {
      var last = Array.isArray(pts) && pts.length ? Number(pts[pts.length - 1].price) : NaN;
      return isFinite(last) && !narrow ? label + ' (' + price(last, true) + ')' : label;
    };
    var overlays = data._base.overlays.filter(function (ov) {
      return (cprOn || !ov || ov.group !== 'cpr') && (emaOn || !isEma(ov));
    }).map(function (ov) {
      if (isEma(ov) && ov.label) return Object.assign({}, ov, { label: valued(String(ov.label).replace(/\s*\(.*\)$/, ''), ov.points) });
      return narrow && ov && ov.label ? Object.assign({}, ov, { label: shortLabel(ov.label, true) }) : ov;
    });
    var lines = data._base.lines.slice();
    if (stOn) {
      supertrendRuns(data.candles || [], ST.period, ST.multiplier).forEach(function (run, i, all) {
        if (run.points.length < 2) return;
        overlays.push({
          label: i === all.length - 1 ? valued('ST ' + ST.period + ',' + ST.multiplier, run.points) : '',
          color: run.dir > 0 ? '#4caf50' : '#ff5252', width: 1.6, points: run.points, supertrend: run.dir
        });
      });
    }
    var mine = tradesOnChart();
    mine.forEach(function (t) {
      var tp = Number(t.target_price) || 0, sl = Number(t.sl_price) || 0;
      if (tp > 0) lines.push({ price: tp, label: 'TP #' + t.trade_id, color: '#10b981', dash: [6, 4], width: 1.2, opacity: 0.9 });
      if (sl > 0) lines.push({ price: sl, label: 'SL #' + t.trade_id, color: '#ef4444', dash: [6, 4], width: 1.2, opacity: 0.9 });
    });
    data.entries = mine.map(function (t) { return { t: tradeTime(t), price: Number(t.entry_price) || 0 }; })
      .filter(function (e) { return e.t > 0 && e.price > 0; });
    var key = drawKey(data);
    var c = canvas();
    var drawingNow = !!c && c.data === data && ((c.drag && c.drag.kind === 'draw') || c.drawPending);
    if (!(drawingNow && Array.isArray(data.drawings))) data.drawings = drawingsFor(key);
    data.onDrawingsChange = function (list, meta) { keepDrawings(key, list, meta); };
    data.onDraw = function () { hudTrades(); };
    data.fontFloor = narrow ? 0.95 : 1.05;
    data.padTop = 43;
    data.padLeft = narrow ? 24 : 0;
    data.padRight = narrow ? 58 : 0;
    data.rsiPaneH = Number(stored('cf_scalp_rsi_pane_h')) || 0;
    data.onRsiResize = function (h) { store('cf_scalp_rsi_pane_h', String(h)); };
    data.magnet = magnetOn;
    data.onCrosshair = function (idx) { ohlcHover = idx; ohlcShow(); };
    data.onContextMenu = function (at) { alarmMenu(data, at); };
    alarmsFor(key).forEach(function (a) {
      lines.push({ price: a.price, label: '\u{1F514} ALARM', color: '#f59e0b', dash: [2, 3], width: 1.3, opacity: 0.95 });
    });
    data.overlays = overlays;
    data.lines = lines;
    data.future_bars = 0;
  }

  function redecorate() {
    var c = canvas();
    if (!shown || !c || c.data !== shown.data || typeof _pfChartCanvasRefresh !== 'function') return;
    var state = _pfChartCanvasRefreshState();
    decorate(shown.data);
    _pfChartCanvasRefresh(shown.data, state);
  }

  // ── loading the chart ────────────────────────────────────────
  function els() {
    return { title: $('cf-scalp-dock-title'), meta: $('cf-scalp-dock-meta'), body: $('cf-scalp-dock-body') };
  }

  function emptyText(text, bad) {
    var body = els().body;
    if (!body) return;
    if (typeof _pfChartCanvasTeardown === 'function' && canvas() && body.contains(canvas().host)) _pfChartCanvasTeardown();
    body.innerHTML = '<div class="cf-desk-empty' + (bad ? ' is-bad' : '') + '">' + esc(text) + '</div>';
    var hud = $('cf-scalp-dock-hud');
    if (hud) hud.hidden = true;
  }

  async function load(options) {
    var opts = options || {};
    if (!pageActive()) return;
    buildToolbar();
    var symbol = formSymbol(), key = symbol + '|' + tf;
    if (!opts.force && failed.key === key && Date.now() - failed.at < 60000) return;
    if (pendingKey === key && !opts.force) return;
    pendingKey = key;
    var id = ++request;
    lastFetch = Date.now();
    var body = els().body;
    var hasCanvas = !!(body && body.querySelector('#pf-bench-canvas-host'));
    if (!hasCanvas && body) body.innerHTML = '<div class="cf-desk-empty">Loading ' + esc(symbol) + ' ' + esc(tf) + ' candles…</div>';
    try {
      var res = await api('/api/scalp/chart?' + new URLSearchParams({ symbol: symbol, timeframe: tf }));
      var data = await res.json().catch(function () { return {}; });
      if (id !== request || targetKey() !== key) return;
      if (!res.ok || data.status !== 'ok') {
        var detail = typeof window.cfApiErrorDetail === 'function' ? window.cfApiErrorDetail(data, '') : (data.detail || '');
        throw new Error(detail || ('Chart failed (' + res.status + ')'));
      }
      if (!(data.candles || []).length) throw new Error('No candles for ' + symbol + ' yet.');
      var sec = TF_SEC[data.timeframe] || 300;
      if (shown) rememberSpan();
      data.initial_bars = spanMin > 0 ? Math.max(5, Math.round(spanMin * 60 / sec)) : DEFAULT_BARS;
      data.countdown = true;
      data.onViewport = viewSoon;
      applyLiveBar(data, key);
      decorate(data);
      shown = { key: key, symbol: symbol, data: data };
      paintUndo();
      var t = els();
      if (t.title) t.title.textContent = symbol;
      if (t.meta) t.meta.textContent = String(data.timeframe || tf).toUpperCase() + ' · ' + String(data.broker || '').replace(/^./, function (ch) { return ch.toUpperCase(); });
      var host = $('cf-scalp-dock-canvas');
      if (!host) {
        body.innerHTML = '<div class="cf-desk-canvas" id="cf-scalp-dock-canvas"></div>';
        host = $('cf-scalp-dock-canvas');
      }
      var c = canvas();
      var mine = !!(c && c.host && c.host.closest('#cf-scalp-dock-canvas') === host);
      var view = mine && drawnKey === key && typeof _pfChartCanvasRefreshState === 'function' ? _pfChartCanvasRefreshState() : null;
      var refreshed = mine && typeof _pfChartCanvasRefresh === 'function' && _pfChartCanvasRefresh(data, view);
      drawnKey = key;
      if (!refreshed) {
        if (typeof window.pfBenchDrawChart !== 'function') throw new Error('Chart renderer is unavailable');
        if (!window.pfBenchDrawChart(host, data)) throw new Error('The chart could not be drawn.');
      }
      failed = { key: '', at: 0 };
      hudTrades();
    } catch (err) {
      if (id !== request) return;
      failed = { key: key, at: Date.now() };
      shown = null;
      emptyText((err && err.message) || 'Chart unavailable', true);
    } finally {
      if (pendingKey === key) pendingKey = '';
    }
  }

  // Each second: follow the form's coin, grow the forming bar, keep the HUD
  // honest. Every 20 seconds the bars are fetched again.
  function tick() {
    if (!pageActive() || document.hidden) return;
    var dock = $('cf-scalp-dock');
    if (!dock || !dock.getClientRects().length) return;
    var key = targetKey();
    if (!shown || shown.key !== key) {
      if (pendingKey !== key) load();
      return;
    }
    if (Date.now() - lastFetch > REFRESH_MS) { load(); return; }
    var c = canvas();
    if (!c || c.data !== shown.data) return;
    var state = typeof _pfChartCanvasRefreshState === 'function' ? _pfChartCanvasRefreshState() : null;
    var grew = applyLiveBar(shown.data, shown.key);
    var tradesKey = tradesSignature();
    if ((grew || tradesKey !== lastTradesKey) && typeof _pfChartCanvasRefresh === 'function') {
      lastTradesKey = tradesKey;
      decorate(shown.data);
      _pfChartCanvasRefresh(shown.data, state);
    }
    hudTrades();
  }
  var lastTradesKey = '';
  function tradesSignature() {
    return tradesOnChart().map(function (t) { return [t.trade_id, t.entry_price, t.target_price, t.sl_price].join(':'); }).join('|');
  }

  // ── toolbar: timeframes, zoom, refresh, the Studio ───────────
  var toolbarBuilt = false;
  function buildToolbar() {
    if (toolbarBuilt) return;
    var actions = $('cf-scalp-dock-actions');
    if (!actions || typeof window.pfChartStrip !== 'function') return;
    toolbarBuilt = true;
    window.pfChartStrip(actions, {
      timeframes: TFS,
      active: tf,
      onTimeframe: function (next) { if (shown) rememberSpan(); tf = next; viewSave(); failed = { key: '', at: 0 }; load({ force: true }); },
      onRefresh: function () { failed = { key: '', at: 0 }; load({ force: true }); }
    });
    var title = document.querySelector('#cf-scalp-dock .cf-desk-title');
    var drawer = studioDrawer(chartTools());
    if (title) title.after(drawer); else actions.insertBefore(drawer, actions.firstChild);
    placeStudio(drawer);
  }

  var STUDIO_KEY = 'cf-scalp-studio-open';
  function placeStudio(drawer) {
    var bar = drawer.closest('.cf-desk-toolbar');
    var tab = drawer.querySelector('.cf-desk-studio-tab');
    var tray = $('cf-desk-studio-tray');
    if (!bar || !tab || !tray) return;
    var b = bar.getBoundingClientRect(), t = tab.getBoundingClientRect();
    if (!b.width || !t.width) return;
    var actions = bar.querySelector('.cf-desk-actions');
    var a = actions && actions.getClientRects().length ? actions.getBoundingClientRect() : null;
    var sameRow = a && a.top < t.bottom && a.bottom > t.top;
    var tools = tray.firstElementChild;
    if (tools) tools.style.flexWrap = 'nowrap';
    tray.style.maxWidth = 'none';
    var kids = tools ? Array.prototype.filter.call(tools.children, function (k) { return k.getClientRects().length; }) : [];
    var natural = kids.length ? kids[kids.length - 1].getBoundingClientRect().right - kids[0].getBoundingClientRect().left + 12 : 0;
    if (tools) tools.style.flexWrap = '';
    var room = sameRow ? a.left - t.right - 16 : (a ? 0 : b.right - t.right - 16);
    if (natural <= room) {
      if (tray.parentElement !== drawer) drawer.appendChild(tray);
      tray.classList.remove('is-row');
      tray.style.left = Math.round(t.right - b.left + 8) + 'px';
      tray.style.top = Math.round(t.top - b.top - 5) + 'px';
      tray.style.maxWidth = Math.round(room) + 'px';
    } else {
      if (tray.parentElement !== bar) bar.appendChild(tray);
      tray.classList.add('is-row');
      tray.style.left = tray.style.top = tray.style.maxWidth = '';
    }
  }
  var wasNarrow = null, narrowTimer = 0;
  window.addEventListener('resize', function () {
    var drawer = document.querySelector('#cf-scalp-dock .cf-desk-studio');
    if (drawer) placeStudio(drawer);
    // Turning a phone, or a window dragged across the phone width: the labels
    // and the left gutter follow.
    clearTimeout(narrowTimer);
    narrowTimer = setTimeout(function () {
      var now = narrowDock();
      if (wasNarrow !== null && now !== wasNarrow) redecorate();
      wasNarrow = now;
    }, 150);
  });
  var studioTimer = null;
  function studioDrawer(tools) {
    var drawer = document.createElement('div');
    drawer.className = 'cf-desk-studio';
    drawer.innerHTML = '<button type="button" class="cf-desk-studio-tab" aria-controls="cf-desk-studio-tray">'
      + '<span class="cf-desk-studio-spark" aria-hidden="true">✦</span><span>Studio</span><span class="cf-desk-studio-arrow" aria-hidden="true">▸</span>'
      + '</button><div class="cf-desk-studio-tray" id="cf-desk-studio-tray"></div>';
    drawer.querySelector('.cf-desk-studio-tray').appendChild(tools);
    drawer.querySelector('.cf-desk-studio-tab').addEventListener('click', function () {
      var open = !drawer.classList.contains('is-open');
      setStudio(drawer, open, true);
      store(STUDIO_KEY, open ? '1' : '0');
    });
    setStudio(drawer, stored(STUDIO_KEY) === '1', false);
    return drawer;
  }
  function setStudio(drawer, open, animate) {
    var tab = drawer.querySelector('.cf-desk-studio-tab');
    var tray = drawer.querySelector('.cf-desk-studio-tray') || $('cf-desk-studio-tray');
    clearTimeout(studioTimer);
    [drawer, tray].forEach(function (el) { el.classList.remove('is-settled'); el.classList.toggle('is-open', open); });
    tab.setAttribute('aria-expanded', String(open));
    tab.title = open ? 'Fold the chart tools away' : 'Studies and drawing tools: Supertrend, CPR, EMA, RSI, Fib, lines';
    tray.inert = !open;
    if (!animate) { tray.style.transition = 'none'; setTimeout(function () { tray.style.transition = ''; }, 400); }
    if (open && drawer.isConnected) placeStudio(drawer);
    if (open) {
      var settle = function () { [drawer, tray].forEach(function (el) { el.classList.add('is-settled'); }); };
      if (animate) studioTimer = setTimeout(settle, 320); else settle();
    } else {
      closeFibMenus();
    }
  }

  function paintTools() {
    var pairs = [['.cf-desk-st', stOn], ['.cf-desk-cpr', cprOn], ['.cf-desk-ema', emaOn], ['.cf-desk-rsi', rsiOn], ['.cf-desk-magnet', magnetOn]];
    pairs.forEach(function (p) {
      document.querySelectorAll('#cf-scalp-dock ' + p[0]).forEach(function (b) { b.setAttribute('aria-pressed', p[1] ? 'true' : 'false'); b.classList.toggle('is-on', p[1]); });
    });
    document.querySelectorAll('#cf-scalp-dock .cf-desk-fib').forEach(function (b) { b.classList.toggle('is-armed', drawTool === 'fib'); });
    document.querySelectorAll('#cf-scalp-dock .cf-desk-line').forEach(function (b) { b.classList.toggle('is-armed', drawTool === 'line'); });
  }
  function closeFibMenus() { document.querySelectorAll('#cf-scalp-dock .cf-desk-fib-menu').forEach(function (m) { m.hidden = true; }); }
  function endDraw() {
    if (typeof window.pfChartDisarmDrawing === 'function' && typeof window.pfChartDrawingArmed === 'function' && window.pfChartDrawingArmed()) window.pfChartDisarmDrawing();
    drawTool = '';
    closeFibMenus();
    paintTools();
  }
  function startDraw(tool, preset) {
    if (!shown) { toast('Wait for the chart to load first.', 'warning'); return; }
    if (typeof window.pfChartArmDrawing !== 'function') return;
    closeFibMenus();
    drawTool = tool;
    var fib = tool === 'fib' ? FIB_PRESETS[preset] : null;
    window.pfChartArmDrawing(tool, {
      levels: fib ? fib.levels : null,
      colors: fib ? FIB_COLORS : null,
      extra: fib ? { preset: preset } : { extend: 'right' },
      onEnd: function () { drawTool = ''; paintTools(); }
    });
    paintTools();
  }
  function clearDrawings() {
    if (!shown) return;
    endDraw();
    recordDrawings(drawKey(shown.data), { fibs: [], lines: [] });
    redecorate();
  }
  document.addEventListener('pointerdown', function (event) {
    if (event.target && event.target.closest && event.target.closest('.cf-desk-fib-wrap')) return;
    closeFibMenus();
  }, true);

  function chartTools() {
    var box = document.createElement('div');
    box.className = 'cf-desk-tools';
    var btn = function (cls, text, title) {
      var b = document.createElement('button');
      b.type = 'button';
      b.className = 'cf-desk-control ' + cls;
      b.textContent = text;
      b.title = title;
      return b;
    };
    var flip = function (name, storeKey, set) {
      return function () { set(); store(storeKey, name() ? '1' : '0'); paintTools(); redecorate(); };
    };
    var st = btn('cf-desk-st', 'ST ' + ST.period + ',' + ST.multiplier, 'Supertrend (' + ST.period + ', ' + ST.multiplier + ')');
    st.addEventListener('click', flip(function () { return stOn; }, 'cf_scalp_chart_supertrend', function () { stOn = !stOn; }));
    var cpr = btn('cf-desk-cpr', 'CPR', "CPR, R1-R4 and S1-S5 from the previous UTC day (the exchange's daily candle)");
    cpr.addEventListener('click', flip(function () { return cprOn; }, 'cf_scalp_chart_cpr', function () { cprOn = !cprOn; }));
    var ema = btn('cf-desk-ema', 'EMA', 'EMA (20)');
    ema.addEventListener('click', flip(function () { return emaOn; }, 'cf_scalp_chart_ema', function () { emaOn = !emaOn; }));
    var rsi = btn('cf-desk-rsi', 'RSI', 'RSI (' + RSI.period + ') under the chart, bands at 60 and 40');
    rsi.addEventListener('click', flip(function () { return rsiOn; }, 'cf_scalp_chart_rsi', function () { rsiOn = !rsiOn; }));
    var fibWrap = document.createElement('div');
    fibWrap.className = 'cf-desk-fib-wrap';
    var fib = btn('cf-desk-fib', 'Fib ▾', 'Fib: pick a set, then click the 0 point and the 1 point');
    var menu = document.createElement('div');
    menu.className = 'cf-desk-fib-menu';
    menu.hidden = true;
    Object.keys(FIB_PRESETS).forEach(function (key) {
      var item = document.createElement('button');
      item.type = 'button';
      item.className = 'cf-desk-fib-item';
      item.textContent = FIB_PRESETS[key].label;
      item.addEventListener('click', function () { startDraw('fib', key); });
      menu.appendChild(item);
    });
    fib.addEventListener('click', function () {
      if (drawTool === 'fib') { endDraw(); return; }
      var opening = menu.hidden;
      closeFibMenus();
      menu.hidden = !opening;
    });
    fibWrap.append(fib, menu);
    var line = btn('cf-desk-line', 'Line', 'Trendline: click two points');
    line.addEventListener('click', function () { if (drawTool === 'line') endDraw(); else startDraw('line'); });
    var magnet = btn('cf-desk-magnet', '', "Magnet: the crosshair reads the candle's open, high, low or close");
    magnet.setAttribute('aria-label', 'Magnet');
    magnet.innerHTML = '<svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" aria-hidden="true"><path d="M6 3v8a6 6 0 0 0 12 0V3"/><path d="M6 7h4M14 7h4"/></svg>';
    magnet.addEventListener('click', function () {
      magnetOn = !magnetOn;
      store('cf_scalp_chart_magnet', magnetOn ? '1' : '0');
      if (shown) shown.data.magnet = magnetOn;
      paintTools();
    });
    var undo = btn('cf-desk-undo', '↶', 'Undo the last drawing change (Ctrl/⌘+Z)');
    undo.setAttribute('aria-label', 'Undo');
    undo.addEventListener('click', function () { stepDrawings(-1); });
    var redo = btn('cf-desk-redo', '↷', 'Redo (Ctrl/⌘+Shift+Z)');
    redo.setAttribute('aria-label', 'Redo');
    redo.addEventListener('click', function () { stepDrawings(1); });
    var clear = btn('cf-desk-clear', 'Clear', "Remove this coin's Fibs and trendlines");
    clear.addEventListener('click', clearDrawings);
    var full = btn('cf-desk-full-btn', '⤢', 'Chart full screen (Esc to leave)');
    full.setAttribute('aria-label', 'Chart full screen');
    full.addEventListener('click', toggleFull);
    box.append(st, cpr, ema, rsi, fibWrap, line, magnet, undo, redo, clear, full);
    setTimeout(function () { paintTools(); paintUndo(); }, 0);
    return box;
  }

  // ── full screen ──────────────────────────────────────────────
  function toggleFull(force) {
    var on = typeof force === 'boolean' ? force : !document.body.classList.contains('cf-desk-full');
    document.body.classList.toggle('cf-desk-full', on);
    document.querySelectorAll('#cf-scalp-dock .cf-desk-full-btn').forEach(function (b) { b.classList.toggle('is-on', on); b.setAttribute('aria-pressed', String(on)); });
    // The renderer re-measures on resize; the HUD follows on its next draw.
    setTimeout(function () { window.dispatchEvent(new Event('resize')); hudTrades(); }, 60);
  }
  document.addEventListener('keydown', function (event) {
    if (event.key === 'Escape' && document.body.classList.contains('cf-desk-full')) toggleFull(false);
  });

  // ── the HUD: ticket, O H L C, the trade lines ────────────────
  var ticketBusy = false;
  var ticketTimer = null;
  var ohlcHover = null;

  function hudCanvas() {
    var c = canvas();
    if (!c || !c.host || !c.host.isConnected || !c.projection || !c.host.closest('#cf-scalp-dock')) return null;
    if (!shown || c.data !== shown.data) return null;
    return c;
  }

  function qtyMode() {
    var el = $('cf-scalp-qty-mode');
    return el && String(el.value).toLowerCase() === 'base' ? 'base' : 'usdt';
  }

  function syncTicket() {
    var size = $('cf-desk-ticket-size'), form = $('cf-scalp-qty');
    if (size && form && document.activeElement !== size) size.value = form.value || '';
    var unit = $('cf-desk-ticket-unit');
    if (unit) {
      var base = qtyMode() === 'base';
      unit.textContent = base ? 'Qty' : '$';
      unit.title = base ? 'Size is a coin quantity. Click for margin in $.' : 'Size is margin in $. Click for a coin quantity.';
    }
    var live = (($('cf-scalp-mode') || {}).value || 'paper') === 'live';
    document.querySelectorAll('#cf-desk-ticket [data-desk-mode]').forEach(function (b) {
      var on = b.dataset.deskMode === (live ? 'live' : 'paper');
      b.classList.toggle('active', on);
      b.setAttribute('aria-pressed', String(on));
    });
    var broker = (typeof cfScalpSelectedBroker === 'function' ? cfScalpSelectedBroker() : '') || '';
    var label = live ? 'LIVE · ' + broker.toUpperCase() : 'PAPER';
    var modeEl = $('cf-desk-ticket-mode');
    if (modeEl) { modeEl.textContent = label; modeEl.classList.toggle('is-live', live); }
    var orderEl = $('cf-desk-ticket-order');
    var ot = ($('cf-scalp-order-type') || {}).value || 'market';
    if (orderEl) orderEl.textContent = (typeof cfScalpOrderLabel === 'function' ? cfScalpOrderLabel(ot) : ot).toUpperCase();
    var ticket = $('cf-desk-ticket');
    if (ticket) ticket.classList.toggle('is-live', live);
    var matches = !!shown && symKey(shown.data.symbol) === symKey(formSymbol());
    ['BUY', 'SELL'].forEach(function (dir) {
      var b = $(dir === 'BUY' ? 'cf-desk-ticket-buy' : 'cf-desk-ticket-sell');
      if (!b) return;
      b.disabled = !matches || ticketBusy;
      b.title = matches ? (dir === 'BUY' ? 'Buy ' : 'Sell ') + formSymbol() + ' · ' + label + ' · the form\'s order, size, TP and SL'
        : 'The chart is still loading the form\'s coin';
    });
  }

  function ticketSay(text, tone) {
    var el = $('cf-desk-ticket-status');
    if (!el) return;
    el.textContent = text || '';
    el.dataset.tone = tone || '';
    clearTimeout(ticketTimer);
    if (text) ticketTimer = setTimeout(function () { el.textContent = ''; el.dataset.tone = ''; }, 8000);
  }

  // The chart's size IS the form's size, written straight through.
  function ticketSetSize(value) {
    var form = $('cf-scalp-qty');
    if (!form) return;
    var n = Number(value);
    if (!(n > 0)) return;
    if (String(form.value) !== String(value)) {
      form.value = String(value);
      form.dispatchEvent(new Event('input', { bubbles: true }));
      form.dispatchEvent(new Event('change', { bubbles: true }));
    }
  }
  function stepSize(dir) {
    var form = $('cf-scalp-qty');
    var n = Number(form && form.value) || 0;
    var step;
    if (qtyMode() === 'base') {
      step = n > 0 ? Math.pow(10, Math.floor(Math.log10(n))) / 2 : 0.001;
    } else {
      step = n >= 10 ? Math.pow(10, Math.floor(Math.log10(n))) / 2 : 1;
    }
    var next = Math.max(step, n + dir * step);
    var digits = Math.max(0, -Math.floor(Math.log10(step)) + 1);
    ticketSetSize(Number(next.toFixed(Math.min(digits, 8))));
    syncTicket();
  }

  async function ticketTrade(direction) {
    if (ticketBusy || typeof cfSubmitScalp !== 'function') return;
    if (!shown || symKey(shown.data.symbol) !== symKey(formSymbol())) { ticketSay('The chart is still loading the form\'s coin', 'bad'); return; }
    var size = $('cf-desk-ticket-size');
    if (size) ticketSetSize(size.value);
    ticketBusy = true;
    syncTicket();
    ticketSay('Placing…', '');
    try {
      await cfSubmitScalp(direction);
      var el = $('cf-scalp-entry-status');
      var text = String((el && el.textContent) || '').trim();
      var bad = el && /red|var\(--red\)/.test(el.style.color || '');
      ticketSay(text, bad ? 'bad' : (/✓|armed|entered/i.test(text) ? 'good' : ''));
    } finally {
      ticketBusy = false;
      syncTicket();
    }
  }

  function pillHtml(t) {
    var buy = String(t.side || 'LONG').toUpperCase() !== 'SHORT' && String(t.side || '').toUpperCase() !== 'SELL';
    var pnl = Number(t.unrealized_pnl != null ? t.unrealized_pnl : t.pnl) || 0;
    var live = String(t.mode || 'paper').toLowerCase() === 'live';
    var broker = live ? String(t.broker_name || 'live').toUpperCase() : 'PAPER';
    var entry = Number(t.entry_price) || 0;
    var qty = Number(t.size || t.base_qty) || 0;
    return '<div class="cf-desk-pill-row ' + (buy ? 'is-buy' : 'is-sell') + '" data-tid="' + esc(t.trade_id) + '" data-price="' + entry + '">'
      + '<div class="cf-desk-pill" title="#' + esc(t.trade_id) + ' · ' + (buy ? 'LONG' : 'SHORT') + ' ' + esc(qty) + ' @ $' + esc(price(entry, true)) + '">'
      + '<span class="cf-desk-pill-side">' + (buy ? 'B' : 'S') + '</span>'
      + '<span class="cf-desk-pill-broker' + (live ? ' is-live' : '') + '">' + esc(broker) + '</span>'
      + '<span class="cf-desk-pill-entry">$' + esc(price(entry, true)) + '</span>'
      + '<span class="cf-desk-pill-pnl ' + (pnl > 0 ? 'is-up' : pnl < 0 ? 'is-down' : '') + '">' + esc(money(pnl)) + '</span>'
      + '<button type="button" class="btn btn-danger btn-sm cf-desk-pill-exit read-only-hide" data-desk-exit="' + esc(t.trade_id) + '" title="Exit #' + esc(t.trade_id) + '">Exit</button>'
      + '</div></div>';
  }

  function hudTrades() {
    var host = $('cf-desk-pills');
    if (!host) return;
    var mine = tradesOnChart();
    var key = mine.map(function (t) { return [t.trade_id, t.entry_price, t.mode, t.broker_name, t.side].join(':'); }).join('|');
    if (host.dataset.key !== key) {
      host.innerHTML = mine.map(pillHtml).join('');
      host.dataset.key = key;
    } else {
      mine.forEach(function (t) {
        var el = host.querySelector('.cf-desk-pill-row[data-tid="' + String(t.trade_id).replace(/"/g, '') + '"] .cf-desk-pill-pnl');
        if (!el) return;
        var pnl = Number(t.unrealized_pnl != null ? t.unrealized_pnl : t.pnl) || 0;
        el.textContent = money(pnl);
        el.classList.toggle('is-up', pnl > 0);
        el.classList.toggle('is-down', pnl < 0);
      });
    }
    hudPlace();
  }

  function ohlcShow() {
    var el = $('cf-desk-ohlc');
    var bars = (shown && shown.data && shown.data.candles) || [];
    if (!el) return;
    if (!bars.length) { el.innerHTML = ''; return; }
    var i = Number.isInteger(ohlcHover) && ohlcHover >= 0 && ohlcHover < bars.length ? ohlcHover : bars.length - 1;
    var bar = bars[i], prev = bars[i - 1];
    var up = Number(bar.c) >= Number(bar.o);
    var chg = prev ? Number(bar.c) - Number(prev.c) : 0;
    var pct = prev && Number(prev.c) ? (chg / Number(prev.c)) * 100 : 0;
    var sign = chg > 0 ? '+' : chg < 0 ? '−' : '';
    var html = '<span>O</span><b>' + price(bar.o, true) + '</b><span>H</span><b>' + price(bar.h, true) + '</b><span>L</span><b>' + price(bar.l, true) + '</b><span>C</span><b>' + price(bar.c, true) + '</b>'
      + (prev ? '<em>' + sign + price(Math.abs(chg)) + ' (' + sign + Math.abs(pct).toFixed(2) + '%)</em>' : '');
    if (el.dataset.html !== html) { el.innerHTML = html; el.dataset.html = html; }
    el.classList.toggle('is-up', up);
    el.classList.toggle('is-down', !up);
  }

  // Lay the ticket, the O H L C and the trade lines over the chart where its
  // prices are now.
  function hudPlace() {
    var hud = $('cf-scalp-dock-hud');
    if (!hud) return;
    var c = hudCanvas();
    if (!c) { hud.hidden = true; return; }
    hud.hidden = false;
    syncTicket();
    var p = c.projection;
    var box = hud.getBoundingClientRect(), hostBox = c.host.getBoundingClientRect();
    var ox = hostBox.left - box.left, oy = hostBox.top - box.top;
    var ticket = $('cf-desk-ticket');
    var zoom = c.host.querySelector('[data-bench-zoom]');
    var zb = zoom && zoom.getClientRects().length ? zoom.getBoundingClientRect() : null;
    var rowTop = zb ? zb.top - box.top : oy + Math.max(4, Math.round((p.padT - 27) / 2));
    var leftEdge = ox + 8;
    var rightEdge = zb ? zb.left - box.left - 8 : ox + p.padL + p.plotW - 8;
    var dropped = false;
    // A phone: the plot is too narrow for the ticket, so it takes the chart's
    // own top row from its left edge, compact, and the O H L C drops into the
    // plot as a legend.
    var narrow = hostBox.width < 600;
    if (ticket && narrow) {
      ticket.classList.add('is-compact');
      ticket.style.left = Math.round(leftEdge) + 'px';
      ticket.style.top = Math.round(oy + Math.max(4, Math.round((p.padT - 27) / 2))) + 'px';
      dropped = true;
    } else if (ticket) {
      ticket.classList.remove('is-compact');
      var minLeft = Math.max(leftEdge, ox + p.padL + 8);
      if (minLeft + ticket.offsetWidth > rightEdge) ticket.classList.add('is-compact');
      dropped = minLeft + ticket.offsetWidth > rightEdge;
      var left = Math.max(ox + p.padL + 8, (dropped ? ox + p.padL + p.plotW - 8 : rightEdge) - ticket.offsetWidth);
      ticket.style.left = Math.round(left) + 'px';
      var dropTop = Math.max(oy + p.padT + 6, zb ? zb.bottom - box.top + 6 : 0);
      ticket.style.top = Math.round(dropped ? dropTop : rowTop) + 'px';
    }
    var ohlc = $('cf-desk-ohlc');
    if (ohlc) {
      ohlcShow();
      ohlc.classList.remove('is-legend');
      var roomRight = ticket && !dropped ? parseFloat(ticket.style.left) - 8 : rightEdge;
      if (dropped || leftEdge + ohlc.offsetWidth > roomRight) {
        ohlc.classList.add('is-legend');
        ohlc.style.left = Math.round(ox + p.padL + 6) + 'px';
        var under = dropped && ticket && !narrow ? ticket.offsetTop + ticket.offsetHeight + 6 : oy + p.padT + 4;
        ohlc.style.top = Math.round(under) + 'px';
      } else {
        ohlc.style.left = Math.round(leftEdge) + 'px';
        ohlc.style.top = Math.round(rowTop) + 'px';
      }
    }
    var top = oy + p.padT, bottom = oy + p.padT + p.plotH;
    var legend = ohlc && ohlc.classList.contains('is-legend') ? ohlc.offsetTop + ohlc.offsetHeight + 16 : 0;
    var ceiling = Math.max(dropped && ticket && !narrow ? Math.max(top + 40, ticket.offsetTop + ticket.offsetHeight + 16) : top + 40, legend);
    var lastY = -Infinity;
    Array.prototype.map.call(document.querySelectorAll('#cf-desk-pills .cf-desk-pill-row'), function (row) {
      return { row: row, y: oy + p.yOf(Number(row.dataset.price)) };
    }).sort(function (a, b) { return a.y - b.y; }).forEach(function (item) {
      var at = Math.min(Math.max(item.y, ceiling), bottom - 12);
      if (at - lastY < 26) at = lastY + 26;
      lastY = at;
      item.row.classList.toggle('is-off', item.y < top || item.y > bottom);
      item.row.style.top = Math.round(at) + 'px';
      item.row.style.left = Math.round(ox + p.padL) + 'px';
      item.row.style.width = Math.round(p.plotW) + 'px';
    });
  }

  // ── PRICE ALARMS ─────────────────────────────────────────────
  // Right-click the chart: an alarm at that price, an amber bell line. The
  // server keeps them and rings each once (Telegram, and here a beep and a
  // banner); this page also rings the moment its own chart shows the level
  // reached, and tells the server so Telegram hears of it once.
  var RUNG_STORE = 'cf_scalp_alarms_rung_v1';
  var FRESH_S = 30;
  var alarms = [];
  var alarmFiring = {};
  var alarmGone = {};
  var pageFrom = Math.floor(Date.now() / 1000) - 5;

  function alarmsFor(key) { return alarms.filter(function (a) { return symKey(a.symbol) === key; }); }
  function rungList() { try { return JSON.parse(stored(RUNG_STORE) || '[]') || []; } catch (_) { return []; } }
  function markRung(id) { store(RUNG_STORE, JSON.stringify(rungList().filter(function (x) { return x !== id; }).concat([id]).slice(-50))); }

  function alarmsTake(d) {
    var before = JSON.stringify(alarms.map(function (a) { return a.id; }));
    alarms = (Array.isArray(d.alarms) ? d.alarms : []).filter(function (a) { return a && !alarmGone[a.id]; });
    var fired = (d.fired || []).filter(function (f) { return f && !alarmGone[f.id]; });
    alarmQuiet(function (id) { return !fired.some(function (f) { return f.id === id; }); });
    var rung = rungList(), now = Date.now() / 1000;
    fired.forEach(function (f) {
      if (rung.indexOf(f.id) >= 0 || Number(f.fired_at || 0) < pageFrom) return;
      markRung(f.id);
      ring(f, Number(f.hit) || Number(f.price), { silent: now - Number(f.fired_at || 0) > FRESH_S });
    });
    if (JSON.stringify(alarms.map(function (a) { return a.id; })) !== before) redecorate();
  }
  var syncTimer = null;
  async function alarmsSync() {
    clearTimeout(syncTimer);
    try {
      var r = await api('/api/scalp/alarms');
      if (r.ok) alarmsTake(await r.json());
    } catch (_) { /* offline for a moment: the next sync catches up */ }
    syncTimer = setTimeout(alarmsSync, alarms.length ? 2000 : 30000);
  }

  function alarmNow(data) {
    var p = livePrice(data && data.symbol);
    var candles = (data && data.candles) || [];
    return p > 0 ? p : (candles.length ? Number(candles[candles.length - 1].c) || 0 : 0);
  }
  function menuClose() { var m = $('cf-desk-alarm-menu'); if (m) m.remove(); }
  function alarmMenu(data, at) {
    menuClose();
    var key = drawKey(data);
    var raw = Number(at.price), mag = Math.abs(raw);
    var level = Number(raw.toFixed(mag >= 100 ? 2 : mag >= 1 ? 4 : 6));
    if (!(level > 0)) return;
    var menu = document.createElement('div');
    menu.id = 'cf-desk-alarm-menu';
    menu.className = 'cf-desk-alarm-menu';
    menu.setAttribute('role', 'menu');
    var item = function (text, fn, cls) {
      var b = document.createElement('button');
      b.type = 'button';
      b.className = 'cf-desk-alarm-item' + (cls ? ' ' + cls : '');
      b.setAttribute('role', 'menuitem');
      b.textContent = text;
      b.addEventListener('click', function () { menuClose(); fn(); });
      menu.appendChild(b);
    };
    item('\u{1F514}  Alarm at $' + price(level), function () { alarmAdd(data, level); }, 'is-add');
    var existing = alarmsFor(key);
    existing.forEach(function (a) { item('✕  Remove alarm $' + price(a.price), function () { alarmRemove([a.id]); }); });
    if (existing.length > 1) item('✕  Remove all alarms', function () { alarmRemove(existing.map(function (a) { return a.id; })); });
    document.body.appendChild(menu);
    var b = menu.getBoundingClientRect();
    menu.style.left = Math.max(8, Math.min(at.clientX, window.innerWidth - b.width - 8)) + 'px';
    menu.style.top = Math.max(8, Math.min(at.clientY, window.innerHeight - b.height - 8)) + 'px';
    var first = menu.querySelector('button');
    if (first) first.focus();
  }
  document.addEventListener('pointerdown', function (event) {
    var m = $('cf-desk-alarm-menu');
    if (m && !m.contains(event.target)) menuClose();
  }, true);
  document.addEventListener('keydown', function (event) { if (event.key === 'Escape') menuClose(); });
  window.addEventListener('wheel', menuClose, { passive: true });
  window.addEventListener('touchmove', menuClose, { passive: true });

  async function alarmAdd(data, level) {
    soundReady();
    try { if (window.Notification && Notification.permission === 'default') Notification.requestPermission(); } catch (_) {}
    try {
      var r = await api('/api/scalp/alarms', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ symbol: data.symbol, price: level, now: alarmNow(data) })
      });
      var d = await r.json().catch(function () { return {}; });
      if (!r.ok || d.status !== 'ok') throw new Error((typeof window.cfApiErrorDetail === 'function' && window.cfApiErrorDetail(d, '')) || 'Could not set the alarm');
      alarms = alarms.concat([d.alarm]);
      redecorate();
      alarmsSync();
      toast('Alarm set at $' + price(d.alarm.price), 'success');
    } catch (err) {
      toast((err && err.message) || 'Could not set the alarm', 'danger');
    }
  }
  async function alarmRemove(ids) {
    ids.forEach(function (id) { alarmGone[id] = true; });
    alarmQuiet(function (id) { return ids.indexOf(id) >= 0; });
    alarms = alarms.filter(function (a) { return ids.indexOf(a.id) < 0; });
    redecorate();
    await Promise.all(ids.map(function (id) { return api('/api/scalp/alarms/' + encodeURIComponent(id), { method: 'DELETE' }).catch(function () { return null; }); }));
    alarmsSync();
  }
  function alarmHit(a, p, candles) {
    var up = a.dir !== 'down';
    if (p > 0 && (up ? p >= a.price : p <= a.price)) return true;
    return (candles || []).some(function (b) {
      return Number(b.t) >= Number(a.at || 0) && (up ? Number(b.h) >= a.price : Number(b.l) <= a.price);
    });
  }
  function checkAlarms() {
    if (!alarms.length || !shown) return;
    var data = shown.data;
    var list = alarmsFor(drawKey(data)).filter(function (a) { return !alarmFiring[a.id]; });
    if (!list.length) return;
    var p = alarmNow(data), candles = (data.candles || []).slice(-3);
    list.filter(function (a) { return alarmHit(a, p, candles); }).forEach(async function (a) {
      alarmFiring[a.id] = true;
      try {
        var r = await api('/api/scalp/alarms/' + encodeURIComponent(a.id) + '/fire', {
          method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ price: p > 0 ? p : a.price })
        });
        var d = await r.json().catch(function () { return {}; });
        if (d.fired && !alarmGone[d.fired.id] && rungList().indexOf(d.fired.id) < 0) {
          markRung(d.fired.id);
          ring(d.fired, Number(d.fired.hit) || a.price);
        }
      } catch (_) { /* the server's own check still has it */ }
      delete alarmFiring[a.id];
      alarmsSync();
    });
  }

  var audio = null, ringing = null;
  function soundReady() {
    try {
      var Ctx = window.AudioContext || window.webkitAudioContext;
      if (!audio && Ctx) audio = new Ctx();
      if (audio && audio.state === 'suspended') audio.resume();
    } catch (_) { audio = null; }
  }
  function beeps() {
    if (!audio) return;
    if (audio.state !== 'running') { try { audio.resume(); } catch (_) {} return; }
    [0, 0.18, 0.36].forEach(function (offset) {
      var osc = audio.createOscillator(), gain = audio.createGain();
      osc.type = 'square';
      osc.frequency.value = 1046;
      gain.gain.setValueAtTime(0.0001, audio.currentTime + offset);
      gain.gain.exponentialRampToValueAtTime(0.18, audio.currentTime + offset + 0.01);
      gain.gain.exponentialRampToValueAtTime(0.0001, audio.currentTime + offset + 0.12);
      osc.connect(gain).connect(audio.destination);
      osc.start(audio.currentTime + offset);
      osc.stop(audio.currentTime + offset + 0.13);
    });
  }
  function silence() { if (ringing) { clearInterval(ringing); ringing = null; } }
  function alarmQuiet(gone) {
    var stack = $('cf-desk-alarm-stack');
    if (!stack) return;
    stack.querySelectorAll('.cf-desk-alarm[data-alarm-id]').forEach(function (card) { if (gone(card.dataset.alarmId)) card.remove(); });
    if (!stack.querySelector('.cf-desk-alarm.is-ringing')) silence();
  }
  function ring(a, p, opts) {
    opts = opts || {};
    var text = a.symbol + ' ' + (a.dir === 'down' ? 'fell to' : 'rose to') + ' $' + price(a.price);
    var stack = $('cf-desk-alarm-stack');
    if (!stack) {
      stack = document.createElement('div');
      stack.id = 'cf-desk-alarm-stack';
      stack.className = 'cf-desk-alarm-stack';
      stack.setAttribute('role', 'alert');
      document.body.appendChild(stack);
    }
    var card = document.createElement('div');
    card.className = opts.silent ? 'cf-desk-alarm' : 'cf-desk-alarm is-ringing';
    card.dataset.alarmId = a.id || '';
    var at = new Date(Number(a.fired_at) ? Number(a.fired_at) * 1000 : Date.now()).toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false, timeZone: 'Asia/Kolkata' });
    card.innerHTML = '<span class="cf-desk-alarm-bell" aria-hidden="true">\u{1F514}</span><div class="cf-desk-alarm-text"><strong></strong><span></span></div><button type="button" class="cf-desk-alarm-dismiss">Dismiss</button>';
    card.querySelector('strong').textContent = text;
    card.querySelector('.cf-desk-alarm-text span').textContent = 'Now $' + price(p) + ' · ' + at + ' IST';
    card.querySelector('.cf-desk-alarm-dismiss').addEventListener('click', function () {
      card.remove();
      if (!stack.querySelector('.cf-desk-alarm.is-ringing')) silence();
      if (a.id) {
        alarmGone[a.id] = true;
        api('/api/scalp/alarms/' + encodeURIComponent(a.id), { method: 'DELETE' }).catch(function () { return null; });
      }
    });
    stack.appendChild(card);
    if (opts.silent) return;
    silence();
    stack.querySelectorAll('.cf-desk-alarm.is-ringing').forEach(function (other) { if (other !== card) other.classList.remove('is-ringing'); });
    soundReady();
    beeps();
    var started = Date.now(), rings = 1;
    ringing = setInterval(function () {
      card.classList.toggle('is-ringing', ++rings <= 10 && Date.now() - started < 11000 && card.isConnected);
      if (!card.classList.contains('is-ringing')) { silence(); return; }
      beeps();
    }, 1000);
    try {
      if (document.hidden && window.Notification && Notification.permission === 'granted') {
        new Notification('CryptoForge alarm', { body: text, tag: 'cf-scalp-alarm-' + a.id });
      }
    } catch (_) { /* notifications unavailable here */ }
  }

  // ── wiring ───────────────────────────────────────────────────
  function bind() {
    var dock = $('cf-scalp-dock');
    if (!dock || dock.dataset.bound) return;
    dock.dataset.bound = '1';
    dock.addEventListener('click', function (event) {
      var t = event.target.closest('button, [data-desk-unit]');
      if (!t || !dock.contains(t)) return;
      if (t.id === 'cf-desk-ticket-buy') ticketTrade('BUY');
      else if (t.id === 'cf-desk-ticket-sell') ticketTrade('SELL');
      else if (t.id === 'cf-desk-ticket-more') {
        var row = $('cf-desk-ticket-row'), open = t.getAttribute('aria-expanded') !== 'true';
        if (row) row.hidden = !open;
        t.setAttribute('aria-expanded', String(open));
        hudPlace();
      } else if (t.dataset.deskStep) stepSize(Number(t.dataset.deskStep));
      else if (t.dataset.deskMode && typeof cfSetScalpExecutionMode === 'function') { cfSetScalpExecutionMode(t.dataset.deskMode); syncTicket(); }
      else if (t.id === 'cf-desk-ticket-unit') {
        var sel = $('cf-scalp-qty-mode');
        if (sel) {
          sel.value = qtyMode() === 'base' ? 'usdt' : 'base';
          sel.dispatchEvent(new Event('change', { bubbles: true }));
          if (typeof cfUpdateScalpQtyUi === 'function') cfUpdateScalpQtyUi();
        }
        syncTicket();
      } else if (t.dataset.deskExit) {
        var id = t.dataset.deskExit;
        var trade = openTrades().find(function (x) { return String(x.trade_id) === id; });
        if (typeof cfExitScalpTrade === 'function') cfExitScalpTrade(trade ? trade.trade_id : id);
      }
    });
    var size = $('cf-desk-ticket-size');
    if (size) {
      size.addEventListener('change', function () { ticketSetSize(size.value); });
      size.addEventListener('keydown', function (event) { if (event.key === 'Enter') { ticketSetSize(size.value); size.blur(); } });
    }
    var symbol = $('cf-scalp-symbol');
    if (symbol) symbol.addEventListener('change', function () { if (shown) rememberSpan(); setTimeout(function () { load(); }, 0); });
  }

  function start() {
    bind();
    setInterval(function () { tick(); checkAlarms(); }, 1000);
    setTimeout(alarmsSync, 1500);
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start);
  else start();

  // For the tests and the console.
  window.cfScalpDesk = {
    load: load,
    toggleFull: toggleFull,
    supertrendRuns: supertrendRuns,
    rsiSeries: rsiSeries,
    symKey: symKey,
    applyLiveBar: applyLiveBar,
    state: function () { return { tf: tf, shown: shown, alarms: alarms.slice() }; }
  };
})();
