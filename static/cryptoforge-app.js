// ── State ──────────────────────────────────────────────────
let selectedCrypto = 'BTCUSDT';
let selectedSide = 'LONG';
let selectedLeverage = 10;
let indicators = [];
let entryConditions = [];
let exitConditions = [];
let currentRunId = null;
let leverageOptions = [1, 2, 3, 5, 10, 20, 50, 100];
let _currentTrades = [];       // for sorting/pagination
let _currentRunSymbol = '';    // symbol for current run detail
let _tradesSortCol = 'id';
let _tradesSortAsc = true;
let _tradesPage = 1;
let _paperLogs = [];
let _liveLogs = [];


const _CF_LEGACY_ATTRS = {
  click: 'data-cf-click',
  change: 'data-cf-change',
  keydown: 'data-cf-keydown',
  mouseover: 'data-cf-mouseover',
  mouseout: 'data-cf-mouseout',
  error: 'data-cf-error',
};

function _cfSplitTopLevel(input, delimiter) {
  const parts = [];
  let current = '';
  let depthParen = 0;
  let depthBrace = 0;
  let quote = '';
  for (let i = 0; i < input.length; i++) {
    const ch = input[i];
    const prev = input[i - 1];
    if (quote) {
      current += ch;
      if (ch === quote && prev !== '\\') quote = '';
      continue;
    }
    if (ch === '"' || ch === "'") {
      quote = ch;
      current += ch;
      continue;
    }
    if (ch === '(') depthParen++;
    else if (ch === ')') depthParen = Math.max(0, depthParen - 1);
    else if (ch === '{') depthBrace++;
    else if (ch === '}') depthBrace = Math.max(0, depthBrace - 1);
    if (ch === delimiter && depthParen === 0 && depthBrace === 0) {
      parts.push(current.trim());
      current = '';
      continue;
    }
    current += ch;
  }
  if (current.trim()) parts.push(current.trim());
  return parts;
}

function _cfUnquote(value) {
  const raw = (value || '').trim();
  if (!raw) return '';
  if ((raw.startsWith("'") && raw.endsWith("'")) || (raw.startsWith('"') && raw.endsWith('"'))) {
    return raw.slice(1, -1).replace(/\\'/g, "'").replace(/\\"/g, '"');
  }
  return raw;
}

function _cfResolveValue(token, el, event) {
  const raw = (token || '').trim();
  if (!raw) return undefined;
  if (raw === 'this') return el;
  if (raw === 'event') return event;
  if (raw === 'this.value') return el && 'value' in el ? el.value : undefined;
  if (raw === 'this.checked') return !!(el && el.checked);
  if (raw === 'this.dataset.val') return el && el.dataset ? el.dataset.val : undefined;
  if (raw === 'null') return null;
  if (raw === 'true') return true;
  if (raw === 'false') return false;
  if (/^-?\d+(?:\.\d+)?$/.test(raw)) return Number(raw);
  let match = raw.match(/^document\.getElementById\((['"])(.+?)\1\)$/);
  if (match) return document.getElementById(match[2]);
  if ((raw.startsWith("'") && raw.endsWith("'")) || (raw.startsWith('"') && raw.endsWith('"'))) {
    return _cfUnquote(raw);
  }
  return raw;
}

function _cfResolveAssignmentValue(expr, el, event) {
  const raw = (expr || '').trim();
  const ternary = raw.match(/^this\.value===['"]([^'"]+)['"]\?['"]([^'"]*)['"]:['"]([^'"]*)['"]$/);
  if (ternary) return (el && el.value === ternary[1]) ? ternary[2] : ternary[3];
  return _cfResolveValue(raw, el, event);
}

function _cfInvokeNamedFunction(name, args) {
  const target = window[name];
  if (typeof target === 'function') {
    return target.apply(window, args);
  }
  return undefined;
}

function _cfRunLegacyScript(script, el, event) {
  _cfSplitTopLevel(script || '', ';').forEach(function(statement) {
    const stmt = (statement || '').trim().replace(/;$/, '');
    if (!stmt) return;

    let match = stmt.match(/^if\((.+)\)\{?(.*)\}?$/);
    if (match) {
      const cond = match[1].trim();
      const body = match[2].trim();
      let ok = false;
      if (cond === 'event.target===this') ok = event && event.target === el;
      else {
        const keyMatch = cond.match(/^event\.key===['"]([^'"]+)['"]$/);
        if (keyMatch) ok = !!event && event.key === keyMatch[1];
      }
      if (ok && body) _cfRunLegacyScript(body, el, event);
      return;
    }

    if (stmt === 'event.stopPropagation()') {
      if (event && typeof event.stopPropagation === 'function') event.stopPropagation();
      return;
    }

    match = stmt.match(/^this\.style\.([A-Za-z0-9_$-]+)\s*=\s*(['"])(.*?)\2$/);
    if (match) {
      el.style[match[1]] = match[3];
      return;
    }

    match = stmt.match(/^document\.getElementById\((['"])(.+?)\1\)\.([A-Za-z0-9_$]+)\s*=\s*(.+)$/);
    if (match) {
      const target = document.getElementById(match[2]);
      if (target) target[match[3]] = _cfResolveAssignmentValue(match[4], el, event);
      return;
    }

    match = stmt.match(/^var a=\(['"](entry|exit)['"]===['"]entry['"]\?entryConditions:exitConditions\);a\[(\d+)\]\.right_time=this\.value$/);
    if (match) {
      const arr = match[1] === 'entry' ? window.entryConditions : window.exitConditions;
      const idx = Number(match[2]);
      if (Array.isArray(arr) && arr[idx]) arr[idx].right_time = el.value;
      return;
    }

    match = stmt.match(/^([A-Za-z0-9_$]+)\((.*)\)$/);
    if (match) {
      const args = _cfSplitTopLevel(match[2], ',').map(function(part) {
        return _cfResolveValue(part, el, event);
      });
      _cfInvokeNamedFunction(match[1], args);
    }
  });
}

function _cfFindLegacyTarget(event, attrName) {
  if (!event || !event.target) return null;
  if (typeof event.target.closest === 'function') {
    return event.target.closest('[' + attrName + ']');
  }
  return event.target && event.target.hasAttribute && event.target.hasAttribute(attrName) ? event.target : null;
}

function _cfBindLegacyAttrBridge() {
  document.addEventListener('click', function(event) {
    const el = _cfFindLegacyTarget(event, _CF_LEGACY_ATTRS.click);
    if (el) _cfRunLegacyScript(el.getAttribute(_CF_LEGACY_ATTRS.click), el, event);
  });
  document.addEventListener('change', function(event) {
    const el = _cfFindLegacyTarget(event, _CF_LEGACY_ATTRS.change);
    if (el) _cfRunLegacyScript(el.getAttribute(_CF_LEGACY_ATTRS.change), el, event);
  });
  document.addEventListener('keydown', function(event) {
    const el = _cfFindLegacyTarget(event, _CF_LEGACY_ATTRS.keydown);
    if (el) _cfRunLegacyScript(el.getAttribute(_CF_LEGACY_ATTRS.keydown), el, event);
  });
  document.addEventListener('mouseover', function(event) {
    const el = _cfFindLegacyTarget(event, _CF_LEGACY_ATTRS.mouseover);
    if (el) _cfRunLegacyScript(el.getAttribute(_CF_LEGACY_ATTRS.mouseover), el, event);
  });
  document.addEventListener('mouseout', function(event) {
    const el = _cfFindLegacyTarget(event, _CF_LEGACY_ATTRS.mouseout);
    if (el) _cfRunLegacyScript(el.getAttribute(_CF_LEGACY_ATTRS.mouseout), el, event);
  });
  window.addEventListener('error', function(event) {
    const target = event && event.target;
    if (target && target.getAttribute) {
      const script = target.getAttribute(_CF_LEGACY_ATTRS.error);
      if (script) _cfRunLegacyScript(script, target, event);
    }
  }, true);
}

_cfBindLegacyAttrBridge();


const CF_APPEARANCE_TINT_LABELS = {
  aqua: 'Aqua Terminal',
  gold: 'Gold Desk',
  emerald: 'Emerald Grid',
  ruby: 'Ruby Risk',
  violet: 'Violet Pulse'
};
const CF_APPEARANCE_FONT_LABELS = {
  terminal: 'Terminal Pro',
  institutional: 'Institutional',
  modern: 'Modern Desk',
  quant: 'Quant Lab',
  editorial: 'Premium Serif'
};

function cfCurrentAppearance() {
  if (typeof window.cfGetAppearance === 'function') return window.cfGetAppearance();
  return { tint: 'aqua', font: 'terminal' };
}

function cfSyncAppearancePanel() {
  const state = cfCurrentAppearance();
  document.querySelectorAll('[data-appearance-tint]').forEach(function(btn) {
    const active = btn.getAttribute('data-appearance-tint') === state.tint;
    btn.classList.toggle('active', active);
    btn.setAttribute('aria-pressed', active ? 'true' : 'false');
  });
  document.querySelectorAll('[data-appearance-font]').forEach(function(btn) {
    const active = btn.getAttribute('data-appearance-font') === state.font;
    btn.classList.toggle('active', active);
    btn.setAttribute('aria-pressed', active ? 'true' : 'false');
  });
}

function cfOpenAppearancePanel() {
  const modal = document.getElementById('appearance-modal');
  if (!modal) return;
  cfSyncAppearancePanel();
  modal.hidden = false;
  modal.classList.add('open');
  document.body.classList.add('appearance-open');
}

function cfCloseAppearancePanel() {
  const modal = document.getElementById('appearance-modal');
  if (!modal) return;
  modal.classList.remove('open');
  modal.hidden = true;
  document.body.classList.remove('appearance-open');
}

function cfSetAppearanceTint(tint) {
  if (typeof window.cfApplyAppearance === 'function') {
    window.cfApplyAppearance({ tint: tint }, { persist: true });
  }
  cfSyncAppearancePanel();
  cfToast('Tint changed to ' + (CF_APPEARANCE_TINT_LABELS[tint] || tint), 'success');
}

function cfSetAppearanceFont(font) {
  if (typeof window.cfApplyAppearance === 'function') {
    window.cfApplyAppearance({ font: font }, { persist: true });
  }
  cfSyncAppearancePanel();
  cfToast('Font changed to ' + (CF_APPEARANCE_FONT_LABELS[font] || font), 'success');
}

function cfResetAppearance() {
  if (typeof window.cfApplyAppearance === 'function') {
    window.cfApplyAppearance({ tint: 'aqua', font: 'terminal' }, { persist: true });
  }
  cfSyncAppearancePanel();
  cfToast('Appearance reset', 'info');
}

const TOP_25 = [
  {symbol:"BTCUSDT",name:"Bitcoin",ticker:"BTC",icon:"₿"},
  {symbol:"ETHUSDT",name:"Ethereum",ticker:"ETH",icon:"Ξ"},
  {symbol:"SOLUSDT",name:"Solana",ticker:"SOL",icon:"◎"},
  {symbol:"XRPUSDT",name:"Ripple",ticker:"XRP",icon:"✕"},
  {symbol:"DOGEUSDT",name:"Dogecoin",ticker:"DOGE",icon:"Ð"},
  {symbol:"PAXGUSD",name:"PAXGUSD",ticker:"PAXGUSD",icon:"Au"},
];

// ── Core Assets — BTC / ETH / SOL (primary three) ─────────
const CORE_ASSETS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'];

function cfInputNumber(id, fallback) {
  const el = document.getElementById(id);
  const value = parseFloat(el ? el.value : '');
  return isNaN(value) ? fallback : value;
}

function cfBuilderCostModel() {
  return {
    fee_pct: cfInputNumber('b-fee', 0),
    spread_bps: cfInputNumber('b-spread', 0),
    slippage_bps: cfInputNumber('b-slippage', 0),
    funding_bps_per_8h: cfInputNumber('b-funding', 0),
  };
}

function cfApplyBuilderCostModel(data) {
  data = data || {};
  document.getElementById('b-fee').value = data.fee_pct != null ? data.fee_pct : 0;
  document.getElementById('b-spread').value = data.spread_bps != null ? data.spread_bps : 0;
  document.getElementById('b-slippage').value = data.slippage_bps != null ? data.slippage_bps : 0;
  document.getElementById('b-funding').value = data.funding_bps_per_8h != null ? data.funding_bps_per_8h : 0;
}

function cfCostModelSummary(data) {
  data = data || {};
  return 'Fee ' + (data.fee_pct || 0) + '% • Spread ' + (data.spread_bps || 0) + ' bps • Slip ' + (data.slippage_bps || 0) + ' bps • Funding ' + (data.funding_bps_per_8h || 0) + ' bps/8h';
}

function getBuilderPayload() {
  var positionSizeMode = (document.getElementById('b-possize-mode') || {}).value || 'pct';
  var positionSizeValue = cfInputNumber('b-possize', 0);
  return {
    run_name: (document.getElementById('b-name') || {}).value || 'Untitled',
    symbol: selectedCrypto,
    leverage: selectedLeverage,
    trade_side: selectedSide,
    initial_capital: parseFloat((document.getElementById('b-capital') || {}).value) || 10000,
    position_size_mode: positionSizeMode,
    position_size_value: positionSizeValue,
    position_size_pct: positionSizeMode === 'pct' ? positionSizeValue : 100,
    fixed_qty: positionSizeMode === 'fixed_qty' ? positionSizeValue : 0,
    stoploss_pct: parseFloat((document.getElementById('b-sl') || {}).value) || 5,
    target_profit_pct: parseFloat((document.getElementById('b-tp') || {}).value) || 10,
    trailing_sl_pct: parseFloat((document.getElementById('b-trail') || {}).value) || 0,
    max_trades_per_day: parseInt((document.getElementById('b-maxtrades') || {}).value) || 5,
    candle_interval: (document.getElementById('b-interval') || {}).value || '5m',
    from_date: (document.getElementById('b-from') || {}).value || '',
    to_date: (document.getElementById('b-to') || {}).value || '',
    indicators: indicators.slice(),
    entry_conditions: entryConditions.slice(),
    exit_conditions: exitConditions.slice(),
    ...cfBuilderCostModel(),
  };
}

function computeStrategyIntel(payload) {
  payload = payload || getBuilderPayload();
  var indicatorCount = (payload.indicators || []).length;
  var entryCount = (payload.entry_conditions || []).length;
  var exitCount = (payload.exit_conditions || []).length;
  var riskMode = payload.position_size_mode === 'fixed_qty' ? 'Fixed Qty' : '% Capital';
  var from = payload.from_date || 'start';
  var to = payload.to_date || 'now';
  var liveReady = indicatorCount > 0 && entryCount > 0 && exitCount > 0;
  var riskText = payload.leverage + 'x • SL ' + payload.stoploss_pct + '% • TP ' + payload.target_profit_pct + '%';
  if (payload.trailing_sl_pct > 0) riskText += ' • Trail ' + payload.trailing_sl_pct + '%';
  var coverage = [];
  if (!indicatorCount) coverage.push('add indicators');
  if (!entryCount) coverage.push('add entry logic');
  if (!exitCount) coverage.push('add exit logic');
  return {
    title: payload.run_name || 'Untitled',
    lane: payload.trade_side + ' • ' + payload.symbol,
    riskText: riskText,
    signalText: indicatorCount + ' indicators • ' + entryCount + ' entry • ' + exitCount + ' exit',
    backtestWindow: from + ' → ' + to + ' • ' + payload.candle_interval,
    deploymentLane: liveReady ? 'Validated Candidate' : 'Paper First',
    deploymentCopy: liveReady
      ? 'Runtime shape is complete. Use backtests and paper fills to verify behavior before any live capital.'
      : 'Builder is still incomplete. Finish the signal stack and validate in paper mode first.',
    controlCopy: 'Capital ' + fmtINR(payload.initial_capital) + ' • ' + riskMode + ' • Max ' + payload.max_trades_per_day + ' trades/day',
    intel: [
      'Coverage: ' + (coverage.length ? coverage.join(', ') : 'signal stack configured'),
      'Costs: ' + cfCostModelSummary(payload),
      'Sizing: ' + riskMode + ' @ ' + payload.position_size_value,
    ],
    liveReady: liveReady,
  };
}

function renderBuilderDeck() {
  var intel = computeStrategyIntel();
  var titleEl = document.getElementById('builder-brief-title');
  var copyEl = document.getElementById('builder-brief-copy');
  var metricsEl = document.getElementById('builder-brief-metrics');
  var intelEl = document.getElementById('builder-intel-list');
  var safetyEl = document.getElementById('builder-safety-list');
  if (titleEl) titleEl.textContent = intel.title + ' is staged for ' + intel.lane + '.';
  if (copyEl) copyEl.textContent = intel.deploymentCopy;
  if (metricsEl) {
    metricsEl.innerHTML =
      '<div class="brief-metric"><div class="brief-metric-label">Trade Lane</div><div class="brief-metric-value">' + _escapeHtml(intel.lane) + '</div></div>' +
      '<div class="brief-metric"><div class="brief-metric-label">Risk Package</div><div class="brief-metric-value">' + _escapeHtml(intel.riskText) + '</div></div>' +
      '<div class="brief-metric"><div class="brief-metric-label">Signal Stack</div><div class="brief-metric-value">' + _escapeHtml(intel.signalText) + '</div></div>' +
      '<div class="brief-metric"><div class="brief-metric-label">Backtest Window</div><div class="brief-metric-value">' + _escapeHtml(intel.backtestWindow) + '</div></div>';
  }
  if (intelEl) {
    intelEl.innerHTML = intel.intel.map(function(line) {
      var parts = line.split(': ');
      return '<div class="intel-pill"><strong>' + _escapeHtml(parts[0] || 'Intel') + '</strong>' + (parts[1] ? ' ' + _escapeHtml(parts[1]) : '') + '</div>';
    }).join('');
  }
  if (safetyEl) {
    safetyEl.innerHTML =
      '<div class="safety-chip"><strong>1</strong> ' + _escapeHtml(intel.liveReady ? 'Runtime shape is complete' : 'Finish indicator and condition coverage') + '</div>' +
      '<div class="safety-chip"><strong>2</strong> ' + _escapeHtml('Review ' + intel.riskText) + '</div>' +
      '<div class="safety-chip"><strong>3</strong> ' + _escapeHtml(intel.liveReady ? 'Backtest and paper-validate before live' : 'Backtest only after the brief is complete') + '</div>';
  }
}

function renderDashboardMission(summary, runs) {
  summary = summary || {};
  runs = runs || [];
  var totalEngines = (summary.paper_count || 0) + (summary.live_count || 0);
  var hasLive = !!summary.live_running;
  var hasPaper = !!summary.paper_running;
  var brokerLabel = _brokerLabel();
  var latestRun = runs[0] || null;
  var title = 'Crypto desk is standing by.';
  var copy = 'Backtests, paper engines, live monitoring, and broker connectivity are consolidated into a single operating surface. Start with a controlled paper run, validate regime fit, then graduate only verified setups to live.';
  if (hasLive) {
    title = 'Live capital is armed across ' + totalEngines + ' engine' + (totalEngines === 1 ? '' : 's') + '.';
    copy = 'Execution requires tighter discipline now. Monitor broker health, keep risk controls visible, and use the Live workspace for intervention rather than editing strategy state in-flight.';
  } else if (hasPaper) {
    title = 'Paper validation is active across ' + (summary.paper_count || totalEngines) + ' engine' + ((summary.paper_count || totalEngines) === 1 ? '' : 's') + '.';
    copy = 'Use the paper lane to verify fills, signal cadence, and risk packaging before promoting any strategy to live deployment.';
  } else if (latestRun) {
    title = 'Latest validation: ' + (latestRun.run_name || ('Run #' + latestRun.id));
    copy = 'Runbook history is available for replay. Review the latest archive, compare outcomes, and tighten the next deployment package before arming the broker.';
  }
  var pills = [
    '<div class="mission-pill"><strong>Readiness</strong> ' + _escapeHtml(hasLive ? 'Live armed' : hasPaper ? 'Paper active' : 'No engines running') + '</div>',
    '<div class="mission-pill"><strong>Broker</strong> ' + _escapeHtml(_brokerConnected ? 'Connected' : 'Connection pending') + '</div>',
    '<div class="mission-pill"><strong>Pipeline</strong> ' + _escapeHtml((summary.backtest_count || 0) + ' archived run' + ((summary.backtest_count || 0) === 1 ? '' : 's')) + '</div>',
  ];
  var titleEl = document.getElementById('dash-mission-title');
  var copyEl = document.getElementById('dash-mission-copy');
  var pillsEl = document.getElementById('dash-mission-pills');
  var laneEl = document.getElementById('dash-deploy-lane');
  var laneCopyEl = document.getElementById('dash-deploy-copy');
  var controlValueEl = document.getElementById('dash-control-value');
  var controlCopyEl = document.getElementById('dash-control-copy');
  if (titleEl) titleEl.textContent = title;
  if (copyEl) copyEl.textContent = copy;
  if (pillsEl) pillsEl.innerHTML = pills.join('');
  if (laneEl) laneEl.textContent = hasLive ? 'Live Guarded' : hasPaper ? 'Paper Verification' : 'Paper First';
  if (laneCopyEl) laneCopyEl.textContent = hasLive
    ? ('Real ' + brokerLabel + ' orders are enabled. Operate from monitoring and risk control surfaces.')
    : hasPaper
      ? 'Paper execution is available for behavior validation before live cutover.'
      : 'Start with paper mode to verify execution flow, fills, and risk behavior before enabling live capital.';
  if (controlValueEl) controlValueEl.textContent = totalEngines + ' Engine' + (totalEngines === 1 ? '' : 's');
  if (controlCopyEl) controlCopyEl.textContent = totalEngines
    ? ((summary.paper_count || 0) + ' paper • ' + (summary.live_count || 0) + ' live monitored from one command rail.')
    : 'No active paper or live engines are currently driving orders.';
}

function renderResultsOverview(runs) {
  runs = runs || [];
  var titleEl = document.getElementById('results-overview-title');
  var gridEl = document.getElementById('results-overview-grid');
  if (!titleEl || !gridEl) return;
  if (!runs.length) {
    titleEl.textContent = 'Result archive is waiting for fresh validation.';
    gridEl.innerHTML =
      '<div class="overview-card"><div class="overview-label">Archive</div><div class="overview-value">0 runs</div></div>' +
      '<div class="overview-card"><div class="overview-label">Best P&L</div><div class="overview-value">$0.00</div></div>' +
      '<div class="overview-card"><div class="overview-label">Active Filter</div><div class="overview-value">' + _escapeHtml(_currentRunFilter) + '</div></div>' +
      '<div class="overview-card"><div class="overview-label">Latest Mode</div><div class="overview-value">None</div></div>';
    return;
  }
  var best = runs.reduce(function(acc, run) {
    var pnl = parseFloat(run.total_pnl) || 0;
    return pnl > acc.pnl ? { pnl: pnl, run: run } : acc;
  }, { pnl: -Infinity, run: null });
  var latest = runs[0];
  var positive = runs.filter(function(run) { return (parseFloat(run.total_pnl) || 0) >= 0; }).length;
  titleEl.textContent = 'Archive contains ' + runs.length + ' runs across backtest, paper, live, and scalp workflows.';
  gridEl.innerHTML =
    '<div class="overview-card"><div class="overview-label">Archive</div><div class="overview-value">' + runs.length + ' runs</div></div>' +
    '<div class="overview-card"><div class="overview-label">Best P&L</div><div class="overview-value" style="color:' + (best.pnl >= 0 ? 'var(--green)' : 'var(--red)') + ';">' + fmtINR(best.pnl) + '</div></div>' +
    '<div class="overview-card"><div class="overview-label">Active Filter</div><div class="overview-value">' + _escapeHtml(_currentRunFilter.toUpperCase()) + '</div></div>' +
    '<div class="overview-card"><div class="overview-label">Latest Mode</div><div class="overview-value">' + _escapeHtml((_normalizeMode(latest.mode) || 'none').toUpperCase()) + '</div></div>' +
    '<div class="overview-card"><div class="overview-label">Positive Runs</div><div class="overview-value">' + positive + ' / ' + runs.length + '</div></div>' +
    '<div class="overview-card"><div class="overview-label">Latest Run</div><div class="overview-value">' + _escapeHtml(latest.run_name || ('Run #' + latest.id)) + '</div></div>';
}

function resetDeployChecks() {
  ['deploy-check-runtime', 'deploy-check-risk', 'deploy-check-live'].forEach(function(id) {
    var el = document.getElementById(id);
    if (el) el.checked = false;
  });
}

function updateDeployModalState() {
  var intel = computeStrategyIntel();
  var liveRow = document.getElementById('deploy-check-live-row');
  var isLive = _deployType === 'live';
  if (liveRow) liveRow.style.display = isLive ? 'flex' : 'none';
  var nameEl = document.getElementById('deploy-summary-name');
  var symbolEl = document.getElementById('deploy-summary-symbol');
  var riskEl = document.getElementById('deploy-summary-risk');
  var logicEl = document.getElementById('deploy-summary-logic');
  if (nameEl) nameEl.textContent = intel.title;
  if (symbolEl) symbolEl.textContent = intel.lane + ' • ' + intel.backtestWindow;
  if (riskEl) riskEl.textContent = intel.riskText;
  if (logicEl) logicEl.textContent = intel.signalText;
  var canConfirm = !!(document.getElementById('deploy-check-runtime') || {}).checked
    && !!(document.getElementById('deploy-check-risk') || {}).checked
    && (!isLive || !!(document.getElementById('deploy-check-live') || {}).checked);
  var btn = document.getElementById('deploy-confirm-btn');
  if (btn) btn.disabled = !canConfirm;
}

/**
 * Quick Asset Switcher: sets the active asset globally.
 * Updates the builder crypto selector AND navigates to the builder tab.
 */
function setQuickAsset(symbol, pillEl) {
  selectedCrypto = symbol;
  // Update pill UI
  document.querySelectorAll('.asset-pill').forEach(p => p.classList.remove('active'));
  if (pillEl) pillEl.classList.add('active');
  // Sync builder selector if it exists
  if (typeof initCryptoSelector === 'function') initCryptoSelector();
  if (typeof fetchLeverage === 'function') fetchLeverage(symbol);
  // Navigate to builder
  var builderNav = document.getElementById('nav-builder');
  showPage('builder-page', builderNav);
  // Toast feedback
  var coin = TOP_25.find(c => c.symbol === symbol);
  if (coin && typeof cfToast === 'function') {
    cfToast(coin.icon + ' ' + coin.name + ' selected', 'info');
  }
}

// ── Custom Modal System ────────────────────────────────────
function cfModal(title, msg, icon, buttons) {
  return new Promise(resolve => {
    const overlay = document.createElement('div');
    overlay.className = 'cf-modal-overlay';

    const modal = document.createElement('div');
    modal.className = 'cf-modal';

    const iconEl = document.createElement('div');
    iconEl.className = 'cf-modal-icon';
    iconEl.textContent = icon || '💎';           // textContent: never executes HTML

    const titleEl = document.createElement('div');
    titleEl.className = 'cf-modal-title';
    titleEl.textContent = title;                  // safe — no innerHTML

    const msgEl = document.createElement('div');
    msgEl.className = 'cf-modal-msg';
    if (msg instanceof Node) {
      msgEl.appendChild(msg);
    } else if (msg && typeof msg === 'object' && typeof msg.html === 'string') {
      const tpl = document.createElement('template');
      tpl.innerHTML = msg.html;
      msgEl.appendChild(tpl.content.cloneNode(true));
    } else {
      msgEl.textContent = msg == null ? '' : String(msg);
    }

    const actionsEl = document.createElement('div');
    actionsEl.className = 'cf-modal-actions';
    buttons.forEach(function(b, i) {
      const cls = b.cls || (i === buttons.length - 1 ? 'btn-primary' : 'btn-outline');
      const btn = document.createElement('button');
      btn.className = 'btn btn-sm ' + cls;
      btn.dataset.idx = i;
      btn.textContent = b.label;
      actionsEl.appendChild(btn);
    });

    modal.appendChild(iconEl);
    modal.appendChild(titleEl);
    modal.appendChild(msgEl);
    modal.appendChild(actionsEl);
    overlay.appendChild(modal);

    overlay.addEventListener('click', function(e) {
      var btn = e.target.closest('[data-idx]');
      if (btn) { overlay.remove(); resolve(parseInt(btn.dataset.idx)); }
      if (e.target === overlay) { overlay.remove(); resolve(-1); }
    });
    document.body.appendChild(overlay);
  });
}
async function cfAlert(msg, title, icon, allowHtml) {
  await cfModal(
    title || 'Notice',
    allowHtml ? { html: String(msg == null ? '' : msg) } : _escapeHtml(msg),
    icon || 'ℹ️',
    [{label:'OK', cls:'btn-primary'}]
  );
}
async function cfConfirm(msg, title, icon, allowHtml) {
  var r = await cfModal(
    title || 'Confirm',
    allowHtml ? { html: String(msg == null ? '' : msg) } : _escapeHtml(msg),
    icon || '⚠️',
    [{label:'Cancel', cls:'btn-outline'}, {label:'Continue', cls:'btn-danger'}]
  );
  return r === 1;
}
async function cfSuccess(msg, title, allowHtml) {
  await cfModal(
    title || 'Success',
    allowHtml ? { html: String(msg == null ? '' : msg) } : _escapeHtml(msg),
    '✅',
    [{label:'OK', cls:'btn-success'}]
  );
}

// ── Prompt Modal (replaces native prompt()) ────────────────
function cfPrompt(title, label, defaultVal) {
  return new Promise(resolve => {
    const overlay = document.createElement('div');
    overlay.className = 'cf-modal-overlay';

    const modal = document.createElement('div');
    modal.className = 'cf-modal';

    const iconEl = document.createElement('div');
    iconEl.className = 'cf-modal-icon';
    iconEl.textContent = '⚙️';

    const titleEl = document.createElement('div');
    titleEl.className = 'cf-modal-title';
    titleEl.textContent = title;

    const labelEl = document.createElement('div');
    labelEl.className = 'cf-modal-msg';
    labelEl.textContent = label;

    const input = document.createElement('input');
    input.type = 'text';
    input.className = 'cf-prompt-input';
    input.id = 'cf-prompt-val';
    input.value = defaultVal || '';
    input.autocomplete = 'off';

    const actionsEl = document.createElement('div');
    actionsEl.className = 'cf-modal-actions';
    actionsEl.innerHTML = '<button class="btn btn-sm btn-outline" data-act="cancel">Cancel</button>'
      + '<button class="btn btn-sm btn-primary" data-act="ok">OK</button>';

    modal.appendChild(iconEl);
    modal.appendChild(titleEl);
    modal.appendChild(labelEl);
    modal.appendChild(input);
    modal.appendChild(actionsEl);
    overlay.appendChild(modal);

    function finish(val) { overlay.remove(); resolve(val); }
    overlay.addEventListener('click', function(e) {
      var act = e.target.dataset.act;
      if (act === 'cancel') finish(null);
      if (act === 'ok') finish(document.getElementById('cf-prompt-val').value);
      if (e.target === overlay) finish(null);
    });
    overlay.addEventListener('keydown', function(e) {
      if (e.key === 'Enter') finish(document.getElementById('cf-prompt-val').value);
      if (e.key === 'Escape') finish(null);
    });
    document.body.appendChild(overlay);
    setTimeout(function() { document.getElementById('cf-prompt-val').focus(); }, 50);
  });
}

// ── Select Modal (dropdown instead of text input) ──────────
function cfSelect(title, label, options, defaultVal) {
  return new Promise(resolve => {
    const overlay = document.createElement('div');
    overlay.className = 'cf-modal-overlay';

    const modal = document.createElement('div');
    modal.className = 'cf-modal';

    const iconEl = document.createElement('div');
    iconEl.className = 'cf-modal-icon';
    iconEl.textContent = '⚙️';

    const titleEl = document.createElement('div');
    titleEl.className = 'cf-modal-title';
    titleEl.textContent = title;

    const labelEl = document.createElement('div');
    labelEl.className = 'cf-modal-msg';
    labelEl.textContent = label;

    const sel = document.createElement('select');
    sel.id = 'cf-select-val';
    sel.style.cssText = 'width:100%;padding:10px 12px;background:rgba(255,255,255,0.05);color:var(--text);border:1px solid var(--border);border-radius:8px;font-size:14px;font-family:Outfit,sans-serif;margin-bottom:16px;';
    options.forEach(function(o) {
      const opt = document.createElement('option');
      opt.value = o.value;            // value is data attribute — safe
      opt.textContent = o.label;      // textContent: option labels can't XSS
      if (o.value === defaultVal) opt.selected = true;
      sel.appendChild(opt);
    });

    const actionsEl = document.createElement('div');
    actionsEl.className = 'cf-modal-actions';
    actionsEl.innerHTML = '<button class="btn btn-sm btn-outline" data-act="cancel">Cancel</button>'
      + '<button class="btn btn-sm btn-primary" data-act="ok">OK</button>';

    modal.appendChild(iconEl);
    modal.appendChild(titleEl);
    modal.appendChild(labelEl);
    modal.appendChild(sel);
    modal.appendChild(actionsEl);
    overlay.appendChild(modal);

    function finish(val) { overlay.remove(); resolve(val); }
    overlay.addEventListener('click', function(e) {
      var act = e.target.dataset.act;
      if (act === 'cancel') finish(null);
      if (act === 'ok') finish(document.getElementById('cf-select-val').value);
      if (e.target === overlay) finish(null);
    });
    overlay.addEventListener('keydown', function(e) {
      if (e.key === 'Enter') finish(document.getElementById('cf-select-val').value);
      if (e.key === 'Escape') finish(null);
    });
    document.body.appendChild(overlay);
    setTimeout(function() { document.getElementById('cf-select-val').focus(); }, 50);
  });
}

// ── Toast Notifications ────────────────────────────────────
function cfToast(msg, type) {
  type = type || 'info';
  var icons = {success:'✅', error:'❌', warning:'⚠️', info:'ℹ️'};
  var container = document.getElementById('toast-container');
  if (!container) return;
  var toast = document.createElement('div');
  toast.className = 'cf-toast ' + type;

  var iconEl = document.createElement('span');
  iconEl.className = 'cf-toast-icon';
  iconEl.textContent = icons[type] || 'ℹ️';   // textContent: no HTML execution

  var msgEl = document.createElement('span');
  msgEl.className = 'cf-toast-msg';
  msgEl.textContent = msg;                      // safe — broker error strings can't XSS

  toast.appendChild(iconEl);
  toast.appendChild(msgEl);
  container.appendChild(toast);
  setTimeout(function() { if (toast.parentNode) toast.remove(); }, 4200);
}

// ── Session Expiry Interceptor ─────────────────────────────
(function() {
  const _fetch = window.fetch;
  let redirectingForSession = false;

  function requestPath(input) {
    var raw = typeof input === 'string' ? input : (input && input.url ? input.url : '');
    if (!raw) return '';
    try { return new URL(raw, window.location.origin).pathname; } catch (e) { return raw; }
  }

  function handleExpiredSession() {
    if (redirectingForSession) return;
    redirectingForSession = true;
    cfToast('Session expired - returning to unlock screen', 'warning');
    setTimeout(function() {
      if (window.location.pathname !== '/' || window.location.hash) window.location.assign('/');
      else window.location.reload();
    }, 900);
  }

  window.fetch = async function() {
    const resp = await _fetch.apply(this, arguments);
    var path = requestPath(arguments[0]);
    if (resp.status === 401 && path.startsWith('/api/') && !path.startsWith('/api/auth/')) {
      handleExpiredSession();
    }
    return resp;
  };
})();

// ── Page Navigation ────────────────────────────────────────
let _cfPageHistoryDepth = 0;

function cfUpdateAppNavControls() {
  var backBtn = document.getElementById('topbar-back-btn');
  if (backBtn) backBtn.disabled = _cfPageHistoryDepth <= 0 && window.history.length <= 1;
}

function cfAppBack() {
  if (_cfPageHistoryDepth > 0 || window.history.length > 1) {
    window.history.back();
    return;
  }
  showPage('dashboard-page', document.getElementById('nav-dashboard'), { replaceHistory: true, forceReload: true });
}

function cfAppRefresh() {
  window.location.reload();
}

function cfPageTabName(pageId) {
  return String(pageId || 'dashboard-page').replace(/-page$/, '');
}

function cfNavButtonForPage(pageId) {
  return document.getElementById('nav-' + cfPageTabName(pageId));
}

function cfPageIdFromLocation() {
  var hash = String(window.location.hash || '').replace(/^#/, '').trim().toLowerCase();
  if (!hash) return '';
  var pageId = hash + '-page';
  return document.getElementById(pageId) ? pageId : '';
}

function cfSyncPageHistory(pageId, options) {
  if (!window.history || typeof window.history.pushState !== 'function') return;
  var opts = options || {};
  if (opts.skipHistory) return;
  var url = new URL(window.location.href);
  url.hash = cfPageTabName(pageId);
  var state = { pageId: pageId, cfDepth: _cfPageHistoryDepth };
  if (opts.replaceHistory) {
    window.history.replaceState(state, '', url.toString());
    cfUpdateAppNavControls();
    return;
  }
  var currentState = window.history.state || {};
  if (currentState.pageId === pageId && window.location.hash === ('#' + cfPageTabName(pageId))) {
    cfUpdateAppNavControls();
    return;
  }
  _cfPageHistoryDepth += 1;
  state.cfDepth = _cfPageHistoryDepth;
  window.history.pushState(state, '', url.toString());
  cfUpdateAppNavControls();
}

function cfSetActivePageShell(pageId, btn) {
  document.querySelectorAll('.page-section').forEach(function(p) { p.classList.remove('active-page'); });
  document.querySelectorAll('.nav-tab').forEach(function(t) { t.classList.remove('active'); });
  document.getElementById(pageId).classList.add('active-page');
  if (!btn) btn = cfNavButtonForPage(pageId);
  if (btn) btn.classList.add('active');
}

function showPage(pageId, btn, options) {
  if (!document.getElementById(pageId)) return;
  var opts = options || {};
  var tabName = cfPageTabName(pageId);
  var activePage = document.querySelector('.page-section.active-page');
  var alreadyActive = activePage && activePage.id === pageId;
  if (alreadyActive && !opts.forceReload) {
    cfSetActivePageShell(pageId, btn);
    localStorage.setItem('cf_active_tab', tabName);
    cfSyncPageHistory(pageId, opts);
    return;
  }
  cfSetActivePageShell(pageId, btn);
  // Persist active tab
  localStorage.setItem('cf_active_tab', tabName);
  cfSyncPageHistory(pageId, opts);

  if (pageId === 'dashboard-page') { loadBrokerSettings(true); refreshBrokerState(true); loadDashboard(); }
  if (pageId === 'market-page') refreshMarket();
  if (pageId === 'results-page' && !_backtestRunning) { loadRuns(); fetchStrategies(); }
  if (pageId === 'live-page') startLiveMonitor();
  if (pageId === 'portfolio-page') { loadBrokerSettings(true); refreshBrokerState(true); loadPortfolioData(); }
  if (pageId === 'scalp-page') cfInitScalpPage();
  // Stop live monitor when leaving live page
  if (pageId !== 'live-page') stopLiveMonitor();
}

window.addEventListener('popstate', function(event) {
  _cfPageHistoryDepth = Math.max(0, Number(event.state && event.state.cfDepth) || 0);
  cfUpdateAppNavControls();
  var pageId = (event.state && event.state.pageId) || cfPageIdFromLocation() || 'dashboard-page';
  showPage(pageId, cfNavButtonForPage(pageId), { skipHistory: true });
});

window.addEventListener('hashchange', function() {
  var pageId = cfPageIdFromLocation();
  if (!pageId) return;
  showPage(pageId, cfNavButtonForPage(pageId), { skipHistory: true });
});

window.addEventListener('pageshow', function(event) {
  if (!event.persisted) return;
  var pageId = cfPageIdFromLocation();
  if (!pageId) return;
  var activePage = document.querySelector('.page-section.active-page');
  var btn = cfNavButtonForPage(pageId);
  if (activePage && activePage.id === pageId) {
    cfSetActivePageShell(pageId, btn);
    cfSyncPageHistory(pageId, { replaceHistory: true });
    return;
  }
  showPage(pageId, btn, { replaceHistory: true });
});

// ── Theme Toggle ───────────────────────────────────────────
function toggleTheme() {
  if (typeof window.cfToggleTheme === 'function') {
    window.cfToggleTheme();
    return;
  }
  const html = document.documentElement;
  html.dataset.theme = html.dataset.theme === 'light' ? 'dark' : 'light';
  html.style.colorScheme = html.dataset.theme;
  localStorage.setItem('cf-theme', html.dataset.theme);
}
(function() {
  if (typeof window.cfApplyTheme === 'function') {
    window.cfApplyTheme(typeof window.cfGetStoredTheme === 'function' ? window.cfGetStoredTheme() : '', { persist: false });
    return;
  }
  const saved = localStorage.getItem('cf-theme');
  if (saved) {
    document.documentElement.dataset.theme = saved;
    document.documentElement.style.colorScheme = saved;
  }
})();

// ── Clock (IST primary) ─────────────────────────────────────
function updateClock() {
  const now = new Date();
  const ist = now.toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour12: false });
  document.getElementById('topbar-clock').textContent = ist + ' IST';
}
setInterval(updateClock, 1000);
updateClock();

function cfGetCookie(name) {
  const prefix = name + '=';
  const parts = document.cookie ? document.cookie.split(';') : [];
  for (let i = 0; i < parts.length; i++) {
    const item = parts[i].trim();
    if (item.startsWith(prefix)) return decodeURIComponent(item.slice(prefix.length));
  }
  return '';
}

function cfApiFetch(url, options) {
  const opts = Object.assign({ credentials: 'same-origin' }, options || {});
  const method = String(opts.method || 'GET').toUpperCase();
  const headers = new Headers(opts.headers || {});
  if (['POST', 'PUT', 'PATCH', 'DELETE'].includes(method) && url !== '/api/auth/login') {
    const csrf = cfGetCookie('cryptoforge_csrf');
    if (csrf) headers.set('X-CSRF-Token', csrf);
    headers.set('X-Requested-With', 'XMLHttpRequest');
  }
  opts.headers = headers;
  return fetch(url, opts);
}

function cfApiErrorDetail(payload, fallback) {
  if (!payload || typeof payload !== 'object') return fallback;
  if (payload.message) return payload.message;
  if (payload.detail) return payload.detail;
  if (payload.error && typeof payload.error === 'object') {
    if (payload.error.detail) return payload.error.detail;
    if (payload.error.message) return payload.error.message;
    if (payload.error.title) return payload.error.title;
  }
  return fallback;
}

async function cfReadApiPayload(response) {
  if (!response) return {};
  let raw = '';
  try {
    raw = await response.text();
  } catch (_) {
    raw = '';
  }
  if (!raw) {
    return response.ok
      ? {}
      : { status: 'error', message: (String(response.status || '') + ' ' + String(response.statusText || 'Request failed')).trim() };
  }
  try {
    return JSON.parse(raw);
  } catch (_) {
    return {
      status: 'error',
      message: cfTrimUiText(raw, 180) || (String(response.status || '') + ' ' + String(response.statusText || 'Request failed')).trim()
    };
  }
}

function cfEl(id) {
  return document.getElementById(id);
}

function cfFieldValue(id, fallback) {
  const el = cfEl(id);
  if (!el || el.value === undefined || el.value === null) return fallback;
  return el.value;
}

function cfFieldNumber(id, fallback) {
  const raw = cfFieldValue(id, '');
  if (raw === '' || raw === null || raw === undefined) return fallback;
  const val = parseFloat(raw);
  return Number.isFinite(val) ? val : fallback;
}

function cfRequireElement(id, label) {
  const el = cfEl(id);
  if (el) return el;
  const err = new Error((label || id) + ' control is unavailable. Reload the page to refresh the scalp form.');
  err.code = 'cf_ui_mismatch';
  throw err;
}

function cfFormatLatency(ms) {
  const val = Number(ms);
  if (!Number.isFinite(val) || val < 0) return '—';
  if (val < 1000) return Math.round(val) + 'ms';
  const totalSeconds = Math.floor(val / 1000);
  if (totalSeconds < 60) return totalSeconds + 's';
  const totalMinutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  if (totalMinutes < 60) return totalMinutes + 'm ' + seconds + 's';
  const totalHours = Math.floor(totalMinutes / 60);
  const minutes = totalMinutes % 60;
  if (totalHours < 24) return totalHours + 'h ' + minutes + 'm';
  const days = Math.floor(totalHours / 24);
  const hours = totalHours % 24;
  return days + 'd ' + hours + 'h';
}

function cfPriceSourceLabel(source) {
  const raw = String(source || '').toLowerCase();
  if (!raw) return 'Idle';
  if (raw === 'ws') return 'WS';
  if (raw === 'rest_bulk') return 'REST bulk';
  if (raw === 'rest_quote') return 'REST quote';
  if (raw === 'broker_fill') return 'Broker fill';
  if (raw === 'entry_snapshot') return 'Entry snap';
  if (raw === 'entry') return 'Entry';
  return raw.replace(/_/g, ' ');
}

// ── Logout ─────────────────────────────────────────────────
async function doLogout() {
  await cfApiFetch('/api/auth/logout', { method: 'POST' });
  window.location.reload();
}

// ── Emergency Stop ─────────────────────────────────────────
async function emergencyStop() {
  const ok = await cfConfirm('This will immediately kill all running paper, live, and scalp flows. Are you sure?', 'Emergency Stop', '🚨');
  if (!ok) return;
  try {
    const r = await cfApiFetch('/api/emergency-stop', { method: 'POST' });
    const d = await r.json();
    cfToast(d.message || 'All engines stopped', 'success');
    setTimeout(function() { location.reload(); }, 1000);
  } catch(e) { cfToast('Emergency stop failed: ' + e.message, 'error'); }
}

// ── Connect Broker ─────────────────────────────────────────
function _brokerLabel() {
  return (_brokerInfo && _brokerInfo.currentLabel) || 'Broker';
}

function _brokerLockMessage() {
  var locks = (_brokerInfo && _brokerInfo.runtimeLocks) || {};
  var reasons = Array.isArray(locks.reasons) ? locks.reasons : [];
  return reasons[0] || 'Stop active live, paper, or scalp exposure before switching brokers.';
}

function _renderBrokerSelectOptions() {
  var brokers = Array.isArray(_brokerInfo.availableBrokers) && _brokerInfo.availableBrokers.length
    ? _brokerInfo.availableBrokers
    : [{ name: _brokerInfo.currentBroker || 'delta', label: _brokerLabel() }];
  document.querySelectorAll('[data-broker-select]').forEach(function(select) {
    var currentValue = select.value || _brokerInfo.currentBroker;
    select.innerHTML = brokers.map(function(item) {
      var label = item.label || item.name || 'Broker';
      return '<option value="' + _escapeHtml(item.name || '') + '">' + _escapeHtml(label) + '</option>';
    }).join('');
    select.value = _brokerInfo.currentBroker || currentValue || brokers[0].name;
    select.disabled = !_brokerInfo.switchable;
    select.title = _brokerInfo.switchable ? 'Choose the active broker' : _brokerLockMessage();
  });
}

function _applyBrokerConfig(payload) {
  payload = payload || {};
  _brokerInfo = {
    currentBroker: payload.current_broker || _brokerInfo.currentBroker || 'delta',
    currentLabel: payload.current_label || payload.broker || _brokerInfo.currentLabel || 'Broker',
    configured: payload.configured != null ? !!payload.configured : !!_brokerInfo.configured,
    feedKind: payload.feed_kind || _brokerInfo.feedKind || 'polling',
    availableBrokers: Array.isArray(payload.available_brokers) ? payload.available_brokers : (_brokerInfo.availableBrokers || []),
    switchable: payload.switchable != null ? !!payload.switchable : (_brokerInfo.switchable != null ? _brokerInfo.switchable : true),
    runtimeLocks: payload.runtime_locks || _brokerInfo.runtimeLocks || {},
  };
  var dashBroker = document.getElementById('dash-broker');
  if (dashBroker) dashBroker.textContent = _brokerLabel();
  document.querySelectorAll('[data-broker-switch]').forEach(function(btn) {
    btn.disabled = !_brokerInfo.switchable;
    btn.title = _brokerInfo.switchable ? 'Switch the active broker' : _brokerLockMessage();
  });
  _renderBrokerSelectOptions();
}

function _applyBrokerState(connected, message) {
  _brokerConnected = !!connected;
  document.querySelectorAll('[data-broker-toggle]').forEach(function(btn) {
    btn.classList.toggle('connected', _brokerConnected);
    btn.textContent = _brokerConnected ? '🔌 Disconnect Broker' : '🔗 Connect Broker';
  });
  var brokerCard = document.getElementById('dash-broker-state');
  if (brokerCard) brokerCard.textContent = message || (_brokerConnected ? (_brokerLabel() + ' connected') : (_brokerLabel() + ' disconnected'));
}

async function loadBrokerSettings(silent) {
  try {
    const r = await cfApiFetch('/api/broker/settings', { cache: 'no-store' });
    const d = await cfReadApiPayload(r);
    if (!r.ok || d.status === 'error') throw new Error(cfApiErrorDetail(d, 'Broker settings unavailable'));
    _applyBrokerConfig(d);
    return d;
  } catch (e) {
    if (!silent) cfToast('Broker settings failed: ' + e.message, 'warning');
    return null;
  }
}

async function refreshBrokerState(silent) {
  try {
    const r = await cfApiFetch('/api/broker/check', { method: 'POST', cache: 'no-store' });
    const d = await cfReadApiPayload(r);
    _applyBrokerConfig(d);
    _applyBrokerState(
      d.status === 'connected',
      d.status === 'connected' ? (_brokerLabel() + ' connected') : (d.message || (_brokerLabel() + ' disconnected'))
    );
  } catch(e) {
    _applyBrokerState(false, 'Unavailable');
    if (!silent) cfToast('Broker check failed: ' + e.message, 'warning');
  }
}

async function connectBroker() {
  try {
    const r = await cfApiFetch('/api/broker/connect', { method: 'POST', cache: 'no-store' });
    const d = await cfReadApiPayload(r);
    _applyBrokerConfig(d);
    if (d.status === 'connected') {
      _applyBrokerState(true, _brokerLabel() + ' connected');
      cfToast('Connected to ' + _brokerLabel(), 'success');
    } else {
      _applyBrokerState(false, d.message || 'Disconnected');
      cfToast(d.message || 'Broker not configured', 'warning');
    }
    loadDashboard();
    if (document.getElementById('portfolio-page').classList.contains('active-page')) loadPortfolioData();
  } catch(e) { cfToast('Connection failed: ' + e.message, 'error'); }
}

function disconnectBroker() {
  _applyBrokerState(false, 'Disconnected');
  cfToast('Broker panel disconnected', 'info');
}

async function switchBroker(scope) {
  var select = document.querySelector('[data-broker-select="' + String(scope || '') + '"]') || document.querySelector('[data-broker-select]');
  if (!select) {
    cfToast('Broker selector is unavailable', 'warning');
    return;
  }
  var targetBroker = String(select.value || '').trim().toLowerCase();
  if (!targetBroker) {
    cfToast('Choose a broker first', 'warning');
    return;
  }
  if (targetBroker === _brokerInfo.currentBroker) {
    cfToast(_brokerLabel() + ' is already active', 'info');
    return;
  }
  try {
    const r = await cfApiFetch('/api/broker/settings', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ broker: targetBroker }),
      cache: 'no-store',
    });
    const d = await cfReadApiPayload(r);
    _applyBrokerConfig(d);
    if (!r.ok || d.status !== 'ok') {
      throw new Error(cfApiErrorDetail(d, _brokerLockMessage()));
    }
    await refreshBrokerState(true);
    loadDashboard();
    refreshTopbarTicker();
    if (document.getElementById('portfolio-page').classList.contains('active-page')) loadPortfolioData();
    if (document.getElementById('market-page').classList.contains('active-page')) refreshMarket();
    if (document.getElementById('builder-page').classList.contains('active-page')) fetchLeverage(selectedCrypto);
    if (document.getElementById('scalp-page').classList.contains('active-page')) {
      cfRefreshScalpWorkspace({ reconcile: 'manual' }).catch(function() { return null; });
    }
    cfToast(d.message || ('Broker switched to ' + _brokerLabel()), 'success');
  } catch (e) {
    if (select) select.value = _brokerInfo.currentBroker || select.value;
    cfToast('Broker switch failed: ' + e.message, 'warning');
  }
}

function toggleBrokerConnection() {
  if (_brokerConnected) disconnectBroker();
  else connectBroker();
}

// ── Crypto Selector (Builder) ──────────────────────────────
function initCryptoSelector() {
  // Merge market coins into selector list (if loaded)
  const allCoins = [...TOP_25];
  if (window._marketCoins) {
    window._marketCoins.forEach(mc => {
      const sym = mc.trade_symbol || (mc.symbol + 'USDT');
      if (!allCoins.find(c => c.symbol === sym)) {
        allCoins.push({ symbol: sym, name: mc.name, ticker: mc.symbol, icon: '🪙' });
      }
    });
  }
  const grid = document.getElementById('crypto-selector');
  grid.innerHTML = allCoins.map(c => `
    <div class="crypto-item ${c.symbol === selectedCrypto ? 'selected' : ''}"
         data-cf-click="selectCrypto('${c.symbol}', this)">
      <div class="ci-icon">${c.icon}</div>
      <div class="ci-ticker">${c.ticker}</div>
      <div class="ci-name">${c.name}</div>
    </div>
  `).join('');
}

function selectCrypto(symbol, el) {
  selectedCrypto = symbol;
  document.querySelectorAll('.crypto-item').forEach(i => i.classList.remove('selected'));
  el.classList.add('selected');
  fetchLeverage(symbol);
  renderBuilderDeck();
}

// ── Leverage Selector ──────────────────────────────────────
function renderLeverage(options, current) {
  const cont = document.getElementById('leverage-selector');
  cont.innerHTML = options.map(lev => `
    <div class="lev-btn ${lev === current ? 'active' : ''}" data-cf-click="setLeverage(${lev}, this)">${lev}x</div>
  `).join('');
}

function setLeverage(lev, el) {
  selectedLeverage = lev;
  document.querySelectorAll('.lev-btn').forEach(b => b.classList.remove('active'));
  if (el) el.classList.add('active');
  renderBuilderDeck();
}

async function fetchLeverage(symbol) {
  try {
    const r = await fetch(`/api/leverage/${symbol}`, { credentials: 'same-origin' });
    const d = await r.json();
    if (d.status === 'ok') {
      leverageOptions = d.options;
      selectedLeverage = d.default;
      renderLeverage(d.options, d.default);
      renderBuilderDeck();
      return;
    }
  } catch(e) { /* fallback below */ }
  // Default leverage options for non-Delta coins
  leverageOptions = [1, 2, 5, 10, 20, 50, 100];
  selectedLeverage = 10;
  renderLeverage(leverageOptions, selectedLeverage);
  renderBuilderDeck();
}

// ── Side Toggle ────────────────────────────────────────────
function setSide(side) {
  selectedSide = side;
  document.getElementById('side-long').className = 'side-btn' + (side === 'LONG' ? ' long-active' : '');
  document.getElementById('side-short').className = 'side-btn' + (side === 'SHORT' ? ' short-active' : '');
  renderBuilderDeck();
}

// ── Indicators ─────────────────────────────────────────────
var _indInputStyle = 'padding:7px 8px;background:var(--input-bg,rgba(255,255,255,0.05));border:1px solid var(--border);border-radius:8px;color:var(--text);font-family:"Outfit",sans-serif;font-size:12px;';
var _indTfHtml = '<select id="ind-tf" style="width:100px;' + _indInputStyle + '">' +
  '<option value="1">1 Min</option><option value="3">3 Min</option><option value="5" selected>5 Min</option>' +
  '<option value="15">15 Min</option><option value="30">30 Min</option><option value="60">1 Hour</option></select>';

function renderIndicatorFields() {
  var name = document.getElementById('new-indicator-name').value;
  var c = document.getElementById('dynamic-indicator-fields');
  if (name === 'EMA' || name === 'SMA')
    c.innerHTML = '<input type="number" id="ind-period" value="14" min="1" style="width:70px;' + _indInputStyle + '" title="Period">' + _indTfHtml;
  else if (name === 'Supertrend')
    c.innerHTML = '<input type="number" id="ind-period" value="10" min="1" style="width:70px;' + _indInputStyle + '" title="Period">' +
      '<input type="number" id="ind-multiplier" value="3" step="0.1" min="0.1" style="width:70px;' + _indInputStyle + '" title="Multiplier">' + _indTfHtml;
  else if (name === 'RSI')
    c.innerHTML = '<input type="number" id="ind-period" value="14" min="1" style="width:70px;' + _indInputStyle + '" title="Period">' + _indTfHtml;
  else if (name === 'MACD')
    c.innerHTML = '<input type="number" id="ind-macd-fast" value="12" min="1" style="width:55px;' + _indInputStyle + '" title="Fast">' +
      '<input type="number" id="ind-macd-slow" value="26" min="1" style="width:55px;' + _indInputStyle + '" title="Slow">' +
      '<input type="number" id="ind-macd-signal" value="9" min="1" style="width:55px;' + _indInputStyle + '" title="Signal">' + _indTfHtml;
  else if (name === 'BB')
    c.innerHTML = '<input type="number" id="ind-period" value="20" min="1" style="width:70px;' + _indInputStyle + '" title="Period">' +
      '<input type="number" id="ind-bb-std" value="2" step="0.1" min="0.1" style="width:70px;' + _indInputStyle + '" title="Std Dev">' + _indTfHtml;
  else if (name === 'ATR' || name === 'ADX' || name === 'StochRSI')
    c.innerHTML = '<input type="number" id="ind-period" value="14" min="1" style="width:70px;' + _indInputStyle + '" title="Period">' + _indTfHtml;
  else if (name === 'VWAP' || name === 'Current_Candle')
    c.innerHTML = _indTfHtml;
  else if (name === 'ORB')
    c.innerHTML = '<div style="display:flex;align-items:center;gap:6px"><label style="font-size:11px;color:var(--muted);white-space:nowrap;">Minutes:</label>' +
      '<input type="number" id="ind-orb-minutes" value="15" style="width:70px;' + _indInputStyle + '" title="ORB window in minutes" min="5" max="60" step="5"></div>';
  else if (name === 'CPR')
    c.innerHTML = '<span style="font-size:11px;color:var(--muted);">Click + Add to configure</span>';
  else if (name === 'Previous_Day')
    c.innerHTML = '';
  else
    c.innerHTML = '';
}

function addIndicator() {
  var name = document.getElementById('new-indicator-name').value;
  var id = '';
  var tf = (document.getElementById('ind-tf') || {}).value || '5';

  // CPR opens a configuration modal instead of adding directly
  if (name === 'CPR') {
    if (indicators.some(function(i) { return i.startsWith('CPR'); })) { cfToast('CPR already added!', 'warning'); return; }
    document.getElementById('cpr-modal').style.display = 'flex';
    return;
  }

  if (name === 'EMA' || name === 'SMA') {
    var p = (document.getElementById('ind-period') || {}).value || '14';
    id = name + '_' + p + '_' + tf + 'm';
  } else if (name === 'RSI') {
    var p = (document.getElementById('ind-period') || {}).value || '14';
    id = 'RSI_' + p + '_' + tf + 'm';
  } else if (name === 'Supertrend') {
    var p = (document.getElementById('ind-period') || {}).value || '10';
    var m = (document.getElementById('ind-multiplier') || {}).value || '3';
    id = 'Supertrend_' + p + '_' + m + '_' + tf + 'm';
  } else if (name === 'MACD') {
    var fast = (document.getElementById('ind-macd-fast') || {}).value || '12';
    var slow = (document.getElementById('ind-macd-slow') || {}).value || '26';
    var sig = (document.getElementById('ind-macd-signal') || {}).value || '9';
    id = 'MACD_' + fast + '_' + slow + '_' + sig + '_' + tf + 'm';
  } else if (name === 'BB') {
    var p = (document.getElementById('ind-period') || {}).value || '20';
    var std = (document.getElementById('ind-bb-std') || {}).value || '2';
    id = 'BB_' + p + '_' + std + '_' + tf + 'm';
  } else if (name === 'ATR' || name === 'ADX' || name === 'StochRSI') {
    var p = (document.getElementById('ind-period') || {}).value || '14';
    id = name + '_' + p + '_' + tf + 'm';
  } else if (name === 'VWAP') {
    id = 'VWAP_' + tf + 'm';
  } else if (name === 'Current_Candle') {
    id = 'Current_Candle_' + tf + 'm';
  } else if (name === 'Previous_Day') {
    id = 'Previous_Day';
  } else if (name === 'ORB') {
    var mins = (document.getElementById('ind-orb-minutes') || {}).value || '15';
    id = 'ORB_' + mins + 'min';
  }

  if (!id) return;
  if (!indicators.includes(id)) {
    indicators.push(id);
    renderIndicators();
    cfToast(id.replace(/_/g, ' ') + ' added', 'success');
  } else {
    cfToast('Already added!', 'warning');
  }
}

// ── CPR Modal Functions ──
function closeCPRModal() { document.getElementById('cpr-modal').style.display = 'none'; }

function confirmAddCPR() {
  var cprTf = (document.getElementById('cpr-timeframe') || {}).value || 'Day';
  var narrowPct = parseFloat((document.getElementById('cpr-narrow-pct') || {}).value) || 0.2;
  var moderatePct = parseFloat((document.getElementById('cpr-moderate-pct') || {}).value) || 0.5;

  // Encode: CPR_Day_0.2_0.5
  var indId = 'CPR_' + cprTf + '_' + narrowPct + '_' + moderatePct;

  if (!indicators.some(function(i) { return i.startsWith('CPR'); })) {
    indicators.push(indId);
    renderIndicators();
    closeCPRModal();
    cfToast('Added CPR ' + cprTf + ' (Narrow \u2264' + narrowPct + '%, Moderate \u2264' + moderatePct + '%)', 'success');
  } else {
    cfToast('CPR already added!', 'warning');
    closeCPRModal();
  }
}

function _formatCPRBadge(indId) {
  // CPR_Day_0.2_0.5 → "CPR Day (N:0.2% M:0.5%)"
  var parts = indId.split('_');
  var tf = parts[1] || 'Day';
  var narrow = parts[2] || '0.2';
  var moderate = parts[3] || '0.5';
  return 'CPR ' + tf + ' (N:' + narrow + '% M:' + moderate + '%)';
}

function removeIndicator(idx) {
  indicators.splice(idx, 1);
  renderIndicators();
}

function renderIndicators() {
  var list = document.getElementById('indicator-list');
  if (!list) return;
  list.innerHTML = indicators.map(function(ind, i) {
    var isCPR = ind.startsWith('CPR');
    var display = isCPR ? _formatCPRBadge(ind) : ind.replace(/_/g, ' ');
    var bgStyle = isCPR ? 'background:linear-gradient(135deg, var(--accent2), #7c3aed);' : '';
    return '<span class="tag tag-purple" style="cursor:pointer;display:inline-flex;align-items:center;gap:4px;' + bgStyle + '" data-cf-click="removeIndicator(' + i + ')">' +
      display + ' <span style="color:#ffb3b3;font-size:14px;">\u00d7</span></span>';
  }).join('');
  renderConditions('entry');
  renderConditions('exit');
  renderBuilderDeck();
}

// Initialize indicator fields on page load
document.addEventListener('DOMContentLoaded', function() { if (document.getElementById('new-indicator-name')) renderIndicatorFields(); });

// ── Conditions ─────────────────────────────────────────────
function getIndicatorOptions() {
  var _d = function(id) { return id.replace(/_/g, ' '); };

  // ── Current Candle (live price action) ──
  var html = '<optgroup label="Current Candle">';
  html += '<option value="current_open">Current Candle — Open</option>';
  html += '<option value="current_high">Current Candle — High</option>';
  html += '<option value="current_low">Current Candle — Low</option>';
  html += '<option value="current_close">Current Candle — Close</option>';
  html += '<option value="current_volume">Current Candle — Volume</option>';
  html += '</optgroup>';

  // ── Custom Number ──
  html += '<optgroup label="── Custom Value ──">';
  html += '<option value="number">Number (e.g. 30, 70)</option>';
  html += '</optgroup>';

  // ── Moving Averages ──
  var mas = indicators.filter(function(i) { return i.startsWith('EMA') || i.startsWith('SMA'); });
  if (mas.length) {
    html += '<optgroup label="── Moving Averages ──">';
    mas.forEach(function(ind) { html += '<option value="' + ind + '">' + _d(ind) + '</option>'; });
    html += '</optgroup>';
  }

  // ── Oscillators (RSI, MACD, StochRSI) ──
  var rsiInds = indicators.filter(function(i) { return i.startsWith('RSI'); });
  var macdInds = indicators.filter(function(i) { return i.startsWith('MACD'); });
  var stochInds = indicators.filter(function(i) { return i.startsWith('StochRSI'); });
  if (rsiInds.length || macdInds.length || stochInds.length) {
    html += '<optgroup label="── Oscillators ──">';
    rsiInds.forEach(function(ind) { html += '<option value="' + ind + '">' + _d(ind) + '</option>'; });
    macdInds.forEach(function(ind) {
      html += '<option value="' + ind + '__line">' + _d(ind) + ' Line</option>';
      html += '<option value="' + ind + '__signal">' + _d(ind) + ' Signal</option>';
      html += '<option value="' + ind + '__histogram">' + _d(ind) + ' Histogram</option>';
    });
    stochInds.forEach(function(ind) {
      html += '<option value="' + ind + '__K">' + _d(ind) + ' K</option>';
      html += '<option value="' + ind + '__D">' + _d(ind) + ' D</option>';
    });
    html += '</optgroup>';
  }

  // ── Volatility (BB, ATR) ──
  var bbInds = indicators.filter(function(i) { return i.startsWith('BB'); });
  var atrInds = indicators.filter(function(i) { return i.startsWith('ATR'); });
  if (bbInds.length || atrInds.length) {
    html += '<optgroup label="── Volatility ──">';
    bbInds.forEach(function(ind) {
      html += '<option value="' + ind + '__upper">' + _d(ind) + ' Upper</option>';
      html += '<option value="' + ind + '__middle">' + _d(ind) + ' Middle</option>';
      html += '<option value="' + ind + '__lower">' + _d(ind) + ' Lower</option>';
    });
    atrInds.forEach(function(ind) { html += '<option value="' + ind + '">' + _d(ind) + '</option>'; });
    html += '</optgroup>';
  }

  // ── Trend (Supertrend, ADX, VWAP) ──
  var stInds = indicators.filter(function(i) { return i.startsWith('Supertrend'); });
  var adxInds = indicators.filter(function(i) { return i.startsWith('ADX'); });
  var vwapInds = indicators.filter(function(i) { return i.startsWith('VWAP'); });
  if (stInds.length || adxInds.length || vwapInds.length) {
    html += '<optgroup label="── Trend ──">';
    stInds.forEach(function(ind) { html += '<option value="' + ind + '">' + _d(ind) + ' (Price Level)</option>'; });
    adxInds.forEach(function(ind) {
      html += '<option value="' + ind + '">' + _d(ind) + '</option>';
      html += '<option value="' + ind + '__plus">' + _d(ind) + ' +DI</option>';
      html += '<option value="' + ind + '__minus">' + _d(ind) + ' -DI</option>';
    });
    vwapInds.forEach(function(ind) { html += '<option value="' + ind + '">' + _d(ind) + '</option>'; });
    html += '</optgroup>';
  }

  // ── Support/Resistance (CPR) ──
  var cprInds = indicators.filter(function(i) { return i.startsWith('CPR'); });
  if (cprInds.length) {
    var cprLabel = _formatCPRBadge(cprInds[0]);
    html += '<optgroup label="── CPR (' + cprLabel + ') ──">';
    html += '<option value="CPR_Pivot">CPR \u2014 Pivot</option>';
    html += '<option value="CPR_TC">CPR \u2014 TC (Top Central)</option>';
    html += '<option value="CPR_BC">CPR \u2014 BC (Bottom Central)</option>';
    html += '<option value="CPR_R1">CPR \u2014 R1</option><option value="CPR_R2">CPR \u2014 R2</option>';
    html += '<option value="CPR_R3">CPR \u2014 R3</option><option value="CPR_R4">CPR \u2014 R4</option><option value="CPR_R5">CPR \u2014 R5</option>';
    html += '<option value="CPR_S1">CPR \u2014 S1</option><option value="CPR_S2">CPR \u2014 S2</option>';
    html += '<option value="CPR_S3">CPR \u2014 S3</option><option value="CPR_S4">CPR \u2014 S4</option><option value="CPR_S5">CPR \u2014 S5</option>';
    html += '<option value="CPR_width_pct">CPR \u2014 Width %</option>';
    html += '<option value="CPR_is_narrow">CPR \u2014 Is Narrow (true/false)</option>';
    html += '<option value="CPR_is_moderate">CPR \u2014 Is Moderate (true/false)</option>';
    html += '<option value="CPR_is_wide">CPR \u2014 Is Wide (true/false)</option>';
    html += '</optgroup>';
  }

  // ── Previous Day ──
  var prevDayInds = indicators.filter(function(i) { return i === 'Previous_Day'; });
  if (prevDayInds.length) {
    html += '<optgroup label="── Previous Day ──">';
    html += '<option value="yesterday_high">Previous Day \u2014 High</option>';
    html += '<option value="yesterday_low">Previous Day \u2014 Low</option>';
    html += '<option value="yesterday_close">Previous Day \u2014 Close</option>';
    html += '<option value="yesterday_open">Previous Day \u2014 Open</option>';
    html += '</optgroup>';
  }

  // ── ORB (Opening Range Breakout) ──
  var orbInds = indicators.filter(function(i) { return i.startsWith('ORB'); });
  if (orbInds.length) {
    html += '<optgroup label="── ORB ──">';
    html += '<option value="ORB_high">ORB \u2014 High</option>';
    html += '<option value="ORB_low">ORB \u2014 Low</option>';
    html += '</optgroup>';
  }

  // ── Day / Time (always available) ──
  html += '<optgroup label="── Day / Time ──">';
  html += '<option value="Time_Of_Day">Time Of Day</option>';
  html += '<option value="Day_Of_Week">Day Of Week</option>';
  html += '</optgroup>';

  return html;
}

// Boolean fields — only show is_true / is_false operators, no right side
var BOOLEAN_FIELDS = ['CPR_is_narrow', 'CPR_is_moderate', 'CPR_is_wide', 'ORB_is_breakout_up', 'ORB_is_breakout_down', 'ORB_is_inside'];

function addCondition(type) {
  const arr = type === 'entry' ? entryConditions : exitConditions;
  arr.push({ left: 'current_close', operator: 'crosses_above', right: 'current_close', connector: 'AND', right_number_value: '0' });
  renderConditions(type);
}

function removeCondition(type, idx) {
  const arr = type === 'entry' ? entryConditions : exitConditions;
  arr.splice(idx, 1);
  renderConditions(type);
}

function updateCondition(type, idx, field, value) {
  const arr = type === 'entry' ? entryConditions : exitConditions;
  arr[idx][field] = value;
  // When left field changes, reset operator/right to match the new field type
  if (field === 'left') {
    if (BOOLEAN_FIELDS.includes(value)) {
      arr[idx].operator = 'is_true';
      arr[idx].right = '';
    } else if (value === 'Time_Of_Day') {
      arr[idx].operator = 'is_above';
      arr[idx].right = '';
      arr[idx].right_time = arr[idx].right_time || '09:30';
    } else if (value === 'Day_Of_Week') {
      arr[idx].operator = 'contains';
      arr[idx].right = '';
      arr[idx].right_days = arr[idx].right_days || [];
    } else {
      // Normal field — reset to default operator if coming from boolean/time/dow
      var oldOp = arr[idx].operator;
      if (oldOp === 'is_true' || oldOp === 'is_false' || oldOp === 'contains' || oldOp === 'not_contains') {
        arr[idx].operator = 'crosses_above';
        arr[idx].right = 'current_close';
      }
    }
    renderConditions(type);
  }
  if (field === 'right') renderConditions(type);
  renderBuilderDeck();
}

// Day picker helpers
function toggleDayDropdown(el) {
  var dd = el.nextElementSibling;
  var card = el.closest('.card');
  if (dd.style.display === 'none') {
    dd.style.display = 'block';
    if (card) card.style.zIndex = '10';
  } else {
    dd.style.display = 'none';
    if (card) card.style.zIndex = '';
  }
}
function updateDayLabel(type, idx, cb) {
  var picker = cb.closest('.day-picker');
  var checks = picker.querySelectorAll('input:checked');
  var label = picker.querySelector('.day-picker-toggle');
  var days = Array.from(checks).map(function(c) { return c.value; });
  if (days.length === 0) label.textContent = 'Select days \u25BE';
  else label.textContent = days.map(function(d) { return d.substring(0,3); }).join(', ') + ' \u25BE';
  var arr = type === 'entry' ? entryConditions : exitConditions;
  arr[idx].right_days = days;
}

function renderConditions(type) {
  const arr = type === 'entry' ? entryConditions : exitConditions;
  const cont = document.getElementById(type + '-conditions');
  const opts = getIndicatorOptions();

  // ── Auto-fix stale operator/right from saved conditions ──
  arr.forEach(function(c) {
    if (BOOLEAN_FIELDS.includes(c.left)) {
      if (c.operator !== 'is_true' && c.operator !== 'is_false') {
        c.operator = 'is_true';
        c.right = '';
      }
    } else if (c.left === 'Day_Of_Week') {
      if (c.operator !== 'contains' && c.operator !== 'not_contains') {
        c.operator = 'contains';
        c.right = '';
        c.right_days = c.right_days || [];
      }
    } else if (c.left === 'Time_Of_Day') {
      if (!['is_above','is_below','>=','<='].includes(c.operator)) {
        c.operator = 'is_above';
        c.right = '';
        c.right_time = c.right_time || '09:30';
      }
    } else {
      // Normal indicator — if operator is boolean/dow-only, reset to default
      if (c.operator === 'is_true' || c.operator === 'is_false' || c.operator === 'contains' || c.operator === 'not_contains') {
        c.operator = 'crosses_above';
        c.right = c.right || 'current_close';
      }
    }
  });

  cont.innerHTML = arr.map((c, i) => {
    let connectorHtml = '';
    if (i > 0) {
      connectorHtml = `<div class="condition-connector">
        <select data-cf-change="updateCondition('${type}',${i},'connector',this.value)">
          <option value="AND" ${c.connector==='AND'?'selected':''}>AND</option>
          <option value="OR" ${c.connector==='OR'?'selected':''}>OR</option>
        </select>
      </div>`;
    }

    var isBool = BOOLEAN_FIELDS.includes(c.left);
    var isTime = c.left === 'Time_Of_Day';
    var isDow = c.left === 'Day_Of_Week';

    // Operator dropdown depends on left-side type
    var opHtml = '';
    if (isBool) {
      opHtml = `<select style="min-width:120px;" data-cf-change="updateCondition('${type}',${i},'operator',this.value)">
        <option value="is_true" ${c.operator==='is_true'?'selected':''}>Is True</option>
        <option value="is_false" ${c.operator==='is_false'?'selected':''}>Is False</option>
      </select>`;
    } else if (isTime) {
      opHtml = `<select style="min-width:120px;" data-cf-change="updateCondition('${type}',${i},'operator',this.value)">
        <option value="is_above" ${c.operator==='is_above'?'selected':''}>Is Above</option>
        <option value="is_below" ${c.operator==='is_below'?'selected':''}>Is Below</option>
        <option value=">=" ${c.operator==='>='?'selected':''}>Equal or Above</option>
        <option value="<=" ${c.operator==='<='?'selected':''}>Equal or Below</option>
      </select>`;
    } else if (isDow) {
      opHtml = `<select style="min-width:120px;" data-cf-change="updateCondition('${type}',${i},'operator',this.value)">
        <option value="contains" ${c.operator==='contains'?'selected':''}>Contains</option>
        <option value="not_contains" ${c.operator==='not_contains'?'selected':''}>Not Contains</option>
      </select>`;
    } else {
      opHtml = `<select style="min-width:150px;" data-cf-change="updateCondition('${type}',${i},'operator',this.value)">
        <option value="crosses_above" ${c.operator==='crosses_above'?'selected':''}>Crosses Above</option>
        <option value="crosses_below" ${c.operator==='crosses_below'?'selected':''}>Crosses Below</option>
        <option value="is_above" ${c.operator==='is_above'?'selected':''}>Is Above</option>
        <option value="is_below" ${c.operator==='is_below'?'selected':''}>Is Below</option>
        <option value="==" ${c.operator==='=='?'selected':''}>== (Equals)</option>
        <option value=">=" ${c.operator==='>='?'selected':''}>>=</option>
        <option value="<=" ${c.operator==='<='?'selected':''}><=</option>
      </select>`;
    }

    // Right-side depends on type
    var rhsHtml = '';
    if (isBool) {
      rhsHtml = ''; // No right side for boolean
    } else if (isTime) {
      rhsHtml = `<input type="time" value="${c.right_time||'09:30'}" step="1" style="flex:1;padding:8px;font-family:'JetBrains Mono',monospace;font-size:13px;background:var(--input-bg,rgba(255,255,255,0.05));border:1px solid var(--border);border-radius:8px;color:var(--text);"
        data-cf-change="var a=('${type}'==='entry'?entryConditions:exitConditions);a[${i}].right_time=this.value;">`;
    } else if (isDow) {
      var selDays = c.right_days || [];
      var dayLabel = selDays.length ? selDays.map(function(d){return d.substring(0,3);}).join(', ') + ' \u25BE' : 'Select days \u25BE';
      rhsHtml = `<div class="day-picker" style="flex:1;position:relative;">
        <div class="day-picker-toggle" data-cf-click="toggleDayDropdown(this)" style="padding:8px 10px;background:var(--input-bg,rgba(255,255,255,0.05));border:1px solid var(--border);border-radius:8px;cursor:pointer;font-size:12px;color:var(--text);">${dayLabel}</div>
        <div style="display:none;position:absolute;top:100%;left:0;right:0;z-index:9999;background:linear-gradient(180deg,rgba(25,34,58,0.99),rgba(10,16,30,0.99));border:1px solid var(--border);border-radius:8px;box-shadow:0 8px 24px rgba(0,0,0,0.7);margin-top:4px;padding:4px 0;">` +
        ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'].map(function(day) {
          var chk = selDays.includes(day) ? 'checked' : '';
          return `<label style="display:block;padding:8px 14px;cursor:pointer;font-size:13px;border-bottom:1px solid var(--border);" data-cf-mouseover="this.style.background='rgba(0,200,150,0.08)'" data-cf-mouseout="this.style.background='transparent'"><input type="checkbox" value="${day}" ${chk} style="margin-right:10px;accent-color:var(--accent);" data-cf-change="updateDayLabel('${type}',${i},this)"> ${day}</label>`;
        }).join('') +
        `</div></div>`;
    } else {
      rhsHtml = `<select data-cf-change="updateCondition('${type}',${i},'right',this.value)">
        ${opts.replace(`value="${c.right}"`, `value="${c.right}" selected`)}
      </select>
      ${c.right === 'number' ? `<input type="number" style="width:80px;" value="${c.right_number_value||0}"
        data-cf-change="updateCondition('${type}',${i},'right_number_value',this.value)">` : ''}`;
    }

    return `${connectorHtml}
    <div class="condition-row">
      ${i === 0 ? '<span class="logic-label" style="min-width:40px;text-align:center;font-size:11px;font-weight:700;color:var(--accent);letter-spacing:1px;">IF</span>' : ''}
      <select data-cf-change="updateCondition('${type}',${i},'left',this.value)">
        ${opts.replace(`value="${c.left}"`, `value="${c.left}" selected`)}
      </select>
      ${opHtml}
      ${rhsHtml}
      <button class="remove-cond" data-cf-click="removeCondition('${type}',${i})">✕</button>
    </div>`;
  }).join('');
  renderBuilderDeck();
}

// ── Dashboard ──────────────────────────────────────────────
async function loadDashboard() {
  try {
    const r = await fetch('/api/dashboard/summary', { credentials: 'same-origin' });
    if (r.status === 401) return;
    const d = await r.json();
    _applyBrokerConfig(d);
    document.getElementById('dash-strats').textContent = d.strategy_count || 0;
    document.getElementById('dash-backtests').textContent = d.backtest_count || 0;
    const pnl = d.today_pnl || 0;
    const pnlEl = document.getElementById('dash-pnl');
    pnlEl.textContent = fmtINR(pnl);
    pnlEl.style.color = pnl >= 0 ? 'var(--green)' : 'var(--red)';

    // Paper/Live P&L split
    var paperPnl = d.paper_pnl || 0;
    var livePnl = d.live_pnl || 0;
    var ppEl = document.getElementById('dash-paper-pnl');
    if (ppEl) { ppEl.textContent = fmtINR(paperPnl); ppEl.style.color = paperPnl >= 0 ? 'var(--green)' : 'var(--red)'; }
    var lpEl = document.getElementById('dash-live-pnl');
    if (lpEl) { lpEl.textContent = fmtINR(livePnl); lpEl.style.color = livePnl >= 0 ? 'var(--green)' : 'var(--red)'; }

    var paperStatusEl = document.getElementById('dash-paper');
    if (paperStatusEl) {
      paperStatusEl.textContent = d.paper_running ? '🟢 Running (' + (d.paper_count || 1) + ')' : 'Idle';
      paperStatusEl.style.color = d.paper_running ? 'var(--green)' : 'var(--muted)';
    }
    var liveStatusEl = document.getElementById('dash-live');
    if (liveStatusEl) {
      liveStatusEl.textContent = d.live_running ? '🔴 LIVE (' + (d.live_count || 1) + ')' : 'Idle';
      liveStatusEl.style.color = d.live_running ? 'var(--red)' : 'var(--muted)';
    }
    // Pulse ring on live card when active
    var liveCard = document.getElementById('dash-live-card');
    if (liveCard) liveCard.classList.toggle('live-active', !!d.live_running);

    // Kill switch visibility
    document.getElementById('kill-switch-btn').classList.toggle('hidden', !d.paper_running && !d.live_running);

    // Active engines
    var engCard = document.getElementById('dash-engines-card');
    var engList = document.getElementById('dash-engines-list');
    if (engCard && engList) {
      if (d.paper_running || d.live_running) {
        engCard.style.display = 'block';
        try {
          var er = await fetch('/api/engines/all', { credentials: 'same-origin' });
          var ed = await er.json();
          var engines = ed.engines || [];
          if (engines.length > 0) {
            engList.innerHTML = engines.map(function(e) {
              var ep = parseFloat(e.total_pnl) || 0;
              var strategyName = _escapeHtml(e.strategy_name || e.run_name || '');
              var mode = _escapeHtml(e.mode || '');
              return '<div style="display:flex;justify-content:space-between;align-items:center;padding:8px 12px;border-bottom:1px solid var(--border);">'
                + '<div><b>' + strategyName + '</b> <span class="tag ' + (e.mode === 'paper' ? 'tag-green' : 'tag-red') + '">' + mode + '</span></div>'
                + '<div style="font-family:JetBrains Mono;font-weight:700;color:' + (ep >= 0 ? 'var(--green)' : 'var(--red)') + '">' + fmtINR(ep) + '</div>'
                + '</div>';
            }).join('');
          } else { engCard.style.display = 'none'; }
        } catch(e2) { engCard.style.display = 'none'; }
      } else {
        engCard.style.display = 'none';
      }
    }

    // Load recent runs
    const rr = await fetch('/api/runs', { credentials: 'same-origin' });
    const runs = await rr.json();
    const cont = document.getElementById('dash-runs-list');
    if (runs.length === 0) {
      cont.innerHTML = '<tr><td colspan="7" style="text-align:center;padding:20px;color:var(--muted);">No runs yet. Go to Builder to create one.</td></tr>';
    } else {
      cont.innerHTML = runs.slice().reverse().map(r => {
        const pnl = r.total_pnl || 0;
        const pnlColor = pnl > 0 ? 'var(--green)' : pnl < 0 ? 'var(--red)' : 'var(--muted)';
        const runName = _escapeHtml(r.run_name || ('Run #' + r.id));
        const symbol = _escapeHtml(r.symbol || '—');
        const dt = _getTradeDateParts(r.created_at || r.started_at || '');
        const tradeCount = r.trade_count || 0;
        return `<tr style="cursor:pointer;" data-cf-click="viewRun(${r.id})" data-cf-mouseover="this.style.background='rgba(139,92,246,0.04)'" data-cf-mouseout="this.style.background=''">
          <td>${_getModeBadge(r.mode)}</td>
          <td><div class="table-row-label">${runName}</div><div class="table-note">${symbol} • ${tradeCount} trades</div></td>
          <td><div class="table-row-label">${symbol}</div><div class="table-note">${_escapeHtml(String(r.side || 'Both'))}</div></td>
          <td class="num"><div class="table-value-stack"><div class="table-value-main">${r.leverage || 1}x</div><div class="table-value-sub">leverage</div></div></td>
          <td class="num"><div class="table-value-stack"><div class="table-value-main">${tradeCount}</div><div class="table-value-sub">closed trades</div></div></td>
          <td class="num"><div class="table-value-stack"><div class="table-value-main" style="color:${pnlColor};">${fmtINR(pnl)}</div><div class="table-value-sub">net result</div></div></td>
          <td><div class="table-datetime"><strong>${dt.date}</strong><span>${dt.time || '—'}</span></div></td>
        </tr>`;
      }).join('');
    }
    _renderTablePager('dash-runs-table', 'dash-runs-table', 'dash-runs-pagination');
    renderDashboardMission(d, runs.slice().reverse());
  } catch(e) { console.error('Dashboard error:', e); }
}

// ── USD Currency Formatting ─────────────────────────────────
// All values are in USDT. Display in $ directly — no conversion needed.

/**
 * Format a USD P&L / monetary value.
 * e.g. fmtINR(1234.56) → "$1,234.56"
 * Function names kept as fmtINR/fmtINRPrice/fmtINRLarge for backward compat.
 */
function fmtINR(usd, decimals) {
  if (usd === null || usd === undefined || isNaN(usd)) return '$0.00';
  const d = (decimals !== undefined) ? decimals : 2;
  const val = parseFloat(usd);
  return (val < 0 ? '-$' : '$') + Math.abs(val).toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
}

/**
 * Format large USD numbers (market cap, volume) with standard suffixes.
 * e.g. fmtINRLarge(1e9) → "$1.00B"
 */
function fmtINRLarge(usd) {
  if (!usd || isNaN(usd)) return '$0';
  const abs = Math.abs(usd);
  const sign = usd < 0 ? '-' : '';
  if (abs >= 1e12) return sign + '$' + (abs / 1e12).toFixed(2) + 'T';
  if (abs >= 1e9)  return sign + '$' + (abs / 1e9).toFixed(2)  + 'B';
  if (abs >= 1e6)  return sign + '$' + (abs / 1e6).toFixed(2)  + 'M';
  if (abs >= 1e3)  return sign + '$' + (abs / 1e3).toFixed(1)  + 'K';
  return sign + '$' + abs.toFixed(0);
}

/**
 * Format a USD asset price (handles BTC ~$85K, altcoins, micro-caps).
 * e.g. fmtINRPrice(85000) → "$85,000"
 */
function fmtINRPrice(usdPrice) {
  if (!usdPrice || isNaN(usdPrice)) return '$0';
  const p = parseFloat(usdPrice);
  if (p >= 1000) return '$' + p.toLocaleString('en-US', { maximumFractionDigits: 0 });
  if (p >= 1)    return '$' + p.toFixed(2);
  if (p >= 0.01) return '$' + p.toFixed(4);
  return '$' + p.toFixed(6);
}

// Kept for backward compat — same as above
function usdToINR(usd) { return usd || 0; }
function fmtNum(n)   { return fmtINRLarge(n); }
function fmtPrice(p) { return fmtINRPrice(p); }

// ── Market (Top 25 from CoinGecko) ─────────────────────────

async function refreshMarket() {
  const tbody = document.getElementById('market-tbody');
  tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;padding:40px;color:var(--muted);">Loading top 25...</td></tr>';

  try {
    // Fetch top 25 from CoinGecko via backend
    const r = await fetch('/api/market/top25', { credentials: 'same-origin' });
    const d = await r.json();

    if (d.status === 'ok' && d.coins && d.coins.length) {
      // Store coins globally for the builder crypto selector
      window._marketCoins = d.coins;

      _renderMarketRows(d.coins);

      // Also update topbar tickers from the coins data
      const btc = d.coins.find(c => c.symbol === 'BTC');
      const eth = d.coins.find(c => c.symbol === 'ETH');
      const sol = d.coins.find(c => c.symbol === 'SOL');
      if (btc) updateTopbarTicker('btc', btc.price, btc.change_24h);
      if (eth) updateTopbarTicker('eth', eth.price, eth.change_24h);
      if (sol) updateTopbarTicker('sol', sol.price, sol.change_24h);

    } else {
      tbody.innerHTML = `<tr><td colspan="8" style="text-align:center;padding:40px;color:var(--muted);">
        Market data unavailable. ${d.message || ''}</td></tr>`;
    }
  } catch(e) {
    tbody.innerHTML = `<tr><td colspan="8" style="text-align:center;padding:40px;color:var(--muted);">
      Failed to fetch market data.</td></tr>`;
  }

  // Also refresh Delta tickers for funding bar
  try {
    const r2 = await fetch('/api/ticker', { credentials: 'same-origin' });
    const d2 = await r2.json();
    if (d2.status === 'ok' && d2.tickers) {
      const btc = d2.tickers['BTCUSDT'] || {};
      const eth = d2.tickers['ETHUSDT'] || {};
      if (btc.funding_rate) document.getElementById('fund-btc').textContent = (btc.funding_rate * 100).toFixed(4) + '%';
      if (eth.funding_rate) document.getElementById('fund-eth').textContent = (eth.funding_rate * 100).toFixed(4) + '%';
      const totalVol = Object.values(d2.tickers).reduce((s, t) => s + (t.volume_24h || 0), 0);
      document.getElementById('total-vol').textContent = fmtNum(totalVol);
    }
  } catch(e) {}
}

function fmtUSDPrice(usd) {
  if (!usd || isNaN(usd)) return '$0.00';
  if (usd >= 1) return '$' + usd.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  if (usd >= 0.01) return '$' + usd.toFixed(4);
  return '$' + usd.toFixed(6);
}

function updateTopbarTicker(id, price, change) {
  const priceEl = document.getElementById('tk-' + id);
  const subEl = document.getElementById('tk-' + id + '-usd');
  const chgEl = document.getElementById('tk-' + id + '-chg');
  if (price > 0) {
    priceEl.textContent = fmtUSDPrice(price);
    if (subEl) subEl.textContent = fmtINRPrice(price);
    const c = change || 0;
    chgEl.textContent = (c >= 0 ? '+' : '') + c.toFixed(2) + '%';
    chgEl.style.color = c >= 0 ? 'var(--green)' : 'var(--red)';
  }
}

function selectCryptoFromMarket(symbol, name) {
  selectedCrypto = symbol;
  // Dynamically add to TOP_25 if not already present
  if (!TOP_25.find(c => c.symbol === symbol)) {
    const ticker = symbol.replace('USDT', '');
    TOP_25.push({ symbol: symbol, name: name || ticker, ticker: ticker, icon: '🪙' });
  }
  showPage('builder-page', document.getElementById('nav-builder'));
  initCryptoSelector();
  fetchLeverage(symbol);
  renderBuilderDeck();
}

// ── Run Backtest ───────────────────────────────────────────
async function runBacktest() {
  const payload = {
    run_name: document.getElementById('b-name').value || 'Untitled',
    symbol: selectedCrypto,
    from_date: document.getElementById('b-from').value,
    to_date: document.getElementById('b-to').value,
    initial_capital: parseFloat(document.getElementById('b-capital').value) || 10000,
    leverage: selectedLeverage,
    trade_side: selectedSide,
    position_size_pct: document.getElementById('b-possize-mode').value === 'pct' ? (parseFloat(document.getElementById('b-possize').value) || 100) : 100,
    position_size_mode: document.getElementById('b-possize-mode').value,
    fixed_qty: document.getElementById('b-possize-mode').value === 'fixed_qty' ? (parseFloat(document.getElementById('b-possize').value) || 0.1) : 0,
    stoploss_pct: parseFloat(document.getElementById('b-sl').value) || 5,
    target_profit_pct: parseFloat(document.getElementById('b-tp').value) || 10,
    trailing_sl_pct: parseFloat(document.getElementById('b-trail').value) || 0,
    ...cfBuilderCostModel(),
    compounding: document.getElementById('b-compounding').checked,
    max_trades_per_day: parseInt(document.getElementById('b-maxtrades').value) || 5,
    indicators: indicators,
    entry_conditions: entryConditions.map(c => {
      var o = { left: c.left, operator: c.operator, right: c.right, connector: c.connector, right_number_value: c.right_number_value };
      if (c.right_time) o.right_time = c.right_time;
      if (c.right_days) o.right_days = c.right_days;
      return o;
    }),
    exit_conditions: exitConditions.map(c => {
      var o = { left: c.left, operator: c.operator, right: c.right, connector: c.connector, right_number_value: c.right_number_value };
      if (c.right_time) o.right_time = c.right_time;
      if (c.right_days) o.right_days = c.right_days;
      return o;
    }),
    candle_interval: document.getElementById('b-interval').value,
  };

  const btn = document.getElementById('backtest-run-btn');
  btn.disabled = true; btn.textContent = '⏳ Running...';
  btn.classList.add('loading');

  // Navigate to results page without triggering loadRuns (which would overwrite countdown)
  _backtestRunning = true;
  showPage('results-page', document.getElementById('nav-results'));
  // Hide any existing pagination from previous runs view
  var existingPager = document.getElementById('runs-table-pagination');
  if (existingPager) existingPager.style.display = 'none';
  var countdownSec = 15;
  var countdownEl = document.getElementById('results-list');
  function renderCountdown(sec) {
    countdownEl.innerHTML = '<tr><td colspan="10" style="text-align:center;padding:60px 20px;">'
      + '<div style="font-size:48px;margin-bottom:16px;">⏳</div>'
      + '<div style="font-size:20px;font-weight:700;margin-bottom:8px;">Running Backtest...</div>'
      + '<div style="font-size:14px;color:var(--muted);margin-bottom:20px;">' + _escapeHtml(payload.run_name) + ' • ' + _escapeHtml(payload.symbol) + ' • ' + _escapeHtml(payload.candle_interval) + '</div>'
      + '<div style="font-size:42px;font-weight:800;font-family:JetBrains Mono;color:var(--accent);">' + sec + 's</div>'
      + '<div style="font-size:12px;color:var(--muted);margin-top:8px;">Fetching candle data & computing indicators...</div>'
      + '</td></tr>';
  }
  renderCountdown(countdownSec);
  var countdownTimer = setInterval(function() {
    countdownSec--;
    if (countdownSec > 0) renderCountdown(countdownSec);
    else {
      clearInterval(countdownTimer);
      countdownEl.innerHTML = '<tr><td colspan="10" style="text-align:center;padding:60px 20px;">'
        + '<div style="font-size:48px;margin-bottom:16px;">⏳</div>'
        + '<div style="font-size:20px;font-weight:700;margin-bottom:8px;">Still running...</div>'
        + '<div style="font-size:14px;color:var(--muted);">Processing large dataset, please wait...</div>'
        + '</td></tr>';
    }
  }, 1000);

  try {
    const r = await cfApiFetch('/api/backtest', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    clearInterval(countdownTimer);

    if (!r.ok) {
      var errText = '';
      try { var ed = await r.json(); errText = ed.message || ed.detail || r.statusText; } catch(_) { errText = r.status + ' ' + r.statusText; }
      await cfAlert('Backtest failed: ' + errText, 'Error', '❌');
      await loadRuns();
      return;
    }

    const d = await r.json();

    if (d.status === 'success') {
      if (d.strategy_warnings && d.strategy_warnings.length) {
        d.strategy_warnings.forEach(function(w) { cfToast(w, 'warning'); });
      }
      if (d.model_assumptions && d.model_assumptions.length) {
        d.model_assumptions.forEach(function(line) { cfToast(line, 'info'); });
      }
      var pnl = d.stats.total_pnl || 0;
      var pnlColor = pnl >= 0 ? 'color:var(--green)' : 'color:var(--red)';
      var assumptionsHtml = '';
      var diagHtml = '';
      if (d.model_assumptions && d.model_assumptions.length) {
        assumptionsHtml = '<div style="margin-top:14px;padding:12px;background:rgba(110,170,255,0.08);border:1px solid rgba(110,170,255,0.18);border-radius:8px;text-align:left;">'
          + '<div style="font-size:12px;font-weight:700;color:var(--accent);margin-bottom:6px;">Model Assumptions</div>'
          + d.model_assumptions.map(function(line) {
              return '<div style="font-size:11px;font-family:JetBrains Mono,monospace;color:var(--muted);padding:2px 0;">' + _escapeHtml(line) + '</div>';
            }).join('')
          + '</div>';
      }
      if ((d.stats.total_trades || 0) === 0 && d.diagnostics && d.diagnostics.length) {
        diagHtml = '<div style="margin-top:14px;padding:12px;background:rgba(255,100,100,0.08);border:1px solid rgba(255,100,100,0.2);border-radius:8px;text-align:left;">'
          + '<div style="font-size:12px;font-weight:700;color:var(--red);margin-bottom:6px;">Diagnostics (0 trades):</div>'
          + d.diagnostics.map(function(line) {
              var safeLine = _escapeHtml(line)
                .replace(/NOT IN DF/g, '<span style="color:var(--red);font-weight:700;">NOT IN DF</span>')
                .replace(/MISSING/g, '<span style="color:var(--red);font-weight:700;">MISSING</span>');
              return '<div style="font-size:11px;font-family:JetBrains Mono,monospace;color:var(--muted);padding:2px 0;">' + safeLine + '</div>';
            }).join('')
          + '</div>';
      }
      await cfModal('Backtest Complete', { html: '<div style="font-size:15px;margin-bottom:10px;">'
        + '<b>' + (d.stats.total_trades || 0) + '</b> trades executed</div>'
        + '<div style="font-size:22px;font-weight:800;font-family:JetBrains Mono;' + pnlColor + '">' + fmtINR(pnl) + '</div>'
        + '<div style="margin-top:6px;font-size:12px;">Win Rate: ' + (d.stats.win_rate || 0) + '% • Max DD: ' + (d.stats.max_drawdown || 0).toFixed(1) + '% • Sharpe: ' + (parseFloat(d.stats.sharpe_ratio) || 0).toFixed(2) + '</div>'
        + assumptionsHtml
        + diagHtml },
        '📊', [{label:'View Results', cls:'btn-primary'}]);
      await loadRuns();
      if (d.run_id) { viewRun(d.run_id); }
    } else {
      await cfAlert('Backtest failed: ' + (d.message || 'Unknown error'), 'Error', '❌');
      await loadRuns();
    }
  } catch(e) {
    clearInterval(countdownTimer);
    await cfAlert('Error: ' + e.message, 'Error', '❌');
    await loadRuns();
  } finally {
    _backtestRunning = false;
    btn.disabled = false; btn.textContent = '⚡ Run Backtest'; btn.classList.remove('loading');
  }
}

// ── Results (All Runs: Backtest / Paper / Live) ───────────
let _backtestRunning = false;
let _currentRunFilter = 'all';
let _runsSortCol = 'created_at';
let _runsSortAsc = false;
let _shSortCol = 'exit_time';
let _shSortAsc = false;
let _mktSortCol = 'rank';
let _mktSortAsc = true;
const TABLE_PAGE_SIZE = 10;
const _tablePagerState = {};
let _brokerConnected = false;
let _brokerInfo = {
  currentBroker: 'delta',
  currentLabel: 'Broker',
  configured: false,
  feedKind: 'polling',
  availableBrokers: [],
  switchable: true,
  runtimeLocks: {},
};

function _safeDomId(value) {
  return String(value || 'table').replace(/[^a-zA-Z0-9_-]+/g, '-');
}

function _setTablePage(tableId, page, stateKey, pagerId) {
  _tablePagerState[stateKey || tableId] = page;
  _renderTablePager(tableId, stateKey, pagerId);
}

function _renderTablePager(tableId, stateKey, pagerId) {
  var table = document.getElementById(tableId);
  if (!table) return;
  var tbody = table.querySelector('tbody');
  if (!tbody) return;
  var rows = Array.from(tbody.querySelectorAll('tr'));
  var pagerKey = stateKey || tableId;
  var hostId = pagerId || (tableId + '-pagination');
  var host = document.getElementById(hostId);
  if (!host) {
    host = document.createElement('div');
    host.id = hostId;
    host.className = 'pagination-bar';
    var anchor = table.parentElement || table;
    anchor.insertAdjacentElement('afterend', host);
  }
  if (!rows.length || (rows.length === 1 && rows[0].querySelector('td[colspan]'))) {
    rows.forEach(function(row) { row.style.display = ''; });
    host.style.display = 'none';
    host.innerHTML = '';
    return;
  }
  var total = rows.length;
  var totalPages = Math.max(1, Math.ceil(total / TABLE_PAGE_SIZE));
  var page = _tablePagerState[pagerKey] || 1;
  if (page > totalPages) page = totalPages;
  if (page < 1) page = 1;
  _tablePagerState[pagerKey] = page;
  var start = (page - 1) * TABLE_PAGE_SIZE;
  rows.forEach(function(row, idx) {
    row.style.display = idx >= start && idx < start + TABLE_PAGE_SIZE ? '' : 'none';
  });
  var shown = total <= TABLE_PAGE_SIZE ? Math.min(TABLE_PAGE_SIZE, total) : Math.min(start + TABLE_PAGE_SIZE, total) - start;
  var info = total <= TABLE_PAGE_SIZE
    ? (shown + ' shown of ' + total)
    : ('Showing ' + (start + 1) + '-' + Math.min(start + TABLE_PAGE_SIZE, total) + ' of ' + total);
  var btns = '';
  if (totalPages > 1) {
    btns += '<button class="page-btn" data-cf-click="_setTablePage(\'' + tableId + '\',1,\'' + pagerKey + '\',\'' + hostId + '\')" ' + (page <= 1 ? 'disabled' : '') + '>«</button>';
    btns += '<button class="page-btn" data-cf-click="_setTablePage(\'' + tableId + '\',' + (page - 1) + ',\'' + pagerKey + '\',\'' + hostId + '\')" ' + (page <= 1 ? 'disabled' : '') + '>‹</button>';
    for (var p = Math.max(1, page - 2); p <= Math.min(totalPages, page + 2); p++) {
      btns += '<button class="page-btn ' + (p === page ? 'active' : '') + '" data-cf-click="_setTablePage(\'' + tableId + '\',' + p + ',\'' + pagerKey + '\',\'' + hostId + '\')">' + p + '</button>';
    }
    btns += '<button class="page-btn" data-cf-click="_setTablePage(\'' + tableId + '\',' + (page + 1) + ',\'' + pagerKey + '\',\'' + hostId + '\')" ' + (page >= totalPages ? 'disabled' : '') + '>›</button>';
    btns += '<button class="page-btn" data-cf-click="_setTablePage(\'' + tableId + '\',' + totalPages + ',\'' + pagerKey + '\',\'' + hostId + '\')" ' + (page >= totalPages ? 'disabled' : '') + '>»</button>';
  }
  host.style.display = 'flex';
  host.innerHTML = '<div class="pagination-info">' + info + '</div><div class="pagination-actions">' + btns + '</div>';
}

function _resultsUsesTradeView() {
  return _currentRunFilter === 'paper' || _currentRunFilter === 'live' || _currentRunFilter === 'scalp';
}

function _renderResultsHead() {
  var head = document.getElementById('results-head');
  if (!head) return;
  if (_resultsUsesTradeView()) {
    head.innerHTML = '<tr>'
      + '<th>Run</th><th>Trade</th><th>Entry At</th><th>Exit At</th><th>Side</th>'
      + '<th class="num">Entry $</th><th class="num">Exit $</th><th class="num">P&amp;L</th><th>Reason</th><th>Action</th>'
      + '</tr>';
    return;
  }
  head.innerHTML = '<tr>'
    + '<th style="width:30px;"><input type="checkbox" class="tbl-cb" data-cf-change="toggleAllRunsCb(this)"></th>'
    + '<th class="sortable-th" data-cf-click="sortRunsTable(\'mode\')">Mode <span class="sort-icon rt-sort" id="rts-mode">▲</span></th>'
    + '<th class="sortable-th" data-cf-click="sortRunsTable(\'run_name\')">Name <span class="sort-icon rt-sort" id="rts-run_name">▲</span></th>'
    + '<th class="sortable-th" data-cf-click="sortRunsTable(\'symbol\')">Symbol <span class="sort-icon rt-sort" id="rts-symbol">▲</span></th>'
    + '<th class="sortable-th num" data-cf-click="sortRunsTable(\'leverage\')">Lev <span class="sort-icon rt-sort" id="rts-leverage">▲</span></th>'
    + '<th class="sortable-th num" data-cf-click="sortRunsTable(\'trade_count\')">Trades <span class="sort-icon rt-sort" id="rts-trade_count">▲</span></th>'
    + '<th class="sortable-th num" data-cf-click="sortRunsTable(\'win_rate\')">Win Rate <span class="sort-icon rt-sort" id="rts-win_rate">▲</span></th>'
    + '<th class="sortable-th num" data-cf-click="sortRunsTable(\'total_pnl\')">P&amp;L <span class="sort-icon rt-sort" id="rts-total_pnl">▲</span></th>'
    + '<th class="sortable-th" data-cf-click="sortRunsTable(\'created_at\')">Date <span class="sort-icon rt-sort" id="rts-created_at">▲</span></th>'
    + '<th>Actions</th>'
    + '</tr>';
}

function _flattenRunTrades(runs) {
  var rows = [];
  var seen = {};
  runs.forEach(function(run) {
    (run.trades || []).forEach(function(trade, idx) {
      var row = {
        run_id: run.id,
        run_name: run.run_name || ('Run #' + run.id),
        mode: _normalizeMode(run.mode),
        symbol: trade.symbol || run.symbol || '—',
        leverage: trade.leverage || run.leverage || 1,
        trade_id: trade.id || trade.trade_id || (idx + 1),
        entry_time: trade.entry_time,
        exit_time: trade.exit_time,
        entry_price: parseFloat(trade.entry_price) || 0,
        exit_price: parseFloat(trade.exit_price) || 0,
        side: trade.side || run.trade_side || '—',
        pnl: parseFloat(trade.pnl) || 0,
        exit_reason: trade.exit_reason || '—',
      };
      var signature = [
        row.mode,
        row.symbol,
        row.side,
        row.entry_time || '',
        row.exit_time || '',
        row.entry_price,
        row.exit_price,
        row.pnl,
        row.exit_reason || '',
      ].join('|');
      if (!seen[signature]) {
        seen[signature] = true;
        rows.push(row);
      }
    });
  });
  rows.sort(function(a, b) {
    return String(b.exit_time || b.entry_time || '').localeCompare(String(a.exit_time || a.entry_time || ''));
  });
  return rows;
}

function _buildRunTradeRows(runs) {
  var trades = _flattenRunTrades(runs);
  if (!trades.length) {
    return '<tr><td colspan="10" style="text-align:center;padding:30px;color:var(--muted);">No completed trades found for this mode.</td></tr>';
  }
  return trades.map(function(t) {
    var entryParts = _getTradeDateParts(t.entry_time);
    var exitParts = _getTradeDateParts(t.exit_time);
    var pnlClass = t.pnl >= 0 ? 'positive' : 'negative';
    var runName = _escapeHtml(t.run_name || 'Run');
    var mode = _escapeHtml(t.mode || '');
    var symbol = _escapeHtml(t.symbol || '—');
    return '<tr>'
      + '<td><div class="table-row-label">' + runName + '</div><div class="table-note">' + mode + ' • ' + symbol + '</div></td>'
      + '<td><div class="table-row-label">' + symbol + '</div><div class="table-note">#' + _escapeHtml(t.trade_id || '—') + ' • ' + (t.leverage || 1) + 'x</div></td>'
      + '<td><div class="table-datetime"><div class="table-datetime-date">' + entryParts.date + '</div><div class="table-datetime-time">' + entryParts.time + '</div></div></td>'
      + '<td><div class="table-datetime"><div class="table-datetime-date">' + exitParts.date + '</div><div class="table-datetime-time">' + exitParts.time + '</div></div></td>'
      + '<td><span class="tag ' + ((t.side || '').toUpperCase() === 'LONG' ? 'tag-green' : 'tag-red') + '">' + _escapeHtml(t.side || '—') + '</span></td>'
      + '<td class="num"><div class="table-value-stack"><div class="table-value-main">' + fmtINRPrice(t.entry_price || 0) + '</div></div></td>'
      + '<td class="num"><div class="table-value-stack"><div class="table-value-main">' + fmtINRPrice(t.exit_price || 0) + '</div></div></td>'
      + '<td class="num"><div class="table-value-stack"><div class="table-value-main ' + pnlClass + '">' + fmtINR(t.pnl || 0) + '</div><div class="table-value-sub ' + pnlClass + '">' + (t.pnl >= 0 ? 'profit' : 'loss') + '</div></div></td>'
      + '<td>' + reasonTag(t.exit_reason || '—') + '</td>'
      + '<td><button class="action-icon-btn delete" data-cf-click="deleteRun(' + t.run_id + ')" title="Delete Run">🗑</button></td>'
      + '</tr>';
  }).join('');
}

function _getFilterPillClass(key) {
  const k = (key || 'all').toLowerCase();
  const map = {
    all: 'filter-pill-all',
    backtest: 'filter-pill-backtest',
    paper: 'filter-pill-paper',
    live: 'filter-pill-live',
    scalp: 'filter-pill-scalp',
    long: 'filter-pill-long',
    short: 'filter-pill-short',
    wins: 'filter-pill-wins',
    losses: 'filter-pill-losses',
  };
  return map[k] || 'filter-pill-all';
}

function _getModeBadge(mode) {
  const m = (mode || 'backtest').toLowerCase();
  if (m === 'scalp') return '<span class="tag tag-cyan">Scalp</span>';
  if (m === 'paper') return '<span class="tag tag-yellow">Paper</span>';
  if (m === 'live' || m === 'auto' || m === 'real') return '<span class="tag tag-purple">Live</span>';
  return '<span class="tag tag-blue">Backtest</span>';
}

function _normalizeMode(mode) {
  const m = (mode || 'backtest').toLowerCase();
  if (m === 'scalp') return 'scalp';
  if (m === 'live' || m === 'auto' || m === 'real') return 'live';
  if (m === 'paper') return 'paper';
  return 'backtest';
}

function _buildRunCards(runs) {
  if (!runs.length) return '<tr><td colspan="10" style="text-align:center;padding:30px;color:var(--muted);">No runs found.</td></tr>';
  // Sort
  var sorted = [...runs];
  sorted.sort(function(a, b) {
    var va, vb;
    if (_runsSortCol === 'win_rate') { va = a.stats?.win_rate || 0; vb = b.stats?.win_rate || 0; }
    else { va = a[_runsSortCol]; vb = b[_runsSortCol]; }
    if (va == null) va = ''; if (vb == null) vb = '';
    if (typeof va === 'number' && typeof vb === 'number') return _runsSortAsc ? va - vb : vb - va;
    va = String(va); vb = String(vb);
    return _runsSortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
  });
  return sorted.map(r => {
    const pnl = r.total_pnl || 0;
    const pnlColor = pnl > 0 ? 'var(--green)' : pnl < 0 ? 'var(--red)' : 'var(--muted)';
    const wr = r.stats?.win_rate || 0;
    const wrColor = wr >= 50 ? 'var(--green)' : wr > 0 ? 'var(--red)' : 'var(--muted)';
    const dt = _getTradeDateParts(r.created_at || r.started_at || '');
    const isBacktest = _normalizeMode(r.mode) === 'backtest';
    const runName = _escapeHtml(r.run_name || ('Run #' + r.id));
    const symbol = _escapeHtml(r.symbol || '—');
    return `<tr${isBacktest ? ' style="cursor:pointer;" data-cf-click="viewRun(' + r.id + ')"' : ''}>
      <td data-cf-click="event.stopPropagation()"><input type="checkbox" class="tbl-cb run-cb" value="${r.id}" data-cf-change="updateRunsBulk()"></td>
      <td>${_getModeBadge(r.mode)}</td>
      <td><div class="table-row-label">${runName}</div><div class="table-note">${symbol} • ${r.trade_count || 0} trades</div></td>
      <td><div class="table-row-label">${symbol}</div><div class="table-note">${_escapeHtml(String(r.timeframe || 'active'))}</div></td>
      <td class="num"><div class="table-value-stack"><div class="table-value-main">${r.leverage || 1}x</div><div class="table-value-sub">leverage</div></div></td>
      <td class="num"><div class="table-value-stack"><div class="table-value-main">${r.trade_count || 0}</div><div class="table-value-sub">closed</div></div></td>
      <td class="num"><div class="table-value-stack"><div class="table-value-main" style="color:${wrColor};">${wr.toFixed(1)}%</div><div class="table-value-sub">win rate</div></div></td>
      <td class="num"><div class="table-value-stack"><div class="table-value-main" style="color:${pnlColor};">${pnl >= 0 ? '+' : ''}${fmtINR(pnl)}</div><div class="table-value-sub">net result</div></div></td>
      <td><div class="table-datetime"><strong>${dt.date}</strong><span>${dt.time || '—'}</span></div></td>
      <td style="white-space:nowrap;">
        <div class="action-icon-group">
          ${isBacktest ? '<button class="action-icon-btn view" data-cf-click="event.stopPropagation();viewRun(' + r.id + ')" title="View">👁</button><button class="action-icon-btn edit" data-cf-click="event.stopPropagation();copyEditRun(' + r.id + ')" title="Copy & Edit">📝</button>' : ''}
          <button class="action-icon-btn delete" data-cf-click="event.stopPropagation();deleteRun(${r.id})" title="Delete">🗑</button>
        </div>
      </td>
    </tr>`;
  }).join('');
}

function sortRunsTable(col) {
  if (_resultsUsesTradeView()) return;
  if (_runsSortCol === col) { _runsSortAsc = !_runsSortAsc; } else { _runsSortCol = col; _runsSortAsc = true; }
  document.querySelectorAll('.rt-sort').forEach(el => { el.textContent = '▲'; el.classList.remove('active'); });
  var icon = document.getElementById('rts-' + col);
  if (icon) { icon.textContent = _runsSortAsc ? '▲' : '▼'; icon.classList.add('active'); }
  _renderFilteredRuns();
}

function toggleAllRunsCb(master) {
  if (_resultsUsesTradeView()) return;
  document.querySelectorAll('.run-cb').forEach(cb => { cb.checked = master.checked; });
  updateRunsBulk();
}

function updateRunsBulk() {
  var checked = document.querySelectorAll('.run-cb:checked');
  var bar = document.getElementById('runs-bulk-bar');
  var count = document.getElementById('runs-bulk-count');
  if (_resultsUsesTradeView()) {
    bar.style.display = 'none';
    return;
  }
  if (checked.length > 0) {
    bar.style.display = 'flex';
    count.textContent = checked.length + ' selected';
  } else {
    bar.style.display = 'none';
  }
}

async function bulkDeleteRuns() {
  var ids = Array.from(document.querySelectorAll('.run-cb:checked')).map(cb => parseInt(cb.value));
  if (!ids.length) return;
  var ok = await cfConfirm('Delete ' + ids.length + ' selected runs? This cannot be undone.', 'Bulk Delete?', '🗑️');
  if (!ok) return;
  try {
    for (var id of ids) {
      await cfApiFetch('/api/runs/' + id, { method: 'DELETE' });
    }
    cfToast(ids.length + ' runs deleted', 'success');
    loadRuns();
  } catch(e) { cfToast('Delete failed: ' + e.message, 'error'); }
}

function filterRuns(mode, btn) {
  _currentRunFilter = mode;
  document.querySelectorAll('.runs-filter-btn').forEach(b => {
    b.classList.toggle('active', b === btn);
  });
  _renderFilteredRuns();
}

function _renderFilteredRuns() {
  const cont = document.getElementById('results-list');
  if (!cont) return;
  _renderResultsHead();
  var compareBtn = document.getElementById('results-compare-btn');
  if (compareBtn) compareBtn.style.display = _resultsUsesTradeView() ? 'none' : '';
  if (_resultsUsesTradeView()) {
    document.getElementById('runs-bulk-bar').style.display = 'none';
    document.getElementById('compare-panel').style.display = 'none';
  }
  let filtered = _allRunsCache;
  if (_currentRunFilter !== 'all') {
    filtered = _allRunsCache.filter(r => _normalizeMode(r.mode) === _currentRunFilter);
  }
  if (!filtered.length) {
    cont.innerHTML = '<tr><td colspan="' + (_resultsUsesTradeView() ? '10' : '10') + '" style="text-align:center;padding:30px;color:var(--muted);">No ' + (_currentRunFilter === 'all' ? '' : _currentRunFilter + ' ') + (_resultsUsesTradeView() ? 'trades yet.' : 'runs yet.') + '</td></tr>';
  } else {
    cont.innerHTML = _resultsUsesTradeView() ? _buildRunTradeRows(filtered) : _buildRunCards(filtered);
  }
  var master = document.querySelector('#runs-table thead .tbl-cb');
  if (master) master.checked = false;
  updateRunsBulk();
  _renderTablePager('runs-table', 'runs-table', 'runs-table-pagination');
}

function _renderPortfolioPaperRuns(runs) {
  const container = document.getElementById('portfolio-paper-runs');
  const empty = document.getElementById('portfolio-paper-runs-empty');
  if (!container) return;
  const paperRuns = runs.filter(r => _normalizeMode(r.mode) === 'paper');
  if (!paperRuns.length) {
    if (empty) empty.style.display = 'block';
    container.innerHTML = '';
    return;
  }
  if (empty) empty.style.display = 'none';
  container.innerHTML = paperRuns.map(r => {
    const pnl = r.total_pnl || 0;
    const pnlColor = pnl > 0 ? 'var(--green)' : pnl < 0 ? 'var(--red)' : 'var(--muted)';
    const wr = r.stats?.win_rate || 0;
    const wrColor = wr >= 50 ? 'var(--green)' : wr > 0 ? 'var(--red)' : 'var(--muted)';
    const runName = _escapeHtml(r.run_name || ('Run #' + r.id));
    const symbol = _escapeHtml(r.symbol || '—');
    const dt = _getTradeDateParts(r.created_at || r.started_at || '');
    return `<tr style="cursor:pointer;" data-cf-click="viewRun(${r.id})" data-cf-mouseover="this.style.background='rgba(139,92,246,0.04)'" data-cf-mouseout="this.style.background=''">
      <td><div class="table-row-label">${runName}</div><div class="table-note">${symbol} • paper run</div></td>
      <td><div class="table-row-label">${symbol}</div><div class="table-note">${_escapeHtml(String(r.side || 'Both'))}</div></td>
      <td class="num"><div class="table-value-stack"><div class="table-value-main">${r.leverage || 1}x</div><div class="table-value-sub">leverage</div></div></td>
      <td class="num"><div class="table-value-stack"><div class="table-value-main">${r.trade_count || 0}</div><div class="table-value-sub">closed</div></div></td>
      <td class="num"><div class="table-value-stack"><div class="table-value-main" style="color:${wrColor};">${wr}%</div><div class="table-value-sub">win rate</div></div></td>
      <td class="num"><div class="table-value-stack"><div class="table-value-main" style="color:${pnlColor};">${fmtINR(pnl)}</div><div class="table-value-sub">net result</div></div></td>
      <td><div class="table-datetime"><strong>${dt.date}</strong><span>${dt.time || '—'}</span></div></td>
    </tr>`;
  }).join('');
  _renderTablePager('portfolio-paper-runs-table', 'portfolio-paper-runs-table', 'portfolio-paper-runs-pagination');
}

async function loadRuns() {
  try {
    const r = await fetch('/api/runs', { credentials: 'same-origin' });
    const runs = await r.json();
    _allRunsCache = runs.slice().reverse();
    closeRunModal();

    // Apply current filter
    _renderFilteredRuns();

    // Update portfolio paper runs
    _renderPortfolioPaperRuns(_allRunsCache);
    renderResultsOverview(_allRunsCache);
  } catch(e) { console.error(e); }
}

async function viewRun(rid) {
  currentRunId = rid;
  try {
    const r = await fetch(`/api/runs/${rid}`, { credentials: 'same-origin' });
    const run = await r.json();

    const mode = (run.mode || 'backtest').toLowerCase();
    const isScalp = mode === 'scalp';
    const showAnalysis = mode === 'backtest' || mode === 'live' || mode === 'auto';

    document.getElementById('rd-title').textContent =
      `${run.run_name || 'Run'} — ${run.symbol || ''} ${run.leverage || 1}x`;

    // Show/hide equity curve and analytics sections
    document.getElementById('rd-equity-wrap').style.display = showAnalysis ? 'block' : 'none';
    document.getElementById('rd-analysis-extra').style.display = showAnalysis ? 'block' : 'none';

    // ── Stats area ──────────────────────────────────────────
    if (isScalp) {
      const t = (run.trades || [])[0] || {};
      const pnl = run.total_pnl || t.pnl || 0;
      const pnlColor = pnl >= 0 ? 'var(--green)' : 'var(--red)';
      const row = (label, val) =>
        `<div style="padding:10px 0;border-bottom:1px solid var(--border);">
          <div style="color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:3px;">${_escapeHtml(label)}</div>
          <div style="font-weight:600;font-size:14px;">${_escapeHtml(val)}</div>
        </div>`;
      document.getElementById('rd-stats').innerHTML =
        `<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:0 24px;font-size:13px;margin-bottom:16px;">
          ${row('Symbol', t.symbol || run.symbol || '—')}
          ${row('Side', t.side || run.trade_side || '—')}
          ${row('Leverage', (t.leverage || run.leverage || 1) + '×')}
          ${row('Entry Price', t.entry_price ? fmtINRPrice(t.entry_price) : '—')}
          ${row('Exit Price', t.exit_price ? fmtINRPrice(t.exit_price) : '—')}
          ${row('Qty (USDT)', t.qty_usdt ? '$' + t.qty_usdt : '—')}
          ${row('Entry Time', fmtDt(t.entry_time) || '—')}
          ${row('Exit Time', fmtDt(t.exit_time) || '—')}
          ${row('Exit Reason', t.exit_reason || '—')}
          ${row('Execution Mode', t.mode || 'paper')}
          ${row('Profit Lock', t.target_usd ? '$' + t.target_usd : '—')}
          ${row('Risk Cap', t.sl_usd ? '$' + t.sl_usd : '—')}
          ${(t.guardrail_price || t.sl_price) ? row('Guardrail Price', fmtINRPrice(t.guardrail_price || t.sl_price)) : ''}
          <div style="padding:10px 0;border-bottom:1px solid var(--border);grid-column:span 3;">
            <div style="color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:3px;">P&amp;L</div>
            <div style="font-weight:700;font-size:22px;font-family:'JetBrains Mono',monospace;color:${pnlColor};">${fmtINR(pnl)}</div>
          </div>
        </div>`;
    } else {
      // Paper / Backtest / Live — aggregate stats grid
      const s = run.stats || {};
      document.getElementById('rd-stats').innerHTML = [
        ['Total Trades', s.total_trades || 0],
        ['Win Rate', (s.win_rate || 0) + '%'],
        ['Total P&L', fmtINR(s.total_pnl || 0), s.total_pnl >= 0],
        ['Profit Factor', (parseFloat(s.profit_factor) || 0).toFixed(2)],
        ['Max Drawdown', (s.max_drawdown || 0).toFixed(1) + '%'],
        ['Sharpe Ratio', (parseFloat(s.sharpe_ratio) || 0).toFixed(2)],
        ['Calmar Ratio', (parseFloat(s.calmar_ratio) || 0).toFixed(2)],
        ['Expectancy', fmtINR(parseFloat(s.expectancy) || 0)],
        ['Avg Duration', s.avg_trade_duration || '—'],
        ['Return', (s.total_return_pct || 0).toFixed(1) + '%'],
        ['Avg Win', fmtINR(s.avg_win || 0)],
        ['Avg Loss', fmtINR(s.avg_loss || 0)],
        ['Fees Paid', fmtINR(s.total_fees || 0)],
        ['Execution Drag', fmtINR(s.total_execution_cost || 0)],
        ['Funding', fmtINR(s.total_funding || 0)],
        ['Final Capital', fmtINR(s.final_capital || s.initial_capital || 0)],
      ].map(([label, val, isGreen]) => `
        <div class="stat-box">
          <div class="stat-label">${label}</div>
          <div class="stat-value" ${isGreen !== undefined ? `style="color:${isGreen?'var(--green)':'var(--red)'}"` : ''}>${val}</div>
        </div>
      `).join('');
    }

    // ── Equity chart (backtest/live only) ───────────────────
    if (showAnalysis) {
      const eq = run.equity || [];
      const eqDiv = document.getElementById('rd-equity');
      if (eq.length > 1) {
        const values = eq.map(e => e.value);
        const minV = Math.min(...values);
        const maxV = Math.max(...values);
        const range = maxV - minV || 1;
        const padL = 70, padR = 10, padT = 16, padB = 10;
        const W = 1000, H = 280;
        const cW = W - padL - padR, cH = H - padT - padB;
        const pts = values.map((v, i) => `${padL + (i / (values.length - 1)) * cW},${padT + cH - ((v - minV) / range) * cH}`).join(' ');
        const fillPts = `${padL},${padT + cH} ${pts} ${padL + cW},${padT + cH}`;
        const eqColor = values[values.length - 1] >= values[0] ? '#22c55e' : '#ef4444';
        const labelFill = getComputedStyle(document.documentElement).getPropertyValue('--muted').trim() || 'rgba(255,255,255,0.4)';
        let yLabels = '';
        for (let i = 0; i <= 4; i++) {
          const v = minV + (range * i / 4);
          const y = padT + cH - (cH * i / 4);
          yLabels += '<text x="' + (padL - 8) + '" y="' + (y + 4) + '" text-anchor="end" fill="' + labelFill + '" font-size="11" font-family="JetBrains Mono">' + fmtINR(v, 0) + '</text>';
          yLabels += '<line x1="' + padL + '" y1="' + y + '" x2="' + (W - padR) + '" y2="' + y + '" stroke="rgba(128,128,128,0.08)"/>';
        }
        eqDiv.innerHTML = '<h3 style="margin-bottom:8px;">Equity Curve</h3>'
          + '<svg viewBox="0 0 ' + W + ' ' + H + '" preserveAspectRatio="none" style="width:100%;height:260px;display:block;">'
          + '<defs><linearGradient id="eqg" x1="0" y1="0" x2="0" y2="1">'
          + '<stop offset="0%" stop-color="' + eqColor + '" stop-opacity="0.18"/>'
          + '<stop offset="100%" stop-color="' + eqColor + '" stop-opacity="0"/>'
          + '</linearGradient></defs>'
          + yLabels
          + '<polygon points="' + fillPts + '" fill="url(#eqg)"/>'
          + '<polyline points="' + pts + '" fill="none" stroke="' + eqColor + '" stroke-width="2.5" stroke-linejoin="round"/>'
          + '</svg>';
      } else {
        eqDiv.innerHTML = '<h3 style="margin-bottom:8px;">Equity Curve</h3>'
          + '<div style="text-align:center;padding:60px 0;color:var(--muted);">Not enough data for equity chart</div>';
      }

      // Monthly breakdown
      const monthly = run.monthly || [];
      document.querySelector('#rd-monthly tbody').innerHTML = monthly.map(m => `
        <tr>
          <td>${m.month}</td><td>${m.trades}</td><td>${m.wins||0}</td><td>${m.losses||0}</td>
          <td style="color:${m.pnl>=0?'var(--green)':'var(--red)'}">${fmtINR(m.pnl||0)}</td>
        </tr>
      `).join('');
      _renderTablePager('rd-monthly', 'rd-monthly', 'rd-monthly-pagination');
      renderDOW(run.day_of_week || []);
      renderYearlyBreakdown(run.yearly || []);
      renderHeatmap(monthly);
    }

    // ── Trade log (always) ──────────────────────────────────
    _currentRunSymbol = run.symbol || '';
    _currentTrades = (run.trades || []).map(t => ({...t}));
    _tradesSortCol = 'id';
    _tradesSortAsc = true;
    renderTradesPage(1);

    // ── Open modal ──────────────────────────────────────────
    const modal = document.getElementById('run-detail-modal');
    modal.style.display = 'flex';
    modal.querySelector('[style*="overflow-y:auto"]').scrollTop = 0;
    document.body.style.overflow = 'hidden';

  } catch(e) { console.error(e); cfAlert('Failed to load run details.', 'Error', '❌'); }
}

// ── Trade Log: Full Dates, Sort, Pagination ────────────────
function fmtDt(dtStr) {
  return _fmtTradeDateTime(dtStr);
}
window.fmtDt = fmtDt;

function reasonTag(reason) {
  if (!reason) return '—';
  var cls = 'tag-purple';
  var r = reason.toLowerCase();
  if (r.includes('stop') || r.includes('liquidat')) cls = 'tag-red';
  else if (r.includes('profit') || r.includes('target')) cls = 'tag-green';
  else if (r.includes('signal')) cls = 'tag-yellow';
  return '<span class="tag ' + cls + '">' + _escapeHtml(reason) + '</span>';
}

function sortTrades(col) {
  if (_tradesSortCol === col) {
    _tradesSortAsc = !_tradesSortAsc;
  } else {
    _tradesSortCol = col;
    _tradesSortAsc = true;
  }
  // Update sort icons
  document.querySelectorAll('.sort-icon').forEach(el => { el.textContent = '▲'; el.classList.remove('active'); });
  var icon = document.getElementById('sort-' + col);
  if (icon) { icon.textContent = _tradesSortAsc ? '▲' : '▼'; icon.classList.add('active'); }
  renderTradesPage(1);
}

function renderTradesPage(page) {
  _tradesPage = page;
  var perPage = TABLE_PAGE_SIZE;
  var sorted = [..._currentTrades];

  // Sort
  sorted.sort(function(a, b) {
    var va = a[_tradesSortCol], vb = b[_tradesSortCol];
    if (typeof va === 'number' && typeof vb === 'number') return _tradesSortAsc ? va - vb : vb - va;
    va = String(va || ''); vb = String(vb || '');
    return _tradesSortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
  });

  var total = sorted.length;
  var totalPages = Math.ceil(total / perPage) || 1;
  if (page > totalPages) page = totalPages;
  var start = (page - 1) * perPage;
  var slice = sorted.slice(start, start + perPage);

  var tbody = document.querySelector('#rd-trades tbody');
  tbody.innerHTML = slice.map(function(t) {
    var entryParts = _getTradeDateParts(t.entry_time);
    var exitParts = _getTradeDateParts(t.exit_time);
    var pnl = t.pnl != null ? t.pnl : 0;
    var pnlClass = pnl >= 0 ? 'positive' : 'negative';
    var symbol = _escapeHtml(t.symbol || _currentRunSymbol || '');
    var tradeId = _escapeHtml(t.id || '—');
    var side = _escapeHtml(t.side || '');
    return '<tr>'
      + '<td><div class="table-row-label">' + symbol + '</div><div class="table-note">trade #' + tradeId + '</div></td>'
      + '<td><div class="table-datetime"><div class="table-datetime-date">' + entryParts.date + '</div><div class="table-datetime-time">' + entryParts.time + '</div></div></td>'
      + '<td><div class="table-datetime"><div class="table-datetime-date">' + exitParts.date + '</div><div class="table-datetime-time">' + exitParts.time + '</div></div></td>'
      + '<td><span class="tag ' + (t.side === 'LONG' ? 'tag-green' : 'tag-red') + '">' + side + '</span></td>'
      + '<td><div class="table-value-stack"><div class="table-value-main">' + fmtINRPrice(t.entry_price || 0) + '</div></div></td>'
      + '<td><div class="table-value-stack"><div class="table-value-main">' + fmtINRPrice(t.exit_price || 0) + '</div></div></td>'
      + '<td><div class="table-value-stack"><div class="table-value-main ' + pnlClass + '">' + fmtINR(pnl) + '</div><div class="table-value-sub ' + pnlClass + '">' + (pnl >= 0 ? 'profit' : 'loss') + '</div></div></td>'
      + '<td>' + reasonTag(t.exit_reason) + '</td>'
      + '</tr>';
  }).join('');

  // Pagination controls
  var pag = document.getElementById('trades-pagination');
  var info = 'Showing ' + (total ? start + 1 : 0) + '-' + Math.min(start + perPage, total) + ' of ' + total;
  var btns = '';
  btns += '<button class="page-btn" data-cf-click="renderTradesPage(1)" ' + (page <= 1 ? 'disabled' : '') + '>«</button>';
  btns += '<button class="page-btn" data-cf-click="renderTradesPage(' + (page - 1) + ')" ' + (page <= 1 ? 'disabled' : '') + '>‹</button>';
  var startP = Math.max(1, page - 2), endP = Math.min(totalPages, page + 2);
  for (var p = startP; p <= endP; p++) {
    btns += '<button class="page-btn ' + (p === page ? 'active' : '') + '" data-cf-click="renderTradesPage(' + p + ')">' + p + '</button>';
  }
  btns += '<button class="page-btn" data-cf-click="renderTradesPage(' + (page + 1) + ')" ' + (page >= totalPages ? 'disabled' : '') + '>›</button>';
  btns += '<button class="page-btn" data-cf-click="renderTradesPage(' + totalPages + ')" ' + (page >= totalPages ? 'disabled' : '') + '>»</button>';
  pag.innerHTML = '<div class="pagination-info">' + info + '</div><div class="pagination-controls">' + btns + '</div>';
}

function closeRunModal() {
  document.getElementById('run-detail-modal').style.display = 'none';
  document.body.style.overflow = '';
}
function closeDetail() { closeRunModal(); }

async function deleteRun(rid) {
  var ok = await cfConfirm('This will permanently delete this run. This cannot be undone.', 'Delete Run?', '🗑️');
  if (!ok) return;
  try {
    await cfApiFetch(`/api/runs/${rid}`, { method: 'DELETE' });
    cfToast('Run deleted', 'success');
    loadRuns();
  } catch(e) { cfToast('Delete failed: ' + e.message, 'error'); }
}

function exportCSV() {
  if (currentRunId) window.open(`/api/runs/${currentRunId}/csv`, '_blank');
}

// ── Market Table Sort ──────────────────────────────────
function sortMarketTable(col) {
  if (!window._marketCoins || !window._marketCoins.length) return;
  if (_mktSortCol === col) { _mktSortAsc = !_mktSortAsc; } else { _mktSortCol = col; _mktSortAsc = true; }
  document.querySelectorAll('.mkt-sort').forEach(el => { el.textContent = '▲'; el.classList.remove('active'); });
  var icon = document.getElementById('mkts-' + col);
  if (icon) { icon.textContent = _mktSortAsc ? '▲' : '▼'; icon.classList.add('active'); }
  window._marketCoins.sort(function(a, b) {
    var va = a[col], vb = b[col];
    if (va == null) va = ''; if (vb == null) vb = '';
    if (typeof va === 'number' && typeof vb === 'number') return _mktSortAsc ? va - vb : vb - va;
    return _mktSortAsc ? String(va).localeCompare(String(vb)) : String(vb).localeCompare(String(va));
  });
  _renderMarketRows(window._marketCoins);
}

function _renderMarketRows(coins) {
  var tbody = document.getElementById('market-tbody');
  if (!tbody) return;
  tbody.innerHTML = coins.map(function(c) {
    var chgColor = c.change_24h >= 0 ? 'var(--green)' : 'var(--red)';
    var chgSign = c.change_24h >= 0 ? '+' : '';
    var athPct = c.ath_change_pct || 0;
    var tradeSym = c.trade_symbol || (c.symbol + 'USDT');
    var safeName = (c.name || '').replace(/'/g, "\'");
    var safeSymbol = _escapeHtml(c.symbol);
    var safeCoinName = _escapeHtml(c.name);
    var tradeable = c.broker_tradeable != null ? !!c.broker_tradeable : !!c.delta_tradeable;
    var btnClass = tradeable ? 'btn-live' : 'btn-bt';
    var btnLabel = tradeable ? '⚡ Trade' : '📊 Backtest';
    var tradeBtn = '<button class="mkt-trade-btn ' + btnClass + '" data-cf-click="selectCryptoFromMarket(\'' + tradeSym + '\',\'' + safeName + '\')">' + btnLabel + '</button>';
    return '<tr style="cursor:pointer;" data-cf-mouseover="this.style.background=\'rgba(139,92,246,0.04)\'" data-cf-mouseout="this.style.background=\'\'">' +
      '<td style="padding-left:16px;"><div class="table-value-stack"><div class="table-value-main">#' + c.rank + '</div><div class="table-value-sub">rank</div></div></td>' +
      '<td><div style="display:flex;align-items:center;gap:10px;">' +
        '<img src="' + c.image + '" alt="' + safeSymbol + '" width="28" height="28" style="border-radius:50%;" data-cf-error="this.style.display=\'none\'">' +
        '<div><div class="table-row-label">' + safeSymbol + '</div>' +
        '<div class="table-note">' + safeCoinName + '</div></div></div></td>' +
      '<td class="num"><div class="table-value-stack"><div class="table-value-main">' + fmtPrice(c.price) + '</div><div class="table-value-sub">last price</div></div></td>' +
      '<td class="num"><div class="table-value-stack"><div class="table-value-main" style="color:' + chgColor + ';">' + chgSign + c.change_24h.toFixed(2) + '%</div><div class="table-value-sub">24h move</div></div></td>' +
      '<td class="num"><div class="table-value-stack"><div class="table-value-main">' + fmtNum(c.volume_24h) + '</div><div class="table-value-sub">24h volume</div></div></td>' +
      '<td class="num"><div class="table-value-stack"><div class="table-value-main">' + fmtNum(c.market_cap) + '</div><div class="table-value-sub">market cap</div></div></td>' +
      '<td class="num"><div class="table-value-stack"><div class="table-value-main">' + fmtPrice(c.ath) + '</div><div class="table-value-sub" style="color:' + (athPct > -20 ? 'var(--green)' : 'var(--red)') + '">' + athPct.toFixed(1) + '% vs ATH</div></div></td>' +
      '<td style="text-align:center;">' + tradeBtn + '</td></tr>';
  }).join('');
  _renderTablePager('market-table', 'market-table', 'market-table-pagination');
}

// ── Scalp History Sort ──────────────────────────────────
function sortScalpHistory(col) {
  if (_shSortCol === col) { _shSortAsc = !_shSortAsc; } else { _shSortCol = col; _shSortAsc = true; }
  document.querySelectorAll('.sh-sort').forEach(el => { el.textContent = '▲'; el.classList.remove('active'); });
  var icon = document.getElementById('shs-' + col);
  if (icon) { icon.textContent = _shSortAsc ? '▲' : '▼'; icon.classList.add('active'); }
  // Re-render the scalp history with new sort
  var allTrades = Array.from(_cfTradeCache.values());
  allTrades.sort(function(a, b) {
    var va, vb;
    if (col === 'net_pnl') {
      var fa = a.fees || (a.size ? Math.round(a.size * 0.0005 * 1.18 * 2 * 10000) / 10000 : 0);
      var fb = b.fees || (b.size ? Math.round(b.size * 0.0005 * 1.18 * 2 * 10000) / 10000 : 0);
      va = a.net_pnl !== undefined ? a.net_pnl : (a.pnl || 0) - fa;
      vb = b.net_pnl !== undefined ? b.net_pnl : (b.pnl || 0) - fb;
    } else if (col === 'fees') {
      va = a.fees || (a.size ? Math.round(a.size * 0.0005 * 1.18 * 2 * 10000) / 10000 : 0);
      vb = b.fees || (b.size ? Math.round(b.size * 0.0005 * 1.18 * 2 * 10000) / 10000 : 0);
    } else if (col === 'exit_time') {
      va = a.exit_time || a.entry_time || '';
      vb = b.exit_time || b.entry_time || '';
    } else {
      va = a[col]; vb = b[col];
    }
    if (va == null) va = ''; if (vb == null) vb = '';
    if (typeof va === 'number' && typeof vb === 'number') return _shSortAsc ? va - vb : vb - va;
    return _shSortAsc ? String(va).localeCompare(String(vb)) : String(vb).localeCompare(String(va));
  });
  var body = document.getElementById('cf-scalp-history-body');
  if (body) {
    if (!allTrades.length) {
      body.innerHTML = '<tr><td colspan="9" class="cf-table-empty-cell">No trades yet</td></tr>';
    } else {
      body.innerHTML = allTrades.map(_cfTradeRow).join('');
    }
  }
  _renderTablePager('cf-scalp-history-table', 'cf-scalp-history-table', 'cf-scalp-history-pagination');
}

// ── Generic Table Sort (for portfolio tables) ──────────
var _genSortState = {};
function sortGenericTable(tablePrefix, colIdx) {
  var key = tablePrefix + '-' + colIdx;
  if (_genSortState[tablePrefix + '-col'] === colIdx) {
    _genSortState[tablePrefix + '-asc'] = !_genSortState[tablePrefix + '-asc'];
  } else {
    _genSortState[tablePrefix + '-col'] = colIdx;
    _genSortState[tablePrefix + '-asc'] = true;
  }
  var asc = _genSortState[tablePrefix + '-asc'];
  // Update icons
  var sortClass = tablePrefix.replace('pf-', 'pf-').replace('positions', 'pos').replace('orders', 'ord') + '-sort';
  document.querySelectorAll('.' + sortClass).forEach(el => { el.textContent = '▲'; el.classList.remove('active'); });
  // Find the nth sort icon
  var icons = document.querySelectorAll('.' + sortClass);
  if (icons[colIdx]) { icons[colIdx].textContent = asc ? '▲' : '▼'; icons[colIdx].classList.add('active'); }
  // Sort tbody rows in-place
  var tableId = tablePrefix === 'pf-positions' ? 'pf-positions-table' : 'pf-orders-table';
  var tbody = document.querySelector('#' + tableId + ' tbody');
  if (!tbody) return;
  var rows = Array.from(tbody.querySelectorAll('tr'));
  if (rows.length <= 1 && rows[0]?.querySelector('td[colspan]')) return; // empty state
  rows.sort(function(a, b) {
    var cellA = a.cells[colIdx], cellB = b.cells[colIdx];
    if (!cellA || !cellB) return 0;
    var va = cellA.textContent.replace(/[$₹,%x]/g, '').replace(/,/g, '').trim();
    var vb = cellB.textContent.replace(/[$₹,%x]/g, '').replace(/,/g, '').trim();
    var na = parseFloat(va), nb = parseFloat(vb);
    if (!isNaN(na) && !isNaN(nb)) return asc ? na - nb : nb - na;
    return asc ? va.localeCompare(vb) : vb.localeCompare(va);
  });
  rows.forEach(r => tbody.appendChild(r));
  _renderTablePager(tableId, tableId, tableId + '-pagination');
}

// ── Save Strategy ──────────────────────────────────────────
async function saveStrategy() {
  const payload = {
    run_name: document.getElementById('b-name').value,
    symbol: selectedCrypto,
    leverage: selectedLeverage,
    trade_side: selectedSide,
    indicators: indicators,
    entry_conditions: entryConditions,
    exit_conditions: exitConditions,
    stoploss_pct: parseFloat(document.getElementById('b-sl').value),
    target_profit_pct: parseFloat(document.getElementById('b-tp').value),
    trailing_sl_pct: parseFloat(document.getElementById('b-trail').value) || 0,
    ...cfBuilderCostModel(),
    compounding: document.getElementById('b-compounding').checked,
    max_trades_per_day: parseInt(document.getElementById('b-maxtrades').value) || 5,
    folder: getSelectedFolder(),
    initial_capital: parseFloat(document.getElementById('b-capital').value) || 10000,
    position_size_pct: document.getElementById('b-possize-mode').value === 'pct' ? (parseFloat(document.getElementById('b-possize').value) || 100) : 100,
    position_size_mode: document.getElementById('b-possize-mode').value,
    fixed_qty: document.getElementById('b-possize-mode').value === 'fixed_qty' ? (parseFloat(document.getElementById('b-possize').value) || 0.1) : 0,
    candle_interval: document.getElementById('b-interval').value,
  };
  try {
    await cfApiFetch('/api/strategies', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    cfToast('Strategy saved!', 'success');
    fetchStrategies();
  } catch(e) { cfToast('Save failed: ' + e.message, 'error'); }
}

// ── Live / Paper Trading ───────────────────────────────────

// Populate the paper trading strategy dropdown
function populatePaperStrategyDropdown() {
  var sel = document.getElementById('paper-strat-select');
  if (!sel) return;
  var current = sel.value;
  sel.innerHTML = '<option value="">— Select a saved strategy —</option>';
  // Add Builder option
  sel.innerHTML += '<option value="__builder__">📝 Use Current Builder Settings</option>';
  // Add saved strategies
  if (_savedStrategies && _savedStrategies.length) {
    _savedStrategies.forEach(function(s) {
      var lbl = (s.run_name || 'Untitled') + ' — ' + (s.symbol || '') + ' ' + (s.leverage || 1) + 'x ' + (s.trade_side || '');
      sel.innerHTML += '<option value="' + s.id + '">' + lbl + '</option>';
    });
  }
  if (current) sel.value = current;
}

async function startPaperFromSelector() {
  var sel = document.getElementById('paper-strat-select');
  var choice = sel ? sel.value : '';
  if (!choice) {
    cfToast('Please select a strategy first', 'warning');
    return;
  }
  if (choice === '__builder__') {
    // Validate builder has indicators/conditions
    if (!indicators || indicators.length === 0) {
      await cfAlert('No indicators configured in Builder. Please add at least one indicator first.', 'Missing Indicators', '⚠️');
      return;
    }
    if (!entryConditions || entryConditions.length === 0) {
      var ok = await cfConfirm('No entry conditions set. The engine will use the default (Close > EMA_20). Continue?', 'No Entry Conditions', '⚠️');
      if (!ok) return;
    }
    return startPaper();
  }
  // Load from saved strategy
  var s = _savedStrategies.find(function(x) { return x.id == choice; });
  if (!s) { cfToast('Strategy not found', 'error'); return; }
  // Validate
  if (!s.indicators || s.indicators.length === 0) {
    await cfAlert('This strategy has no indicators. Please edit it first in the Builder.', 'Missing Indicators', '⚠️');
    return;
  }
  // Load strategy into builder vars (for startPaper to read)
  document.getElementById('b-name').value = s.run_name || 'Paper';
  selectedCrypto = s.symbol || 'BTCUSDT';
  selectedSide = s.trade_side || 'LONG';
  selectedLeverage = s.leverage || 10;
  indicators = [...(s.indicators || [])];
  entryConditions = (s.entry_conditions || []).map(function(c) { return {...c}; });
  exitConditions = (s.exit_conditions || []).map(function(c) { return {...c}; });
  if (s.stoploss_pct != null) document.getElementById('b-sl').value = s.stoploss_pct;
  if (s.target_profit_pct != null) document.getElementById('b-tp').value = s.target_profit_pct;
  cfApplyBuilderCostModel(s);
  document.getElementById('b-compounding').checked = !!s.compounding;
  if (s.trailing_sl_pct != null) document.getElementById('b-trail').value = s.trailing_sl_pct;
  if (s.max_trades_per_day != null) document.getElementById('b-maxtrades').value = s.max_trades_per_day;
  if (s.initial_capital != null) document.getElementById('b-capital').value = s.initial_capital;
  if (s.position_size_mode === 'fixed_qty') {
    document.getElementById('b-possize-mode').value = 'fixed_qty';
    document.getElementById('b-possize').value = s.fixed_qty || 0.1;
    document.getElementById('b-possize').step = '0.01';
    document.getElementById('b-possize-label').textContent = 'Qty';
  } else {
    document.getElementById('b-possize-mode').value = 'pct';
    if (s.position_size_pct != null) document.getElementById('b-possize').value = s.position_size_pct;
    document.getElementById('b-possize').step = '1';
    document.getElementById('b-possize-label').textContent = '%';
  }
  if (s.candle_interval) document.getElementById('b-interval').value = s.candle_interval;
  cfToast('Loaded strategy: ' + (s.run_name || 'Untitled'), 'info');
  return startPaper();
}

async function startPaper() {
  var btn = document.getElementById('paper-start-btn');
  if (btn) { btn.classList.add('loading'); btn.disabled = true; }
  const payload = {
    run_name: document.getElementById('b-name').value || 'Paper',
    symbol: selectedCrypto,
    leverage: selectedLeverage,
    trade_side: selectedSide,
    indicators: indicators,
    entry_conditions: entryConditions,
    exit_conditions: exitConditions,
    initial_capital: parseFloat(document.getElementById('b-capital').value) || 10000,
    stoploss_pct: parseFloat(document.getElementById('b-sl').value) || 5,
    target_profit_pct: parseFloat(document.getElementById('b-tp').value) || 10,
    trailing_sl_pct: parseFloat(document.getElementById('b-trail').value) || 0,
    ...cfBuilderCostModel(),
    compounding: document.getElementById('b-compounding').checked,
    max_trades_per_day: parseInt(document.getElementById('b-maxtrades').value) || 5,
    position_size_pct: document.getElementById('b-possize-mode').value === 'pct' ? (parseFloat(document.getElementById('b-possize').value) || 100) : 100,
    position_size_mode: document.getElementById('b-possize-mode').value,
    fixed_qty: document.getElementById('b-possize-mode').value === 'fixed_qty' ? (parseFloat(document.getElementById('b-possize').value) || 0.1) : 0,
    candle_interval: document.getElementById('b-interval').value,
    order_type: document.getElementById('deploy-order-type') ? document.getElementById('deploy-order-type').value : 'market',
    margin_mode: document.getElementById('deploy-margin-mode') ? document.getElementById('deploy-margin-mode').value : 'cross',
    sl_reference: document.getElementById('deploy-sl-ref') ? document.getElementById('deploy-sl-ref').value : 'signal',
    tp_reference: document.getElementById('deploy-tp-ref') ? document.getElementById('deploy-tp-ref').value : 'signal',
  };
  try {
    const r = await cfApiFetch('/api/paper/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const d = await r.json();
    if (d.status === 'ok' || d.status === 'started') {
      if (d.warnings && d.warnings.length) {
        d.warnings.forEach(function(w) { cfToast(w, 'warning'); });
      }
      cfToast('Paper trading started: ' + payload.symbol + ' ' + payload.leverage + 'x', 'success');
      // Navigate to live page and start monitoring
      setTimeout(function() {
        showPage('live-page', document.getElementById('nav-live'));
        startLiveMonitor();
      }, 800);
    } else if (d.status === 'already_running') {
      cfToast('Paper engine already running.', 'warning');
      showPage('live-page', document.getElementById('nav-live'));
    } else {
      await cfAlert(d.message || d.status, 'Paper Start', '⚠️');
    }
    loadLiveMonitor(); // trigger immediate refresh
  } catch(e) { await cfAlert(e.message, 'Error', '❌'); }
  finally { if (btn) { btn.classList.remove('loading'); btn.disabled = false; } }
}

async function stopPaper() {
  const ok = await cfConfirm('Stop the paper trading engine?', 'Stop Paper', '⏹️');
  if (!ok) return;
  await cfApiFetch('/api/paper/stop', { method: 'POST' });
  cfToast('Paper engine stopped', 'info');
  loadLiveMonitor();
}

async function startLive() {
  const ok = await cfConfirm('This will place <b>REAL orders</b> on ' + _escapeHtml(_brokerLabel()) + ' with <b>real funds</b>.<br><br>Symbol: <b>' + _escapeHtml(selectedCrypto) + '</b><br>Leverage: <b>' + selectedLeverage + 'x</b><br><br>Are you sure?', 'Go Live', '⚠️', true);
  if (!ok) return;
  const payload = {
    run_name: document.getElementById('b-name').value || 'Live',
    symbol: selectedCrypto,
    leverage: selectedLeverage,
    trade_side: selectedSide,
    indicators: indicators,
    entry_conditions: entryConditions,
    exit_conditions: exitConditions,
    initial_capital: parseFloat(document.getElementById('b-capital').value) || 10000,
    stoploss_pct: parseFloat(document.getElementById('b-sl').value) || 5,
    target_profit_pct: parseFloat(document.getElementById('b-tp').value) || 10,
    trailing_sl_pct: parseFloat(document.getElementById('b-trail').value) || 0,
    ...cfBuilderCostModel(),
    compounding: document.getElementById('b-compounding').checked,
    max_trades_per_day: parseInt(document.getElementById('b-maxtrades').value) || 5,
    position_size_pct: document.getElementById('b-possize-mode').value === 'pct' ? (parseFloat(document.getElementById('b-possize').value) || 100) : 100,
    position_size_mode: document.getElementById('b-possize-mode').value,
    fixed_qty: document.getElementById('b-possize-mode').value === 'fixed_qty' ? (parseFloat(document.getElementById('b-possize').value) || 0.1) : 0,
    candle_interval: document.getElementById('b-interval').value,
    order_type: document.getElementById('deploy-order-type') ? document.getElementById('deploy-order-type').value : 'market',
    margin_mode: document.getElementById('deploy-margin-mode') ? document.getElementById('deploy-margin-mode').value : 'cross',
    sl_reference: document.getElementById('deploy-sl-ref') ? document.getElementById('deploy-sl-ref').value : 'signal',
    tp_reference: document.getElementById('deploy-tp-ref') ? document.getElementById('deploy-tp-ref').value : 'signal',
  };
  try {
    const r = await cfApiFetch('/api/live/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const d = await r.json();
    if (d.status === 'ok' || d.status === 'started') {
      if (d.warnings && d.warnings.length) {
        d.warnings.forEach(function(w) { cfToast(w, 'warning'); });
      }
      cfToast('LIVE trading started: ' + selectedCrypto + ' ' + selectedLeverage + 'x', 'success');
      showPage('live-page', document.getElementById('nav-live'));
    } else if (d.status === 'already_running') {
      cfToast('Live engine already running.', 'warning');
      showPage('live-page', document.getElementById('nav-live'));
    } else {
      await cfAlert(d.message || d.status, 'Live Start', '⚠️');
    }
    startLiveMonitor();
  } catch(e) { await cfAlert(e.message, 'Error', '❌'); }
}

async function stopLive() {
  const ok = await cfConfirm('Stop the <b>LIVE</b> trading engine? Open positions will remain.', 'Stop Live', '⏹️', true);
  if (!ok) return;
  await cfApiFetch('/api/live/stop', { method: 'POST' });
  cfToast('Live engine stopped', 'info');
  loadLiveMonitor();
}

// Legacy panel functions (no longer needed — single dynamic panel)
function resetPaperPanel() {}
function resetLivePanel() {}
function appendLog() {} // Event log now rendered by renderLivePanel
function updatePanel() {} // Stats now rendered by renderLivePanel

async function pollLiveStatus() {
  try {
    const [paperR, liveR] = await Promise.all([
      fetch('/api/paper/status', { credentials: 'same-origin' }),
      fetch('/api/live/status', { credentials: 'same-origin' }),
    ]);
    const paper = await paperR.json();
    const live = await liveR.json();

    // Kill switch visibility
    var killBtn = document.getElementById('kill-switch-btn');
    if (killBtn) killBtn.classList.toggle('hidden', !paper.running && !live.running);

    // Live tab dot
    var dot = document.getElementById('live-tab-dot');
    if (dot) dot.classList.toggle('active', paper.running || live.running);

  } catch(e) { console.error('pollLiveStatus error:', e); }
}

// ── Ticker Auto-refresh ────────────────────────────────────
async function refreshTopbarTicker() {
  try {
    // Use CoinGecko data for topbar prices (more reliable)
    const r = await fetch('/api/market/top25', { credentials: 'same-origin' });
    const d = await r.json();
    if (d.status === 'ok' && d.coins) {
      const btc = d.coins.find(c => c.symbol === 'BTC');
      const eth = d.coins.find(c => c.symbol === 'ETH');
      const sol = d.coins.find(c => c.symbol === 'SOL');
      if (btc) updateTopbarTicker('btc', btc.price, btc.change_24h);
      if (eth) updateTopbarTicker('eth', eth.price, eth.change_24h);
      if (sol) updateTopbarTicker('sol', sol.price, sol.change_24h);
    }
  } catch(e) {}

  // Funding rates from Delta
  try {
    const r2 = await fetch('/api/ticker', { credentials: 'same-origin' });
    const d2 = await r2.json();
    if (d2.status === 'ok' && d2.tickers) {
      const btc = d2.tickers['BTCUSDT'] || {};
      const eth = d2.tickers['ETHUSDT'] || {};
      if (btc.funding_rate) document.getElementById('fund-btc').textContent = (btc.funding_rate * 100).toFixed(4) + '%';
      if (eth.funding_rate) document.getElementById('fund-eth').textContent = (eth.funding_rate * 100).toFixed(4) + '%';
    }
  } catch(e) {}
}

// ── WebSocket ──────────────────────────────────────────────
function connectWS() {
  try {
    if (_cfAppSocket && (_cfAppSocket.readyState === WebSocket.OPEN || _cfAppSocket.readyState === WebSocket.CONNECTING)) {
      return;
    }
    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    const ws = new WebSocket(`${proto}://${location.host}/ws`);
    _cfAppSocket = ws;
    ws.onopen = () => {
      _cfAppSocketConnected = true;
    };
    ws.onmessage = (e) => {
      let data;
      try { data = JSON.parse(e.data); } catch(err) { console.warn('WS parse error:', err); return; }
      // Handle periodic status updates from server (multi-engine)
      if (data.type === 'status') {
        // Update engines if we get full engine data
        if (data.engines) {
          _liveEngines = data.engines;
          if (document.getElementById('live-page').classList.contains('active-page')) {
            renderLiveTabs();
            if (_liveSelectedTab >= 0 && _liveSelectedTab < _liveEngines.length) {
              renderLivePanel(_liveEngines[_liveSelectedTab], _liveSelectedTab);
            }
          }
        }
        var killBtn = document.getElementById('kill-switch-btn');
        if (killBtn) killBtn.classList.toggle('hidden', !data.paper_running && !data.live_running);
        var dot = document.getElementById('live-tab-dot');
        if (dot) dot.classList.toggle('active', data.paper_running || data.live_running);
        return;
      }
      if (data.source === 'scalp' && data.type === 'scalp_status') {
        _cfScalpLastWsUpdateAt = Date.now();
        cfMergeScalpStatusPatch(data.status || {});
        cfApplyScalpStatus(_cfLatestScalpStatus);
        return;
      }
      // Trade events — trigger immediate refresh
      if (data.source === 'live' || data.source === 'paper') {
        var srcLabel = data.source === 'paper' ? '📄 Paper' : '🔴 Live';
        if (data.type === 'entry') {
          sendTradeNotification(srcLabel + ': Entry', (data.trade ? data.trade.side + ' ' + (data.trade.symbol||'') + ' @ ' + fmtINRPrice(parseFloat(data.trade.entry_price)||0) : 'New trade entry'));
        } else if (data.type === 'exit') {
          var pnl = data.trade ? (parseFloat(data.trade.pnl)||0).toFixed(2) : '0.00';
          sendTradeNotification(srcLabel + ': Exit', (data.trade ? (data.trade.symbol||'') + ' P&L: $' + pnl : 'Trade closed'));
        }
        // Immediate refresh of the live panel
        loadLiveMonitor();
      }
    };
    ws.onerror = () => {
      _cfAppSocketConnected = false;
    };
    ws.onclose = () => {
      if (_cfAppSocket === ws) _cfAppSocket = null;
      _cfAppSocketConnected = false;
      _cfScalpLastWsUpdateAt = 0;
      var scalpPage = document.getElementById('scalp-page');
      if (scalpPage && scalpPage.classList.contains('active-page')) {
        setTimeout(cfLoadScalpStatus, 250);
      }
      setTimeout(connectWS, 5000);
    };
  } catch(e) {}
}

// ── Strategy Templates ─────────────────────────────────────
const STRATEGY_TEMPLATES = {
  btc_supertrend: {
    name: 'BTC Supertrend Rider',
    icon: '🔥',
    desc: 'Trend-following strategy using Supertrend on BTC. Rides momentum with ATR-based stops.',
    tags: ['Trend', 'BTC', 'Medium TF'],
    symbol: 'BTCUSDT',
    leverage: 10,
    side: 'LONG',
    indicators: ['Supertrend_10_3.0_15m', 'ATR_14_15m'],
    entry: [{ left: 'current_close', operator: 'crosses_above', right: 'Supertrend_10_3.0_15m', connector: 'AND', right_number_value: '0' }],
    exit: [{ left: 'current_close', operator: 'crosses_below', right: 'Supertrend_10_3.0_15m', connector: 'AND', right_number_value: '0' }],
    sl: 3, tp: 8, interval: '15m',
  },
  eth_ema_cross: {
    name: 'ETH EMA Crossover',
    icon: '💎',
    desc: 'Classic EMA 9/21 crossover on ETH. Fast signals with tight risk management.',
    tags: ['Crossover', 'ETH', 'Scalp'],
    symbol: 'ETHUSDT',
    leverage: 20,
    side: 'LONG',
    indicators: ['EMA_9_5m', 'EMA_21_5m'],
    entry: [{ left: 'EMA_9_5m', operator: 'crosses_above', right: 'EMA_21_5m', connector: 'AND', right_number_value: '0' }],
    exit: [{ left: 'EMA_9_5m', operator: 'crosses_below', right: 'EMA_21_5m', connector: 'AND', right_number_value: '0' }],
    sl: 2, tp: 5, interval: '5m',
  },
  sol_rsi_reversal: {
    name: 'SOL RSI Reversal',
    icon: '⚡',
    desc: 'Mean-reversion on SOL using RSI oversold bounces with BB confirmation.',
    tags: ['Reversal', 'SOL', 'Mean Revert'],
    symbol: 'SOLUSDT',
    leverage: 15,
    side: 'LONG',
    indicators: ['RSI_14_15m', 'BB_20_2.0_15m'],
    entry: [
      { left: 'RSI_14_15m', operator: 'crosses_above', right: 'number', connector: 'AND', right_number_value: '30' },
      { left: 'current_close', operator: 'is_below', right: 'BB_20_2.0_15m__lower', connector: 'AND', right_number_value: '0' },
    ],
    exit: [{ left: 'RSI_14_15m', operator: 'is_above', right: 'number', connector: 'AND', right_number_value: '70' }],
    sl: 4, tp: 12, interval: '15m',
  },
  multi_macd_momentum: {
    name: 'MACD Momentum',
    icon: '📊',
    desc: 'MACD histogram momentum with VWAP filter. Works on any coin with good volume.',
    tags: ['Momentum', 'Any Coin', 'Intraday'],
    symbol: 'BTCUSDT',
    leverage: 10,
    side: 'LONG',
    indicators: ['MACD_12_26_9_5m', 'VWAP_5m'],
    entry: [
      { left: 'MACD_12_26_9_5m__histogram', operator: 'crosses_above', right: 'number', connector: 'AND', right_number_value: '0' },
      { left: 'current_close', operator: 'is_above', right: 'VWAP_5m', connector: 'AND', right_number_value: '0' },
    ],
    exit: [{ left: 'MACD_12_26_9_5m__histogram', operator: 'crosses_below', right: 'number', connector: 'AND', right_number_value: '0' }],
    sl: 2.5, tp: 7, interval: '5m',
  },
  cpr_breakout: {
    name: 'CPR Breakout',
    icon: '🎯',
    desc: 'Central Pivot Range breakout strategy. Catches strong directional moves from support/resistance.',
    tags: ['Breakout', 'CPR', 'Swing'],
    symbol: 'BTCUSDT',
    leverage: 5,
    side: 'LONG',
    indicators: ['CPR_Day_0.2_0.5', 'EMA_50_60m'],
    entry: [
      { left: 'current_close', operator: 'crosses_above', right: 'CPR_TC', connector: 'AND', right_number_value: '0' },
      { left: 'current_close', operator: 'is_above', right: 'EMA_50_60m', connector: 'AND', right_number_value: '0' },
    ],
    exit: [{ left: 'current_close', operator: 'crosses_below', right: 'CPR_Pivot', connector: 'AND', right_number_value: '0' }],
    sl: 3, tp: 10, interval: '1h',
  },
  stochrsi_scalp: {
    name: 'StochRSI Scalp',
    icon: '🏎️',
    desc: 'Fast scalping with StochRSI crossovers. Short timeframe, quick entries/exits.',
    tags: ['Scalp', 'Fast', 'Any Coin'],
    symbol: 'ETHUSDT',
    leverage: 25,
    side: 'LONG',
    indicators: ['StochRSI_14_1m', 'EMA_9_1m'],
    entry: [
      { left: 'StochRSI_14_1m__K', operator: 'crosses_above', right: 'StochRSI_14_1m__D', connector: 'AND', right_number_value: '0' },
      { left: 'current_close', operator: 'is_above', right: 'EMA_9_1m', connector: 'AND', right_number_value: '0' },
    ],
    exit: [{ left: 'StochRSI_14_1m__K', operator: 'crosses_below', right: 'StochRSI_14_1m__D', connector: 'AND', right_number_value: '0' }],
    sl: 1.5, tp: 4, interval: '1m',
  },
};

function renderTemplates() {
  var grid = document.getElementById('template-grid');
  if (!grid) return;
  grid.innerHTML = Object.keys(STRATEGY_TEMPLATES).map(function(key) {
    var t = STRATEGY_TEMPLATES[key];
    return '<div class="template-card" data-cf-click="loadTemplate(\'' + key + '\')">' +
      '<div class="tc-icon">' + t.icon + '</div>' +
      '<div class="tc-name">' + t.name + '</div>' +
      '<div class="tc-desc">' + t.desc + '</div>' +
      '<div class="tc-tags">' + t.tags.map(function(tag) { return '<span class="tc-tag">' + tag + '</span>'; }).join('') + '</div>' +
      '</div>';
  }).join('');
}

function loadTemplate(key) {
  var t = STRATEGY_TEMPLATES[key];
  if (!t) return;
  // Set builder state
  selectedCrypto = t.symbol;
  selectedLeverage = t.leverage;
  selectedSide = t.side;
  indicators = [...t.indicators];
  entryConditions = t.entry.map(function(c) { return {...c}; });
  exitConditions = t.exit.map(function(c) { return {...c}; });
  // Update UI
  document.getElementById('b-name').value = t.name;
  document.getElementById('b-sl').value = t.sl;
  document.getElementById('b-tp').value = t.tp;
  document.getElementById('b-interval').value = t.interval;
  // Navigate to builder
  showPage('builder-page', document.getElementById('nav-builder'));
  initCryptoSelector();
  fetchLeverage(t.symbol);
  setSide(t.side);
  renderIndicators();
  renderConditions('entry');
  renderConditions('exit');
  renderBuilderDeck();
  cfToast('Template loaded: ' + t.name, 'success');
}

// ── Backtest Comparison ────────────────────────────────────
var _allRunsCache = [];

function toggleCompare() {
  var panel = document.getElementById('compare-panel');
  panel.style.display = panel.style.display === 'none' ? 'block' : 'none';
  if (panel.style.display !== 'none') populateCompareDropdowns();
}

async function populateCompareDropdowns() {
  try {
    var r = await fetch('/api/runs', { credentials: 'same-origin' });
    _allRunsCache = await r.json();
    var compareRuns = _allRunsCache.filter(function(run) { return _normalizeMode(run.mode) === 'backtest'; });
    var opts = '<option value="">Select run...</option>';
    compareRuns.forEach(function(run) {
      opts += '<option value="' + run.id + '">' + _escapeHtml(run.run_name || 'Run #' + run.id) +
        ' — $' + (run.total_pnl || 0).toFixed(2) + '</option>';
    });
    document.getElementById('compare-a').innerHTML = opts;
    document.getElementById('compare-b').innerHTML = opts;
  } catch(e) { cfToast('Failed to load runs', 'error'); }
}

async function runComparison() {
  var aId = document.getElementById('compare-a').value;
  var bId = document.getElementById('compare-b').value;
  if (!aId || !bId) { document.getElementById('compare-results').innerHTML = ''; return; }
  if (aId === bId) { document.getElementById('compare-results').innerHTML = '<div style="color:var(--muted);text-align:center;padding:20px;">Select two different runs to compare.</div>'; return; }
  try {
    var [ra, rb] = await Promise.all([
      fetch('/api/runs/' + aId, { credentials: 'same-origin' }).then(function(r) { return r.json(); }),
      fetch('/api/runs/' + bId, { credentials: 'same-origin' }).then(function(r) { return r.json(); }),
    ]);
    var sa = ra.stats || {};
    var sb = rb.stats || {};
    var metrics = [
      ['Total Trades', sa.total_trades, sb.total_trades],
      ['Win Rate', (sa.win_rate || 0) + '%', (sb.win_rate || 0) + '%'],
      ['Total P&L', '$' + (sa.total_pnl || 0).toFixed(2), '$' + (sb.total_pnl || 0).toFixed(2), sa.total_pnl, sb.total_pnl],
      ['Profit Factor', (parseFloat(sa.profit_factor) || 0).toFixed(2), (parseFloat(sb.profit_factor) || 0).toFixed(2)],
      ['Max Drawdown', (sa.max_drawdown || 0).toFixed(1) + '%', (sb.max_drawdown || 0).toFixed(1) + '%'],
      ['Sharpe Ratio', (parseFloat(sa.sharpe_ratio) || 0).toFixed(2), (parseFloat(sb.sharpe_ratio) || 0).toFixed(2)],
      ['Calmar Ratio', (parseFloat(sa.calmar_ratio) || 0).toFixed(2), (parseFloat(sb.calmar_ratio) || 0).toFixed(2)],
      ['Expectancy', '$' + (parseFloat(sa.expectancy) || 0).toFixed(2), '$' + (parseFloat(sb.expectancy) || 0).toFixed(2)],
      ['Return', (sa.total_return_pct || 0).toFixed(1) + '%', (sb.total_return_pct || 0).toFixed(1) + '%', sa.total_return_pct, sb.total_return_pct],
      ['Avg Win', '$' + (sa.avg_win || 0).toFixed(2), '$' + (sb.avg_win || 0).toFixed(2)],
      ['Avg Loss', '$' + (sa.avg_loss || 0).toFixed(2), '$' + (sb.avg_loss || 0).toFixed(2)],
      ['Fees', '$' + (sa.total_fees || 0).toFixed(2), '$' + (sb.total_fees || 0).toFixed(2)],
      ['Execution Drag', '$' + (sa.total_execution_cost || 0).toFixed(2), '$' + (sb.total_execution_cost || 0).toFixed(2)],
      ['Funding', '$' + (sa.total_funding || 0).toFixed(2), '$' + (sb.total_funding || 0).toFixed(2)],
    ];

    var html = '<div class="compare-grid"><div class="compare-col">' +
      '<h4>' + _escapeHtml(ra.run_name || 'Run A') + '</h4>';
    metrics.forEach(function(m) {
      var cls = m[3] !== undefined ? (m[3] >= 0 ? ' positive' : ' negative') : '';
      html += '<div class="compare-row"><span class="cr-label">' + m[0] + '</span><span class="cr-val' + cls + '">' + m[1] + '</span></div>';
    });
    html += '</div><div class="compare-col"><h4>' + _escapeHtml(rb.run_name || 'Run B') + '</h4>';
    metrics.forEach(function(m) {
      var cls = m[4] !== undefined ? (m[4] >= 0 ? ' positive' : ' negative') : '';
      html += '<div class="compare-row"><span class="cr-label">' + m[0] + '</span><span class="cr-val' + cls + '">' + m[2] + '</span></div>';
    });
    html += '</div></div>';
    document.getElementById('compare-results').innerHTML = html;
  } catch(e) { cfToast('Comparison failed: ' + e.message, 'error'); }
}

// ── Day of Week Rendering ──────────────────────────────────
function renderDOW(dowData) {
  var cont = document.getElementById('rd-dow');
  if (!cont) return;
  if (!dowData || dowData.length === 0) {
    cont.innerHTML = '<div style="color:var(--muted);text-align:center;padding:20px;">No day-of-week data available.</div>';
    return;
  }
  var dayOrder = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'];
  var dayShort = { Monday: 'MON', Tuesday: 'TUE', Wednesday: 'WED', Thursday: 'THU', Friday: 'FRI', Saturday: 'SAT', Sunday: 'SUN' };
  var byDay = {};
  dowData.forEach(function(d) { byDay[d.day] = d; });
  cont.innerHTML = dayOrder.map(function(day) {
    var d = byDay[day] || { day: day, trades: 0, pnl: 0 };
    var color = d.pnl >= 0 ? 'var(--green)' : 'var(--red)';
    var bgAlpha = d.pnl >= 0 ? 'rgba(34,197,94,0.06)' : 'rgba(239,68,68,0.06)';
    return '<div class="dow-bar" style="background:' + bgAlpha + '">' +
      '<div class="dow-day">' + (dayShort[day] || day.slice(0,3).toUpperCase()) + '</div>' +
      '<div class="dow-pnl" style="color:' + color + '">$' + d.pnl.toFixed(2) + '</div>' +
      '<div class="dow-trades">' + d.trades + ' trades</div>' +
      '</div>';
  }).join('');
}

// ── Yearly Breakdown ───────────────────────────────────────
function renderYearlyBreakdown(yearlyData) {
  var cont = document.getElementById('rd-yearly');
  var oldPager = document.getElementById('rd-yearly-pagination');
  if (oldPager) oldPager.remove();
  if (!cont) return;
  if (!yearlyData || yearlyData.length === 0) {
    cont.innerHTML = '<div style="color:var(--muted);text-align:center;padding:20px;">No yearly data available.</div>';
    return;
  }
  var html = '<table class="trade-table" id="rd-yearly-table"><thead><tr>';
  html += '<th>Year</th><th>Trades</th><th>Wins</th><th>Losses</th><th>Win Rate</th><th>P&L</th><th>Return %</th>';
  html += '</tr></thead><tbody>';
  yearlyData.forEach(function(y) {
    var pnl = y.pnl || 0;
    var winRate = y.trades > 0 ? ((y.wins || 0) / y.trades * 100).toFixed(1) : '0.0';
    html += '<tr>';
    html += '<td style="font-weight:700;">' + (y.year || '') + '</td>';
    html += '<td>' + (y.trades || 0) + '</td>';
    html += '<td style="color:var(--green);">' + (y.wins || 0) + '</td>';
    html += '<td style="color:var(--red);">' + (y.losses || 0) + '</td>';
    html += '<td>' + winRate + '%</td>';
    html += '<td style="color:' + (pnl >= 0 ? 'var(--green)' : 'var(--red)') + ';font-weight:700;">$' + pnl.toFixed(2) + '</td>';
    html += '<td>' + (y.return_pct || 0).toFixed(1) + '%</td>';
    html += '</tr>';
  });
  html += '</tbody></table>';
  cont.innerHTML = html;
  _renderTablePager('rd-yearly-table', 'rd-yearly-table', 'rd-yearly-pagination');
}

// ── P&L Heatmap ────────────────────────────────────────────
function renderHeatmap(monthlyData) {
  var cont = document.getElementById('rd-heatmap');
  if (!cont) return;
  if (!monthlyData || monthlyData.length === 0) {
    cont.innerHTML = '<div style="color:var(--muted);text-align:center;padding:20px;">No monthly data for heatmap.</div>';
    return;
  }
  // Group by year
  var years = {};
  monthlyData.forEach(function(m) {
    var parts = m.month.split('-');
    var yr = parts[0];
    var mo = parseInt(parts[1]) - 1;
    if (!years[yr]) years[yr] = new Array(12).fill(null);
    years[yr][mo] = m.pnl;
  });
  var allPnl = monthlyData.map(function(m) { return Math.abs(m.pnl); });
  var maxAbs = Math.max.apply(null, allPnl) || 1;
  var monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

  var html = '<div style="display:flex;gap:6px;margin-bottom:8px;">' +
    '<div style="width:48px;"></div>' +
    monthNames.map(function(mn) { return '<div style="width:48px;text-align:center;font-size:10px;color:var(--muted);font-weight:600;">' + mn + '</div>'; }).join('') +
    '</div>';

  Object.keys(years).sort().forEach(function(yr) {
    html += '<div style="display:flex;gap:6px;align-items:center;margin-bottom:4px;">' +
      '<div style="width:48px;font-size:12px;font-weight:700;color:var(--muted);">' + yr + '</div>';
    years[yr].forEach(function(pnl, i) {
      if (pnl === null) {
        html += '<div class="hm-cell" style="background:rgba(255,255,255,0.02);"><div class="hm-label">—</div></div>';
      } else {
        var intensity = Math.min(Math.abs(pnl) / maxAbs, 1);
        var color, bg;
        if (pnl >= 0) {
          bg = 'rgba(34,197,94,' + (0.1 + intensity * 0.5) + ')';
          color = 'var(--green)';
        } else {
          bg = 'rgba(239,68,68,' + (0.1 + intensity * 0.5) + ')';
          color = 'var(--red)';
        }
        html += '<div class="hm-cell" style="background:' + bg + ';color:' + color + ';" title="' + monthNames[i] + ' ' + yr + ': $' + pnl.toFixed(2) + '">' +
          '<div style="font-size:11px;font-weight:700;">$' + (Math.abs(pnl) >= 1000 ? (pnl/1000).toFixed(1) + 'K' : pnl.toFixed(0)) + '</div>' +
          '<div class="hm-label">' + monthNames[i] + '</div></div>';
      }
    });
    html += '</div>';
  });
  cont.innerHTML = html;
}

// ── Deploy Modal ───────────────────────────────────────────
var _deployType = 'paper';

function openDeployModal() {
  // Validate strategy first
  if (indicators.length === 0) { cfToast('Add at least one indicator first', 'warning'); return; }
  if (entryConditions.length === 0) { cfToast('Add entry conditions first', 'warning'); return; }
  if (exitConditions.length === 0) { cfToast('Add exit conditions first', 'warning'); return; }
  _deployType = 'paper';
  document.getElementById('deploy-tab-paper').className = 'deploy-type-tab active-paper';
  document.getElementById('deploy-tab-live').className = 'deploy-type-tab';
  document.getElementById('deploy-live-warning').style.display = 'none';
  document.getElementById('deploy-paper-info').style.display = 'flex';
  document.getElementById('deploy-confirm-btn').textContent = '🚀 Deploy Paper';
  document.getElementById('deploy-confirm-btn').className = 'btn btn-primary';
  document.getElementById('deploy-overlay').style.display = 'flex';
  resetDeployChecks();
  updateDeployModalState();
  // Server-side validation
  validateStrategy();
}

function closeDeployModal() {
  document.getElementById('deploy-overlay').style.display = 'none';
}

function setDeployType(type) {
  _deployType = type;
  if (type === 'paper') {
    document.getElementById('deploy-tab-paper').className = 'deploy-type-tab active-paper';
    document.getElementById('deploy-tab-live').className = 'deploy-type-tab';
    document.getElementById('deploy-live-warning').style.display = 'none';
    document.getElementById('deploy-paper-info').style.display = 'flex';
    document.getElementById('deploy-confirm-btn').textContent = '🚀 Deploy Paper';
    document.getElementById('deploy-confirm-btn').className = 'btn btn-primary';
  } else {
    document.getElementById('deploy-tab-paper').className = 'deploy-type-tab';
    document.getElementById('deploy-tab-live').className = 'deploy-type-tab active-live';
    document.getElementById('deploy-live-warning').style.display = 'flex';
    document.getElementById('deploy-paper-info').style.display = 'none';
    document.getElementById('deploy-confirm-btn').textContent = '🔴 Deploy LIVE';
    document.getElementById('deploy-confirm-btn').className = 'btn btn-danger';
  }
  updateDeployModalState();
}

async function validateStrategy() {
  var msgDiv = document.getElementById('deploy-validation-msg');
  msgDiv.innerHTML = '<div style="color:var(--muted);font-size:13px;padding:8px;">Validating strategy...</div>';
  try {
    var payload = {
      symbol: selectedCrypto,
      leverage: selectedLeverage,
      trade_side: selectedSide,
      indicators: indicators,
      entry_conditions: entryConditions,
      exit_conditions: exitConditions,
      candle_interval: document.getElementById('b-interval').value,
      stoploss_pct: parseFloat(document.getElementById('b-sl').value) || 5,
      target_profit_pct: parseFloat(document.getElementById('b-tp').value) || 10,
      trailing_sl_pct: parseFloat(document.getElementById('b-trail').value) || 0,
    };
    var r = await cfApiFetch('/api/validate-strategy', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    var d = await r.json();
    if (d.errors && d.errors.length > 0) {
      msgDiv.innerHTML = d.errors.map(function(err) {
        return '<div style="background:rgba(239,68,68,0.10);border:1px solid rgba(239,68,68,0.24);border-radius:8px;padding:8px 12px;margin-bottom:6px;font-size:12px;color:var(--red);">❌ ' + _escapeHtml(err) + '</div>';
      }).join('') + ((d.warnings && d.warnings.length > 0) ? d.warnings.map(function(w) {
        return '<div style="background:rgba(245,158,11,0.10);border:1px solid rgba(245,158,11,0.22);border-radius:8px;padding:8px 12px;margin-bottom:6px;font-size:12px;color:var(--yellow);">⚠️ ' + _escapeHtml(w) + '</div>';
      }).join('') : '');
    } else if (d.warnings && d.warnings.length > 0) {
      msgDiv.innerHTML = d.warnings.map(function(w) {
        return '<div style="background:rgba(245,158,11,0.1);border:1px solid rgba(245,158,11,0.2);border-radius:8px;padding:8px 12px;margin-bottom:6px;font-size:12px;color:var(--yellow);">⚠️ ' + _escapeHtml(w) + '</div>';
      }).join('');
    } else {
      msgDiv.innerHTML = '<div style="background:rgba(34,197,94,0.08);border:1px solid rgba(34,197,94,0.2);border-radius:8px;padding:8px 12px;font-size:12px;color:var(--green);">✅ Strategy validation passed</div>';
    }
    updateDeployModalState();
  } catch(e) {
    msgDiv.innerHTML = '<div style="color:var(--muted);font-size:12px;">Validation unavailable</div>';
    updateDeployModalState();
  }
}

async function confirmDeploy() {
  updateDeployModalState();
  if ((document.getElementById('deploy-confirm-btn') || {}).disabled) {
    cfToast('Review the deployment checklist before proceeding', 'warning');
    return;
  }
  closeDeployModal();
  if (_deployType === 'paper') {
    await startPaper();
  } else {
    await startLive();
  }
}

// ── Browser Notifications ──────────────────────────────────
function requestNotificationPermission() {
  if ('Notification' in window && Notification.permission === 'default') {
    Notification.requestPermission();
  }
}

function sendTradeNotification(title, body) {
  if ('Notification' in window && Notification.permission === 'granted') {
    try {
      new Notification(title, {
        body: body,
        icon: '⚡',
        badge: '⚡',
        tag: 'cf-trade-' + Date.now(),
      });
    } catch(e) { /* Silent fail on unsupported platforms */ }
  }
}

// ── CSV Download Buttons for Live/Paper ────────────────────
async function downloadPaperCSV() {
  try {
    var r = await fetch('/api/paper/trades/csv', { credentials: 'same-origin' });
    if (r.ok) {
      var blob = await r.blob();
      var url = URL.createObjectURL(blob);
      var a = document.createElement('a');
      a.href = url; a.download = 'paper_trades.csv'; a.click();
      URL.revokeObjectURL(url);
      cfToast('Paper trades exported', 'success');
    } else {
      var d = await r.json();
      cfToast(d.detail || 'No paper trades to export', 'warning');
    }
  } catch(e) { cfToast('Export failed: ' + e.message, 'error'); }
}

async function downloadLiveCSV() {
  try {
    var r = await fetch('/api/live/trades/csv', { credentials: 'same-origin' });
    if (r.ok) {
      var blob = await r.blob();
      var url = URL.createObjectURL(blob);
      var a = document.createElement('a');
      a.href = url; a.download = 'live_trades.csv'; a.click();
      URL.revokeObjectURL(url);
      cfToast('Live trades exported', 'success');
    } else {
      var d = await r.json();
      cfToast(d.detail || 'No live trades to export', 'warning');
    }
  } catch(e) { cfToast('Export failed: ' + e.message, 'error'); }
}

// ── Portfolio Page ─────────────────────────────────────────
async function loadPortfolioData() {
  try {
    var [summaryR, paperR, liveR] = await Promise.allSettled([
      fetch('/api/portfolio/summary', { credentials: 'same-origin' }).then(function(r) { return r.json(); }),
      fetch('/api/paper/status', { credentials: 'same-origin' }).then(function(r) { return r.json(); }),
      fetch('/api/live/status', { credentials: 'same-origin' }).then(function(r) { return r.json(); }),
    ]);

    var summary = summaryR.status === 'fulfilled' ? summaryR.value : {};
    var paper = paperR.status === 'fulfilled' ? paperR.value : {};
    var live = liveR.status === 'fulfilled' ? liveR.value : {};

    // Show broker error if portfolio API returned an error
    if (summary.status === 'error' && summary.message) {
      cfToast('Broker: ' + summary.message, 'warning');
    }

    // ── Balance card ──
    var bal = summary.balance || 0;
    var balEl = document.getElementById('pf-balance');
    if (balEl) {
      balEl.textContent = fmtINR(bal);
    }

    // ── Unrealized P&L card ──
    var upnl = summary.unrealized_pnl || 0;
    var upnlEl = document.getElementById('pf-unrealized');
    if (upnlEl) {
      upnlEl.textContent = fmtINR(upnl);
      upnlEl.style.color = upnl >= 0 ? 'var(--green)' : 'var(--red)';
    }

    // ── Counts ──
    var positions = summary.open_positions || [];
    var orders = summary.filled_orders || [];
    var countEl = document.getElementById('pf-open-count');
    if (countEl) countEl.textContent = positions.length;
    var orderCountEl = document.getElementById('pf-order-count');
    if (orderCountEl) orderCountEl.textContent = orders.length;

    // ── Open Positions Table ──
    var posTbody = document.getElementById('pf-positions-body');
    if (posTbody) {
      if (positions.length === 0) {
        posTbody.innerHTML = '<tr><td colspan="9" style="text-align:center;padding:30px;color:var(--muted);">No open positions</td></tr>';
      } else {
        posTbody.innerHTML = positions.map(function(p) {
          var upnl = parseFloat(p.unrealized_pnl) || 0;
          var rpnl = parseFloat(p.realized_pnl) || 0;
          var side = _escapeHtml(p.side || '--');
          var symbol = _escapeHtml(p.symbol || '--');
          return '<tr>' +
            '<td><div class="table-row-label">' + symbol + '</div><div class="table-note">size ' + (p.size || 0) + '</div></td>' +
            '<td><span class="tag ' + (p.side === 'LONG' ? 'tag-green' : 'tag-red') + '">' + side + '</span></td>' +
            '<td><div class="table-value-stack"><div class="table-value-main">' + (p.size || 0) + '</div><div class="table-value-sub">contracts</div></div></td>' +
            '<td class="num"><div class="table-value-stack"><div class="table-value-main">' + fmtINRPrice(parseFloat(p.entry_price) || 0) + '</div><div class="table-value-sub">entry</div></div></td>' +
            '<td class="num"><div class="table-value-stack"><div class="table-value-main">' + fmtINRPrice(parseFloat(p.mark_price) || 0) + '</div><div class="table-value-sub">mark</div></div></td>' +
            '<td class="num"><div class="table-value-stack"><div class="table-value-main">' + (p.leverage || '--') + 'x</div><div class="table-value-sub">gear</div></div></td>' +
            '<td class="num"><div class="table-value-stack"><div class="table-value-main" style="color:' + (upnl >= 0 ? 'var(--green)' : 'var(--red)') + ';">' + fmtINR(upnl) + '</div><div class="table-value-sub">unrealized</div></div></td>' +
            '<td class="num"><div class="table-value-stack"><div class="table-value-main" style="color:' + (rpnl >= 0 ? 'var(--green)' : 'var(--red)') + ';">' + fmtINR(rpnl) + '</div><div class="table-value-sub">realized</div></div></td>' +
            '<td class="num"><div class="table-value-stack"><div class="table-value-main" style="color:var(--yellow);">' + fmtINRPrice(parseFloat(p.liquidation_price) || 0) + '</div><div class="table-value-sub">liq.</div></div></td>' +
            '</tr>';
        }).join('');
      }
      _renderTablePager('pf-positions-table', 'pf-positions-table', 'pf-positions-pagination');
    }

    // ── Filled Orders Table ──
    var ordTbody = document.getElementById('pf-orders-body');
    if (ordTbody) {
      if (orders.length === 0) {
        ordTbody.innerHTML = '<tr><td colspan="8" style="text-align:center;padding:30px;color:var(--muted);">No filled orders</td></tr>';
      } else {
        ordTbody.innerHTML = orders.slice(0, 50).map(function(o) {
          var side = (o.side || '').toUpperCase();
          var safeSide = _escapeHtml(side || '--');
          var fillPrice = parseFloat(o.average_fill_price) || parseFloat(o.price) || 0;
          var ts = o.created_at || o.timestamp || '';
          var dt = _getTradeDateParts(ts);
          var symbol = _escapeHtml(o.product_symbol || o.symbol || ('ID:' + (o.product_id || '')));
          var fee = parseFloat(o.paid_commission) || 0;
          var orderType = _escapeHtml((o.order_type || o.type || '--').replace('_', ' '));
          var orderState = _escapeHtml(o.state || o.status || 'filled');
          return '<tr>' +
            '<td><div class="table-datetime"><strong>' + dt.date + '</strong><span>' + (dt.time || '—') + '</span></div></td>' +
            '<td><div class="table-row-label">' + symbol + '</div><div class="table-note">' + orderType + '</div></td>' +
            '<td><span class="tag ' + (side === 'BUY' ? 'tag-green' : 'tag-red') + '">' + safeSide + '</span></td>' +
            '<td><div class="table-value-stack"><div class="table-value-main">' + (o.size || o.quantity || 0) + '</div><div class="table-value-sub">filled</div></div></td>' +
            '<td class="num"><div class="table-value-stack"><div class="table-value-main">' + fmtINRPrice(fillPrice) + '</div><div class="table-value-sub">avg fill</div></div></td>' +
            '<td class="num"><div class="table-value-stack"><div class="table-value-main" style="color:var(--yellow);">' + fmtINR(fee) + '</div><div class="table-value-sub">fees</div></div></td>' +
            '<td><div class="table-row-label">' + orderType + '</div><div class="table-note">' + _escapeHtml(_brokerLabel()) + '</div></td>' +
            '<td><span class="tag tag-purple">' + orderState + '</span></td>' +
            '</tr>';
        }).join('');
      }
      _renderTablePager('pf-orders-table', 'pf-orders-table', 'pf-orders-pagination');
    }

    // ── Paper Trading Summary ──
    var pfPaperStatus = document.getElementById('pf-paper-status');
    if (pfPaperStatus) {
      if (paper.running) {
        pfPaperStatus.textContent = '🟢 Running';
        pfPaperStatus.style.color = 'var(--green)';
      } else {
        pfPaperStatus.textContent = 'Idle';
        pfPaperStatus.style.color = 'var(--muted)';
      }
    }
    var pfPaperSym = document.getElementById('pf-paper-symbol');
    if (pfPaperSym) pfPaperSym.textContent = paper.symbol || '--';
    var pfPaperPnl = document.getElementById('pf-paper-pnl');
    if (pfPaperPnl) {
      var pp = parseFloat(paper.total_pnl) || 0;
      pfPaperPnl.textContent = fmtINR(pp);
      pfPaperPnl.style.color = pp >= 0 ? 'var(--green)' : 'var(--red)';
    }
    var pfPaperTrades = document.getElementById('pf-paper-trades');
    if (pfPaperTrades) pfPaperTrades.textContent = paper.trades_today || paper.closed_trades || 0;

    // ── Live Trading Summary ──
    var pfLiveStatus = document.getElementById('pf-live-status');
    if (pfLiveStatus) {
      if (live.running) {
        pfLiveStatus.textContent = '🔴 LIVE';
        pfLiveStatus.style.color = 'var(--red)';
      } else {
        pfLiveStatus.textContent = 'Idle';
        pfLiveStatus.style.color = 'var(--muted)';
      }
    }
    var pfLiveSym = document.getElementById('pf-live-symbol');
    if (pfLiveSym) pfLiveSym.textContent = live.symbol || '--';
    var pfLivePnl = document.getElementById('pf-live-pnl');
    if (pfLivePnl) {
      var lp = parseFloat(live.total_pnl) || 0;
      pfLivePnl.textContent = fmtINR(lp);
      pfLivePnl.style.color = lp >= 0 ? 'var(--green)' : 'var(--red)';
    }
    var pfLiveTrades = document.getElementById('pf-live-trades');
    if (pfLiveTrades) pfLiveTrades.textContent = live.trades_today || live.closed_trades || 0;

    // Load portfolio history for monthly/yearly grids
    loadPortfolioHistory();

  } catch(e) {
    console.error('Portfolio load error:', e);
    cfToast('Failed to load portfolio data', 'error');
  }
}

// ── Folder Helper ──────────────────────────────────────────
function onFolderChange() {
  var sel = document.getElementById('b-folder');
  var custom = document.getElementById('b-folder-custom');
  if (sel && custom) {
    custom.style.display = sel.value === '_custom' ? 'block' : 'none';
  }
}

function getSelectedFolder() {
  var sel = document.getElementById('b-folder');
  if (sel && sel.value === '_custom') {
    return document.getElementById('b-folder-custom').value || 'Default';
  }
  return sel ? sel.value : 'Default';
}

// ── Saved Strategies ───────────────────────────────────────
let _savedStrategies = [];
let _viewStrategyId = null;
let _moveStrategyId = null;

async function fetchStrategies() {
  try {
    var r = await fetch('/api/strategies', { credentials: 'same-origin' });
    _savedStrategies = await r.json();
    renderStrategiesList();
    populatePaperStrategyDropdown();
  } catch(e) { console.error('fetchStrategies error:', e); }
}

function renderStrategiesList() {
  var cont = document.getElementById('strategies-list');
  if (!cont) return;
  if (!_savedStrategies || _savedStrategies.length === 0) {
    cont.innerHTML = '<div style="color:var(--muted);text-align:center;padding:20px;">No saved strategies yet. Build one in the Strategy Builder!</div>';
    return;
  }
  // Group by folder
  var folders = {};
  _savedStrategies.forEach(function(s) {
    var f = s.folder || 'Default';
    if (!folders[f]) folders[f] = [];
    folders[f].push(s);
  });
  var html = '';
  Object.keys(folders).sort().forEach(function(folder) {
    var strats = folders[folder];
    html += '<div class="strat-folder-group">';
    html += '<div class="strat-folder-header" data-cf-click="toggleFolder(this)">';
    html += '<span class="folder-icon">📁</span> ' + _escapeHtml(folder);
    html += '<span class="folder-count">' + strats.length + ' strategies</span>';
    html += '</div>';
    html += '<div class="strat-folder-body">';
    strats.forEach(function(s) {
      var strategyName = _escapeHtml(s.run_name || 'Untitled');
      var symbol = _escapeHtml(s.symbol || '');
      var side = _escapeHtml(s.trade_side || '');
      html += '<div class="strat-row">';
      html += '<div style="flex:1;">';
      html += '<div class="sr-name">' + strategyName + '</div>';
      html += '<div class="sr-meta">' + symbol + ' • ' + (s.leverage || 1) + 'x • ' + side + '</div>';
      html += '</div>';
      html += '<div class="strat-actions">';
      html += '<button data-cf-click="viewStrategy(\'' + s.id + '\')">👁 View</button>';
      html += '<button data-cf-click="loadStrategyToBuilder(\'' + s.id + '\')">📝 Edit</button>';
      html += '<button data-cf-click="openMoveFolder(\'' + s.id + '\')">📁 Move</button>';
      html += '<button data-cf-click="copyStrategy(\'' + s.id + '\')">📋 Copy</button>';
      html += '<button data-cf-click="deleteStrategy(\'' + s.id + '\')">🗑</button>';
      html += '</div>';
      html += '</div>';
    });
    html += '</div></div>';
  });
  cont.innerHTML = html;
}

function toggleFolder(headerEl) {
  var body = headerEl.nextElementSibling;
  if (body) body.classList.toggle('collapsed');
}

async function viewStrategy(sid) {
  _viewStrategyId = sid;
  var s = _savedStrategies.find(function(x) { return x.id == sid; });
  if (!s) { cfToast('Strategy not found', 'error'); return; }
  document.getElementById('vs-title').textContent = s.run_name || 'Strategy Details';
  // Config grid
  var cfg = [
    ['Symbol', s.symbol || '--'],
    ['Side', s.trade_side || '--'],
    ['Leverage', (s.leverage || 1) + 'x'],
    ['Stop Loss', (s.stoploss_pct || 0) + '%'],
    ['Take Profit', (s.target_profit_pct || 0) + '%'],
    ['Cost Model', cfCostModelSummary(s)],
    ['Folder', s.folder || 'Default'],
  ];
  document.getElementById('vs-config').innerHTML = cfg.map(function(c) {
    return '<div class="ti-item"><div class="ti-label">' + _escapeHtml(c[0]) + '</div><div class="ti-value">' + _escapeHtml(c[1]) + '</div></div>';
  }).join('');
  // Indicators
  var inds = s.indicators || [];
  document.getElementById('vs-indicators').innerHTML = inds.length > 0
    ? inds.map(function(ind) { return '<span class="tag tag-purple">' + _escapeHtml(ind) + '</span>'; }).join('')
    : '<span style="color:var(--muted);font-size:13px;">No indicators</span>';
  // Conditions
  var formatConds = function(conds) {
    if (!conds || conds.length === 0) return '<div style="color:var(--muted);font-size:13px;">No conditions</div>';
    return conds.map(function(c, i) {
      var prefix = i > 0 ? '<span style="color:var(--accent);font-weight:700;">' + (c.connector || 'AND') + '</span> ' : '';
      var rightVal = c.right === 'number' ? (c.right_number_value || 0) : c.right;
      return '<div class="vs-cond-row">' + prefix + _escapeHtml(c.left) + ' <b>' + _escapeHtml((c.operator || '').replace(/_/g, ' ')) + '</b> ' + _escapeHtml(rightVal) + '</div>';
    }).join('');
  };
  document.getElementById('vs-entry-conds').innerHTML = formatConds(s.entry_conditions);
  document.getElementById('vs-exit-conds').innerHTML = formatConds(s.exit_conditions);
  document.getElementById('view-strategy-overlay').style.display = 'flex';
}

function closeViewStrategy() {
  document.getElementById('view-strategy-overlay').style.display = 'none';
}

function editStrategyFromView() {
  closeViewStrategy();
  if (_viewStrategyId) loadStrategyToBuilder(_viewStrategyId);
}

function deployStrategyFromView() {
  closeViewStrategy();
  if (_viewStrategyId) {
    loadStrategyToBuilder(_viewStrategyId);
    setTimeout(openDeployModal, 300);
  }
}

async function loadStrategyToBuilder(sid) {
  var s = _savedStrategies.find(function(x) { return x.id == sid; });
  if (!s) { cfToast('Strategy not found', 'error'); return; }
  // Load into builder state
  document.getElementById('b-name').value = s.run_name || '';
  selectedCrypto = s.symbol || 'BTCUSDT';
  selectedSide = s.trade_side || 'LONG';
  selectedLeverage = s.leverage || 10;
  indicators = [...(s.indicators || [])];
  entryConditions = (s.entry_conditions || []).map(function(c) { return {...c}; });
  exitConditions = (s.exit_conditions || []).map(function(c) { return {...c}; });
  if (s.stoploss_pct != null) document.getElementById('b-sl').value = s.stoploss_pct;
  if (s.target_profit_pct != null) document.getElementById('b-tp').value = s.target_profit_pct;
  cfApplyBuilderCostModel(s);
  document.getElementById('b-compounding').checked = !!s.compounding;
  if (s.trailing_sl_pct != null) document.getElementById('b-trail').value = s.trailing_sl_pct;
  if (s.max_trades_per_day != null) document.getElementById('b-maxtrades').value = s.max_trades_per_day;
  if (s.initial_capital != null) document.getElementById('b-capital').value = s.initial_capital;
  if (s.position_size_mode === 'fixed_qty') {
    document.getElementById('b-possize-mode').value = 'fixed_qty';
    document.getElementById('b-possize').value = s.fixed_qty || 0.1;
    document.getElementById('b-possize').step = '0.01';
    document.getElementById('b-possize-label').textContent = 'Qty';
  } else {
    document.getElementById('b-possize-mode').value = 'pct';
    if (s.position_size_pct != null) document.getElementById('b-possize').value = s.position_size_pct;
    document.getElementById('b-possize').step = '1';
    document.getElementById('b-possize-label').textContent = '%';
  }
  if (s.candle_interval) document.getElementById('b-interval').value = s.candle_interval;
  // Set folder
  var folderSel = document.getElementById('b-folder');
  if (folderSel) {
    var found = false;
    for (var i = 0; i < folderSel.options.length; i++) {
      if (folderSel.options[i].value === (s.folder || 'Default')) { folderSel.selectedIndex = i; found = true; break; }
    }
    if (!found) { folderSel.value = '_custom'; document.getElementById('b-folder-custom').value = s.folder || ''; onFolderChange(); }
    else onFolderChange();
  }
  // Update UI
  showPage('builder-page', document.getElementById('nav-builder'));
  initCryptoSelector();
  renderLeverage(leverageOptions, selectedLeverage);
  setSide(selectedSide);
  renderIndicators();
  renderConditions('entry');
  renderConditions('exit');
  cfToast('Strategy loaded: ' + (s.run_name || 'Untitled'), 'success');
}

async function copyStrategy(sid) {
  var s = _savedStrategies.find(function(x) { return x.id == sid; });
  if (!s) return;
  var newName = generateCopyName(s.run_name || 'Untitled');
  var payload = {...s, run_name: newName};
  delete payload.id;
  delete payload.created_at;
  try {
    await cfApiFetch('/api/strategies', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload),
    });
    cfToast('Strategy copied as: ' + newName, 'success');
    fetchStrategies();
  } catch(e) { cfToast('Copy failed: ' + e.message, 'error'); }
}

function generateCopyName(name) {
  if (name.match(/_copy(_\d+)?$/)) {
    var base = name.replace(/_copy(_\d+)?$/, '');
    var num = 2;
    var match = name.match(/_copy_(\d+)$/);
    if (match) num = parseInt(match[1]) + 1;
    return base + '_copy_' + num;
  }
  return name + '_copy';
}

async function deleteStrategy(sid) {
  var ok = await cfConfirm('Delete this saved strategy permanently?', 'Delete Strategy', '🗑️');
  if (!ok) return;
  try {
    await cfApiFetch('/api/strategies/' + sid, { method: 'DELETE' });
    cfToast('Strategy deleted', 'success');
    fetchStrategies();
  } catch(e) { cfToast('Delete failed: ' + e.message, 'error'); }
}

// ── Move Strategy Folder ───────────────────────────────────
function openMoveFolder(sid) {
  _moveStrategyId = sid;
  document.getElementById('move-folder-select').value = 'Default';
  document.getElementById('move-folder-custom').style.display = 'none';
  document.getElementById('move-folder-overlay').style.display = 'flex';
  // Set up change handler
  document.getElementById('move-folder-select').onchange = function() {
    document.getElementById('move-folder-custom').style.display = this.value === '_custom' ? 'block' : 'none';
  };
}

function closeMoveFolder() {
  document.getElementById('move-folder-overlay').style.display = 'none';
}

async function confirmMoveTo() {
  var sel = document.getElementById('move-folder-select').value;
  var folder = sel === '_custom' ? (document.getElementById('move-folder-custom').value || 'Default') : sel;
  closeMoveFolder();
  try {
    await cfApiFetch('/api/strategies/' + _moveStrategyId, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({folder: folder}),
    });
    cfToast('Strategy moved to ' + folder, 'success');
    fetchStrategies();
  } catch(e) { cfToast('Move failed: ' + e.message, 'error'); }
}

// ── Copy/Edit Run from Results ─────────────────────────────
async function copyEditRun(runId) {
  try {
    var r = await fetch('/api/runs/' + runId, { credentials: 'same-origin' });
    var run = await r.json();
    copyEditStrategy(run);
  } catch(e) { cfToast('Failed to load run', 'error'); }
}

function copyEditStrategy(data) {
  document.getElementById('b-name').value = (data.run_name || 'Untitled') + '_edit';
  selectedCrypto = data.symbol || 'BTCUSDT';
  selectedSide = data.trade_side || 'LONG';
  selectedLeverage = data.leverage || 10;
  indicators = [...(data.indicators || [])];
  entryConditions = (data.entry_conditions || []).map(function(c) { return {...c}; });
  exitConditions = (data.exit_conditions || []).map(function(c) { return {...c}; });
  if (data.stoploss_pct != null) document.getElementById('b-sl').value = data.stoploss_pct;
  if (data.target_profit_pct != null) document.getElementById('b-tp').value = data.target_profit_pct;
  cfApplyBuilderCostModel(data);
  document.getElementById('b-compounding').checked = !!data.compounding;
  if (data.trailing_sl_pct != null) document.getElementById('b-trail').value = data.trailing_sl_pct;
  if (data.max_trades_per_day != null) document.getElementById('b-maxtrades').value = data.max_trades_per_day;
  if (data.initial_capital != null) document.getElementById('b-capital').value = data.initial_capital;
  if (data.position_size_mode === 'fixed_qty') {
    document.getElementById('b-possize-mode').value = 'fixed_qty';
    document.getElementById('b-possize').value = data.fixed_qty || 0.1;
    document.getElementById('b-possize').step = '0.01';
    document.getElementById('b-possize-label').textContent = 'Qty';
  } else {
    document.getElementById('b-possize-mode').value = 'pct';
    if (data.position_size_pct != null) document.getElementById('b-possize').value = data.position_size_pct;
    document.getElementById('b-possize').step = '1';
    document.getElementById('b-possize-label').textContent = '%';
  }
  if (data.candle_interval) document.getElementById('b-interval').value = data.candle_interval;
  // Navigate to builder
  showPage('builder-page', document.getElementById('nav-builder'));
  initCryptoSelector();
  renderLeverage(leverageOptions, selectedLeverage);
  setSide(selectedSide);
  renderIndicators();
  renderConditions('entry');
  renderConditions('exit');
  renderBuilderDeck();
  cfToast('Run loaded for editing', 'success');
}

// ── Multi-Engine Live Monitor ───────────────────────────────────
let _liveMonitorInterval = null;
let _liveEngines = [];
let _liveSelectedTab = 0; // index of selected tab
let _liveTradeFilters = {};
let _liveTradeSelections = {};

function _escapeJsString(str) {
  return String(str == null ? '' : str).replace(/\\/g, '\\\\').replace(/'/g, "\\'");
}

function _escapeHtml(str) {
  return String(str == null ? '' : str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function _getLiveEngineByRunId(runId) {
  for (var i = 0; i < _liveEngines.length; i++) {
    if ((_liveEngines[i].run_id || '') === runId) return _liveEngines[i];
  }
  return _liveEngines[_liveSelectedTab] || null;
}

function _getLiveTradeState(runId) {
  if (!_liveTradeFilters[runId]) _liveTradeFilters[runId] = { mode: 'all', query: '' };
  if (!_liveTradeSelections[runId]) _liveTradeSelections[runId] = {};
  return {
    filters: _liveTradeFilters[runId],
    selected: _liveTradeSelections[runId],
  };
}

function _getLiveTradeRowKey(trade) {
  if (trade && trade.id !== undefined && trade.id !== null && trade.id !== '') return 'id:' + trade.id;
  return [
    trade && trade.symbol || '',
    trade && trade.side || '',
    trade && trade.entry_time || '',
    trade && trade.exit_time || '',
    trade && trade.entry_price || '',
  ].join('|');
}

function _getLiveClosedTradeRows(data) {
  var rows = [];
  if (Array.isArray(data.closed_trade_rows) && data.closed_trade_rows.length) rows = data.closed_trade_rows.slice();
  else if (Array.isArray(data.recent_trades) && data.recent_trades.length) rows = data.recent_trades.slice();
  return rows.reverse();
}

function _getTradeDateParts(raw) {
  if (!raw || raw === 'None') return { date: '—', time: '—', label: '—' };
  var s = String(raw).trim().replace('T', ' ');
  var full = s.match(/(\d{4})-(\d{2})-(\d{2}).*?(\d{2}:\d{2}:\d{2})/);
  if (full) {
    var months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    var monthIdx = Math.max(0, Math.min(11, parseInt(full[2], 10) - 1));
    var date = full[3] + ' ' + months[monthIdx] + ' ' + full[1];
    var time = full[4] + ' IST';
    return { date: date, time: time, label: date + ', ' + time };
  }
  var timeOnly = s.match(/(\d{2}:\d{2}:\d{2})/);
  if (timeOnly) return { date: '—', time: timeOnly[1] + ' IST', label: timeOnly[1] + ' IST' };
  return { date: s, time: '', label: s };
}

function _fmtTradeDateTime(raw) {
  var parts = _getTradeDateParts(raw);
  if (parts.date === '—') return parts.label;
  return parts.time ? (parts.date + ', ' + parts.time) : parts.date;
}

function _fmtUsd(usd) {
  var n = parseFloat(usd);
  if (isNaN(n)) n = 0;
  return '$' + n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function _filterLiveClosedTrades(rows, filters) {
  var mode = (filters.mode || 'all').toLowerCase();
  var query = (filters.query || '').trim().toLowerCase();
  return rows.filter(function(t) {
    var side = String(t.side || '').toUpperCase();
    var pnl = parseFloat(t.pnl) || 0;
    if (mode === 'long' && side !== 'LONG') return false;
    if (mode === 'short' && side !== 'SHORT') return false;
    if (mode === 'wins' && pnl < 0) return false;
    if (mode === 'losses' && pnl >= 0) return false;
    if (!query) return true;
    var hay = [
      t.id,
      t.symbol,
      t.side,
      t.exit_reason,
      _fmtTradeDateTime(t.entry_time),
      _fmtTradeDateTime(t.exit_time),
      t.leverage,
      t.size,
      t.notional,
    ].join(' ').toLowerCase();
    return hay.indexOf(query) !== -1;
  });
}

function _rerenderLiveTradeTable(runId) {
  var eng = _getLiveEngineByRunId(runId);
  if (!eng) return;
  renderLivePanel(eng, _liveSelectedTab);
}

function _setLiveTradeFilter(runId, mode) {
  _getLiveTradeState(runId).filters.mode = mode;
  _rerenderLiveTradeTable(runId);
}

function _setLiveTradeQuery(runId, query) {
  _getLiveTradeState(runId).filters.query = query;
  _rerenderLiveTradeTable(runId);
}

function _toggleLiveTradeCheck(runId, tradeKey, checked) {
  var sel = _getLiveTradeState(runId).selected;
  if (checked) sel[tradeKey] = true;
  else delete sel[tradeKey];
  _rerenderLiveTradeTable(runId);
}

function _toggleAllLiveTradeChecks(runId, checked) {
  var eng = _getLiveEngineByRunId(runId);
  if (!eng) return;
  var state = _getLiveTradeState(runId);
  var visible = _filterLiveClosedTrades(_getLiveClosedTradeRows(eng), state.filters);
  visible.forEach(function(t) {
    var key = _getLiveTradeRowKey(t);
    if (checked) state.selected[key] = true;
    else delete state.selected[key];
  });
  _rerenderLiveTradeTable(runId);
}

function _clearLiveTradeSelection(runId) {
  _liveTradeSelections[runId] = {};
  _rerenderLiveTradeTable(runId);
}

function _exportLiveTradeSelection(runId) {
  var eng = _getLiveEngineByRunId(runId);
  if (!eng) return;
  var rows = _getLiveClosedTradeRows(eng);
  var selected = _getLiveTradeState(runId).selected;
  var picked = rows.filter(function(t) { return !!selected[_getLiveTradeRowKey(t)]; });
  if (!picked.length) {
    cfToast('Select at least one completed trade first.', 'warn');
    return;
  }

  function esc(v) {
    var s = String(v == null ? '' : v);
    return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
  }

  var lines = [[
    'Trade ID',
    'Symbol',
    'Side',
    'Entry Time',
    'Exit Time',
    'Entry $',
    'Entry $ Fmt',
    'Exit $',
    'Exit $ Fmt',
    'PnL $',
    'PnL $ Fmt',
    'Reason',
    'Leverage',
    'Size',
    'Notional $',
    'Notional $ Fmt',
  ].join(',')];

  picked.forEach(function(t) {
    var entry = parseFloat(t.entry_price) || 0;
    var exit = parseFloat(t.exit_price) || 0;
    var pnl = parseFloat(t.pnl) || 0;
    var notional = parseFloat(t.notional) || 0;
    lines.push([
      esc(t.id || ''),
      esc(t.symbol || ''),
      esc(t.side || ''),
      esc(_fmtTradeDateTime(t.entry_time)),
      esc(_fmtTradeDateTime(t.exit_time)),
      esc(entry.toFixed(2)),
      esc(fmtINRPrice(entry)),
      esc(exit.toFixed(2)),
      esc(fmtINRPrice(exit)),
      esc(pnl.toFixed(2)),
      esc(fmtINR(pnl)),
      esc(t.exit_reason || ''),
      esc(t.leverage || ''),
      esc(t.size || ''),
      esc(notional.toFixed(2)),
      esc(fmtINR(notional)),
    ].join(','));
  });

  var blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8;' });
  var url = URL.createObjectURL(blob);
  var a = document.createElement('a');
  a.href = url;
  a.download = (runId || 'engine') + '_selected_trades.csv';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  setTimeout(function() { URL.revokeObjectURL(url); }, 250);
}

function startLiveMonitor() {
  loadLiveMonitor();
  if (_liveMonitorInterval) clearInterval(_liveMonitorInterval);
  _liveMonitorInterval = setInterval(loadLiveMonitor, 5000);
}

function stopLiveMonitor() {
  if (_liveMonitorInterval) { clearInterval(_liveMonitorInterval); _liveMonitorInterval = null; }
}

async function loadLiveMonitor() {
  try {
    var r = await fetch('/api/engines/all', { credentials: 'same-origin' });
    var data = await r.json();
    _liveEngines = data.engines || [];

    // Update live dot in nav
    var liveDot = document.getElementById('live-tab-dot');
    var anyRunning = _liveEngines.some(function(e) { return e.running; });
    if (liveDot) { if (anyRunning) liveDot.classList.add('active'); else liveDot.classList.remove('active'); }

    // Clamp selected tab
    if (_liveSelectedTab >= _liveEngines.length) _liveSelectedTab = Math.max(0, _liveEngines.length - 1);

    renderLiveTabs();
    if (_liveEngines.length > 0) {
      renderLivePanel(_liveEngines[_liveSelectedTab], _liveSelectedTab);
    } else {
      var liveContainer = document.getElementById('live-panels-container');
      if (liveContainer) liveContainer.innerHTML = cfLiveEmptyStateHtml();
    }
  } catch(e) { console.error('loadLiveMonitor error:', e); }
}

function selectLiveTab(idx) {
  _liveSelectedTab = idx;
  renderLiveTabs();
  if (_liveEngines[idx]) renderLivePanel(_liveEngines[idx], idx);
}

function renderLiveTabs() {
  var bar = document.getElementById('live-tabs-bar');
  if (!bar) return;
  if (!_liveEngines.length) {
    bar.innerHTML = cfLiveEmptyRailHtml();
    return;
  }

  // Combined summary at left
  var totalPnl = _liveEngines.reduce(function(s, e) { return s + (parseFloat(e.total_pnl) || 0); }, 0);
  var totalRunning = _liveEngines.filter(function(e) { return e.running; }).length;
  var pnlState = totalPnl >= 0 ? 'positive' : 'negative';

  var html = '<div class="live-tabs-summary" data-state="' + pnlState + '">';
  html += '<div class="live-tabs-summary-label">Combined P&amp;L</div>';
  html += '<div class="live-tabs-summary-value">' + fmtINR(totalPnl) + '</div>';
  html += '<div class="live-tabs-summary-meta">' + totalRunning + ' running</div>';
  html += '</div>';

  // Individual strategy tabs
  _liveEngines.forEach(function(eng, idx) {
    var active = idx === _liveSelectedTab;
    var name = _escapeHtml(eng.strategy_name || eng.run_name || 'Strategy');
    var modeIcon = eng.mode === 'paper' ? '📄' : '🔴';
    var pnl = parseFloat(eng.total_pnl) || 0;
    var running = eng.running;
    var inTrade = eng.in_trade;
    var pnlStateClass = pnl >= 0 ? 'positive' : 'negative';
    var statusClass = running && inTrade ? 'warning' : running ? 'running' : 'stopped';

    html += '<button class="live-tab-btn' + (active ? ' active' : '') + '" data-cf-click="selectLiveTab(' + idx + ')">';
    html += '<span class="lt-dot ' + (eng.mode === 'paper' ? 'paper' : 'live') + ' ' + statusClass + '"></span>';
    html += '<span class="live-tab-copy">';
    html += '<span class="live-tab-name">' + name + ' <span class="live-tab-mode">' + modeIcon + '</span></span>';
    html += '<span class="live-tab-pnl ' + pnlStateClass + '">' + fmtINR(pnl) + '</span>';
    html += '</span>';
    html += '</button>';
  });

  // Refresh button at right
  html += '<div class="live-tabs-actions">';
  html += '<button class="btn btn-danger btn-sm" data-cf-click="emergencyStop()">⛔ Master Kill</button>';
  html += '<button class="btn btn-primary btn-sm" data-cf-click="loadLiveMonitor()">🔄 Refresh</button>';
  html += '</div>';

  bar.innerHTML = html;
}

function cfLiveEmptyRailHtml() {
  return ''
    + '<div class="live-empty-rail">'
    + '<div class="live-empty-rail-kicker">Live Monitor</div>'
    + '<div class="live-empty-rail-copy">Deploy a strategy to populate paper and live engine tabs here.</div>'
    + '</div>';
}

function cfLiveEmptyStateHtml() {
  return ''
    + '<div class="live-empty-state">'
    + '<div class="live-empty-icon" aria-hidden="true">📡</div>'
    + '<div class="live-empty-title">No Active Strategies</div>'
    + '<div class="live-empty-copy">Deploy a strategy with Paper Trade or Live Trade to see monitoring here.</div>'
    + '<div class="live-empty-actions">'
    + '<button class="btn btn-outline btn-sm" data-cf-click="showPage(\'builder-page\', document.getElementById(\'nav-builder\'))">Open Builder</button>'
    + '<button class="btn btn-outline btn-sm" data-cf-click="showPage(\'results-page\', document.getElementById(\'nav-results\'))">Review Runs</button>'
    + '</div>'
    + '</div>';
}

function renderLivePanel(d, idx) {
  var container = document.getElementById('live-panels-container');
  if (!container || !d) return;

  var running = d.running;
  var inTrade = d.in_trade;
  var name = _escapeHtml(d.strategy_name || d.run_name || 'Strategy');
  var mode = d.mode || 'paper';
  var runId = d.run_id || '';
  var safeRunId = _escapeJsString(runId);
  var panelKey = _safeDomId(runId || ('engine-' + idx));
  var pnl = parseFloat(d.total_pnl) || 0;
  var dailyPnl = parseFloat(d.daily_pnl) || 0;

  // Status badge
  var badgeHtml, statusText, statusColor;
  if (running && inTrade) {
    badgeHtml = '<span class="status-pill status-pill-trade">In Trade</span>';
    statusText = 'In Trade'; statusColor = '#fbbf24';
  } else if (running) {
    badgeHtml = '<span class="status-pill status-pill-running">Scanning</span>';
    statusText = 'Scanning'; statusColor = '#4ade80';
  } else {
    badgeHtml = '<span class="status-pill status-pill-stopped">Stopped</span>';
    statusText = 'Stopped'; statusColor = 'var(--muted)';
  }

  var modeIcon = mode === 'paper' ? '📄' : '🔴';
  var modeLabel = mode === 'paper' ? 'Paper' : 'Live';
  var pnlColor = pnl >= 0 ? 'var(--green)' : 'var(--red)';
  var dailyPnlColor = dailyPnl >= 0 ? 'var(--green)' : 'var(--red)';

  var html = '';

  // ── Header bar
  html += '<div style="display:flex;align-items:center;justify-content:space-between;padding:14px 28px;background:var(--card);border-bottom:1px solid var(--border);">';
  html += '<div style="display:flex;align-items:center;gap:12px;">';
  html += '<span style="font-size:22px;">' + modeIcon + '</span>';
  html += '<div><div style="font-size:18px;font-weight:700;">' + name + '</div>';
  html += '<div style="font-size:11px;color:var(--muted);margin-top:2px;">' + modeLabel + ' • ' + _escapeHtml(d.symbol || '') + ' • ' + (d.leverage || 1) + 'x</div></div>';
  html += badgeHtml;
  html += '</div>';
  html += '<div style="display:flex;gap:8px;">';
  if (running) {
    html += '<button class="btn btn-danger btn-sm" data-cf-click="stopEngine(\'' + safeRunId + '\',\'' + mode + '\')" style="padding:6px 14px;">⏹ Stop</button>';
  }
  html += '<button class="btn btn-primary btn-sm" data-cf-click="viewEngineDetails(\'' + safeRunId + '\',\'' + mode + '\')" style="padding:6px 14px;">👁️ View</button>';
  html += '<button class="btn btn-warning btn-sm" data-cf-click="downloadEngineCSV(\'' + safeRunId + '\',\'' + mode + '\')" style="padding:6px 14px;">📥 CSV</button>';
  html += '</div></div>';

  // ── 4 Stat Cards
  html += '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:16px;padding:20px 28px 8px;">';
  html += '<div class="ti-item"><div class="ti-label">Total P&L</div><div class="ti-value" style="color:' + pnlColor + ';font-weight:700;">' + fmtINR(pnl) + '</div></div>';
  html += '<div class="ti-item"><div class="ti-label">Run Type</div><div class="ti-value" style="color:' + (mode === 'paper' ? 'var(--accent)' : 'var(--red)') + ';">' + modeIcon + ' ' + modeLabel + '</div></div>';
  html += '<div class="ti-item"><div class="ti-label">Trades Today</div><div class="ti-value">' + (d.trades_today || 0) + '</div></div>';
  html += '<div class="ti-item"><div class="ti-label">Status</div><div class="ti-value" style="color:' + statusColor + ';">' + statusText + '</div></div>';
  html += '</div>';

  // ── 2-column grid: Signal/Indicators | Positions + Event Log
  html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;padding:8px 28px 20px;">';

  // LEFT: Signal / Indicator table
  html += '<div>';
  html += '<h4 style="margin:0 0 8px;font-size:13px;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px;">📊 Signal / Indicator</h4>';
  html += '<div style="background:rgba(0,0,0,0.15);border:1px solid rgba(255,255,255,0.04);border-radius:10px;overflow:hidden;">';
  html += '<table class="trade-table live-monitor-table" id="live-signal-table-' + panelKey + '" style="width:100%;font-size:12px;">';

  var candle = d.current_candle || {};
  if (!running || !candle.close) {
    html += '<tr><td colspan="2" style="text-align:center;padding:30px;color:var(--muted);">Deploy a strategy to see live signals</td></tr>';
  } else {
    // OHLCV rows
    var ohlcv = [['current', candle.close],['open', candle.open],['high', candle.high],['low', candle.low],['close', candle.close],['volume', candle.volume]];
    var indicators = d.current_indicators || {};
    var indEntries = [];
    Object.keys(indicators).forEach(function(k) { indEntries.push([k, indicators[k]]); });
    var allRows = ohlcv.concat(indEntries);
    allRows.forEach(function(pair) {
      var k = pair[0], v = pair[1];
      var isInd = !['current','open','high','low','close','volume'].includes(k);
      var valStr = typeof v === 'number' ? (v > 100 ? v.toLocaleString('en-US', {maximumFractionDigits:2}) : v.toFixed(4)) : (v != null ? v : '—');
      html += '<tr style="border-bottom:1px solid rgba(255,255,255,0.04);">';
      html += '<td style="padding:7px 16px;color:' + (isInd ? 'var(--text)' : 'var(--muted)') + ';font-weight:' + (isInd ? '600' : '400') + ';">' + _escapeHtml(k) + '</td>';
      html += '<td style="padding:7px 16px;text-align:right;font-family:\'JetBrains Mono\',monospace;font-size:12px;color:' + (isInd ? 'var(--accent2)' : 'var(--text)') + ';">' + _escapeHtml(valStr) + '</td>';
      html += '</tr>';
    });
  }
  html += '</table></div></div>';

  // RIGHT: Open Positions + Event Log
  html += '<div>';

  // Open Positions
  var openTrades = d.open_trades || [];
  html += '<h4 style="margin:0 0 8px;font-size:13px;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px;">📈 Open Positions</h4>';
  html += '<div style="background:rgba(0,0,0,0.15);border:1px solid rgba(255,255,255,0.04);border-radius:10px;overflow:hidden;margin-bottom:12px;">';
  html += '<table class="trade-table live-monitor-table" id="live-open-table-' + panelKey + '" style="width:100%;font-size:12px;">';
  html += '<thead><tr style="border-bottom:1px solid rgba(255,255,255,0.06);color:var(--muted);"><th style="padding:7px 10px;text-align:left;">Symbol</th><th style="text-align:right;padding:7px 10px;">Side</th><th style="text-align:right;padding:7px 10px;">Entry $</th><th style="text-align:right;padding:7px 10px;">Notional</th></tr></thead>';
  html += '<tbody>';
  if (!openTrades.length) {
    html += '<tr><td colspan="4" style="text-align:center;padding:16px;color:var(--muted);font-size:12px;">No open positions</td></tr>';
  } else {
    openTrades.forEach(function(p) {
      var symbol = _escapeHtml(p.symbol || d.symbol || '—');
      var side = _escapeHtml(p.side || '—');
      html += '<tr style="border-bottom:1px solid rgba(255,255,255,0.04);">';
      html += '<td style="padding:7px 10px;">' + symbol + '</td>';
      html += '<td style="padding:7px 10px;text-align:right;color:' + ((p.side||'').toLowerCase() === 'long' ? 'var(--green)' : 'var(--red)') + ';">' + side + '</td>';
      html += '<td style="padding:7px 10px;text-align:right;font-family:\'JetBrains Mono\',monospace;">' + fmtINRPrice(parseFloat(p.entry_price) || 0) + '</td>';
      html += '<td style="padding:7px 10px;text-align:right;font-family:\'JetBrains Mono\',monospace;">' + fmtINR(parseFloat(p.notional) || 0, 0) + '</td>';
      html += '</tr>';
    });
  }
  html += '</tbody></table></div>';

  // Event Log
  var events = (d.event_log || []).slice().reverse();
  var evtColors = {signal:'#4ade80', error:'#f87171', warning:'#fbbf24', warn:'#fbbf24', stop:'#f87171', start:'#4ade80', entry:'#4ade80', exit:'#fbbf24', info:'var(--muted)'};
  html += '<h4 style="margin:0 0 8px;font-size:13px;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px;">📋 Event Log</h4>';
  html += '<div class="trade-log-panel" style="max-height:220px;">';
  if (!events.length) {
    html += '<div style="color:var(--muted);padding:12px;">No events yet.</div>';
  } else {
    events.forEach(function(e) {
      html += '<div class="log-entry"><span class="log-time">[' + _escapeHtml(e.time || '') + ']</span> <span style="color:' + (evtColors[e.type] || 'var(--text)') + ';">[' + _escapeHtml((e.type || 'info').toUpperCase()) + ']</span> ' + _escapeHtml(e.message || '') + '</div>';
    });
  }
  html += '</div>';

  html += '</div>'; // end right column
  html += '</div>'; // end 2-column grid

  // ── Completed Trades table (full width)
  var closedTrades = _getLiveClosedTradeRows(d);
  var tradeState = _getLiveTradeState(runId);
  var tradeFilters = tradeState.filters;
  var tradeSelected = tradeState.selected;
  var visibleTrades = _filterLiveClosedTrades(closedTrades, tradeFilters);
  var closedTradeTotal = typeof d.closed_trades === 'number' ? d.closed_trades : closedTrades.length;
  var closedTradeSummary = closedTradeTotal > closedTrades.length
    ? (visibleTrades.length + ' shown from latest ' + closedTrades.length + ' of ' + closedTradeTotal + ' trades')
    : (visibleTrades.length + ' shown of ' + closedTradeTotal + ' trade' + (closedTradeTotal === 1 ? '' : 's'));
  var existingKeys = {};
  closedTrades.forEach(function(t) { existingKeys[_getLiveTradeRowKey(t)] = true; });
  Object.keys(tradeSelected).forEach(function(key) {
    if (!existingKeys[key]) delete tradeSelected[key];
  });
  var selectedCount = closedTrades.filter(function(t) { return !!tradeSelected[_getLiveTradeRowKey(t)]; }).length;
  var visibleAllSelected = visibleTrades.length > 0 && visibleTrades.every(function(t) {
    return !!tradeSelected[_getLiveTradeRowKey(t)];
  });
  var filterDefs = [
    { key: 'all', label: 'All' },
    { key: 'long', label: 'Long' },
    { key: 'short', label: 'Short' },
    { key: 'wins', label: 'Wins' },
    { key: 'losses', label: 'Losses' },
  ];
  html += '<div style="margin:0 28px 28px;">';
  html += '<div class="table-toolbar">';
  html += '<div class="table-title-group">';
  html += '<h4 class="table-title">Completed Trades</h4>';
  html += '<div class="table-meta">' + closedTradeSummary + '</div>';
  html += '</div>';
  html += '<div class="table-actions">';
  html += '<div class="filter-pill-row">';
  html += filterDefs.map(function(def) {
    var active = tradeFilters.mode === def.key;
    return '<button class="filter-pill ' + _getFilterPillClass(def.key) + (active ? ' active' : '') + '" data-cf-click="_setLiveTradeFilter(\'' + safeRunId + '\',\'' + def.key + '\')">' + def.label + '</button>';
  }).join('');
  html += '</div>';
  html += '<input class="table-search-input" type="text" value="' + _escapeHtml(tradeFilters.query || '') + '" data-cf-change="_setLiveTradeQuery(\'' + safeRunId + '\', this.value)" data-cf-keydown="if(event.key===\'Enter\'){_setLiveTradeQuery(\'' + safeRunId + '\', this.value)}" placeholder="Search symbol, date, reason">';
  html += '</div></div>';
  if (selectedCount > 0) {
    html += '<div class="table-selection-bar">';
    html += '<div class="table-selection-count">' + selectedCount + ' trade' + (selectedCount === 1 ? '' : 's') + ' selected</div>';
    html += '<div class="table-actions">';
    html += '<button class="btn btn-warning btn-sm" data-cf-click="_exportLiveTradeSelection(\'' + safeRunId + '\')" style="padding:5px 12px;font-size:11px;">📥 Export Selected</button>';
    html += '<button class="btn btn-outline btn-sm" data-cf-click="_clearLiveTradeSelection(\'' + safeRunId + '\')" style="padding:5px 12px;font-size:11px;">Clear</button>';
    html += '</div></div>';
  }
  html += '<div style="overflow:auto;max-height:420px;background:rgba(0,0,0,0.15);border:1px solid rgba(255,255,255,0.04);border-radius:10px;">';
  html += '<table class="trade-table live-monitor-table" id="live-completed-table-' + panelKey + '" style="width:100%;font-size:12px;min-width:1080px;">';
  html += '<thead><tr style="position:sticky;top:0;background:rgba(10,14,24,0.96);backdrop-filter:blur(10px);border-bottom:1px solid rgba(255,255,255,0.08);color:var(--muted);z-index:1;">';
  html += '<th style="padding:8px 10px;text-align:center;width:36px;"><input type="checkbox" class="tbl-chk" data-cf-change="_toggleAllLiveTradeChecks(\'' + safeRunId + '\', this.checked)"' + (visibleAllSelected ? ' checked' : '') + '></th>';
  html += '<th style="padding:8px 10px;text-align:left;">Trade</th><th style="padding:8px 10px;text-align:left;">Entry At</th><th style="padding:8px 10px;text-align:left;">Exit At</th><th style="padding:8px 10px;text-align:left;">Side</th><th style="padding:8px 10px;text-align:right;">Entry $</th><th style="padding:8px 10px;text-align:right;">Exit $</th><th style="padding:8px 10px;text-align:right;">P&amp;L</th><th style="padding:8px 10px;text-align:left;">Reason</th>';
  html += '</tr></thead><tbody>';
  if (!closedTrades.length) {
    html += '<tr><td colspan="9" style="text-align:center;padding:20px;color:var(--muted);">No completed trades yet</td></tr>';
  } else if (!visibleTrades.length) {
    html += '<tr><td colspan="9" style="text-align:center;padding:20px;color:var(--muted);">No trades match the current filters.</td></tr>';
  } else {
    visibleTrades.forEach(function(t) {
      var key = _getLiveTradeRowKey(t);
      var tpnl = parseFloat(t.pnl) || 0;
      var entryPrice = parseFloat(t.entry_price) || 0;
      var exitPrice = parseFloat(t.exit_price) || 0;
      var entryParts = _getTradeDateParts(t.entry_time);
      var exitParts = _getTradeDateParts(t.exit_time);
      var meta = [];
      if (t.id !== undefined && t.id !== null && t.id !== '') meta.push('#' + t.id);
      if (t.leverage) meta.push(t.leverage + 'x');
      if (t.size) meta.push('size ' + t.size);
      if (t.notional) meta.push(fmtINR(parseFloat(t.notional) || 0) + ' notional');
      html += '<tr style="border-bottom:1px solid rgba(255,255,255,0.04);">';
      html += '<td style="padding:7px 10px;text-align:center;"><input type="checkbox" class="tbl-chk" data-cf-change="_toggleLiveTradeCheck(\'' + safeRunId + '\',\'' + _escapeJsString(key) + '\', this.checked)"' + (tradeSelected[key] ? ' checked' : '') + '></td>';
      html += '<td style="padding:7px 10px;"><div class="table-row-label">' + _escapeHtml(t.symbol || d.symbol || '—') + '</div><div class="table-note">' + _escapeHtml(meta.length ? meta.join(' • ') : '—') + '</div></td>';
      html += '<td style="padding:7px 10px;"><div class="table-datetime"><div class="table-datetime-date">' + entryParts.date + '</div><div class="table-datetime-time">' + entryParts.time + '</div></div></td>';
      html += '<td style="padding:7px 10px;"><div class="table-datetime"><div class="table-datetime-date">' + exitParts.date + '</div><div class="table-datetime-time">' + exitParts.time + '</div></div></td>';
      html += '<td style="padding:7px 10px;"><span class="tag ' + ((t.side || '').toUpperCase() === 'LONG' ? 'tag-green' : 'tag-red') + '">' + _escapeHtml(t.side || '—') + '</span></td>';
      html += '<td style="padding:7px 10px;text-align:right;font-family:\'JetBrains Mono\',monospace;">' + fmtINRPrice(entryPrice) + '</td>';
      html += '<td style="padding:7px 10px;text-align:right;font-family:\'JetBrains Mono\',monospace;">' + fmtINRPrice(exitPrice) + '</td>';
      html += '<td style="padding:7px 10px;text-align:right;font-family:\'JetBrains Mono\',monospace;font-weight:700;color:' + (tpnl >= 0 ? 'var(--green)' : 'var(--red)') + ';">' + fmtINR(tpnl) + '</td>';
      html += '<td style="padding:7px 10px;">' + reasonTag(t.exit_reason || '—') + '</td>';
      html += '</tr>';
    });
  }
  html += '</tbody></table></div></div>';

  container.innerHTML = html;
  _renderTablePager('live-signal-table-' + panelKey, 'live-signal-table-' + panelKey, 'live-signal-pagination-' + panelKey);
  _renderTablePager('live-open-table-' + panelKey, 'live-open-table-' + panelKey, 'live-open-pagination-' + panelKey);
  _renderTablePager('live-completed-table-' + panelKey, 'live-completed-table-' + panelKey, 'live-completed-pagination-' + panelKey);
}

async function stopEngine(runId, mode) {
  var ok = await cfConfirm('Stop this ' + mode + ' engine?', 'Stop Engine', '⏹️');
  if (!ok) return;
  var endpoint = mode === 'live' ? '/api/live/stop' : '/api/paper/stop';
  try {
    await cfApiFetch(endpoint, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({run_id: runId}),
    });
    cfToast('Engine stopped', 'success');
    setTimeout(loadLiveMonitor, 500);
  } catch(e) { cfToast('Stop failed: ' + e.message, 'error'); }
}

async function downloadEngineCSV(runId, mode) {
  try {
    var endpoint = mode === 'live' ? '/api/live/trades/csv' : '/api/paper/trades/csv';
    if (runId) endpoint += '?run_id=' + encodeURIComponent(runId);
    var r = await fetch(endpoint, { credentials: 'same-origin' });
    if (r.ok) {
      var blob = await r.blob();
      var url = URL.createObjectURL(blob);
      var a = document.createElement('a');
      a.href = url; a.download = mode + '_trades_' + (runId || 'all') + '.csv'; a.click();
      URL.revokeObjectURL(url);
      cfToast('Trades exported', 'success');
    } else {
      cfToast('No trades to export', 'warning');
    }
  } catch(e) { cfToast('Export failed', 'error'); }
}

function viewEngineDetails(runId, mode) {
  // Show a detailed overlay with full trade stats
  var engines = _liveEngines || [];
  var d = engines.find(function(e) { return e.run_id === runId; });
  if (!d) { cfToast('Engine data not available', 'warning'); return; }

  var trades = d.recent_trades || [];
  var pnl = d.total_pnl || 0;
  var winners = trades.filter(function(t) { return (t.pnl || 0) > 0; });
  var losers = trades.filter(function(t) { return (t.pnl || 0) <= 0; });
  var winRate = trades.length ? (winners.length / trades.length * 100).toFixed(1) : '0.0';
  var avgWin = winners.length ? (winners.reduce(function(s,t){return s + t.pnl;}, 0) / winners.length).toFixed(2) : '0.00';
  var avgLoss = losers.length ? (losers.reduce(function(s,t){return s + t.pnl;}, 0) / losers.length).toFixed(2) : '0.00';
  var bestTrade = trades.length ? Math.max.apply(null, trades.map(function(t){return t.pnl||0;})) : 0;
  var worstTrade = trades.length ? Math.min.apply(null, trades.map(function(t){return t.pnl||0;})) : 0;

  var name = _escapeHtml(d.strategy_name || d.run_name || 'Engine');
  var wrapper = document.createElement('div');
  wrapper.style.maxWidth = '600px';
  wrapper.style.margin = '0 auto';
  wrapper.innerHTML =
    '<h3 style="margin:0 0 16px;font-size:18px;">' + name + ' — ' + _escapeHtml(d.symbol || '') + '</h3>'
    + '<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:16px;">'
    + '<div class="ti-item"><div class="ti-label">Total P&L</div><div class="ti-value" style="color:' + (pnl >= 0 ? 'var(--green)' : 'var(--red)') + ';font-weight:700;">$' + pnl.toFixed(2) + '</div></div>'
    + '<div class="ti-item"><div class="ti-label">Win Rate</div><div class="ti-value">' + winRate + '%</div></div>'
    + '<div class="ti-item"><div class="ti-label">Total Trades</div><div class="ti-value">' + trades.length + '</div></div>'
    + '<div class="ti-item"><div class="ti-label">Winners / Losers</div><div class="ti-value" style="color:var(--green);">' + winners.length + '<span style="color:var(--muted);"> / </span><span style="color:var(--red);">' + losers.length + '</span></div></div>'
    + '<div class="ti-item"><div class="ti-label">Avg Win</div><div class="ti-value" style="color:var(--green);">$' + avgWin + '</div></div>'
    + '<div class="ti-item"><div class="ti-label">Avg Loss</div><div class="ti-value" style="color:var(--red);">$' + avgLoss + '</div></div>'
    + '<div class="ti-item"><div class="ti-label">Best Trade</div><div class="ti-value" style="color:var(--green);">$' + bestTrade.toFixed(2) + '</div></div>'
    + '<div class="ti-item"><div class="ti-label">Worst Trade</div><div class="ti-value" style="color:var(--red);">$' + worstTrade.toFixed(2) + '</div></div>'
    + '</div>'
    + '<div style="background:rgba(0,0,0,0.15);border:1px solid rgba(255,255,255,0.04);border-radius:10px;padding:12px;font-size:12px;">'
    + '<div style="margin-bottom:4px;"><b>Mode:</b> ' + _escapeHtml(mode || 'paper') + '  |  <b>Leverage:</b> ' + (d.leverage || 1) + 'x  |  <b>Side:</b> ' + _escapeHtml(d.side || d.trade_side || '') + '</div>'
    + '<div><b>Run ID:</b> <span style="font-family:monospace;font-size:10px;color:var(--muted);">' + _escapeHtml(runId) + '</span></div>'
    + '</div>';

  cfModal('Engine Details — ' + name, wrapper, '📊', [{label:'OK', cls:'btn-primary'}]);
}

// ── Portfolio Monthly/Yearly P&L ───────────────────────────
let _portfolioHistory = null;
let _monthlyViewMonth = null; // YYYY-MM

async function loadPortfolioHistory() {
  try {
    var r = await fetch('/api/portfolio/history', { credentials: 'same-origin' });
    _portfolioHistory = await r.json();
    if (!_monthlyViewMonth) {
      var now = new Date();
      _monthlyViewMonth = now.getFullYear() + '-' + String(now.getMonth() + 1).padStart(2, '0');
    }
    renderMonthlyDailyGrid();
    renderYearlyMonthlyTable();
  } catch(e) { console.error('Portfolio history error:', e); }
}

function changeMonthlyMonth(delta) {
  if (!_monthlyViewMonth) {
    var now = new Date();
    _monthlyViewMonth = now.getFullYear() + '-' + String(now.getMonth() + 1).padStart(2, '0');
  }
  var parts = _monthlyViewMonth.split('-');
  var yr = parseInt(parts[0]);
  var mo = parseInt(parts[1]) + delta;
  if (mo < 1) { yr--; mo = 12; }
  if (mo > 12) { yr++; mo = 1; }
  _monthlyViewMonth = yr + '-' + String(mo).padStart(2, '0');
  renderMonthlyDailyGrid();
}

function renderMonthlyDailyGrid() {
  var titleEl = document.getElementById('monthly-title');
  var gridEl = document.getElementById('monthly-daily-grid');
  var tableEl = document.getElementById('monthly-daily-table');
  if (!titleEl || !gridEl) return;
  if (!_monthlyViewMonth) {
    var now = new Date();
    _monthlyViewMonth = now.getFullYear() + '-' + String(now.getMonth() + 1).padStart(2, '0');
  }
  var parts = _monthlyViewMonth.split('-');
  var yr = parseInt(parts[0]);
  var mo = parseInt(parts[1]);
  var monthNames = ['January','February','March','April','May','June','July','August','September','October','November','December'];
  titleEl.textContent = monthNames[mo - 1] + ' ' + yr + ' P&L';
  var daily = (_portfolioHistory && _portfolioHistory.daily) ? _portfolioHistory.daily : {};
  // Generate days in month
  var daysInMonth = new Date(yr, mo, 0).getDate();
  var html = '';
  var totalPnl = 0;
  var totalTrades = 0;
  var dayData = [];
  for (var d = 1; d <= daysInMonth; d++) {
    var key = yr + '-' + String(mo).padStart(2, '0') + '-' + String(d).padStart(2, '0');
    var data = daily[key] || { pnl: 0, trades: 0 };
    var pnlForDay = data.total_pnl != null ? data.total_pnl : (data.pnl != null ? data.pnl : ((data.real_pnl || 0) + (data.paper_pnl || 0)));
    var tradesForDay = data.trades != null ? data.trades : ((data.real_trades || 0) + (data.paper_trades || 0));
    totalPnl += pnlForDay || 0;
    totalTrades += tradesForDay || 0;
    dayData.push({day: d, key: key, pnl: pnlForDay || 0, trades: tradesForDay || 0});
    var pnlVal = pnlForDay || 0;
    var color = pnlVal > 0 ? 'var(--green)' : pnlVal < 0 ? 'var(--red)' : 'var(--muted)';
    var bg = pnlVal > 0 ? 'rgba(34,197,94,0.06)' : pnlVal < 0 ? 'rgba(239,68,68,0.06)' : '';
    html += '<div class="day-cell" style="background:' + bg + ';">';
    html += '<div class="dc-day">' + d + '</div>';
    html += '<div class="dc-pnl" style="color:' + color + '">$' + pnlVal.toFixed(2) + '</div>';
    if (tradesForDay) html += '<div class="dc-trades">' + tradesForDay + ' trades</div>';
    html += '</div>';
  }
  gridEl.innerHTML = html;
  // Summary below
  if (tableEl) {
    tableEl.innerHTML = '<div style="display:flex;gap:20px;padding:12px;background:rgba(255,255,255,0.02);border-radius:10px;">'
      + '<div><span style="color:var(--muted);font-size:12px;">Monthly P&L:</span> <span style="font-weight:700;color:' + (totalPnl >= 0 ? 'var(--green)' : 'var(--red)') + '">$' + totalPnl.toFixed(2) + '</span></div>'
      + '<div><span style="color:var(--muted);font-size:12px;">Total Trades:</span> <span style="font-weight:700;">' + totalTrades + '</span></div>'
      + '</div>';
  }
}

function renderYearlyMonthlyTable() {
  var cont = document.getElementById('ytd-table');
  if (!cont) return;
  if (!_portfolioHistory || !_portfolioHistory.monthly) {
    cont.innerHTML = '<div style="color:var(--muted);text-align:center;padding:20px;">No historical data available yet.</div>';
    return;
  }
  var monthly = _portfolioHistory.monthly || {};
  var monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  // Collect years
  var years = {};
  Object.keys(monthly).forEach(function(key) {
    var parts = key.split('-');
    var yr = parts[0];
    var mo = parseInt(parts[1]) - 1;
    if (!years[yr]) years[yr] = new Array(12).fill(null);
    years[yr][mo] = monthly[key];
  });
  var yearKeys = Object.keys(years).sort();
  if (yearKeys.length === 0) {
    cont.innerHTML = '<div style="color:var(--muted);text-align:center;padding:20px;">No historical data available.</div>';
    return;
  }
  var html = '<div class="ytd-grid">';
  // Headers
  html += '<div class="ytd-header">Year</div>';
  monthNames.forEach(function(m) { html += '<div class="ytd-header">' + m + '</div>'; });
  // Rows
  yearKeys.forEach(function(yr) {
    html += '<div class="ytd-year-label">' + yr + '</div>';
    years[yr].forEach(function(data) {
      if (data === null) {
        html += '<div class="ytd-cell" style="color:var(--muted);">—</div>';
      } else {
        var pnl = data.total_pnl != null ? data.total_pnl : (data.pnl || 0);
        var tradeCount = data.trades != null ? data.trades : ((data.real_trades || 0) + (data.paper_trades || 0));
        var color = pnl >= 0 ? 'var(--green)' : 'var(--red)';
        var bg = pnl >= 0 ? 'rgba(34,197,94,0.08)' : 'rgba(239,68,68,0.08)';
        html += '<div class="ytd-cell" style="background:' + bg + ';color:' + color + ';" title="$' + pnl.toFixed(2) + ' (' + tradeCount + ' trades)">$' + (Math.abs(pnl) >= 1000 ? (pnl/1000).toFixed(1) + 'K' : pnl.toFixed(0)) + '</div>';
      }
    });
  });
  html += '</div>';
  cont.innerHTML = html;
}

// ── Init ───────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  _cfPageHistoryDepth = Math.max(0, Number(window.history && window.history.state && window.history.state.cfDepth) || 0);
  cfUpdateAppNavControls();
  initCryptoSelector();
  renderLeverage(leverageOptions, selectedLeverage);
  renderBuilderDeck();
  renderTemplates();
  loadBrokerSettings(true);
  refreshBrokerState(true);
  loadDashboard();
  refreshTopbarTicker();
  connectWS();
  requestNotificationPermission();
  fetchStrategies();
  cfApplyScalpDefaults();
  cfUpdateSLTPHints();
  cfUpdateScalpSymbol();
  cfToggleScalpLiveSafety();
  cfInitOperatorLounge();
  cfRefreshScalpEntryLaneFromState();
  cfSyncScalpLogPanelHeight();

  ['cf-scalp-entry-stop', 'cf-scalp-entry-limit'].forEach(function(id) {
    var el = document.getElementById(id);
    if (el) el.addEventListener('input', cfRefreshScalpEntryLaneFromState);
  });
  var qtyModeEl = document.getElementById('cf-scalp-qty-mode');
  if (qtyModeEl) qtyModeEl.addEventListener('change', cfUpdateScalpQtyUi);
  var scalpAck = document.getElementById('cf-scalp-live-ack');
  if (scalpAck) scalpAck.addEventListener('change', cfRefreshScalpEntryLaneFromState);
  var scalpMode = document.getElementById('cf-scalp-mode');
  if (scalpMode) scalpMode.addEventListener('change', cfRefreshScalpEntryLaneFromState);
  window.addEventListener('resize', cfSyncScalpLogPanelHeight);

  setInterval(refreshTopbarTicker, 30000);
  setInterval(pollLiveStatus, 10000);
  setInterval(function() { if (document.getElementById('portfolio-page').classList.contains('active-page')) loadPortfolioData(); }, 60000);
  // Restore the initial page and seed browser history so minimal-ui back/refresh works in the installed app
  var savedTab = localStorage.getItem('cf_active_tab');
  var initialPageId = cfPageIdFromLocation();
  if (!initialPageId && savedTab && document.getElementById(savedTab + '-page')) {
    initialPageId = savedTab + '-page';
  }
  if (!initialPageId) initialPageId = 'dashboard-page';
  showPage(initialPageId, cfNavButtonForPage(initialPageId), { replaceHistory: true });

  ['b-name', 'b-capital', 'b-possize-mode', 'b-possize', 'b-sl', 'b-tp', 'b-trail', 'b-fee', 'b-spread', 'b-slippage', 'b-funding', 'b-maxtrades', 'b-from', 'b-to', 'b-interval'].forEach(function(id) {
    var el = document.getElementById(id);
    if (el) {
      el.addEventListener('input', renderBuilderDeck);
      el.addEventListener('change', renderBuilderDeck);
    }
  });
});

// ══════════════════════════════════════════════════════
// CRYPTO SCALP ENGINE
// ══════════════════════════════════════════════════════

var _cfScalpPollTimer = null;
var _cfScalpActivityTimer = null;
var _cfScalpLastActivityFetch = 0;
var _cfScalpActivityInFlight = null;
var _cfLatestScalpStatus = {};
var _cfAppSocket = null;
var _cfAppSocketConnected = false;
var _cfScalpLastWsUpdateAt = 0;
// Persistent trade cache — never loses trades across polls
var _cfTradeCache = new Map();
var _cfScalpEventCache = new Map();
// Track the rendered open-trade structure so scale-ins and target edits repaint immediately
var _cfOpenTradeSnapshot = "";
var _cfOpenTradeDisplayCache = [];
var _cfOpenTradeDisplayCacheUntil = 0;
var _cfScalpActionLocks = new Map();
var _cfScalpActionCooldowns = new Map();
var _cfScalpActionCooldownTimer = 0;
var _cfScalpEntrySubmitBusy = "";
var _cfScalpLastStatusOkAt = 0;
var _cfScalpRefreshInFlight = null;
var _cfScalpReconcileInFlight = null;
var _cfScalpLastReconcileAt = 0;
var _cfOperatorFactIndex = 0;
var _cfOperatorPuzzleIndex = 0;
var _cfOperatorReadIndex = 0;
const _cfOperatorFacts = [
  'A feed that is 2-3 seconds late can still look alive while being completely wrong for scalping. Freshness matters more than visual movement.',
  'Most bad scalp entries happen after the trader loses track of market state, not because the button was hard to find.',
  'When funding cools while price holds trend, continuation quality is usually better than when both spike together.',
  'A guardrail price is not just a safety trigger. It is a way to keep patience encoded in the execution path.',
  'If your mark price source keeps flipping between WS and REST, your first task is feed stability, not strategy tweaking.',
  'The best backtest metric for a scalper is often execution drag, because it tells you how much edge disappears in the real path.'
];
const _cfOperatorPuzzles = [
  {
    title: 'Puzzle 01 — Trend Or Trap?',
    prompt: 'BTC prints higher highs on 5m, open interest is flat, funding is cooling, and basis is stable. Are you more likely looking at healthy continuation or late leveraged chasing?',
    answer: 'More likely healthy continuation. Flat leverage and cooling funding suggest the move is not being driven by crowded late longs.'
  },
  {
    title: 'Puzzle 02 — Wait Or Fire?',
    prompt: 'Your setup is valid but the last mark update is 11 seconds old and source is REST quote. Do you hit entry because price looks close enough or wait for a fresh tick?',
    answer: 'Wait. For a scalp, stale price is a structural risk, not a cosmetic one. Execution on an old mark destroys decision quality.'
  },
  {
    title: 'Puzzle 03 — Good PnL, Bad Process?',
    prompt: 'You took profit quickly, but the trade only worked because the spread snapped in your favor after a delayed entry fill. Was that a good execution?',
    answer: 'No. Positive outcome does not prove good process. If fill quality is random, the strategy edge is overstated.'
  },
  {
    title: 'Puzzle 04 — When To Use Guardrails?',
    prompt: 'Price is ranging under a breakout shelf. You want in only if momentum proves itself. Do you market in early or arm a guardrail above the shelf?',
    answer: 'Arm the guardrail. It preserves the thesis while reducing impulse entries before confirmation.'
  }
];
const _cfOperatorReads = [
  {
    title: 'Read — The 20 Second Check',
    body: 'Before any scalp, check four things in order: feed freshness, symbol context, current spread behaviour, and where your invalidation actually lives. This takes less than 20 seconds and filters out most low-quality clicks.'
  },
  {
    title: 'Read — Why Delayed Feeds Feel Dangerous',
    body: 'A delayed feed does not just make the UI look slow. It changes the trade itself. Your entry becomes a guess on an old state, and then your stop and target are anchored to the wrong moment.'
  },
  {
    title: 'Read — Backtests For Scalpers Need Friction',
    body: 'A scalp backtest without spread, slippage, and funding assumptions is not conservative enough. The raw signal can be fine while the realised path is untradeable after costs.'
  },
  {
    title: 'Read — Quiet Sessions Beat Forced Sessions',
    body: 'The operator who waits through dead tape is usually safer than the one who fills the session with random clicks. Good scalping is mostly selective boredom with a fast trigger.'
  }
];

function cfScalpWsFresh() {
  return _cfAppSocketConnected && _cfScalpLastWsUpdateAt > 0 && (Date.now() - _cfScalpLastWsUpdateAt) < 6000;
}

const _CF_SCALP_DEFAULTS = Object.freeze({
  symbol: 'BTCUSDT',
  qtyMode: 'usdt',
  qty: '10000',
  leverage: '10',
  sl: '1000',
  tp: '1000',
  slType: 'usdt',
  tpType: 'usdt',
  mode: 'paper',
  entryStop: '',
  entryLimit: '',
  slPrice: '',
  tpPrice: ''
});

function cfPrettyScalpSymbol(symbol) {
  var raw = String(symbol || '').toUpperCase();
  if (raw === 'PAXGUSD' || raw === 'GOLD' || raw === 'GOLDUSDT') return 'PAXGUSD';
  return raw || '—';
}

function cfApplyScalpDefaults() {
  var defaults = _CF_SCALP_DEFAULTS;
  var setValue = function(id, value) {
    var el = document.getElementById(id);
    if (el) el.value = value;
  };
  setValue('cf-scalp-symbol', defaults.symbol);
  setValue('cf-scalp-qty-mode', defaults.qtyMode);
  setValue('cf-scalp-qty', defaults.qty);
  setValue('cf-scalp-sl', defaults.sl);
  setValue('cf-scalp-tp', defaults.tp);
  setValue('cf-sl-type', defaults.slType);
  setValue('cf-tp-type', defaults.tpType);
  setValue('cf-scalp-mode', defaults.mode);
  setValue('cf-scalp-entry-stop', defaults.entryStop);
  setValue('cf-scalp-entry-limit', defaults.entryLimit);
  setValue('cf-scalp-sl-price', defaults.slPrice);
  setValue('cf-scalp-tp-price', defaults.tpPrice);
  var levEl = document.getElementById('cf-scalp-leverage');
  if (levEl) levEl.value = defaults.leverage;
  var liveAck = document.getElementById('cf-scalp-live-ack');
  if (liveAck) liveAck.checked = false;
  cfUpdateScalpQtyUi();
}

function cfScalpSelectedSymbol() {
  var el = document.getElementById('cf-scalp-symbol');
  return (el && el.value ? String(el.value) : '').toUpperCase();
}

function cfScalpSelectedMode() {
  var el = document.getElementById('cf-scalp-mode');
  return el && el.value === 'live' ? 'live' : 'paper';
}

function cfScalpSelectedQtyMode() {
  var el = document.getElementById('cf-scalp-qty-mode');
  return el && String(el.value).toLowerCase() === 'base' ? 'base' : 'usdt';
}

function cfScalpPendingPriceActive() {
  var stopEl = document.getElementById('cf-scalp-entry-stop');
  var limitEl = document.getElementById('cf-scalp-entry-limit');
  return (!!stopEl && Number(stopEl.value) > 0) || (!!limitEl && Number(limitEl.value) > 0);
}

function cfUpdateScalpQtyUi() {
  var mode = cfScalpSelectedQtyMode();
  var qtyEl = document.getElementById('cf-scalp-qty');
  var hintEl = document.getElementById('cf-scalp-qty-hint');
  if (!qtyEl) return;
  if (mode === 'base') {
    qtyEl.min = '0.0001';
    qtyEl.step = '0.0001';
    qtyEl.placeholder = '0.0015';
    if (hintEl) hintEl.textContent = 'Base asset quantity. Example: 0.0015 BTC is roughly a Rs.10,000 ticket.';
    if (!qtyEl.value || Number(qtyEl.value) >= 1000) qtyEl.value = '0.0015';
  } else {
    qtyEl.min = '1';
    qtyEl.step = '1';
    qtyEl.placeholder = '10000';
    if (hintEl) hintEl.textContent = 'Margin in USD. Final notional scales by leverage.';
    if (!qtyEl.value || Number(qtyEl.value) <= 1) qtyEl.value = _CF_SCALP_DEFAULTS.qty;
  }
  cfRefreshScalpEntryLaneFromState();
  cfSyncScalpLogPanelHeight();
}

var _cfScalpLogPanelHeightRaf = 0;
function cfSyncScalpLogPanelHeight() {
  if (_cfScalpLogPanelHeightRaf) window.cancelAnimationFrame(_cfScalpLogPanelHeightRaf);
  _cfScalpLogPanelHeightRaf = window.requestAnimationFrame(function() {
    _cfScalpLogPanelHeightRaf = 0;
    var scalpPage = document.getElementById('scalp-page');
    var formCard = document.querySelector('.cf-scalp-form-card');
    var logCard = document.querySelector('.cf-scalp-log-card');
    var logEl = document.getElementById('cf-scalp-event-log');
    if (!formCard || !logCard || !logEl) return;
    if (!scalpPage || !scalpPage.classList.contains('active-page') || window.innerWidth <= 1180) {
      logCard.style.height = '';
      logEl.style.height = '';
      return;
    }
    var formHeight = Math.ceil(formCard.getBoundingClientRect().height);
    if (!Number.isFinite(formHeight) || formHeight <= 0) {
      logCard.style.height = '';
      logEl.style.height = '';
      return;
    }
    logCard.style.height = formHeight + 'px';
    var cardRect = logCard.getBoundingClientRect();
    var logRect = logEl.getBoundingClientRect();
    var padBottom = parseFloat(window.getComputedStyle(logCard).paddingBottom) || 0;
    var available = Math.floor(cardRect.bottom - logRect.top - padBottom);
    logEl.style.height = Math.max(available, 160) + 'px';
  });
}

function cfScalpStateLabel(state) {
  var raw = String(state || '').toLowerCase();
  if (raw === 'fresh') return 'Fresh';
  if (raw === 'degraded') return 'Degraded';
  if (raw === 'stale') return 'Stale';
  if (raw === 'waiting') return 'Waiting';
  return raw ? (raw.charAt(0).toUpperCase() + raw.slice(1)) : 'Waiting';
}

function cfScalpGateTone(allowed, state) {
  var raw = String(state || '').toLowerCase();
  if (allowed && raw === 'degraded') return 'caution';
  if (allowed) return 'open';
  if (raw === 'waiting') return 'waiting';
  return 'blocked';
}

function cfScalpGateLabel(allowed, state) {
  var tone = cfScalpGateTone(allowed, state);
  if (tone === 'open' || tone === 'caution') return 'Open';
  if (tone === 'waiting') return 'Waiting';
  return 'Blocked';
}

function cfTrimUiText(text, maxLen) {
  var raw = String(text || '').trim();
  if (!raw) return '';
  return raw.length > maxLen ? raw.slice(0, maxLen - 1) + '…' : raw;
}

function cfSetScalpEntryButtonState(btn, enabled, reason) {
  if (!btn) return;
  if (!btn.dataset.defaultLabel) btn.dataset.defaultLabel = btn.textContent.trim();
  btn.dataset.cfAllowed = enabled ? 'true' : 'false';
  btn.disabled = !enabled;
  btn.classList.toggle('is-disabled', !enabled);
  btn.setAttribute('aria-disabled', enabled ? 'false' : 'true');
  if (!enabled && reason) btn.title = reason;
  else btn.removeAttribute('title');
}

function cfTitleCaseText(value) {
  return String(value || '')
    .replace(/_/g, ' ')
    .replace(/\b\w/g, function(ch) { return ch.toUpperCase(); });
}

function cfScalpNormalizeLifecycle(value) {
  var raw = String(value || '').trim().toLowerCase();
  if (!raw) return '';
  if (raw === 'paper_fill') return 'filled';
  if (raw === 'unknown') return 'submitted';
  if (raw === 'acknowledged') return 'acked';
  if (raw === 'canceled') return 'cancelled';
  if (raw === 'partially_filled' || raw === 'partially-filled' || raw === 'partial_fill') return 'partial';
  return raw;
}

function cfScalpLifecycleLabel(value) {
  var raw = cfScalpNormalizeLifecycle(value);
  var labels = {
    submitted: 'Submitted',
    acked: 'Acked',
    filled: 'Filled',
    partial: 'Partial',
    rejected: 'Rejected',
    cancelled: 'Cancelled',
    updated: 'Updated',
    verified: 'Verified',
    cleared: 'Cleared',
    armed: 'Armed',
    error: 'Error'
  };
  return labels[raw] || (raw ? cfTitleCaseText(raw) : '');
}

function cfScalpPhaseLabel(value) {
  var raw = String(value || '').trim().toLowerCase();
  var labels = {
    entry: 'Entry',
    exit: 'Exit',
    armed: 'Pending Entry',
    targets: 'TP/SL Update',
    scale_in: 'Scale-In',
    reconcile: 'Broker Sync',
    scale_in_reject: 'Scale-In Reject',
    entry_reject: 'Entry Reject',
    exit_reject: 'Exit Reject',
    scale_in_error: 'Scale-In Error',
    entry_error: 'Entry Error',
    exit_error: 'Exit Error',
    reconcile_error: 'Broker Sync Error',
  };
  return labels[raw] || (raw ? cfTitleCaseText(raw) : '');
}

function cfFormatQtyValue(value, mode) {
  var num = Number(value || 0);
  if (!Number.isFinite(num) || num <= 0) return mode === 'base' ? '0.0015' : '1000';
  if (mode === 'base') {
    if (num >= 1) return String(num.toFixed(4)).replace(/\.?0+$/, '');
    if (num >= 0.1) return String(num.toFixed(4)).replace(/\.?0+$/, '');
    if (num >= 0.01) return String(num.toFixed(5)).replace(/\.?0+$/, '');
    return String(num.toFixed(6)).replace(/\.?0+$/, '');
  }
  return String(num >= 100 ? Math.round(num) : num.toFixed(2)).replace(/\.?0+$/, '');
}

function cfScalpTradeAddConfig(trade) {
  var mode = String((trade && trade.qty_mode) || 'usdt').toLowerCase() === 'base' ? 'base' : 'usdt';
  var seedValue = mode === 'base'
    ? Number((trade && (trade.qty_value || trade.base_qty)) || 0.0015)
    : Number((trade && (trade.qty_value || trade.qty_usdt || trade.margin_usd)) || 1000);
  return {
    mode: mode,
    label: mode === 'base' ? 'Add Qty' : 'Add Margin $',
    helper: mode === 'base' ? 'same unit as entry' : 'margin before leverage',
    step: mode === 'base' ? '0.0001' : '1',
    min: mode === 'base' ? '0.000001' : '1',
    value: cfFormatQtyValue(seedValue, mode),
  };
}

function cfScalpExecStages(execMetrics) {
  var meta = execMetrics || {};
  var phase = String(meta.phase || '').toLowerCase();
  var lifecycle = cfScalpNormalizeLifecycle(meta.order_lifecycle || meta.fill_status || '');
  var acked = Number(meta.ack_ms) > 0 || ['acked', 'partial', 'filled', 'cancelled', 'rejected', 'updated'].includes(lifecycle);
  if (!phase) return [];
  if (phase === 'armed') return [{ label: 'Armed', tone: 'active' }];
  if (phase === 'reconcile') {
    var reconcileLabel = lifecycle === 'cleared' ? 'Cleared' : (lifecycle === 'updated' ? 'Aligned' : 'Verified');
    var reconcileTone = lifecycle === 'cleared' ? 'active' : 'success';
    return [
      { label: 'Broker Sync', tone: 'done' },
      { label: reconcileLabel, tone: reconcileTone }
    ];
  }
  if (phase === 'reconcile_error') {
    return [
      { label: 'Broker Sync', tone: 'done' },
      { label: 'Error', tone: 'error' }
    ];
  }
  if (phase === 'targets') {
    return [
      { label: 'Targets', tone: 'done' },
      { label: 'Updated', tone: lifecycle === 'rejected' || lifecycle === 'cancelled' ? 'error' : 'active' }
    ];
  }
  var finalLabel = phase === 'exit' ? 'Closed' : (phase === 'scale_in' ? 'Scaled' : 'Filled');
  if (lifecycle === 'partial') finalLabel = 'Partial';
  else if (lifecycle === 'rejected') finalLabel = 'Rejected';
  else if (lifecycle === 'cancelled') finalLabel = 'Cancelled';
  var finalTone = 'pending';
  if (lifecycle === 'rejected' || lifecycle === 'cancelled' || meta.verified === false) finalTone = 'error';
  else if (lifecycle === 'partial') finalTone = 'active';
  else if (meta.verified === true || lifecycle === 'filled' || lifecycle === 'updated') finalTone = 'success';
  return [
    { label: 'Submitted', tone: lifecycle === 'submitted' && !acked ? 'active' : 'done' },
    { label: 'Acked', tone: acked ? (lifecycle === 'acked' ? 'active' : 'done') : 'pending' },
    { label: finalLabel, tone: finalTone }
  ];
}

function cfScalpExecTone(execMetrics) {
  var meta = execMetrics || {};
  var phase = String(meta.phase || '').toLowerCase();
  var lifecycle = cfScalpNormalizeLifecycle(meta.order_lifecycle || meta.fill_status || '');
  if (!phase) return 'neutral';
  if (phase === 'reconcile_error') return 'error';
  if (phase === 'reconcile') return lifecycle === 'cleared' ? 'active' : 'success';
  if (meta.verified === false || meta.error || lifecycle === 'rejected' || lifecycle === 'cancelled') return 'error';
  if (phase === 'targets' || phase === 'armed' || lifecycle === 'submitted' || lifecycle === 'acked' || lifecycle === 'partial') return 'active';
  if (lifecycle === 'filled' || lifecycle === 'updated') return 'success';
  return 'active';
}

function cfScalpExecDetailHtml(execMetrics) {
  var meta = execMetrics || {};
  if (!meta.phase) return '<span class="cf-scalp-exec-note">Awaiting next broker action</span>';
  var stages = cfScalpExecStages(meta);
  var detail = cfScalpExecDetail(meta);
  var html = '';
  if (stages.length) {
    html += '<div class="cf-scalp-exec-stage-row">' + stages.map(function(stage) {
      return '<span class="cf-scalp-exec-chip" data-state="' + _escapeHtml(stage.tone) + '">' + _escapeHtml(stage.label) + '</span>';
    }).join('') + '</div>';
  }
  html += '<div class="cf-scalp-exec-note">' + _escapeHtml(detail) + '</div>';
  return html;
}

function cfScalpSyncEntrySubmitUi() {
  var buyBtn = cfEl('cf-scalp-buy-btn');
  var sellBtn = cfEl('cf-scalp-sell-btn');
  [
    { btn: buyBtn, action: 'BUY', busyLabel: 'Buying…' },
    { btn: sellBtn, action: 'SELL', busyLabel: 'Selling…' }
  ].forEach(function(item) {
    var btn = item.btn;
    if (!btn) return;
    if (!btn.dataset.defaultLabel) btn.dataset.defaultLabel = btn.textContent.trim();
    if (_cfScalpEntrySubmitBusy) {
      var active = _cfScalpEntrySubmitBusy === item.action;
      btn.disabled = true;
      btn.classList.toggle('loading', active);
      btn.classList.add('is-disabled');
      btn.setAttribute('aria-disabled', 'true');
      if (active) btn.setAttribute('aria-busy', 'true');
      else btn.removeAttribute('aria-busy');
      btn.textContent = active ? item.busyLabel : btn.dataset.defaultLabel;
      return;
    }
    btn.classList.remove('loading');
    btn.removeAttribute('aria-busy');
    btn.textContent = btn.dataset.defaultLabel;
    var enabled = btn.dataset.cfAllowed !== 'false';
    btn.disabled = !enabled;
    btn.classList.toggle('is-disabled', !enabled);
    btn.setAttribute('aria-disabled', enabled ? 'false' : 'true');
  });
}

function cfSetScalpEntrySubmitBusy(direction) {
  if (_cfScalpEntrySubmitBusy) return false;
  _cfScalpEntrySubmitBusy = String(direction || '').toUpperCase();
  cfScalpSyncEntrySubmitUi();
  return true;
}

function cfClearScalpEntrySubmitBusy() {
  _cfScalpEntrySubmitBusy = '';
  cfScalpSyncEntrySubmitUi();
}

function cfScalpTradeActionState(tradeId) {
  return _cfScalpActionLocks.get(String(tradeId || '')) || '';
}

function cfScalpTradeActionBusyMessage(action) {
  var raw = String(action || '').toLowerCase();
  if (raw === 'add') return 'Submitting scale-in request…';
  if (raw === 'targets') return 'Saving TP/SL update…';
  if (raw === 'exit') return 'Submitting exit request…';
  return 'Syncing broker action…';
}

function cfScalpTradeCooldownState(tradeId) {
  var key = String(tradeId || '');
  if (!key) return null;
  var meta = _cfScalpActionCooldowns.get(key);
  if (!meta) return null;
  var remainingMs = Math.max(0, Number(meta.until || 0) - Date.now());
  if (!(remainingMs > 0)) {
    _cfScalpActionCooldowns.delete(key);
    return null;
  }
  return {
    action: String(meta.action || 'sync'),
    remainingMs: remainingMs,
  };
}

function cfScheduleScalpTradeCooldownRefresh() {
  if (_cfScalpActionCooldownTimer || !_cfScalpActionCooldowns.size) return;
  _cfScalpActionCooldownTimer = window.setTimeout(function() {
    _cfScalpActionCooldownTimer = 0;
    Array.from(_cfScalpActionCooldowns.keys()).forEach(function(key) {
      cfSyncScalpTradeActionUi(key);
    });
    if (_cfScalpActionCooldowns.size) cfScheduleScalpTradeCooldownRefresh();
  }, 150);
}

function cfStartScalpTradeActionCooldown(tradeId, action, durationMs) {
  var key = String(tradeId || '');
  if (!key) return;
  _cfScalpActionCooldowns.set(key, {
    action: String(action || 'sync'),
    until: Date.now() + Math.max(600, Number(durationMs) || 1200),
  });
  cfSyncScalpTradeActionUi(key);
  cfScheduleScalpTradeCooldownRefresh();
}

function cfScalpTradeActionBlockMessage(tradeId) {
  var key = String(tradeId || '');
  if (!key) return 'Trade action is unavailable';
  var busyAction = cfScalpTradeActionState(key);
  if (busyAction) return cfScalpTradeActionBusyMessage(busyAction);
  var cooldown = cfScalpTradeCooldownState(key);
  if (cooldown) return 'Broker state is syncing • retry in ' + cfFormatLatency(cooldown.remainingMs);
  return 'This trade already has an action in progress';
}

function cfSyncScalpTradeActionUi(tradeId) {
  var key = String(tradeId || '');
  if (!key) return;
  var busyAction = cfScalpTradeActionState(key);
  var cooldown = cfScalpTradeCooldownState(key);
  var busy = !!busyAction;
  var locked = busy || !!cooldown;
  var row = document.querySelector('#cf-scalp-active-body tr[data-tid="' + key + '"]');
  var setBtn = cfEl('cf-set-btn-' + key);
  var addBtn = cfEl('cf-add-btn-' + key);
  var exitBtn = cfEl('cf-exit-btn-' + key);
  var syncNote = cfEl('cf-trade-sync-' + key);
  ['cf-tp-usd-', 'cf-tp-price-', 'cf-sl-usd-', 'cf-sl-price-', 'cf-add-qty-'].forEach(function(prefix) {
    var input = cfEl(prefix + key);
    if (input) input.disabled = locked;
  });
  if (row) {
    row.dataset.busy = busy ? 'true' : 'false';
    row.dataset.cooldown = cooldown ? 'true' : 'false';
  }
  [
    { btn: addBtn, action: 'add', busyLabel: 'Adding…' },
    { btn: setBtn, action: 'targets', busyLabel: 'Saving…' },
    { btn: exitBtn, action: 'exit', busyLabel: 'Exiting…' }
  ].forEach(function(item) {
    var btn = item.btn;
    if (!btn) return;
    if (!btn.dataset.defaultLabel) btn.dataset.defaultLabel = btn.textContent.trim();
    var active = busy && busyAction === item.action;
    var cooling = !busy && cooldown && cooldown.action === item.action;
    btn.disabled = locked;
    btn.classList.toggle('loading', active);
    btn.classList.toggle('is-disabled', locked);
    btn.setAttribute('aria-disabled', locked ? 'true' : 'false');
    if (active) btn.setAttribute('aria-busy', 'true');
    else btn.removeAttribute('aria-busy');
    btn.textContent = active ? item.busyLabel : (cooling ? 'Syncing…' : btn.dataset.defaultLabel);
  });
  if (syncNote) {
    if (busyAction) {
      syncNote.textContent = cfScalpTradeActionBusyMessage(busyAction);
      syncNote.dataset.state = 'active';
    } else if (cooldown) {
      syncNote.textContent = 'Broker state is syncing • ' + cfFormatLatency(cooldown.remainingMs);
      syncNote.dataset.state = 'active';
    } else {
      syncNote.textContent = '';
      syncNote.dataset.state = 'idle';
    }
  }
}

function cfSetScalpTradeActionLock(tradeId, action) {
  var key = String(tradeId || '');
  if (!key || _cfScalpActionLocks.has(key) || cfScalpTradeCooldownState(key)) return false;
  _cfScalpActionCooldowns.delete(key);
  _cfScalpActionLocks.set(key, String(action || 'busy'));
  cfSyncScalpTradeActionUi(key);
  return true;
}

function cfClearScalpTradeActionLock(tradeId) {
  var key = String(tradeId || '');
  if (!key) return;
  _cfScalpActionLocks.delete(key);
  cfSyncScalpTradeActionUi(key);
}

function cfSyncAllScalpTradeActionUi(openTrades) {
  var activeIds = new Set((openTrades || []).map(function(t) {
    return String(t.trade_id || t.id || '');
  }).filter(Boolean));
  Array.from(_cfScalpActionLocks.keys()).forEach(function(key) {
    if (!activeIds.has(key)) _cfScalpActionLocks.delete(key);
  });
  Array.from(_cfScalpActionCooldowns.keys()).forEach(function(key) {
    if (!activeIds.has(key)) _cfScalpActionCooldowns.delete(key);
  });
  activeIds.forEach(function(key) { cfSyncScalpTradeActionUi(key); });
}

function cfRenderOperatorFact() {
  var target = document.getElementById('cf-operator-fact');
  if (target) target.textContent = _cfOperatorFacts[_cfOperatorFactIndex % _cfOperatorFacts.length];
}

function cfRenderOperatorPuzzle() {
  var item = _cfOperatorPuzzles[_cfOperatorPuzzleIndex % _cfOperatorPuzzles.length];
  var title = document.getElementById('cf-operator-puzzle-title');
  var prompt = document.getElementById('cf-operator-puzzle');
  var answer = document.getElementById('cf-operator-answer');
  if (title) title.textContent = item.title;
  if (prompt) prompt.textContent = item.prompt;
  if (answer) {
    answer.textContent = item.answer;
    answer.classList.remove('revealed');
  }
}

function cfRenderOperatorRead() {
  var item = _cfOperatorReads[_cfOperatorReadIndex % _cfOperatorReads.length];
  var title = document.getElementById('cf-operator-read-title');
  var body = document.getElementById('cf-operator-read');
  if (title) title.textContent = item.title;
  if (body) body.textContent = item.body;
}

function cfInitOperatorLounge() {
  cfRenderOperatorFact();
  cfRenderOperatorPuzzle();
  cfRenderOperatorRead();
}

function cfOperatorNextFact() {
  _cfOperatorFactIndex = (_cfOperatorFactIndex + 1) % _cfOperatorFacts.length;
  cfRenderOperatorFact();
}

function cfOperatorRevealAnswer() {
  var answer = document.getElementById('cf-operator-answer');
  if (answer) answer.classList.add('revealed');
}

function cfOperatorNextPuzzle() {
  _cfOperatorPuzzleIndex = (_cfOperatorPuzzleIndex + 1) % _cfOperatorPuzzles.length;
  cfRenderOperatorPuzzle();
}

function cfOperatorNextRead() {
  _cfOperatorReadIndex = (_cfOperatorReadIndex + 1) % _cfOperatorReads.length;
  cfRenderOperatorRead();
}

function cfInitScalpPage() {
  cfRefreshScalpWorkspace({ reconcile: 'auto' }).finally(cfSyncScalpLogPanelHeight);
  cfRefreshScalpEntryLaneFromState();
  cfSyncScalpLogPanelHeight();
  if (!_cfScalpPollTimer) {
    _cfScalpPollTimer = setInterval(function() {
      if (!cfScalpWsFresh()) cfLoadScalpStatus();
    }, 2000);
  }
  if (!_cfScalpActivityTimer) {
    _cfScalpActivityTimer = setInterval(function() { cfLoadScalpActivity(false); }, 15000);
  }
}

var _origShowPage = showPage;
showPage = function(pageId, btn, options) {
  if (pageId !== 'scalp-page' && _cfScalpPollTimer) {
    clearInterval(_cfScalpPollTimer);
    _cfScalpPollTimer = null;
  }
  if (pageId !== 'scalp-page' && _cfScalpActivityTimer) {
    clearInterval(_cfScalpActivityTimer);
    _cfScalpActivityTimer = null;
  }
  _origShowPage(pageId, btn, options);
};

function _cfTradeRow(t) {
  const grossPnl = t.pnl || 0;
  const fees = t.fees || (t.size ? Math.round(t.size * 0.0005 * 1.18 * 2 * 10000) / 10000 : 0);
  const netPnl = t.net_pnl !== undefined ? t.net_pnl : Math.round((grossPnl - fees) * 100) / 100;
  const side = t.side || t.transaction_type || '';
  const timeParts = _getTradeDateParts(t.exit_time || t.entry_time);
  const sideTag = (side === 'LONG' || side === 'BUY') ? 'tag-green' : 'tag-red';
  const grossClass = grossPnl > 0 ? 'positive' : grossPnl < 0 ? 'negative' : '';
  const netClass = netPnl > 0 ? 'positive' : netPnl < 0 ? 'negative' : '';
  const qtyMeta = [];
  if (t.trade_id || t.id) qtyMeta.push('#' + (t.trade_id || t.id));
  if (t.leverage) qtyMeta.push(String(t.leverage) + 'x');
  if (t.qty_mode === 'base' && t.base_qty) qtyMeta.push(Number(t.base_qty).toFixed(6) + ' qty');
  else if (t.qty_usdt) qtyMeta.push('$' + Number(t.qty_usdt).toFixed(2) + ' margin');
  return `<tr>
    <td><div class="table-datetime"><div class="table-datetime-date">${timeParts.date}</div><div class="table-datetime-time">${timeParts.time}</div></div></td>
    <td><div class="table-row-label">${_escapeHtml(cfPrettyScalpSymbol(t.symbol || '—'))}</div><div class="table-note">${_escapeHtml(qtyMeta.join(' • ') || 'trade')}</div></td>
    <td><span class="tag ${sideTag}">${_escapeHtml(side || '—')}</span></td>
    <td><div class="table-value-stack"><div class="table-value-main">$${(t.entry_price || 0).toFixed(4)}</div></div></td>
    <td><div class="table-value-stack"><div class="table-value-main">$${(t.exit_price || 0).toFixed(4)}</div></div></td>
    <td><div class="table-value-stack"><div class="table-value-main">${fmtINR(fees)}</div><div class="table-value-sub">fees</div></div></td>
    <td><div class="table-value-stack"><div class="table-value-main ${grossClass}">${grossPnl >= 0 ? '+' : ''}${fmtINR(grossPnl)}</div><div class="table-value-sub ${grossClass}">gross</div></div></td>
    <td><div class="table-value-stack"><div class="table-value-main ${netClass}">${netPnl >= 0 ? '+' : ''}${fmtINR(netPnl)}</div><div class="table-value-sub ${netClass}">net</div></div></td>
    <td>${reasonTag(t.exit_reason || '—')}</td>
  </tr>`;
}

async function cfLoadScalpStatus() {
  try {
    const symbol = cfScalpSelectedSymbol();
    const url = symbol ? ('/api/scalp/status?symbol=' + encodeURIComponent(symbol)) : '/api/scalp/status';
    const r = await cfApiFetch(url, { cache: 'no-store' });
    const d = await cfReadApiPayload(r);
    if (!r.ok) {
      const staleStatus = cfBuildScalpCachedStatus(cfApiErrorDetail(d, 'Status refresh failed'));
      if (staleStatus) {
        cfApplyScalpStatus(staleStatus);
        return staleStatus;
      }
      return null;
    }
    _cfScalpLastStatusOkAt = Date.now();
    cfMergeScalpStatusPatch(d || {});
    cfApplyScalpStatus(_cfLatestScalpStatus);
    return _cfLatestScalpStatus;
  } catch(e) {
    const staleStatus = cfBuildScalpCachedStatus((e && e.message) ? e.message : 'Status refresh failed');
    if (staleStatus) {
      cfApplyScalpStatus(staleStatus);
      return staleStatus;
    }
    return null;
  }
}

function cfMergeScalpStatusPatch(payload) {
  if (!payload || typeof payload !== 'object') return _cfLatestScalpStatus || {};
  const merged = Object.assign({}, _cfLatestScalpStatus || {});
  if (payload.execution_metrics) {
    merged.execution_metrics = Object.assign({}, merged.execution_metrics || {}, payload.execution_metrics);
  }
  if (payload.feed_metrics) {
    merged.feed_metrics = Object.assign({}, merged.feed_metrics || {}, payload.feed_metrics);
  }
  if (payload.entry_controls) {
    merged.entry_controls = Object.assign({}, merged.entry_controls || {}, payload.entry_controls);
  }
  ['open_trades', 'pending_entries', 'closed_trades', 'event_log', 'file_trades', 'file_events'].forEach(function(key) {
    if (Array.isArray(payload[key])) merged[key] = payload[key].slice();
  });
  ['running', 'in_trade', 'session_pnl', 'symbol'].forEach(function(key) {
    if (Object.prototype.hasOwnProperty.call(payload, key)) merged[key] = payload[key];
  });
  _cfLatestScalpStatus = merged;
  return merged;
}

function cfBuildScalpCachedStatus(reason) {
  const hasLatest = _cfLatestScalpStatus && Object.keys(_cfLatestScalpStatus).length;
  const hasOpenCache = Array.isArray(_cfOpenTradeDisplayCache) && _cfOpenTradeDisplayCache.length;
  if (!hasLatest && !hasOpenCache) return null;
  const staleStatus = Object.assign({}, _cfLatestScalpStatus || {});
  staleStatus.feed_metrics = Object.assign({}, staleStatus.feed_metrics || {});
  staleStatus.feed_metrics.state = 'stale';
  staleStatus.feed_metrics.last_error = reason || 'Status refresh failed';
  if (!staleStatus.feed_metrics.symbol) staleStatus.feed_metrics.symbol = cfScalpSelectedSymbol();
  if (_cfScalpLastStatusOkAt > 0) {
    const staleAge = Date.now() - _cfScalpLastStatusOkAt;
    staleStatus.feed_metrics.age_ms = Math.max(Number(staleStatus.feed_metrics.age_ms) || 0, staleAge);
    staleStatus.feed_metrics.last_message_age_ms = Math.max(Number(staleStatus.feed_metrics.last_message_age_ms) || 0, staleAge);
  }
  if ((!Array.isArray(staleStatus.open_trades) || !staleStatus.open_trades.length) && hasOpenCache) {
    staleStatus.open_trades = _cfOpenTradeDisplayCache.slice();
  }
  staleStatus.running = !!staleStatus.running || !!(staleStatus.open_trades || []).length || !!(staleStatus.pending_entries || []).length;
  staleStatus.in_trade = !!(staleStatus.open_trades || []).length;
  staleStatus.client_snapshot = true;
  return staleStatus;
}

function cfScalpHasLiveExposure(status) {
  var state = status || {};
  var openTrades = Array.isArray(state.open_trades) ? state.open_trades : [];
  var pendingEntries = Array.isArray(state.pending_entries) ? state.pending_entries : [];
  if (openTrades.some(function(t) { return String((t && t.mode) || '').toLowerCase() === 'live'; })) return true;
  if (pendingEntries.some(function(t) { return String((t && t.mode) || '').toLowerCase() === 'live'; })) return true;
  return String((((state.execution_metrics || {}).mode) || '')).toLowerCase() === 'live';
}

function cfScalpShouldReconcile(status, force) {
  if (force) return true;
  var state = status || _cfLatestScalpStatus || {};
  if (!cfScalpHasLiveExposure(state)) return false;
  var sinceLast = Date.now() - _cfScalpLastReconcileAt;
  if (sinceLast < 8000) return false;
  var phase = String((((state.execution_metrics || {}).phase) || '')).toLowerCase();
  if ([
    'entry', 'scale_in', 'exit', 'reconcile',
    'entry_reject', 'scale_in_reject', 'exit_reject',
    'entry_error', 'scale_in_error', 'exit_error', 'reconcile_error'
  ].includes(phase)) {
    return true;
  }
  return Array.isArray(state.open_trades) && state.open_trades.some(function(t) {
    return String((t && t.mode) || '').toLowerCase() === 'live';
  });
}

async function cfReconcileScalpBroker(options) {
  var opts = options || {};
  var latest = opts.status || _cfLatestScalpStatus || {};
  var forced = !!opts.force;
  if (!forced && !cfScalpShouldReconcile(latest, false)) return latest;
  if (forced && !cfScalpHasLiveExposure(latest)) {
    if (opts.showToast) cfToast('No live scalp position needs broker sync right now', 'info');
    return latest;
  }
  if (_cfScalpReconcileInFlight) return _cfScalpReconcileInFlight;
  _cfScalpReconcileInFlight = (async function() {
    try {
      var r = await cfApiFetch('/api/scalp/reconcile', { method: 'POST', cache: 'no-store' });
      var d = await cfReadApiPayload(r);
      if (!r.ok) {
        var errorText = cfApiErrorDetail(d, 'Broker sync failed');
        cfMergeScalpStatusPatch({
          execution_metrics: {
            phase: 'reconcile_error',
            symbol: (((latest.execution_metrics || {}).symbol) || latest.symbol || cfScalpSelectedSymbol() || ''),
            mode: (((latest.execution_metrics || {}).mode) || 'live'),
            error: errorText,
          }
        });
        cfApplyScalpStatus(_cfLatestScalpStatus);
        if (!opts.silent) cfToast(errorText, 'warning');
        return d;
      }
      _cfScalpLastReconcileAt = Date.now();
      cfMergeScalpStatusPatch(d || {});
      cfApplyScalpStatus(_cfLatestScalpStatus);
      if (opts.showToast) {
        var rec = d.reconciliation || {};
        var parts = [];
        if (Number(rec.checked) > 0) parts.push('checked ' + rec.checked);
        if (Number(rec.updated) > 0) parts.push('updated ' + rec.updated);
        if (Number(rec.cleared) > 0) parts.push('cleared ' + rec.cleared);
        cfToast(parts.length ? ('Broker sync complete • ' + parts.join(' • ')) : 'Broker sync complete', 'success');
      }
      return d;
    } catch (e) {
      var message = (e && e.message) ? e.message : 'Broker sync failed';
      cfMergeScalpStatusPatch({
        execution_metrics: {
          phase: 'reconcile_error',
          symbol: (((latest.execution_metrics || {}).symbol) || latest.symbol || cfScalpSelectedSymbol() || ''),
          mode: (((latest.execution_metrics || {}).mode) || 'live'),
          error: message,
        }
      });
      cfApplyScalpStatus(_cfLatestScalpStatus);
      if (!opts.silent) cfToast(message, 'warning');
      return { status: 'error', message: message };
    } finally {
      _cfScalpReconcileInFlight = null;
    }
  })();
  return _cfScalpReconcileInFlight;
}

async function cfRefreshScalpWorkspace(options) {
  var opts = (typeof options === 'boolean') ? { forceSync: options } : (options || {});
  if (_cfScalpRefreshInFlight && !opts.forceReload) return _cfScalpRefreshInFlight;
  _cfScalpRefreshInFlight = (async function() {
    var status = await cfLoadScalpStatus();
    var reconcileMode = opts.forceSync ? 'force' : (opts.reconcile || 'none');
    if (reconcileMode === 'force' || cfScalpShouldReconcile(status, false)) {
      await cfReconcileScalpBroker({
        force: reconcileMode === 'force',
        status: status,
        silent: !opts.showToast,
        showToast: !!opts.showToast && reconcileMode === 'force',
      });
      status = await cfLoadScalpStatus();
    }
    await cfLoadScalpActivity(true);
    return status;
  })();
  try {
    return await _cfScalpRefreshInFlight;
  } finally {
    _cfScalpRefreshInFlight = null;
  }
}

async function cfLoadScalpActivity(force) {
  try {
    const scalpPage = document.getElementById('scalp-page');
    if (!force && (!scalpPage || !scalpPage.classList.contains('active-page'))) return;
    const now = Date.now();
    const minInterval = cfScalpWsFresh() ? 45000 : 15000;
    if (!force && _cfScalpActivityInFlight) return _cfScalpActivityInFlight;
    if (!force && now - _cfScalpLastActivityFetch < minInterval) return;
    _cfScalpLastActivityFetch = now;
    _cfScalpActivityInFlight = (async function() {
      const r = await cfApiFetch('/api/scalp/activity', { cache: 'no-store' });
      const d = await cfReadApiPayload(r);
      if (!r.ok) return;
      cfApplyScalpActivity(d || {});
    })();
    await _cfScalpActivityInFlight;
  } catch(e) {
  } finally {
    _cfScalpActivityInFlight = null;
  }
}

function cfApplyScalpActivity(status) {
  if (!status || (
    status.closed_trades === undefined
    && status.file_trades === undefined
    && status.event_log === undefined
    && status.file_events === undefined
  )) {
    return;
  }

  const memTrades = Array.isArray(status.closed_trades) ? status.closed_trades : [];
  const fileTrades = Array.isArray(status.file_trades) ? status.file_trades : [];
  [...memTrades, ...fileTrades].forEach(function(t) {
    const id = (t.trade_id || '') + '|' + (t.entry_time || '');
    _cfTradeCache.set(id, t);
  });

  const body = cfEl('cf-scalp-history-body');
  if (body) {
    const allTrades = Array.from(_cfTradeCache.values());
    allTrades.sort(function(a, b) {
      const ta = a.exit_time || a.entry_time || '';
      const tb = b.exit_time || b.entry_time || '';
      return String(tb).localeCompare(String(ta));
    });
    if (!allTrades.length) {
      body.innerHTML = '<tr><td colspan="9" class="cf-table-empty-cell">No trades yet</td></tr>';
    } else {
      body.innerHTML = allTrades.map(_cfTradeRow).join('');
    }
  }
  _renderTablePager('cf-scalp-history-table', 'cf-scalp-history-table', 'cf-scalp-history-pagination');

  const logEl = cfEl('cf-scalp-event-log');
  const fileEvents = Array.isArray(status.file_events) ? status.file_events : [];
  const eventLog = Array.isArray(status.event_log) ? status.event_log : [];
  const events = [...fileEvents, ...eventLog];
  events.forEach(function(e) {
    const key = (e.ts || e.time || '') + '|' + (e.level || e.type || '') + '|' + (e.msg || e.message || '');
    _cfScalpEventCache.set(key, e);
  });
  if (logEl) {
    const allEvents = Array.from(_cfScalpEventCache.values())
      .sort(function(a, b) { return String(b.ts || b.time || '').localeCompare(String(a.ts || a.time || '')); })
      .slice(0, 200);
    if (!allEvents.length) {
      logEl.innerHTML = '<div class="cf-scalp-event-empty">No events yet</div>';
    } else {
      logEl.innerHTML = allEvents.map(function(e) {
        const level = String(e.level || e.type || 'info').toLowerCase();
        const timeLabel = e.time || String(e.ts || '').split('T').pop().slice(0, 8) || '—';
        return '<div class="cf-scalp-event-row">'
          + '<span class="cf-scalp-event-time">' + _escapeHtml(timeLabel) + '</span>'
          + '<span class="cf-scalp-event-msg" data-level="' + _escapeHtml(level) + '">' + _escapeHtml(e.msg || e.message || '') + '</span>'
          + '</div>';
      }).join('');
    }
  }
}

function cfScalpFeedSummary(feed) {
  const meta = feed || {};
  const symbol = meta.symbol || cfScalpSelectedSymbol() || '';
  const stateLabel = cfScalpStateLabel(meta.state);
  const source = cfPriceSourceLabel(meta.source);
  const age = meta.age_ms !== undefined && meta.age_ms !== null ? cfFormatLatency(meta.age_ms) : '—';
  if (!symbol && !meta.ws_connected) return 'Awaiting ticks';
  if (!symbol) return meta.ws_connected ? (stateLabel + ' • WS ready') : 'Waiting for price';
  return stateLabel + ' • ' + source + ' • ' + cfPrettyScalpSymbol(symbol) + ' • ' + age;
}

function cfScalpFeedDetail(feed) {
  const meta = feed || {};
  const bits = [];
  const connectionState = String(meta.connection_state || '').trim();
  bits.push(meta.ws_connected ? (meta.authenticated ? 'WS auth' : 'WS live') : 'WS idle');
  if (connectionState && connectionState !== 'connected') bits.push(connectionState);
  bits.push(String(Number(meta.messages_received) || 0) + ' msgs');
  bits.push(String(Number(meta.reconnect_count) || 0) + ' reconnects');
  bits.push(String(Number(meta.rest_fallbacks) || 0) + ' REST');
  if (Number.isFinite(Number(meta.last_message_age_ms)) && Number(meta.last_message_age_ms) >= 0) {
    bits.push('last msg ' + cfFormatLatency(meta.last_message_age_ms));
  }
  const active = Array.isArray(meta.subscribed_channels) && meta.subscribed_channels.length ? meta.subscribed_channels[0] : '';
  if (active) bits.push(cfTrimUiText(active, 28));
  const disconnect = String(meta.last_disconnect_reason || '').trim();
  if (disconnect) bits.push('drop ' + cfTrimUiText(disconnect, 40));
  const error = String(meta.last_error || '').trim();
  if (error) bits.push(cfTrimUiText(error, 56));
  return bits.join(' • ');
}

function cfScalpExecSummary(execMetrics) {
  const meta = execMetrics || {};
  if (!meta.phase) return 'No broker actions yet';
  const phase = cfScalpPhaseLabel(meta.phase || '');
  const symbol = cfPrettyScalpSymbol(meta.symbol || '—');
  const lifecycle = cfScalpLifecycleLabel(meta.order_lifecycle || meta.fill_status || '');
  const latency = Number(meta.latency_ms) > 0 ? cfFormatLatency(meta.latency_ms) : (meta.verified === false ? 'unverified' : 'pending');
  return [phase, symbol, lifecycle || latency].filter(Boolean).join(' • ');
}

function cfScalpExecDetail(execMetrics) {
  const meta = execMetrics || {};
  if (!meta.phase) return 'Awaiting next broker action';
  const bits = [];
  const phase = cfScalpPhaseLabel(meta.phase || '');
  if (phase) bits.push(phase);
  if (meta.trade_id) bits.push('trade #' + meta.trade_id);
  if (meta.order_id) bits.push('order ' + meta.order_id);
  if (meta.fill_status) bits.push(cfScalpLifecycleLabel(meta.fill_status));
  if (meta.order_lifecycle && meta.order_lifecycle !== meta.fill_status) bits.push(cfScalpLifecycleLabel(meta.order_lifecycle));
  if (meta.exchange_state && meta.exchange_state !== meta.fill_status) bits.push('exchange ' + cfScalpLifecycleLabel(meta.exchange_state));
  if (meta.verification_state && meta.verification_state !== meta.order_lifecycle) bits.push('verify ' + cfScalpLifecycleLabel(meta.verification_state));
  if (Number(meta.ack_ms) > 0) bits.push('ack ' + cfFormatLatency(meta.ack_ms));
  if (Number(meta.latency_ms) > 0) bits.push('verify ' + cfFormatLatency(meta.latency_ms));
  if (Number(meta.verified_at_attempt) > 0) bits.push('attempt ' + String(meta.verified_at_attempt));
  if (Number(meta.requested_size) > 0) bits.push('contracts ' + String(meta.requested_size));
  if (Number(meta.requested_qty_value) > 0) bits.push('qty ' + String(meta.requested_qty_value));
  else if (Number(meta.position_size) > 0) bits.push('size ' + String(meta.position_size));
  if (meta.verification_summary) bits.push(cfTrimUiText(meta.verification_summary, 84));
  if (meta.note) bits.push(cfTrimUiText(meta.note, 72));
  if (meta.error) bits.push(cfTrimUiText(meta.error, 84));
  if (!bits.length) return meta.verified === false ? 'verification failed' : 'awaiting broker metrics';
  return bits.join(' • ');
}

function cfScalpMarkMeta(trade) {
  const source = cfPriceSourceLabel(trade && trade.price_source);
  const age = cfFormatLatency(trade && trade.price_age_ms);
  return source === 'Idle' ? 'mark' : source + ' • ' + age;
}

function cfRefreshScalpEntryLaneFromState() {
  const lane = document.querySelector('.cf-scalp-entry-lane');
  const titleEl = document.getElementById('cf-scalp-entry-title');
  const stateEl = document.getElementById('cf-scalp-entry-state');
  const symbolEl = document.getElementById('cf-scalp-entry-symbol');
  const ageEl = document.getElementById('cf-scalp-entry-age');
  const paperGateEl = document.getElementById('cf-scalp-paper-gate');
  const liveGateEl = document.getElementById('cf-scalp-live-gate');
  const noteEl = document.getElementById('cf-scalp-entry-note');
  const buyBtn = document.getElementById('cf-scalp-buy-btn');
  const sellBtn = document.getElementById('cf-scalp-sell-btn');
  const liveAck = document.getElementById('cf-scalp-live-ack');
  const mode = cfScalpSelectedMode();
  const pendingArmed = cfScalpPendingPriceActive();
  const status = _cfLatestScalpStatus || {};
  const entry = status.entry_controls || {};
  const feed = status.feed_metrics || {};
  const symbol = cfPrettyScalpSymbol((entry.symbol || cfScalpSelectedSymbol() || feed.symbol || '—').toUpperCase());
  const state = String(entry.state || feed.state || 'waiting').toLowerCase();
  const ageMs = entry.age_ms !== undefined && entry.age_ms !== null ? entry.age_ms : feed.age_ms;
  const paperAllowed = !!entry.paper_allowed;
  const liveAllowed = !!entry.live_allowed;
  const requiresAck = mode === 'live' && liveAck && !liveAck.checked;
  let effectiveAllowed = mode === 'live' ? liveAllowed : paperAllowed;
  if (pendingArmed) effectiveAllowed = true;
  if (requiresAck) effectiveAllowed = false;

  let note = '';
  if (pendingArmed) {
    note = 'Pending price entry armed. The first touched stop or limit level will trigger the order.';
  } else if (requiresAck) {
    note = 'Confirm live mode to enable real orders.';
  } else if (state === 'waiting') {
    note = 'Waiting for the first reliable market tick.';
  } else if (state === 'degraded') {
    note = mode === 'live' ? 'Live entry stays blocked until a fresh tick arrives.' : '';
  } else if (state === 'stale') {
    note = 'Wait for a fresher market tick before buying or selling.';
  }

  if (lane) {
    lane.dataset.state = state;
    lane.classList.toggle('has-note', !!note);
  }
  if (titleEl) titleEl.textContent = 'Velocity Entry';
  if (stateEl) {
    stateEl.textContent = cfScalpStateLabel(state).toUpperCase();
    stateEl.dataset.state = state;
  }
  if (symbolEl) symbolEl.textContent = symbol;
  if (ageEl) ageEl.textContent = ageMs !== undefined && ageMs !== null ? cfFormatLatency(ageMs) : '—';
  if (paperGateEl) {
    paperGateEl.textContent = cfScalpGateLabel(paperAllowed, state);
    paperGateEl.dataset.gate = cfScalpGateTone(paperAllowed, state);
  }
  if (liveGateEl) {
    liveGateEl.textContent = cfScalpGateLabel(liveAllowed, state);
    liveGateEl.dataset.gate = cfScalpGateTone(liveAllowed, state);
  }
  if (noteEl) {
    noteEl.textContent = note || ' ';
    noteEl.classList.toggle('is-empty', !note);
  }

  const disableReason = pendingArmed ? '' : (note || entry.reason || feed.entry_block_reason || 'Entry unavailable');
  cfSetScalpEntryButtonState(buyBtn, effectiveAllowed, disableReason);
  cfSetScalpEntryButtonState(sellBtn, effectiveAllowed, disableReason);
  cfScalpSyncEntrySubmitUi();
}

function cfApplyScalpStatus(d) {
  try {
    const status = d || {};
    _cfLatestScalpStatus = status;
    const payloadOpen = Array.isArray(status.open_trades) ? status.open_trades : [];
    const pending = Array.isArray(status.pending_entries) ? status.pending_entries : [];
    const exec = status.execution_metrics || {};
    const execPhase = String(exec.phase || '').toLowerCase();
    const hasFreshOpenPayload = payloadOpen.length > 0;
    if (hasFreshOpenPayload) {
      _cfOpenTradeDisplayCache = payloadOpen.slice();
      _cfOpenTradeDisplayCacheUntil = Date.now() + 30000;
    }
    const open = hasFreshOpenPayload
      ? payloadOpen
      : (_cfOpenTradeDisplayCache.length && Date.now() < _cfOpenTradeDisplayCacheUntil && !execPhase.includes('exit')
          ? _cfOpenTradeDisplayCache.slice()
          : []);
    if (!open.length && !pending.length && (!execPhase || execPhase.includes('exit'))) {
      _cfOpenTradeDisplayCache = [];
      _cfOpenTradeDisplayCacheUntil = 0;
    }
    const running = !!status.running || open.length > 0 || pending.length > 0;

    const dot = cfEl('cf-scalp-status-dot');
    const label = cfEl('cf-scalp-status-label');
    if (dot) dot.dataset.running = running ? 'true' : 'false';
    if (label) label.textContent = running ? (status.in_trade ? 'In Trade' : (pending.length ? 'Pending Entry' : 'Monitoring')) : 'Engine Idle';
    const openCount = cfEl('cf-scalp-open-count');
    if (openCount) {
      const openText = open.length + ' open trade' + (open.length !== 1 ? 's' : '');
      openCount.textContent = pending.length ? (openText + ' • ' + pending.length + ' pending') : openText;
    }
    const engDot = cfEl('cf-scalp-dot');
    if (engDot) engDot.classList.toggle('is-live', running);

    const feed = status.feed_metrics || {};
    const feedMeta = cfEl('cf-scalp-feed-meta');
    if (feedMeta) {
      const feedState = String(feed.state || '').toLowerCase() || 'waiting';
      feedMeta.textContent = cfScalpFeedSummary(feed);
      feedMeta.dataset.state = feedState;
    }
    const feedDetail = cfEl('cf-scalp-feed-detail');
    if (feedDetail) {
      const feedState = feed.last_error ? 'error' : (String(feed.state || '').toLowerCase() || 'waiting');
      feedDetail.textContent = cfScalpFeedDetail(feed);
      feedDetail.dataset.state = feedState;
    }

    const execMeta = cfEl('cf-scalp-exec-meta');
    if (execMeta) {
      const execState = cfScalpExecTone(exec);
      execMeta.textContent = cfScalpExecSummary(exec);
      execMeta.dataset.state = execState;
    }
    const execDetail = cfEl('cf-scalp-exec-detail');
    if (execDetail) {
      execDetail.innerHTML = cfScalpExecDetailHtml(exec);
      execDetail.dataset.state = cfScalpExecTone(exec);
    }

    cfRefreshScalpEntryLaneFromState();

    const pendingWrap = cfEl('cf-scalp-pending-wrap');
    const pendingCount = cfEl('cf-scalp-pending-count');
    const pendingList = cfEl('cf-scalp-pending-list');
    if (pendingWrap && pendingList) {
      pendingWrap.hidden = !pending.length;
      if (!pending.length) {
        pendingList.innerHTML = '';
      } else {
        if (pendingCount) pendingCount.textContent = pending.length + ' armed';
        pendingList.innerHTML = pending.map(function(p) {
          const side = String(p.side || '').toLowerCase();
          const armedAt = p.created_at ? fmtDt(p.created_at) : '—';
          const qtyMain = p.qty_mode === 'base' && p.base_qty
            ? Number(p.base_qty).toFixed(6) + ' qty'
            : '$' + Number(p.qty_usdt || 0).toFixed(2) + ' margin';
          return `
            <div class="cf-scalp-pending-card">
              <div class="cf-scalp-pending-card-head">
                <div class="cf-scalp-pending-side" data-side="${_escapeHtml(side)}">${_escapeHtml(p.side === 'LONG' ? 'BUY Pending' : 'SELL Pending')}</div>
                <div class="cf-scalp-pending-mode">${_escapeHtml(String(p.mode || 'paper').toUpperCase())}</div>
              </div>
              <div class="cf-scalp-pending-symbol">${_escapeHtml(cfPrettyScalpSymbol(p.symbol || '—'))}</div>
              <div class="cf-scalp-pending-trigger">${_escapeHtml(p.trigger_summary || 'Pending trigger')}</div>
              <div class="cf-scalp-pending-meta">
                <div>Qty: <span>${_escapeHtml(qtyMain)}</span></div>
                <div>Lev: <span>${_escapeHtml(String(p.leverage || '—'))}x</span></div>
                <div class="cf-scalp-pending-meta-wide">Armed: <span>${_escapeHtml(armedAt)}</span></div>
              </div>
            </div>`;
        }).join('');
      }
    }

    const sessionPnlEl = cfEl('cf-scalp-session-pnl');
    if (sessionPnlEl) {
      let pnl;
      if (status.session_pnl !== undefined) {
        pnl = status.session_pnl;
      } else {
        const todayUtc = new Date().toISOString().slice(0, 10);
        const closedTrades = Array.isArray(status.closed_trades) ? status.closed_trades : [];
        const todayRealized = closedTrades
          .filter(function(t) { return t.exit_time && String(t.exit_time).startsWith(todayUtc); })
          .reduce(function(sum, t) { return sum + (t.pnl || 0); }, 0);
        const openUnrealized = open.reduce(function(sum, t) { return sum + (t.unrealized_pnl || 0); }, 0);
        pnl = todayRealized + openUnrealized;
      }
      const sign = pnl >= 0 ? '+' : '';
      sessionPnlEl.textContent = sign + fmtINR(pnl);
      sessionPnlEl.dataset.state = pnl > 0 ? 'success' : pnl < 0 ? 'error' : 'neutral';
    }

    cfRenderActivePositions(open, exec);
    cfApplyScalpActivity(status);
  } catch (e) {
    console.error('Scalp status render failed', e);
  }
}

function _cfOpenTradeSignature(open) {
  return (open || []).map(function(t) {
    return [
      t.trade_id || t.id || '',
      t.size || 0,
      t.qty_mode || '',
      t.qty_usdt || 0,
      t.base_qty || 0,
      t.entry_price || 0,
      t.target_price || 0,
      t.sl_price || 0,
      t.target_usd || 0,
      t.sl_usd || 0,
      t.mode || ''
    ].join(':');
  }).join('|');
}

function cfScalpTradeExecMetrics(trade, execMetrics) {
  const tradeId = String((trade && (trade.trade_id || trade.id)) || '');
  if (trade && trade.execution_metrics && typeof trade.execution_metrics === 'object' && Object.keys(trade.execution_metrics).length) {
    return trade.execution_metrics;
  }
  const meta = execMetrics || {};
  if (!meta.phase) return null;
  if (tradeId && String(meta.trade_id || '') === tradeId) return meta;
  return null;
}

function cfRenderScalpTradeExecHtml(trade, execMetrics) {
  const meta = cfScalpTradeExecMetrics(trade, execMetrics);
  if (!meta) {
    const source = cfPriceSourceLabel(trade && trade.price_source);
    const tone = source === 'WS' ? 'success' : (source === 'REST' ? 'active' : 'pending');
    return '<div class="cf-scalp-exec-stage-row">'
      + '<span class="cf-scalp-exec-chip" data-state="done">' + _escapeHtml(String((trade && trade.mode) || 'paper').toUpperCase()) + '</span>'
      + '<span class="cf-scalp-exec-chip" data-state="' + _escapeHtml(tone) + '">Monitoring</span>'
      + '</div>'
      + '<div class="cf-scalp-exec-note">' + _escapeHtml(cfScalpMarkMeta(trade)) + '</div>';
  }
  return cfScalpExecDetailHtml(meta);
}

function cfSyncScalpTradeExecUi(trade, execMetrics) {
  const key = String((trade && (trade.trade_id || trade.id)) || '');
  if (!key) return;
  const el = cfEl('cf-trade-exec-' + key);
  if (!el) return;
  const meta = cfScalpTradeExecMetrics(trade, execMetrics);
  el.innerHTML = cfRenderScalpTradeExecHtml(trade, execMetrics);
  el.dataset.state = meta ? cfScalpExecTone(meta) : 'neutral';
}

function cfRenderActivePositions(open, execMetrics) {
  const body = document.getElementById('cf-scalp-active-body');
  const countEl = document.getElementById('cf-scalp-active-count');
  if (!body) return;
  if (countEl) countEl.textContent = open.length + ' open';

  if (!open.length) {
    _cfOpenTradeSnapshot = '';
    _cfScalpActionLocks.clear();
    _cfScalpActionCooldowns.clear();
    if (_cfScalpActionCooldownTimer) {
      window.clearTimeout(_cfScalpActionCooldownTimer);
      _cfScalpActionCooldownTimer = 0;
    }
    body.innerHTML = '<tr><td colspan="10" class="cf-table-empty-cell">No active positions</td></tr>';
    _renderTablePager('cf-scalp-active-table', 'cf-scalp-active-table', 'cf-scalp-active-pagination');
    return;
  }

  const openSignature = _cfOpenTradeSignature(open);
  const needFullRender = openSignature !== _cfOpenTradeSnapshot;

  if (needFullRender) {
    _cfOpenTradeSnapshot = openSignature;
    body.innerHTML = open.map(function(t) {
      const tid = t.trade_id || t.id;
      const pnl = t.unrealized_pnl || 0;
      const isProfit = pnl > 0;
      const isLoss = pnl < 0;
      const prettySymbol = cfPrettyScalpSymbol(t.symbol || '—');
      const sideTag = t.side === 'BUY' || t.side === 'LONG'
        ? `<span class="tag tag-green">${t.side}</span>`
        : `<span class="tag tag-red">${t.side || '—'}</span>`;
      const pnlState = isProfit ? 'profit' : (isLoss ? 'loss' : 'flat');
      const qtyMain = t.qty_mode === 'base' && t.base_qty
        ? Number(t.base_qty).toFixed(6)
        : '$' + Number(t.qty_usdt || t.quantity || 0).toFixed(2);
      const qtySub = t.qty_mode === 'base'
        ? ('$' + Number(t.qty_usdt || 0).toFixed(2) + ' margin')
        : ((t.base_qty ? Number(t.base_qty).toFixed(6) + ' qty' : 'margin'));
      const addConfig = cfScalpTradeAddConfig(t);
      return `<tr data-tid="${tid}" data-pnl-state="${pnlState}">
        <td><div class="table-row-label">${prettySymbol}</div><div class="table-note">trade #${tid || '—'} • ${(t.leverage || 1)}x • ${(t.mode || 'paper').toUpperCase()}</div><div class="cf-scalp-trade-exec" id="cf-trade-exec-${tid}" data-state="neutral"></div><div class="cf-scalp-trade-sync" id="cf-trade-sync-${tid}" data-state="idle"></div></td>
        <td>${sideTag}</td>
        <td><div class="table-value-stack"><div class="table-value-main">${qtyMain}</div><div class="table-value-sub">${qtySub}</div></div></td>
        <td><div class="table-value-stack"><div class="table-value-main">$${(t.entry_price || 0).toFixed(4)}</div><div class="table-value-sub">entry</div></div></td>
        <td data-field="mark"><div class="table-value-stack"><div class="table-value-main">$${(t.mark_price || t.current_price || 0).toFixed(4)}</div><div class="table-value-sub">${cfScalpMarkMeta(t)}</div></div></td>
        <td data-field="pnl"><div class="table-value-stack"><div class="table-value-main ${isProfit ? 'positive' : isLoss ? 'negative' : ''}">${pnl >= 0 ? '+' : ''}${fmtINR(pnl)}</div><div class="table-value-sub ${isProfit ? 'positive' : isLoss ? 'negative' : ''}">unrealized</div></div></td>
        <td><div class="table-edit-stack table-edit-stack-pairs"><label class="table-field-pair"><span class="table-field-label">TP $</span><input type="number" class="table-input-sm" id="cf-tp-usd-${tid}" value="${t.target_usd || 0}" step="1" min="0" placeholder="TP $"></label><label class="table-field-pair"><span class="table-field-label">TP Px</span><input type="number" class="table-input-sm" id="cf-tp-price-${tid}" value="${t.target_price || 0}" step="0.1" min="0" placeholder="TP price"></label></div></td>
        <td><div class="table-edit-stack table-edit-stack-pairs"><label class="table-field-pair"><span class="table-field-label">SL $</span><input type="number" class="table-input-sm" id="cf-sl-usd-${tid}" value="${t.sl_usd || 0}" step="1" min="0" placeholder="SL $"></label><label class="table-field-pair"><span class="table-field-label">SL Px</span><input type="number" class="table-input-sm" id="cf-sl-price-${tid}" value="${t.sl_price || 0}" step="0.1" min="0" placeholder="SL price"></label></div></td>
        <td><div class="table-inline-actions table-inline-actions-stack table-add-stack" data-qty-mode="${addConfig.mode}"><label class="table-field-pair table-field-pair-compact"><span class="table-field-label">${addConfig.label}</span><input type="number" class="table-input-sm table-add-input" id="cf-add-qty-${tid}" value="${addConfig.value}" step="${addConfig.step}" min="${addConfig.min}" data-default-qty="${addConfig.value}" data-qty-mode="${addConfig.mode}" placeholder="${addConfig.value}"></label><div class="table-field-meta">${addConfig.helper}</div><button class="btn btn-outline btn-sm table-add-btn" id="cf-add-btn-${tid}" data-cf-click="cfAddScalpQuantity('${tid}')">Scale In</button></div></td>
        <td><div class="table-inline-actions table-action-stack"><button class="btn btn-success btn-sm" id="cf-set-btn-${tid}" data-cf-click="cfModifyScalpTrade('${tid}')">Save</button><button class="btn btn-danger btn-sm" id="cf-exit-btn-${tid}" data-cf-click="cfExitScalpTrade('${tid}')">Exit</button></div></td>
      </tr>`;
    }).join('');
  } else {
    open.forEach(function(t) {
      const tid = t.trade_id || t.id;
      const row = body.querySelector(`tr[data-tid="${tid}"]`);
      if (!row) return;
      const pnl = t.unrealized_pnl || 0;
      const isProfit = pnl > 0;
      const isLoss = pnl < 0;
      const markCell = row.querySelector('[data-field="mark"]');
      const pnlCell = row.querySelector('[data-field="pnl"]');
      if (markCell) {
        markCell.innerHTML = '<div class="table-value-stack"><div class="table-value-main">$' + (t.mark_price || t.current_price || 0).toFixed(4) + '</div><div class="table-value-sub">' + cfScalpMarkMeta(t) + '</div></div>';
      }
      if (pnlCell) {
        const pnlClass = isProfit ? 'positive' : isLoss ? 'negative' : '';
        pnlCell.innerHTML = '<div class="table-value-stack"><div class="table-value-main ' + pnlClass + '">' + (pnl >= 0 ? '+' : '') + fmtINR(pnl) + '</div><div class="table-value-sub ' + pnlClass + '">unrealized</div></div>';
      }
      row.dataset.pnlState = isProfit ? 'profit' : (isLoss ? 'loss' : 'flat');
    });
  }
  open.forEach(function(t) {
    cfSyncScalpTradeExecUi(t, execMetrics);
  });
  cfSyncAllScalpTradeActionUi(open);
  _renderTablePager('cf-scalp-active-table', 'cf-scalp-active-table', 'cf-scalp-active-pagination');
}

async function cfSubmitScalp(direction) {
  const statusEl = cfEl('cf-scalp-entry-status');
  let submitLocked = false;
  try {
    const symbolEl = cfRequireElement('cf-scalp-symbol', 'Symbol');
    const qtyEl = cfRequireElement('cf-scalp-qty', 'Size');
    const leverageEl = cfRequireElement('cf-scalp-leverage', 'Leverage');
    const slTypeEl = cfRequireElement('cf-sl-type', 'Risk cap type');
    const tpTypeEl = cfRequireElement('cf-tp-type', 'Profit lock type');
    const modeEl = cfRequireElement('cf-scalp-mode', 'Execution mode');
    const symbol = symbolEl.value;
    const qtyMode = cfScalpSelectedQtyMode();
    const qty = cfFieldNumber('cf-scalp-qty', qtyMode === 'base' ? 0.0015 : 10000);
    const leverage = parseInt(leverageEl.value, 10) || 10;
    const slVal = cfFieldNumber('cf-scalp-sl', 0);
    const tpVal = cfFieldNumber('cf-scalp-tp', 0);
    const slPrice = cfFieldNumber('cf-scalp-sl-price', 0);
    const tpPrice = cfFieldNumber('cf-scalp-tp-price', 0);
    const entryStop = cfFieldNumber('cf-scalp-entry-stop', 0);
    const entryLimit = cfFieldNumber('cf-scalp-entry-limit', 0);
    const slType = slTypeEl.value;
    const tpType = tpTypeEl.value;
    const mode = modeEl.value;
    const liveAck = cfEl('cf-scalp-live-ack');

    if (mode === 'live' && liveAck && !liveAck.checked) {
      if (statusEl) { statusEl.textContent = 'Confirm live acknowledgement first'; statusEl.style.color = 'var(--red)'; }
      cfToast('Acknowledge live scalp mode before placing real orders', 'warning');
      cfRefreshScalpEntryLaneFromState();
      return;
    }
    if (!qtyEl.value || qty <= 0) {
      if (statusEl) { statusEl.textContent = 'Enter a valid quantity'; statusEl.style.color = 'var(--red)'; }
      cfToast('Enter a valid scalp quantity before placing the trade', 'warning');
      return;
    }

    if (!cfSetScalpEntrySubmitBusy(direction)) {
      cfToast('A scalp entry is already being submitted', 'info');
      return;
    }
    submitLocked = true;
    if (statusEl) statusEl.textContent = 'Submitting…';
    const payload = { symbol, side: direction, qty_mode: qtyMode, qty_value: qty, leverage, mode };
    if (slType === 'pct' && slVal > 0) payload.stop_loss_pct = slVal;
    else if (slVal > 0) payload.sl_usd = slVal;
    if (tpType === 'pct' && tpVal > 0) payload.take_profit_pct = tpVal;
    else if (tpVal > 0) payload.tp_usd = tpVal;
    if (slPrice > 0) payload.sl_price = slPrice;
    if (tpPrice > 0) payload.target_price = tpPrice;
    if (entryStop > 0) payload.entry_stop_price = entryStop;
    if (entryLimit > 0) payload.entry_limit_price = entryLimit;
    const res = await cfApiFetch('/api/scalp/enter', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    const d = await cfReadApiPayload(res);
    cfMergeScalpStatusPatch(d);
    cfRefreshScalpEntryLaneFromState();
    if (d.status === 'ok' || d.status === 'entered' || d.trade_id) {
      if (statusEl) { statusEl.textContent = direction + ' entered ✓'; statusEl.style.color = 'var(--green)'; }
      cfToast(`Scalp ${direction} entered on ${cfPrettyScalpSymbol(symbol)}`, 'success');
      await cfRefreshScalpWorkspace();
      setTimeout(cfLoadScalpStatus, 250);
    } else if (d.status === 'pending' || d.entry_id) {
      if (statusEl) { statusEl.textContent = d.message || 'Pending entry armed'; statusEl.style.color = '#fbbf24'; }
      cfToast(d.message || `Pending entry armed for ${cfPrettyScalpSymbol(symbol)}`, 'info');
      await cfRefreshScalpWorkspace();
      setTimeout(cfLoadScalpStatus, 250);
    } else {
      if (statusEl) { statusEl.textContent = cfApiErrorDetail(d, d.status || 'Error'); statusEl.style.color = 'var(--red)'; }
      cfToast(cfApiErrorDetail(d, 'Entry failed'), 'danger');
    }
  } catch (e) {
    if (e && e.code === 'cf_ui_mismatch') {
      if (statusEl) { statusEl.textContent = 'Reload required'; statusEl.style.color = '#fbbf24'; }
      cfToast(e.message || 'Scalp form is out of date. Reload the page.', 'warning');
      return;
    }
    if (statusEl) { statusEl.textContent = 'Network error'; statusEl.style.color = 'var(--red)'; }
    cfToast((e && e.message) ? e.message : 'Network error submitting scalp', 'danger');
  } finally {
    if (submitLocked) cfClearScalpEntrySubmitBusy();
  }
}

async function cfExitScalpTrade(tradeId) {
  let locked = false;
  try {
    if (!cfSetScalpTradeActionLock(tradeId, 'exit')) {
      cfToast(cfScalpTradeActionBlockMessage(tradeId), 'info');
      return;
    }
    locked = true;
    const res = await cfApiFetch('/api/scalp/exit', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ trade_id: tradeId })
    });
    const d = await cfReadApiPayload(res);
    cfMergeScalpStatusPatch(d);
    if (d.status === 'ok' || d.status === 'exited') {
      cfToast('Position closed', 'success');
      await cfRefreshScalpWorkspace();
      setTimeout(cfLoadScalpStatus, 200);
    } else {
      cfToast(cfApiErrorDetail(d, 'Exit failed'), 'danger');
    }
  } catch(e) { cfToast((e && e.message) ? e.message : 'Network error exiting trade', 'danger'); }
  finally {
    if (locked) {
      cfClearScalpTradeActionLock(tradeId);
      cfStartScalpTradeActionCooldown(tradeId, 'exit');
    }
  }
}

async function cfModifyScalpTrade(tradeId) {
  const btn = cfEl('cf-set-btn-' + tradeId);
  const tpUsdInput = cfEl('cf-tp-usd-' + tradeId);
  const slUsdInput = cfEl('cf-sl-usd-' + tradeId);
  const tpPriceInput = cfEl('cf-tp-price-' + tradeId);
  const slPriceInput = cfEl('cf-sl-price-' + tradeId);
  if (!tpUsdInput || !slUsdInput || !tpPriceInput || !slPriceInput) {
    cfToast('Trade controls are out of date. Reload the page.', 'warning');
    return;
  }

  const payload = {};
  const newTP = parseFloat(tpUsdInput.value) || 0;
  const newSL = parseFloat(slUsdInput.value) || 0;
  const newTPPrice = parseFloat(tpPriceInput.value) || 0;
  const newSLPrice = parseFloat(slPriceInput.value) || 0;
  if (newTP >= 0) payload.target_usd = newTP;
  if (newSL >= 0) payload.sl_usd = newSL;
  if (newTPPrice >= 0) payload.target_price = newTPPrice;
  if (newSLPrice >= 0) payload.sl_price = newSLPrice;
  if (!Object.keys(payload).length) {
    cfToast('Set at least one TP or SL value', 'danger');
    return;
  }

  let locked = false;
  try {
    if (!cfSetScalpTradeActionLock(tradeId, 'targets')) {
      cfToast(cfScalpTradeActionBlockMessage(tradeId), 'info');
      return;
    }
    locked = true;
    const res = await cfApiFetch('/api/scalp/trades/' + tradeId + '/targets', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const d = await cfReadApiPayload(res);
    cfMergeScalpStatusPatch(d);
    if (d.status === 'ok') {
      cfToast(`Trade #${tradeId} updated`, 'success');
      await cfRefreshScalpWorkspace();
      setTimeout(cfLoadScalpStatus, 200);
    } else {
      cfToast(cfApiErrorDetail(d, 'Update failed'), 'danger');
    }
  } catch (e) {
    cfToast('Error: ' + e.message, 'danger');
  } finally {
    if (locked) {
      cfClearScalpTradeActionLock(tradeId);
      cfStartScalpTradeActionCooldown(tradeId, 'targets');
    }
    else if (btn) cfSyncScalpTradeActionUi(tradeId);
  }
}

async function cfAddScalpQuantity(tradeId) {
  const qtyInput = cfEl('cf-add-qty-' + tradeId);
  if (!qtyInput) {
    cfToast('Trade controls are out of date. Reload the page.', 'warning');
    return;
  }
  const qtyMode = String(qtyInput.getAttribute('data-qty-mode') || 'base').toLowerCase() === 'base' ? 'base' : 'usdt';
  const rawValue = String(qtyInput.value || '').trim();
  const fallbackValue = String(qtyInput.getAttribute('data-default-qty') || qtyInput.placeholder || '').trim();
  const qtyValue = parseFloat(rawValue || fallbackValue);
  if (!Number.isFinite(qtyValue) || qtyValue <= 0) {
    cfToast('Add quantity must be greater than zero', 'danger');
    return;
  }
  let locked = false;
  try {
    if (!cfSetScalpTradeActionLock(tradeId, 'add')) {
      cfToast(cfScalpTradeActionBlockMessage(tradeId), 'info');
      return;
    }
    locked = true;
    const res = await cfApiFetch('/api/scalp/trades/' + tradeId + '/add', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ qty_mode: qtyMode, qty_value: qtyValue }),
    });
    const d = await cfReadApiPayload(res);
    cfMergeScalpStatusPatch(d);
    if (d.status === 'ok') {
      qtyInput.value = qtyInput.getAttribute('data-default-qty') || fallbackValue || '';
      const qtyText = qtyMode === 'base'
        ? (cfFormatQtyValue(qtyValue, 'base') + ' qty')
        : ('$' + cfFormatQtyValue(qtyValue, 'usdt') + ' margin');
      cfToast(`Scaled trade #${tradeId} by ${qtyText}`, 'success');
      await cfRefreshScalpWorkspace();
      setTimeout(cfLoadScalpStatus, 200);
    } else {
      cfToast(cfApiErrorDetail(d, 'Add failed'), 'danger');
    }
  } catch (e) {
    cfToast('Error: ' + e.message, 'danger');
  } finally {
    if (locked) {
      cfClearScalpTradeActionLock(tradeId);
      cfStartScalpTradeActionCooldown(tradeId, 'add');
    }
  }
}

async function cfUpdateScalpSymbol() {
  const symbolEl = document.getElementById('cf-scalp-symbol');
  const levEl = document.getElementById('cf-scalp-leverage');
  if (!symbolEl || !levEl) return;
  const preferred = String(levEl.value || _CF_SCALP_DEFAULTS.leverage || '10');
  try {
    const r = await fetch('/api/leverage/' + symbolEl.value, { credentials: 'same-origin' });
    const d = await r.json();
    if (d.status === 'ok' && Array.isArray(d.options) && d.options.length) {
      const options = d.options.map(function(lev) { return String(lev); });
      const brokerDefault = String(d.default || d.options[0]);
      const selected = options.includes(preferred)
        ? preferred
        : (options.includes(String(_CF_SCALP_DEFAULTS.leverage)) ? String(_CF_SCALP_DEFAULTS.leverage) : brokerDefault);
      levEl.innerHTML = d.options.map(function(lev) {
        const value = String(lev);
        return '<option value="' + value + '"' + (value === selected ? ' selected' : '') + '>' + value + '×</option>';
      }).join('');
      levEl.value = selected;
    }
  } catch(e) {}
  cfRefreshScalpEntryLaneFromState();
  if (document.getElementById('scalp-page') && document.getElementById('scalp-page').classList.contains('active-page')) {
    cfLoadScalpStatus();
  }
}

function cfToggleScalpLiveSafety() {
  const modeEl = document.getElementById('cf-scalp-mode');
  const wrap = document.getElementById('cf-scalp-live-safety');
  const ack = document.getElementById('cf-scalp-live-ack');
  const isLive = !!modeEl && modeEl.value === 'live';
  if (wrap) wrap.hidden = !isLive;
  if (!isLive && ack) ack.checked = false;
  cfRefreshScalpEntryLaneFromState();
  cfSyncScalpLogPanelHeight();
}

function cfUpdateSLTPHints() {
  const slType = (document.getElementById('cf-sl-type') || {}).value || 'usdt';
  const tpType = (document.getElementById('cf-tp-type') || {}).value || 'usdt';
  const slHint = document.getElementById('cf-sl-hint');
  const tpHint = document.getElementById('cf-tp-hint');
  if (slHint) slHint.textContent = slType === 'pct' ? 'Max loss as % of position before auto-exit' : 'Max loss in USDT before auto-exit';
  if (tpHint) tpHint.textContent = tpType === 'pct' ? 'Profit target as % of position before auto-exit' : 'Profit target in USDT before auto-exit';
}


// Close run-detail modal on ESC key
document.addEventListener('keydown', function(e) {
  if (e.key === 'Escape') {
    const appearanceModal = document.getElementById('appearance-modal');
    if (appearanceModal && !appearanceModal.hidden) cfCloseAppearancePanel();
    const m = document.getElementById('run-detail-modal');
    if (m && m.style.display === 'flex') closeRunModal();
  }
});
