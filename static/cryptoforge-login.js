try {
  if (typeof window.cfApplyTheme === 'function') {
    window.cfApplyTheme(typeof window.cfGetStoredTheme === 'function' ? window.cfGetStoredTheme() : '', { persist: false });
  }
} catch(e) {}


function syncLoginAppearancePanel() {
  const state = typeof window.cfGetAppearance === 'function' ? window.cfGetAppearance() : { tint: 'gold', font: 'institutional' };
  document.querySelectorAll('[data-login-tint]').forEach((btn) => {
    const active = btn.getAttribute('data-login-tint') === state.tint;
    btn.classList.toggle('active', active);
    btn.setAttribute('aria-pressed', active ? 'true' : 'false');
  });
  document.querySelectorAll('[data-login-font]').forEach((btn) => {
    const active = btn.getAttribute('data-login-font') === state.font;
    btn.classList.toggle('active', active);
    btn.setAttribute('aria-pressed', active ? 'true' : 'false');
  });
}

function initLoginAppearance() {
  const toggle = document.getElementById('login-appearance-toggle');
  const panel = document.getElementById('login-appearance-panel');
  if (!toggle || !panel) return;
  syncLoginAppearancePanel();
  toggle.addEventListener('click', () => {
    panel.hidden = !panel.hidden;
    panel.classList.toggle('open', !panel.hidden);
    syncLoginAppearancePanel();
  });
  panel.addEventListener('click', (event) => {
    const tintBtn = event.target.closest('[data-login-tint]');
    const fontBtn = event.target.closest('[data-login-font]');
    if (tintBtn && typeof window.cfApplyAppearance === 'function') {
      window.cfApplyAppearance({ tint: tintBtn.getAttribute('data-login-tint') }, { persist: true });
      syncLoginAppearancePanel();
    }
    if (fontBtn && typeof window.cfApplyAppearance === 'function') {
      window.cfApplyAppearance({ font: fontBtn.getAttribute('data-login-font') }, { persist: true });
      syncLoginAppearancePanel();
    }
  });
  document.addEventListener('click', (event) => {
    if (panel.hidden) return;
    if (panel.contains(event.target) || toggle.contains(event.target)) return;
    panel.hidden = true;
    panel.classList.remove('open');
  });
}

const PIN_LENGTH = 6;
let pin = '';
let locked = false;

// Second factor, when the server has one configured. Both values are 6 digits,
// so the same keypad collects them in two passes rather than growing a second
// input the keypad cannot reach.
let totpRequired = false;
let stage = 'pin';          // 'pin' -> 'totp'
let savedPin = '';

const dots = document.querySelectorAll('.pin-dot');
const status = document.getElementById('unlock-status');
const card = document.getElementById('unlock-card');
const subtitle = document.querySelector('.unlock-sub');

const PROMPTS = {
  pin: 'Enter your 6-digit PIN',
  totp: 'Enter the 6-digit code from your authenticator'
};

function setStage(next) {
  stage = next;
  pin = '';
  updateDots();
  status.textContent = PROMPTS[stage];
  status.className = 'unlock-status';
  if (subtitle) subtitle.textContent = stage === 'totp' ? 'Two-factor code' : 'Enter PIN to unlock';
}

async function detectSecondFactor() {
  try {
    const res = await fetch('/api/auth/status', { credentials: 'same-origin', cache: 'no-store' });
    if (!res.ok) return;
    const data = await res.json();
    totpRequired = Boolean(data && data.totp_required);
  } catch (e) { /* offline: fall back to PIN only, the server still enforces both */ }
}

function updateDots() {
  dots.forEach((dot, i) => {
    dot.classList.remove('filled', 'error', 'success');
    if (i < pin.length) dot.classList.add('filled');
  });
}

function setError(msg, holdMs) {
  status.textContent = msg;
  status.className = 'unlock-status error';
  dots.forEach(d => { d.classList.remove('filled'); d.classList.add('error'); });
  card.classList.add('shake');
  setTimeout(() => {
    card.classList.remove('shake');
    // A wrong code sends you back to the PIN, not just to the code — a half
    // completed login should not leave a verified PIN sitting in the page.
    savedPin = '';
    setStage('pin');
    locked = false;
  }, holdMs || 800);
}

function setSuccess() {
  status.textContent = 'Unlocked! Redirecting...';
  status.className = 'unlock-status success';
  dots.forEach(d => { d.classList.remove('filled'); d.classList.add('success'); });
  card.classList.add('unlock-pulse');
}

async function tryUnlock() {
  if (locked) return;
  locked = true;
  status.textContent = 'Verifying...';
  status.className = 'unlock-status';
  try {
    const res = await fetch('/api/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ password: savedPin || pin, totp: savedPin ? pin : '' }),
      credentials: 'same-origin'
    });
    if (res.ok) {
      setSuccess();
      setTimeout(() => { window.location.href = '/'; }, 400);
      return;
    }
    // The lockout now escalates, so "try again in 6 hours" is real information.
    // Showing the stock wrong-PIN line instead would have you retyping into a
    // door that is not going to open for a while.
    if (res.status === 429) {
      let detail = 'Too many attempts. Try again later.';
      try {
        const data = await res.json();
        if (data && data.detail) detail = data.detail;
      } catch (e) { /* keep the default */ }
      setError(detail, 4000);
      return;
    }
    setError(totpRequired ? 'Wrong PIN or code. Try again.' : 'Wrong PIN. Try again.');
  } catch (e) {
    setError('Connection error.');
  }
}

function addDigit(d) {
  if (locked || pin.length >= PIN_LENGTH) return;
  pin += d;
  updateDots();
  if (pin.length !== PIN_LENGTH) return;
  if (stage === 'pin' && totpRequired) {
    // Hold the PIN and collect the code before contacting the server, so a
    // wrong code costs one attempt rather than two.
    savedPin = pin;
    setTimeout(() => setStage('totp'), 150);
    return;
  }
  setTimeout(tryUnlock, 150);
}

function removeDigit() {
  if (locked || pin.length === 0) return;
  pin = pin.slice(0, -1);
  updateDots();
}

function clearAll() {
  if (locked) return;
  savedPin = '';
  setStage('pin');
}

document.getElementById('keypad').addEventListener('click', (e) => {
  const btn = e.target.closest('.key');
  if (!btn) return;
  const val = btn.dataset.val;
  if (val === 'clear') clearAll();
  else if (val === 'back') removeDigit();
  else addDigit(val);
});

document.addEventListener('keydown', (e) => {
  if (e.key >= '0' && e.key <= '9') addDigit(e.key);
  else if (e.key === 'Backspace') removeDigit();
  else if (e.key === 'Escape') clearAll();
});

initLoginAppearance();
detectSecondFactor();
