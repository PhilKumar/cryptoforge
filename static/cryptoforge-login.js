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
  const theme = document.documentElement.getAttribute('data-theme') || 'dark';
  document.querySelectorAll('[data-login-theme]').forEach((btn) => {
    const active = btn.getAttribute('data-login-theme') === theme;
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
    const themeBtn = event.target.closest('[data-login-theme]');
    if (tintBtn && typeof window.cfApplyAppearance === 'function') {
      window.cfApplyAppearance({ tint: tintBtn.getAttribute('data-login-tint') }, { persist: true });
      syncLoginAppearancePanel();
    }
    if (fontBtn && typeof window.cfApplyAppearance === 'function') {
      window.cfApplyAppearance({ font: fontBtn.getAttribute('data-login-font') }, { persist: true });
      syncLoginAppearancePanel();
    }
    if (themeBtn && typeof window.cfApplyTheme === 'function') {
      window.cfApplyTheme(themeBtn.getAttribute('data-login-theme'), { persist: true });
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

// ══════════════════════════════════════════════════════════════
//  ACCOUNT SIGN-IN — username + password, then the account's
//  authenticator code when it has one (the server answers 428 and the
//  code field appears), or a passkey. Same flow as PhilForge's door.
// ══════════════════════════════════════════════════════════════
let locked = false;
let mfaPending = false;

const status = document.getElementById('unlock-status');
const card = document.getElementById('unlock-card');
const usernameInput = document.getElementById('username-input');
const passwordInput = document.getElementById('password-input');
const passwordToggle = document.getElementById('password-toggle');
const totpField = document.getElementById('totp-field');
const totpInput = document.getElementById('totp-input');
const unlockBtn = document.getElementById('unlock-btn');
const subtitle = document.getElementById('unlock-sub');

function baseStatusMessage() {
  return mfaPending ? 'Enter the code from your authenticator app' : 'Enter username & password';
}

function setIdleStatus() {
  status.textContent = baseStatusMessage();
  status.className = 'unlock-status';
  unlockBtn.disabled = false;
}

function resetToPassword(focus) {
  mfaPending = false;
  totpField.classList.add('hidden');
  totpInput.value = '';
  passwordInput.value = '';
  unlockBtn.textContent = 'Unlock';
  if (subtitle) subtitle.textContent = 'Sign in to continue';
  locked = false;
  setIdleStatus();
  if (!focus) return;
  if (!usernameInput.value.trim()) usernameInput.focus();
  else passwordInput.focus();
}

function showValidationError(msg, focusEl) {
  status.textContent = msg;
  status.className = 'unlock-status error';
  card.classList.add('shake');
  unlockBtn.disabled = false;
  setTimeout(() => {
    card.classList.remove('shake');
    if (focusEl) focusEl.focus();
  }, 400);
}

function setError(msg, holdMs) {
  status.textContent = msg;
  status.className = 'unlock-status error';
  card.classList.add('shake');
  setTimeout(() => {
    card.classList.remove('shake');
    if (mfaPending) {
      // A wrong code keeps the verified password out of the page: back to the start.
      resetToPassword(true);
    } else {
      passwordInput.value = '';
      locked = false;
      setIdleStatus();
      passwordInput.focus();
    }
  }, holdMs || 800);
}

function setSuccess(role) {
  status.textContent = role === 'viewer' ? 'Unlocked — view only. Redirecting...' : 'Unlocked! Redirecting...';
  status.className = 'unlock-status success';
  card.classList.add('unlock-pulse');
}

async function tryUnlock() {
  if (locked) return;
  const username = usernameInput.value.trim();
  const password = passwordInput.value;
  const totp = totpInput.value.trim();
  if (!username) { showValidationError('Enter your username', usernameInput); return; }
  if (!password) { showValidationError('Enter your password', passwordInput); return; }
  if (mfaPending && !/^\d{6}$/.test(totp)) { showValidationError('Enter the 6-digit authenticator code', totpInput); return; }
  locked = true;
  status.textContent = 'Verifying...';
  status.className = 'unlock-status';
  unlockBtn.disabled = true;
  try {
    const res = await fetch('/api/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      credentials: 'same-origin',
      body: JSON.stringify({ username, password, ...(mfaPending ? { totp } : {}) }),
    });
    if (res.ok) {
      let role = '';
      try { role = String(((await res.json()) || {}).role || ''); } catch (e) { /* the redirect is the same */ }
      setSuccess(role);
      setTimeout(() => { window.location.href = '/app'; }, 400);
      return;
    }
    const data = await res.json().catch(() => ({}));
    // error_handlers.py reshapes 4xx bodies into {success, error:{detail}}.
    const detail = (data && data.detail) || (data && data.error && data.error.detail) || '';
    const code = (data && data.code) || (data && data.error && data.error.code) || '';
    if (res.status === 428 && (code === 'mfa_required' || /authenticator/i.test(detail))) {
      locked = false;
      mfaPending = true;
      totpField.classList.remove('hidden');
      unlockBtn.disabled = false;
      unlockBtn.textContent = 'Verify & Unlock';
      if (subtitle) subtitle.textContent = 'Two-factor code';
      status.textContent = detail || baseStatusMessage();
      status.className = 'unlock-status';
      totpInput.focus();
      return;
    }
    // The lockout escalates, so "try again in 6 hours" is real information.
    if (res.status === 429) { setError(detail || 'Too many attempts. Try again later.', 4000); return; }
    setError(detail || 'Wrong credentials. Try again.');
  } catch (e) {
    setError('Connection error.');
  }
}

usernameInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') { e.preventDefault(); passwordInput.focus(); }
});
passwordInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') { e.preventDefault(); tryUnlock(); }
});
totpInput.addEventListener('input', () => {
  totpInput.value = totpInput.value.replace(/\D/g, '').slice(0, 6);
});
totpInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') { e.preventDefault(); tryUnlock(); }
});
passwordToggle.addEventListener('click', () => {
  const nextType = passwordInput.type === 'password' ? 'text' : 'password';
  passwordInput.type = nextType;
  passwordToggle.textContent = nextType === 'password' ? 'Show' : 'Hide';
  passwordToggle.setAttribute('aria-label', nextType === 'password' ? 'Show password' : 'Hide password');
});
unlockBtn.addEventListener('click', tryUnlock);
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && !locked) resetToPassword(true);
});

initLoginAppearance();
resetToPassword(false);

// ══════════════════════════════════════════════════════════════
//  PASSKEY SIGN-IN (Face ID / fingerprint)
//
//  The biometric never reaches this code or the server. The phone unlocks a
//  private key held in its own secure hardware and hands back a signature;
//  CryptoForge only ever stores and checks a public key.
// ══════════════════════════════════════════════════════════════
(() => {
  const btn = document.getElementById('passkey-btn');
  if (!btn) return;
  const b64urlToBytes = (value) => {
    const padded = value.replace(/-/g, '+').replace(/_/g, '/') + '='.repeat((4 - (value.length % 4)) % 4);
    return Uint8Array.from(atob(padded), (c) => c.charCodeAt(0));
  };
  const bytesToB64url = (buffer) =>
    btoa(String.fromCharCode(...new Uint8Array(buffer))).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  // Only offer this where it can actually work: a secure context with a
  // built-in authenticator. Otherwise the button stays hidden.
  const supported = window.PublicKeyCredential
    && window.isSecureContext
    && typeof PublicKeyCredential.isUserVerifyingPlatformAuthenticatorAvailable === 'function';
  if (!supported) return;
  PublicKeyCredential.isUserVerifyingPlatformAuthenticatorAvailable()
    .then((available) => { if (available) btn.hidden = false; })
    .catch(() => {});
  btn.addEventListener('click', async () => {
    btn.disabled = true;
    status.textContent = 'Waiting for your fingerprint or face...';
    status.className = 'unlock-status';
    try {
      const optionsRes = await fetch('/api/auth/passkeys/login/options', {
        method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' }, body: '{}',
      });
      const optionsData = await optionsRes.json();
      if (!optionsRes.ok) throw new Error(optionsData.detail || 'Could not start passkey sign-in');
      const assertion = await navigator.credentials.get({
        publicKey: {
          ...optionsData.options,
          challenge: b64urlToBytes(optionsData.options.challenge),
          allowCredentials: [],
        },
      });
      if (!assertion) throw new Error('No passkey was chosen');
      const verifyRes = await fetch('/api/auth/passkeys/login/verify', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          challenge_id: optionsData.challenge_id,
          credential: {
            id: assertion.id,
            type: assertion.type,
            response: {
              clientDataJSON: bytesToB64url(assertion.response.clientDataJSON),
              authenticatorData: bytesToB64url(assertion.response.authenticatorData),
              signature: bytesToB64url(assertion.response.signature),
            },
          },
        }),
      });
      const verifyData = await verifyRes.json();
      if (!verifyRes.ok) throw new Error(verifyData.detail || (verifyData.error && verifyData.error.detail) || 'That passkey was not accepted');
      setSuccess(String(verifyData.role || ''));
      setTimeout(() => { window.location.href = '/app'; }, 300);
    } catch (error) {
      // A user who changes their mind is not an error worth shouting about.
      const cancelled = error && (error.name === 'NotAllowedError' || error.name === 'AbortError');
      status.textContent = cancelled ? baseStatusMessage() : (error.message || 'Passkey sign-in failed');
      status.className = cancelled ? 'unlock-status' : 'unlock-status error';
      btn.disabled = false;
    }
  });
})();
