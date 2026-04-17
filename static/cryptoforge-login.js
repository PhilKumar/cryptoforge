try {
  if (typeof window.cfApplyTheme === 'function') {
    window.cfApplyTheme(typeof window.cfGetStoredTheme === 'function' ? window.cfGetStoredTheme() : '', { persist: false });
  }
} catch(e) {}

const PIN_LENGTH = 6;
let pin = '';
let locked = false;

const dots = document.querySelectorAll('.pin-dot');
const status = document.getElementById('unlock-status');
const card = document.getElementById('unlock-card');

function updateDots() {
  dots.forEach((dot, i) => {
    dot.classList.remove('filled', 'error', 'success');
    if (i < pin.length) dot.classList.add('filled');
  });
}

function setError(msg) {
  status.textContent = msg;
  status.className = 'unlock-status error';
  dots.forEach(d => { d.classList.remove('filled'); d.classList.add('error'); });
  card.classList.add('shake');
  setTimeout(() => {
    card.classList.remove('shake');
    pin = '';
    updateDots();
    status.textContent = 'Enter your 6-digit PIN';
    status.className = 'unlock-status';
    locked = false;
  }, 800);
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
      body: JSON.stringify({ password: pin }),
      credentials: 'same-origin'
    });
    if (res.ok) {
      setSuccess();
      setTimeout(() => { window.location.href = '/'; }, 400);
    } else {
      setError('Wrong PIN. Try again.');
    }
  } catch (e) {
    setError('Connection error.');
  }
}

function addDigit(d) {
  if (locked || pin.length >= PIN_LENGTH) return;
  pin += d;
  updateDots();
  if (pin.length === PIN_LENGTH) {
    setTimeout(tryUnlock, 150);
  }
}

function removeDigit() {
  if (locked || pin.length === 0) return;
  pin = pin.slice(0, -1);
  updateDots();
}

function clearAll() {
  if (locked) return;
  pin = '';
  updateDots();
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
