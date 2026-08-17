/* ══════════════════════════════════════════════════════════════
   ACCOUNTS — Account Settings for whoever is signed in, and the Users
   panel of the Admin Console.

   Phil, 2026-08-17: "I need the same kinda authentication for cryptoforge
   as well with username and password and authentication... Also I want to
   add user from admin console... Also with biometrics on cryptoforge."

   Ported from PhilForge (philforge-app.js: openAccountModal, the MFA
   enrolment trio, registerPasskey, changeOwnPasswordFromSettings, the admin
   users table) onto CryptoForge's helpers — cfApiFetch (CSRF), cfToast,
   cfConfirm, cfPrompt, the data-cf-click dispatcher. Every function here is
   a top-level declaration so the dispatcher finds it on window.
   ══════════════════════════════════════════════════════════════ */

var _cfAccountProfile = null;

function _cfAccountText(id, value) {
  var el = document.getElementById(id);
  if (el) el.textContent = value == null || value === '' ? '—' : String(value);
}

function _cfAccountWhen(value) {
  if (!value) return '—';
  var d = new Date(value);
  if (isNaN(d.getTime())) return String(value);
  return d.toLocaleString('en-IN', { timeZone: 'Asia/Kolkata', day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false }) + ' IST';
}

function _cfAccountErr(payload, fallback) {
  return typeof cfApiErrorDetail === 'function' ? cfApiErrorDetail(payload, fallback) : fallback;
}

// ── Account Settings modal ────────────────────────────────────
function cfOpenAccountModal() {
  var modal = document.getElementById('account-modal');
  if (!modal) return;
  ['account-current-password', 'account-new-password', 'account-confirm-password', 'account-mfa-password', 'account-mfa-code'].forEach(function (id) {
    var el = document.getElementById(id);
    if (el) el.value = '';
  });
  _cfResetMfaEnrollmentBox();
  modal.hidden = false;
  modal.classList.add('open');
  document.body.classList.add('admin-console-open');
  cfLoadAccountProfile(true);
}

function cfCloseAccountModal() {
  var modal = document.getElementById('account-modal');
  if (!modal) return;
  modal.classList.remove('open');
  modal.hidden = true;
  document.body.classList.remove('admin-console-open');
  ['account-mfa-password', 'account-mfa-code'].forEach(function (id) {
    var el = document.getElementById(id);
    if (el) el.value = '';
  });
  _cfResetMfaEnrollmentBox();
}

function _cfResetMfaEnrollmentBox() {
  var box = document.getElementById('account-mfa-enrollment');
  if (box) box.hidden = true;
  var verify = document.getElementById('account-mfa-verify-btn');
  if (verify) verify.hidden = true;
  var secret = document.getElementById('account-mfa-secret');
  if (secret) secret.textContent = '';
  var qr = document.getElementById('account-mfa-qr');
  if (qr) qr.removeAttribute('src');
  var uri = document.getElementById('account-mfa-uri');
  if (uri) uri.removeAttribute('href');
}

async function cfLoadAccountProfile(silent) {
  try {
    var r = await cfApiFetch('/api/user/profile', { cache: 'no-store' });
    var d = await cfReadApiPayload(r);
    if (!r.ok || d.status !== 'ok') throw new Error(_cfAccountErr(d, 'Could not load your account'));
    _cfAccountProfile = d;
    var user = d.user || {};
    _cfAccountText('account-profile-username', user.username);
    _cfAccountText('account-profile-role', String(user.role || 'user').toUpperCase());
    _cfAccountText('account-modal-sub', 'Signed in as ' + (user.username || '') + ' · ' + String(user.role || 'user'));
    _cfAccountText('account-profile-created', _cfAccountWhen(user.created_at));
    _cfAccountText('account-profile-login', _cfAccountWhen(user.last_login));
    var mfa = !!user.mfa_enabled;
    _cfAccountText('account-mfa-status-chip', mfa ? 'Enabled' : 'Not set up');
    _cfAccountText('account-mfa-status-line', mfa
      ? 'Authenticator protection is on' + (user.mfa_enrolled_at ? ' since ' + _cfAccountWhen(user.mfa_enrolled_at) : '') + '. Sign-in asks for a fresh 6-digit code.'
      : 'Not set up. Add an authenticator so sign-in needs your password AND a code from your phone.');
    var start = document.getElementById('account-mfa-start-btn');
    if (start) start.textContent = mfa ? 'Replace Authenticator' : 'Set Up Authenticator';
    var disable = document.getElementById('account-mfa-disable-btn');
    if (disable) disable.hidden = !mfa;
    cfRefreshPasskeyStatus(d.passkeys);
    if (!silent) cfToast('Account settings loaded', 'success');
  } catch (e) {
    if (!silent) cfToast(e.message || 'Could not load your account', 'error');
  }
}

// ── Authenticator (TOTP) ──────────────────────────────────────
async function cfStartMfaEnrollment() {
  var password = (document.getElementById('account-mfa-password') || {}).value || '';
  var code = ((document.getElementById('account-mfa-code') || {}).value || '').trim();
  if (!password) { cfToast('Enter your current password', 'warning'); return; }
  var enrolled = !!(_cfAccountProfile && _cfAccountProfile.user && _cfAccountProfile.user.mfa_enabled);
  if (enrolled && !/^\d{6}$/.test(code)) { cfToast('Enter a fresh code from your current authenticator before replacing it', 'warning'); return; }
  try {
    var r = await cfApiFetch('/api/auth/mfa/enroll/start', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(Object.assign({ password: password }, code ? { totp: code } : {})),
    });
    var d = await cfReadApiPayload(r);
    if (!r.ok || d.status !== 'pending') throw new Error(_cfAccountErr(d, 'Could not start authenticator setup'));
    document.getElementById('account-mfa-secret').textContent = d.secret || '';
    var qr = document.getElementById('account-mfa-qr');
    if (d.qr_data_uri) { qr.src = d.qr_data_uri; qr.hidden = false; } else { qr.removeAttribute('src'); qr.hidden = true; }
    document.getElementById('account-mfa-uri').href = d.otpauth_uri || '#';
    document.getElementById('account-mfa-enrollment').hidden = false;
    document.getElementById('account-mfa-verify-btn').hidden = false;
    var codeEl = document.getElementById('account-mfa-code');
    codeEl.value = ''; codeEl.focus();
    cfToast('Scan the QR (or add the key), then enter its first code and press Verify', 'info');
  } catch (e) {
    cfToast(e.message || 'Could not start authenticator setup', 'error');
  }
}

async function cfVerifyMfaEnrollment() {
  var password = (document.getElementById('account-mfa-password') || {}).value || '';
  var code = ((document.getElementById('account-mfa-code') || {}).value || '').trim();
  if (!password || !/^\d{6}$/.test(code)) { cfToast('Enter your current password and the new 6-digit code', 'warning'); return; }
  try {
    var r = await cfApiFetch('/api/auth/mfa/enroll/verify', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ password: password, totp: code }),
    });
    var d = await cfReadApiPayload(r);
    if (!r.ok || d.status !== 'ok') throw new Error(_cfAccountErr(d, 'Authenticator verification failed'));
    _cfResetMfaEnrollmentBox();
    document.getElementById('account-mfa-password').value = '';
    document.getElementById('account-mfa-code').value = '';
    cfToast('Authenticator enabled. Sign-in now asks for a code. Other devices were signed out.', 'success');
    await cfLoadAccountProfile(true);
  } catch (e) {
    cfToast(e.message || 'Authenticator verification failed', 'error');
  }
}

async function cfDisableMfa() {
  var password = (document.getElementById('account-mfa-password') || {}).value || '';
  var code = ((document.getElementById('account-mfa-code') || {}).value || '').trim();
  if (!password || !/^\d{6}$/.test(code)) { cfToast('Enter your current password and a fresh authenticator code', 'warning'); return; }
  var ok = await cfConfirm('Disable authenticator protection? You will be signed out, and sign-in will need only your password until it is set up again.', 'Disable Authenticator', '🔓');
  if (!ok) return;
  try {
    var r = await cfApiFetch('/api/auth/mfa', {
      method: 'DELETE', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ password: password, totp: code }),
    });
    var d = await cfReadApiPayload(r);
    if (!r.ok || d.status !== 'ok') throw new Error(_cfAccountErr(d, 'Could not disable authenticator'));
    if (typeof window.cfBeginSignOut === 'function') window.cfBeginSignOut();
    window.location.assign('/app');
  } catch (e) {
    cfToast(e.message || 'Could not disable authenticator', 'error');
  }
}

// ── Passkeys — Face ID / fingerprint for this device ──────────
// The biometric stays on the device. What is registered here is a public key;
// the private half never leaves the phone's secure hardware, so nothing on the
// server can impersonate anyone.
function _cfB64urlToBytes(value) {
  var padded = String(value).replace(/-/g, '+').replace(/_/g, '/') + '='.repeat((4 - (String(value).length % 4)) % 4);
  return Uint8Array.from(atob(padded), function (c) { return c.charCodeAt(0); });
}

function _cfBytesToB64url(buffer) {
  return btoa(String.fromCharCode.apply(null, new Uint8Array(buffer))).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

async function cfRefreshPasskeyStatus(preloaded) {
  var chip = document.getElementById('account-passkey-status-chip');
  var line = document.getElementById('account-passkey-status-line');
  var list = document.getElementById('account-passkey-list');
  var addBtn = document.getElementById('account-passkey-add-btn');
  if (!chip || !line || !list) return;
  var supported = window.PublicKeyCredential && window.isSecureContext;
  if (!supported) {
    chip.textContent = 'Unavailable';
    line.textContent = 'This browser cannot use passkeys here. Open the site over HTTPS on a phone or laptop with a fingerprint or face reader.';
    if (addBtn) addBtn.disabled = true;
    list.textContent = '';
    return;
  }
  try {
    var keys = Array.isArray(preloaded) ? preloaded : null;
    if (!keys) {
      var r = await cfApiFetch('/api/auth/passkeys', { cache: 'no-store' });
      var d = await cfReadApiPayload(r);
      if (!r.ok) throw new Error(_cfAccountErr(d, 'Could not read registered devices'));
      keys = d.passkeys || [];
    }
    chip.textContent = keys.length ? (keys.length + ' device' + (keys.length === 1 ? '' : 's')) : 'Not set up';
    line.textContent = keys.length
      ? 'Sign in with your fingerprint or face on the devices below.'
      : 'No device registered yet. Password sign-in still works everywhere.';
    list.textContent = '';
    keys.forEach(function (key) {
      var row = document.createElement('div');
      row.className = 'cf-passkey-row';
      var text = document.createElement('div');
      var name = document.createElement('div');
      name.textContent = key.label || 'Device';
      var sub = document.createElement('div');
      sub.className = 'admin-user-sub';
      sub.textContent = 'added ' + String(key.created_at || '').slice(0, 10) + (key.last_used_at ? ', last used ' + String(key.last_used_at).slice(0, 10) : ', never used');
      text.appendChild(name); text.appendChild(sub);
      var remove = document.createElement('button');
      remove.type = 'button';
      remove.className = 'admin-action-btn admin-action-danger';
      remove.textContent = 'Remove';
      remove.addEventListener('click', function () { cfRemovePasskey(key.credential_id, key.label); });
      row.appendChild(text); row.appendChild(remove);
      list.appendChild(row);
    });
    if (addBtn) addBtn.disabled = false;
  } catch (e) {
    chip.textContent = 'Error';
    line.textContent = e.message || 'Could not read registered devices';
  }
}

async function cfRegisterPasskey() {
  var btn = document.getElementById('account-passkey-add-btn');
  if (btn) btn.disabled = true;
  try {
    var optionsRes = await cfApiFetch('/api/auth/passkeys/register/options', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}',
    });
    var optionsData = await cfReadApiPayload(optionsRes);
    if (!optionsRes.ok) throw new Error(_cfAccountErr(optionsData, 'Could not start registration'));
    var options = optionsData.options;
    var credential = await navigator.credentials.create({
      publicKey: Object.assign({}, options, {
        challenge: _cfB64urlToBytes(options.challenge),
        user: Object.assign({}, options.user, { id: _cfB64urlToBytes(options.user.id) }),
        excludeCredentials: (options.excludeCredentials || []).map(function (c) { return Object.assign({}, c, { id: _cfB64urlToBytes(c.id) }); }),
      }),
    });
    if (!credential) throw new Error('No passkey was created');
    var verifyRes = await cfApiFetch('/api/auth/passkeys/register/verify', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        challenge_id: optionsData.challenge_id,
        // A name the owner will recognise on the list, not a device fingerprint.
        label: /iPhone|iPad|Android/i.test(navigator.userAgent) ? 'Phone' : 'Computer',
        credential: {
          id: credential.id,
          type: credential.type,
          response: {
            clientDataJSON: _cfBytesToB64url(credential.response.clientDataJSON),
            attestationObject: _cfBytesToB64url(credential.response.attestationObject),
          },
        },
      }),
    });
    var verifyData = await cfReadApiPayload(verifyRes);
    if (!verifyRes.ok) throw new Error(_cfAccountErr(verifyData, 'That device was not accepted'));
    cfToast('This device can now sign you in with Face ID or your fingerprint', 'success');
    cfRefreshPasskeyStatus();
  } catch (e) {
    var cancelled = e && (e.name === 'NotAllowedError' || e.name === 'AbortError');
    if (!cancelled) cfToast(e.message || 'Could not register this device', 'error');
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function cfRemovePasskey(credentialId, label) {
  var ok = await cfConfirm('Remove ' + (label || 'this device') + '? It will no longer sign in with a fingerprint. Password sign-in is unaffected.', 'Remove Passkey', '🔑');
  if (!ok) return;
  try {
    var r = await cfApiFetch('/api/auth/passkeys/' + encodeURIComponent(credentialId), { method: 'DELETE' });
    var d = await cfReadApiPayload(r);
    if (!r.ok) throw new Error(_cfAccountErr(d, 'Could not remove that device'));
    cfRefreshPasskeyStatus();
  } catch (e) {
    cfToast(e.message || 'Could not remove that device', 'error');
  }
}

// ── Password ──────────────────────────────────────────────────
var CF_PASSWORD_HINT = 'At least 8 characters, with a letter and a number.';

function cfPasswordRuleError(password, label) {
  var value = String(password || '');
  label = label || 'Password';
  if (value.length < 8) return label + ' must be at least 8 characters';
  if (!/[A-Za-z]/.test(value) || !/\d/.test(value)) return label + ' needs at least one letter and one number';
  return '';
}

async function cfChangeOwnPassword() {
  var current = (document.getElementById('account-current-password') || {}).value || '';
  var next = (document.getElementById('account-new-password') || {}).value || '';
  var confirm = (document.getElementById('account-confirm-password') || {}).value || '';
  if (!current || !next) { cfToast('Enter both your current and new password', 'warning'); return; }
  if (next !== confirm) { cfToast('New password and confirmation do not match', 'warning'); return; }
  var problem = cfPasswordRuleError(next, 'New password');
  if (problem) { cfToast(problem, 'warning'); return; }
  try {
    var r = await cfApiFetch('/api/user/password', {
      method: 'PUT', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ current_password: current, new_password: next }),
    });
    var d = await cfReadApiPayload(r);
    if (!r.ok || d.status !== 'ok') throw new Error(_cfAccountErr(d, 'Password change failed'));
    ['account-current-password', 'account-new-password', 'account-confirm-password'].forEach(function (id) { document.getElementById(id).value = ''; });
    cfToast(d.message || 'Password changed', 'success');
  } catch (e) {
    cfToast(e.message || 'Password change failed', 'error');
  }
}

// ── Admin Console → Users ─────────────────────────────────────
var _cfAdminUsers = [];

async function cfAdminLoadUsers(silent) {
  var container = document.getElementById('admin-users-list');
  if (!container) return;
  try {
    var r = await cfApiFetch('/api/admin/users', { cache: 'no-store' });
    var d = await cfReadApiPayload(r);
    if (!r.ok || d.status !== 'ok') throw new Error(_cfAccountErr(d, 'Could not load users'));
    _cfAdminUsers = d.users || [];
    cfAdminRenderUsers(_cfAdminUsers);
    var pill = document.getElementById('admin-users-pill');
    if (pill) {
      var active = _cfAdminUsers.filter(function (u) { return u.is_active; }).length;
      pill.textContent = _cfAdminUsers.length + ' account' + (_cfAdminUsers.length === 1 ? '' : 's') + ' · ' + active + ' active';
    }
    if (!silent) cfToast('Users refreshed', 'success');
  } catch (e) {
    container.className = 'admin-empty-state';
    container.textContent = e.message || 'Could not load users';
    if (!silent) cfToast(e.message || 'Could not load users', 'error');
  }
}

function _cfRoleChip(text, cls) {
  var chip = document.createElement('span');
  chip.className = 'admin-role-chip ' + cls;
  chip.textContent = text;
  return chip;
}

function cfAdminRenderUsers(users) {
  var container = document.getElementById('admin-users-list');
  if (!container) return;
  if (!users.length) {
    container.className = 'admin-empty-state';
    container.textContent = 'No accounts yet.';
    return;
  }
  container.className = '';
  container.textContent = '';
  var me = window._cfAuthUser || {};
  var table = document.createElement('table');
  table.className = 'admin-users-table';
  var thead = document.createElement('thead');
  var hr = document.createElement('tr');
  ['Account', 'Security', 'Activity', 'Actions'].forEach(function (h) {
    var th = document.createElement('th'); th.textContent = h; hr.appendChild(th);
  });
  thead.appendChild(hr);
  table.appendChild(thead);
  var tbody = document.createElement('tbody');
  users.forEach(function (user) {
    var row = document.createElement('tr');
    var isMe = Number(user.id) === Number(me.user_id);

    var cell = document.createElement('td');
    cell.className = 'admin-user-cell';
    var nameLine = document.createElement('div');
    nameLine.className = 'admin-user-name';
    var strong = document.createElement('strong');
    strong.textContent = user.username;
    nameLine.appendChild(strong);
    var roleLabel = user.role === 'admin' ? 'Admin' : user.role === 'viewer' ? 'Viewer' : 'User';
    nameLine.appendChild(_cfRoleChip(roleLabel, user.role || 'user'));
    nameLine.appendChild(_cfRoleChip(user.is_active ? 'Active' : 'Disabled', user.is_active ? 'on' : 'off'));
    if (isMe) nameLine.appendChild(_cfRoleChip('You', 'mfa'));
    cell.appendChild(nameLine);
    var created = document.createElement('div');
    created.className = 'admin-user-sub';
    created.textContent = 'Created ' + _cfAccountWhen(user.created_at);
    cell.appendChild(created);
    row.appendChild(cell);

    var sec = document.createElement('td');
    sec.className = 'admin-user-cell';
    var secLine = document.createElement('div');
    secLine.className = 'admin-user-name';
    secLine.appendChild(_cfRoleChip(user.mfa_enabled ? 'Authenticator on' : 'No authenticator', user.mfa_enabled ? 'on' : 'mfa'));
    sec.appendChild(secLine);
    var secSub = document.createElement('div');
    secSub.className = 'admin-user-sub';
    secSub.textContent = user.mfa_enabled ? 'Sign-in needs password + code' : 'Password only until they set one up';
    sec.appendChild(secSub);
    row.appendChild(sec);

    var act = document.createElement('td');
    act.className = 'admin-user-cell';
    var last = document.createElement('div');
    last.textContent = 'Last sign-in ' + _cfAccountWhen(user.last_login);
    act.appendChild(last);
    row.appendChild(act);

    var actions = document.createElement('td');
    var actRow = document.createElement('div');
    actRow.className = 'admin-action-row';
    if (isMe) {
      var note = document.createElement('span');
      note.className = 'admin-user-sub';
      note.textContent = 'Your own account — change it in Account Settings';
      actRow.appendChild(note);
    } else {
      var toggle = document.createElement('button');
      toggle.type = 'button';
      toggle.className = 'admin-action-btn';
      toggle.textContent = user.is_active ? 'Disable' : 'Enable';
      toggle.addEventListener('click', function () { cfAdminToggleUser(user.id); });
      actRow.appendChild(toggle);

      var reset = document.createElement('button');
      reset.type = 'button';
      reset.className = 'admin-action-btn';
      reset.textContent = 'Reset Password';
      reset.addEventListener('click', function () { cfAdminResetPassword(user.id, user.username); });
      actRow.appendChild(reset);

      var role = document.createElement('button');
      role.type = 'button';
      role.className = 'admin-action-btn';
      role.textContent = 'Change Role';
      role.addEventListener('click', function () { cfAdminSetRole(user.id, user.username, user.role); });
      actRow.appendChild(role);

      // Last, and marked: Disable is the reversible retirement path, this
      // one removes the account and its passkeys.
      var del = document.createElement('button');
      del.type = 'button';
      del.className = 'admin-action-btn admin-action-danger';
      del.textContent = 'Delete';
      del.addEventListener('click', function () { cfAdminDeleteUser(user.id, user.username); });
      actRow.appendChild(del);
    }
    actions.appendChild(actRow);
    row.appendChild(actions);
    tbody.appendChild(row);
  });
  table.appendChild(tbody);
  container.appendChild(table);
}

async function cfAdminCreateUser() {
  var username = ((document.getElementById('admin-create-username') || {}).value || '').trim();
  var password = (document.getElementById('admin-create-password') || {}).value || '';
  var role = (document.getElementById('admin-create-role') || {}).value || 'viewer';
  if (!username || !password) { cfToast('Enter a username and a password', 'warning'); return; }
  var problem = cfPasswordRuleError(password);
  if (problem) { cfToast(problem, 'warning'); return; }
  try {
    var r = await cfApiFetch('/api/admin/users', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: username, password: password, role: role }),
    });
    var d = await cfReadApiPayload(r);
    if (!r.ok || d.status !== 'ok') throw new Error(_cfAccountErr(d, 'Could not create the account'));
    cfToast('Account "' + username + '" created as ' + role, 'success');
    document.getElementById('admin-create-username').value = '';
    document.getElementById('admin-create-password').value = '';
    document.getElementById('admin-create-role').value = 'viewer';
    await cfAdminLoadUsers(true);
  } catch (e) {
    cfToast(e.message || 'Could not create the account', 'error');
  }
}

async function cfAdminToggleUser(userId) {
  try {
    var r = await cfApiFetch('/api/admin/users/' + encodeURIComponent(userId) + '/toggle', { method: 'PUT' });
    var d = await cfReadApiPayload(r);
    if (!r.ok || d.status !== 'ok') throw new Error(_cfAccountErr(d, 'Could not update the account'));
    cfToast(d.is_active ? 'Account enabled' : 'Account disabled and signed out', d.is_active ? 'success' : 'warning');
    await cfAdminLoadUsers(true);
  } catch (e) {
    cfToast(e.message || 'Could not update the account', 'error');
  }
}

async function cfAdminResetPassword(userId, username) {
  var password = await cfPrompt('Reset password for ' + username, CF_PASSWORD_HINT, '', 'password');
  if (password == null) return;
  var problem = cfPasswordRuleError(password);
  if (problem) { cfToast(problem, 'warning'); return; }
  try {
    var r = await cfApiFetch('/api/admin/users/' + encodeURIComponent(userId) + '/password', {
      method: 'PUT', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ password: password }),
    });
    var d = await cfReadApiPayload(r);
    if (!r.ok || d.status !== 'ok') throw new Error(_cfAccountErr(d, 'Could not reset the password'));
    cfToast(d.message || ('Password reset for ' + username + ' — they are signed out everywhere'), 'success');
  } catch (e) {
    cfToast(e.message || 'Could not reset the password', 'error');
  }
}

async function cfAdminSetRole(userId, username, currentRole) {
  var role = await cfSelect('Change role for ' + username, 'Viewer sees everything and changes nothing. User can trade. Admin runs the desk.',
    [{ value: 'viewer', label: 'Viewer' }, { value: 'user', label: 'User' }, { value: 'admin', label: 'Admin' }], currentRole || 'viewer');
  if (role == null || role === currentRole) return;
  try {
    var r = await cfApiFetch('/api/admin/users/' + encodeURIComponent(userId) + '/role', {
      method: 'PUT', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ role: role }),
    });
    var d = await cfReadApiPayload(r);
    if (!r.ok || d.status !== 'ok') throw new Error(_cfAccountErr(d, 'Could not change the role'));
    cfToast(username + ' is now ' + role + ' — they are signed out and sign in with the new role', 'success');
    await cfAdminLoadUsers(true);
  } catch (e) {
    cfToast(e.message || 'Could not change the role', 'error');
  }
}

async function cfAdminDeleteUser(userId, username) {
  // Typing the name is the point: Delete sits one button along from Disable.
  var typed = await cfPrompt('Delete ' + username + '?', 'This removes the account and its Face ID / fingerprint devices, and cannot be undone. Disable is the reversible option. Type the username to confirm.', '');
  if (typed == null) return;
  if (String(typed).trim() !== String(username)) { cfToast('Name did not match — nothing was deleted', 'warning'); return; }
  try {
    var r = await cfApiFetch('/api/admin/users/' + encodeURIComponent(userId), { method: 'DELETE' });
    var d = await cfReadApiPayload(r);
    if (!r.ok || d.status !== 'ok') throw new Error(_cfAccountErr(d, 'Could not delete the account'));
    cfToast('Deleted "' + username + '"', 'success');
    await cfAdminLoadUsers(true);
  } catch (e) {
    cfToast(e.message || 'Could not delete the account', 'error');
  }
}
