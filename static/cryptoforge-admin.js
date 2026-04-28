(function () {
  var adminState = null;
  var sectionLabels = {
    routing: 'Routing',
    security: 'Security',
    delta: 'Delta',
    coindcx: 'CoinDCX'
  };

  function adminEl(id) { return document.getElementById(id); }

  function adminEscape(value) {
    return String(value == null ? '' : value).replace(/[&<>'"]/g, function (ch) {
      return ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;' })[ch];
    });
  }

  function adminFieldId(key) {
    return 'admin-env-' + String(key || '').toLowerCase().replace(/[^a-z0-9]+/g, '-');
  }

  function adminFields(section) {
    var fields = (adminState && Array.isArray(adminState.fields)) ? adminState.fields : [];
    return fields.filter(function (field) { return field.section === section; });
  }

  function adminField(key) {
    var fields = (adminState && Array.isArray(adminState.fields)) ? adminState.fields : [];
    return fields.find(function (field) { return field.key === key; }) || {};
  }

  function adminSectionConfigured(section) {
    var fields = adminFields(section).filter(function (field) { return field.secret; });
    return fields.length > 0 && fields.every(function (field) { return !!field.configured; });
  }

  function adminSetText(id, value) {
    var node = adminEl(id);
    if (node) node.textContent = value == null ? '--' : String(value);
  }

  function adminSetPill(id, configured) {
    var node = adminEl(id);
    if (!node) return;
    node.textContent = configured ? 'Configured' : 'Missing';
    node.dataset.state = configured ? 'ok' : 'warn';
  }

  function adminCreateField(field) {
    var wrap = document.createElement('div');
    wrap.className = 'admin-field';
    wrap.dataset.key = field.key;

    var label = document.createElement('label');
    label.setAttribute('for', adminFieldId(field.key));
    label.textContent = field.label || field.key;
    wrap.appendChild(label);

    var input;
    if (field.kind === 'select' || field.kind === 'boolean') {
      input = document.createElement('select');
      var options = Array.isArray(field.options) && field.options.length ? field.options : [''];
      options.forEach(function (optionValue) {
        var option = document.createElement('option');
        option.value = String(optionValue);
        option.textContent = String(optionValue).toUpperCase();
        input.appendChild(option);
      });
      input.value = field.value || options[0] || '';
    } else {
      input = document.createElement('input');
      input.type = field.secret ? 'password' : 'text';
      input.autocomplete = 'off';
      input.spellcheck = false;
      input.value = field.secret ? '' : (field.value || '');
      input.placeholder = field.secret
        ? (field.configured ? ('Saved: ' + (field.masked || 'configured')) : 'Not set')
        : '';
    }
    input.id = adminFieldId(field.key);
    input.dataset.adminEnvKey = field.key;
    input.dataset.adminSecret = field.secret ? '1' : '0';
    wrap.appendChild(input);

    var meta = document.createElement('div');
    meta.className = 'admin-field-meta';
    if (field.secret) {
      if (field.key === 'CRYPTOFORGE_PIN') {
        meta.textContent = field.configured ? 'Leave blank to keep the current unlock PIN.' : 'Set an unlock PIN before production use.';
      } else {
        var clearId = adminFieldId(field.key) + '-clear';
        var clearLabel = document.createElement('label');
        clearLabel.className = 'admin-clear-secret';
        var clear = document.createElement('input');
        clear.type = 'checkbox';
        clear.id = clearId;
        clear.dataset.adminClearKey = field.key;
        clearLabel.appendChild(clear);
        clearLabel.appendChild(document.createTextNode(' Clear saved value'));
        meta.appendChild(clearLabel);
      }
    } else {
      meta.textContent = field.help || field.key;
    }
    wrap.appendChild(meta);
    return wrap;
  }

  function adminRenderFieldSection(section) {
    var mount = adminEl('admin-fields-' + section);
    if (!mount) return;
    mount.innerHTML = '';
    var fields = adminFields(section);
    if (!fields.length) {
      var empty = document.createElement('div');
      empty.className = 'admin-empty';
      empty.textContent = 'No ' + (sectionLabels[section] || section) + ' fields available';
      mount.appendChild(empty);
      return;
    }
    fields.forEach(function (field) { mount.appendChild(adminCreateField(field)); });
  }

  function adminRenderBrokerCards(settings) {
    var mount = adminEl('admin-broker-cards');
    var select = adminEl('admin-active-broker-select');
    if (!mount || !select) return;
    var brokers = Array.isArray(settings.available_brokers) ? settings.available_brokers : [];
    var current = settings.current_broker || '';
    mount.innerHTML = '';
    select.innerHTML = '';
    brokers.forEach(function (broker) {
      var option = document.createElement('option');
      option.value = broker.name;
      option.textContent = broker.label || broker.name;
      select.appendChild(option);

      var card = document.createElement('button');
      card.type = 'button';
      card.className = 'admin-broker-card';
      card.dataset.broker = broker.name;
      if (broker.name === current) card.classList.add('active');
      card.innerHTML =
        '<span class="admin-broker-card-label">' + adminEscape(broker.label || broker.name) + '</span>' +
        '<strong>' + adminEscape(String(broker.name || '').toUpperCase()) + '</strong>' +
        '<span>' + adminEscape(broker.feed_kind || 'polling') + ' / ' + (broker.configured ? 'configured' : 'missing') + '</span>';
      card.addEventListener('click', function () { select.value = broker.name; });
      mount.appendChild(card);
    });
    if (current) select.value = current;
    select.disabled = !settings.switchable;
  }

  function adminRenderLocks(settings) {
    var mount = adminEl('admin-lock-reasons');
    if (!mount) return;
    var locks = settings.runtime_locks || {};
    var reasons = Array.isArray(locks.reasons) ? locks.reasons : [];
    mount.innerHTML = '';
    if (!reasons.length) {
      var ok = document.createElement('div');
      ok.className = 'admin-lock-ok';
      ok.textContent = 'Broker switching is available';
      mount.appendChild(ok);
      return;
    }
    reasons.forEach(function (reason) {
      var item = document.createElement('div');
      item.className = 'admin-lock-reason';
      item.textContent = reason;
      mount.appendChild(item);
    });
  }

  function adminRender(data) {
    adminState = data || {};
    var settings = adminState.broker_settings || {};
    var brokerName = settings.current_broker || '--';
    adminSetText('admin-active-broker', String(brokerName).toUpperCase());
    adminSetText('admin-active-broker-note', settings.current_label || '--');
    adminSetText('admin-broker-configured', settings.configured ? 'Ready' : 'Missing');
    adminSetText('admin-broker-configured-note', settings.feed_kind || '--');
    var lockMessage = 'Stop active broker workflows before switching brokers.';
    if (typeof _brokerLockMessage === 'function') {
      lockMessage = _brokerLockMessage();
    }
    adminSetText('admin-switch-state', settings.switchable ? 'Open' : 'Locked');
    adminSetText('admin-switch-note', settings.switchable ? 'No active locks' : lockMessage);
    adminSetText('admin-env-state', adminState.env_writable ? 'Writable' : 'Read only');
    adminSetText('admin-env-path', adminState.env_path || '--');
    adminSetText('admin-console-updated', adminState.updated_at ? ('Runtime configuration - ' + adminState.updated_at) : 'Runtime configuration');

    adminRenderBrokerCards(settings);
    adminRenderLocks(settings);
    ['delta', 'coindcx', 'routing', 'security'].forEach(adminRenderFieldSection);
    adminSetPill('admin-delta-pill', adminSectionConfigured('delta'));
    adminSetPill('admin-coindcx-pill', adminSectionConfigured('coindcx'));
    var runtimePill = adminEl('admin-runtime-pill');
    if (runtimePill) runtimePill.dataset.state = adminField('CRYPTOFORGE_PIN').configured ? 'ok' : 'warn';
  }

  async function cfAdminLoad(silent) {
    try {
      var response = await cfApiFetch('/api/admin/config', { cache: 'no-store' });
      var data = await cfReadApiPayload(response);
      if (!response.ok || data.status === 'error') throw new Error(cfApiErrorDetail(data, 'Admin config unavailable'));
      adminRender(data);
      if (!silent) cfToast('Admin console refreshed', 'success');
      return data;
    } catch (error) {
      if (!silent) cfToast('Admin console failed: ' + error.message, 'danger');
      var mount = adminEl('admin-lock-reasons');
      if (mount) mount.textContent = error.message || 'Admin config unavailable';
      return null;
    }
  }

  function adminCollectPayload() {
    var values = {};
    var clearKeys = [];
    document.querySelectorAll('[data-admin-env-key]').forEach(function (input) {
      var key = input.dataset.adminEnvKey;
      if (!key) return;
      var isSecret = input.dataset.adminSecret === '1';
      if (isSecret) {
        var clear = document.querySelector('[data-admin-clear-key="' + key + '"]');
        if (clear && clear.checked) {
          clearKeys.push(key);
          return;
        }
        if (String(input.value || '').trim()) values[key] = input.value.trim();
        return;
      }
      values[key] = String(input.value || '').trim();
    });
    var activeSelect = adminEl('admin-active-broker-select');
    return {
      values: values,
      clear_keys: clearKeys,
      active_broker: activeSelect ? activeSelect.value : ''
    };
  }

  async function cfAdminSave() {
    var btn = adminEl('admin-save-btn');
    if (btn) btn.disabled = true;
    try {
      var response = await cfApiFetch('/api/admin/config', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(adminCollectPayload()),
        cache: 'no-store'
      });
      var data = await cfReadApiPayload(response);
      if (data.status === 'locked') {
        adminRender(data);
        cfToast(data.message || 'Environment update locked', 'warning');
        return;
      }
      if (!response.ok || data.status !== 'ok') {
        throw new Error(cfApiErrorDetail(data, 'Environment save failed'));
      }
      adminRender(data);
      if (typeof loadBrokerSettings === 'function') await loadBrokerSettings(true);
      if (typeof refreshBrokerState === 'function') await refreshBrokerState(true);
      if (typeof refreshTopbarTicker === 'function') refreshTopbarTicker();
      if (typeof loadDashboard === 'function') loadDashboard();
      cfToast(data.message || 'Environment settings saved', 'success');
    } catch (error) {
      cfToast('Environment save failed: ' + error.message, 'danger');
    } finally {
      if (btn) btn.disabled = false;
    }
  }

  async function cfAdminSwitchBroker() {
    var select = adminEl('admin-active-broker-select');
    if (!select || !select.value) {
      cfToast('Choose a broker first', 'warning');
      return;
    }
    await cfAdminSave();
  }

  var originalShowPage = window.showPage;
  if (typeof originalShowPage === 'function' && !window.__cfAdminShowPageWrapped) {
    window.__cfAdminShowPageWrapped = true;
    window.showPage = function (pageId, btn, options) {
      var result = originalShowPage.call(window, pageId, btn, options);
      if (pageId === 'admin-page') setTimeout(function () { cfAdminLoad(true); }, 0);
      return result;
    };
  }

  document.addEventListener('DOMContentLoaded', function () {
    if (document.getElementById('admin-page') && document.getElementById('admin-page').classList.contains('active-page')) {
      cfAdminLoad(true);
    }
  });

  window.cfAdminLoad = cfAdminLoad;
  window.cfAdminSave = cfAdminSave;
  window.cfAdminSwitchBroker = cfAdminSwitchBroker;
})();
