(function () {
  const INSTALL_SELECTOR = '[data-install-app]';
  let deferredPrompt = null;
  let registrationReady = false;
  let installDialog = null;

  function isStandalone() {
    return window.matchMedia('(display-mode: standalone)').matches || window.navigator.standalone === true;
  }

  function isIosSafari() {
    const ua = window.navigator.userAgent || '';
    const isIOS = /iphone|ipad|ipod/i.test(ua);
    const isSafari = /safari/i.test(ua) && !/crios|fxios|edgios/i.test(ua);
    return isIOS && isSafari;
  }

  function isChromiumLike() {
    const ua = window.navigator.userAgent || '';
    return /chrome|chromium|edg/i.test(ua) && !/opr|opera|fxios|firefox/i.test(ua);
  }

  function hasServiceWorkerControl() {
    return !!(navigator.serviceWorker && navigator.serviceWorker.controller);
  }

  function buttons() {
    return Array.from(document.querySelectorAll(INSTALL_SELECTOR));
  }

  function installMode() {
    if (isStandalone()) return 'installed';
    if (deferredPrompt) return 'prompt';
    if (isIosSafari()) return 'ios';
    return 'manual';
  }

  function manualInstallMessage() {
    return 'Install is available in supported browsers once CryptoForge is considered installable. Use Chrome or Edge over HTTPS, refresh once, then use the browser menu and choose "Install App".';
  }

  function installDialogMarkup() {
    return (
      '<div class="cf-pwa-overlay" hidden>' +
        '<div class="cf-pwa-sheet" role="dialog" aria-modal="true" aria-labelledby="cf-pwa-title">' +
          '<div class="cf-pwa-sheet-top">' +
            '<div class="cf-pwa-sheet-copy">' +
              '<div class="cf-pwa-kicker">CryptoForge App Install</div>' +
              '<h3 id="cf-pwa-title">Install guidance</h3>' +
            '</div>' +
            '<button type="button" class="cf-pwa-close" aria-label="Close install guidance">×</button>' +
          '</div>' +
          '<p class="cf-pwa-message"></p>' +
          '<div class="cf-pwa-points"></div>' +
          '<div class="cf-pwa-actions">' +
            '<button type="button" class="cf-pwa-btn" data-pwa-action="secondary" hidden></button>' +
            '<button type="button" class="cf-pwa-btn cf-pwa-btn-primary" data-pwa-action="primary">OK</button>' +
          '</div>' +
        '</div>' +
      '</div>'
    );
  }

  function handleInstallDialogKeydown(event) {
    if (event.key === 'Escape') closeInstallDialog();
  }

  function ensureInstallDialog() {
    if (installDialog) return installDialog;
    const mount = document.createElement('div');
    mount.innerHTML = installDialogMarkup();
    installDialog = mount.firstElementChild;
    document.body.appendChild(installDialog);
    return installDialog;
  }

  function closeInstallDialog() {
    if (!installDialog) return;
    document.removeEventListener('keydown', handleInstallDialogKeydown);
    installDialog.remove();
    installDialog = null;
  }

  function showInstallDialog(config) {
    const {
      title,
      message,
      points = [],
      primaryLabel = 'OK',
      secondaryLabel = '',
      onPrimary = null,
      onSecondary = null,
    } = config;
    closeInstallDialog();
    const dialog = ensureInstallDialog();
    dialog.hidden = false;
    dialog.querySelector('#cf-pwa-title').textContent = title;
    dialog.querySelector('.cf-pwa-message').textContent = message;
    const pointsEl = dialog.querySelector('.cf-pwa-points');
    pointsEl.innerHTML = points.map((point) => '<div class="cf-pwa-point">' + point + '</div>').join('');
    const closeBtn = dialog.querySelector('.cf-pwa-close');
    const primary = dialog.querySelector('[data-pwa-action="primary"]');
    const secondary = dialog.querySelector('[data-pwa-action="secondary"]');
    const close = () => closeInstallDialog();

    closeBtn.addEventListener('click', (event) => {
      event.preventDefault();
      close();
    });
    dialog.addEventListener('click', (event) => {
      if (event.target === dialog) close();
    });
    primary.textContent = primaryLabel;
    primary.addEventListener('click', (event) => {
      event.preventDefault();
      close();
      if (typeof onPrimary === 'function') onPrimary();
    });
    if (secondaryLabel) {
      secondary.hidden = false;
      secondary.textContent = secondaryLabel;
      secondary.addEventListener('click', (event) => {
        event.preventDefault();
        close();
        if (typeof onSecondary === 'function') onSecondary();
      });
    } else {
      secondary.hidden = true;
    }
    document.addEventListener('keydown', handleInstallDialogKeydown);
  }

  function syncButtons() {
    const mode = installMode();
    buttons().forEach((btn) => {
      btn.hidden = mode === 'installed';
      btn.disabled = false;
      btn.dataset.installMode = mode;
      if (mode === 'prompt') {
        btn.title = 'Install App';
        btn.setAttribute('aria-label', 'Install App');
      } else if (mode === 'ios') {
        btn.title = 'Add to Home Screen';
        btn.setAttribute('aria-label', 'Add to Home Screen');
      } else if (mode === 'manual') {
        btn.title = registrationReady ? 'Install Guide' : 'Install App (Preparing install support)';
        btn.setAttribute('aria-label', 'Install App');
      }
    });
  }

  async function openInstallPrompt() {
    if (isStandalone()) return;
    if (deferredPrompt) {
      deferredPrompt.prompt();
      try {
        await deferredPrompt.userChoice;
      } catch (error) {}
      deferredPrompt = null;
      syncButtons();
      return;
    }
    if (isIosSafari()) {
      showInstallDialog({
        title: 'Add CryptoForge to Home Screen',
        message: 'iPhone does not show the same install prompt as desktop Chrome. Use Safari’s share menu instead.',
        points: [
          'Tap the Share button in Safari.',
          'Choose "Add to Home Screen".',
          'Open CryptoForge from the new icon for the app-style full-screen view.',
        ],
      });
      return;
    }
    if (!registrationReady) {
      showInstallDialog({
        title: 'Install shell is still preparing',
        message: 'CryptoForge is still registering its app shell. Wait a moment, then retry the install button.',
        points: [
          'Keep this tab open for a few seconds.',
          'Refresh once if the install button still shows guidance only.',
          'Chrome or Edge over HTTPS gives the most reliable desktop install prompt.',
        ],
      });
      return;
    }
    if (!hasServiceWorkerControl()) {
      showInstallDialog({
        title: 'Reload once to finish install setup',
        message: 'CryptoForge has registered its app shell, but this tab is not yet controlled by the service worker.',
        points: [
          'Reload this page once.',
          'Then retry the install button or use the install icon in the address bar.',
          'If the prompt still does not appear, open the browser menu and choose "Install App".',
        ],
        primaryLabel: 'Reload Now',
        secondaryLabel: 'Close',
        onPrimary: () => window.location.reload(),
      });
      return;
    }
    const chromiumSpecific = isChromiumLike();
    showInstallDialog({
      title: chromiumSpecific ? 'Chrome has not exposed the install prompt yet' : 'Install guidance',
      message: chromiumSpecific
        ? 'You are in a Chromium browser, but the browser has not surfaced the native install prompt for this page yet.'
        : manualInstallMessage(),
      points: chromiumSpecific
        ? [
            'Look for the install icon in the address bar.',
            'Or open the browser menu and choose "Install App".',
            'If this is your first visit after the update, refresh once and try again.',
          ]
        : [
            'Use Chrome or Edge over HTTPS.',
            'Refresh once after the app shell is installed.',
            'Then use the browser menu and choose "Install App".',
          ],
      primaryLabel: chromiumSpecific ? 'Reload Now' : 'OK',
      secondaryLabel: chromiumSpecific ? 'Close' : '',
      onPrimary: chromiumSpecific ? () => window.location.reload() : null,
    });
  }

  function bindInstallButtons() {
    buttons().forEach((btn) => {
      if (btn.dataset.installBound === '1') return;
      btn.dataset.installBound = '1';
      btn.addEventListener('click', openInstallPrompt);
    });
  }

  window.addEventListener('beforeinstallprompt', (event) => {
    event.preventDefault();
    deferredPrompt = event;
    bindInstallButtons();
    syncButtons();
  });

  window.addEventListener('appinstalled', () => {
    deferredPrompt = null;
    syncButtons();
  });

  if ('serviceWorker' in navigator) {
    window.addEventListener('load', () => {
      navigator.serviceWorker.register('/sw.js').then(() => {
        registrationReady = true;
        syncButtons();
      }).catch(() => {
        registrationReady = false;
        syncButtons();
      });
    });
  }

  window.CryptoForgePWA = {
    openInstallPrompt,
    syncButtons,
    getInstallState() {
      return {
        mode: installMode(),
        hasPrompt: !!deferredPrompt,
        registrationReady,
        standalone: isStandalone(),
        iosSafari: isIosSafari(),
      };
    },
  };

  bindInstallButtons();
  syncButtons();
})();
