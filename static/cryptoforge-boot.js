(function () {
    var THEME_KEY = 'cf-theme';
    var THEME_META_SELECTOR = 'meta[name="theme-color"]';
    var THEME_COLORS = {
      dark: '#040814',
      light: '#f5f8fc'
    };

    var APPEARANCE_KEY = 'cf-appearance';
    var APPEARANCE_DEFAULT = { tint: 'aqua', font: 'terminal' };
    var APPEARANCE_TINTS = { aqua: true, gold: true, emerald: true, ruby: true, violet: true };
    var APPEARANCE_FONTS = {
      terminal: '',
      institutional: 'https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600;700&family=IBM+Plex+Sans:wght@400;500;600;700;800&display=swap',
      modern: 'https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=Space+Grotesk:wght@500;600;700&display=swap',
      quant: 'https://fonts.googleapis.com/css2?family=Exo+2:wght@400;500;600;700;800&family=Roboto+Mono:wght@400;500;600;700&display=swap',
      editorial: 'https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700;800&family=Fraunces:opsz,wght@9..144,600;9..144,700;9..144,800&family=IBM+Plex+Mono:wght@400;500;600;700&display=swap'
    };

    function normalizeAppearance(value) {
      var state = value || {};
      if (typeof state === 'string') state = { tint: state };
      var tint = APPEARANCE_TINTS[state.tint] ? state.tint : APPEARANCE_DEFAULT.tint;
      var font = Object.prototype.hasOwnProperty.call(APPEARANCE_FONTS, state.font) ? state.font : APPEARANCE_DEFAULT.font;
      return { tint: tint, font: font };
    }

    function getStoredAppearance() {
      try {
        var raw = localStorage.getItem(APPEARANCE_KEY);
        if (!raw) return normalizeAppearance();
        return normalizeAppearance(JSON.parse(raw));
      } catch (e) {
        return normalizeAppearance();
      }
    }

    function loadFontTheme(font) {
      var href = APPEARANCE_FONTS[font] || '';
      var existing = document.getElementById('cf-appearance-font');
      if (!href) return;
      if (existing && existing.getAttribute('href') === href) return;
      if (!existing) {
        existing = document.createElement('link');
        existing.id = 'cf-appearance-font';
        existing.rel = 'stylesheet';
        document.head.appendChild(existing);
      }
      existing.setAttribute('href', href);
    }

    function applyAppearance(next, options) {
      var opts = options || {};
      var current = getStoredAppearance();
      var incoming = next || {};
      if (typeof incoming === 'string') incoming = { tint: incoming };
      var state = normalizeAppearance({
        tint: incoming.tint || current.tint,
        font: incoming.font || current.font
      });
      var root = document.documentElement;
      root.setAttribute('data-tint', state.tint);
      root.setAttribute('data-font-theme', state.font);
      loadFontTheme(state.font);
      if (opts.persist) {
        try { localStorage.setItem(APPEARANCE_KEY, JSON.stringify(state)); } catch (e) {}
      }
      return state;
    }

    function prefersLightTheme() {
      try {
        return !!(window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches);
      } catch (e) {
        return false;
      }
    }

    function getStoredTheme() {
      try {
        var saved = localStorage.getItem(THEME_KEY);
        return saved === 'light' || saved === 'dark' ? saved : '';
      } catch (e) {
        return '';
      }
    }

    function resolveTheme(theme) {
      if (theme === 'light' || theme === 'dark') return theme;
      return prefersLightTheme() ? 'light' : 'dark';
    }

    function applyTheme(theme, options) {
      var opts = options || {};
      var resolved = resolveTheme(theme);
      var root = document.documentElement;
      var themeMeta = document.querySelector(THEME_META_SELECTOR);

      root.setAttribute('data-theme', resolved);
      root.style.colorScheme = resolved;
      if (themeMeta) themeMeta.setAttribute('content', THEME_COLORS[resolved] || THEME_COLORS.dark);

      if (opts.persist) {
        try {
          localStorage.setItem(THEME_KEY, resolved);
        } catch (e) {}
      }

      return resolved;
    }

    function toggleTheme() {
      var current = document.documentElement.getAttribute('data-theme');
      return applyTheme(current === 'light' ? 'dark' : 'light', { persist: true });
    }

    function bindSystemThemeWatcher() {
      if (!window.matchMedia) return;
      var media = window.matchMedia('(prefers-color-scheme: light)');
      var onChange = function () {
        if (!getStoredTheme()) applyTheme('', { persist: false });
      };
      try {
        media.addEventListener('change', onChange);
      } catch (e) {
        if (typeof media.addListener === 'function') media.addListener(onChange);
      }
    }

    window.cfGetStoredTheme = getStoredTheme;
    window.cfResolveTheme = resolveTheme;
    window.cfApplyTheme = applyTheme;
    window.cfToggleTheme = toggleTheme;
    window.cfGetAppearance = getStoredAppearance;
    window.cfApplyAppearance = applyAppearance;

    applyAppearance(getStoredAppearance(), { persist: false });
    applyTheme(getStoredTheme(), { persist: false });
    bindSystemThemeWatcher();

    function earlyFmtDt(raw) {
      if (!raw || raw === 'None') return '—';
      var s = String(raw).trim().replace('T', ' ');
      var full = s.match(/(\d{4})-(\d{2})-(\d{2}).*?(\d{2}:\d{2}:\d{2})/);
      if (full) {
        var months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        var monthIdx = Math.max(0, Math.min(11, parseInt(full[2], 10) - 1));
        return full[3] + ' ' + months[monthIdx] + ' ' + full[1] + ', ' + full[4] + ' IST';
      }
      var timeOnly = s.match(/(\d{2}:\d{2}:\d{2})/);
      if (timeOnly) return timeOnly[1] + ' IST';
      return s;
    }

    window.fmtDt = earlyFmtDt;
  })();
