(function () {
    var THEME_KEY = 'cf-theme';
    var THEME_META_SELECTOR = 'meta[name="theme-color"]';
    var THEME_COLORS = {
      dark: '#07090f',
      light: '#f8fafc'
    };

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
