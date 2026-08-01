  // The terminal used to live at "/", so old bookmarks and PWA shortcuts point
  // at /#cascade and friends. Hand those straight through to /app rather than
  // dropping someone on the marketing page.
  var APP_TABS = ['journal', 'portfolio', 'cascade', 'dashboard', 'scalp',
                  'live', 'builder', 'market', 'results'];
  var hash = (window.location.hash || '').replace('#', '');
  if (APP_TABS.indexOf(hash) !== -1) {
    window.location.replace('/app#' + hash);
  }

  document.documentElement.classList.remove('no-js');
  var io = new IntersectionObserver(function(entries) {
    entries.forEach(function(e) {
      if (e.isIntersecting) { e.target.classList.add('in-view'); io.unobserve(e.target); }
    });
  }, { threshold: 0.15 });
  document.querySelectorAll('.rv').forEach(function(el) { io.observe(el); });

  // Gentle parallax on the full-bleed chapter images.
  var bands = [].slice.call(document.querySelectorAll('.band-media img'));
  var reduce = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  if (bands.length && !reduce) {
    var ticking = false;
    var apply = function() {
      var vh = window.innerHeight;
      bands.forEach(function(img) {
        var r = img.parentElement.parentElement.getBoundingClientRect();
        if (r.bottom < -200 || r.top > vh + 200) return;
        var progress = (r.top + r.height / 2 - vh / 2) / vh;   // -1 … 1
        img.style.transform = 'scale(1.12) translateY(' + (progress * -4.5).toFixed(2) + '%)';
      });
      ticking = false;
    };
    window.addEventListener('scroll', function() {
      if (!ticking) { ticking = true; requestAnimationFrame(apply); }
    }, { passive: true });
    apply();
  }
