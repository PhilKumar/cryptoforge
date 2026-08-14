// scene.js — the landing's living layer: motion in the chapter plates and a
// procedural, scroll-driven soundtrack.
//
// Everything here decorates the page; nothing depends on it. The plates are
// flat paintings, so the "life" is drawn over them — soft, additive, and
// deliberately loose, because object-fit:cover means we can never register
// an overlay to a pixel of the artwork and hard shapes would betray that.
//
// The audio is SYNTHESISED, not streamed. There is no track and no licence:
// a Web Audio graph builds each chapter's sound from oscillators and shaped
// noise, and the stems crossfade with scroll position. Browsers refuse to
// start audio without a user gesture, so the page boots silent and the
// SOUND button is the only way in. All styling lives in landing.css — the
// landing is served under a CSP that forbids inline style/script.
(function () {
  'use strict';

  var reduce = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  var scenes = [].slice.call(document.querySelectorAll('[data-scene]'));
  if (!scenes.length) return;

  /* ── Scene weights ─────────────────────────────────────────────────────
     How much of the viewport each chapter owns right now (0..1), smoothed,
     so both the audio mix and any scroll-reactive visual read one number. */
  var weight = {};   // smoothed
  var target = {};   // raw, recomputed on scroll
  scenes.forEach(function (el) { weight[el.dataset.scene] = 0; target[el.dataset.scene] = 0; });

  function measure() {
    var vh = window.innerHeight;
    Object.keys(target).forEach(function (k) { target[k] = 0; });
    scenes.forEach(function (el) {
      var r = el.getBoundingClientRect();
      var overlap = Math.min(r.bottom, vh) - Math.max(r.top, 0);
      var w = Math.max(0, Math.min(1, overlap / Math.min(vh, r.height || vh)));
      var k = el.dataset.scene;
      if (w > target[k]) target[k] = w;
    });
  }
  window.addEventListener('scroll', measure, { passive: true });
  window.addEventListener('resize', measure, { passive: true });
  measure();

  function smooth() {
    Object.keys(weight).forEach(function (k) {
      weight[k] += (target[k] - weight[k]) * 0.07;
    });
  }

  /* ── The living hero ───────────────────────────────────────────────────
     A candle tape that forms bar by bar over the SCREEN half of the plate —
     jade and vermilion bodies glowing and growing, loose enough that it
     reads as the painting's own chart alive. The parchment half gets no
     second chart (it already has one, painted); it gets a slow candlelight
     sheen instead, so both halves move without fighting the artwork. */
  var hero = document.querySelector('[data-scene="hero"]');
  var tape = null, g = null, candles = [], growing = 0;
  var heroVisible = true;   // flipped by an IntersectionObserver, never polled

  /* Glow is PRERENDERED. shadowBlur per candle per frame is what hung a real
     Mac: it is the most expensive thing a 2d canvas can do, and it ran 28
     times a frame on a retina-sized buffer. Each colour's halo is painted
     once into a small offscreen sprite; per frame we only drawImage-stretch
     it, which is close to free. The tape also renders at DPR 1 — it is a
     soft glow layer, not text — and at 24fps, only while the hero is on
     screen. */
  var glowSprite = {};
  function makeGlow(col) {
    var c = document.createElement('canvas');
    c.width = 64; c.height = 64;
    var x = c.getContext('2d');
    var grad = x.createRadialGradient(32, 32, 4, 32, 32, 30);
    grad.addColorStop(0, 'rgba(' + col + ',.55)');
    grad.addColorStop(0.55, 'rgba(' + col + ',.18)');
    grad.addColorStop(1, 'rgba(' + col + ',0)');
    x.fillStyle = grad;
    x.fillRect(0, 0, 64, 64);
    return c;
  }

  var momentum = 0;
  function newCandle(prev) {
    var o = prev ? prev.c : 46 + Math.random() * 10;
    // Momentum makes runs: a few green candles in a row, then a break —
    // the shape of a real tape, not white noise.
    momentum = momentum * 0.6 + (Math.random() - 0.47) * 11;
    var c = Math.max(14, Math.min(90, o + momentum));
    if (c === 14 || c === 90) momentum = -momentum * 0.5;    // bounce off the frame
    var h = Math.max(o, c) + Math.random() * 4.5;
    var l = Math.min(o, c) - Math.random() * 4.5;
    return { o: o, h: h, l: l, c: c, t: 0 };                 // t: growth 0..1
  }

  function initTape() {
    tape = document.createElement('canvas');
    tape.className = 'scene-tape';
    tape.setAttribute('aria-hidden', 'true');
    hero.appendChild(tape);
    g = tape.getContext('2d');
    glowSprite.up = makeGlow('46,158,107');
    glowSprite.dn = makeGlow('194,69,45');
    new IntersectionObserver(function (entries) {
      heroVisible = entries[0].isIntersecting;
    }).observe(hero);
    window.addEventListener('resize', function () { lastSize = [0, 0]; }, { passive: true });
    for (var i = 0; i < 14; i++) {
      var cd = newCandle(candles[candles.length - 1]);
      cd.t = 1;
      candles.push(cd);
    }
    candles[candles.length - 1].t = 0;                        // last one grows in
  }

  function fitTape() {
    var r = tape.getBoundingClientRect();                     // CSS box, never the attribute
    tape.width = r.width;                                     // DPR 1, deliberately
    tape.height = r.height;
    g.setTransform(1, 0, 0, 1, 0, 0);
    return [r.width, r.height];
  }

  function drawCluster(w, h, x0, x1, y0, y1, pulse, alpha) {
    var n = candles.length;
    var span = (x1 - x0) * w;
    var cw = span / n;
    var bw = Math.max(6, cw * 0.42);
    for (var i = 0; i < n; i++) {
      var cd = candles[i];
      var t = cd.t;
      var x = x0 * w + i * cw + cw / 2;
      var yy = function (v) { return y0 * h + (1 - v / 100) * (y1 - y0) * h; };
      var oY = yy(cd.o);
      var cY = oY + (yy(cd.c) - oY) * t;                      // body grows to close
      var hY = oY + (yy(cd.h) - oY) * t;
      var lY = oY + (yy(cd.l) - oY) * t;
      var up = cd.c >= cd.o;
      // Jade up, vermilion down; the halo is the prerendered sprite.
      var col = up ? '46,158,107' : '194,69,45';
      var top = Math.min(oY, cY), bh = Math.max(3, Math.abs(cY - oY));
      var halo = (1.6 + pulse) * bw;
      g.globalAlpha = alpha * (0.5 + 0.4 * pulse);
      g.drawImage(glowSprite[up ? 'up' : 'dn'],
                  x - halo, top + bh / 2 - halo, halo * 2, halo * 2);
      g.globalAlpha = alpha;
      g.strokeStyle = 'rgba(' + col + ',' + (0.5 + 0.3 * t) + ')';
      g.fillStyle = 'rgba(' + col + ',' + (0.30 + 0.22 * pulse) + ')';
      g.lineWidth = 1.4;
      g.beginPath(); g.moveTo(x, hY); g.lineTo(x, lY); g.stroke();
      g.fillRect(x - bw / 2, top, bw, bh);
      g.strokeRect(x - bw / 2, top, bw, bh);
    }
  }

  var lastSize = [0, 0];
  var lastDraw = 0;
  function drawTape(now) {
    if (!heroVisible) return;
    if (now - lastDraw < 41) return;                          // ~24fps is plenty for a glow
    lastDraw = now;
    if (!lastSize[0]) lastSize = fitTape();                   // refit only after resize
    var w = lastSize[0], h = lastSize[1];
    g.clearRect(0, 0, w, h);
    var vis = weight.hero || 0;
    if (vis < 0.02) return;

    var grow = candles[candles.length - 1];
    grow.t = Math.min(1, grow.t + 0.014);
    if (grow.t >= 1) {
      growing += 1;
      candles.push(newCandle(grow));
      if (candles.length > 14) candles.shift();
    }
    var pulse = 0.5 + 0.5 * Math.sin(now / 1400);
    drawCluster(w, h, 0.565, 0.935, 0.20, 0.62, pulse, 0.42 + 0.3 * vis);
    g.globalAlpha = 1;
  }

  /* ── Chapter dressing: lanterns, haze, ink, chalk, breath ─────────────
     All elements are created here and styled entirely from landing.css. */
  function el(tag, cls, parent, text) {
    var n = document.createElement(tag);
    n.className = cls;
    if (text) n.textContent = text;
    n.setAttribute('aria-hidden', 'true');
    parent.appendChild(n);
    return n;
  }

  function dressPort(section) {
    var media = section.querySelector('.band-media');
    if (!media) return;
    el('div', 'scene-lantern a', media);
    el('div', 'scene-lantern b', media);
    el('div', 'scene-haze', media);
  }

  function dressMerchant(section) {
    var visual = section.querySelector('.split-visual');
    if (!visual) return;
    // Columns of tally marks being brushed onto the board, over and over.
    // pathLength=100 makes the dash arithmetic uniform for every stroke.
    var svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('class', 'scene-ink');
    svg.setAttribute('viewBox', '0 0 100 140');
    svg.setAttribute('aria-hidden', 'true');
    var strokes = [
      'M 18 12 q 2 30 -1 54',        // long column
      'M 12 26 h 13',                // tick
      'M 44 10 q -2 26 1 46',        // second column
      'M 38 40 h 13',                // tick
      'M 70 16 q 2 34 -2 62',        // third column
      'M 64 58 h 13',                // tick
    ];
    strokes.forEach(function (d, i) {
      var p = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      p.setAttribute('d', d);
      p.setAttribute('pathLength', '100');
      p.setAttribute('class', 'scene-ink-stroke s' + i);
      svg.appendChild(p);
    });
    visual.appendChild(svg);
  }

  function dressLedger(section) {
    var media = section.querySelector('.band-media');
    if (!media) return;
    el('div', 'scene-flame', media);
    var board = el('div', 'scene-chalk', media);
    ['146', '151', '149', '155'].forEach(function (n, i) {
      el('span', 'c' + i, board, n);
    });
  }

  function dressShape(section) {
    var frame = section.querySelector('.shape-frame');
    if (frame) frame.classList.add('scene-breathe');
  }

  /* ── The soundtrack ────────────────────────────────────────────────────
     One AudioContext, one master bus, five stems whose gains follow the
     scene weights. Nothing plays until the SOUND button is pressed. */
  var audio = null;

  function buildAudio() {
    var Ctx = window.AudioContext || window.webkitAudioContext;
    if (!Ctx) return null;
    var ctx = new Ctx();
    var master = ctx.createGain(); master.gain.value = 0;
    var comp = ctx.createDynamicsCompressor();
    comp.threshold.value = -30; comp.ratio.value = 8;
    var analyser = ctx.createAnalyser(); analyser.fftSize = 512;
    master.connect(comp); comp.connect(analyser); analyser.connect(ctx.destination);

    function stem() { var s = ctx.createGain(); s.gain.value = 0; s.connect(master); return s; }
    var stems = {
      warm: stem(),     // the room: two detuned low voices, felt not heard
      murmur: stem(),   // the port: water and a crowd, shaped noise
      swell: stem(),    // the shape: a slow rising pad
      cold: stem(),     // the terminal: a colder drone and a soft tick
    };

    // Warm drone — D2 + A2 triangles through a dark lowpass.
    var lpWarm = ctx.createBiquadFilter(); lpWarm.type = 'lowpass'; lpWarm.frequency.value = 240;
    [73.42, 110.0].forEach(function (f, i) {
      var o = ctx.createOscillator(); o.type = 'triangle';
      o.frequency.value = f; o.detune.value = i ? 4 : -3;
      var og = ctx.createGain(); og.gain.value = 0.5;
      o.connect(og); og.connect(lpWarm); o.start();
    });
    lpWarm.connect(stems.warm);

    // Murmur — looped noise through a talking-range bandpass, slowly wobbled.
    var len = ctx.sampleRate * 2, buf = ctx.createBuffer(1, len, ctx.sampleRate);
    var data = buf.getChannelData(0), v = 0;
    for (var i = 0; i < len; i++) { v = v * 0.97 + (Math.random() * 2 - 1) * 0.24; data[i] = v; }
    var noise = ctx.createBufferSource(); noise.buffer = buf; noise.loop = true;
    var bp = ctx.createBiquadFilter(); bp.type = 'bandpass'; bp.frequency.value = 460; bp.Q.value = 0.8;
    var wob = ctx.createGain(); wob.gain.value = 0.6;
    var lfo = ctx.createOscillator(); lfo.frequency.value = 0.13;
    var lfoAmt = ctx.createGain(); lfoAmt.gain.value = 0.35;
    lfo.connect(lfoAmt); lfoAmt.connect(wob.gain);
    noise.connect(bp); bp.connect(wob); wob.connect(stems.murmur);
    noise.start(); lfo.start();

    // Swell — A3 + E4 sines that breathe on an 8-second cycle.
    var swellIn = ctx.createGain(); swellIn.gain.value = 0.4;
    var breathe = ctx.createOscillator(); breathe.frequency.value = 1 / 8;
    var breatheAmt = ctx.createGain(); breatheAmt.gain.value = 0.25;
    breathe.connect(breatheAmt); breatheAmt.connect(swellIn.gain); breathe.start();
    [220.0, 329.63].forEach(function (f) {
      var o = ctx.createOscillator(); o.frequency.value = f;
      var og = ctx.createGain(); og.gain.value = 0.3;
      o.connect(og); og.connect(swellIn); o.start();
    });
    swellIn.connect(stems.swell);

    // Cold drone — the same root an octave up, square through a tight lowpass.
    var lpCold = ctx.createBiquadFilter(); lpCold.type = 'lowpass'; lpCold.frequency.value = 500;
    var oc = ctx.createOscillator(); oc.type = 'square'; oc.frequency.value = 146.83; // D3
    var ocg = ctx.createGain(); ocg.gain.value = 0.12;
    oc.connect(ocg); ocg.connect(lpCold); lpCold.connect(stems.cold); oc.start();

    // Koto pluck — a struck voice on the hirajoshi scale, scheduled sparsely
    // while the merchant and ledger chapters hold the viewport.
    var SCALE = [293.66, 311.13, 392.0, 440.0, 466.16, 587.33];
    function pluck(when, freq, loud) {
      var o = ctx.createOscillator(); o.type = 'triangle'; o.frequency.value = freq;
      var body = ctx.createBiquadFilter(); body.type = 'bandpass';
      body.frequency.value = freq * 2; body.Q.value = 6;
      var e = ctx.createGain();
      e.gain.setValueAtTime(0.0001, when);
      e.gain.exponentialRampToValueAtTime(loud, when + 0.012);
      e.gain.exponentialRampToValueAtTime(0.0001, when + 1.4);
      o.connect(body); body.connect(e); e.connect(master);
      o.start(when); o.stop(when + 1.6);
    }
    // Terminal tick — a filtered click, metronomic and quiet.
    function tick(when, loud) {
      var o = ctx.createOscillator(); o.type = 'sine'; o.frequency.value = 1180;
      var e = ctx.createGain();
      e.gain.setValueAtTime(0.0001, when);
      e.gain.exponentialRampToValueAtTime(loud, when + 0.004);
      e.gain.exponentialRampToValueAtTime(0.0001, when + 0.09);
      o.connect(e); e.connect(master);
      o.start(when); o.stop(when + 0.12);
    }

    var nextTick = 0;
    var scheduler = setInterval(function () {
      if (ctx.state !== 'running') return;
      var wInk = Math.max(weight.merchant || 0, weight.ledger || 0);
      if (wInk > 0.25 && Math.random() < wInk * 0.5) {
        pluck(ctx.currentTime + Math.random() * 0.4,
              SCALE[Math.floor(Math.random() * SCALE.length)],
              0.05 + 0.06 * wInk);
      }
      var wCold = weight.terminal || 0;
      if (wCold > 0.3 && ctx.currentTime >= nextTick) {
        tick(ctx.currentTime + 0.05, 0.02 + 0.025 * wCold);
        nextTick = ctx.currentTime + 0.75;
      }
    }, 400);

    return {
      ctx: ctx, master: master, stems: stems, analyser: analyser,
      stop: function () { clearInterval(scheduler); ctx.close(); },
    };
  }

  function mixAudio() {
    if (!audio || audio.ctx.state !== 'running') return;
    var wCold = weight.terminal || 0;
    audio.stems.warm.gain.value = 0.055 * (1 - wCold * 0.75);
    audio.stems.murmur.gain.value = 0.05 * (weight.port || 0);
    audio.stems.swell.gain.value = 0.05 * (weight.shape || 0);
    audio.stems.cold.gain.value = 0.05 * wCold;
  }

  /* ── The SOUND button — the only way audio starts ─────────────────────── */
  var btn = document.createElement('button');
  btn.type = 'button';
  btn.className = 'scene-sound';
  btn.setAttribute('aria-pressed', 'false');
  btn.textContent = 'Sound off';
  document.body.appendChild(btn);

  btn.addEventListener('click', function () {
    var on = btn.getAttribute('aria-pressed') !== 'true';
    if (on && !audio) audio = buildAudio();
    if (!audio) { btn.hidden = true; return; }               // no Web Audio here
    if (on) {
      audio.ctx.resume();
      audio.master.gain.setTargetAtTime(0.6, audio.ctx.currentTime, 0.6);
    } else {
      audio.master.gain.setTargetAtTime(0, audio.ctx.currentTime, 0.25);
      setTimeout(function () { if (audio) audio.ctx.suspend(); }, 900);
    }
    btn.setAttribute('aria-pressed', String(on));
    btn.textContent = on ? 'Sound on' : 'Sound off';
  });

  // A tiny debug handle so a test can prove sound without ears.
  window.__scene = { weight: weight, audio: function () { return audio; } };

  /* ── Boot ──────────────────────────────────────────────────────────────── */
  scenes.forEach(function (s) {
    if (reduce) return;                                       // stills stay stills
    switch (s.dataset.scene) {
      case 'port': dressPort(s); break;
      case 'merchant': dressMerchant(s); break;
      case 'ledger': dressLedger(s); break;
      case 'shape': dressShape(s); break;
    }
  });
  if (hero && !reduce) {
    initTape();
    el('div', 'scene-sheen', hero);                          // candlelight over the parchment
  }

  /* The killswitch. This layer hung a real machine once; it never gets to
     do that again. After a warmup (image decode makes early frames lie), a
     WALL-CLOCK watchdog measures achieved fps: two consecutive 2.5-second
     windows under 15fps and the whole visual layer tears itself down —
     canvas, overlays, CSS animations — leaving the stills and the sound
     button, because audio is cheap. Wall-clock, not frame-count: on the
     machine that needs this, frames arrive so slowly that a frame-counted
     window never completes, which is exactly how the first version of this
     watchdog failed its own test. Deltas over 900ms are treated as a hidden
     tab or a sleeping machine and reset the window instead of counting. */
  var born = 0, lastFrame = 0, winStart = 0, winFrames = 0, strikes = 0, defused = false;
  function defuse() {
    defused = true;
    document.documentElement.classList.add('scene-off');
    if (tape && tape.parentNode) tape.parentNode.removeChild(tape);
    tape = null;
    [].forEach.call(
      document.querySelectorAll('.scene-lantern, .scene-haze, .scene-sheen, .scene-ink, .scene-flame, .scene-chalk'),
      function (n) { n.parentNode.removeChild(n); });
    [].forEach.call(document.querySelectorAll('.scene-breathe'),
      function (n) { n.classList.remove('scene-breathe'); });
  }

  function frame(now) {
    if (!born) { born = now; winStart = now; }
    if (lastFrame && now - born > 3000 && !defused) {         // warmup over
      if (now - lastFrame > 900) {                            // hidden tab / sleep, not jank
        winStart = now; winFrames = 0;
      } else {
        winFrames += 1;
        var span = now - winStart;
        if (span >= 2500) {
          var fps = winFrames / (span / 1000);
          strikes = fps < 15 ? strikes + 1 : 0;
          if (strikes >= 2) defuse();
          winStart = now; winFrames = 0;
        }
      }
    }
    lastFrame = now;
    smooth();
    if (!defused && tape) drawTape(now);
    mixAudio();
    requestAnimationFrame(frame);
  }
  requestAnimationFrame(frame);
})();
