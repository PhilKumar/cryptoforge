"""
executor/pwa.py — what makes the buyer's console installable.

A manifest, a service worker, and icons, all generated here rather than
shipped as files. The executor is a package a buyer unzips and runs; every
binary asset in it is one more thing to go missing, and an icon that 404s
turns "Install" into a grey square.

The icons are written as PNG bytes by hand because this package must keep its
four dependencies. Pillow for a 512-pixel square of three bars would be a
20 MB dependency for a logo.

**Scope note.** This makes the console installable on the machine the
executor RUNS on: its own window, its own icon, no browser chrome. It cannot
put the console on a phone, because the server binds to loopback only — a
deliberate boundary, not an oversight (see `UIServer`). A phone would need it
bound to the LAN and authenticated, which is a different decision.
"""

from __future__ import annotations

import json
import struct
import zlib

# The parent's palette, so an installed buyer console does not look like a
# different product from the site they bought it on.
INK = (0x07, 0x09, 0x0F)
BARS = ((0x5B, 0x9B, 0xD5), (0xF4, 0x72, 0xB6), (0xF5, 0xA6, 0x23))

APP_NAME = "Cascade Executor"
SHORT_NAME = "Cascade"


def manifest(port: int) -> bytes:
    """The install descriptor.

    `start_url` is relative on purpose: the buyer may run the UI on another
    port with `--ui-port`, and an absolute URL baked at build time would open
    an installed window pointing at a port nothing is listening on.
    """
    document = {
        "id": "/",
        "name": APP_NAME,
        "short_name": SHORT_NAME,
        "description": "Follows CryptoForge Cascade signals and places your own orders, from your own machine.",
        "start_url": ".",
        "scope": ".",
        "display": "standalone",
        "background_color": "#07090f",
        "theme_color": "#07090f",
        "icons": [
            {"src": "icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "any"},
            {"src": "icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any"},
            {"src": "icon-maskable-512.png", "sizes": "512x512", "type": "image/png", "purpose": "maskable"},
        ],
    }
    return json.dumps(document, indent=2).encode("utf-8")


# Network-first, with the shell as the fallback. A cache-first worker on a
# trading console is a way to show a buyer yesterday's positions: everything
# on this page is live, and stale is worse than absent. The cache exists only
# so an installed window opens to something rather than a browser error while
# the executor is starting.
SERVICE_WORKER = """
const SHELL = 'cascade-shell-v1';

self.addEventListener('install', event => {
  event.waitUntil(caches.open(SHELL).then(c => c.addAll(['./'])).then(() => self.skipWaiting()));
});

self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys()
      .then(names => Promise.all(names.filter(n => n !== SHELL).map(n => caches.delete(n))))
      .then(() => self.clients.claim()));
});

self.addEventListener('fetch', event => {
  const request = event.request;
  if (request.method !== 'GET') return;
  const url = new URL(request.url);
  /* Never the state, the chart or an action: those are the live picture, and
     a cached answer would be a lie about money. */
  if (url.pathname.startsWith('/api/')) return;
  event.respondWith(
    fetch(request)
      .then(response => {
        if (response && response.ok && url.origin === self.location.origin) {
          const copy = response.clone();
          caches.open(SHELL).then(c => c.put(request, copy));
        }
        return response;
      })
      .catch(() => caches.match(request).then(hit => hit || caches.match('./'))));
});
""".strip()


def service_worker() -> bytes:
    return SERVICE_WORKER.encode("utf-8")


def _png(width: int, height: int, pixels: bytes) -> bytes:
    """Minimal RGB PNG. `pixels` is width*height*3 bytes, row-major."""

    def chunk(kind: bytes, payload: bytes) -> bytes:
        body = kind + payload
        return struct.pack(">I", len(payload)) + body + struct.pack(">I", zlib.crc32(body) & 0xFFFFFFFF)

    # Each scanline carries a leading filter byte; 0 means "no filter", which
    # costs a little size and removes every chance of an encoder bug here.
    raw = b"".join(b"\x00" + pixels[y * width * 3 : (y + 1) * width * 3] for y in range(height))
    header = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", header) + chunk(b"IDAT", zlib.compress(raw, 9)) + chunk(b"IEND", b"")


def icon(size: int, *, maskable: bool = False) -> bytes:
    """Three ascending bars on the parent's ink, as a square PNG.

    Maskable icons are drawn smaller and centred: the platform crops them to
    whatever shape it likes, and a mark that fills the square loses its edges
    to a circle mask on Android.
    """
    inset = 0.30 if maskable else 0.22
    field = size * (1 - inset * 2)
    left = (size - field) / 2
    bar_w = field / 5.0
    gap = bar_w / 2.0
    # Ascending, left to right — the same rising shape as the site's logo.
    heights = (field * 0.45, field * 0.72, field * 1.0)
    bars = []
    for index, height in enumerate(heights):
        x0 = left + index * (bar_w + gap)
        y1 = left + field
        bars.append((x0, y1 - height, x0 + bar_w, y1, BARS[index]))

    row_cache: dict = {}
    rows = []
    for y in range(size):
        key = tuple(1 if b[1] <= y < b[3] else 0 for b in bars)
        if key not in row_cache:
            row = bytearray()
            for x in range(size):
                colour = INK
                for bar in bars:
                    if bar[0] <= x < bar[2] and bar[1] <= y < bar[3]:
                        colour = bar[4]
                        break
                row += bytes(colour)
            row_cache[key] = bytes(row)
        rows.append(row_cache[key])
    return _png(size, size, b"".join(rows))
