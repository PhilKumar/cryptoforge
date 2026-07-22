const CACHE_NAME = 'cryptoforge-shell-v84';

// Scripts and stylesheets are requested with a content-hash ?v= token that the
// server computes at render time, so their URLs cannot be known here — listing
// guessed versions only precached bytes the page would never ask for. They are
// picked up by the runtime cache below on first load instead, and a changed
// file arrives under a new URL that no cache entry can shadow. Only assets
// referenced with a stable URL belong in the precache.
const APP_SHELL = [
  '/manifest.webmanifest',
  '/apple-touch-icon.png',
  '/static/pwa-icons/favicon-16.png',
  '/static/pwa-icons/favicon-32.png',
  '/static/pwa-icons/apple-touch-icon.png',
  '/static/pwa-icons/icon-192.png',
  '/static/pwa-icons/icon-512.png',
  '/static/pwa-icons/icon-maskable-192.png',
  '/static/pwa-icons/icon-maskable-512.png'
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(APP_SHELL)).catch(() => undefined)
  );
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key)))
    )
  );
  self.clients.claim();
});

self.addEventListener('fetch', (event) => {
  const { request } = event;
  if (request.method !== 'GET') return;

  const url = new URL(request.url);
  if (url.origin !== self.location.origin) return;
  if (url.pathname.startsWith('/api/')) return;
  if (request.mode === 'navigate') return;

  const cacheable =
    url.pathname.startsWith('/static/') ||
    url.pathname === '/manifest.webmanifest' ||
    url.pathname === '/site.webmanifest' ||
    url.pathname === '/apple-touch-icon.png';

  if (!cacheable) return;

  event.respondWith(
    caches.match(request).then((cached) => {
      const networkFetch = fetch(request)
        .then((response) => {
          if (response && response.status === 200) {
            const clone = response.clone();
            caches.open(CACHE_NAME).then((cache) => cache.put(request, clone)).catch(() => undefined);
          }
          return response;
        })
        .catch(() => cached);
      return cached || networkFetch;
    })
  );
});
