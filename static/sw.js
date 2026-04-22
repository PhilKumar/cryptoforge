const CACHE_NAME = 'cryptoforge-shell-v13';
const APP_SHELL = [
  '/manifest.webmanifest?v=20260422-1',
  '/apple-touch-icon.png?v=20260422-1',
  '/static/pwa-icons/favicon-16.png?v=20260422-1',
  '/static/pwa-icons/favicon-32.png?v=20260422-1',
  '/static/pwa-icons/apple-touch-icon.png?v=20260422-1',
  '/static/pwa-icons/icon-192.png?v=20260422-1',
  '/static/pwa-icons/icon-512.png?v=20260422-1',
  '/static/pwa-icons/icon-maskable-192.png?v=20260422-1',
  '/static/pwa-icons/icon-maskable-512.png?v=20260422-1',
  '/static/cryptoforge-boot.js?v=20260417-1',
  '/static/cryptoforge-app.css?v=20260422-1',
  '/static/cryptoforge-app.js?v=20260422-1',
  '/static/cryptoforge-login.css?v=20260408-1',
  '/static/cryptoforge-login.js?v=20260408-1',
  '/static/cryptoforge-pwa.css?v=20260417-1',
  '/static/error-handler.js?v=20260408-1',
  '/static/pwa.js?v=20260417-1'
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
