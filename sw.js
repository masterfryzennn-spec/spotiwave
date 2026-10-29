const CACHE_NAME = 'spotiwave-cache-v37';

self.addEventListener('install', (event) => {
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) => Promise.all(keys.map(k => caches.delete(k))))
  );
  self.clients.claim();
});

self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);

  // 1. NEVER intercept media streams or range requests with respondWith()!
  // Required so iOS Safari & Android WebAPK standalone PWAs stream natively in background!
  if (url.pathname.startsWith('/api/stream') || event.request.headers.get('range')) {
    return;
  }

  // 2. Always fetch fresh network content first for HTML, API and shared playlist routes
  if (url.pathname === '/' || url.pathname.endsWith('.html') || url.pathname.startsWith('/api/') || url.pathname.startsWith('/playlist/')) {
    event.respondWith(fetch(event.request));
    return;
  }

  // 3. Static assets fallback to cache
  event.respondWith(
    fetch(event.request).catch(() => caches.match(event.request))
  );
});
