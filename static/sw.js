/*
 * MakoletChain service worker.
 *
 * CRITICAL: this worker MUST NEVER cache /api/* responses — the app shows
 * live financial data and stale revenue numbers would be catastrophic.
 *  - /api/*  →  network-only, no cache read, no cache write, no offline fallback.
 *  - navigations (HTML)  →  network-first, fall back to cached shell on failure.
 *  - /static/* (css/js/icons/manifest)  →  cache-first (versioned via static_v).
 */

const CACHE_NAME = 'makolet-shell-v1';

// Static shell — only stable assets. Versioned URLs are added at runtime.
const SHELL_ASSETS = [
  '/static/css/style.css',
  '/static/dialog.js',
  '/static/manifest.json',
  '/static/icons/icon-192.png',
  '/static/icons/icon-512.png',
  '/static/icons/icon-512-maskable.png',
  '/static/icons/icon-180.png',
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => {
      // Each addAll item is fetched without credentials — fine for /static/*.
      return Promise.all(
        SHELL_ASSETS.map((url) =>
          fetch(url, { cache: 'reload' })
            .then((resp) => (resp.ok ? cache.put(url, resp) : null))
            .catch(() => null)
        )
      );
    }).then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k))
      )
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  const req = event.request;

  // Only handle GET. POST/PUT/DELETE pass straight through to the network.
  if (req.method !== 'GET') return;

  const url = new URL(req.url);

  // Same-origin only — never touch cross-origin requests.
  if (url.origin !== self.location.origin) return;

  // ---- RULE 1 (NON-NEGOTIABLE): /api/* is network-only. ----
  // No cache read, no cache write, no offline fallback. Financial data must
  // always be live. If the network fails, the request fails — the UI handles it.
  if (url.pathname.startsWith('/api/') || url.pathname.includes('/api/')) {
    event.respondWith(fetch(req));
    return;
  }

  // ---- RULE 2: static assets — cache-first. ----
  // Versioned via ?v=<mtime>, so the URL itself changes on deploy.
  if (url.pathname.startsWith('/static/')) {
    event.respondWith(
      caches.match(req, { ignoreSearch: false }).then((cached) => {
        if (cached) return cached;
        return fetch(req).then((resp) => {
          if (resp && resp.ok && resp.type === 'basic') {
            const copy = resp.clone();
            caches.open(CACHE_NAME).then((c) => c.put(req, copy));
          }
          return resp;
        });
      })
    );
    return;
  }

  // ---- RULE 3: navigations (HTML pages) — network-first. ----
  // Fall back to a cached shell ONLY if the network totally fails (offline).
  if (req.mode === 'navigate' || (req.headers.get('accept') || '').includes('text/html')) {
    event.respondWith(
      fetch(req).catch(() =>
        caches.match('/static/css/style.css').then(() => {
          // Minimal offline response — we do not cache HTML pages because they
          // are rendered with live data. Return a plain offline notice.
          return new Response(
            '<!doctype html><html lang="he" dir="rtl"><meta charset="utf-8">' +
            '<title>אופליין</title>' +
            '<body style="background:#0f172a;color:#f1f5f9;font-family:system-ui;' +
            'display:flex;align-items:center;justify-content:center;height:100vh;margin:0">' +
            '<div style="text-align:center"><h1>אין חיבור לאינטרנט</h1>' +
            '<p>נסה שוב כשהחיבור חוזר.</p></div></body></html>',
            { headers: { 'Content-Type': 'text/html; charset=utf-8' } }
          );
        })
      )
    );
    return;
  }

  // Default: just fetch from network.
});
