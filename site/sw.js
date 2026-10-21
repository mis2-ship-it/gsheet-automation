// Service worker: lets the dashboard install to the home screen.
// Page visits always go to the network, so the sign-in page can appear when the sign-in has ended.
// Other shell files come from the network first; the last copy is used only when the network fails.
// Data files (/data/...) are never kept on the device, because each login has its own data.
const CACHE = "fb-sales-v4";
const SHELL = ["./", "./index.html", "./manifest.webmanifest", "./icon-192.png", "./icon-512.png", "./apple-touch-icon.png"];

self.addEventListener("install", event => {
  event.waitUntil(caches.open(CACHE).then(cache => Promise.allSettled(SHELL.map(u => cache.add(u)))).then(() => self.skipWaiting()));
});

self.addEventListener("activate", event => {
  event.waitUntil(
    caches.keys()
      .then(keys => Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", event => {
  const url = new URL(event.request.url);
  if (event.request.method !== "GET" || url.origin !== self.location.origin) return;
  if (event.request.mode === "navigate") return;   // page visits go straight to the network
  if (url.pathname.startsWith("/data/")) return;   // never keep one login's data on the device
  event.respondWith(
    fetch(event.request)
      .then(response => {
        if (response.ok) {
          const copy = response.clone();
          caches.open(CACHE).then(cache => cache.put(event.request, copy));
        }
        return response;
      })
      .catch(() => caches.match(event.request))
  );
});
