/* ============ Sign-in safety net ============
   Loaded before the main page script (index.html), so every data request passes through here.
   1. A data file that has loaded once stays in memory for this visit. Switching tabs or stores does not ask
      the server again, and nothing is kept on the device.
   2. The tab files (products, comments, KPT, hourly, buckets, ratings) load in the background shortly after the
      page starts, while the sign-in is fresh, so opening those tabs is instant.
   3. A data request that fails is retried once. If it still fails because the sign-in has ended, the page
      restarts once by itself, so Cloudflare's sign-in page appears straight away, and the tab you were on is
      restored after you sign in. A second automatic restart is not made within two minutes, so the page
      cannot loop.
   4. If the device is offline, nothing restarts; the connection notice from layout.js is shown instead. */
(function () {
  const nativeFetch = window.fetch.bind(window);
  const DATA_CACHE = new Map();                 // url -> ArrayBuffer, for this visit only
  const RESTART_KEY = "fb-restart-at";
  const STATE_KEY = "fb-restore-state";
  const RESTART_GAP_MS = 120000;
  const PREFETCH_DELAY_MS = 2000;
  const PREFETCH_STEP_MS = 300;
  const PREFETCH = ["data/products.json.gz", "data/comments.json.gz", "data/kpt.json.gz", "data/hourly.json.gz",
                    "data/aov_buckets.json.gz", "data/disc_buckets.json.gz", "data/ratings.json.gz"];

  const isData = url => /(^|\/)data\//.test(url);
  const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
  const stayPending = () => new Promise(() => {});   // the page is restarting; nothing more to show

  // Restart the page once. Returns false if a restart happened less than two minutes ago.
  function restartOnce() {
    let last = 0;
    try { last = Number(sessionStorage.getItem(RESTART_KEY)) || 0; } catch (err) { /* no storage */ }
    if (Date.now() - last < RESTART_GAP_MS) return false;
    try {
      sessionStorage.setItem(RESTART_KEY, String(Date.now()));
      if (typeof S !== "undefined") sessionStorage.setItem(STATE_KEY, JSON.stringify(S));
    } catch (err) { /* private mode: restart without restoring the tab */ }
    location.reload();
    return true;
  }

  // A data request that was redirected to another site is the sign-in page.
  function sentToSignIn(res) {
    return !!res.redirected && !!res.url && new URL(res.url).origin !== location.origin;
  }

  function attempt(input, init, url, tries) {
    return nativeFetch(input, init).then(res => {
      if (sentToSignIn(res)) return restartOnce() ? stayPending() : res;   // no restart: layout.js shows the notice
      if (res.ok) res.clone().arrayBuffer().then(buf => DATA_CACHE.set(url, buf)).catch(() => {});
      return res;
    }, err => {
      if (!navigator.onLine) throw err;                                   // really offline: show the connection notice
      if (tries < 2) return sleep(1500).then(() => attempt(input, init, url, tries + 1));
      if (restartOnce()) return stayPending();
      throw err;
    });
  }

  window.fetch = function (input, init) {
    const url = String(input && input.url ? input.url : input);
    if (!isData(url)) return nativeFetch(input, init);
    if (DATA_CACHE.has(url)) return Promise.resolve(new Response(DATA_CACHE.get(url).slice(0), { status: 200 }));
    return attempt(input, init, url, 1);
  };

  // After a restart, put back the tab, store and filters the person had open.
  function restoreTab() {
    let saved = null;
    try {
      saved = sessionStorage.getItem(STATE_KEY);
      sessionStorage.removeItem(STATE_KEY);
    } catch (err) { return; }
    if (!saved || typeof S === "undefined") return;
    try { Object.assign(S, JSON.parse(saved)); } catch (err) { return; }
    try { if (typeof render === "function") render(); } catch (err) { /* the page renders again when its data arrives */ }
  }

  // Load the tab files once, in the background, while the sign-in is fresh.
  function prefetchTabData() {
    PREFETCH.forEach((url, i) => {
      setTimeout(() => {
        window.fetch(url).then(res => (res.ok ? res.arrayBuffer() : null)).catch(() => {});
      }, i * PREFETCH_STEP_MS);
    });
  }

  document.addEventListener("DOMContentLoaded", () => {
    restoreTab();
    setTimeout(prefetchTabData, PREFETCH_DELAY_MS);
  });
})();
