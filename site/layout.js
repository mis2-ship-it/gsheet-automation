/* ============ Top bar, mobile and desktop views, refresh, hour labels, and sign-in notice ============
   The top bar holds the Frozen Bottle logo, Mobile / Desktop, a Refresh button and the time of the latest live data.
   If a data request fails because the sign-in has ended, a notice offers to sign in again.
   Sections titled "Overall" keep their full width. The hour-by-hour chart gets a revenue and growth label under each hour.
   If index.html did not load live.js, this loads it. Loaded last. */
(function () {
  const KEY = "fb_view_mode";
  const root = document.documentElement;

  function applyMode(mode) {
    root.classList.toggle("mode-mobile", mode === "mobile");
    root.classList.toggle("mode-desktop", mode === "desktop");
    document.querySelectorAll("[data-mode]").forEach(b =>
      b.setAttribute("aria-pressed", String(b.dataset.mode === mode)));
  }
  function startMode() {
    let saved = null;
    try { saved = localStorage.getItem(KEY); } catch (e) { saved = null; }
    if (saved === "mobile" || saved === "desktop") return saved;
    return window.matchMedia("(max-width: 760px)").matches ? "mobile" : "desktop";
  }

  const bar = document.createElement("div");
  bar.className = "topbar";
  bar.innerHTML = '<span class="brand-logo" role="img" aria-label="Frozen Bottle"></span>' +
    '<div class="modeseg" role="group" aria-label="View mode">' +
    '<button data-mode="mobile">Mobile</button><button data-mode="desktop">Desktop</button></div>' +
    '<button class="refresh" id="refresh-btn" type="button">Refresh</button>' +
    '<span class="stamp" id="stamp"></span>';
  const l1 = document.getElementById("l1");
  if (l1) l1.parentNode.insertBefore(bar, l1);
  bar.addEventListener("click", e => {
    if (e.target.closest("#refresh-btn")) { location.reload(); return; }
    const b = e.target.closest("[data-mode]");
    if (!b) return;
    try { localStorage.setItem(KEY, b.dataset.mode); } catch (err) { /* private mode: this visit only */ }
    applyMode(b.dataset.mode);
  });
  applyMode(startMode());

  // Sign-in notice: shown when a data request is refused or redirected to the sign-in page.
  function signInNotice() {
    if (document.getElementById("signin-notice")) return;
    const d = document.createElement("div");
    d.id = "signin-notice";
    d.className = "signin";
    d.innerHTML = 'Your sign-in has ended or the connection was lost. <button type="button" id="signin-btn">Sign in again</button>';
    (document.querySelector(".app") || document.body).prepend(d);
    document.getElementById("signin-btn").addEventListener("click", () => location.reload());
  }
  function clearSignInNotice() {
    const n = document.getElementById("signin-notice");
    if (n) n.remove();
  }
  const nativeFetch = window.fetch.bind(window);
  const isData = url => /(^|\/)data\//.test(url);   // matches "data/x" and "/data/x"
  window.fetch = function (input, init) {
    const url = String(input && input.url ? input.url : input);
    if (!isData(url)) return nativeFetch(input, init);
    return nativeFetch(input, init).then(res => {
      if (res.redirected && new URL(res.url).origin !== location.origin) signInNotice();
      else if (res.ok) clearSignInNotice();
      return res;
    }, err => {
      signInNotice();
      throw err;
    });
  };

  function updateStamp() {
    const el = document.getElementById("stamp");
    if (!el || typeof M === "undefined" || !M || !M.live_as_of) return;
    el.textContent = "Live data as of " + M.live_as_of.slice(11, 16) + " IST";
  }

  function markSections() {
    document.querySelectorAll("#view section.block").forEach(sec => {
      const h = sec.querySelector("h2");
      sec.classList.toggle("wide-table", !!h && h.textContent.trim().startsWith("Overall"));
      sec.classList.toggle("bucket-table", !!h && /bucket/i.test(h.textContent));
    });
  }

  function addHourLabels() {
    const sec = [...document.querySelectorAll("#view section.block")]
      .find(s => (s.querySelector("h2") || {}).textContent === "Hour by hour");
    if (!sec || sec.querySelector(".hourlabels")) return;
    const svg = sec.querySelector("svg.chart");
    const rows = [...sec.querySelectorAll("table.tbl tbody tr")].filter(r => !r.classList.contains("tot"));
    if (!svg || !rows.length) return;
    const fmtK = v => (v >= 1000 ? (v / 1000).toFixed(1) + "k" : String(Math.round(v)));
    const labels = rows.map(r => {
      const td = r.querySelectorAll("td");
      const rev = parseFloat((td[1].textContent || "").replace(/[^0-9.]/g, ""));
      const g = (td[3].textContent || "").replace(/[()]/g, "").trim();
      const cls = g.includes("▼") ? "dn" : g.includes("▲") ? "upc" : "";
      return `<div class="hl"><span class="hv">${isNaN(rev) ? "" : fmtK(rev)}</span>` +
        `<span class="hg ${cls}">${g || "—"}</span></div>`;
    }).join("");
    const wrap = document.createElement("div");
    wrap.className = "hourwrap";
    svg.parentNode.insertBefore(wrap, svg);
    wrap.appendChild(svg);
    const grid = document.createElement("div");
    grid.className = "hourlabels";
    grid.style.gridTemplateColumns = `repeat(${rows.length}, 1fr)`;
    grid.innerHTML = labels;
    wrap.appendChild(grid);
  }

  function onRender() { markSections(); addHourLabels(); updateStamp(); }
  const view = document.getElementById("view");
  if (view) new MutationObserver(onRender).observe(view, { childList: true });
  onRender();

  if (typeof salesLiveView === "function" && !String(salesLiveView).includes("ensureHourly")) {
    const s = document.createElement("script");
    s.src = "live.js";
    s.onload = () => { if (typeof render === "function") render(); };
    document.body.appendChild(s);
  }
})();
