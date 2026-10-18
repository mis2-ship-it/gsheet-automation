/* ============ Mobile and desktop views ============
   A Mobile / Desktop switch in the header. The first visit picks Mobile on a narrow screen; the choice is remembered.
   Sections titled "Overall" keep their full width. If index.html did not load live.js, this loads it.
   Loaded last. */
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
  bar.className = "modeseg";
  bar.setAttribute("role", "group");
  bar.setAttribute("aria-label", "View mode");
  bar.innerHTML = '<button data-mode="mobile">Mobile</button><button data-mode="desktop">Desktop</button>';
  const l1 = document.getElementById("l1");
  if (l1) l1.parentNode.insertBefore(bar, l1);
  bar.addEventListener("click", e => {
    const b = e.target.closest("[data-mode]");
    if (!b) return;
    try { localStorage.setItem(KEY, b.dataset.mode); } catch (err) { /* private mode: the choice lasts for this visit only */ }
    applyMode(b.dataset.mode);
  });
  applyMode(startMode());

  function markSections() {
    document.querySelectorAll("#view section.block").forEach(sec => {
      const h = sec.querySelector("h2");
      sec.classList.toggle("wide-table", !!h && h.textContent.trim().startsWith("Overall"));
      sec.classList.toggle("bucket-table", !!h && /bucket/i.test(h.textContent));
    });
  }
  const view = document.getElementById("view");
  if (view) new MutationObserver(markSections).observe(view, { childList: true });
  markSections();

  if (typeof salesLiveView === "function" && !String(salesLiveView).includes("ensureHourly")) {
    const s = document.createElement("script");
    s.src = "live.js";
    s.onload = () => { if (typeof render === "function") render(); };
    document.body.appendChild(s);
  }
})();
