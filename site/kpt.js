/* ============ KPT and O2D tab ============
   Adds a third top tab, "KPT and O2D", beside Sales and Rating. Its views (Month to date and Day) show
   once the order-timing data is built into the site (manifest flag has_kpt). Until then the tab says
   what is missing. Loaded after the main page script and the other tab scripts. */

SUBS.kpt = [["mtd", "Month to date"], ["ftd", "Day"]];

const renderHeaderBase = renderHeader;
renderHeader = function () {
  renderHeaderBase();
  const l1 = document.getElementById("l1");
  if (l1 && !l1.querySelector('[data-top="kpt"]')) {
    l1.insertAdjacentHTML("beforeend", `<button data-top="kpt" aria-pressed="${S.top === "kpt"}">KPT and O2D</button>`);
  }
};

function kptView() {
  if (!M.has_kpt) {
    return `<section class="empty"><h2>KPT and O2D</h2>
      <p>KPT and O2D are not connected yet. Once the order-timing file is built into the site, this tab shows the Day and Month-to-date tables by brand, region, source and session, and the stores over the SLA lines.</p>
      <p class="muted">KPT is the time from order to ready. O2D is the time from order to delivery. The SLA lines are 12 minutes for KPT and 30 minutes for O2D.</p></section>`;
  }
  return `<section class="empty"><h2>KPT and O2D</h2><p>The KPT and O2D tables are being built.</p></section>`;
}

const renderBase = render;
render = function () {
  if (S.top !== "kpt") return renderBase();
  if (!M) return;
  renderHeader();
  renderFilters();
  setPeriodBar("month", null, null, false);
  renderPeriodBar();
  document.getElementById("view").innerHTML = kptView();
  renderNote();
};

document.addEventListener("click", e => {
  if (!e.target.closest('[data-top="kpt"]')) return;
  S.sub = "mtd";
  S.sel = null;
  render();
});
