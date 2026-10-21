/* ============ Store filter: loading, no-sales and load-failure messages ============
   A store's data is loaded when it is chosen. If it cannot be loaded (usually an ended sign-in),
   the page says so and offers a retry. A failed load is not kept, so the next choice tries again.
   Loaded after periods.js. */
const STORE_FAILED = new Set();
async function ensureStore(i) {
  if (STORE_ROWS.has(i)) return;
  try {
    const data = await loadGz(`data/stores/${i}.json.gz`);
    STORE_ROWS.set(i, data && data.rows ? data.rows : []);
    STORE_FAILED.delete(i);
  } catch (err) {
    STORE_FAILED.add(i);
  }
}
const esc3 = t => String(t).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
const salesPeriodViewBase4 = salesPeriodView;
salesPeriodView = function (kind) {
  const base = salesPeriodViewBase4(kind);
  if (S.store === "all") return base;
  const i = +S.store;
  const st = M.stores.find(x => x.i === i);
  const name = st ? esc3(st.name) : "This store";
  if (STORE_FAILED.has(i)) {
    return `<section class="empty"><h2>${name}</h2>
      <p>The data for this store could not be loaded. Usually the sign-in has ended.</p>
      <p><button class="refresh" type="button" data-store-retry="${i}">Try again</button></p></section>`;
  }
  const rows = STORE_ROWS.get(i);
  if (rows === undefined) return `<section class="empty"><h2>${name}</h2><p>Loading this store's data…</p></section>`;
  if (!rows.length) {
    return `<section class="empty"><h2>${name}</h2>
      <p>No sales are recorded for this store in the sales file. Choose another store, or All stores.</p></section>` + base;
  }
  return base;
};
document.addEventListener("click", e => {
  const b = e.target.closest("[data-store-retry]");
  if (!b) return;
  const i = +b.dataset.storeRetry;
  STORE_FAILED.delete(i);
  ensureStore(i).then(() => render());
});
