/* ============ Store filter: no-sales message and safe loading ============
   If the chosen store has no sales rows, the page says so instead of showing zeros only.
   If a store's data cannot be loaded, the page shows no sales for it and keeps working.
   Loaded after periods.js. */
async function ensureStore(i) {
  if (STORE_ROWS.has(i)) return;
  try {
    const data = await loadGz(`data/stores/${i}.json.gz`);
    STORE_ROWS.set(i, data && data.rows ? data.rows : []);
  } catch (err) {
    STORE_ROWS.set(i, []);
    console.warn("Store data could not be loaded:", i, err);
  }
}
const esc3 = t => String(t).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
const salesPeriodViewBase4 = salesPeriodView;
salesPeriodView = function (kind) {
  const base = salesPeriodViewBase4(kind);
  if (S.store === "all") return base;
  const rows = STORE_ROWS.get(+S.store);
  if (!rows || rows.length) return base;
  const st = M.stores.find(x => x.i === +S.store);
  return `<section class="empty"><h2>${esc3(st ? st.name : "This store")}</h2>
    <p>No sales are recorded for this store in the sales file. Choose another store, or All stores.</p></section>` + base;
};
