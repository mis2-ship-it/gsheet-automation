/* ============ Rating tab: product level, star slicer and customer comments ============
   Category and item tables (top 15 by orders in Month B, with Month A), a 1-5 star slicer,
   and the customer comments for the chosen star and product. Google has no product columns,
   so it shows its review text only. A product choice is cleared when the platform changes.
   Loaded after ratings.js. It wraps ratingView(). */

let PROD = null, COMM = null, PROD_LOADING = null;
const SLICE = { star: "all", item: null, platform: null };   // item: { kind: 0 = item, 1 = category, n: name index }

function ensureProducts() {
  if (PROD && COMM) return Promise.resolve();
  if (!PROD_LOADING) {
    PROD_LOADING = Promise.all([
      loadGz("data/products.json.gz").catch(() => ({ names: [], rows: [] })),
      loadGz("data/comments.json.gz").catch(() => ({ rows: [] }))
    ]).then(([p, c]) => { PROD = p; COMM = c; });
  }
  return PROD_LOADING;
}
const esc = t => String(t).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

// Product row: [month, platform, brand, region, kind, name, rated, rating_sum_x100, orders, s1..s5]
function productAgg(platform, kind) {
  const m = new Map();
  for (const r of PROD.rows) {
    if (r[4] !== kind || r[1] !== platform) continue;
    if (S.brand !== "all" && r[2] !== +S.brand) continue;
    if (S.region !== "all" && r[3] !== +S.region) continue;
    let e = m.get(r[5]);
    if (!e) m.set(r[5], e = {});
    const v = e[r[0]] || (e[r[0]] = new Array(8).fill(0));   // rated, rsum, orders, s1..s5
    for (let i = 0; i < 8; i++) v[i] += r[6 + i];
  }
  return m;
}
function productTable(platform, kind, A, B, la, lb) {
  const agg = productAgg(platform, kind);
  const list = [...agg.entries()]
    .filter(([, e]) => e[B] && e[B][2] > 0)
    .sort((x, y) => y[1][B][2] - x[1][B][2])
    .slice(0, 15);
  if (!list.length) return `<p class="muted">No ${kind ? "categories" : "items"} for these filters.</p>`;
  const avg = v => (v && v[0] ? v[1] / 100 / v[0] : null);
  const bt = v => (v && v[0] ? ((v[3] + v[4]) / v[0]) * 100 : null);
  const aw = v => (v && v[0] ? (v[7] / v[0]) * 100 : null);
  const body = list.map(([n, e]) => {
    const a = e[A], b = e[B];
    const oa = a ? a[2] : 0, ob = b[2];
    const aa = avg(a), ab = avg(b);
    const sel = SLICE.item && SLICE.item.kind === kind && SLICE.item.n === n;
    return `<tr${sel ? ' style="background:var(--line)"' : ""}>
      <td><span data-item="${kind}:${n}" style="text-decoration:underline;cursor:pointer">${esc(PROD.names[n])}</span></td>
      <td>${num(oa)}</td><td>${num(ob)}</td><td>${oa ? dlt(rel(ob, oa), "%", true) : "—"}</td>
      <td>${f2(aa)}</td><td>${f2(ab)}</td><td>${ch(aa, ab, " pts", true, 2)}</td>
      <td>${f1(bt(b))}</td><td>${f1(aw(b))}</td></tr>`;
  }).join("");
  return `<div class="scroll"><table class="tbl"><thead><tr><th>${kind ? "Category" : "Item"}</th>
    <th>${la}</th><th>${lb}</th><th>Orders change</th><th>Avg ${la}</th><th>Avg ${lb}</th><th>Change</th>
    <th>BT ${lb}</th><th>Awesome ${lb}</th></tr></thead><tbody>${body}</tbody></table></div>`;
}
function commentsSection(platform, A, B, lb, isGoogle) {
  const stars = ["all", "1", "2", "3", "4", "5"].map(s =>
    `<button data-sl="${s}" aria-pressed="${SLICE.star === s}">${s === "all" ? "All" : s + "★"}</button>`).join("");
  const item = isGoogle ? null : SLICE.item;   // a product choice from Swiggy or Zomato does not apply to Google
  const all = COMM.rows.filter(c => c[1] === platform &&
    (DAY_MON[c[0]] === A || DAY_MON[c[0]] === B) &&
    (S.brand === "all" || c[3] === +S.brand) &&
    (S.region === "all" || c[4] === +S.region) &&
    (S.store === "all" || c[5] === +S.store));
  const starOk = c => SLICE.star === "all" || c[2] === +SLICE.star;
  const itemOk = c => !item || (item.kind === 0 ? c[6] === item.n : c[7] === item.n);
  const list = all.filter(c => starOk(c) && itemOk(c)).sort((x, y) => y[0] - x[0]);
  const shown = list.slice(0, 30);
  const hint = item ? `Comments for <b>${esc(PROD.names[item.n])}</b>. Click it again to clear.`
                    : (isGoogle ? "Google reviews." : "Click an item or category above to see its comments.");
  const rowsHtml = shown.map(c => `<div class="srow">
      <div class="sh"><span class="sname">${"★".repeat(c[2])}</span><span class="muted">${fmtLong(c[0])}, ${esc(M.stores[c[5]].name)}</span></div>
      <p style="margin-top:4px">${esc(c[8])}</p>
      ${c[6] >= 0 ? `<p class="muted">${esc(PROD.names[c[6]])}</p>` : ""}
    </div>`).join("") || `<p class="muted">No comments for these filters.</p>`;
  return `<section class="block"><h2>${isGoogle ? "Reviews" : "Customer comments"}</h2>
    <p class="sub">${hint} Showing ${shown.length} of ${list.length} comments, most recent first.</p>
    <div class="seg3" style="grid-template-columns:repeat(6,1fr)">${stars}</div>
    <div style="margin-top:10px">${rowsHtml}</div></section>`;
}

const ratingViewBase = ratingView;
ratingView = function (platformName) {
  const base = ratingViewBase(platformName);
  if (!M.has_ratings || !RAT) return base;
  if (SLICE.platform !== platformName) { SLICE.item = null; SLICE.platform = platformName; }
  if (!PROD || !COMM) {
    ensureProducts().then(() => render());
    return base + `<p class="muted" style="margin-top:16px">Loading product and comment data…</p>`;
  }
  const platform = M.platforms.indexOf(platformName);
  const A = RM.a, B = RM.b, la = monLabel(A), lb = monLabel(B);
  const isGoogle = platformName === "Google";
  const prodBlock = isGoogle ? "" :
    `<section class="block"><h2>Category level, top 15 by orders (${lb})</h2>${productTable(platform, 1, A, B, la, lb)}</section>` +
    `<section class="block"><h2>Item level, top 15 by orders (${lb})</h2>${productTable(platform, 0, A, B, la, lb)}</section>`;
  return base + prodBlock + commentsSection(platform, A, B, lb, isGoogle);
};

document.addEventListener("click", e => {
  const s = e.target.closest("[data-sl]");
  if (s) { SLICE.star = s.dataset.sl; render(); return; }
  const it = e.target.closest("[data-item]");
  if (it) {
    const [kind, n] = it.dataset.item.split(":").map(Number);
    SLICE.item = SLICE.item && SLICE.item.kind === kind && SLICE.item.n === n ? null : { kind, n };
    render();
  }
});
