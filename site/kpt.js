/* ============ KPT and O2D tab ============
   A third top tab with two views: Day (the latest day in the order file) and Month to date.
   Tables by brand, region, source and session, then the stores over a limit, ranked by orders over a limit.
   KPT limit 12 min (order to ready). O2D limit 30 min (order to delivery). P80 is the time 80% of orders beat.
   Data: data/kpt.json.gz, built from sync_kpt.py. If the data cannot be loaded (usually an expired sign-in),
   the tab says so and offers a retry. Loaded after the main page script and the other tab scripts. */

SUBS.kpt = [["mtd", "Month to date"], ["ftd", "Day"]];
const KPT_LIMIT = { kpt: 12, o2d: 30 };
let KPT = null, KPT_LOADING = null, KPT_ERROR = null;

function ensureKpt() {
  if (KPT) return Promise.resolve();
  if (!KPT_LOADING) {
    KPT_ERROR = null;
    KPT_LOADING = loadGz("data/kpt.json.gz")
      .then(d => { KPT = d; KPT_LOADING = null; })
      .catch(err => { KPT_ERROR = (err && err.message) || "network error"; KPT_LOADING = null; });
  }
  return KPT_LOADING;
}

function p80(arr) {
  if (!arr.length) return null;
  const s = arr.slice().sort((a, b) => a - b);
  const pos = (s.length - 1) * 0.8, lo = Math.floor(pos), hi = Math.ceil(pos);
  return s[lo] + (s[hi] - s[lo]) * (pos - lo);
}

// Row: [day, store, brand, region, source, session, kpt_x10, o2d_x10 (-1 = missing)]
function kptMetrics(rows) {
  const kpt = [], o2d = [];
  let kOver = 0, oOver = 0, either = 0;
  for (const r of rows) {
    const k = r[6] / 10;
    const o = r[7] < 0 ? null : r[7] / 10;
    kpt.push(k);
    const kb = k > KPT_LIMIT.kpt;
    const ob = o !== null && o > KPT_LIMIT.o2d;
    if (kb) kOver++;
    if (o !== null) { o2d.push(o); if (ob) oOver++; }
    if (kb || ob) either++;
  }
  return { n: rows.length, kptP80: p80(kpt), o2dP80: p80(o2d), kOver, oOver, either, o2dN: o2d.length };
}

function kptFilter(lo, hi) {
  const brand = S.brand === "all" ? null : M.brands[+S.brand];
  const region = S.region === "all" ? null : M.regions[+S.region];
  const store = S.store === "all" ? null : (M.stores.find(x => x.i === +S.store) || {}).name;
  return r => r[0] >= lo && r[0] <= hi &&
    (!brand || KPT.brands[r[2]] === brand) &&
    (!region || KPT.regions[r[3]] === region) &&
    (!store || KPT.stores[r[1]] === store);
}

function kptRange(sub) {
  const last = KPT.lastDay;
  if (sub === "ftd") return [last, last];
  const dt = toDate(last);
  return [monStart((dt.getUTCFullYear() - 2025) * 12 + dt.getUTCMonth()), last];
}

const mins = v => (v === null ? "—" : v.toFixed(1));
const share = (c, n) => (n ? `${num(c)} (${Math.round((c / n) * 100)}%)` : "—");

function kptDimTable(title, rows, keyPos, names) {
  const groups = names.map(() => []);
  for (const r of rows) groups[r[keyPos]].push(r);
  const line = (name, m, cls) => `<tr class="${cls || ""}"><td>${esc(name)}</td><td>${num(m.n)}</td>` +
    `<td>${mins(m.kptP80)}</td><td>${share(m.kOver, m.n)}</td><td>${mins(m.o2dP80)}</td><td>${share(m.oOver, m.o2dN)}</td></tr>`;
  const body = line("Total", kptMetrics(rows), "tot") +
    names.map((name, i) => (groups[i].length ? line(name, kptMetrics(groups[i])) : "")).join("");
  return `<section class="block"><h2>By ${title}</h2>
    <div class="scroll"><table class="tbl"><thead><tr><th></th><th>Orders</th><th>KPT P80</th><th>KPT over 12</th>
    <th>O2D P80</th><th>O2D over 30</th></tr></thead><tbody>${body}</tbody></table></div></section>`;
}

function kptStoreTable(rows) {
  const groups = KPT.stores.map(() => []);
  for (const r of rows) groups[r[1]].push(r);
  const list = groups.map((g, i) => ({
    name: KPT.stores[i], m: kptMetrics(g), region: g.length ? KPT.regions[g[0][3]] : ""
  })).filter(x => x.m.n > 0 && ((x.m.kptP80 !== null && x.m.kptP80 > KPT_LIMIT.kpt) ||
    (x.m.o2dP80 !== null && x.m.o2dP80 > KPT_LIMIT.o2d)))
    .sort((a, b) => b.m.either - a.m.either);
  const body = list.map(x => `<tr><td>${esc(x.name)}</td><td>${esc(x.region)}</td><td>${num(x.m.n)}</td>
    <td>${mins(x.m.kptP80)}</td><td>${mins(x.m.o2dP80)}</td><td>${share(x.m.either, x.m.n)}</td></tr>`).join("") ||
    `<tr><td colspan="6" class="muted">No store is over a limit at P80 for this period.</td></tr>`;
  return `<section class="block"><h2>Stores over a limit at P80</h2>
    <p class="sub">KPT P80 over 12 min or O2D P80 over 30 min, ranked by orders over either limit.</p>
    <div class="scroll"><table class="tbl"><thead><tr><th>Store</th><th>Region</th><th>Orders</th><th>KPT P80</th>
    <th>O2D P80</th><th>Over a limit</th></tr></thead><tbody>${body}</tbody></table></div></section>`;
}

function kptView() {
  if (!M.has_kpt) {
    return `<section class="empty"><h2>KPT and O2D</h2>
      <p>KPT and O2D are not connected yet. The order-timing file is built each night and appears here once it is in the site.</p>
      <p class="muted">KPT is the time from order to ready. O2D is the time from order to delivery. The limits are 12 minutes for KPT and 30 minutes for O2D.</p></section>`;
  }
  if (!KPT) {
    if (KPT_ERROR) {
      return `<section class="empty"><h2>KPT and O2D</h2>
        <p>The KPT and O2D data could not be loaded.</p>
        <p class="muted">Usually the sign-in has expired. Refresh the page to sign in again, or try again below.</p>
        <p><button class="refresh" type="button" data-kpt-retry="1">Try again</button></p></section>`;
    }
    if (!KPT_LOADING) ensureKpt().then(() => render());
    return `<p class="muted" style="margin-top:16px">Loading KPT and O2D data…</p>`;
  }
  if (KPT.lastDay === null || !KPT.rows.length) {
    return `<section class="empty"><h2>KPT and O2D</h2><p>No online orders in the file for this login.</p></section>`;
  }
  const sub = S.sub === "ftd" ? "ftd" : "mtd";
  const [lo, hi] = kptRange(sub);
  const rows = KPT.rows.filter(kptFilter(lo, hi));
  const label = sub === "ftd" ? `Day: ${fmtLong(hi)}` : `Month to date: ${fmtDay(lo)} to ${fmtDay(hi)}`;
  return `<p class="sub" style="margin-top:14px">${label}. KPT is order to ready, O2D is order to delivery. ` +
    `P80 is the time that 80% of orders beat. "Over" counts the orders past the limit.</p>` +
    kptDimTable("brand", rows, 2, KPT.brands) +
    kptDimTable("region", rows, 3, KPT.regions) +
    kptDimTable("source", rows, 4, KPT.sources) +
    kptDimTable("session", rows, 5, KPT.sessions) +
    kptStoreTable(rows);
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

const renderHeaderBase = renderHeader;
renderHeader = function () {
  renderHeaderBase();
  const l1 = document.getElementById("l1");
  if (l1 && !l1.querySelector('[data-top="kpt"]')) {
    l1.insertAdjacentHTML("beforeend", `<button data-top="kpt" aria-pressed="${S.top === "kpt"}">KPT and O2D</button>`);
  }
};

document.addEventListener("click", e => {
  if (e.target.closest("[data-kpt-retry]")) { KPT_ERROR = null; render(); return; }
  if (!e.target.closest('[data-top="kpt"]')) return;
  S.sub = "mtd";
  S.sel = null;
  render();
});
