/* ============ By area manager: every sales tab, Live, Rating, and KPT and O2D ============
   Each tab gets a "By area manager" table. Each area manager's stores are added up with the same filters.
   Click an area manager's name to open its stores; click again to close them. The area manager for each store
   comes from the User Access tab (AM Name). The store view has no area manager split.
   Loaded after spikes.js, live.js, ratings.js, products.js and kpt.js. */
const amAdd = (x, y) => { const r = Object.assign({}, x); for (const k of Object.keys(y)) r[k] = (r[k] || 0) + y[k]; return r; };
const escA = t => String(t).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
const amName = k => (M.ams && M.ams[k]) || "Unassigned";
let AM_OF_STORE = null;
function amOfStore(i) {
  if (!AM_OF_STORE) AM_OF_STORE = new Map(M.stores.map(s => [s.i, s.a || 0]));
  return AM_OF_STORE.get(i) || 0;
}

// Which area managers are open (their stores shown). Kept between redraws.
const AM_OPEN = {};
const AM_LOADING = new Set();
function amStoresOf(k) { return M.stores.filter(s => (s.a || 0) === k); }

// The figures for one row: net, orders and discount, this period against the one it is compared with.
function amMetricCells(a, b) {
  if (a === null || b === null) return `<td colspan="9" style="color:#9AA8C2">loading…</td>`;
  const am = metrics(a), bm = metrics(b);
  const gr = (x, y) => (y ? dlt(rel(x, y), "%", true) : "—");
  return `<td>${num(am.net)}</td><td>${num(bm.net)}</td><td>${gr(am.net, bm.net)}</td>` +
    `<td>${num(am.txn)}</td><td>${num(bm.txn)}</td><td>${gr(am.txn, bm.txn)}</td>` +
    `<td>${am.discPct.toFixed(1)}%</td><td>${bm.discPct.toFixed(1)}%</td><td>${dlt(pts(am.discPct, bm.discPct), " pts", false)}</td>`;
}

// The store rows under an open area manager.
function amStoreRowsHtml(k, detail) {
  if (!detail || !AM_OPEN[k]) return "";
  const list = detail(k).filter(r => r.failed || r.a === null || r.a.net || (r.b && r.b.net));
  list.sort((x, y) => (y.a ? y.a.net : -1) - (x.a ? x.a.net : -1));
  if (!list.length) {
    return `<tr class="amstore"><td style="padding-left:22px;color:#9AA8C2">No sales in these periods</td><td colspan="9"></td></tr>`;
  }
  return list.map(r => r.failed
    ? `<tr class="amstore"><td style="padding-left:22px;color:#9AA8C2">${escA(r.name)}</td><td colspan="9" style="color:#F87171">could not be loaded</td></tr>`
    : `<tr class="amstore"><td style="padding-left:22px;color:#C9D5EA">${escA(r.name)}</td>${amMetricCells(r.a, r.b)}</tr>`
  ).join("");
}

// One table: this period against the period it is compared with. Each map is keyed by area manager.
// detail(k) gives the store rows for area manager k (used when its name is opened).
function amTableFromMaps(ca, cb, c, p, note, detail) {
  const sumAll = m => [...m.values()].reduce((acc, a) => amAdd(acc, a), Object.assign({}, EMPTY_A));
  const keys = [...new Set([...ca.keys(), ...cb.keys()])]
    .filter(k => (ca.get(k) || EMPTY_A).net || (cb.get(k) || EMPTY_A).net)
    .sort((x, y) => (ca.get(y) || EMPTY_A).net - (ca.get(x) || EMPTY_A).net);
  const totalRow = `<tr class="tot"><td>Total</td>${amMetricCells(sumAll(ca), sumAll(cb))}</tr>`;
  const amRows = keys.map(k => {
    const open = !!AM_OPEN[k];
    const nameCell = `<td data-amk="${k}" style="cursor:pointer;font-weight:600">${open ? "▾ " : "▸ "}${escA(amName(k))}</td>`;
    return `<tr>${nameCell}${amMetricCells(ca.get(k) || EMPTY_A, cb.get(k) || EMPTY_A)}</tr>` + amStoreRowsHtml(k, detail);
  }).join("");
  return `<section class="block"><h2>By area manager</h2><p class="sub">${note} Click an area manager's name to see its stores.</p>` +
    `<div class="scroll"><table class="tbl"><thead><tr><th>Area manager</th><th>${c} net</th><th>${p} net</th><th>Growth</th>` +
    `<th>${c} orders</th><th>${p} orders</th><th>Growth</th><th>${c} Dis %</th><th>${p} Dis %</th><th>Dis change</th></tr></thead>` +
    `<tbody>${totalRow}${amRows}</tbody></table></div></section>`;
}

// Opens and closes an area manager's stores when its name is clicked.
document.addEventListener("click", e => {
  const cell = e.target && e.target.closest ? e.target.closest("[data-amk]") : null;
  if (!cell) return;
  const k = Number(cell.getAttribute("data-amk"));
  AM_OPEN[k] = !AM_OPEN[k];
  render();
});

// Sales tabs: the sales rows already carry the area manager in their second column.
function amByWins(rows, wins, pass) {
  const out = new Map();
  for (const w of wins) {
    for (const [k, a] of aggByDim(rows, w[0], w[1], 1, pass)) {
      out.set(k, out.has(k) ? amAdd(out.get(k), a) : Object.assign({}, a));
    }
  }
  return out;
}

// Store figures for the sales periods, from each store's own file (loaded the first time it is needed).
function aggWins(rows, wins) {
  let acc = Object.assign({}, EMPTY_A);
  for (const w of wins) acc = amAdd(acc, aggFor(rows, w[0], w[1], salesPass));
  return acc;
}
function amLoadStores(ids) {
  const need = ids.filter(i => !AM_LOADING.has(i));
  if (!need.length) return;
  need.forEach(i => AM_LOADING.add(i));
  const done = () => { need.forEach(i => AM_LOADING.delete(i)); render(); };
  Promise.all(need.map(i => ensureStore(i))).then(done, done);
}
function amPeriodStoreRows(k, curWins, prevWins) {
  const missing = [];
  const out = amStoresOf(k).map(s => {
    const rows = STORE_ROWS.get(s.i);
    if (rows === undefined) {
      if (STORE_FAILED.has(s.i)) return { name: s.name, a: null, b: null, failed: true };
      missing.push(s.i);
      return { name: s.name, a: null, b: null };
    }
    return { name: s.name, a: aggWins(rows, curWins), b: aggWins(rows, prevWins) };
  });
  if (missing.length) amLoadStores(missing);
  return out;
}

function amSalesBlock(curWins, prevWins, c, p, note) {
  const rows = baseSales();
  return amTableFromMaps(amByWins(rows, curWins, salesPass), amByWins(rows, prevWins, salesPass), c, p, note,
    k => amPeriodStoreRows(k, curWins, prevWins));
}
function amPeriodWins(kind) {
  if (kind === "mtd") {
    const last = Math.min(M.last_day, yesterdayIdx());
    const cm = monthKeyOfDay(last);
    const span = last - monStart(cm);
    const pm = cm - 1;
    return { cur: [[monStart(cm), last]], prev: [[monStart(pm), Math.min(monStart(pm) + span, monEnd(pm))]],
             c: "Month to date", p: "Same days last month", note: "Month to date, 1st to yesterday, with each area manager's stores." };
  }
  const keys = periodKeys(kind);
  if (!keys.length) return null;
  const key = S.sel !== null && keys.includes(S.sel) ? S.sel : keys[keys.length - 1];
  const w = sumWins(kind, key);
  return { cur: [w.cur], prev: [w.prev], c: w.c, p: w.p, note: "Each area manager's stores, with the filters above." };
}
const salesPeriodViewAm = salesPeriodView;
salesPeriodView = function (kind) {
  const base = salesPeriodViewAm(kind);
  if (!["daily", "weekly", "monthly", "mtd"].includes(kind) || S.store !== "all") return base;
  const w = amPeriodWins(kind);
  return w ? base + amSalesBlock(w.cur, w.prev, w.c, w.p, w.note) : base;
};
const spikeBlockAm = spikeBlock;
spikeBlock = function () {
  try {
    const state = S.store !== "all" ? storeStateHtml(+S.store) : "";
    const base = spikeBlockAm();
    if (state) return state + base;
    if (S.store !== "all") return base;
    let block = "";
    try {
      const events = spEvents(SP.year, SP.month);
      if (events.length) {
        const g = spGroup(events, SP.year, spAvail(), baseSales());
        block = amSalesBlock(g.curWins, g.prevWins, String(SP.year), String(SP.year - 1),
          `Spike days in ${spMonthName(SP.month)} ${SP.year}, with the same festivals last year.`);
      }
    } catch (err) {
      console.error("By area manager (Spike Days):", err);
      block = `<section class="block"><h2>By area manager</h2><p class="muted">This table could not be drawn: ${escA(err.message)}</p></section>`;
    }
    return base + block;
  } catch (err) {
    console.error("Spike Days:", err);
    return `<section class="empty"><h2>Spike Days could not be drawn</h2><p>${escA(err.message)}</p>` +
      `<p class="muted">Open the browser console (F12) for the full error.</p></section>`;
  }
};

// Live: complete hours, from the hourly rows, grouped by the area manager of each store.
function amFromStoreMap(m) {
  const out = new Map();
  for (const [st, a] of m) {
    const k = amOfStore(st);
    out.set(k, out.has(k) ? amAdd(out.get(k), a) : Object.assign({}, a));
  }
  return out;
}
// Live store rows: complete hours today and the same hours last week, from the hourly rows.
function amLiveStoreRows(k) {
  const clock = liveClock();
  const ca = hourlyDim(M.live_day, clock.cut, 1), cb = hourlyDim(M.live_day - 7, clock.cut, 1);
  return amStoresOf(k).map(s => ({ name: s.name, a: ca.get(s.i) || EMPTY_A, b: cb.get(s.i) || EMPTY_A }));
}
function amLiveBlock() {
  const clock = liveClock();
  if (!clock.active || clock.cut === 0) {
    return `<section class="block"><h2>By area manager</h2><p class="muted">No complete hour yet.</p></section>`;
  }
  const cut = clock.cut, day = M.live_day;
  const ca = amFromStoreMap(hourlyDim(day, cut, 1)), cb = amFromStoreMap(hourlyDim(day - 7, cut, 1));
  return amTableFromMaps(ca, cb, "Today", "Last week",
    `Complete hours, 9 AM to ${hourName(cut)}. Brackets compare with last week, same hours.`,
    k => amLiveStoreRows(k));
}
const salesLiveViewAm = salesLiveView;
salesLiveView = function () {
  const base = salesLiveViewAm();
  if (S.store !== "all" || typeof HOURLY === "undefined" || !HOURLY || !M.has_live) return base;
  return base + amLiveBlock();
};

// Rating: rated orders, average rating, BT and Awesome, by area manager.
function amRatingBlock(platformName) {
  const platform = M.platforms.indexOf(platformName);
  const A = RM.a, B = RM.b, la = monLabel(A), lb = monLabel(B);
  const wa = [monStart(A), monEnd(A)], wb = [monStart(B), monEnd(B)];
  const line = (label, test, cls) => {
    const ta = rTotals(platform, wa[0], wa[1], test), tb = rTotals(platform, wb[0], wb[1], test);
    if (!ta[6] && !tb[6]) return "";
    const a = rMetrics(ta), b = rMetrics(tb);
    return `<tr class="${cls || ""}"><td>${escA(label)}</td><td>${f2(a.avg)}</td><td>${f2(b.avg)}</td><td>${ch(a.avg, b.avg, " pts", true, 2)}</td>` +
      `<td>${f1(b.bt)}</td><td>${f1(b.awe)}</td><td>${num(a.rated)}</td><td>${num(b.rated)}</td>` +
      `<td>${platformName === "Google" ? "n/a" : num(b.issues)}</td></tr>`;
  };
  const body = line("All stores", () => true, "tot") +
    M.ams.map((_, k) => line(amName(k), r => amOfStore(r[1]) === k)).join("");
  return `<section class="block"><h2>By area manager</h2><p class="sub">Ratings for ${lb}, with ${la} for comparison. Each area manager's stores, with the filters above.</p>` +
    `<div class="scroll"><table class="tbl"><thead><tr><th>Area manager</th><th>Avg ${la}</th><th>Avg ${lb}</th><th>Change</th>` +
    `<th>BT ${lb}</th><th>Awesome ${lb}</th><th>Rated ${la}</th><th>Rated ${lb}</th><th>Issues ${lb}</th></tr></thead>` +
    `<tbody>${body}</tbody></table></div></section>`;
}
const ratingViewAm = ratingView;
ratingView = function (platformName) {
  const base = ratingViewAm(platformName);
  if (!M.has_ratings || !RAT || S.store !== "all") return base;
  return base + amRatingBlock(platformName);
};

// KPT and O2D: orders, P80 and the orders over each limit, by area manager.
function amKptBlock() {
  const [lo, hi] = kptRange(S.sub === "ftd" ? "ftd" : "mtd");
  const rows = KPT.rows.filter(kptFilter(lo, hi));
  const groups = M.ams.map(() => []);
  for (const r of rows) groups[KPT.storeAm[r[1]] || 0].push(r);
  const line = (label, m, cls) => `<tr class="${cls || ""}"><td>${escA(label)}</td><td>${num(m.n)}</td><td>${mins(m.kptP80)}</td>` +
    `<td>${share(m.kOver, m.n)}</td><td>${mins(m.o2dP80)}</td><td>${share(m.oOver, m.o2dN)}</td></tr>`;
  const body = line("Total", kptMetrics(rows), "tot") +
    groups.map((g, k) => (g.length ? line(amName(k), kptMetrics(g)) : "")).join("");
  return `<section class="block"><h2>By area manager</h2><p class="sub">Each area manager's stores, with the same limits: KPT 12 minutes, O2D 30 minutes.</p>` +
    `<div class="scroll"><table class="tbl"><thead><tr><th>Area manager</th><th>Orders</th><th>KPT P80</th><th>KPT over 12</th>` +
    `<th>O2D P80</th><th>O2D over 30</th></tr></thead><tbody>${body}</tbody></table></div></section>`;
}
const kptViewAm = kptView;
kptView = function () {
  const base = kptViewAm();
  if (!M.has_kpt || !KPT || !KPT.storeAm || !KPT.rows || !KPT.rows.length) return base;
  return base + amKptBlock();
};
