/* ============ By area manager: every sales tab, Live, Rating, and KPT and O2D ============
   Each tab gets a "By area manager" table. Each area manager's stores are added up with the same filters.
   The area manager for each store comes from the User Access tab (AM Name).
   The store view has no area manager split. Loaded after spikes.js, live.js, ratings.js, products.js and kpt.js. */
const amAdd = (x, y) => { const r = Object.assign({}, x); for (const k of Object.keys(y)) r[k] = (r[k] || 0) + y[k]; return r; };
const escA = t => String(t).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
const amName = k => (M.ams && M.ams[k]) || "Unassigned";
let AM_OF_STORE = null;
function amOfStore(i) {
  if (!AM_OF_STORE) AM_OF_STORE = new Map(M.stores.map(s => [s.i, s.a || 0]));
  return AM_OF_STORE.get(i) || 0;
}

// One table: this period against the period it is compared with. Each map is keyed by area manager.
function amTableFromMaps(ca, cb, c, p, note) {
  const sumAll = m => [...m.values()].reduce((acc, a) => amAdd(acc, a), Object.assign({}, EMPTY_A));
  const keys = [...new Set([...ca.keys(), ...cb.keys()])]
    .filter(k => (ca.get(k) || EMPTY_A).net || (cb.get(k) || EMPTY_A).net)
    .sort((x, y) => (ca.get(y) || EMPTY_A).net - (ca.get(x) || EMPTY_A).net);
  const gr = (a, b) => (b ? dlt(rel(a, b), "%", true) : "—");
  const line = (label, a, b, cls) => {
    const am = metrics(a), bm = metrics(b);
    return `<tr class="${cls || ""}"><td>${escA(label)}</td><td>${num(am.net)}</td><td>${num(bm.net)}</td><td>${gr(am.net, bm.net)}</td>` +
      `<td>${num(am.txn)}</td><td>${num(bm.txn)}</td><td>${gr(am.txn, bm.txn)}</td>` +
      `<td>${am.discPct.toFixed(1)}%</td><td>${bm.discPct.toFixed(1)}%</td><td>${dlt(pts(am.discPct, bm.discPct), " pts", false)}</td></tr>`;
  };
  const body = line("Total", sumAll(ca), sumAll(cb), "tot") +
    keys.map(k => line(amName(k), ca.get(k) || EMPTY_A, cb.get(k) || EMPTY_A)).join("");
  return `<section class="block"><h2>By area manager</h2><p class="sub">${note}</p>` +
    `<div class="scroll"><table class="tbl"><thead><tr><th>Area manager</th><th>${c} net</th><th>${p} net</th><th>Growth</th>` +
    `<th>${c} orders</th><th>${p} orders</th><th>Growth</th><th>${c} Dis %</th><th>${p} Dis %</th><th>Dis change</th></tr></thead>` +
    `<tbody>${body}</tbody></table></div></section>`;
}

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
function amSalesBlock(curWins, prevWins, c, p, note) {
  const rows = baseSales();
  return amTableFromMaps(amByWins(rows, curWins, salesPass), amByWins(rows, prevWins, salesPass), c, p, note);
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
  const state = S.store !== "all" ? storeStateHtml(+S.store) : "";
  const base = spikeBlockAm();
  if (state) return state + base;
  if (S.store !== "all") return base;
  if (!events.length) return base;
  const g = spGroup(events, SP.year, spAvail(), baseSales());
  return base + amSalesBlock(g.curWins, g.prevWins, String(SP.year), String(SP.year - 1),
    `Spike days in ${spMonthName(SP.month)} ${SP.year}, with the same festivals last year.`);
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
function amLiveBlock() {
  const clock = liveClock();
  if (!clock.active || clock.cut === 0) {
    return `<section class="block"><h2>By area manager</h2><p class="muted">No complete hour yet.</p></section>`;
  }
  const cut = clock.cut, day = M.live_day;
  const ca = amFromStoreMap(hourlyDim(day, cut, 1)), cb = amFromStoreMap(hourlyDim(day - 7, cut, 1));
  return amTableFromMaps(ca, cb, "Today", "Last week",
    `Complete hours, 9 AM to ${hourName(cut)}. Brackets compare with last week, same hours.`);
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
