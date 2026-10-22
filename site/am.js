/* ============ Sales: By area manager ============
   An area manager table at the foot of Daily, Weekly, Monthly, Month to date and Spike Days.
   Each area manager's stores are added up with the same filters (Brand, Region, Source, Session, Store).
   Loaded after spikes.js. It wraps salesPeriodView() and spikeBlock(). */
const amAdd = (x, y) => { const r = Object.assign({}, x); for (const k of Object.keys(y)) r[k] = (r[k] || 0) + y[k]; return r; };
const escA = t => String(t).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
const amName = k => (M.ams && M.ams[k]) || "Unassigned";

function amByWins(rows, wins, pass) {
  const out = new Map();
  for (const w of wins) {
    for (const [k, a] of aggByDim(rows, w[0], w[1], 1, pass)) {
      out.set(k, out.has(k) ? amAdd(out.get(k), a) : Object.assign({}, a));
    }
  }
  return out;
}
function amTotal(rows, wins, pass) {
  return wins.reduce((acc, w) => amAdd(acc, aggFor(rows, w[0], w[1], pass)), Object.assign({}, EMPTY_A));
}
function amBlock(curWins, prevWins, c, p, note) {
  const rows = baseSales();
  const ca = amByWins(rows, curWins, salesPass), cb = amByWins(rows, prevWins, salesPass);
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
  const body = line("Total", amTotal(rows, curWins, salesPass), amTotal(rows, prevWins, salesPass), "tot") +
    keys.map(k => line(amName(k), ca.get(k) || EMPTY_A, cb.get(k) || EMPTY_A)).join("");
  return `<section class="block"><h2>By area manager</h2><p class="sub">${note}</p>` +
    `<div class="scroll"><table class="tbl"><thead><tr><th>Area manager</th><th>${c} net</th><th>${p} net</th><th>Growth</th>` +
    `<th>${c} orders</th><th>${p} orders</th><th>Growth</th><th>${c} Dis %</th><th>${p} Dis %</th><th>Dis change</th></tr></thead>` +
    `<tbody>${body}</tbody></table></div></section>`;
}

// The periods for each sales tab: the current period and the one it is compared with.
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
  return w ? base + amBlock(w.cur, w.prev, w.c, w.p, w.note) : base;
};

const spikeBlockAm = spikeBlock;
spikeBlock = function () {
  const base = spikeBlockAm();
  if (S.store !== "all") return base;
  const events = spEvents(SP.year, SP.month);
  if (!events.length) return base;
  const g = spGroup(events, SP.year, spAvail(), baseSales());
  return base + amBlock(g.curWins, g.prevWins, String(SP.year), String(SP.year - 1),
    `Spike days in ${spMonthName(SP.month)} ${SP.year}, with the same festivals last year.`);
};
