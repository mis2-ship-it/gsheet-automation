/* ============================================================
   Target Planner (Google Apps Script)
   Plans the target month's sales from last year's month-on-month (MoM) movement.
   Reads the Target_Base tab (daily sales by store, brand and channel), written by build_target_base.py.
   Set TARGET_SHEET_ID to the Google Sheet that holds Target_Base.

   Automatic target (Growth % blank):
     target = projected sales of the month before the target month
              x (1 + last year's MoM % for that store)
     MoM % = compare month / the month before it, last year, per store. A store counts only if it traded
     from before the start of that month. A store without a valid MoM uses its region's MoM, then the overall MoM.
     A month still in progress is projected: actual sales to date + the balance days. Each balance day takes the
     sales of the same weekday in the last 7 days of data (Friday of last week for a Friday, and so on).
   Manual target (Growth % entered):
     target = last year's compare month x (1 + Growth %)
   ============================================================ */
const TARGET_SHEET_ID = "PASTE_SHEET_ID_HERE";
const TARGET_TAB = "Target_Base";
const DAY_MS = 86400000;
const WDAY_NAMES = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const WEEK_ORDER = [1, 2, 3, 4, 5, 6, 0];       // display order: Mon ... Sun

function doGet() {
  return HtmlService.createHtmlOutputFromFile("Index").setTitle("Target Planner");
}

/* ---------- Reading the sheet ---------- */
function loadRows_() {
  const sh = SpreadsheetApp.openById(TARGET_SHEET_ID).getSheetByName(TARGET_TAB);
  if (!sh) throw new Error("The tab " + TARGET_TAB + " was not found. Run build_target_base.py first.");
  const last = sh.getLastRow();
  if (last < 2) throw new Error("Target_Base has no data rows yet.");
  const tz = Session.getScriptTimeZone();
  return sh.getRange(2, 1, last - 1, 11).getValues().map(v => ({
    d: v[0] instanceof Date ? Utilities.formatDate(v[0], tz, "yyyy-MM-dd") : String(v[0]).slice(0, 10),
    branch: String(v[1]), region: String(v[2]), type: String(v[3]), brand: String(v[4]), channel: String(v[5]),
    net: Number(v[6]) || 0, gross: Number(v[7]) || 0, disc: Number(v[8]) || 0,
    orders: Number(v[9]) || 0, qty: Number(v[10]) || 0
  })).filter(r => r.d.length === 10);
}

/* ---------- Dates (day numbers count days since 1970-01-01, in UTC) ---------- */
function dn_(s) { const p = s.split("-").map(Number); return Math.round(Date.UTC(p[0], p[1] - 1, p[2]) / DAY_MS); }
function ymd_(n) { return new Date(n * DAY_MS).toISOString().slice(0, 10); }
function ym_(key, k) { const p = key.split("-").map(Number); return new Date(Date.UTC(p[0], p[1] - 1 + k, 1)).toISOString().slice(0, 7); }
function monthDays_(key) {
  const p = key.split("-").map(Number);
  const first = Math.round(Date.UTC(p[0], p[1] - 1, 1) / DAY_MS);
  const len = new Date(Date.UTC(p[0], p[1], 0)).getUTCDate();
  return Array.from({ length: len }, (_, i) => first + i);
}
function wdayOf_(n) { return new Date(n * DAY_MS).getUTCDay(); }
function isMonThu_(n) { const w = wdayOf_(n); return w >= 1 && w <= 4; }
function domOf_(n) { return new Date(n * DAY_MS).getUTCDate(); }
function fmtN_(x) { return Math.round(x).toLocaleString("en-IN"); }
function weekdayCounts_(days) { const c = [0, 0, 0, 0, 0, 0, 0]; for (const n of days) c[wdayOf_(n)]++; return c; }

/* ---------- Filters and grouping ---------- */
function pick_(r, f) {
  return (f.channel === "all" || r.channel === f.channel) && (f.type === "all" || r.type === f.type) &&
         (f.brand === "all" || r.brand === f.brand) && (f.region === "all" || r.region === f.region) &&
         (f.store === "all" || r.branch === f.store);
}
function sumOf_(list, fn) { return list.reduce((s, r) => s + fn(r), 0); }
function groupBy_(list, keyFn) {
  const m = new Map();
  for (const r of list) { const k = keyFn(r); if (!m.has(k)) m.set(k, []); m.get(k).push(r); }
  return m;
}

/* ---------- Phasing: how the target is spread over the days ---------- */
// weekday : each day gets last year's average for its weekday (Mon, Tue, ... Sun)
// daytype : each day gets the average Mon-Thu day or the average Fri-Sun day of last year
// samedate: each day gets last year's sales on the same day of the month (daytype if none)
function phaseWeights_(lyRows, targetKey, lyKey, method) {
  const lyDays = monthDays_(lyKey), days = monthDays_(targetKey);
  const cnt = weekdayCounts_(lyDays);
  const sum = [0, 0, 0, 0, 0, 0, 0];
  for (const r of lyRows) sum[wdayOf_(r.n)] += r.net;
  const avgWd = cnt.map((c, w) => (c ? sum[w] / c : 0));
  const monThuCnt = cnt[1] + cnt[2] + cnt[3] + cnt[4];
  const monThuNet = sum[1] + sum[2] + sum[3] + sum[4];
  const allNet = sum.reduce((a, b) => a + b, 0);
  const avgMT = monThuCnt ? monThuNet / monThuCnt : 0;
  const avgFS = (lyDays.length - monThuCnt) ? (allNet - monThuNet) / (lyDays.length - monThuCnt) : 0;
  const byDom = new Map();
  for (const r of lyRows) { const k = domOf_(r.n); byDom.set(k, (byDom.get(k) || 0) + r.net); }
  const raw = days.map(n => {
    if (method === "weekday") return avgWd[wdayOf_(n)];
    const typeAvg = isMonThu_(n) ? avgMT : avgFS;
    if (method === "samedate") { const same = byDom.get(domOf_(n)) || 0; return same > 0 ? same : typeAvg; }
    return typeAvg;
  });
  const total = raw.reduce((a, b) => a + b, 0);
  return days.map((n, i) => ({ n: n, w: total > 0 ? raw[i] / total : 1 / days.length }));
}

/* ---------- The planner ---------- */
// p: { targetMonth, compareMonth (default: target minus 12 months), channel, type, brand, region, store,
//      growth (percent, blank = automatic), basis: 'store' | 'region' | 'overall' (automatic only),
//      storeGrowth: { storeName: percent } (typed in the store table), method: 'weekday' | 'daytype' | 'samedate' }
function computeDashboard(p, rowsIn) {
  const rows = rowsIn.map(r => (r.n === undefined ? Object.assign({}, r, { n: dn_(r.d) }) : r));
  const f = { channel: p.channel || "all", type: p.type || "all", brand: p.brand || "all",
              region: p.region || "all", store: p.store || "all" };
  const latestDay = rows.reduce((m, r) => (r.n > m ? r.n : m), -Infinity);
  const latestKey = ymd_(latestDay).slice(0, 7);
  const target = p.targetMonth || ym_(latestKey, 1);
  const ly = p.compareMonth || ym_(target, -12);          // last year's compare month
  const prevKey = ym_(ly, -1);                             // the month before it (for MoM)
  const baseKey = ym_(target, -1);                        // the month before the target (base)
  const topGrowth = (p.growth === "" || p.growth === null || p.growth === undefined) ? null : Number(p.growth) / 100;
  const useLy = topGrowth !== null;                       // manual Growth %: last year's month is the base
  const basis = ["region", "overall"].indexOf(p.basis) >= 0 ? p.basis : "store";
  const method = ["samedate", "daytype"].indexOf(p.method) >= 0 ? p.method : "weekday";
  const storeG = {};                                      // growth typed for individual stores
  Object.keys(p.storeGrowth || {}).forEach(k => {
    const raw = String(p.storeGrowth[k]).trim();
    if (raw !== "" && isFinite(Number(raw))) storeG[k] = Number(raw) / 100;
  });

  const sel = rows.filter(r => pick_(r, f));
  const chan = rows.filter(r => f.channel === "all" || r.channel === f.channel);
  const inMonth = (list, key) => list.filter(r => r.d.slice(0, 7) === key);

  // Store facts on the chosen channel: first sale day, region, net per store per month.
  const firstDay = new Map(), regionOf = new Map(), netOf = new Map();
  for (const r of chan) {
    if (!firstDay.has(r.branch) || r.n < firstDay.get(r.branch)) firstDay.set(r.branch, r.n);
    regionOf.set(r.branch, r.region);
    const k = r.branch + "|" + r.d.slice(0, 7);
    netOf.set(k, (netOf.get(k) || 0) + r.net);
  }

  // Last year's MoM per store. A store counts only if it traded in both months and was open before the earlier month.
  const prevStart = dn_(prevKey + "-01");
  const mom = new Map(), regionSum = new Map(), allSum = [0, 0];
  for (const s of firstDay.keys()) {
    const a = netOf.get(s + "|" + ly) || 0, b = netOf.get(s + "|" + prevKey) || 0;
    if (!(a > 0 && b > 0 && firstDay.get(s) <= prevStart)) continue;
    mom.set(s, a / b - 1);
    const reg = regionOf.get(s);
    const rs = regionSum.get(reg) || [0, 0];
    rs[0] += a; rs[1] += b; regionSum.set(reg, rs);
    allSum[0] += a; allSum[1] += b;
  }
  const overallMom = allSum[1] > 0 ? allSum[0] / allSum[1] - 1 : 0;
  const regionMom = reg => { const rs = regionSum.get(reg); return rs && rs[1] > 0 ? rs[0] / rs[1] - 1 : overallMom; };
  const basisMom = store => {
    if (basis === "overall") return overallMom;
    if (basis === "region") return regionMom(regionOf.get(store));
    if (mom.has(store)) return mom.get(store);
    return regionMom(regionOf.get(store));
  };
  // Growth for a store: its own typed growth, else the manual Growth %, else the automatic basis.
  const growthOf = store => (storeG[store] !== undefined ? storeG[store] : (topGrowth !== null ? topGrowth : basisMom(store)));

  // The base month, projected to month end:
  //   actual sales to date  +  balance days.
  // Each balance day (after the latest data day) takes the sales of the same weekday in the last 7 days of data.
  // Example: data to Thu 22 Oct -> 23 Oct (Fri) takes Fri 16 Oct, 24 Oct (Sat) takes Sat 17 Oct, and so on,
  // so the 9 balance days are the last 7 days plus Fri 16 and Sat 17.
  const baseStart = dn_(baseKey + "-01");
  const baseDays = monthDays_(baseKey).length;
  const baseEnd = baseStart + baseDays - 1;
  const lastBaseDay = Math.min(latestDay, baseEnd);
  const lastWeekStart = latestDay - 6;
  const balanceDays = [];
  for (let d = Math.max(baseStart, latestDay + 1); d <= baseEnd; d++) balanceDays.push(d);
  const sourceDay = d => lastWeekStart + ((((d - lastWeekStart) % 7) + 7) % 7);
  const lastWeek = new Map();                              // row + day -> net, for the last 7 days of data
  for (const r of sel) {
    if (r.n < lastWeekStart || r.n > latestDay) continue;
    const k = r.branch + "|" + r.brand + "|" + r.channel + "|" + r.n;
    lastWeek.set(k, (lastWeek.get(k) || 0) + r.net);
  }
  const balanceOf = r => {
    let total = 0;
    const base = r.branch + "|" + r.brand + "|" + r.channel + "|";
    for (const d of balanceDays) total += lastWeek.get(base + sourceDay(d)) || 0;
    return total;
  };
  // One row per store, brand and channel for the base month: actual to date, then its balance added once.
  const baseAgg = new Map();
  for (const r of inMonth(sel, baseKey)) {
    const k = r.branch + "|" + r.brand + "|" + r.channel;
    if (!baseAgg.has(k)) baseAgg.set(k, Object.assign({}, r, { net: 0 }));
    baseAgg.get(k).net += r.net;
  }
  const baseRows = [...baseAgg.values()].map(rep => {
    const bal = balanceOf(rep);
    return { r: rep, actual: rep.net, bal: bal, proj: rep.net + bal };
  });
  const lyRows = inMonth(sel, ly), prevRows = inMonth(sel, prevKey);

  // Target rows: manual = last year's month x (1 + growth); automatic = base projection x (1 + growth).
  const tRows = useLy
    ? lyRows.map(r => ({ r: r, t: r.net * (1 + growthOf(r.branch)) }))
    : baseRows.map(b => ({ r: b.r, t: b.proj * (1 + growthOf(b.r.branch)) }));

  // Outlets: stores with sales in a month (net above zero).
  const storeNet = list => { const m = new Map(); for (const r of list) m.set(r.branch, (m.get(r.branch) || 0) + r.net); return m; };
  const trading = m => new Set([...m.keys()].filter(s => m.get(s) > 0));
  const lyStores = trading(storeNet(lyRows)), baseStores = trading(storeNet(baseRows.map(b => b.r)));
  const notTrading = [...lyStores].filter(s => !baseStores.has(s)).length;
  const newStores = [...baseStores].filter(s => !lyStores.has(s)).length;

  const levelTable = (keyFn, byRegion) => {
    const isStore = byRegion === "store";
    const lyG = groupBy_(lyRows, keyFn), prevG = groupBy_(prevRows, keyFn);
    const baseG = groupBy_(baseRows, b => keyFn(b.r)), tG = groupBy_(tRows, t => keyFn(t.r));
    const keys = new Set([...lyG.keys(), ...baseG.keys(), ...tG.keys()]);
    const out = [...keys].map(k => {
      const lyList = lyG.get(k) || [], prevList = prevG.get(k) || [];
      const baseList = baseG.get(k) || [], tList = tG.get(k) || [];
      const first = lyList[0] || (baseList[0] && baseList[0].r) || (tList[0] && tList[0].r);
      const lyNet = sumOf_(lyList, r => r.net), prevNet = sumOf_(prevList, r => r.net);
      const base = sumOf_(baseList, b => b.proj), t = sumOf_(tList, x => x.t);
      const lyOut = trading(storeNet(lyList)), baseOut = trading(storeNet(baseList.map(b => b.r)));
      const growthUsed = isStore ? growthOf(String(k))
        : (useLy ? (lyNet > 0 ? t / lyNet - 1 : null) : (base > 0 ? t / base - 1 : null));
      const lyWd = sumOf_(lyList.filter(r => isMonThu_(r.n)), r => r.net);
      const lyWe = lyNet - lyWd;
      const wdDays = weekdayCounts_(monthDays_(ly)).slice(1, 5).reduce((a, b) => a + b, 0);
      const weDays = monthDays_(ly).length - wdDays;
      return {
        name: String(k), region: first.region, type: first.type,
        ly: lyNet, orders: sumOf_(lyList, r => r.orders), prev: prevNet,
        mom: prevNet > 0 ? lyNet / prevNet - 1 : null,
        base: base, growthUsed: growthUsed, target: t,
        outletsLy: lyOut.size, outletsBase: baseOut.size,
        status: (lyNet > 0 && base > 0) ? "Operating" : (lyNet > 0 ? "Not trading now" : (base > 0 ? "New" : "—")),
        typed: storeG[String(k)] !== undefined ? storeG[String(k)] * 100 : null,
        lyWeekdayPerDay: wdDays ? lyWd / wdDays : 0,
        lyWeekendPerDay: weDays ? lyWe / weDays : 0
      };
    });
    if (isStore) out.sort((a, b) => (a.region.localeCompare(b.region)) || (b.ly - a.ly));
    else out.sort((a, b) => b.target - a.target);
    return out;
  };

  // Phasing for the selected filters.
  const targetTotal = sumOf_(tRows, x => x.t);
  const lyDaysArr = monthDays_(ly);
  const weights = phaseWeights_(lyRows, target, ly, method);
  const lyDaily = new Map();
  for (const r of lyRows) lyDaily.set(r.d, (lyDaily.get(r.d) || 0) + r.net);
  let cum = 0;
  const phasing = weights.map(({ n, w }) => {
    const planned = targetTotal * w;
    cum += planned;
    const sameLy = lyDaysArr[domOf_(n) - 1];
    return {
      date: ymd_(n), weekday: WDAY_NAMES[wdayOf_(n)], dayType: isMonThu_(n) ? "Mon-Thu" : "Fri-Sun",
      lySameDate: sameLy !== undefined ? (lyDaily.get(ymd_(sameLy)) || 0) : 0,
      planned: planned, cumulative: cum, cumulativePct: targetTotal > 0 ? cum / targetTotal : 0, w: wdayOf_(n)
    };
  });

  // Days and average day sales, Mon to Sun.
  const tDays = monthDays_(target);
  const lyCounts = weekdayCounts_(lyDaysArr), tCounts = weekdayCounts_(tDays);
  const lyWdNet = [0, 0, 0, 0, 0, 0, 0];
  for (const r of lyRows) lyWdNet[wdayOf_(r.n)] += r.net;
  const planWd = [0, 0, 0, 0, 0, 0, 0];
  for (const d of phasing) planWd[d.w] += d.planned;
  const lyNet = sumOf_(lyRows, r => r.net);
  const prevNet = sumOf_(prevRows, r => r.net);
  const baseNet = sumOf_(baseRows, b => b.proj);
  const baseActual = sumOf_(baseRows, b => b.actual);
  const baseBalance = sumOf_(baseRows, b => b.bal);
  const baseElapsed = Math.max(0, lastBaseDay - baseStart + 1);

  return {
    meta: {
      targetMonth: target, compareMonth: ly, prevMonth: prevKey, baseMonth: baseKey, latestDataMonth: latestKey,
      method: method, basis: basis, storeOverrides: Object.keys(storeG).length, storesWithMoM: mom.size,
      growthMode: useLy
        ? "entered by you: last year's month x (1 + Growth %)"
        : "automatic: " + (basis === "store" ? "each store's own last-year MoM"
            : basis === "region" ? "last year's region MoM, same for all stores in the region"
            : "last year's overall MoM, same for all stores") + ", applied to " + baseKey +
          " projected (actual " + fmtN_(baseActual) + " + balance " + fmtN_(baseBalance) + ")"
    },
    summary: {
      lyNet: lyNet, lyOrders: sumOf_(lyRows, r => r.orders), lyDays: lyDaysArr.length, prevNet: prevNet,
      mom: prevNet > 0 ? lyNet / prevNet - 1 : null, overallMom: overallMom,
      baseNet: baseNet, baseActual: baseActual, baseBalance: baseBalance, baseElapsed: baseElapsed,
      baseDays: baseDays, balanceDayCount: balanceDays.length,
      target: targetTotal,
      growthApplied: useLy ? (lyNet > 0 ? targetTotal / lyNet - 1 : null) : (baseNet > 0 ? targetTotal / baseNet - 1 : null),
      outletsLy: lyStores.size, outletsBase: baseStores.size, notTrading: notTrading, newStores: newStores,
      outletsTarget: baseStores.size
    },
    days: {
      columns: ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
      rows: [
        { key: ly, counts: WEEK_ORDER.map(w => lyCounts[w]), total: lyDaysArr.length },
        { key: target, counts: WEEK_ORDER.map(w => tCounts[w]), total: tDays.length }
      ]
    },
    avgDay: {
      columns: ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
      rows: [
        { key: ly, label: "actual", avg: WEEK_ORDER.map(w => (lyCounts[w] ? lyWdNet[w] / lyCounts[w] : 0)),
          overall: lyDaysArr.length ? lyNet / lyDaysArr.length : 0 },
        { key: target, label: "projected", avg: WEEK_ORDER.map(w => (tCounts[w] ? planWd[w] / tCounts[w] : 0)),
          overall: tDays.length ? targetTotal / tDays.length : 0 }
      ]
    },
    brand: levelTable(r => r.brand, false),
    region: levelTable(r => r.region, false),
    store: levelTable(r => r.branch, "store"),
    phasing: phasing.map(d => ({ date: d.date, weekday: d.weekday, dayType: d.dayType, lySameDate: d.lySameDate,
      planned: d.planned, cumulative: d.cumulative, cumulativePct: d.cumulativePct }))
  };
}

/* ---------- Called from the page ---------- */
function getOptions() {
  const rows = loadRows_();
  const months = [...new Set(rows.map(r => r.d.slice(0, 7)))].sort();
  const latest = months[months.length - 1];
  const stores = [...new Map(rows.map(r => [r.branch, { name: r.branch, region: r.region, type: r.type }])).values()]
    .sort((a, b) => a.name.localeCompare(b.name));
  return {
    months: months, latest: latest, nextMonth: ym_(latest, 1),
    brands: [...new Set(rows.map(r => r.brand))].sort(),
    regions: [...new Set(rows.map(r => r.region))].sort(),
    types: [...new Set(rows.map(r => r.type))].sort(),
    stores: stores
  };
}
function getDashboard(p) { return computeDashboard(p, loadRows_()); }
