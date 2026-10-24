/* ============================================================
   Target Planner (Google Apps Script)
   Plans next month's sales target from the same month last year, for overall, brand, region, store and day.
   Reads the Target_Base tab (daily sales by store, brand and channel), written by build_target_base.py.
   Set TARGET_SHEET_ID to the Google Sheet that holds Target_Base.
   ============================================================ */
const TARGET_SHEET_ID = "PASTE_SHEET_ID_HERE";
const TARGET_TAB = "Target_Base";
const DAY_MS = 86400000;
const WDAY_NAMES = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

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
function isMonThu_(n) { const w = wdayOf_(n); return w >= 1 && w <= 4; }     // Mon-Thu = weekdays; Fri-Sun = weekend
function domOf_(n) { return new Date(n * DAY_MS).getUTCDate(); }
function calendarCounts_(days) {
  const weekday = days.filter(isMonThu_).length;
  return { weekday: weekday, weekend: days.length - weekday, days: days.length };
}

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
function growth_(cur, base) { return base > 0 ? cur / base - 1 : null; }

/* ---------- Phasing: how the month's target is spread over the days ---------- */
// daytype: each Mon-Thu day gets the average Mon-Thu day of last year's month, and Fri-Sun the same for weekends.
// samedate: each day gets last year's sales on the same day of the month (falls back to daytype if none).
function phaseWeights_(lyRows, targetKey, lyKey, method) {
  const lyDays = monthDays_(lyKey);
  const lyCal = calendarCounts_(lyDays);
  const wdNet = sumOf_(lyRows.filter(r => isMonThu_(r.n)), r => r.net);
  const weNet = sumOf_(lyRows.filter(r => !isMonThu_(r.n)), r => r.net);
  const avgWd = lyCal.weekday ? wdNet / lyCal.weekday : 0;
  const avgWe = lyCal.weekend ? weNet / lyCal.weekend : 0;
  const byDom = new Map();
  for (const r of lyRows) { const k = domOf_(r.n); byDom.set(k, (byDom.get(k) || 0) + r.net); }
  const days = monthDays_(targetKey);
  const raw = days.map(n => {
    const same = byDom.get(domOf_(n)) || 0;
    if (method === "samedate" && same > 0) return same;
    return isMonThu_(n) ? avgWd : avgWe;
  });
  const total = raw.reduce((a, b) => a + b, 0);
  return days.map((n, i) => ({ n: n, w: total > 0 ? raw[i] / total : 1 / days.length }));
}

/* ---------- The planner ---------- */
// p: { targetMonth 'YYYY-MM', compareMonth 'YYYY-MM' (default: target minus 12 months), channel 'all'|'Offline'|'Online',
//      brand, region, store ('all' = no filter), growth (percent, blank = automatic), method 'daytype'|'samedate' }
// rowsIn: all rows from Target_Base.
function computeDashboard(p, rowsIn) {
  const rows = rowsIn.map(r => (r.n === undefined ? Object.assign({}, r, { n: dn_(r.d) }) : r));
  const f = { channel: p.channel || "all", type: p.type || "all", brand: p.brand || "all",
              region: p.region || "all", store: p.store || "all" };
  const latestKey = rows.reduce((m, r) => (r.d > m ? r.d : m), "").slice(0, 7);
  const target = p.targetMonth || ym_(latestKey, 1);
  const ly = p.compareMonth || ym_(target, -12);
  const prevKey = ym_(ly, -1);
  const override = (p.growth === "" || p.growth === null || p.growth === undefined) ? null : Number(p.growth) / 100;
  const method = p.method === "samedate" ? "samedate" : "daytype";

  const sel = rows.filter(r => pick_(r, f));
  // Growth is worked out on the chosen channel only. So the store, brand, region and store-type splits
  // always add up to the same target, whatever store, brand, region or type is selected.
  const basis = rows.filter(r => f.channel === "all" || r.channel === f.channel);
  const lyRows = sel.filter(r => r.d.slice(0, 7) === ly);
  const prevRows = sel.filter(r => r.d.slice(0, 7) === prevKey);

  // Suggested growth: the latest three months against the same three months last year, like-for-like.
  // A store counts only if it sold in all six of those months, so new stores do not inflate the growth.
  const recentKeys = [-2, -1, 0].map(k => ym_(latestKey, k));
  const recentLyKeys = recentKeys.map(k => ym_(k, -12));
  const winMonths = new Set(recentKeys.concat(recentLyKeys));
  const monthsSold = new Map();
  for (const r of basis) {
    const m = r.d.slice(0, 7);
    if (!winMonths.has(m)) continue;
    if (!monthsSold.has(r.branch)) monthsSold.set(r.branch, new Set());
    monthsSold.get(r.branch).add(m);
  }
  const lfl = new Set([...monthsSold.keys()].filter(s => monthsSold.get(s).size === winMonths.size));
  const recNow = basis.filter(r => recentKeys.includes(r.d.slice(0, 7)) && lfl.has(r.branch));
  const recLy = basis.filter(r => recentLyKeys.includes(r.d.slice(0, 7)) && lfl.has(r.branch));
  const recentGrowth = keyFn => {
    const now = groupBy_(recNow, keyFn), base = groupBy_(recLy, keyFn);
    const m = new Map();
    for (const [k, list] of now) {
      const g = growth_(sumOf_(list, r => r.net), sumOf_(base.get(k) || [], r => r.net));
      if (g !== null) m.set(k, g);
    }
    return m;
  };
  const gStore = recentGrowth(r => r.branch);
  const gRegion = recentGrowth(r => r.region);
  const gAll = growth_(sumOf_(recNow, r => r.net), sumOf_(recLy, r => r.net));
  const storeRegion = new Map();
  for (const r of lyRows) if (!storeRegion.has(r.branch)) storeRegion.set(r.branch, r.region);

  // Growth for a store: the override if given; else its own recent growth; else its region's; else overall.
  const growthFor = store => {
    if (override !== null) return override;
    if (gStore.has(store)) return gStore.get(store);
    if (gRegion.has(storeRegion.get(store))) return gRegion.get(storeRegion.get(store));
    return gAll !== null ? gAll : 0;
  };
  const tgt = new Map();                       // each last-year row -> its target
  for (const r of lyRows) tgt.set(r, r.net * (1 + growthFor(r.branch)));
  const tsum = list => sumOf_(list, r => tgt.get(r));
  const lyCal = calendarCounts_(monthDays_(ly));

  const levelTable = (keyFn, sortByRegion) => {
    const lyG = groupBy_(lyRows, keyFn), prevG = groupBy_(prevRows, keyFn);
    const recNowG = groupBy_(recNow, keyFn), recLyG = groupBy_(recLy, keyFn);
    const out = [...lyG.keys()].map(k => {
      const list = lyG.get(k);
      const lyNet = sumOf_(list, r => r.net), t = tsum(list);
      const pv = sumOf_(prevG.get(k) || [], r => r.net);
      const wd = list.filter(r => isMonThu_(r.n)), we = list.filter(r => !isMonThu_(r.n));
      return {
        name: String(k), region: list[0].region, type: list[0].type,
        ly: lyNet, orders: sumOf_(list, r => r.orders), prev: pv, mom: growth_(lyNet, pv),
        recent: growth_(sumOf_(recNowG.get(k) || [], r => r.net), sumOf_(recLyG.get(k) || [], r => r.net)),
        growthUsed: lyNet > 0 ? t / lyNet - 1 : null, target: t,
        lyWeekdayPerDay: lyCal.weekday ? sumOf_(wd, r => r.net) / lyCal.weekday : 0,
        lyWeekendPerDay: lyCal.weekend ? sumOf_(we, r => r.net) / lyCal.weekend : 0
      };
    });
    if (sortByRegion) out.sort((a, b) => (a.region.localeCompare(b.region)) || (b.ly - a.ly));
    else out.sort((a, b) => b.target - a.target);
    return out;
  };

  // Phasing for the selected filters.
  const targetTotal = tsum(lyRows);
  const weights = phaseWeights_(lyRows, target, ly, method);
  const lyDaily = new Map();
  for (const r of lyRows) lyDaily.set(r.d, (lyDaily.get(r.d) || 0) + r.net);
  const lyDays = monthDays_(ly);
  let cum = 0;
  const phasing = weights.map(({ n, w }) => {
    const planned = targetTotal * w;
    cum += planned;
    const sameLy = lyDays[domOf_(n) - 1];
    return {
      date: ymd_(n), weekday: WDAY_NAMES[wdayOf_(n)], dayType: isMonThu_(n) ? "Mon-Thu" : "Fri-Sun",
      lySameDate: sameLy !== undefined ? lyDaily.get(ymd_(sameLy)) || 0 : 0,
      planned: planned, cumulative: cum, cumulativePct: targetTotal > 0 ? cum / targetTotal : 0
    };
  });

  const tCal = calendarCounts_(monthDays_(target));
  const plannedWd = sumOf_(phasing.filter(d => d.dayType === "Mon-Thu"), d => d.planned);
  const plannedWe = sumOf_(phasing.filter(d => d.dayType === "Fri-Sun"), d => d.planned);
  const lyNet = sumOf_(lyRows, r => r.net);
  const prevNet = sumOf_(prevRows, r => r.net);

  return {
    meta: { targetMonth: target, compareMonth: ly, prevMonth: prevKey, latestDataMonth: latestKey, method: method,
            growthMode: override === null ? "automatic: like-for-like stores, latest 3 months vs the same 3 months last year" : "entered by you",
            likeForLikeStores: lfl.size },
    summary: {
      lyNet: lyNet, lyOrders: sumOf_(lyRows, r => r.orders), lyDays: lyCal.days,
      lyWeekdayDays: lyCal.weekday, lyWeekendDays: lyCal.weekend,
      lyWeekdayAvg: lyCal.weekday ? sumOf_(lyRows.filter(r => isMonThu_(r.n)), r => r.net) / lyCal.weekday : 0,
      lyWeekendAvg: lyCal.weekend ? sumOf_(lyRows.filter(r => !isMonThu_(r.n)), r => r.net) / lyCal.weekend : 0,
      prevNet: prevNet, mom: growth_(lyNet, prevNet), recentYoY: gAll,
      target: targetTotal, growthApplied: lyNet > 0 ? targetTotal / lyNet - 1 : null,
      targetWeekdayDays: tCal.weekday, targetWeekendDays: tCal.weekend,
      targetWeekdayAvg: tCal.weekday ? plannedWd / tCal.weekday : 0,
      targetWeekendAvg: tCal.weekend ? plannedWe / tCal.weekend : 0
    },
    brand: levelTable(r => r.brand, false),
    region: levelTable(r => r.region, false),
    store: levelTable(r => r.branch, true),
    phasing: phasing
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
