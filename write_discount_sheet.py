/* ============================================================
   Discount Dashboard (Google Apps Script)
   Discount Code sales for Swiggy and Zomato orders.
   Reads the tabs Disc_Label and Disc_Item, written by write_discount_sheet.py.
   Set DISCOUNT_SHEET_ID to the Google Sheet that holds those tabs.

   Each order counts once, under its discount label (the label combines all discounts on the order).
   Orders = orders (unique invoices). Items = item lines. Net = Gross - Discount. Dis % = Discount / Gross.
   Contribution % = a code's net / total net of all orders in the period.
   Discount codes listed are those with sales in the calendar month of the date chosen.
   Periods, for the date you choose:
     Daily   : that day, against the day before. Trend: the last 15 days.
     Weekly  : the 7 days ending on that day, against the 7 days before. Trend: the last 8 weeks.
     Monthly : the calendar month, against the month before.
     MTD     : the 1st of the month to that day, against the same days of the month before.
   ============================================================ */
const DISCOUNT_SHEET_ID = "PASTE_DISCOUNT_SHEET_ID_HERE";
const DAY_MS = 86400000;
const KINDS = ["daily", "weekly", "monthly", "mtd"];

function doGet() {
  return HtmlService.createHtmlOutputFromFile("Index").setTitle("Discount Dashboard");
}

/* ---------- Reading the sheet ---------- */
function loadDisc_() {
  const ss = SpreadsheetApp.openById(DISCOUNT_SHEET_ID);
  const tz = Session.getScriptTimeZone();
  const readTab_ = name => {
    const sh = ss.getSheetByName(name);
    if (!sh) throw new Error("The tab " + name + " was not found. Run write_discount_sheet.py first.");
    const last = sh.getLastRow();
    return last < 2 ? [] : sh.getRange(2, 1, last - 1, sh.getLastColumn()).getValues();
  };
  const dateOf_ = v => (v instanceof Date ? Utilities.formatDate(v, tz, "yyyy-MM-dd") : String(v).slice(0, 10));
  const labelRows = readTab_("Disc_Label").map(v => ({
    d: dateOf_(v[0]), brand: String(v[1]), region: String(v[2]), source: String(v[3]), session: String(v[4]),
    label: String(v[5]), inv: Number(v[6]) || 0, qty: Number(v[7]) || 0, gross: Number(v[8]) || 0,
    disc: Number(v[9]) || 0, net: Number(v[10]) || 0, lines: Number(v[11]) || 0
  })).filter(r => r.d.length === 10);
  const itemRows = readTab_("Disc_Item").map(v => ({
    d: dateOf_(v[0]), brand: String(v[1]), region: String(v[2]), source: String(v[3]), session: String(v[4]),
    label: String(v[5]), item: String(v[6]), cat: String(v[7]), inv: Number(v[8]) || 0, qty: Number(v[9]) || 0,
    gross: Number(v[10]) || 0, disc: Number(v[11]) || 0, net: Number(v[12]) || 0, lines: Number(v[13]) || 0
  })).filter(r => r.d.length === 10);
  return { labelRows: labelRows, itemRows: itemRows };
}

/* ---------- Dates (day numbers count days since 1970-01-01, in UTC) ---------- */
function dn_(s) { const p = s.split("-").map(Number); return Math.round(Date.UTC(p[0], p[1] - 1, p[2]) / DAY_MS); }
function ymd_(n) { return new Date(n * DAY_MS).toISOString().slice(0, 10); }
function monthStart_(n) { const d = new Date(n * DAY_MS); return Math.round(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1) / DAY_MS); }
function monthEnd_(n) { const d = new Date(n * DAY_MS); return Math.round(Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 0) / DAY_MS); }
function rangeText_(w) { return w[0] === w[1] ? ymd_(w[0]) : ymd_(w[0]) + " to " + ymd_(w[1]); }

/* ---------- Periods ---------- */
function windows_(kind, anchor) {
  const a = dn_(anchor);
  const ms = monthStart_(a);
  if (kind === "weekly") return { cur: [a - 6, a], prev: [a - 13, a - 7], note: "the 7 days ending on the date, against the 7 days before" };
  if (kind === "monthly") return { cur: [ms, monthEnd_(a)], prev: [monthStart_(ms - 1), monthEnd_(ms - 1)], note: "the calendar month, against the month before" };
  if (kind === "mtd") {
    const pms = monthStart_(ms - 1);
    return { cur: [ms, a], prev: [pms, Math.min(pms + (a - ms), monthEnd_(ms - 1))], note: "month to date, against the same days of the month before" };
  }
  return { cur: [a, a], prev: [a - 1, a - 1], note: "the day, against the day before" };
}

// Trend buckets, oldest first. The first bucket is only used for the growth of the second.
function trendWindows_(kind, anchor) {
  const a = dn_(anchor);
  const out = [];
  if (kind === "daily") { for (let k = 15; k >= 0; k--) out.push([a - k, a - k]); }
  if (kind === "weekly") { for (let k = 8; k >= 0; k--) out.push([a - 7 * k - 6, a - 7 * k]); }
  return out;
}

/* ---------- Totals ---------- */
function blank_() { return { inv: 0, qty: 0, gross: 0, disc: 0, net: 0, lines: 0 }; }
function add_(a, r) {
  a.inv += r.inv || 0; a.qty += r.qty || 0; a.gross += r.gross || 0;
  a.disc += r.disc || 0; a.net += r.net || 0; a.lines += r.lines || 0;
  return a;
}

/* ---------- The dashboard ---------- */
// data: { labelRows, itemRows } as read from the sheet.
// p: { kind, anchor 'YYYY-MM-DD', brand, region, source, session ('all' = no filter),
//      codes (array of discount codes; empty = all codes) }
function computeDiscount_(data, p) {
  const kind = KINDS.indexOf(p.kind) >= 0 ? p.kind : "monthly";
  const w = windows_(kind, p.anchor);
  const f = { brand: p.brand || "all", region: p.region || "all", source: p.source || "all", session: p.session || "all" };
  const codes = Array.isArray(p.codes) ? p.codes.filter(c => c) : (p.code && p.code !== "all" ? [p.code] : []);
  const codeSet = new Set(codes);
  const inCodes = label => codeSet.size === 0 || codeSet.has(label);
  const passF = r => (f.brand === "all" || r.brand === f.brand) && (f.region === "all" || r.region === f.region) &&
                     (f.source === "all" || r.source === f.source) && (f.session === "all" || r.session === f.session);
  const inWin = (r, win) => { const n = dn_(r.d); return n >= win[0] && n <= win[1]; };
  const sumBy_ = (rows, keyFn, win, extra) => {
    const m = new Map();
    for (const r of rows) {
      if (!inWin(r, win) || !passF(r) || (extra && !extra(r))) continue;
      const k = keyFn(r);
      m.set(k, add_(m.get(k) || blank_(), r));
    }
    return m;
  };

  // Discount codes: only those with sales in the calendar month of the date chosen, and only the selected codes.
  const a = dn_(p.anchor);
  const monthWin = [monthStart_(a), monthEnd_(a)];
  const curL = sumBy_(data.labelRows, r => r.label, w.cur);
  const prvL = sumBy_(data.labelRows, r => r.label, w.prev);
  const activeL = sumBy_(data.labelRows, r => r.label, monthWin);
  const labels = [...activeL.keys()].filter(inCodes).map(k => ({
    label: k, cur: curL.get(k) || blank_(), prev: prvL.get(k) || blank_()
  }));

  // Trend for the selected codes (all codes when none are selected).
  const trend = trendWindows_(kind, p.anchor).map(win => {
    const acc = blank_();
    for (const r of data.labelRows) {
      if (!inWin(r, win) || !passF(r) || !inCodes(r.label)) continue;
      add_(acc, r);
    }
    return { range: rangeText_(win), inv: acc.inv, qty: acc.qty, gross: acc.gross, disc: acc.disc, net: acc.net, lines: acc.lines };
  });

  // Items for the selected codes, current period, with each item's category.
  const curI = sumBy_(data.itemRows, r => r.item + "\u0001" + r.cat, w.cur, r => inCodes(r.label));
  const items = [...curI.entries()].map(([k, x]) => {
    const parts = k.split("\u0001");
    return { item: parts[0], cat: parts[1], cur: x };
  });

  const total_ = m => [...m.values()].reduce((t, x) => add_(t, x), blank_());
  const selCur = new Map([...curL.entries()].filter(([k]) => inCodes(k)));
  const selPrv = new Map([...prvL.entries()].filter(([k]) => inCodes(k)));
  const scope = codes.length === 0 ? "all" : (codes.length === 1 ? codes[0] : codes.length + " selected codes");
  return {
    meta: { kind: kind, anchor: p.anchor, codes: codes, code: scope, cur: rangeText_(w.cur), prev: rangeText_(w.prev),
            note: w.note, activeMonth: rangeText_(monthWin) },
    labels: labels,
    items: items,
    totals: { cur: total_(selCur), prev: total_(selPrv) },
    overall: { cur: total_(curL), prev: total_(prvL) },
    trend: { kind: (kind === "daily" || kind === "weekly") ? kind : "", rows: trend }
  };
}

/* ---------- Called from the page ---------- */
function getDiscountOptions() {
  const data = loadDisc_();
  const uniq = list => [...new Set(list)].sort();
  const days = data.labelRows.map(r => r.d).sort();
  return {
    first: days.length ? days[0] : "", latest: days.length ? days[days.length - 1] : "",
    brands: uniq(data.labelRows.map(r => r.brand)),
    regions: uniq(data.labelRows.map(r => r.region)),
    sources: uniq(data.labelRows.map(r => r.source)),
    sessions: uniq(data.labelRows.map(r => r.session)),
    labels: uniq(data.labelRows.map(r => r.label)),
    categories: uniq(data.itemRows.map(r => r.cat))
  };
}
function getDiscountData(p) { return computeDiscount_(loadDisc_(), p); }

/* ---------- Live: today so far (business day 9 AM to 5 AM), against the same hours last week ---------- */
// Business hour: 0 = 9 AM, 19 = 4 AM. Only complete hours are compared. Hours still running are left out.
function liveContext_(now) {
  const t = now.getTime();
  const istHour = new Date(t + 5.5 * 3600000).getUTCHours();          // India time
  const istDate = new Date(t + 5.5 * 3600000).toISOString().slice(0, 10);
  let liveDay, cutBh;
  if (istHour >= 9) { liveDay = istDate; cutBh = istHour - 9; }                // day started at 9 AM today
  else if (istHour < 5) { liveDay = ymd_(dn_(istDate) - 1); cutBh = istHour + 15; }  // after midnight: still yesterday's day
  else { liveDay = ymd_(dn_(istDate) - 1); cutBh = 20; }                        // 5 to 9 AM: yesterday's day is complete
  return { liveDay: liveDay, cutBh: cutBh };
}
function hourLabel_(bh) {
  const h = (bh + 9) % 24;
  return ((h % 12) || 12) + " " + (h < 12 ? "AM" : "PM");
}
function loadHour_() {
  const ss = SpreadsheetApp.openById(DISCOUNT_SHEET_ID);
  const tz = Session.getScriptTimeZone();
  const sh = ss.getSheetByName("Disc_Hour");
  if (!sh) throw new Error("The tab Disc_Hour was not found. Run the Discount Code rows workflow first.");
  const last = sh.getLastRow();
  const vals = last < 2 ? [] : sh.getRange(2, 1, last - 1, sh.getLastColumn()).getValues();
  return vals.map(v => ({
    d: v[0] instanceof Date ? Utilities.formatDate(v[0], tz, "yyyy-MM-dd") : String(v[0]).slice(0, 10),
    bh: Number(v[1]) || 0, brand: String(v[2]), region: String(v[3]), source: String(v[4]), session: String(v[5]),
    label: String(v[6]), inv: Number(v[7]) || 0, qty: Number(v[8]) || 0, gross: Number(v[9]) || 0,
    disc: Number(v[10]) || 0, net: Number(v[11]) || 0, lines: Number(v[12]) || 0
  })).filter(r => r.d.length === 10);
}
// p: { brand, region, source, session, codes, now (optional ISO time, for testing) }
function computeDiscountLive_(hourRows, p) {
  const now = p.now ? new Date(p.now) : new Date();
  const ctx = liveContext_(now);
  const lwDay = ymd_(dn_(ctx.liveDay) - 7);
  const codes = Array.isArray(p.codes) ? p.codes.filter(c => c) : [];
  const codeSet = new Set(codes);
  const inCodes = label => codeSet.size === 0 || codeSet.has(label);
  const f = { brand: p.brand || "all", region: p.region || "all", source: p.source || "all", session: p.session || "all" };
  const passF = r => (f.brand === "all" || r.brand === f.brand) && (f.region === "all" || r.region === f.region) &&
                     (f.source === "all" || r.source === f.source) && (f.session === "all" || r.session === f.session);
  const overallCur = blank_(), overallPrev = blank_(), selCur = blank_(), selPrev = blank_();
  const labelCur = new Map(), labelPrev = new Map(), hourCur = new Map(), hourPrev = new Map();
  const addTo = (m, key, r) => { m.set(key, add_(m.get(key) || blank_(), r)); };
  for (const r of hourRows) {
    if (r.bh >= ctx.cutBh || !passF(r)) continue;
    const isCur = r.d === ctx.liveDay, isPrev = r.d === lwDay;
    if (!isCur && !isPrev) continue;
    if (isCur) add_(overallCur, r); else add_(overallPrev, r);
    if (!inCodes(r.label)) continue;
    if (isCur) { add_(selCur, r); addTo(labelCur, r.label, r); addTo(hourCur, r.bh, r); }
    else { add_(selPrev, r); addTo(labelPrev, r.label, r); addTo(hourPrev, r.bh, r); }
  }
  const labelKeys = [...new Set([...labelCur.keys(), ...labelPrev.keys()])];
  const labels = labelKeys.map(k => ({ label: k, cur: labelCur.get(k) || blank_(), prev: labelPrev.get(k) || blank_() }));
  const hours = [];
  for (let bh = 0; bh < ctx.cutBh; bh++) {
    hours.push({ label: hourLabel_(bh), bh: bh, cur: hourCur.get(bh) || blank_(), prev: hourPrev.get(bh) || blank_() });
  }
  const note = ctx.cutBh === 0
    ? "No complete hour yet. The business day starts at 9 AM."
    : "Complete hours: 9 AM to " + hourLabel_(ctx.cutBh - 1) + ". Hours still running are not counted.";
  const scope = codes.length === 0 ? "all" : (codes.length === 1 ? codes[0] : codes.length + " selected codes");
  return {
    meta: { kind: "live", liveDay: ctx.liveDay, lwDay: lwDay, codes: codes, code: scope,
            cur: ctx.liveDay + " so far", prev: lwDay + ", same hours", activeMonth: "today so far", note: note },
    labels: labels,
    hours: hours,
    items: [],
    totals: { cur: selCur, prev: selPrev },
    overall: { cur: overallCur, prev: overallPrev },
    trend: { kind: "", rows: [] }
  };
}
function getDiscountLive(p) { return computeDiscountLive_(loadHour_(), p); }

/* ---------- Product performance and discount analysis ----------
   Tabs: Disc_Bucket (orders and sales by AOV and discount band), Disc_Prod (items split by whether the order
   had a discount), Disc_Prod_Bucket (items inside each band). Online only (Swiggy and Zomato).
   Bands are the same as the Sales dashboard. Product tabs cover the last 62 completed days. */
const AOV_ORDER = ["0-100", "100-200", "200-300", "300-400", "400-500", "500-600", "600-900", ">900"];
const DISC_ORDER = ["0%", "1%-10%", "10%-20%", "20%-30%", "30%-40%", "40%-50%", "50%-60%", "60%-70%", "70%-80%", "80%-90%", "90%-100%"];

function readTab_(name) {
  const sh = SpreadsheetApp.openById(DISCOUNT_SHEET_ID).getSheetByName(name);
  if (!sh) throw new Error("The tab " + name + " was not found. Run write_discount_sheet.py first.");
  const last = sh.getLastRow();
  return last < 2 ? [] : sh.getRange(2, 1, last - 1, sh.getLastColumn()).getValues();
}
function dateText_(v) {
  return v instanceof Date ? Utilities.formatDate(v, Session.getScriptTimeZone(), "yyyy-MM-dd") : String(v).slice(0, 10);
}
// read(name) returns the rows of a tab (no header). The page uses the sheet; tests pass their own rows.
function loadBucket_(read) {
  const rd = read || readTab_;
  return rd("Disc_Bucket").map(v => ({
    d: dateText_(v[0]), brand: String(v[1]), region: String(v[2]), source: String(v[3]), session: String(v[4]),
    kind: String(v[5]), bucket: String(v[6]), inv: Number(v[7]) || 0, qty: Number(v[8]) || 0,
    gross: Number(v[9]) || 0, disc: Number(v[10]) || 0, net: Number(v[11]) || 0
  })).filter(r => r.d.length === 10);
}
function loadProd_(read) {
  const rd = read || readTab_;
  return rd("Disc_Prod").map(v => ({
    d: dateText_(v[0]), brand: String(v[1]), region: String(v[2]), source: String(v[3]), yes: String(v[4]) === "Yes",
    cat: String(v[5]), item: String(v[6]), inv: Number(v[7]) || 0, qty: Number(v[8]) || 0,
    gross: Number(v[9]) || 0, disc: Number(v[10]) || 0, net: Number(v[11]) || 0
  })).filter(r => r.d.length === 10);
}
function loadProdBucket_(read) {
  const rd = read || readTab_;
  return rd("Disc_Prod_Bucket").map(v => ({
    d: dateText_(v[0]), brand: String(v[1]), region: String(v[2]), source: String(v[3]), kind: String(v[4]),
    bucket: String(v[5]), cat: String(v[6]), item: String(v[7]), inv: Number(v[8]) || 0, qty: Number(v[9]) || 0,
    gross: Number(v[10]) || 0, disc: Number(v[11]) || 0, net: Number(v[12]) || 0
  })).filter(r => r.d.length === 10);
}
function pFilter_(p) { return { brand: p.brand || "all", region: p.region || "all", source: p.source || "all", session: p.session || "all" }; }
function passP_(r, f) {
  return (f.brand === "all" || r.brand === f.brand) && (f.region === "all" || r.region === f.region) &&
         (f.source === "all" || r.source === f.source);
}
function inWin_(r, win) { const n = dn_(r.d); return n >= win[0] && n <= win[1]; }
function sideBlank_() { return { yes: blank_(), no: blank_() }; }
function sideNet_(s) { return s.yes.net + s.no.net; }

// Products: each item's sales split into orders with a discount (yes) and without (no), this period against the one before.
function computeProducts_(rows, p) {
  const kind = KINDS.indexOf(p.kind) >= 0 ? p.kind : "monthly";
  const w = windows_(kind, p.anchor);
  const f = pFilter_(p);
  const totCur = sideBlank_(), totPrev = sideBlank_();
  const items = new Map();
  let first = null, last = null;
  for (const r of rows) {
    if (!first || r.d < first) first = r.d;
    if (!last || r.d > last) last = r.d;
    if (!passP_(r, f)) continue;
    const isCur = inWin_(r, w.cur), isPrev = !isCur && inWin_(r, w.prev);
    if (!isCur && !isPrev) continue;
    const side = r.yes ? "yes" : "no";
    const key = r.cat + "\u0001" + r.item;
    if (!items.has(key)) items.set(key, { item: r.item, cat: r.cat, cur: sideBlank_(), prev: sideBlank_() });
    const e = items.get(key);
    if (isCur) { add_(e.cur[side], r); add_(totCur[side], r); }
    else { add_(e.prev[side], r); add_(totPrev[side], r); }
  }
  const products = [...items.values()]
    .filter(e => sideNet_(e.cur) > 0 || sideNet_(e.prev) > 0)
    .sort((x, y) => sideNet_(y.cur) - sideNet_(x.cur));
  const catMap = new Map();
  for (const e of products) {
    if (!catMap.has(e.cat)) catMap.set(e.cat, { cat: e.cat, cur: sideBlank_(), prev: sideBlank_() });
    const c = catMap.get(e.cat);
    for (const s of ["yes", "no"]) { add_(c.cur[s], e.cur[s]); add_(c.prev[s], e.prev[s]); }
  }
  const categories = [...catMap.values()].sort((x, y) => sideNet_(y.cur) - sideNet_(x.cur));
  return {
    meta: { kind: kind, cur: rangeText_(w.cur), prev: rangeText_(w.prev), note: w.note,
            dataFrom: first || "", dataTo: last || "" },
    totals: { cur: totCur, prev: totPrev },
    products: products,
    categories: categories
  };
}

// Bands: orders and sales per band, this period against the one before, and the items inside each band (current period).
function computeBuckets_(bucketRows, drillRows, p) {
  const kind = KINDS.indexOf(p.kind) >= 0 ? p.kind : "monthly";
  const w = windows_(kind, p.anchor);
  const f = pFilter_(p);
  const bandSets = { AOV: AOV_ORDER, Discount: DISC_ORDER };
  const out = {};
  for (const name of Object.keys(bandSets)) {
    const order = bandSets[name];
    const idx = new Map(order.map((lb, i) => [lb, i]));
    const bands = order.map(lb => ({ label: lb, cur: blank_(), prev: blank_(), items: new Map() }));
    for (const r of bucketRows) {
      if (r.kind !== name || !passP_(r, f) || (f.session !== "all" && r.session !== f.session)) continue;
      const isCur = inWin_(r, w.cur), isPrev = !isCur && inWin_(r, w.prev);
      if (!isCur && !isPrev) continue;
      const bi = idx.get(r.bucket);
      if (bi === undefined) continue;
      add_(isCur ? bands[bi].cur : bands[bi].prev, r);
    }
    for (const r of drillRows) {
      if (r.kind !== name || !passP_(r, f) || !inWin_(r, w.cur)) continue;
      const bi = idx.get(r.bucket);
      if (bi === undefined) continue;
      const m = bands[bi].items, key = r.cat + "\u0001" + r.item;
      if (!m.has(key)) m.set(key, { item: r.item, cat: r.cat, v: blank_() });
      add_(m.get(key).v, r);
    }
    out[name] = bands.map(b => ({
      label: b.label, cur: b.cur, prev: b.prev,
      items: [...b.items.values()].sort((x, y) => y.v.net - x.v.net).slice(0, 200)
    }));
  }
  const sumBands = (arr, key) => arr.reduce((acc, b) => add_(acc, b[key]), blank_());
  return {
    meta: { kind: kind, cur: rangeText_(w.cur), prev: rangeText_(w.prev), note: w.note },
    aov: out.AOV,
    discount: out.Discount,
    totals: { cur: sumBands(out.AOV, "cur"), prev: sumBands(out.AOV, "prev") }
  };
}

function getProductData(p) { return computeProducts_(loadProd_(), p); }
function getBucketData(p) { return computeBuckets_(loadBucket_(), loadProdBucket_(), p); }
