/* ============================================================
   Discount Dashboard (Google Apps Script)
   Discount Code sales for Swiggy and Zomato orders, and product sales for all channels.
   Reads the tabs written by write_discount_sheet.py: Disc_Label, Disc_Item, Disc_Hour, Disc_Bucket,
   Disc_Prod, Disc_Prod_Bucket, Disc_Cat, Disc_Prod_Month.
   Set DISCOUNT_SHEET_ID to the Google Sheet that holds those tabs.

   Charges and other differences are not products: they are left out of the product and category views.
   Txn (orders) in a category counts each order once, from the (all items) rows the sheet writes.
   Periods, for the date you choose:
     Daily   : that day, against the day before. Trend: the last 15 days.
     Weekly  : the 7 days ending on that day, against the 7 days before. Trend: the last 8 weeks.
     Monthly : the calendar month, against the month before.
     MTD     : the 1st of the month to that day, against the same days of the month before.
     Custom  : the dates you choose, against the same number of days before.
   ============================================================ */
const DISCOUNT_SHEET_ID = "PASTE_DISCOUNT_SHEET_ID_HERE";
const DAY_MS = 86400000;
const KINDS = ["daily", "weekly", "monthly", "mtd", "custom"];
const ALL_ITEMS_ = "(all items)";
const ALL_CATEGORIES_ = "(all categories)";
const NON_PRODUCT_ = ["Charges (not a product)", "Other differences (not a product)"];
const MONTH_NAMES_ = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const AOV_ORDER = ["0-100", "100-200", "200-300", "300-400", "400-500", "500-600", "600-900", ">900"];
const DISC_ORDER = ["0%", "1%-10%", "10%-20%", "20%-30%", "30%-40%", "40%-50%", "50%-60%", "60%-70%", "70%-80%", "80%-90%", "90%-100%"];

function doGet() {
  return HtmlService.createHtmlOutputFromFile("Index").setTitle("Discount Dashboard");
}

function isProduct_(name) { return NON_PRODUCT_.indexOf(String(name)) < 0; }

/* ---------- Reading the sheet ---------- */
function readTab_(name, ss) {
  const book = ss || SpreadsheetApp.openById(DISCOUNT_SHEET_ID);
  const sh = book.getSheetByName(name);
  if (!sh) throw new Error("The tab " + name + " was not found. Run write_discount_sheet.py first.");
  const last = sh.getLastRow();
  return last < 2 ? [] : sh.getRange(2, 1, last - 1, sh.getLastColumn()).getValues();
}
function dateText_(v) {
  return v instanceof Date ? Utilities.formatDate(v, Session.getScriptTimeZone(), "yyyy-MM-dd") : String(v).slice(0, 10);
}
// read(name) returns the rows of a tab (no header). The page uses the sheet; tests pass their own rows.
/* ---------- Data: the Sheet (open months) and the GitHub archive (closed months, day level) ----------
   The Sheet holds the current month and the month before. Each month that has ended is in the archive: one gzipped
   CSV per month in the GitHub repository (data/discount/archive), listed in index.json. A month comes from the
   archive if it is listed there, otherwise from the Sheet, so nothing is counted twice.
   Set ARCHIVE_REPO_ to your repository ("owner/name"). For a private repository, add the token as the script
   property GITHUB_TOKEN (Project Settings > Script properties). */
const ARCHIVE_REPO_ = "OWNER/REPO";
const ARCHIVE_BRANCH_ = "main";
const ARCHIVE_PATH_ = "data/discount/archive";
let ARCHIVE_MEMO_ = null;        // one request: the archive index, and the months already read

// The file from the archive, or null when it is not there (not archived yet).
function archiveFetch_(file) {
  if (ARCHIVE_REPO_ === "OWNER/REPO") throw new Error("Set ARCHIVE_REPO_ in Code.gs to your GitHub repository (owner/name).");
  const token = PropertiesService.getScriptProperties().getProperty("GITHUB_TOKEN");
  const url = "https://raw.githubusercontent.com/" + ARCHIVE_REPO_ + "/" + ARCHIVE_BRANCH_ + "/" + ARCHIVE_PATH_ + "/" + file;
  const res = UrlFetchApp.fetch(url, { muteHttpExceptions: true, headers: token ? { Authorization: "token " + token } : {} });
  const code = res.getResponseCode();
  if (code === 404) return null;
  if (code !== 200) throw new Error("Could not read " + file + " from the archive (status " + code + ").");
  return res.getBlob();
}
// The archived months, oldest first. No index yet means no archived months.
function archiveMonths_() {
  if (!ARCHIVE_MEMO_) ARCHIVE_MEMO_ = { months: null, read: {} };
  if (ARCHIVE_MEMO_.months === null) {
    const blob = archiveFetch_("index.json");
    ARCHIVE_MEMO_.months = blob ? (JSON.parse(blob.getDataAsString()).months || []).slice().sort() : [];
  }
  return ARCHIVE_MEMO_.months;
}
// One archived month: label rows and item rows, in the same shape as the Sheet rows.
// Columns: kind, day, brand, region, source, session, label, item, category, invoices, qty, gross, discount, net, lines.
function archiveMonth_(month) {
  if (!ARCHIVE_MEMO_.read[month]) {
    const blob = archiveFetch_(month + ".csv.gz");
    if (!blob) throw new Error("The archive is missing " + month + ". Check the archive folder in GitHub.");
    const rows = Utilities.parseCsv(Utilities.ungzip(blob).getDataAsString()).slice(1);
    const labelRows = [], itemRows = [];
    for (const v of rows) {
      if (v.length < 15) continue;
      const base = { d: v[1], brand: v[2], region: v[3], source: v[4], session: v[5], label: v[6] };
      const nums = { inv: Number(v[9]) || 0, qty: Number(v[10]) || 0, gross: Number(v[11]) || 0,
                     disc: Number(v[12]) || 0, net: Number(v[13]) || 0, lines: Number(v[14]) || 0 };
      if (v[0] === "label") labelRows.push(Object.assign(base, nums));
      else if (isProduct_(v[7])) itemRows.push(Object.assign(base, { item: v[7], cat: v[8] }, nums));
    }
    ARCHIVE_MEMO_.read[month] = { labelRows: labelRows, itemRows: itemRows };
  }
  return ARCHIVE_MEMO_.read[month];
}
// The Sheet's open months, plus the archived months asked for (months = "YYYY-MM" list; omitted = all archived).
function loadDisc_(read, months) {
  const rd = read || readTab_;
  const sheetLabel = rd("Disc_Label").map(v => ({
    d: dateText_(v[0]), brand: String(v[1]), region: String(v[2]), source: String(v[3]), session: String(v[4]),
    label: String(v[5]), inv: Number(v[6]) || 0, qty: Number(v[7]) || 0, gross: Number(v[8]) || 0,
    disc: Number(v[9]) || 0, net: Number(v[10]) || 0, lines: Number(v[11]) || 0
  })).filter(r => r.d.length === 10);
  const sheetItem = rd("Disc_Item").map(v => ({
    d: dateText_(v[0]), brand: String(v[1]), region: String(v[2]), source: String(v[3]), session: String(v[4]),
    label: String(v[5]), item: String(v[6]), cat: String(v[7]), inv: Number(v[8]) || 0, qty: Number(v[9]) || 0,
    gross: Number(v[10]) || 0, disc: Number(v[11]) || 0, net: Number(v[12]) || 0, lines: Number(v[13]) || 0
  })).filter(r => r.d.length === 10 && isProduct_(r.item));
  const archived = archiveMonths_();
  const wanted = (months || archived).filter(m => archived.indexOf(m) >= 0);
  const closed = new Set(archived);
  const labelRows = sheetLabel.filter(r => !closed.has(r.d.slice(0, 7)));
  const itemRows = sheetItem.filter(r => !closed.has(r.d.slice(0, 7)));
  for (const m of wanted) {
    const a = archiveMonth_(m);
    for (const x of a.labelRows) labelRows.push(x);
    for (const x of a.itemRows) itemRows.push(x);
  }
  let sheetLatest = "";
  for (const r of sheetLabel) if (r.d > sheetLatest) sheetLatest = r.d;
  return { labelRows: labelRows, itemRows: itemRows, sheetLatest: sheetLatest };
}
// The "YYYY-MM" months that a set of day-number windows touches.
function monthsOfWindows_(wins) {
  const keys = new Set();
  for (const w of wins) for (let d = w[0]; d <= w[1]; d += 1) keys.add(ymd_(d).slice(0, 7));
  return [...keys];
}

/* ---------- Dates (day numbers count days since 1970-01-01, in UTC) ---------- */
function dn_(s) { const p = s.split("-").map(Number); return Math.round(Date.UTC(p[0], p[1] - 1, p[2]) / DAY_MS); }
function ymd_(n) { return new Date(n * DAY_MS).toISOString().slice(0, 10); }
function monthStart_(n) { const d = new Date(n * DAY_MS); return Math.round(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1) / DAY_MS); }
function monthEnd_(n) { const d = new Date(n * DAY_MS); return Math.round(Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 0) / DAY_MS); }
function rangeText_(w) { return w[0] === w[1] ? ymd_(w[0]) : ymd_(w[0]) + " to " + ymd_(w[1]); }
function isDate_(s) { return /^\d{4}-\d{2}-\d{2}$/.test(String(s || "")); }

/* ---------- Weeks, days and months ----------
   Weeks run Monday to Sunday and are numbered as in the ISO calendar (WK 45 = 2–8 Nov 2026). */
const DAY_NAMES_ = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
function dow_(n) { return (new Date(n * DAY_MS).getUTCDay() + 6) % 7; }   // Monday = 0 ... Sunday = 6
function mondayOf_(n) { return n - dow_(n); }
function isoWeekNo_(n) {
  const thu = mondayOf_(n) + 3;                                             // the Thursday of the same week
  const y = new Date(thu * DAY_MS).getUTCFullYear();
  const jan1 = Math.round(Date.UTC(y, 0, 1) / DAY_MS);
  return Math.floor((thu - jan1) / 7) + 1;
}
function weekLabel_(mon) {
  const a = new Date(mon * DAY_MS), b = new Date((mon + 6) * DAY_MS);
  const span = a.getUTCMonth() === b.getUTCMonth()
    ? a.getUTCDate() + "–" + b.getUTCDate() + " " + MONTH_NAMES_[b.getUTCMonth()]
    : a.getUTCDate() + " " + MONTH_NAMES_[a.getUTCMonth()] + " – " + b.getUTCDate() + " " + MONTH_NAMES_[b.getUTCMonth()];
  return "WK " + isoWeekNo_(mon) + " · " + span;
}
function dayLabel_(n) {
  const d = new Date(n * DAY_MS);
  return DAY_NAMES_[dow_(n)] + " " + d.getUTCDate() + " " + MONTH_NAMES_[d.getUTCMonth()];
}
function rangeLabel_(lo, hi) {
  const a = new Date(lo * DAY_MS), b = new Date(hi * DAY_MS);
  const A = a.getUTCDate() + " " + MONTH_NAMES_[a.getUTCMonth()] + " " + a.getUTCFullYear();
  const B = b.getUTCDate() + " " + MONTH_NAMES_[b.getUTCMonth()] + " " + b.getUTCFullYear();
  return lo === hi ? A : A + " – " + B;
}
// The same calendar date k months away (the day is kept, or the last day of a shorter month).
function addMonthsDn_(n, k) {
  const d = new Date(n * DAY_MS);
  const first = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + k, 1));
  const ty = first.getUTCFullYear(), tm = first.getUTCMonth();
  const last = new Date(Date.UTC(ty, tm + 1, 0)).getUTCDate();
  return Math.round(Date.UTC(ty, tm, Math.min(d.getUTCDate(), last)) / DAY_MS);
}

/* ---------- Periods ---------- */
// cmp: "last" = the previous period; "year" = the same period a year earlier.
function windows_(kind, anchor, cmp) {
  const a = dn_(anchor);
  const ms = monthStart_(a), me = monthEnd_(a);
  const year = cmp === "year";
  if (kind === "weekly") {
    const m = mondayOf_(a);
    return year
      ? { cur: [m, m + 6], prev: [addMonthsDn_(m, -12), addMonthsDn_(m, -12) + 6], vs: "same week last year",
          note: "the ISO week (Mon–Sun), against the same dates last year" }
      : { cur: [m, m + 6], prev: [m - 7, m - 1], vs: "previous week", note: "the ISO week (Mon–Sun), against the week before" };
  }
  if (kind === "monthly") {
    const py = monthStart_(addMonthsDn_(ms, -12));
    return year
      ? { cur: [ms, me], prev: [py, monthEnd_(py)], vs: "same month last year", note: "the calendar month, against the same month last year" }
      : { cur: [ms, me], prev: [monthStart_(ms - 1), monthEnd_(ms - 1)], vs: "previous month", note: "the calendar month, against the month before" };
  }
  if (kind === "mtd") {
    const back = year ? monthStart_(addMonthsDn_(ms, -12)) : monthStart_(ms - 1);
    return { cur: [ms, a], prev: [back, Math.min(back + (a - ms), monthEnd_(back))],
             vs: year ? "same days last year" : "same days last month",
             note: year ? "month to date, against the same days last year" : "month to date, against the same days of the month before" };
  }
  // daily
  return year
    ? { cur: [a, a], prev: [addMonthsDn_(a, -12), addMonthsDn_(a, -12)], vs: "same day last year", note: "the day, against the same day last year" }
    : { cur: [a, a], prev: [a - 7, a - 7], vs: "same weekday last week", note: "the day, against the same weekday last week" };
}
// The window for the chosen period. Custom uses the dates chosen, against the same number of days before (or the same dates last year).
function windowsFor_(p) {
  const kind = KINDS.indexOf(p.kind) >= 0 ? p.kind : "monthly";
  let cmp = p.cmp === "year" || p.cmp === "last" ? p.cmp : "auto";
  if (cmp === "auto") cmp = kind === "monthly" ? "year" : "last";
  let w;
  if (kind === "custom" && isDate_(p.from) && isDate_(p.to)) {
    let a = dn_(p.from), b = dn_(p.to);
    if (b < a) { const t = a; a = b; b = t; }
    const len = b - a + 1;
    w = cmp === "year"
      ? { kind: "custom", cur: [a, b], prev: [addMonthsDn_(a, -12), addMonthsDn_(b, -12)], vs: "same dates last year",
          note: "the chosen dates, against the same dates last year" }
      : { kind: "custom", cur: [a, b], prev: [a - len, a - 1], vs: "the days before",
          note: "the chosen dates, against the same number of days before" };
  } else {
    const base = kind === "custom" ? "monthly" : kind;
    w = Object.assign({ kind: base }, windows_(base, p.anchor, cmp));
  }
  // The current period stops at the last day with data, so a part-finished week or month is not compared as a whole one.
  if (isDate_(p.latest) && w.cur[1] > dn_(p.latest)) {
    w.cur = [w.cur[0], Math.max(w.cur[0], dn_(p.latest))];
    w.note += ", to " + ymd_(dn_(p.latest));
  }
  return w;
}

// Trend buckets, oldest first. Weeks are Monday to Sunday. The first bucket is only used for the growth of the second.
function trendWindows_(kind, anchor) {
  const a = dn_(anchor);
  const out = [];
  if (kind === "daily") { for (let k = 15; k >= 0; k--) out.push([a - k, a - k]); }
  if (kind === "weekly") { const m = mondayOf_(a); for (let k = 8; k >= 0; k--) out.push([m - 7 * k, m - 7 * k + 6]); }
  return out;
}

/* ---------- Totals ---------- */
function blank_() { return { inv: 0, qty: 0, gross: 0, disc: 0, net: 0, lines: 0 }; }
function add_(a, r) {
  a.inv += r.inv || 0; a.qty += r.qty || 0; a.gross += r.gross || 0;
  a.disc += r.disc || 0; a.net += r.net || 0; a.lines += r.lines || 0;
  return a;
}
function inWin_(r, win) { const n = dn_(r.d); return n >= win[0] && n <= win[1]; }

// Category block: category totals come from the (all items) rows, so Txn counts each order once. The items inside
// each category come from the item rows. keep(r) applies the filters.
function catBlock_(rows, win, keep) {
  const cats = new Map();
  for (const r of rows) {
    if (!inWin_(r, win) || !keep(r)) continue;
    if (!cats.has(r.cat)) cats.set(r.cat, { cat: r.cat, cur: blank_(), items: new Map(), hasAll: false });
    const c = cats.get(r.cat);
    if (r.item === ALL_ITEMS_) { add_(c.cur, r); c.hasAll = true; continue; }
    if (!c.items.has(r.item)) c.items.set(r.item, { item: r.item, cur: blank_() });
    add_(c.items.get(r.item).cur, r);
  }
  return [...cats.values()].map(c => {
    const items = [...c.items.values()].sort((a, b) => b.cur.net - a.cur.net);
    if (!c.hasAll) { c.cur = blank_(); items.forEach(x => add_(c.cur, x.cur)); }   // older data without (all items) rows
    return { cat: c.cat, cur: c.cur, items: items };
  }).sort((a, b) => b.cur.net - a.cur.net);
}

/* ---------- Offer types: discount codes grouped like the Swiggy offer-type sheet ----------
   Each discount label goes to one offer type. A label with several parts (for example "15% Off + TRYNEW") takes the
   first part that matches, in this order: Price-point (toing), Buy 1 Get 1, Items under Rs X, Flat Rs Off, then the
   highest percentage off. "Zero discount" is "No offer tag". Anything else is "Other". */
function singleOfferType_(s) {
  const t = String(s).trim().toLowerCase();
  if (t === "" || t === "zero discount") return "No offer tag";
  if (/toing/.test(t)) return "Price-point (toing)";
  if (/buy ?1 ?get ?1|b1g1|bogo/.test(t)) return "Buy 1 Get 1";
  if (/items? under/.test(t)) return "Items under Rs X";
  if (/^flat/.test(t)) return "Flat Rs Off";
  const m = t.match(/(\d{1,3})\s*% ?off/);
  if (m) return m[1] + "% Off";
  return "Other";
}
function offerTypeOf_(label) {
  const parts = String(label || "").split(" + ").map(x => x.trim()).filter(x => x);
  if (!parts.length) return "No offer tag";
  const kinds = parts.map(singleOfferType_);
  for (const k of ["Price-point (toing)", "Buy 1 Get 1", "Items under Rs X", "Flat Rs Off"]) {
    if (kinds.indexOf(k) >= 0) return k;
  }
  const pct = kinds.filter(k => /% Off$/.test(k)).sort((a, b) => parseInt(b, 10) - parseInt(a, 10));
  if (pct.length) return pct[0];
  return kinds.indexOf("No offer tag") >= 0 ? "No offer tag" : "Other";
}
// entries: [{ label, cur, prev }]. Returns the offer types, biggest net first, each with the codes inside it.
function groupOffers_(entries) {
  const m = new Map();
  for (const e of entries) {
    const type = offerTypeOf_(e.label);
    if (!m.has(type)) m.set(type, { type: type, cur: blank_(), prev: blank_(), labels: [] });
    const o = m.get(type);
    add_(o.cur, e.cur);
    add_(o.prev, e.prev);
    o.labels.push({ label: e.label, cur: e.cur, prev: e.prev });
  }
  return [...m.values()]
    .map(o => ({ type: o.type, cur: o.cur, prev: o.prev, labels: o.labels.sort((a, b) => b.cur.net - a.cur.net) }))
    .sort((a, b) => b.cur.net - a.cur.net);
}

/* ---------- Hourly product rows: Disc_Item_Hour (last week, nightly) and Disc_Live_Item (today, live job) ---------- */
// A missing tab gives no rows, so the page still loads and shows that there are no categories yet.
function loadItemHour_(name, read) {
  const rd = read || readTab_;
  try {
    return rd(name).map(v => ({
      d: dateText_(v[0]), bh: Number(v[1]) || 0, brand: String(v[2]), region: String(v[3]), source: String(v[4]),
      session: String(v[5]), label: String(v[6]), item: String(v[7]), cat: String(v[8]),
      inv: Number(v[9]) || 0, qty: Number(v[10]) || 0, gross: Number(v[11]) || 0, disc: Number(v[12]) || 0,
      net: Number(v[13]) || 0, lines: Number(v[14]) || 0
    })).filter(r => r.d.length === 10 && isProduct_(r.item));
  } catch (err) {
    return [];
  }
}
function loadLiveItems_(read) {
  return [...loadItemHour_("Disc_Item_Hour", read), ...loadItemHour_("Disc_Live_Item", read)];
}
// Adds the previous period's figures to each category and item (matched by name).
function attachPrev_(curCats, prvCats) {
  const byCat = new Map(prvCats.map(c => [c.cat, c]));
  return curCats.map(c => {
    const pc = byCat.get(c.cat);
    const prevItems = new Map(pc ? pc.items.map(x => [x.item, x.cur]) : []);
    return Object.assign({}, c, {
      prev: pc ? pc.cur : blank_(),
      items: c.items.map(x => Object.assign({}, x, { prev: prevItems.get(x.item) || blank_() }))
    });
  });
}

// Zomato is shown by voucher code, as on the Zomato sheet: one row per code, no offer-type grouping.
function groupCodes_(entries) {
  return entries.map(e => ({ type: e.label, cur: e.cur, prev: e.prev, labels: [] }))
    .sort((a, b) => b.cur.net - a.cur.net);
}
// Offer types by platform (Swiggy by offer type, Zomato by voucher code). rows need d, label and source;
// curWin and prevWin are day-number ranges.
function offersBySource_(rows, curWin, prevWin, keep) {
  const sources = new Map();
  for (const r of rows) {
    if (!keep(r)) continue;
    const inCur = inWin_(r, curWin), inPrev = !inCur && inWin_(r, prevWin);
    if (!inCur && !inPrev) continue;
    if (!sources.has(r.source)) sources.set(r.source, new Map());
    const m = sources.get(r.source);
    if (!m.has(r.label)) m.set(r.label, { label: r.label, cur: blank_(), prev: blank_() });
    add_(inCur ? m.get(r.label).cur : m.get(r.label).prev, r);
  }
  const out = {};
  [...sources.keys()].sort((x, y) => (x === "Swiggy" ? -1 : y === "Swiggy" ? 1 : x.localeCompare(y)))
    .forEach(src => {
      const entries = [...sources.get(src).values()];
      out[src] = src === "Zomato" ? groupCodes_(entries) : groupOffers_(entries);
    });
  return out;
}

/* ---------- The dashboard (Discount Codes) ---------- */
// data: { labelRows, itemRows } as read from the sheet.
// p: { kind, anchor 'YYYY-MM-DD', from, to, cmp ('auto' | 'last' | 'year'), latest, brand, region, source, session, codes }
function computeDiscount_(data, p) {
  const w = windowsFor_(p);
  const kind = w.kind;
  const f = { brand: p.brand || "all", region: p.region || "all", source: p.source || "all", session: p.session || "all" };
  const codes = Array.isArray(p.codes) ? p.codes.filter(c => c) : (p.code && p.code !== "all" ? [p.code] : []);
  const codeSet = new Set(codes);
  const inCodes = label => codeSet.size === 0 || codeSet.has(label);
  const passF = r => (f.brand === "all" || r.brand === f.brand) && (f.region === "all" || r.region === f.region) &&
                     (f.source === "all" || r.source === f.source) && (f.session === "all" || r.session === f.session);
  const sumBy_ = (rows, keyFn, win) => {
    const m = new Map();
    for (const r of rows) {
      if (!inWin_(r, win) || !passF(r)) continue;
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
      if (!inWin_(r, win) || !passF(r) || !inCodes(r.label)) continue;
      add_(acc, r);
    }
    return { range: kind === "weekly" ? weekLabel_(win[0]) : rangeText_(win),
             inv: acc.inv, qty: acc.qty, gross: acc.gross, disc: acc.disc, net: acc.net, lines: acc.lines };
  });

  // Categories and items for the selected codes, current period; offer types by platform, with the codes inside.
  const cats = catBlock_(data.itemRows, w.cur, r => passF(r) && inCodes(r.label));
  const offers = offersBySource_(data.labelRows, w.cur, w.prev, r => passF(r) && inCodes(r.label));
  const prevHasData = data.labelRows.some(r => passF(r) && inWin_(r, w.prev));

  const total_ = m => [...m.values()].reduce((t, x) => add_(t, x), blank_());
  const selCur = new Map([...curL.entries()].filter(([k]) => inCodes(k)));
  const selPrv = new Map([...prvL.entries()].filter(([k]) => inCodes(k)));
  const scope = codes.length === 0 ? "all" : (codes.length === 1 ? codes[0] : codes.length + " selected codes");
  return {
    meta: { kind: kind, anchor: p.anchor, codes: codes, code: scope,
            cur: rangeText_(w.cur), prev: rangeText_(w.prev),
            curLabel: kind === "weekly" ? weekLabel_(w.cur[0]) : rangeText_(w.cur),
            prevLabel: kind === "weekly" ? weekLabel_(w.prev[0]) : rangeText_(w.prev),
            note: w.note, vs: w.vs, prevHasData: prevHasData, activeMonth: rangeText_(monthWin) },
    labels: labels,
    cats: cats,
    offers: offers,
    totals: { cur: total_(selCur), prev: total_(selPrv) },
    overall: { cur: total_(curL), prev: total_(prvL) },
    trend: { kind: (kind === "daily" || kind === "weekly") ? kind : "", rows: trend }
  };
}

/* ---------- Called from the page ---------- */
// Filter choices: brands, regions, sources and sessions from every tab; the labels and codes from the Overview tab.
function optionsFrom_(labelRows, itemRows, prodRows, bucketRows) {
  const uniq = list => [...new Set(list.filter(x => x !== "" && x !== undefined && x !== null))].sort();
  const days = labelRows.map(r => r.d).sort();
  return {
    first: days.length ? days[0] : "", latest: days.length ? days[days.length - 1] : "",
    brands: uniq([...labelRows.map(r => r.brand), ...prodRows.map(r => r.brand), ...bucketRows.map(r => r.brand)]),
    regions: uniq([...labelRows.map(r => r.region), ...prodRows.map(r => r.region), ...bucketRows.map(r => r.region)]),
    sources: uniq([...labelRows.map(r => r.source), ...prodRows.map(r => r.source), ...bucketRows.map(r => r.source)]),
    sessions: uniq([...labelRows.map(r => r.session), ...bucketRows.map(r => r.session)]),
    labels: uniq(labelRows.map(r => r.label)),
    categories: uniq([...itemRows.map(r => r.cat), ...prodRows.map(r => r.cat)])
  };
}
function getDiscountOptions() {
  const archived = archiveMonths_();
  const data = loadDisc_(readTab_, archived.slice(-2));   // the latest archived months, so their codes are in the list
  const o = optionsFrom_(data.labelRows, data.itemRows, loadProd_(), loadBucket_());
  if (archived.length) o.first = archived[0] + "-01";
  return o;
}
function getDiscountData(p) {
  const w = windowsFor_(p);
  const a = dn_(p.anchor);
  const needed = monthsOfWindows_([w.cur, w.prev, [monthStart_(a), monthEnd_(a)]].concat(trendWindows_(w.kind, p.anchor)));
  const data = loadDisc_(readTab_, needed);
  return computeDiscount_(data, Object.assign({}, p, { latest: data.sheetLatest || undefined }));
}

/* ---------- Compare: the same kind of period side by side ----------
   mode "day"    : single days, the same weekday week after week (Day on Day)
   mode "week"   : ISO weeks, Monday to Sunday, with WK numbers
   mode "months" : the same dates (From to To) in earlier months
   mode "year"   : the same dates (From to To) in earlier years
   The last column is the period the anchor date falls in. Figures are by platform, then by offer type. */
function compareColumns_(mode, n, anchor, from, to) {
  const count = Math.max(1, Math.min(12, Number(n) || 5));
  const a = dn_(anchor);
  const cols = [];
  if (mode === "week") {
    const m = mondayOf_(a);
    for (let k = count - 1; k >= 0; k--) { const s0 = m - 7 * k; cols.push({ win: [s0, s0 + 6], label: weekLabel_(s0) }); }
    return cols;
  }
  if (mode === "day") {
    for (let k = count - 1; k >= 0; k--) { const d = a - 7 * k; cols.push({ win: [d, d], label: dayLabel_(d) }); }
    return cols;
  }
  const step = mode === "year" ? 12 : 1;
  const f0 = isDate_(from) ? dn_(from) : a;
  const t0 = isDate_(to) ? dn_(to) : f0;
  const lo = Math.min(f0, t0), hi = Math.max(f0, t0);
  for (let k = count - 1; k >= 0; k--) {
    const l = addMonthsDn_(lo, -step * k), h = addMonthsDn_(hi, -step * k);
    cols.push({ win: [l, h], label: rangeLabel_(l, h) });
  }
  return cols;
}
function metricOf_(cell, metric) {
  if (metric === "discPct") return cell.gross ? (cell.disc / cell.gross) * 100 : null;
  return metric === "net" ? cell.net : metric === "inv" ? cell.inv : cell.disc;
}

function computeCompare_(labelRows, p) {
  const mode = ["day", "week", "months", "year"].indexOf(p.mode) >= 0 ? p.mode : "day";
  const metric = ["disc", "discPct", "net", "inv"].indexOf(p.metric) >= 0 ? p.metric : "disc";
  const cols = compareColumns_(mode, p.periods, p.anchor, p.from, p.to);   // newest first: cols[0] is the latest period
  const f = { brand: p.brand || "all", region: p.region || "all", source: p.source || "all", session: p.session || "all" };
  const passF = r => (f.brand === "all" || r.brand === f.brand) && (f.region === "all" || r.region === f.region) &&
                     (f.source === "all" || r.source === f.source) && (f.session === "all" || r.session === f.session);
  const sources = new Map();                       // source -> Map(label -> one cell per column)
  cols.forEach((c, i) => {
    for (const r of labelRows) {
      if (!passF(r) || !inWin_(r, c.win)) continue;
      if (!sources.has(r.source)) sources.set(r.source, new Map());
      const m = sources.get(r.source);
      if (!m.has(r.label)) m.set(r.label, cols.map(() => blank_()));
      add_(m.get(r.label)[i], r);
    }
  });
  // Highest to lowest on the latest period's value. A null value (no gross) sorts last.
  const rank = v => (v == null ? -Infinity : v);
  const platforms = [...sources.keys()]
    .sort((x, y) => (x === "Swiggy" ? -1 : y === "Swiggy" ? 1 : x.localeCompare(y)))
    .map(src => {
      const types = new Map();
      for (const [label, cells] of sources.get(src)) {
        const t = src === "Zomato" ? label : offerTypeOf_(label);
        if (!types.has(t)) types.set(t, cols.map(() => blank_()));
        const tv = types.get(t);
        cells.forEach((cell, i) => add_(tv[i], cell));
      }
      // The total counts every code and offer type, so each period's total is the real total for that source.
      const totalCells = cols.map(() => blank_());
      for (const cells of types.values()) cells.forEach((cell, i) => add_(totalCells[i], cell));
      const total = totalCells.map(c => metricOf_(c, metric));
      // Shown: only codes and offer types with orders in the latest period, highest to lowest on the selected metric.
      const rows = [...types.entries()]
        .filter(([, cells]) => cells[0].inv > 0)
        .map(([type, cells]) => ({ type: type, values: cells.map(c => metricOf_(c, metric)) }))
        .sort((x, y) => { const a = rank(x.values[0]), b = rank(y.values[0]); return a === b ? 0 : (b > a ? 1 : -1); });
      return { source: src, types: rows, total: total };
    });
  return {
    mode: mode, metric: metric,
    columns: cols.map(c => ({ label: c.label, range: rangeText_(c.win),
                              hasData: labelRows.some(r => passF(r) && inWin_(r, c.win)) })),
    platforms: platforms
  };
}
function getCompareData(p) {
  const mode = ["day", "week", "months", "year"].indexOf(p.mode) >= 0 ? p.mode : "day";
  const cols = compareColumns_(mode, p.periods, p.anchor, p.from, p.to);
  const data = loadDisc_(readTab_, monthsOfWindows_(cols.map(c => c.win)));
  return computeCompare_(data.labelRows, p);
}

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
function computeDiscountLive_(hourRows, p, itemRows) {
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
  // Category block for today so far, against the same hours last week. Only complete hours are counted.
  const liveItems = (itemRows || []).filter(r => passF(r) && inCodes(r.label) && r.bh < ctx.cutBh);
  const curDn = dn_(ctx.liveDay), lwDn = dn_(lwDay);
  const cats = attachPrev_(
    catBlock_(liveItems.filter(r => r.d === ctx.liveDay), [curDn, curDn], () => true),
    catBlock_(liveItems.filter(r => r.d === lwDay), [lwDn, lwDn], () => true));
  const liveRows = hourRows.filter(r => r.bh < ctx.cutBh && passF(r) && inCodes(r.label));
  const offers = offersBySource_(liveRows, [dn_(ctx.liveDay), dn_(ctx.liveDay)], [dn_(lwDay), dn_(lwDay)], () => true);
  const note = ctx.cutBh === 0
    ? "No complete hour yet. The business day starts at 9 AM."
    : "Complete hours: 9 AM to " + hourLabel_(ctx.cutBh - 1) + ". Hours still running are not counted.";
  const scope = codes.length === 0 ? "all" : (codes.length === 1 ? codes[0] : codes.length + " selected codes");
  return {
    meta: { kind: "live", liveDay: ctx.liveDay, lwDay: lwDay, codes: codes, code: scope,
            cur: ctx.liveDay + " so far", prev: lwDay + ", same hours", activeMonth: "today so far", note: note, vs: "same hours last week", prevHasData: true },
    labels: labels,
    hours: hours,
    cats: cats,
    offers: offers,
    totals: { cur: selCur, prev: selPrev },
    overall: { cur: overallCur, prev: overallPrev },
    trend: { kind: "", rows: [] }
  };
}
function getDiscountLive(p) { return computeDiscountLive_(loadHour_(), p, loadLiveItems_()); }

/* ---------- Product performance and discount analysis ----------
   Tabs: Disc_Bucket (orders by AOV and discount band, order totals), Disc_Prod (items split by whether the order
   had a discount), Disc_Prod_Bucket (items inside each band). All channels. Charges are not products. */
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
  })).filter(r => r.d.length === 10 && isProduct_(r.item));
}
function loadProdBucket_(read) {
  const rd = read || readTab_;
  return rd("Disc_Prod_Bucket").map(v => ({
    d: dateText_(v[0]), brand: String(v[1]), region: String(v[2]), source: String(v[3]), kind: String(v[4]),
    bucket: String(v[5]), cat: String(v[6]), item: String(v[7]), inv: Number(v[8]) || 0, qty: Number(v[9]) || 0,
    gross: Number(v[10]) || 0, disc: Number(v[11]) || 0, net: Number(v[12]) || 0
  })).filter(r => r.d.length === 10 && isProduct_(r.item));
}
function pFilter_(p) { return { brand: p.brand || "all", region: p.region || "all", source: p.source || "all", session: p.session || "all" }; }
function passP_(r, f) {
  return (f.brand === "all" || r.brand === f.brand) && (f.region === "all" || r.region === f.region) &&
         (f.source === "all" || r.source === f.source);
}
function sideBlank_() { return { yes: blank_(), no: blank_() }; }
function sideNet_(s) { return s.yes.net + s.no.net; }

// Products: each item's sales split into orders with a discount (yes) and without (no), this period against the one before.
function computeProducts_(rows, p) {
  const w = windowsFor_(p);
  const f = pFilter_(p);
  const totCur = sideBlank_(), totPrev = sideBlank_();
  const items = new Map();
  let first = null, last = null;
  for (const r of rows) {
    if (!first || r.d < first) first = r.d;
    if (!last || r.d > last) last = r.d;
    if (r.item === ALL_ITEMS_ || !passP_(r, f)) continue;
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
    meta: { kind: w.kind, cur: rangeText_(w.cur), prev: rangeText_(w.prev), note: w.note,
            dataFrom: first || "", dataTo: last || "" },
    totals: { cur: totCur, prev: totPrev },
    products: products,
    categories: categories,
    cats: catBlock_(rows, w.cur, r => passP_(r, f))
  };
}

// Bands: orders and sales per band, this period against the one before, and the items inside each band (current period).
function computeBuckets_(bucketRows, drillRows, p) {
  const w = windowsFor_(p);
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
    meta: { kind: w.kind, cur: rangeText_(w.cur), prev: rangeText_(w.prev), note: w.note },
    aov: out.AOV,
    discount: out.Discount,
    totals: { cur: sumBands(out.AOV, "cur"), prev: sumBands(out.AOV, "prev") }
  };
}

/* ---------- Category performance: the last four calendar months, from the Disc_Cat tab ----------
   Current month = the month of the date chosen, against the three months before it.
   Orders in the (all categories) rows are distinct invoices, so the total counts each order once. */
function monthKeyOf_(anchor, back) {
  const p = anchor.split("-").map(Number);
  const d = new Date(Date.UTC(p[0], p[1] - 1 - back, 1));
  return d.getUTCFullYear() + "-" + String(d.getUTCMonth() + 1).padStart(2, "0");
}
function monthLabel_(key) {
  const p = key.split("-").map(Number);
  return MONTH_NAMES_[p[1] - 1] + " " + p[0];
}
// The dates a month covers in this view: 1 to the last day, or to the anchor day for the current month.
function monthSpan_(key, anchor) {
  const p = key.split("-").map(Number);
  const lastDay = new Date(Date.UTC(p[0], p[1], 0)).getUTCDate();
  const endDay = key === String(anchor).slice(0, 7) ? Number(String(anchor).slice(8, 10)) : lastDay;
  return "1–" + endDay + " " + MONTH_NAMES_[p[1] - 1];
}
function loadCat_(read) {
  const rd = read || readTab_;
  return rd("Disc_Cat").map(v => ({
    month: String(v[0]).slice(0, 7), brand: String(v[1]), region: String(v[2]), source: String(v[3]),
    channel: String(v[4]), cat: String(v[5]), inv: Number(v[6]) || 0, qty: Number(v[7]) || 0,
    gross: Number(v[8]) || 0, disc: Number(v[9]) || 0, net: Number(v[10]) || 0
  })).filter(r => r.month.length === 7);
}
// Cells per category: [current month, last month, last 2 months, last 3 months], for all channels, Offline and Online.
function computeCategoryMonths_(rows, p) {
  const f = pFilter_(p);
  const keys = [0, 1, 2, 3].map(b => monthKeyOf_(p.anchor, b));
  const idx = new Map(keys.map((k, i) => [k, i]));
  const blankCells = () => keys.map(() => ({ net: 0, orders: 0, qty: 0, gross: 0, disc: 0 }));
  const add = (cell, r) => { cell.net += r.net; cell.orders += r.inv; cell.qty += r.qty; cell.gross += r.gross; cell.disc += r.disc; };
  const cats = new Map();
  const totals = { all: blankCells(), offline: blankCells(), online: blankCells() };
  const hasData = keys.map(() => false);
  for (const r of rows) {
    const i = idx.get(r.month);
    if (i === undefined || !passP_(r, f)) continue;
    hasData[i] = true;
    const chKey = r.channel === "Offline" ? "offline" : "online";
    if (r.cat === ALL_CATEGORIES_) {
      for (const k of ["all", chKey]) add(totals[k][i], r);
      continue;
    }
    if (!cats.has(r.cat)) cats.set(r.cat, { cat: r.cat, cells: { all: blankCells(), offline: blankCells(), online: blankCells() } });
    const cells = cats.get(r.cat).cells;
    for (const k of ["all", chKey]) add(cells[k][i], r);
  }
  const categories = [...cats.values()].sort((x, y) => y.cells.all[0].net - x.cells.all[0].net);
  return {
    months: keys.map(k => ({ key: k, label: monthLabel_(k), span: monthSpan_(k, p.anchor) })),
    hasData: hasData, categories: categories, totals: totals
  };
}

/* ---------- Product tab, monthly: the selected month, against the month before; the table shows four months ---------- */
// Each row has d = the first day of its month, so the category block can use the same date window as the other views.
function loadProdMonth_(read) {
  const rd = read || readTab_;
  return rd("Disc_Prod_Month").map(v => {
    const month = String(v[0]).slice(0, 7);
    return {
      month: month, d: month + "-01", brand: String(v[1]), region: String(v[2]), source: String(v[3]),
      yes: String(v[4]) === "Yes", cat: String(v[5]), item: String(v[6]), inv: Number(v[7]) || 0,
      qty: Number(v[8]) || 0, gross: Number(v[9]) || 0, disc: Number(v[10]) || 0, net: Number(v[11]) || 0
    };
  }).filter(r => r.month.length === 7 && isProduct_(r.item));
}
// Products and categories by month. cur = the selected month, prev = the month before; m[] = net for the four months.
function computeProductsMonthly_(rows0, p) {
  // Each month's rows get d = its first day, so the category block can use the same date window as the other views.
  const rows = rows0.map(r => (r.d ? r : Object.assign({}, r, { d: r.month + "-01" })));
  const f = pFilter_(p);
  const keys = [0, 1, 2, 3].map(b => monthKeyOf_(p.anchor, b));
  const idx = new Map(keys.map((k, i) => [k, i]));
  const side = () => ({ yes: blank_(), no: blank_() });
  const sides = () => keys.map(() => side());
  const netOf = s => s.yes.net + s.no.net;
  const items = new Map();
  const totals = sides();
  const hasData = keys.map(() => false);
  for (const r of rows) {
    const i = idx.get(r.month);
    if (i === undefined || !passP_(r, f)) continue;
    hasData[i] = true;
    if (r.item === ALL_ITEMS_) continue;
    const s = r.yes ? "yes" : "no";
    const key = r.cat + "\u0001" + r.item;
    if (!items.has(key)) items.set(key, { item: r.item, cat: r.cat, months: sides() });
    add_(items.get(key).months[i][s], r);
    add_(totals[i][s], r);
  }
  const products = [...items.values()].map(x => ({
    item: x.item, cat: x.cat, months: x.months, cur: x.months[0], prev: x.months[1], m: x.months.map(netOf)
  })).filter(x => x.m.some(v => v > 0)).sort((a, b) => b.m[0] - a.m[0]);
  const catMap = new Map();
  for (const x of products) {
    if (!catMap.has(x.cat)) catMap.set(x.cat, { cat: x.cat, months: sides() });
    const c = catMap.get(x.cat);
    x.months.forEach((sd, i) => { for (const s of ["yes", "no"]) add_(c.months[i][s], sd[s]); });
  }
  const categories = [...catMap.values()].map(c => ({ cat: c.cat, cur: c.months[0], prev: c.months[1], months: c.months }))
    .sort((a, b) => netOf(b.cur) - netOf(a.cur));
  const curWin = [monthStart_(dn_(p.anchor)), monthEnd_(dn_(p.anchor))];
  const months = keys.map(k => ({ key: k, label: monthLabel_(k), span: monthSpan_(k, p.anchor) }));
  return {
    monthly: true,
    meta: { kind: "monthly", cur: months[0].label, prev: months[1].label,
            note: "calendar months, the selected month against the month before",
            dataFrom: months[3].label, dataTo: months[0].label },
    months: months,
    hasData: hasData,
    totals: { cur: totals[0], prev: totals[1], months: totals },
    products: products,
    categories: categories,
    cats: catBlock_(rows, curWin, r => passP_(r, f))
  };
}
// Product tab: monthly from the Disc_Prod_Month tab; Daily, Weekly, MTD and Custom from the Disc_Prod tab.
// The category performance table always uses the last four calendar months.
function getProductData(p) {
  const base = (p.kind === "monthly" || !p.kind)
    ? computeProductsMonthly_(loadProdMonth_(), p)
    : computeProducts_(loadProd_(), p);
  base.categoryMonths = computeCategoryMonths_(loadCat_(), p);
  return base;
}
// Analysis tab: the bands, and the category block for the same period (all orders, from Disc_Prod).
function getBucketData(p) {
  const base = computeBuckets_(loadBucket_(), loadProdBucket_(), p);
  base.cats = catBlock_(loadProd_(), windowsFor_(p).cur, r => passP_(r, pFilter_(p)));
  return base;
}
