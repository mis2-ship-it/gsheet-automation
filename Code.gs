/* ============================================================
   Discount Dashboard (Google Apps Script)
   Discount Code sales for Swiggy and Zomato orders.
   Reads the tabs Disc_Label and Disc_Item, written by write_discount_sheet.py.
   Set DISCOUNT_SHEET_ID to the Google Sheet that holds those tabs.

   Each order counts once, under its discount label (the label combines all discounts on the order).
   Tx = orders (invoices). Orders = item lines. Net = Gross - Discount. Dis % = Discount / Gross.
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
// p: { kind, anchor 'YYYY-MM-DD', brand, region, source, session ('all' = no filter), code (a label, or 'all') }
function computeDiscount_(data, p) {
  const kind = KINDS.indexOf(p.kind) >= 0 ? p.kind : "monthly";
  const w = windows_(kind, p.anchor);
  const f = { brand: p.brand || "all", region: p.region || "all", source: p.source || "all", session: p.session || "all" };
  const code = p.code || "all";
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

  // Discount codes: only those with sales in the calendar month of the date chosen.
  const a = dn_(p.anchor);
  const monthWin = [monthStart_(a), monthEnd_(a)];
  const curL = sumBy_(data.labelRows, r => r.label, w.cur);
  const prvL = sumBy_(data.labelRows, r => r.label, w.prev);
  const activeL = sumBy_(data.labelRows, r => r.label, monthWin);
  const labels = [...activeL.keys()].map(k => ({
    label: k, cur: curL.get(k) || blank_(), prev: prvL.get(k) || blank_()
  }));

  // Trend for the selected code (all codes when 'all').
  const trend = trendWindows_(kind, p.anchor).map(win => {
    const acc = blank_();
    for (const r of data.labelRows) {
      if (!inWin(r, win) || !passF(r) || (code !== "all" && r.label !== code)) continue;
      add_(acc, r);
    }
    return { range: rangeText_(win), inv: acc.inv, qty: acc.qty, gross: acc.gross, disc: acc.disc, net: acc.net, lines: acc.lines };
  });

  // Items for the selected code, current period, with each item's category.
  const curI = sumBy_(data.itemRows, r => r.item + "\u0001" + r.cat, w.cur, r => code === "all" || r.label === code);
  const items = [...curI.entries()].map(([k, x]) => {
    const parts = k.split("\u0001");
    return { item: parts[0], cat: parts[1], cur: x };
  });

  const total_ = m => [...m.values()].reduce((t, x) => add_(t, x), blank_());
  return {
    meta: { kind: kind, anchor: p.anchor, code: code, cur: rangeText_(w.cur), prev: rangeText_(w.prev),
            note: w.note, activeMonth: rangeText_(monthWin) },
    labels: labels,
    items: items,
    totals: { cur: total_(curL), prev: total_(prvL) },
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
