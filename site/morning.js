/* ============ Morning Call ============
   Region-wise, store-wise and area-manager tables for the reference day (the latest day of data).
   Growth metrics filter: Net Sales, Dis %, Orders, AOV. The channel bar (All sources, Offline, Online) sits on top.
   Each area manager lists its stores underneath; the Excel file groups the stores so they expand.
   Loaded after insights.js. It wraps salesPeriodView() for the Morning Call tab only. */
(function () {
  const MC = { hourly: null, hourlyDays: null, hourlyLoading: false, hourlyError: null, last: null,
               srcPick: null, regOpen: new Set(), metric: "net" };
  const MC_LATE_FROM = 15, MC_LATE_TO = 19;   // hour 0 is 9 AM, so hours 15 to 19 are 0 to 5 AM
  // The numbers each row carries: net sales (₹), gross (₹), discount (₹) and orders.
  const MC_VALUE_KEYS = ["octFull", "octSame", "novMtd", "trend", "dayCur", "momDayPrev", "yoyLy", "yoyTy",
                         "ytdLy", "ytdTy", "wowPrev", "wowCur", "l2wPrev", "ftdPre", "ftdCur", "lnPrev", "lnCur"];
  // Each growth is a pair of those numbers: growth = pair[0] against pair[1].
  const MC_GROWTH_PAIRS = { trendVsOct: ["trend", "octFull"], mom: ["novMtd", "octSame"], momDay: ["dayCur", "momDayPrev"],
                            yoy: ["yoyTy", "yoyLy"], ytdYoy: ["ytdTy", "ytdLy"], wow: ["wowCur", "wowPrev"],
                            l2w: ["dayCur", "l2wPrev"], ftd: ["ftdCur", "ftdPre"], ln: ["lnCur", "lnPrev"] };
  const MC_METRICS = [["net", "Net Sales"], ["dis", "Dis %"], ["orders", "Orders"], ["aov", "AOV"]];
  const MC_SUMMARY = [
    { h: "MTD sales", k: "novMtd" }, { h: "MoM %", k: "mom" }, { h: "Same day MoM %", k: "momDay" },
    { h: "Trend (projection)", k: "trend" }, { h: "Same day YoY %", k: "yoy" }, { h: "YTD YoY %", k: "ytdYoy" },
    { h: "WoW %", k: "wow" }, { h: "FTD %", k: "ftd" }, { h: "Late Night %", k: "ln" }, { h: "L2W %", k: "l2w" }
  ];
  const mcEsc = s => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  const mcOk = v => v !== null && v !== undefined && isFinite(v);
  const chanOk = r => (window.FB_CHAN_OK ? window.FB_CHAN_OK(r) : true);
  const FB = () => window.FB_INS;
  const mcMonthKey = d => (toDate(d).getUTCFullYear() - 2025) * 12 + toDate(d).getUTCMonth();
  const mcDom = d => toDate(d).getUTCDate();
  const mcDim = d => new Date(Date.UTC(toDate(d).getUTCFullYear(), toDate(d).getUTCMonth() + 1, 0)).getUTCDate();
  const mcIso = d => toDate(d).toISOString().slice(0, 10);
  const isGrowth = c => !!(MC_GROWTH_PAIRS[c.k] || c.g);
  const metricName = m => (MC_METRICS.find(x => x[0] === m) || MC_METRICS[0])[1];

  // ---- numbers: a record is { n, g, d, o } (net, gross and discount in ₹, orders as a count), or null ----
  function mcSumRecs(recs) {
    let any = false, n = 0, g = 0, d = 0, o = 0;
    for (const r of recs) { if (!r) continue; any = true; n += r.n; g += r.g; d += r.d; o += r.o; }
    return any ? { n, g, d, o } : null;
  }
  function mcScale(rec, f) { return rec ? { n: rec.n * f, g: rec.g * f, d: rec.d * f, o: rec.o * f } : null; }
  // The figure for a metric: Net Sales (₹), Orders (count), AOV (₹ per order) or Dis % (discount as % of gross).
  function mcVal(rec, metric) {
    if (!rec) return null;
    if (metric === "orders") return rec.o;
    if (metric === "aov") return rec.o ? rec.n / rec.o : null;
    if (metric === "dis") return rec.g ? (rec.d / rec.g) * 100 : null;
    return rec.n;
  }
  // The growth of a metric: a % change, or for Dis % the change in points.
  function mcGrowth(cur, prev, metric) {
    const a = mcVal(cur, metric), b = mcVal(prev, metric);
    if (!mcOk(a) || !mcOk(b)) return null;
    if (metric === "dis") return a - b;
    return b ? (a / b - 1) * 100 : null;
  }
  // The value of one table cell. Growth cells use the pair of numbers behind them (for totals, only the stores that have both).
  function mcCellVal(row, c, metric) {
    const m = metric || MC.metric;
    if (MC_GROWTH_PAIRS[c.k] || c.g) {
      const pair = MC_GROWTH_PAIRS[c.k] || c.g;
      const p = row.P && row.P[c.k] ? row.P[c.k] : { cur: row.R[pair[0]], prev: row.R[pair[1]] };
      return mcGrowth(p.cur, p.prev, m);
    }
    return mcVal(row.R[c.k], m);
  }
  // Named numbers on a row (net sales values by default), so that code and tests can read row.novMtd and so on.
  function mcDecorate(row) {
    for (const k of MC_VALUE_KEYS) Object.defineProperty(row, k, { get: () => mcVal(row.R[k], "net"), enumerable: true, configurable: true });
    for (const k of Object.keys(MC_GROWTH_PAIRS)) Object.defineProperty(row, k, { get: () => mcCellVal(row, { k }, "net"), enumerable: true, configurable: true });
    return row;
  }
  // Totals for a set of stores: sums of every number, and for each growth the sums of the stores that have both figures.
  function mcTotals(list) {
    const R = {}, P = {};
    for (const k of MC_VALUE_KEYS) R[k] = mcSumRecs(list.map(s => s.R[k]));
    for (const k of Object.keys(MC_GROWTH_PAIRS)) {
      const [a, b] = MC_GROWTH_PAIRS[k];
      const pairs = list.filter(s => s.R[a] && s.R[b]);
      P[k] = { cur: mcSumRecs(pairs.map(s => s.R[a])), prev: mcSumRecs(pairs.map(s => s.R[b])) };
    }
    return mcDecorate({ R, P });
  }

  async function mcLoadHourly() {
    if (MC.hourly || MC.hourlyLoading) return;
    MC.hourlyLoading = true;
    try {
      const data = await loadGz("data/hourly.json.gz");
      MC.hourly = data.rows;
      MC.hourlyDays = new Set(data.rows.map(r => r[0]));
    } catch (err) {
      MC.hourlyError = err.message;
    }
    MC.hourlyLoading = false;
    render();
  }

  // The record for a store over a range of days (store rows: [.., net 7, gross 8, discount 9, orders 10]).
  function mcRec(rows, from, to) {
    if (!rows) return null;
    let n = 0, g = 0, d = 0, o = 0, any = false;
    for (const r of rows) {
      if (r[0] < from || r[0] > to || !salesPass(r) || !chanOk(r) || (MC.srcPick && !MC.srcPick(r))) continue;
      n += r[7]; g += r[8]; d += r[9]; o += r[10]; any = true;
    }
    return any ? { n: n / 100, g: g / 100, d: d / 100, o } : null;
  }
  // Hours 0 to 5 AM by store and day. Hourly rows: [.., hour 7, net 8, gross 9, discount 10, orders 11].
  function mcLateMap(days) {
    const m = new Map();
    if (!MC.hourly) return m;
    for (const r of MC.hourly) {
      if (!days.includes(r[0]) || r[7] < MC_LATE_FROM || r[7] > MC_LATE_TO || !salesPass(r) || !chanOk(r) || (MC.srcPick && !MC.srcPick(r))) continue;
      const k = r[1] + "|" + r[0];
      const x = m.get(k) || { n: 0, g: 0, d: 0, o: 0 };
      x.n += r[8]; x.g += r[9]; x.d += r[10]; x.o += r[11];
      m.set(k, x);
    }
    for (const x of m.values()) { x.n /= 100; x.g /= 100; x.d /= 100; }
    return m;
  }
  function mcLate(lateMap, st, day) {
    if (!MC.hourlyDays || !MC.hourlyDays.has(day)) return null;
    return lateMap.get(st + "|" + day) || { n: 0, g: 0, d: 0, o: 0 };
  }

  // Every number the tables need, for each store, on the reference day.
  function mcCompute() {
    const ref = M.last_day;
    const curKey = mcMonthKey(ref), prevKey = curKey - 1;
    const dom = mcDom(ref), dim = mcDim(ref), balance = dim - dom;
    const ly = shiftMonths(ref, -12), lm = shiftMonths(ref, -1);
    const lyDay = FB().insSameWeekday(ref, 12), lmDay = FB().insSameWeekday(ref, 1);
    const lateMap = mcLateMap([ref - 7, ref]);
    const list = [];
    for (const s of M.stores) {
      const Rw = STORE_ROWS.get(s.i);
      if (!Rw) continue;
      const R = {};
      R.octFull = mcRec(Rw, monStart(prevKey), monEnd(prevKey));
      R.octSame = mcRec(Rw, monStart(prevKey), lm);
      R.novMtd = mcRec(Rw, monStart(curKey), ref);
      R.trend = mcScale(R.novMtd, 1 + balance / dom);
      R.dayCur = mcRec(Rw, ref, ref);
      R.momDayPrev = mcRec(Rw, lmDay, lmDay);
      R.yoyLy = mcRec(Rw, lyDay, lyDay);
      R.yoyTy = R.dayCur;
      R.ytdLy = mcRec(Rw, monStart(curKey - 12), ly);
      R.ytdTy = mcRec(Rw, monStart(curKey), ref);
      R.wowPrev = mcRec(Rw, ref - 7, ref - 7);
      R.wowCur = mcRec(Rw, ref, ref);
      R.l2wPrev = mcRec(Rw, ref - 14, ref - 14);
      R.ftdPre = mcRec(Rw, ref - 1, ref - 1);
      R.ftdCur = R.wowCur;
      R.lnPrev = mcLate(lateMap, s.i, ref - 7);
      R.lnCur = mcLate(lateMap, s.i, ref);
      // a store is in the tables when it has daily sales under the filters (hourly figures alone do not count)
      if (R.octFull || R.novMtd || R.wowCur || R.ftdPre || R.yoyTy || R.ytdTy) {
        list.push(mcDecorate({ i: s.i, name: s.name, region: s.region || "Unmapped", R, P: null }));
      }
    }
    return { ref, dom, balance, list };
  }

  // The tables: each column names a number, or a growth between two numbers.
  function mcBlocksBase(c) {
    const ref = c.ref, ref7 = ref - 7, ly = shiftMonths(ref, -12), lm = shiftMonths(ref, -1);
    const lyDay = FB().insSameWeekday(ref, 12), lmDay = FB().insSameWeekday(ref, 1);
    const curKey = mcMonthKey(ref), prevKey = curKey - 1;
    const dl = d => toDate(d).toLocaleDateString("en-IN", { day: "numeric", month: "short", year: "numeric", timeZone: "UTC" });
    const sCur = ref - (c.dom - 1), sPrev = monStart(prevKey), sLy = monStart(curKey - 12);
    const prevName = monLong(prevKey), curName = monLong(curKey);
    return [
      { id: "mom", sheet: "MoM MTD", title: "MoM (MTD)",
        sub: `${dl(sCur)} to ${dl(ref)} against ${dl(sPrev)} to ${dl(lm)}. Trend = month to date ÷ ${c.dom} days × ${c.balance} balance days + month to date.`,
        main: "novMtd", sort: "mom",
        cols: [
          { k: "octFull", h: `${prevName} full month (reference)` },
          { k: "octSame", h: `Same days last month (${dl(sPrev)} to ${dl(lm)})` },
          { k: "novMtd", h: `MTD (${dl(sCur)} to ${dl(ref)})` },
          { k: "mom", h: "MoM %", g: ["novMtd", "octSame"] },
          { k: "trend", h: `Trend (${curName} projection)` },
          { k: "trendVsOct", h: "Trend vs full last month %", g: ["trend", "octFull"] },
          { k: "dayCur", h: `Same day (${dl(ref)})` },
          { k: "momDayPrev", h: `Same day last month (${dl(lmDay)})` },
          { k: "momDay", h: "Same day MoM %", g: ["dayCur", "momDayPrev"] }
        ] },
      { id: "yoy", sheet: "YoY", title: "YoY (same day and YTD)",
        sub: `Same day: the same weekday in the same week of the year before, ${dl(lyDay)} against ${dl(ref)}. YTD: ${dl(sLy)} to ${dl(ly)} last year, ${dl(sCur)} to ${dl(ref)} this year.`,
        main: "ytdTy", sort: "yoy",
        cols: [
          { k: "yoyLy", h: `Same day last year (${dl(lyDay)})` },
          { k: "yoyTy", h: `Same day (${dl(ref)})` },
          { k: "yoy", h: "YoY %", g: ["yoyTy", "yoyLy"] },
          { k: "ytdLy", h: `YTD last year (${dl(sLy)} to ${dl(ly)})` },
          { k: "ytdTy", h: `YTD (${dl(sCur)} to ${dl(ref)})` },
          { k: "ytdYoy", h: "YTD YoY %", g: ["ytdTy", "ytdLy"] }
        ] },
      { id: "wow", sheet: "WoW", title: "WoW (same weekday)", sub: `${dl(ref7)} against ${dl(ref)}.`, main: "wowCur", sort: "wow",
        cols: [
          { k: "wowPrev", h: dl(ref7) },
          { k: "wowCur", h: dl(ref) },
          { k: "wow", h: "Growth %", g: ["wowCur", "wowPrev"] }
        ] },
      { id: "ftd", sheet: "FTD", title: "FTD vs Pre FTD", sub: `${dl(ref)} (FTD) against ${dl(ref - 1)} (Pre FTD).`, main: "ftdCur", sort: "ftd",
        cols: [
          { k: "ftdPre", h: `Pre FTD (${dl(ref - 1)})` },
          { k: "ftdCur", h: `FTD (${dl(ref)})` },
          { k: "ftd", h: "Growth %", g: ["ftdCur", "ftdPre"] }
        ] },
      { id: "late", sheet: "Late Night", title: "Late Night sale (0–5 AM)",
        sub: `${dl(ref7)} against ${dl(ref)}. Shows — for days the hourly file does not hold (the last 14 business days).`,
        main: "lnCur", sort: "ln",
        cols: [
          { k: "lnPrev", h: dl(ref7) },
          { k: "lnCur", h: dl(ref) },
          { k: "ln", h: "Growth %", g: ["lnCur", "lnPrev"] }
        ] }
    ];
  }
  // Every table shows the growth metrics: WoW, L2W, same day MoM and same day YoY (any already there are kept).
  const MC_GROWTH = [
    { k: "wow", h: "WoW %", g: ["wowCur", "wowPrev"] },
    { k: "l2w", h: "L2W %", g: ["dayCur", "l2wPrev"] },
    { k: "momDay", h: "Same day MoM %", g: ["dayCur", "momDayPrev"] },
    { k: "yoy", h: "Same day YoY %", g: ["yoyTy", "yoyLy"] }
  ];
  function mcWithGrowth(b) {
    const keys = new Set(b.cols.map(c => c.k));
    return Object.assign({}, b, { cols: b.cols.concat(MC_GROWTH.filter(c => !keys.has(c.k))) });
  }
  function mcBlocks(c) { return mcBlocksBase(c).map(mcWithGrowth); }

  // The sources shown under a region: the named ones, then everything else.
  function mcSourceGroups() {
    const named = ["In Store", "Swiggy", "Zomato", "Ownly"];
    const idx = n => M.sources.indexOf(n);
    const namedIdx = named.map(idx).filter(i => i >= 0);
    const groups = named.map(n => ({ label: n, pick: r => idx(n) >= 0 && r[5] === idx(n) }));
    groups.push({ label: "Others (rest of the sources)", pick: r => !namedIdx.includes(r[5]) });
    return groups;
  }
  // Totals for each source, table and region: { source: { tableId: { region: totals } } }. "All regions" is included.
  function mcSourceTotals(blocks) {
    const out = {};
    for (const g of mcSourceGroups()) {
      MC.srcPick = g.pick;
      let list;
      try { list = mcCompute().list; } finally { MC.srcPick = null; }
      out[g.label] = {};
      for (const b of blocks) {
        const grp = mcGroup(list, b);
        const byRegion = { "All regions": grp.all };
        for (const r of grp.groups) byRegion[r.name] = r.tot;
        out[g.label][b.id] = byRegion;
      }
    }
    return out;
  }

  // Regions (largest first on net sales), each with its stores (strongest growth on the block's sort first, nulls last).
  function mcGroup(list, block) {
    const byRegion = new Map();
    for (const s of list) {
      if (!byRegion.has(s.region)) byRegion.set(s.region, []);
      byRegion.get(s.region).push(s);
    }
    const sortVal = v => (mcOk(v) ? v : -Infinity);
    const groups = Array.from(byRegion.entries()).map(([name, stores]) => ({
      name,
      stores: stores.slice().sort((a, b) => {
        const x = mcCellVal(a, { k: block.sort }), y = mcCellVal(b, { k: block.sort });
        if (!mcOk(x) && !mcOk(y)) return 0;
        if (!mcOk(x)) return 1;
        if (!mcOk(y)) return -1;
        return y - x;
      }),
      tot: mcTotals(stores)
    }));
    groups.sort((a, b) => {
      const x = sortVal(a.tot.novMtd), y = sortVal(b.tot.novMtd);
      return x === y ? 0 : y > x ? 1 : -1;
    });
    return { groups, all: mcTotals(list) };
  }

  // ---- the page ----
  function mcColour(v, c) {
    if (!mcOk(v)) return "var(--ink-2)";
    if (!isGrowth(c)) return "var(--ink)";
    const good = MC.metric === "dis" ? v < 0 : v >= 0;     // a fall in discount % is the good way round
    return good ? "var(--up)" : "var(--down)";
  }
  function mcFmt(v, c) {
    if (isGrowth(c)) {
      if (!mcOk(v)) return "—";
      return MC.metric === "dis" ? (v >= 0 ? "+" : "−") + Math.abs(v).toFixed(2) + " pts" : (v >= 0 ? "+" : "−") + Math.abs(v).toFixed(1) + "%";
    }
    if (!mcOk(v)) return "—";
    if (MC.metric === "orders") return Math.round(v).toLocaleString("en-IN");
    if (MC.metric === "dis") return v.toFixed(2) + "%";
    return "₹" + Math.round(v).toLocaleString("en-IN");
  }
  function mcCellHtml(row, c) {
    const v = mcCellVal(row, c, MC.metric);
    return isGrowth(c) ? `<td style="color:${mcColour(v, c)}">${mcFmt(v, c)}</td>` : `<td>${mcFmt(v, c)}</td>`;
  }
  // A column header. Growth columns in Dis % are in points, so their header says so.
  function mcHead(c) { return c.h + (isGrowth(c) && MC.metric === "dis" ? " (pts)" : ""); }
  function mcMetricBar() {
    const btn = (m, label) => `<button data-mc-metric="${m}" aria-pressed="${MC.metric === m}">${label}</button>`;
    return `<div class="reg-filter mc-metric"><span class="muted">Show:</span>${MC_METRICS.map(([m, l]) => btn(m, l)).join("")}</div>`;
  }
  function mcSumHeader(k) {
    if (k === "novMtd") return `MTD ${metricName(MC.metric).toLowerCase() === "net sales" ? "sales" : metricName(MC.metric)}`;
    return MC_SUMMARY.find(x => x.k === k).h;
  }

  // The region summary: one row per region (All regions first), with the key figure from each table.
  function mcSummary(c) {
    const all = mcGroup(c.list, { sort: "mom" }).all;
    const groups = mcGroup(c.list, { sort: "mom" }).groups;
    const rows = [{ name: "All regions", row: all }].concat(groups.map(g => ({ name: g.name, row: g.tot })));
    const cols = MC_SUMMARY.map(m => ({ k: m.k, h: mcSumHeader(m.k) }));
    return { cols, rows: rows.map(x => ({ name: x.name, row: x.row, vals: cols.map(cc => mcCellVal(x.row, cc, MC.metric)) })) };
  }
  function mcSummaryTable(s) {
    const head = `<tr><th>Region</th>${s.cols.map(c => `<th>${mcEsc(mcHead(c))}</th>`).join("")}</tr>`;
    const body = s.rows.map((r, i) => `<tr${i === 0 ? ' class="reg-total"' : ""}><td>${mcEsc(r.name)}</td>` +
      s.cols.map((c, j) => `<td style="color:${mcColour(r.vals[j], c)}">${mcFmt(r.vals[j], c)}</td>`).join("") + `</tr>`).join("");
    return `<div class="scroll"><table class="tbl"><thead>${head}</thead><tbody>${body}</tbody></table></div>`;
  }

  // Area managers: their stores, by net sales. Each store is listed underneath its manager.
  function mcAmGroups(c) {
    const amOf = new Map(M.stores.map(s => [s.i, s.a]));
    const byAm = new Map();
    for (const v of c.list) {
      const a = amOf.get(v.i);
      if (a === undefined) continue;
      if (!byAm.has(a)) byAm.set(a, []);
      byAm.get(a).push(v);
    }
    const sortVal = v => (mcOk(v) ? v : -Infinity);
    const byMetric = (x, y) => { const p = sortVal(mcVal(x.R.novMtd, MC.metric)), q = sortVal(mcVal(y.R.novMtd, MC.metric)); return p === q ? 0 : q > p ? 1 : -1; };
    return Array.from(byAm.entries()).map(([a, stores]) => ({
      a, name: (M.ams && M.ams[a]) || "Unassigned", stores: stores.slice().sort(byMetric), tot: mcTotals(stores)
    })).sort((x, y) => byMetric(x.tot, y.tot));
  }
  function mcAmTable(c, s) {
    const head = `<tr><th>Area manager / Store</th>${s.cols.map(x => `<th>${mcEsc(mcHead(x))}</th>`).join("")}</tr>`;
    let body = `<tr class="reg-total"><td>All areas</td>${s.cols.map(x => mcCellHtml(mcTotals(c.list), x)).join("")}</tr>`;
    for (const g of mcAmGroups(c)) {
      body += `<tr class="ins-am"><td><b>${mcEsc(g.name)}</b></td>${s.cols.map(x => mcCellHtml(g.tot, x)).join("")}</tr>`;
      for (const st of g.stores) body += `<tr class="ins-st"><td>${mcEsc(st.name)}</td>${s.cols.map(x => mcCellHtml(st, x)).join("")}</tr>`;
    }
    return `<div class="scroll"><table class="tbl"><thead>${head}</thead><tbody>${body}</tbody></table></div>`;
  }

  // Region wise, for one table: All regions, then each region. Click a region to show its sources.
  function mcRegionTable(block, g, srcT) {
    const head = `<tr><th>Region</th>${block.cols.map(c => `<th>${mcEsc(mcHead(c))}</th>`).join("")}</tr>`;
    const rows = [`<tr class="reg-total"><td>All regions</td>${block.cols.map(c => mcCellHtml(g.all, c)).join("")}</tr>`];
    for (const r of g.groups) {
      const open = MC.regOpen.has(r.name);
      rows.push(`<tr class="ins-am"><td><button data-mc-region="${mcEsc(r.name)}">${open ? "▾" : "▸"} ${mcEsc(r.name)}</button></td>` +
        `${block.cols.map(c => mcCellHtml(r.tot, c)).join("")}</tr>`);
      if (open && srcT) {
        for (const label of Object.keys(srcT)) {
          const tot = ((srcT[label][block.id] || {})[r.name]) || { R: {}, P: {} };
          rows.push(`<tr class="ins-st"><td>${mcEsc(label)}</td>${block.cols.map(c => mcCellHtml(tot, c)).join("")}</tr>`);
        }
      }
    }
    return `<div class="scroll"><table class="tbl"><thead>${head}</thead><tbody>${rows.join("")}</tbody></table></div>`;
  }
  // Region + store: each region row with its totals, and its stores underneath.
  function mcStoreTable(block, g) {
    const head = `<tr><th>Region / Store</th>${block.cols.map(c => `<th>${mcEsc(mcHead(c))}</th>`).join("")}</tr>`;
    const rows = [];
    for (const r of g.groups) {
      rows.push(`<tr class="reg-head"><td>${mcEsc(r.name)}</td>${block.cols.map(c => mcCellHtml(r.tot, c)).join("")}</tr>`);
      for (const s of r.stores) rows.push(`<tr><td>&nbsp;&nbsp;${mcEsc(s.name)}</td>${block.cols.map(c => mcCellHtml(s, c)).join("")}</tr>`);
    }
    return `<div class="scroll"><table class="tbl"><thead>${head}</thead><tbody>${rows.join("")}</tbody></table></div>`;
  }

  async function mcLoadHourlyOnce() { return mcLoadHourly(); }

  function mcNote(text) {
    return `<section class="block"><h2>Morning Call</h2><p class="muted">${text}</p></section>`;
  }
  function morningView() {
    setPeriodBar("morning", null, null);
    if (!window.FB_INS) return mcNote("The sales module (insights.js) is not loaded.");
    if (!M || !M.stores || !M.stores.length) return mcNote("The store list is missing from the data.");
    if (!window.FB_INS.INS.loaded) {
      window.FB_INS.insLoad();
      return mcNote("Loading store data. The Morning Call appears when it is in.");
    }
    if (M.has_hourly && !MC.hourly && !MC.hourlyLoading && !MC.hourlyError) mcLoadHourly();
    const c = mcCompute();
    const blocks = mcBlocks(c);
    const summary = mcSummary(c);
    const srcT = MC.regOpen.size ? mcSourceTotals(blocks) : null;
    MC.last = { c, blocks, summary };
    let late = "";
    if (!M.has_hourly) late = "The hourly file is not in the data, so Late Night shows —.";
    else if (MC.hourlyError) late = `Hourly data could not be loaded: ${mcEsc(MC.hourlyError)}`;
    else if (!MC.hourly) late = "Loading hourly data for Late Night.";
    const chan = window.FB_CHAN_BAR ? window.FB_CHAN_BAR() : "";
    return chan + mcMetricBar() +
      `<section class="block"><h2>Morning Call: ${fmtLong(c.ref)}</h2>` +
      `<p class="sub">Reference day: the latest day of data. The Sales filters, the channel above and the metric above apply to every table. Showing: ${metricName(MC.metric)}.</p>` +
      `<button data-mc-xlsx="1" class="mc-btn">Download Excel</button>` +
      (late ? `<p class="muted">${late}</p>` : "") + `</section>` +
      `<section class="block"><h2>Region wise summary</h2>` +
      `<p class="sub">One row per region, with the key figure from each table below.</p>${mcSummaryTable(summary)}</section>` +
      `<section class="block"><h2>Stores by area manager</h2>` +
      `<p class="sub">Each area manager, with its stores listed underneath.</p>${mcAmTable(c, summary)}</section>` +
      blocks.map(b => {
        const g = mcGroup(c.list, b);
        return `<section class="block"><h2>${mcEsc(b.title)}</h2><p class="sub">${mcEsc(b.sub)}</p>` +
          `<p class="sub"><b>Region wise</b> <span class="muted">(click a region to see its sources)</span></p>${mcRegionTable(b, g, srcT)}` +
          `<p class="sub"><b>Region + store</b></p>${mcStoreTable(b, g)}</section>`;
      }).join("");
  }

  // ---- Excel (.xlsx), written here with no outside library ----
  const MC_CRC = (() => {
    const t = new Uint32Array(256);
    for (let n = 0; n < 256; n++) {
      let c = n;
      for (let k = 0; k < 8; k++) c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
      t[n] = c >>> 0;
    }
    return t;
  })();
  function mcCrc(bytes) {
    let c = 0xFFFFFFFF;
    for (let i = 0; i < bytes.length; i++) c = MC_CRC[(c ^ bytes[i]) & 0xFF] ^ (c >>> 8);
    return (c ^ 0xFFFFFFFF) >>> 0;
  }
  // A zip file with no compression, which Excel reads.
  function mcZip(files) {
    const parts = [], central = [];
    let offset = 0;
    for (const f of files) {
      const name = f.nameBytes, data = f.bytes, crc = mcCrc(data);
      const lh = new DataView(new ArrayBuffer(30));
      lh.setUint32(0, 0x04034b50, true); lh.setUint16(4, 20, true); lh.setUint16(6, 0x0800, true);
      lh.setUint16(8, 0, true); lh.setUint16(10, 0, true); lh.setUint16(12, 0x21, true);
      lh.setUint32(14, crc, true); lh.setUint32(18, data.length, true); lh.setUint32(22, data.length, true);
      lh.setUint16(26, name.length, true); lh.setUint16(28, 0, true);
      parts.push(new Uint8Array(lh.buffer), name, data);
      const cd = new DataView(new ArrayBuffer(46));
      cd.setUint32(0, 0x02014b50, true); cd.setUint16(4, 20, true); cd.setUint16(6, 20, true);
      cd.setUint16(8, 0x0800, true); cd.setUint16(10, 0, true); cd.setUint16(12, 0, true); cd.setUint16(14, 0x21, true);
      cd.setUint32(16, crc, true); cd.setUint32(20, data.length, true); cd.setUint32(24, data.length, true);
      cd.setUint16(28, name.length, true); cd.setUint16(30, 0, true); cd.setUint16(32, 0, true);
      cd.setUint16(34, 0, true); cd.setUint16(36, 0, true); cd.setUint32(38, 0, true); cd.setUint32(42, offset, true);
      central.push(new Uint8Array(cd.buffer), name);
      offset += 30 + name.length + data.length;
    }
    let cdSize = 0;
    for (const c of central) cdSize += c.length;
    const end = new DataView(new ArrayBuffer(22));
    end.setUint32(0, 0x06054b50, true); end.setUint16(4, 0, true); end.setUint16(6, 0, true);
    end.setUint16(8, files.length, true); end.setUint16(10, files.length, true);
    end.setUint32(12, cdSize, true); end.setUint32(16, offset, true); end.setUint16(20, 0, true);
    const all = parts.concat(central, [new Uint8Array(end.buffer)]);
    const total = all.reduce((a, b) => a + b.length, 0);
    const out = new Uint8Array(total);
    let p = 0;
    for (const a of all) { out.set(a, p); p += a.length; }
    return out;
  }
  // Styles: 0 plain, 1 bold, 2 percent, 3 number, 4 bold number, 5 bold percent, 6/7 growth green/red, 8/9 bold green/red,
  // 10 header, 11 title, 12 note, 13 region name, 14 region number, 15/16 region growth green/red, 17 header (first column),
  // 18 points, 19/20 points green/red, 21/22 region points green/red.
  const MC_STYLES = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
<fonts count="9">
<font><sz val="11"/><name val="Calibri"/></font>
<font><b/><sz val="11"/><name val="Calibri"/></font>
<font><sz val="11"/><color rgb="FF15803D"/><name val="Calibri"/></font>
<font><sz val="11"/><color rgb="FFB91C1C"/><name val="Calibri"/></font>
<font><b/><sz val="11"/><color rgb="FF15803D"/><name val="Calibri"/></font>
<font><b/><sz val="11"/><color rgb="FFB91C1C"/><name val="Calibri"/></font>
<font><b/><sz val="11"/><color rgb="FFFFFFFF"/><name val="Calibri"/></font>
<font><b/><sz val="14"/><color rgb="FF2D6A6E"/><name val="Calibri"/></font>
<font><i/><sz val="10"/><color rgb="FF5E6D6A"/><name val="Calibri"/></font>
</fonts>
<fills count="4"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill>
<fill><patternFill patternType="solid"><fgColor rgb="FF2D6A6E"/><bgColor indexed="64"/></patternFill></fill>
<fill><patternFill patternType="solid"><fgColor rgb="FFEEF3F2"/><bgColor indexed="64"/></patternFill></fill></fills>
<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>
<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>
<cellXfs count="23">
<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>
<xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1"/>
<xf numFmtId="10" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"/>
<xf numFmtId="3" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"/>
<xf numFmtId="3" fontId="1" fillId="0" borderId="0" xfId="0" applyNumberFormat="1" applyFont="1"/>
<xf numFmtId="10" fontId="1" fillId="0" borderId="0" xfId="0" applyNumberFormat="1" applyFont="1"/>
<xf numFmtId="10" fontId="2" fillId="0" borderId="0" xfId="0" applyNumberFormat="1" applyFont="1"/>
<xf numFmtId="10" fontId="3" fillId="0" borderId="0" xfId="0" applyNumberFormat="1" applyFont="1"/>
<xf numFmtId="10" fontId="4" fillId="0" borderId="0" xfId="0" applyNumberFormat="1" applyFont="1"/>
<xf numFmtId="10" fontId="5" fillId="0" borderId="0" xfId="0" applyNumberFormat="1" applyFont="1"/>
<xf numFmtId="0" fontId="6" fillId="2" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1"><alignment horizontal="center" vertical="center" wrapText="1"/></xf>
<xf numFmtId="0" fontId="7" fillId="0" borderId="0" xfId="0" applyFont="1"/>
<xf numFmtId="0" fontId="8" fillId="0" borderId="0" xfId="0" applyFont="1"/>
<xf numFmtId="0" fontId="1" fillId="3" borderId="0" xfId="0" applyFont="1" applyFill="1"/>
<xf numFmtId="3" fontId="1" fillId="3" borderId="0" xfId="0" applyNumberFormat="1" applyFont="1" applyFill="1"/>
<xf numFmtId="10" fontId="4" fillId="3" borderId="0" xfId="0" applyNumberFormat="1" applyFont="1" applyFill="1"/>
<xf numFmtId="10" fontId="5" fillId="3" borderId="0" xfId="0" applyNumberFormat="1" applyFont="1" applyFill="1"/>
<xf numFmtId="0" fontId="6" fillId="2" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1"><alignment horizontal="left" vertical="center" wrapText="1"/></xf>
<xf numFmtId="2" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"/>
<xf numFmtId="2" fontId="2" fillId="0" borderId="0" xfId="0" applyNumberFormat="1" applyFont="1"/>
<xf numFmtId="2" fontId="3" fillId="0" borderId="0" xfId="0" applyNumberFormat="1" applyFont="1"/>
<xf numFmtId="2" fontId="4" fillId="3" borderId="0" xfId="0" applyNumberFormat="1" applyFont="1" applyFill="1"/>
<xf numFmtId="2" fontId="5" fillId="3" borderId="0" xfId="0" applyNumberFormat="1" applyFont="1" applyFill="1"/>
</cellXfs>
</styleSheet>`;
  const xtext = (v, s = 0) => ({ t: "s", v: String(v), s });
  // One cell for the Excel file, in the same colours as the page.
  function mcXCell(val, c, kind) {
    const region = kind !== "store", m = MC.metric;
    if (!mcOk(val)) return xtext("—", region ? 13 : 0);
    const growth = isGrowth(c);
    const good = m === "dis" ? val < 0 : val >= 0;
    if (growth && m === "dis") return { t: "n", v: String(val), s: region ? (good ? 21 : 22) : (good ? 19 : 20) };
    if (growth) return { t: "n", v: String(val / 100), s: region ? (val >= 0 ? 15 : 16) : (val >= 0 ? 6 : 7) };
    if (m === "dis") return { t: "n", v: String(val / 100), s: region ? 5 : 2 };
    return { t: "n", v: String(val), s: region ? 14 : 3 };
  }
  const mcCol = i => { let s = "", n = i + 1; while (n > 0) { const m = (n - 1) % 26; s = String.fromCharCode(65 + m) + s; n = Math.floor((n - 1) / 26); } return s; };
  function mcCellXml(cell, ref) {
    if (!cell) return "";
    if (cell.t === "n") return `<c r="${ref}" s="${cell.s}"><v>${cell.v}</v></c>`;
    return `<c r="${ref}" s="${cell.s}" t="inlineStr"><is><t xml:space="preserve">${mcEsc(cell.v)}</t></is></c>`;
  }
  // The text width of a cell as it is shown (for the column widths).
  const MC_PCT_STYLES = [2, 5, 6, 7, 8, 9, 15, 16];
  const MC_PTS_STYLES = [18, 19, 20, 21, 22];
  function mcShown(cell) {
    if (cell.t === "s") return String(cell.v).length + 2;
    const n = Number(cell.v);
    if (MC_PCT_STYLES.includes(cell.s)) return (n * 100).toFixed(2).length + 3;
    if (MC_PTS_STYLES.includes(cell.s)) return n.toFixed(2).length + 3;
    return Math.round(n).toLocaleString("en-IN").length + 2;
  }
  // A sheet: title, note, a blank row, the header (row 4), then the body. Columns are as wide as their figures.
  function mcBuild(name, title, sub, headers, body, rowMeta) {
    const rows = [[xtext(title, 11)], [xtext(sub, 12)], [], headers.map((h, i) => xtext(h, i === 0 ? 17 : 10)), ...body];
    const meta = [{}, {}, {}, {}].concat(rowMeta || body.map(() => ({})));
    const widths = headers.map((h, ci) => {
      let w = ci === 0 ? String(h).length + 2 : 12;
      for (const r of body) if (r[ci]) w = Math.max(w, mcShown(r[ci]));
      return Math.min(ci === 0 ? 42 : 26, Math.max(ci === 0 ? 16 : 12, w));
    });
    const lines = Math.max(...headers.map((h, ci) => Math.ceil(String(h).length / Math.max(6, widths[ci] - 1))));
    return { name, rows, widths, headerRow: 4, headerHeight: Math.max(30, 15 * lines + 6), freeze: true, merges: [], rowMeta: meta };
  }
  function mcSheetXml(sh) {
    const hdr = sh.headerRow || 0;
    const meta = sh.rowMeta || [];
    const rowsXml = sh.rows.map((r, ri) => {
      const rn = ri + 1, m = meta[ri] || {};
      let attrs = "";
      if (hdr && rn === hdr && sh.headerHeight) attrs += ` ht="${sh.headerHeight}" customHeight="1"`;
      if (m.outline) attrs += ` outlineLevel="${m.outline}"`;
      if (m.hidden) attrs += ` hidden="1"`;
      if (m.collapsed) attrs += ` collapsed="1"`;
      return `<row r="${rn}"${attrs}>` + r.map((cell, ci) => mcCellXml(cell, mcCol(ci) + rn)).join("") + `</row>`;
    }).join("");
    const outlineOn = meta.some(m => m && m.outline);
    const sheetPr = outlineOn ? `<sheetPr><outlinePr summaryBelow="0"/></sheetPr>` : "";
    const fmt = outlineOn ? `<sheetFormatPr defaultRowHeight="15" outlineLevelRow="1"/>` : "";
    const views = sh.freeze && hdr
      ? `<sheetViews><sheetView workbookViewId="0"><pane xSplit="1" ySplit="${hdr}" topLeftCell="B${hdr + 1}" activePane="bottomRight" state="frozen"/></sheetView></sheetViews>`
      : "";
    const cols = sh.widths ? `<cols>${sh.widths.map((w, i) => `<col min="${i + 1}" max="${i + 1}" width="${w}" customWidth="1"/>`).join("")}</cols>` : "";
    const merges = sh.merges && sh.merges.length ? `<mergeCells count="${sh.merges.length}">${sh.merges.map(r => `<mergeCell ref="${r}"/>`).join("")}</mergeCells>` : "";
    return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">` +
      `${sheetPr}${views}${fmt}${cols}<sheetData>${rowsXml}</sheetData>${merges}</worksheet>`;
  }

  // Area manager sheet: each manager's row, with its stores grouped underneath, collapsed so they expand with the +.
  function mcAmSheet(c, s) {
    const headers = ["Area manager / Store", ...s.cols.map(x => mcHead(x))];
    const body = [], meta = [];
    body.push([xtext("All areas", 13), ...s.cols.map(x => mcXCell(mcCellVal(mcTotals(c.list), x), x, "region"))]);
    meta.push({});
    for (const g of mcAmGroups(c)) {
      body.push([xtext(g.name, 13), ...s.cols.map(x => mcXCell(mcCellVal(g.tot, x), x, "region"))]);
      meta.push({ collapsed: true });
      for (const st of g.stores) {
        body.push([xtext("   " + st.name, 0), ...s.cols.map(x => mcXCell(mcCellVal(st, x), x, "store"))]);
        meta.push({ outline: 1, hidden: true });
      }
    }
    return mcBuild("Area manager", "Morning Call: stores by area manager",
      `${fmtLong(c.ref)}. Use the + beside an area manager to expand its stores.`, headers, body, meta);
  }
  // Region wise sheet, every table side by side: each table's name above its columns.
  function mcRegionWiseSheet(c, blocks, srcT) {
    const per = blocks.map(b => ({ b, g: mcGroup(c.list, b) }));
    const names = mcSummary(c).rows.map(r => r.name);
    const groupRow = [xtext("", 10)], headerRow = [xtext("Region", 17)], merges = [];
    let col = 1;
    for (const { b } of per) {
      groupRow.push(xtext(b.title, 10));
      for (let i = 1; i < b.cols.length; i++) groupRow.push(xtext("", 10));
      if (b.cols.length > 1) merges.push(`${mcCol(col)}4:${mcCol(col + b.cols.length - 1)}4`);
      for (const cc of b.cols) headerRow.push(xtext(mcHead(cc), 10));
      col += b.cols.length;
    }
    const body = [];
    for (const name of names) {
      const row = [xtext(name, 13)];
      for (const { b, g } of per) {
        const obj = name === "All regions" ? g.all : ((g.groups.find(x => x.name === name) || { tot: { R: {}, P: {} } }).tot);
        for (const cc of b.cols) row.push(mcXCell(mcCellVal(obj, cc), cc, "region"));
      }
      body.push(row);
      if (srcT) {
        for (const label of Object.keys(srcT)) {
          const r2 = [xtext("   " + label, 0)];
          for (const { b } of per) {
            const obj = ((srcT[label][b.id] || {})[name]) || { R: {}, P: {} };
            for (const cc of b.cols) r2.push(mcXCell(mcCellVal(obj, cc), cc, "store"));
          }
          body.push(r2);
        }
      }
    }
    const headers = headerRow.map(h => h.v);
    const widths = headers.map((h, ci) => {
      let w = ci === 0 ? h.length + 2 : 12;
      for (const r of body) if (r[ci]) w = Math.max(w, mcShown(r[ci]));
      return Math.min(ci === 0 ? 42 : 26, Math.max(ci === 0 ? 16 : 12, w));
    });
    const lines = Math.max(...headers.map((h, ci) => Math.ceil(h.length / Math.max(6, widths[ci] - 1))));
    return {
      name: "Region wise",
      rows: [[xtext("Morning Call: region wise, all tables", 11)], [xtext(`${fmtLong(c.ref)}. One row per region.`, 12)], [], groupRow, headerRow, ...body],
      widths, headerRow: 5, headerHeight: Math.max(30, 15 * lines + 6), freeze: true, merges
    };
  }
  // Region + source sheet: each region followed by its sources.
  function mcRegionSourceSheet(c, blocks, srcT) {
    const sh = mcRegionWiseSheet(c, blocks, srcT);
    sh.name = "Region + source";
    sh.rows[0] = [xtext("Morning Call: region + source, all tables", 11)];
    sh.rows[1] = [xtext(`${fmtLong(c.ref)}. Each region is followed by its sources.`, 12)];
    return sh;
  }
  // Region + store, for one table: each region, then its stores.
  function mcStoreSheet(b, g) {
    const headers = ["Region / Store", ...b.cols.map(c => mcHead(c))];
    const body = [];
    for (const r of g.groups) {
      body.push([xtext(r.name, 13), ...b.cols.map(c => mcXCell(mcCellVal(r.tot, c), c, "region"))]);
      for (const s of r.stores) body.push([xtext("   " + s.name, 0), ...b.cols.map(c => mcXCell(mcCellVal(s, c), c, "store"))]);
    }
    return mcBuild(`${b.sheet} Store`, `${b.title}: region + store`, b.sub, headers, body);
  }
  // Kept for compatibility: the rows of the region + store sheet for one table.
  function mcSheetRows(block, g) { return mcStoreSheet(block, g).rows; }
  // sheets: [{name, rows, ...}] -> the bytes of an .xlsx file.
  function mcXlsx(sheets) {
    const enc = new TextEncoder();
    const files = [];
    const add = (name, text) => files.push({ nameBytes: enc.encode(name), bytes: enc.encode(text) });
    add("[Content_Types].xml", `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>${sheets.map((s, i) => `<Override PartName="/xl/worksheets/sheet${i + 1}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>`).join("")}</Types>`);
    add("_rels/.rels", `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>`);
    add("xl/workbook.xml", `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>${sheets.map((s, i) => `<sheet name="${mcEsc(s.name)}" sheetId="${i + 1}" r:id="rId${i + 1}"/>`).join("")}</sheets></workbook>`);
    add("xl/_rels/workbook.xml.rels", `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">${sheets.map((s, i) => `<Relationship Id="rId${i + 1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet${i + 1}.xml"/>`).join("")}<Relationship Id="rId${sheets.length + 1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/></Relationships>`);
    add("xl/styles.xml", MC_STYLES);
    sheets.forEach((s, i) => add(`xl/worksheets/sheet${i + 1}.xml`, mcSheetXml(s)));
    return mcZip(files);
  }
  // The workbook, from the current figures and the metric chosen: the Area manager sheet (stores grouped), the Region wise
  // sheet and the Region + source sheet, then a Region + store sheet for each table.
  function mcWorkbook(c, blocks) {
    const srcT = mcSourceTotals(blocks);
    const summary = mcSummary(c);
    const sheets = [mcAmSheet(c, summary), mcRegionWiseSheet(c, blocks, null), mcRegionSourceSheet(c, blocks, srcT)];
    for (const b of blocks) sheets.push(mcStoreSheet(b, mcGroup(c.list, b)));
    return mcXlsx(sheets);
  }
  function mcDownload(bytes, name) {
    const blob = new Blob([bytes], { type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = name;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  document.addEventListener("click", e => {
    const mm = e.target.closest && e.target.closest("[data-mc-metric]");
    if (mm) { MC.metric = mm.dataset.mcMetric; render(); return; }
    const rg = e.target.closest && e.target.closest("[data-mc-region]");
    if (rg) {
      const nm = rg.dataset.mcRegion;
      if (MC.regOpen.has(nm)) MC.regOpen.delete(nm); else MC.regOpen.add(nm);
      render();
      return;
    }
    const b = e.target.closest && e.target.closest("[data-mc-xlsx]");
    if (!b || !MC.last) return;
    mcDownload(mcWorkbook(MC.last.c, MC.last.blocks), `Morning_Call_${mcIso(MC.last.c.ref)}.xlsx`);
  });

  // Tab and wiring: the Morning Call tab is added to the Sales tabs; every other tab is left as it is.
  if (typeof SUBS !== "undefined" && SUBS.sales && !SUBS.sales.some(x => x[0] === "morning")) {
    SUBS.sales.push(["morning", "Morning Call"]);
  }
  const salesPeriodViewMc = salesPeriodView;
  salesPeriodView = function (kind) {
    return kind === "morning" ? morningView() : salesPeriodViewMc(kind);
  };

  window.FB_MORNING = { MC, mcCompute, mcBlocks, mcGroup, mcTotals, mcCellVal, mcVal, mcGrowth, mcSourceTotals, mcSourceGroups,
                        mcSummary, mcSheetRows, mcXlsx, mcWorkbook, mcAmGroups, mcLateMap, mcLoadHourly: mcLoadHourlyOnce, morningView };
})();
