/* ============ Sales: Morning Call sub-tab ============
   Region-wise and store-wise tables for the reference day (the latest day of data):
   MoM (MTD), YoY (same day and YTD), WoW, FTD vs Pre FTD, and Late Night (0-5 AM).
   Stores are listed under their region, and the region totals sit on the region row.
   The page's Sales filters apply. Download as Excel: one sheet per table.
   Loaded after insights.js. It wraps salesPeriodView() for the Morning Call tab only. */
(function () {
  const MC = { hourly: null, hourlyDays: null, hourlyLoading: false, hourlyError: null, last: null };
  const MC_LATE_FROM = 15, MC_LATE_TO = 19;   // hour 0 is 9 AM, so hours 15 to 19 are 0 to 5 AM
  const mcEsc = s => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  const mcOk = v => v !== null && v !== undefined && isFinite(v);
  const mcRs = v => (mcOk(v) ? "₹" + num(v) : "—");
  const mcP = v => (mcOk(v) ? (v >= 0 ? "+" : "−") + Math.abs(v).toFixed(1) + "%" : "—");
  const mcC = v => (mcOk(v) ? (v >= 0 ? "var(--up)" : "var(--down)") : "var(--ink-2)");
  const mcG = (cur, prev) => (mcOk(cur) && mcOk(prev) && prev ? (cur / prev - 1) * 100 : null);
  const mcMonthKey = d => (toDate(d).getUTCFullYear() - 2025) * 12 + toDate(d).getUTCMonth();
  const mcDom = d => toDate(d).getUTCDate();
  const mcDim = d => new Date(Date.UTC(toDate(d).getUTCFullYear(), toDate(d).getUTCMonth() + 1, 0)).getUTCDate();
  const mcIso = d => toDate(d).toISOString().slice(0, 10);

  // Net sales (rupees) of a store over a range of days, with the page filters. Null when there are no rows.
  function mcNet(rows, from, to) {
    if (!rows) return null;
    let t = 0, any = false;
    for (const r of rows) {
      if (r[0] < from || r[0] > to || !salesPass(r)) continue;
      t += r[7]; any = true;
    }
    return any ? t / 100 : null;
  }

  // Sales for hours 0 to 5 AM, by store and day, for the days given (rupees).
  function mcLateMap(days) {
    const m = new Map();
    if (!MC.hourly) return m;
    for (const r of MC.hourly) {
      if (!days.includes(r[0]) || r[7] < MC_LATE_FROM || r[7] > MC_LATE_TO || !salesPass(r)) continue;
      const k = r[1] + "|" + r[0];
      m.set(k, (m.get(k) || 0) + r[8]);
    }
    for (const k of Array.from(m.keys())) m.set(k, m.get(k) / 100);
    return m;
  }
  // Null when the hourly file does not hold that day; otherwise the store's 0-5 AM sales (0 if none).
  function mcLate(lateMap, st, day) {
    if (!MC.hourlyDays || !MC.hourlyDays.has(day)) return null;
    return lateMap.get(st + "|" + day) || 0;
  }

  // Every number the tables need, for each store, on the reference day.
  function mcCompute() {
    const ref = M.last_day;
    const curKey = mcMonthKey(ref), prevKey = curKey - 1, yearKey = (toDate(ref).getUTCFullYear() - 2025) * 12;
    const dom = mcDom(ref), dim = mcDim(ref), balance = dim - dom;
    const ly = shiftMonths(ref, -12), lm = shiftMonths(ref, -1);
    const lyDay = window.FB_INS.insSameWeekday(ref, 12), lmDay = window.FB_INS.insSameWeekday(ref, 1);
    const lateMap = mcLateMap([ref - 7, ref]);
    const list = [];
    for (const s of M.stores) {
      const R = STORE_ROWS.get(s.i);
      if (!R) continue;
      const v = { i: s.i, name: s.name, region: s.region || "Unmapped" };
      v.octFull = mcNet(R, monStart(prevKey), monEnd(prevKey));
      v.octSame = mcNet(R, monStart(prevKey), lm);
      v.novMtd = mcNet(R, monStart(curKey), ref);
      v.trend = mcOk(v.novMtd) ? v.novMtd / dom * balance + v.novMtd : null;
      v.trendVsOct = mcG(v.trend, v.octFull);
      v.mom = mcG(v.novMtd, v.octSame);
      v.dayCur = mcNet(R, ref, ref);
      v.momDayPrev = mcNet(R, lmDay, lmDay);
      v.momDay = mcG(v.dayCur, v.momDayPrev);
      v.yoyLy = mcNet(R, lyDay, lyDay);
      v.yoyTy = v.dayCur;
      v.yoy = mcG(v.yoyTy, v.yoyLy);
      v.ytdLy = mcNet(R, monStart(yearKey - 12), ly);
      v.ytdTy = mcNet(R, monStart(yearKey), ref);
      v.ytdYoy = mcG(v.ytdTy, v.ytdLy);
      v.wowPrev = mcNet(R, ref - 7, ref - 7);
      v.wowCur = mcNet(R, ref, ref);
      v.wow = mcG(v.wowCur, v.wowPrev);
      v.ftdPre = mcNet(R, ref - 1, ref - 1);
      v.ftdCur = v.wowCur;
      v.ftd = mcG(v.ftdCur, v.ftdPre);
      v.lnPrev = mcLate(lateMap, s.i, ref - 7);
      v.lnCur = mcLate(lateMap, s.i, ref);
      v.ln = mcG(v.lnCur, v.lnPrev);
      // a store is in the table when it has daily sales under the page filters (hourly zeros alone do not count)
      if (mcOk(v.octFull) || mcOk(v.novMtd) || mcOk(v.wowCur) || mcOk(v.ftdPre) || mcOk(v.yoyTy) || mcOk(v.ytdTy)) list.push(v);
    }
    return { ref, dom, balance, list };
  }

  // The tables. c = the numbers for each store; each column names the number it shows, or a growth between two.
  function mcBlocks(c) {
    const ref = c.ref, ref7 = ref - 7, ly = shiftMonths(ref, -12), lm = shiftMonths(ref, -1);
    const lyDay = window.FB_INS.insSameWeekday(ref, 12), lmDay = window.FB_INS.insSameWeekday(ref, 1);
    const curKey = mcMonthKey(ref), prevKey = curKey - 1;
    const dl = d => toDate(d).toLocaleDateString("en-IN", { day: "numeric", month: "short", year: "numeric", timeZone: "UTC" });
    const sCur = ref - (c.dom - 1), sPrev = monStart(prevKey);
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
        sub: `Same day: the same weekday in the same week of the year before, ${dl(lyDay)} against ${dl(ref)}. YTD: 1 Jan to the same date, each year.`,
        main: "ytdTy", sort: "yoy",
        cols: [
          { k: "yoyLy", h: `Same day last year (${dl(lyDay)})` },
          { k: "yoyTy", h: `Same day (${dl(ref)})` },
          { k: "yoy", h: "YoY %", g: ["yoyTy", "yoyLy"] },
          { k: "ytdLy", h: `YTD last year (1 Jan to ${dl(ly)})` },
          { k: "ytdTy", h: `YTD (1 Jan to ${dl(ref)})` },
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

  // Totals for a set of stores. A growth total uses only the stores that have both figures.
  function mcTotalsOf(list, block) {
    const t = {};
    for (const c of block.cols) {
      if (c.g) continue;
      const vals = list.map(s => s[c.k]).filter(mcOk);
      t[c.k] = vals.length ? vals.reduce((a, b) => a + b, 0) : null;
    }
    for (const c of block.cols) {
      if (!c.g) continue;
      const [cur, prev] = c.g;
      const pairs = list.filter(s => mcOk(s[cur]) && mcOk(s[prev]));
      const pc = pairs.reduce((a, s) => a + s[cur], 0), pp = pairs.reduce((a, s) => a + s[prev], 0);
      t[c.k] = pairs.length && pp ? (pc / pp - 1) * 100 : null;
    }
    return t;
  }

  // Regions (largest first on the block's main figure), each with its stores (worst growth first); nulls last.
  function mcGroup(list, block) {
    const groups = new Map();
    for (const s of list) {
      if (!groups.has(s.region)) groups.set(s.region, []);
      groups.get(s.region).push(s);
    }
    const sortVal = v => (mcOk(v) ? v : -Infinity);
    const out = Array.from(groups.entries()).map(([name, stores]) => ({
      name,
      stores: stores.slice().sort((a, b) => {
        const x = a[block.sort], y = b[block.sort];
        if (!mcOk(x) && !mcOk(y)) return 0;
        if (!mcOk(x)) return 1;
        if (!mcOk(y)) return -1;
        return x - y;
      }),
      tot: mcTotalsOf(stores, block)
    }));
    out.sort((a, b) => {
      const x = sortVal(a.tot[block.main]), y = sortVal(b.tot[block.main]);
      return x === y ? 0 : y > x ? 1 : -1;
    });
    return { groups: out, all: mcTotalsOf(list, block) };
  }

  // ---- the page ----
  function mcCell(c, obj) {
    const v = obj[c.k];
    return c.g ? `<td style="color:${mcC(v)}">${mcP(v)}</td>` : `<td>${mcRs(v)}</td>`;
  }
  // Region-wise: one row per region, with the region totals (no stores).
  function mcRegionTable(block, g) {
    const head = `<tr><th>Region</th>${block.cols.map(c => `<th>${mcEsc(c.h)}</th>`).join("")}</tr>`;
    const rows = [`<tr class="reg-total"><td>All regions</td>${block.cols.map(c => mcCell(c, g.all)).join("")}</tr>`]
      .concat(g.groups.map(r => `<tr><td>${mcEsc(r.name)}</td>${block.cols.map(c => mcCell(c, r.tot)).join("")}</tr>`));
    return `<div class="scroll"><table class="tbl"><thead>${head}</thead><tbody>${rows.join("")}</tbody></table></div>`;
  }
  // Region + store: each region row with its totals, and its stores underneath.
  function mcStoreTable(block, g) {
    const head = `<tr><th>Region / Store</th>${block.cols.map(c => `<th>${mcEsc(c.h)}</th>`).join("")}</tr>`;
    const rows = [];
    for (const r of g.groups) {
      rows.push(`<tr class="reg-head"><td>${mcEsc(r.name)}</td>${block.cols.map(c => mcCell(c, r.tot)).join("")}</tr>`);
      for (const s of r.stores) rows.push(`<tr><td>&nbsp;&nbsp;${mcEsc(s.name)}</td>${block.cols.map(c => mcCell(c, s)).join("")}</tr>`);
    }
    return `<div class="scroll"><table class="tbl"><thead>${head}</thead><tbody>${rows.join("")}</tbody></table></div>`;
  }

  // The region-wise summary: one row per region, with the key figure from each table.
  const MC_SUMMARY = [
    { h: "MTD sales", id: "mom", k: "novMtd" },
    { h: "MoM %", id: "mom", k: "mom", g: true },
    { h: "Same day MoM %", id: "mom", k: "momDay", g: true },
    { h: "Trend (projection)", id: "mom", k: "trend" },
    { h: "Same day YoY %", id: "yoy", k: "yoy", g: true },
    { h: "YTD YoY %", id: "yoy", k: "ytdYoy", g: true },
    { h: "WoW %", id: "wow", k: "wow", g: true },
    { h: "FTD %", id: "ftd", k: "ftd", g: true },
    { h: "Late Night %", id: "late", k: "ln", g: true }
  ];
  function mcSummary(c, blocks) {
    const grp = {};
    for (const b of blocks) grp[b.id] = mcGroup(c.list, b);
    const totOf = (id, name) => (name === "All regions" ? grp[id].all : ((grp[id].groups.find(g => g.name === name) || { tot: {} }).tot));
    const names = new Set();
    for (const b of blocks) for (const g of grp[b.id].groups) names.add(g.name);
    const sortVal = v => (mcOk(v) ? v : -Infinity);
    const order = Array.from(names).sort((x, y) => {
      const p = sortVal(totOf("mom", x).novMtd), q = sortVal(totOf("mom", y).novMtd);
      return p === q ? 0 : q > p ? 1 : -1;
    });
    const rowFor = name => ({ name, vals: MC_SUMMARY.map(m => totOf(m.id, name)[m.k]) });
    return { cols: MC_SUMMARY, rows: [rowFor("All regions")].concat(order.map(rowFor)) };
  }
  function mcSummaryTable(s) {
    const head = `<tr><th>Region</th>${s.cols.map(c => `<th>${mcEsc(c.h)}</th>`).join("")}</tr>`;
    const cell = (c, v) => (c.g ? `<td style="color:${mcC(v)}">${mcP(v)}</td>` : `<td>${mcRs(v)}</td>`);
    const body = s.rows.map((r, i) => `<tr${i === 0 ? ' class="reg-total"' : ""}><td>${mcEsc(r.name)}</td>` +
      r.vals.map((v, j) => cell(s.cols[j], v)).join("") + `</tr>`).join("");
    return `<div class="scroll"><table class="tbl"><thead>${head}</thead><tbody>${body}</tbody></table></div>`;
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
    const summary = mcSummary(c, blocks);
    MC.last = { c, blocks, summary };
    let late = "";
    if (!M.has_hourly) late = "The hourly file is not in the data, so Late Night shows —.";
    else if (MC.hourlyError) late = `Hourly data could not be loaded: ${mcEsc(MC.hourlyError)}`;
    else if (!MC.hourly) late = "Loading hourly data for Late Night.";
    return `<section class="block"><h2>Morning Call: ${fmtLong(c.ref)}</h2>` +
      `<p class="sub">Reference day: the latest day of data. The Sales filters above apply to every table.</p>` +
      `<button data-mc-xlsx="1" class="mc-btn">Download Excel</button>` +
      (late ? `<p class="muted">${late}</p>` : "") + `</section>` +
      `<section class="block"><h2>Region wise summary</h2>` +
      `<p class="sub">One row per region, with the key figure from each table below.</p>${mcSummaryTable(summary)}</section>` +
      blocks.map(b => {
        const g = mcGroup(c.list, b);
        return `<section class="block"><h2>${mcEsc(b.title)}</h2><p class="sub">${mcEsc(b.sub)}</p>` +
          `<p class="sub"><b>Region wise</b></p>${mcRegionTable(b, g)}` +
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
<cellXfs count="18">
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
</cellXfs>
</styleSheet>`;
  // Cells. Styles: 0 plain text, 3 number, 6/7 growth green/red, 10 header, 11 title, 12 note, 13 region name,
  // 14 region number, 15/16 region growth green/red, 17 header (first column). Growth is green when up or flat.
  const xtext = (v, s = 0) => ({ t: "s", v: String(v), s });
  function mcCellFor(val, kind, growth) {
    const region = kind !== "store";
    if (!mcOk(val)) return xtext("—", region ? 13 : 0);
    if (growth) return { t: "n", v: String(val / 100), s: region ? (val >= 0 ? 15 : 16) : (val >= 0 ? 6 : 7) };
    return { t: "n", v: String(val), s: region ? 14 : 3 };
  }
  const mcCol = i => { let s = "", n = i + 1; while (n > 0) { const m = (n - 1) % 26; s = String.fromCharCode(65 + m) + s; n = Math.floor((n - 1) / 26); } return s; };
  function mcCellXml(cell, ref) {
    if (!cell) return "";
    if (cell.t === "n") return `<c r="${ref}" s="${cell.s}"><v>${cell.v}</v></c>`;
    return `<c r="${ref}" s="${cell.s}" t="inlineStr"><is><t xml:space="preserve">${mcEsc(cell.v)}</t></is></c>`;
  }
  // The text width of a cell as it is shown (for the column widths).
  function mcShown(cell) {
    if (cell.t === "s") return String(cell.v).length + 2;
    const n = Number(cell.v);
    if ([2, 5, 6, 7, 8, 9, 15, 16].includes(cell.s)) return (n * 100).toFixed(2).length + 3;
    return Math.round(n).toLocaleString("en-IN").length + 2;
  }
  // A sheet: title, note, a blank row, the header (row 4), then the body. Columns are as wide as their figures,
  // the header wraps, and the first column and the header stay in view.
  function mcBuild(name, title, sub, headers, body) {
    const rows = [[xtext(title, 11)], [xtext(sub, 12)], [], headers.map((h, i) => xtext(h, i === 0 ? 17 : 10)), ...body];
    const widths = headers.map((h, ci) => {
      let w = ci === 0 ? String(h).length + 2 : 12;
      for (const r of body) if (r[ci]) w = Math.max(w, mcShown(r[ci]));
      return Math.min(ci === 0 ? 42 : 26, Math.max(ci === 0 ? 16 : 12, w));
    });
    const lines = Math.max(...headers.map((h, ci) => Math.ceil(String(h).length / Math.max(6, widths[ci] - 1))));
    return { name, rows, widths, headerRow: 4, headerHeight: Math.max(30, 15 * lines + 6), freeze: true };
  }
  function mcSheetXml(sh) {
    const hdr = sh.headerRow || 0;
    const rowsXml = sh.rows.map((r, ri) => {
      const rn = ri + 1;
      const ht = hdr && rn === hdr && sh.headerHeight ? ` ht="${sh.headerHeight}" customHeight="1"` : "";
      return `<row r="${rn}"${ht}>` + r.map((cell, ci) => mcCellXml(cell, mcCol(ci) + rn)).join("") + `</row>`;
    }).join("");
    const views = sh.freeze && hdr
      ? `<sheetViews><sheetView workbookViewId="0"><pane xSplit="1" ySplit="${hdr}" topLeftCell="B${hdr + 1}" activePane="bottomRight" state="frozen"/></sheetView></sheetViews>`
      : "";
    const cols = sh.widths ? `<cols>${sh.widths.map((w, i) => `<col min="${i + 1}" max="${i + 1}" width="${w}" customWidth="1"/>`).join("")}</cols>` : "";
    const merges = sh.merges && sh.merges.length ? `<mergeCells count="${sh.merges.length}">${sh.merges.map(r => `<mergeCell ref="${r}"/>`).join("")}</mergeCells>` : "";
    return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">${views}${cols}<sheetData>${rowsXml}</sheetData>${merges}</worksheet>`;
  }
  // One Region wise sheet for every table: each table's columns side by side, with the table's name above them.
  // Rows: All regions, then each region (largest MTD first). A region a table does not hold shows —.
  function mcRegionWiseSheet(c, blocks) {
    const per = blocks.map(b => ({ b, g: mcGroup(c.list, b) }));
    const names = mcSummary(c, blocks).rows.map(r => r.name);
    const groupRow = [xtext("", 10)], headerRow = [xtext("Region", 17)], merges = [];
    let col = 1;
    for (const { b } of per) {
      groupRow.push(xtext(b.title, 10));
      for (let i = 1; i < b.cols.length; i++) groupRow.push(xtext("", 10));
      if (b.cols.length > 1) merges.push(`${mcCol(col)}4:${mcCol(col + b.cols.length - 1)}4`);
      for (const cc of b.cols) headerRow.push(xtext(cc.h, 10));
      col += b.cols.length;
    }
    const body = names.map(name => {
      const row = [xtext(name, 13)];
      for (const { b, g } of per) {
        const obj = name === "All regions" ? g.all : ((g.groups.find(x => x.name === name) || { tot: {} }).tot);
        for (const cc of b.cols) row.push(mcCellFor(obj[cc.k], "region", cc.g));
      }
      return row;
    });
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
  // Region + store, for one table: each region, then its stores.
  function mcStoreSheet(b, g) {
    const headers = ["Region / Store", ...b.cols.map(c => c.h)];
    const body = [];
    for (const r of g.groups) {
      body.push([xtext(r.name, 13), ...b.cols.map(c => mcCellFor(r.tot[c.k], "region", c.g))]);
      for (const s of r.stores) body.push([xtext("   " + s.name, 0), ...b.cols.map(c => mcCellFor(s[c.k], "store", c.g))]);
    }
    return mcBuild(`${b.sheet} Store`, `${b.title}: region + store`, b.sub, headers, body);
  }
  // Kept for compatibility: the rows of the region + store sheet for one table.
  function mcSheetRows(block, g) {
    return mcStoreSheet(block, g).rows;
  }
  // sheets: [{name, rows, widths?, headerRow?, ...}] -> the bytes of an .xlsx file.
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
  // One Region wise sheet for all tables, then a Store sheet for each table.
  function mcWorkbook(c, blocks) {
    const sheets = [mcRegionWiseSheet(c, blocks)];
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

  window.FB_MORNING = { MC, mcCompute, mcBlocks, mcGroup, mcTotalsOf, mcSheetRows, mcXlsx, mcWorkbook, mcSummary, mcRegionWiseSheet, mcStoreSheet, mcLateMap, mcLoadHourly, morningView };
})();
