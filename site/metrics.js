/* ============ Sales > Metrics: Swiggy and Zomato funnel and ads ============
   Comparison A and comparison B (each a from and to date) at the top, then the summary, three line charts and a
   day-by-day table for each comparison. Filters: brand, store type, region and source (All, Swiggy, Zomato).
   Calculations (ratios from the sums, never averaged):
     M2O% = Orders / Menu Open,  C2O% = Orders / Cart,  In-Organic% = Ad Orders / Orders,  Organic% = 1 - In-Organic%.
   Data: data/metrics.json.gz, built by build_metrics.py from the Metrics sheet. Loaded after productmix.js.
   It wraps salesPeriodView() for the Metrics tab only. */
(function () {
  const MT = { data: null, loading: false, error: null, a: null, b: null, brand: "all", type: "all", region: "all", source: "all", store: "all", perf: { dim: "brand", open: new Set() }, gr: { day: null }, chart: { m2o: { src: "all", show: { "M2O%": true, "C2O%": true } }, org: { src: "all", show: { "Organic%": true, "In-Organic%": true } }, clk: { src: "all", show: { "Clicks": true, "Ad Orders": true } } } };
  const MT_EPOCH = Date.UTC(2025, 0, 1);
  const COL = { m2o: "#2D6A6E", c2o: "#E07A3F", org: "#7A5CC2", inorg: "#C4413B", clicks: "#3C8DBC", ad: "#8A9B0F" };
  const mtEsc = s => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  const mtDay = iso => Math.round((Date.parse(iso + "T00:00:00Z") - MT_EPOCH) / 86400000);
  const mtIso = day => new Date(MT_EPOCH + day * 86400000).toISOString().slice(0, 10);
  const mtLabel = day => new Date(MT_EPOCH + day * 86400000).toLocaleDateString("en-IN", { day: "numeric", month: "short", timeZone: "UTC" });
  const mtNum = v => (v === null || v === undefined || !isFinite(v) ? "—" : Math.round(v).toLocaleString("en-IN"));
  const mtPct = v => (v === null || v === undefined || !isFinite(v) ? "—" : v.toFixed(2) + "%");
  const mtPts = v => (v === null || v === undefined || !isFinite(v) ? "—" : (v >= 0 ? "+" : "−") + Math.abs(v).toFixed(2) + " pts");
  const mtChg = (a, b) => (a === null || b === null || !b || !isFinite(a) || !isFinite(b) ? null : (a / b - 1) * 100);
  const mtPctChg = v => (v === null ? "—" : (v >= 0 ? "+" : "−") + Math.abs(v).toFixed(1) + "%");

  // Says why the file could not be loaded, in terms the user can act on.
  function mtErrorText(err) {
    const msg = String((err && err.message) || err || "");
    if (/Failed to fetch|NetworkError|Load failed|TypeError/i.test(msg))
      return "The browser could not reach data/metrics.json.gz. The file is not published for this login yet, or the login session has expired. Reload the page, or load the file below.";
    if (/\(404\)/.test(msg)) return "data/metrics.json.gz is not on the server yet. Load the file below.";
    return "Metrics could not be loaded (" + msg + "). Load the file below.";
  }
  // Sets the data and the default comparison periods (A = the last 7 days, B = the 7 days before).
  function mtUse(d) {
    if (!d || !Array.isArray(d.rows) || !Array.isArray(d.sources) || !d.rows.length)
      throw new Error("this is not a metrics file (metrics.json.gz from build_metrics.py)");
    MT.data = d;
    MT.error = null;
    const last = Math.max(...d.rows.map(r => r[0]));
    MT.a = { from: last - 6, to: last };
    MT.b = { from: last - 13, to: last - 7 };
  }
  // Reads a file chosen on the page: gzip (.gz) or plain JSON.
  async function mtParse(buf, name) {
    let text;
    if (/\.gz$/i.test(name || "")) {
      const stream = new Blob([buf]).stream().pipeThrough(new DecompressionStream("gzip"));
      text = await new Response(stream).text();
    } else {
      text = new TextDecoder().decode(buf);
    }
    return JSON.parse(text);
  }
  // The uploaded file is kept in this browser (localStorage), so it is still there after a refresh.
  const MT_KEY = "fb-metrics-file";
  function mtB64(buf) {
    const bytes = new Uint8Array(buf);
    let s = "";
    for (let i = 0; i < bytes.length; i += 0x8000) s += String.fromCharCode.apply(null, bytes.subarray(i, i + 0x8000));
    return btoa(s);
  }
  function mtUnB64(text) {
    const bin = atob(text);
    const out = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out.buffer;
  }
  function mtRemember(buf, name) {
    try { localStorage.setItem(MT_KEY, JSON.stringify({ name, data: mtB64(buf), saved: new Date().toISOString() })); } catch (e) { /* no storage: the file is used for this visit only */ }
  }
  function mtForget() {
    try { localStorage.removeItem(MT_KEY); } catch (e) { /* nothing to remove */ }
  }
  async function mtFromStorage() {
    let saved = null;
    try { saved = JSON.parse(localStorage.getItem(MT_KEY) || "null"); } catch (e) { saved = null; }
    if (!saved || !saved.data) return false;
    mtUse(await mtParse(mtUnB64(saved.data), saved.name));
    MT.savedName = saved.name;
    MT.savedAt = saved.saved;
    return true;
  }
  async function mtLoad() {
    if (MT.data || MT.loading) return;
    MT.loading = true;
    try {
      mtUse(await loadGz("data/metrics.json.gz"));
    } catch (err) {
      MT.error = mtErrorText(err);
      try { if (await mtFromStorage()) MT.error = null; } catch (e2) { /* the saved file cannot be used */ }
    }
    MT.loading = false;
    render();
  }
  async function mtPickFile(file) {
    if (!file) return;
    MT.loading = true;
    MT.error = null;
    try {
      const buf = await file.arrayBuffer();
      mtUse(await mtParse(buf, file.name));
      mtRemember(buf, file.name);
      MT.savedName = file.name;
      MT.savedAt = new Date().toISOString();
    } catch (err) {
      MT.error = "That file could not be read as metrics data: " + err.message;
    }
    MT.loading = false;
    render();
  }
  function mtUploadControl() {
    return `<label class="mt-f"><span class="muted">Load the metrics file (metrics.json.gz)</span>` +
      `<input type="file" data-mt-file="1" accept=".gz,.json,application/gzip,application/json"></label>`;
  }

  // Does a row pass the filters? Row: [day, source, brand, region, storeType, menuOpen, cart, orders, clicks, adOrders].
  // The Sales tab's own filters (brand, region, store type, source and store), by name. A filter the data does not
  // have (for example Session, or Offline) is left out.
  function mtPageFilters() {
    // The page's own filter values and manifest, read through local names (a top-level let or const on the page
    // is not a property of window, so window.S would be empty).
    const pageS = (typeof S !== "undefined" && S) || {};
    const pageM = (typeof M !== "undefined" && M) || {};
    const pick = (v, list) => {
      if (v === undefined || v === null || v === "all" || v === "") return null;
      if (!isNaN(+v) && list && list[+v] !== undefined) return list[+v];
      return String(v);
    };
    let store = null;
    if (pageS.store !== undefined && pageS.store !== null && pageS.store !== "all" && pageS.store !== "") {
      if (!isNaN(+pageS.store) && pageM.stores) {
        const s = pageM.stores.find(x => x.i === +pageS.store);
        store = s ? s.name : null;
      } else {
        store = String(pageS.store);
      }
    }
    return { source: pick(pageS.source, pageM.sources), brand: pick(pageS.brand, pageM.brands),
             region: pick(pageS.region, pageM.regions), type: pick(pageS.type, pageM.storeTypes), store };
  }
  // Does a row pass the filters? Row: [day, source, brand, region, storeType, menuOpen, cart, orders, clicks, adOrders, store].
  function mtPass(r, pf) {
    const d = MT.data, p = pf || mtPageFilters();
    if (MT.source !== "all" && d.sources[r[1]] !== MT.source) return false;
    if (MT.brand !== "all" && d.brands[r[2]] !== MT.brand) return false;
    if (MT.region !== "all" && d.regions[r[3]] !== MT.region) return false;
    if (MT.type !== "all" && d.storeTypes[r[4]] !== MT.type) return false;
    if (MT.store !== "all" && (!d.stores || d.stores[r[10]] !== MT.store)) return false;
    if (p.source && d.sources.includes(p.source) && d.sources[r[1]] !== p.source) return false;
    if (p.brand && d.brands.includes(p.brand) && d.brands[r[2]] !== p.brand) return false;
    if (p.region && d.regions.includes(p.region) && d.regions[r[3]] !== p.region) return false;
    if (p.type && d.storeTypes.includes(p.type) && d.storeTypes[r[4]] !== p.type) return false;
    if (p.store && d.stores && r[10] !== undefined && d.stores[r[10]] !== p.store) return false;
    return true;
  }
  // Sums per day for the filtered rows.
  function mtByDay(src) {
    const m = new Map();
    const pf = mtPageFilters();
    for (const r of MT.data.rows) {
      if (!mtPass(r, pf)) continue;
      if (src && src !== "all" && MT.data.sources[r[1]] !== src) continue;
      const x = m.get(r[0]) || { menuOpen: 0, cart: 0, orders: 0, clicks: 0, adOrders: 0 };
      x.menuOpen += r[5]; x.cart += r[6]; x.orders += r[7]; x.clicks += r[8]; x.adOrders += r[9];
      m.set(r[0], x);
    }
    return m;
  }
  // The metrics from sums: M2O, C2O, In-Organic and Organic.
  function mtRatios(t) {
    const div = (a, b) => (b ? (a / b) * 100 : null);
    const inOrg = div(t.adOrders, t.orders);
    return { menuOpen: t.menuOpen, orders: t.orders, m2o: div(t.orders, t.menuOpen), c2o: div(t.orders, t.cart),
             clicks: t.clicks, adOrders: t.adOrders, inOrg: inOrg, org: inOrg === null ? null : 100 - inOrg };
  }
  // One period: the total, and each day (zeros for days with no rows).
  function mtPeriod(p, byDay) {
    const days = [];
    const tot = { menuOpen: 0, cart: 0, orders: 0, clicks: 0, adOrders: 0 };
    for (let d = p.from; d <= p.to; d++) {
      const x = byDay.get(d) || { menuOpen: 0, cart: 0, orders: 0, clicks: 0, adOrders: 0 };
      for (const k of Object.keys(tot)) tot[k] += x[k];
      days.push({ day: d, ...mtRatios(x) });
    }
    return { total: mtRatios(tot), days };
  }
  function mtSummary() {
    const byDay = mtByDay();
    return { a: mtPeriod(MT.a, byDay), b: mtPeriod(MT.b, byDay) };
  }

  // ---- charts: day 1, 2 ... of each period; A solid, B dashed ----
  // Each point shows its value. The solid line's value sits above its point and the dashed line's value below it,
  // so two lines on the same axis do not cover each other. A long period is drawn wider and scrolls sideways.
  function mtChart(series, leftFmt, rightFmt, xLabels) {
    const n = Math.max(1, ...series.map(s => s.values.length));
    const W = Math.max(720, 46 * n + 70), H = 260, L = 56, R = series.some(s => s.axis === "R") ? 60 : 16, T = 22, B = 30;
    const pw = W - L - R, ph = H - T - B;
    const extent = axis => {
      const vs = series.filter(s => s.axis === axis).flatMap(s => s.values).filter(v => v !== null && isFinite(v));
      if (!vs.length) return [0, 1];
      let lo = Math.min(0, ...vs), hi = Math.max(...vs);
      if (hi === lo) hi = lo + 1;
      return [lo, hi * 1.08];
    };
    const ext = { L: extent("L"), R: extent("R") };
    const x = k => L + (n <= 1 ? 0 : (k / (n - 1)) * pw);
    const y = (axis, v) => { const [lo, hi] = ext[axis]; return T + (1 - (v - lo) / (hi - lo)) * ph; };
    const fmtFor = s => (s.name.indexOf("%") >= 0 ? v => v.toFixed(2) + "%" : v => Math.round(v).toLocaleString("en-IN"));
    const widthAttr = n > 12 ? `${W}px` : "100%";
    const xLabel = k => (xLabels && xLabels[k]) || `Day ${k + 1}`;
    let svg = `<svg viewBox="0 0 ${W} ${H}" width="${widthAttr}" role="img">`;
    for (let g = 0; g <= 4; g++) {
      const yy = T + (g / 4) * ph;
      svg += `<line x1="${L}" x2="${W - R}" y1="${yy}" y2="${yy}" stroke="var(--line)" stroke-width="1"/>`;
      const lv = ext.L[1] - (g / 4) * (ext.L[1] - ext.L[0]);
      svg += `<text x="${L - 6}" y="${yy + 4}" text-anchor="end" font-size="10" style="fill:var(--ink-2)">${leftFmt(lv)}</text>`;
      if (R > 16) {
        const rv = ext.R[1] - (g / 4) * (ext.R[1] - ext.R[0]);
        svg += `<text x="${W - R + 6}" y="${yy + 4}" font-size="10" style="fill:var(--ink-2)">${rightFmt(rv)}</text>`;
      }
    }
    const step = Math.max(1, Math.ceil(n / 12));
    for (let k = 0; k < n; k += step) {
      svg += `<text x="${x(k)}" y="${H - 10}" text-anchor="middle" font-size="10" style="fill:var(--ink-2)">${mtEsc(xLabel(k))}</text>`;
    }
    series.forEach(s => {
      const f = fmtFor(s);
      let path = "", pen = false;
      s.values.forEach((v, k) => {
        if (v === null || !isFinite(v)) { pen = false; return; }
        path += (pen ? " L" : " M") + x(k).toFixed(1) + "," + y(s.axis, v).toFixed(1);
        pen = true;
      });
      if (path) svg += `<path d="${path}" fill="none" stroke="${s.color}" stroke-width="2" ${s.dash ? 'stroke-dasharray="6 4"' : ""}/>`;
      s.values.forEach((v, k) => {
        if (v === null || !isFinite(v)) return;
        const px = x(k), py = y(s.axis, v);
        const tip = mtEsc(`${s.name}${s.dash ? " (B)" : " (A)"} · ${(s.labels && s.labels[k]) || xLabel(k)}: ${f(v)}`);
        const ly = s.dash ? py + 18 : py - 9;
        svg += `<circle cx="${px.toFixed(1)}" cy="${py.toFixed(1)}" r="3.5" fill="${s.color}"><title>${tip}</title></circle>`;
        svg += `<text x="${px.toFixed(1)}" y="${ly.toFixed(1)}" text-anchor="middle" font-size="10" font-weight="600" ` +
          `style="fill:${s.color};stroke:#0b1020;stroke-width:3px;paint-order:stroke">${mtEsc(f(v))}</text>`;
      });
    });
    return `<div style="overflow-x:auto">${svg}</svg></div>`;
  }
  function mtLegend(series) {
    return `<div class="legend">${series.map(s => `<span><i class="sw" style="--c:${s.color}"></i>${mtEsc(s.name)}${s.dash ? " (B, dashed)" : " (A)"}</span>`).join("")}</div>`;
  }

  // The three charts. Each has its own source (Swiggy + Zomato, Swiggy or Zomato) and its own metric toggles.
  const MT_CHART_DEFS = [
    { key: "m2o", title: "M2O% and C2O%", kind: "pct", list: [
        { name: "M2O%", color: COL.m2o, axis: "L", field: "m2o" },
        { name: "C2O%", color: COL.c2o, axis: "L", field: "c2o" }] },
    { key: "org", title: "Organic% and In-Organic%", kind: "pct",
      note: "In-Organic% = Ad Orders ÷ Orders. Organic% = 1 − In-Organic%.", list: [
        { name: "Organic%", color: COL.org, axis: "L", field: "org" },
        { name: "In-Organic%", color: COL.inorg, axis: "L", field: "inOrg" }] },
    { key: "clk", title: "Clicks and Ad Orders", kind: "num", list: [
        { name: "Clicks", color: COL.clicks, axis: "L", field: "clicks" },
        { name: "Ad Orders", color: COL.ad, axis: "R", field: "adOrders" }] }
  ];
  // The series of one chart: the chosen metrics, each for period A (solid) and period B (dashed), with the dates.
  function mtChartData(def) {
    const st = MT.chart[def.key];
    const byDay = mtByDay(st.src);
    const a = mtPeriod(MT.a, byDay), b = mtPeriod(MT.b, byDay);
    const series = [];
    for (const m of def.list) {
      if (!st.show[m.name]) continue;
      series.push({ name: m.name, color: m.color, axis: m.axis, dash: false,
                    values: a.days.map(d => d[m.field]), labels: a.days.map(d => mtLabel(d.day)) });
      series.push({ name: m.name, color: m.color, axis: m.axis, dash: true,
                    values: b.days.map(d => d[m.field]), labels: b.days.map(d => mtLabel(d.day)) });
    }
    return { series, a, b, st };
  }
  function mtChartCard(def) {
    const { series, a, b, st } = mtChartData(def);
    const srcOpts = [["all", "Swiggy + Zomato"], ["Swiggy", "Swiggy"], ["Zomato", "Zomato"]]
      .map(([v, l]) => `<option value="${v}"${st.src === v ? " selected" : ""}>${l}</option>`).join("");
    const chips = def.list.map(m => `<button type="button" class="mt-chip${st.show[m.name] ? " on" : ""}" ` +
      `aria-pressed="${st.show[m.name] ? "true" : "false"}" data-mtc-toggle="${def.key}" data-metric="${mtEsc(m.name)}">` +
      `${st.show[m.name] ? "✓ " : ""}${mtEsc(m.name)}</button>`).join("");
    const fmt = def.kind === "pct" ? (v => v.toFixed(1) + "%") : (v => Math.round(v).toLocaleString("en-IN"));
    const body = series.length
      ? mtChart(series, fmt, fmt, a.days.map(d => mtLabel(d.day))) + mtLegend(series)
      : `<p class="muted">Pick at least one metric above.</p>`;
    return `<section class="block"><div class="mt-head"><h2>${def.title}</h2>` +
      `<label class="mt-f"><span class="muted">Source</span><select data-mtc-src="${def.key}">${srcOpts}</select></label></div>` +
      `<div class="mt-chips">${chips}</div>` +
      `<p class="sub">A: ${mtLabel(MT.a.from)} to ${mtLabel(MT.a.to)} · B: ${mtLabel(MT.b.from)} to ${mtLabel(MT.b.to)}</p>` +
      body + (def.note ? `<p class="muted">${def.note}</p>` : "") + `</section>`;
  }
  function mtCharts(sum) {
    return MT_CHART_DEFS.map(mtChartCard).join("");
  }

  function mtSummaryTable(sum) {
    const rows = [["Menu Open", "menuOpen", "n"], ["Orders", "orders", "n"], ["M2O%", "m2o", "p"], ["C2O%", "c2o", "p"],
                  ["Clicks", "clicks", "n"], ["Ad Orders", "adOrders", "n"], ["Organic%", "org", "p"], ["In-Organic%", "inOrg", "p"]];
    const body = rows.map(([label, k, kind]) => {
      const a = sum.a.total[k], b = sum.b.total[k];
      const change = kind === "n" ? mtPctChg(mtChg(a, b)) : mtPts(a !== null && b !== null ? a - b : null);
      const fmt = kind === "n" ? mtNum : mtPct;
      return `<tr><td>${label}</td><td>${fmt(a)}</td><td>${fmt(b)}</td><td>${change}</td></tr>`;
    }).join("");
    return `<div class="scroll"><table class="tbl"><thead><tr><th>Metric</th><th>Comparison A</th><th>Comparison B</th>` +
           `<th>A vs B</th></tr></thead><tbody>${body}</tbody></table></div>`;
  }
  function mtDayTable(label, period) {
    const body = period.days.map(d => `<tr><td>${mtLabel(d.day)}</td><td>${mtNum(d.menuOpen)}</td><td>${mtPerfNum(d.orders)}</td>` +
      `<td>${mtPct(d.m2o)}</td><td>${mtPct(d.c2o)}</td><td>${mtNum(d.clicks)}</td><td>${mtNum(d.adOrders)}</td>` +
      `<td>${mtPct(d.org)}</td><td>${mtPct(d.inOrg)}</td></tr>`).join("");
    return `<section class="block"><h2>${label} by day</h2><p class="sub">${mtLabel(period.days[0].day)} to ${mtLabel(period.days[period.days.length - 1].day)}</p>` +
      `<div class="scroll"><table class="tbl"><thead><tr><th>Date</th><th>Menu Open</th><th>Orders</th><th>M2O%</th><th>C2O%</th>` +
      `<th>Clicks</th><th>Ad Orders</th><th>Organic%</th><th>In-Organic%</th></tr></thead><tbody>${body}</tbody></table></div></section>`;
  }

  function mtFilters() {
    const d = MT.data;
    const sel = (key, label, opts) => `<label class="mt-f"><span class="muted">${label}</span><select data-mt="${key}">` +
      `<option value="all"${MT[key] === "all" ? " selected" : ""}>All</option>` +
      opts.map(o => `<option value="${mtEsc(o)}"${MT[key] === o ? " selected" : ""}>${mtEsc(o)}</option>`).join("") + `</select></label>`;
    const storeNames = d.stores ? [...new Set(d.stores)].sort() : [];
    const storeSel = sel("store", "Store name", storeNames);
    const storeNote = storeNames.length ? "" : `<p class="muted">This file has no store names. Build it from the Google Sheet (build_metrics.py --gsheet).</p>`;
    const date = (key, label, v) => `<label class="mt-f"><span class="muted">${label}</span><input type="date" data-mt="${key}" value="${mtIso(v)}"></label>`;
    return `<div class="mt-bar"><div class="mt-row"><b>Comparison A</b>${date("a.from", "From", MT.a.from)}${date("a.to", "To", MT.a.to)}` +
      `<b>Comparison B</b>${date("b.from", "From", MT.b.from)}${date("b.to", "To", MT.b.to)}</div>` +
      `<div class="mt-row">${sel("source", "Source", ["Swiggy", "Zomato"])}${sel("brand", "Brand", d.brands)}` +
      `${sel("type", "Store type", d.storeTypes)}${sel("region", "Region", d.regions)}${storeSel}</div>${storeNote}` +
      `<div class="mt-row">${mtUploadControl()}</div></div>`;
  }

  // The dates the file covers, and a warning for a comparison period that is partly outside them.
  function mtCoverageNote() {
    const lo = mtDay(MT.data.dateMin), hi = mtDay(MT.data.dateMax);
    if (!isFinite(lo) || !isFinite(hi)) return "";
    const warn = [["Comparison A", MT.a], ["Comparison B", MT.b]]
      .filter(([, p]) => p.from < lo || p.to > hi)
      .map(([name, p]) => `${name} (${mtLabel(p.from)} to ${mtLabel(p.to)}) is partly outside the data in this file.`);
    return `<p class="muted">Data in this file: ${mtLabel(lo)} to ${mtLabel(hi)}.</p>` +
      warn.map(w => `<p class="muted" style="color:#E07A3F">${mtEsc(w)}</p>`).join("");
  }
  function mtSavedNote() {
    if (!MT.savedName) return "";
    return `<p class="muted">Using the file saved in this browser: ${mtEsc(MT.savedName)}. ` +
      `<button class="ghost" data-mt-forget="1">Remove this file</button></p>`;
  }
  // Performance by brand, region or area manager: Comparison A and B side by side. Click a row for its days.
  const MT_PERF_DIMS = [["brand", "Brand"], ["region", "Region"], ["am", "Area manager"]];
  const MT_ZERO = { menuOpen: 0, cart: 0, orders: 0, clicks: 0, adOrders: 0 };
  function mtAmOf(storeName) {
    const mm = typeof M !== "undefined" && M ? M : {};
    const st = (mm.stores || []).find(s => s.name === storeName);
    return st && mm.ams && mm.ams[st.a] ? mm.ams[st.a] : "Unassigned";
  }
  function mtPerfKey(dim, r) {
    const d = MT.data;
    if (dim === "brand") return d.brands[r[2]];
    if (dim === "region") return d.regions[r[3]];
    if (dim === "store") return d.stores && r[10] !== undefined ? d.stores[r[10]] : "(unknown)";
    return mtAmOf(d.stores && r[10] !== undefined ? d.stores[r[10]] : "");
  }
  // key -> the sums of the rows in [from, to] that pass the filters
  function mtPerfTotals(dim, from, to) {
    const out = new Map(), pf = mtPageFilters();
    for (const r of MT.data.rows) {
      if (r[0] < from || r[0] > to || !mtPass(r, pf)) continue;
      const k = mtPerfKey(dim, r);
      const x = out.get(k) || { ...MT_ZERO };
      x.menuOpen += r[5]; x.cart += r[6]; x.orders += r[7]; x.clicks += r[8]; x.adOrders += r[9];
      out.set(k, x);
    }
    return out;
  }
  // One entity's days in a period, with its ratios (zeros on days with no rows).
  function mtPerfDays(dim, key, period) {
    const pf = mtPageFilters(), byDay = new Map();
    for (const r of MT.data.rows) {
      if (r[0] < period.from || r[0] > period.to || !mtPass(r, pf) || mtPerfKey(dim, r) !== key) continue;
      const x = byDay.get(r[0]) || { ...MT_ZERO };
      x.menuOpen += r[5]; x.cart += r[6]; x.orders += r[7]; x.clicks += r[8]; x.adOrders += r[9];
      byDay.set(r[0], x);
    }
    const days = [];
    for (let d = period.from; d <= period.to; d++) days.push({ day: d, ...mtRatios(byDay.get(d) || MT_ZERO) });
    return days;
  }
  function mtPerfSection() {
    const p = MT.perf, A = MT.a, B = MT.b;
    const ta = mtPerfTotals(p.dim, A.from, A.to), tb = mtPerfTotals(p.dim, B.from, B.to);
    const ordA = k => (ta.get(k) || MT_ZERO).orders, ordB = k => (tb.get(k) || MT_ZERO).orders;
    const keys = [...new Set([...ta.keys(), ...tb.keys()])].filter(k => ordA(k) || ordB(k)).sort((x, y) => ordA(y) - ordA(x));
    const pc = (x, y) => (y ? (x / y - 1) * 100 : null);
    const fp = v => (v === null ? "—" : (v >= 0 ? "+" : "") + v.toFixed(1) + "%");
    const fpt = v => (v === null ? "—" : (v >= 0 ? "+" : "") + v.toFixed(2) + " pts");
    const line = (label, a, b, attr) => {
      const ra = mtRatios(a), rb = mtRatios(b);
      return `<tr><td>${attr || ""}${label}${attr ? "</button>" : ""}</td><td>${mtPerfNum(ra.orders)}</td><td>${mtPerfNum(rb.orders)}</td>` +
        `<td>${fp(pc(ra.orders, rb.orders))}</td><td>${ra.m2o === null ? "—" : ra.m2o.toFixed(2) + "%"}</td>` +
        `<td>${rb.m2o === null ? "—" : rb.m2o.toFixed(2) + "%"}</td>` +
        `<td>${fpt(ra.m2o === null || rb.m2o === null ? null : ra.m2o - rb.m2o)}</td></tr>`;
    };
    const total = [...ta.values()].reduce((s, x) => { for (const k of Object.keys(MT_ZERO)) s[k] += x[k]; return s; }, { ...MT_ZERO });
    const totalB = [...tb.values()].reduce((s, x) => { for (const k of Object.keys(MT_ZERO)) s[k] += x[k]; return s; }, { ...MT_ZERO });
    let body = line("All", total, totalB, "");
    for (const k of keys) {
      const open = p.open.has(k);
      body += line(`${open ? "▾" : "▸"} ${mtEsc(k)}`, ta.get(k) || MT_ZERO, tb.get(k) || MT_ZERO,
                   `<button data-mtp-key="${mtEsc(k)}">`);
      if (open) {
        for (const d of mtPerfDays(p.dim, k, A)) body += `<tr class="ins-st"><td>A · ${mtLabel(d.day)}</td><td>${mtPerfNum(d.orders)}</td><td></td><td></td><td>${d.m2o === null ? "—" : d.m2o.toFixed(2) + "%"}</td><td></td><td></td></tr>`;
        for (const d of mtPerfDays(p.dim, k, B)) body += `<tr class="ins-st"><td>B · ${mtLabel(d.day)}</td><td></td><td>${mtPerfNum(d.orders)}</td><td></td><td></td><td>${d.m2o === null ? "—" : d.m2o.toFixed(2) + "%"}</td><td></td></tr>`;
      }
    }
    const label = (MT_PERF_DIMS.find(x => x[0] === p.dim) || MT_PERF_DIMS[0])[1];
    const chips = MT_PERF_DIMS.map(([v, l]) => `<button type="button" class="mt-chip${p.dim === v ? " on" : ""}" data-mtp-dim="${v}">${l}</button>`).join("");
    return `<section class="block"><h2>Performance by ${label.toLowerCase()}</h2>` +
      `<p class="sub">Click a ${label.toLowerCase()} to see its days. Comparison A and B, with the same filters.</p>` +
      `<div class="mt-chips">${chips}</div><div class="scroll"><table class="tbl"><thead><tr><th>${label}</th>` +
      `<th>Orders A</th><th>Orders B</th><th>Orders change</th><th>M2O% A</th><th>M2O% B</th><th>M2O change</th></tr></thead>` +
      `<tbody>${body}</tbody></table></div></section>`;
  }
  function mtPerfNum(v) { return v === null || v === undefined ? "—" : Math.round(v).toLocaleString("en-IN"); }

  // ---- Top and bottom stores, and the growth tables for a chosen date ----
  const MT_RANK_MIN = 10;
  const MT_DAY_MS = 86400000;
  const mtgDayOf = (y, m, d) => Math.round((Date.UTC(y, m - 1, d) - MT_EPOCH) / MT_DAY_MS);
  const mtgYmd = day => { const x = new Date(MT_EPOCH + day * MT_DAY_MS); return [x.getUTCFullYear(), x.getUTCMonth() + 1, x.getUTCDate()]; };
  const mtgIso = day => { const [y, m, d] = mtgYmd(day); return `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`; };
  const mtgMonthStart = day => { const [y, m] = mtgYmd(day); return mtgDayOf(y, m, 1); };
  // The same day of the previous month (the last day of that month, when it is shorter).
  const mtgPrevMonthSameDay = day => {
    const [y, m, d] = mtgYmd(day);
    const py = m === 1 ? y - 1 : y, pm = m === 1 ? 12 : m - 1;
    return mtgDayOf(py, pm, Math.min(d, new Date(Date.UTC(py, pm, 0)).getUTCDate()));
  };
  const mtgDataStart = () => MT.data.rows.reduce((mn, r) => Math.min(mn, r[0]), Infinity);
  const mtgDataEnd = () => MT.data.rows.reduce((mx, r) => Math.max(mx, r[0]), -Infinity);
  const mtgPct = v => (v >= 0 ? "+" : "") + v.toFixed(1) + "%";

  // Top 5 and bottom 5 stores by orders, Comparison A against B. Stores under MT_RANK_MIN orders in either period are left out.
  function mtRankSection() {
    const ta = mtPerfTotals("store", MT.a.from, MT.a.to), tb = mtPerfTotals("store", MT.b.from, MT.b.to);
    const rows = [...new Set([...ta.keys(), ...tb.keys()])]
      .map(k => ({ k, a: ta.get(k) || MT_ZERO, b: tb.get(k) || MT_ZERO }))
      .filter(x => x.a.orders >= MT_RANK_MIN && x.b.orders >= MT_RANK_MIN)
      .map(x => ({ ...x, change: (x.a.orders / x.b.orders - 1) * 100 }));
    const top = [...rows].sort((x, y) => y.change - x.change).slice(0, 5);
    const bottom = [...rows].sort((x, y) => x.change - y.change).slice(0, 5);
    const line = x => {
      const ra = mtRatios(x.a), rb = mtRatios(x.b);
      return `<tr><td>${mtEsc(x.k)}</td><td>${mtPerfNum(ra.orders)}</td><td>${mtPerfNum(rb.orders)}</td><td>${mtgPct(x.change)}</td>` +
        `<td>${ra.m2o === null ? "—" : ra.m2o.toFixed(2) + "%"}</td><td>${rb.m2o === null ? "—" : rb.m2o.toFixed(2) + "%"}</td></tr>`;
    };
    const table = (title, list) => `<h3>${title}</h3><div class="scroll"><table class="tbl"><thead><tr><th>Store</th>` +
      `<th>Orders A</th><th>Orders B</th><th>Orders change</th><th>M2O% A</th><th>M2O% B</th></tr></thead><tbody>` +
      (list.length ? list.map(line).join("") : `<tr><td colspan="6" class="muted">No store with ${MT_RANK_MIN}+ orders in both periods.</td></tr>`) +
      `</tbody></table></div>`;
    return `<section class="block"><h2>Top and bottom stores</h2><p class="sub">By orders, Comparison A against B. ` +
      `Stores with fewer than ${MT_RANK_MIN} orders in either period are left out.</p>${table("Top 5", top)}${table("Bottom 5", bottom)}</section>`;
  }

  // Growth for the chosen day: against the same day last week (WoW), two weeks ago (L2W) and last month (MoM); and the
  // month to date (MTD) against the same days of last month. Orders, by brand, region and area manager.
  function mtgSection() {
    const min = mtgDataStart(), end = MT.gr.day != null ? MT.gr.day : mtgDataEnd();
    const pm = mtgPrevMonthSameDay(end), ms = mtgMonthStart(end), pms = mtgMonthStart(pm);
    const W = { day: [end, end], wow: [end - 7, end - 7], l2w: [end - 14, end - 14], mom: [pm, pm], mtd: [ms, end], mtdPrev: [pms, pm] };
    const valOf = (map, k, w) => (w[0] < min ? null : (map.get(k) || MT_ZERO).orders);
    const g = (cur, prev) => (cur === null || prev === null || !prev ? "—" : mtgPct((cur / prev - 1) * 100));
    const block = (dim, label) => {
      const maps = {};
      for (const k of Object.keys(W)) maps[k] = mtPerfTotals(dim, W[k][0], W[k][1]);
      const keys = [...new Set([...maps.day.keys(), ...maps.mtd.keys()])]
        .sort((x, y) => (maps.mtd.get(y) || MT_ZERO).orders - (maps.mtd.get(x) || MT_ZERO).orders);
      const rows = keys.map(k => {
        const day = valOf(maps.day, k, W.day), wow = valOf(maps.wow, k, W.wow), l2w = valOf(maps.l2w, k, W.l2w);
        const mom = valOf(maps.mom, k, W.mom), mtd = valOf(maps.mtd, k, W.mtd), mtdP = valOf(maps.mtdPrev, k, W.mtdPrev);
        return `<tr><td>${mtEsc(k)}</td><td>${mtPerfNum(day)}</td><td>${g(day, wow)}</td><td>${g(day, l2w)}</td>` +
          `<td>${g(day, mom)}</td><td>${mtPerfNum(mtd)}</td><td>${g(mtd, mtdP)}</td></tr>`;
      }).join("");
      return `<h3>${label}</h3><div class="scroll"><table class="tbl"><thead><tr><th>${label}</th><th>Day orders</th>` +
        `<th>WoW %</th><th>L2W %</th><th>MoM %</th><th>MTD orders</th><th>MTD %</th></tr></thead><tbody>` +
        (rows || `<tr><td colspan="7" class="muted">No orders.</td></tr>`) + `</tbody></table></div>`;
    };
    const note = pm < min ? `<p class="muted">The data starts ${mtLabel(min)}, so comparisons with earlier days show a dash.</p>` : "";
    return `<section class="block"><h2>Growth</h2><p class="sub">Pick the day. It is compared with the same day last week (WoW), ` +
      `two weeks ago (L2W) and last month (MoM); the month to date (MTD) with the same days of last month.</p>` +
      `<div class="mt-bar"><label class="mt-f"><span class="muted">Date</span><input type="date" data-mtg-day="1" value="${mtgIso(end)}"></label></div>` +
      `<p class="muted">Day: ${mtLabel(end)} · MTD: ${mtLabel(ms)} to ${mtLabel(end)}</p>${note}` +
      block("brand", "Brand") + block("region", "Region") + block("am", "Area manager") + `</section>`;
  }

  function metricsView() {
    setPeriodBar("metrics", null, null);
    if (!MT.data) {
      if (!MT.error && !MT.loading) mtLoad();
      const note = MT.error ? `<p class="muted">${mtEsc(MT.error)}</p>` : `<p class="muted">Loading the metrics.</p>`;
      return `<section class="block"><h2>Metrics</h2>${note}<div class="mt-bar">${mtUploadControl()}</div></section>`;
    }
    const sum = mtSummary();
    return `<section class="block"><h2>Metrics: Swiggy and Zomato</h2><p class="sub">Funnel and ads by day. Pick the two comparison periods and the filters. The Sales tab's brand, region, store type, source and store filters apply too.</p>${mtSavedNote()}${mtFilters()}${mtCoverageNote()}</section>` +
      `<section class="block"><h2>Comparison A and B</h2>${mtSummaryTable(sum)}</section>` +
      mtPerfSection() +
      mtRankSection() +
      mtgSection() +
      mtCharts(sum) +
      mtDayTable("Comparison A", sum.a) + mtDayTable("Comparison B", sum.b);
  }

  // The metric buttons of a chart: a click shows the metric, or hides it.
  document.addEventListener("click", e => {
    const tg = e.target && e.target.closest ? e.target.closest("[data-mtc-toggle]") : null;
    if (!tg) return;
    const st = MT.chart[tg.dataset.mtcToggle];
    const m = tg.dataset.metric;
    st.show[m] = !st.show[m];
    render();
  });

  // The performance table: a click on a dimension, or on a row (its days).
  document.addEventListener("click", e => {
    const t = e.target;
    const d = t && t.closest ? t.closest("[data-mtp-dim]") : null;
    if (d) { MT.perf.dim = d.dataset.mtpDim; MT.perf.open.clear(); render(); return; }
    const k = t && t.closest ? t.closest("[data-mtp-key]") : null;
    if (k) {
      const key = k.dataset.mtpKey;
      if (MT.perf.open.has(key)) MT.perf.open.delete(key); else MT.perf.open.add(key);
      render();
    }
  });

  // Remove the saved file: the page then tries the server again.
  document.addEventListener("click", e => {
    if (!(e.target && e.target.closest && e.target.closest("[data-mt-forget]"))) return;
    mtForget();
    MT.data = null;
    MT.savedName = null;
    MT.savedAt = null;
    MT.error = null;
    render();
  });

  // Filters and dates change the page.
  document.addEventListener("change", e => {
    const el = e.target && e.target.dataset ? e.target : null;
    if (el && el.dataset.mtFile) { mtPickFile(el.files && el.files[0]); return; }
    if (el && el.dataset.mtgDay) {
      const v = String(el.value || "").split("-").map(Number);
      if (v.length === 3 && v[0]) { MT.gr.day = mtgDayOf(v[0], v[1], v[2]); render(); }
      return;
    }
    if (el && el.dataset.mtcSrc) { MT.chart[el.dataset.mtcSrc].src = el.value; render(); return; }
    if (el && el.dataset.mtcShow) { MT.chart[el.dataset.mtcShow].show[el.dataset.metric] = el.checked; render(); return; }
    if (!el || !el.dataset.mt) return;
    const k = el.dataset.mt;
    if (k.indexOf(".") > 0) {
      const [p, end] = k.split(".");
      const day = mtDay(el.value);
      if (!isFinite(day)) return;
      MT[p][end === "from" ? "from" : "to"] = day;
      if (MT[p].from > MT[p].to) MT[p].from = MT[p].to;
    } else {
      MT[k] = el.value;
    }
    render();
  });


  (function mtStyle() {
    if (document.getElementById("mt-style")) return;
    const st = document.createElement("style");
    st.id = "mt-style";
    st.textContent = `.mt-bar{display:flex;flex-direction:column;gap:10px;margin:8px 0}
.mt-row{display:flex;flex-wrap:wrap;gap:12px;align-items:flex-end}
.mt-f{display:flex;flex-direction:column;gap:4px;font-size:13px}
.mt-f select,.mt-f input{padding:6px 8px;border-radius:8px;border:1px solid var(--line);background:var(--surface);color:var(--ink)}
.mt-head{display:flex;justify-content:space-between;align-items:flex-end;flex-wrap:wrap;gap:10px}
.mt-chips{display:flex;gap:16px;flex-wrap:wrap;font-size:13px;margin:6px 0}
.mt-chips label{display:flex;gap:6px;align-items:center;cursor:pointer}
.mt-chip{padding:5px 12px;border-radius:999px;border:1px solid var(--line);background:transparent;color:var(--ink-2);cursor:pointer;font-size:13px}
.mt-chip.on{background:var(--surface);color:var(--ink);border-color:var(--ink-2)}`;
    document.head.appendChild(st);
  })();

  const salesPeriodViewMt = salesPeriodView;
  salesPeriodView = function (kind) {
    if (kind !== "metrics") return salesPeriodViewMt(kind);
    return metricsView();
  };
  if (typeof SUBS !== "undefined" && SUBS.sales && !SUBS.sales.some(x => x[0] === "metrics")) {
    SUBS.sales.push(["metrics", "Metrics"]);
  }

  window.FB_MT = { MT, mtSummary, mtByDay, mtRatios, mtPass, mtLoad, mtUse, mtParse, mtErrorText, mtPageFilters, mtPickFile,
                   mtRemember, mtForget, mtFromStorage, mtChart, mtPerfSection, mtPerfTotals, mtPerfDays,
                       mtRankSection, mtgSection, mtgDayOf, mtgDataEnd, mtgDataStart, mtgPrevMonthSameDay, mtgMonthStart };
})();
