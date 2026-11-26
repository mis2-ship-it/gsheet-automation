/* ============ Sales > Metrics: Swiggy and Zomato funnel and ads ============
   Comparison A and comparison B (each a from and to date) at the top, then the summary, three line charts and a
   day-by-day table for each comparison. Filters: brand, store type, region and source (All, Swiggy, Zomato).
   Calculations (ratios from the sums, never averaged):
     M2O% = Orders / Menu Open,  C2O% = Orders / Cart,  In-Organic% = Ad Orders / Orders,  Organic% = 1 - In-Organic%.
   Data: data/metrics.json.gz, built by build_metrics.py from the Metrics sheet. Loaded after productmix.js.
   It wraps salesPeriodView() for the Metrics tab only. */
(function () {
  const MT = { data: null, loading: false, error: null, a: null, b: null, brand: "all", type: "all", region: "all", source: "all" };
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
    if (p.source && d.sources.includes(p.source) && d.sources[r[1]] !== p.source) return false;
    if (p.brand && d.brands.includes(p.brand) && d.brands[r[2]] !== p.brand) return false;
    if (p.region && d.regions.includes(p.region) && d.regions[r[3]] !== p.region) return false;
    if (p.type && d.storeTypes.includes(p.type) && d.storeTypes[r[4]] !== p.type) return false;
    if (p.store && d.stores && r[10] !== undefined && d.stores[r[10]] !== p.store) return false;
    return true;
  }
  // Sums per day for the filtered rows.
  function mtByDay() {
    const m = new Map();
    const pf = mtPageFilters();
    for (const r of MT.data.rows) {
      if (!mtPass(r, pf)) continue;
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
  function mtChart(series, leftFmt, rightFmt) {
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
      svg += `<text x="${x(k)}" y="${H - 10}" text-anchor="middle" font-size="10" style="fill:var(--ink-2)">Day ${k + 1}</text>`;
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
        const tip = mtEsc(`${s.name}${s.dash ? " (B)" : " (A)"} · Day ${k + 1}: ${f(v)}`);
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

  function mtCharts(sum) {
    const pick = (p, k) => p.days.map(d => d[k]);
    const s1 = [
      { name: "M2O%", color: COL.m2o, axis: "L", values: pick(sum.a, "m2o"), dash: false },
      { name: "C2O%", color: COL.c2o, axis: "L", values: pick(sum.a, "c2o"), dash: false },
      { name: "M2O%", color: COL.m2o, axis: "L", values: pick(sum.b, "m2o"), dash: true },
      { name: "C2O%", color: COL.c2o, axis: "L", values: pick(sum.b, "c2o"), dash: true }];
    const s2 = [
      { name: "Organic%", color: COL.org, axis: "L", values: pick(sum.a, "org"), dash: false },
      { name: "In-Organic%", color: COL.inorg, axis: "L", values: pick(sum.a, "inOrg"), dash: false },
      { name: "Organic%", color: COL.org, axis: "L", values: pick(sum.b, "org"), dash: true },
      { name: "In-Organic%", color: COL.inorg, axis: "L", values: pick(sum.b, "inOrg"), dash: true }];
    const s3 = [
      { name: "Clicks (left)", color: COL.clicks, axis: "L", values: pick(sum.a, "clicks"), dash: false },
      { name: "Ad Orders (right)", color: COL.ad, axis: "R", values: pick(sum.a, "adOrders"), dash: false },
      { name: "Clicks (left)", color: COL.clicks, axis: "L", values: pick(sum.b, "clicks"), dash: true },
      { name: "Ad Orders (right)", color: COL.ad, axis: "R", values: pick(sum.b, "adOrders"), dash: true }];
    const pctAxis = v => v.toFixed(1) + "%";
    const numAxis = v => Math.round(v).toLocaleString("en-IN");
    return `<section class="block"><h2>M2O% and C2O%</h2>${mtChart(s1, pctAxis, pctAxis)}${mtLegend(s1)}</section>` +
           `<section class="block"><h2>Organic% and In-Organic%</h2>${mtChart(s2, pctAxis, pctAxis)}${mtLegend(s2)}` +
           `<p class="muted">In-Organic% = Ad Orders ÷ Orders. Organic% = 1 − In-Organic%.</p></section>` +
           `<section class="block"><h2>Clicks and Ad Orders</h2>${mtChart(s3, numAxis, numAxis)}${mtLegend(s3)}</section>`;
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
    const body = period.days.map(d => `<tr><td>${mtLabel(d.day)}</td><td>${mtNum(d.menuOpen)}</td><td>${mtNum(d.orders)}</td>` +
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
    const date = (key, label, v) => `<label class="mt-f"><span class="muted">${label}</span><input type="date" data-mt="${key}" value="${mtIso(v)}"></label>`;
    return `<div class="mt-bar"><div class="mt-row"><b>Comparison A</b>${date("a.from", "From", MT.a.from)}${date("a.to", "To", MT.a.to)}` +
      `<b>Comparison B</b>${date("b.from", "From", MT.b.from)}${date("b.to", "To", MT.b.to)}</div>` +
      `<div class="mt-row">${sel("source", "Source", ["Swiggy", "Zomato"])}${sel("brand", "Brand", d.brands)}` +
      `${sel("type", "Store type", d.storeTypes)}${sel("region", "Region", d.regions)}</div>` +
      `<div class="mt-row">${mtUploadControl()}</div></div>`;
  }

  function mtSavedNote() {
    if (!MT.savedName) return "";
    return `<p class="muted">Using the file saved in this browser: ${mtEsc(MT.savedName)}. ` +
      `<button class="ghost" data-mt-forget="1">Remove this file</button></p>`;
  }
  function metricsView() {
    setPeriodBar("metrics", null, null);
    if (!MT.data) {
      if (!MT.error && !MT.loading) mtLoad();
      const note = MT.error ? `<p class="muted">${mtEsc(MT.error)}</p>` : `<p class="muted">Loading the metrics.</p>`;
      return `<section class="block"><h2>Metrics</h2>${note}<div class="mt-bar">${mtUploadControl()}</div></section>`;
    }
    const sum = mtSummary();
    return `<section class="block"><h2>Metrics: Swiggy and Zomato</h2><p class="sub">Funnel and ads by day. Pick the two comparison periods and the filters. The Sales tab's brand, region, store type, source and store filters apply too.</p>${mtSavedNote()}${mtFilters()}</section>` +
      `<section class="block"><h2>Comparison A and B</h2>${mtSummaryTable(sum)}</section>` +
      mtCharts(sum) +
      mtDayTable("Comparison A", sum.a) + mtDayTable("Comparison B", sum.b);
  }

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
.mt-f select,.mt-f input{padding:6px 8px;border-radius:8px;border:1px solid var(--line);background:var(--surface);color:var(--ink)}`;
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
                   mtRemember, mtForget, mtFromStorage, mtChart };
})();
