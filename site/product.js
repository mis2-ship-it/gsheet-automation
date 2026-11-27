/* Product Level (Sales tab): the closed item lines of the last 62 days, by store, brand, region, store type, source,
   category and item. The data is data/product_items.csv.gz (the days, cut to the login's stores).
   Measures: Net sales, Orders, Dis % (discount / gross), Discount, AOV (net / orders). Orders counts the distinct orders
   with the product. Each product opens to its categories. The columns are the chosen dimension: Brand, Source, Region or Store. */
(function () {
  "use strict";
  const PL_DAY = 86400000, PL_EPOCH = Date.UTC(2025, 0, 1), PL_MAX_COLS = 12;
  const PL = { rows: null, loading: false, error: null, brand: "all", region: "all", type: "all", store: "all", am: "all",
               metric: "net", pivot: "brand", from: null, to: null, open: new Set() };
  const PL_METRICS = [["net", "Net sales"], ["orders", "Orders"], ["dispct", "Dis %"], ["discount", "Discount"], ["aov", "AOV"]];
  const PL_PIVOTS = [["brand", "Brand"], ["src", "Source"], ["region", "Region"], ["branch", "Store"]];
  const plEsc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#39;" }[c]));
  const plDayOf = iso => { const p = String(iso || "").split("-").map(Number); return Math.round((Date.UTC(p[0], p[1] - 1, p[2]) - PL_EPOCH) / PL_DAY); };
  const plIsoOf = day => new Date(PL_EPOCH + day * PL_DAY).toISOString().slice(0, 10);
  const plZero = () => ({ orders: 0, qty: 0, gross: 0, disc: 0, net: 0 });
  const plAdd = (x, r) => { x.orders += r.orders; x.qty += r.qty; x.gross += r.gross; x.disc += r.disc; x.net += r.net; return x; };
  const plNum = v => Math.round(v).toLocaleString("en-IN");

  // CSV: a quoted field may hold commas and line breaks.
  function plParseCsv(text) {
    const rows = []; let row = [], field = "", q = false;
    for (let i = 0; i < text.length; i++) {
      const c = text[i];
      if (q) { if (c === "\"") { if (text[i + 1] === "\"") { field += "\""; i++; } else q = false; } else field += c; }
      else if (c === "\"") q = true;
      else if (c === ",") { row.push(field); field = ""; }
      else if (c === "\n" || c === "\r") { if (c === "\r" && text[i + 1] === "\n") i++; row.push(field); rows.push(row); row = []; field = ""; }
      else field += c;
    }
    if (field.length || row.length) { row.push(field); rows.push(row); }
    const head = (rows.shift() || []).map(h => h.trim());
    return rows.filter(r => r.length > 1).map(r => Object.fromEntries(head.map((h, i) => [h, r[i] === undefined ? "" : r[i]])));
  }
  // The area manager of a store, from the page's store list.
  function plAmOfMaker() {
    const byName = new Map(((typeof M !== "undefined" && M && M.stores) || []).map(s => [s.name, s]));
    return name => {
      const st = byName.get(name);
      return st && M.ams && M.ams[st.a] !== undefined ? M.ams[st.a] : "Unassigned";
    };
  }
  // The file's rows as the tab uses them (Date becomes a day number; dims as names; the area manager from the store).
  function plRowsFrom(objs, amOf) {
    const am = amOf || plAmOfMaker();
    return objs.map(o => ({ day: plDayOf(o.Date), branch: o.Branch, brand: o.Brand, region: o.Region, type: o["Store Type"],
      src: o.Source, cat: o.Category, item: o.Item, am: am(o.Branch), orders: +o.Orders || 0, qty: +o.Qty || 0,
      gross: +o.Gross || 0, disc: +o.Discount || 0, net: +o.Net || 0 }))
      .filter(r => !isNaN(r.day));
  }
  function plPass(r, s, from, to) {
    if (r.day < from || r.day > to) return false;
    if (s.brand !== "all" && r.brand !== s.brand) return false;
    if (s.region !== "all" && r.region !== s.region) return false;
    if (s.type !== "all" && r.type !== s.type) return false;
    if (s.store !== "all" && r.branch !== s.store) return false;
    if (s.am !== "all" && r.am !== s.am) return false;
    return true;
  }
  // The measure of a sum: Net sales, Orders, Dis % (discount over gross), Discount, AOV (net over orders).
  function plValue(x, metric) {
    if (!x) return null;
    if (metric === "net") return x.net;
    if (metric === "orders") return x.orders;
    if (metric === "dispct") return x.gross ? (x.disc / x.gross) * 100 : null;
    if (metric === "discount") return x.disc;
    if (metric === "aov") return x.orders ? x.net / x.orders : null;
    return null;
  }
  // Growth against the previous period of the same length: a percentage, or points for Dis %.
  function plGrowthText(metric, cur, prev) {
    if (cur === null || prev === null || prev === undefined) return "—";
    if (metric === "dispct") return (cur - prev >= 0 ? "+" : "") + (cur - prev).toFixed(2) + " pts";
    if (!prev) return "—";
    const g = (cur / prev - 1) * 100;
    return (g >= 0 ? "+" : "") + g.toFixed(1) + "%";
  }
  const plFmt = (metric, v) => {
    if (v === null || v === undefined || isNaN(v)) return "—";
    if (metric === "dispct") return v.toFixed(2) + "%";
    if (metric === "orders") return plNum(v);
    return "₹" + plNum(v);
  };
  const pivotValue = pivot => r => (pivot === "brand" ? r.brand : pivot === "src" ? r.src : pivot === "region" ? r.region : r.branch);

  // The table: products (sorted by the measure), each with its categories and the columns of the pivot.
  function plModel(rows, s) {
    const len = s.to - s.from + 1, pfrom = s.from - len, pto = s.from - 1;
    const pv = pivotValue(s.pivot);
    const total = plZero(), prev = plZero(), colTot = new Map(), byItem = new Map(), prevItem = new Map();
    for (const r of rows) {
      if (plPass(r, s, pfrom, pto)) { plAdd(prev, r); prevItem.set(r.item, plAdd(prevItem.get(r.item) || plZero(), r)); }
      if (!plPass(r, s, s.from, s.to)) continue;
      plAdd(total, r);
      const v = pv(r);
      colTot.set(v, plAdd(colTot.get(v) || plZero(), r));
      let p = byItem.get(r.item);
      if (!p) { p = { name: r.item, x: plZero(), cols: new Map(), cats: new Map() }; byItem.set(r.item, p); }
      plAdd(p.x, r);
      p.cols.set(v, plAdd(p.cols.get(v) || plZero(), r));
      let c = p.cats.get(r.cat);
      if (!c) { c = { name: r.cat, x: plZero(), cols: new Map() }; p.cats.set(r.cat, c); }
      plAdd(c.x, r);
      c.cols.set(v, plAdd(c.cols.get(v) || plZero(), r));
    }
    const ordered = [...colTot.entries()].sort((a, b) => b[1].net - a[1].net).map(e => e[0]);
    const shown = ordered.slice(0, PL_MAX_COLS);
    const colOf = v => (shown.includes(v) ? v : "Others");
    const columns = ordered.length > PL_MAX_COLS ? [...shown, "Others"] : shown;
    const fold = m => { const out = new Map(); for (const [v, x] of m) { const k = colOf(v); out.set(k, plAdd(out.get(k) || plZero(), x)); } return out; };
    const products = [...byItem.values()].map(p => ({
      name: p.name, x: p.x, prev: prevItem.get(p.name) || plZero(), cols: fold(p.cols),
      cats: [...p.cats.values()].map(c => ({ name: c.name, x: c.x, cols: fold(c.cols) })).sort((a, b) => b.x.net - a.x.net)
    })).sort((a, b) => (plValue(b.x, s.metric) ?? -Infinity) - (plValue(a.x, s.metric) ?? -Infinity));
    return { columns, products, total, prev, colTotals: fold(colTot) };
  }

  function plRender(s) {
    if (PL.error) return `<section class="block"><h2>Product Level</h2><p class="muted">${plEsc(PL.error)}</p></section>`;
    if (!PL.rows) return `<section class="block"><h2>Product Level</h2><p class="muted">Loading the product sales.</p></section>`;
    const pick = (key, label, list) => `<label class="mt-f"><span class="muted">${label}</span><select data-pl="${key}">` +
      `<option value="all">All</option>${list.map(v => `<option value="${plEsc(v)}"${s[key] === v ? " selected" : ""}>${plEsc(v)}</option>`).join("")}</select></label>`;
    const uniq = k => [...new Set(PL.rows.map(r => r[k]))].filter(Boolean).sort();
    const bar = [pick("brand", "Brand", uniq("brand")), pick("region", "Region", uniq("region")), pick("type", "Store type", uniq("type")),
                 pick("store", "Store name", uniq("branch")), pick("am", "Area manager", uniq("am"))].join("");
    const dates = `<label class="mt-f"><span class="muted">From</span><input type="date" data-pl="from" value="${plIsoOf(s.from)}"></label>` +
                  `<label class="mt-f"><span class="muted">To</span><input type="date" data-pl="to" value="${plIsoOf(s.to)}"></label>`;
    const metricSel = `<label class="mt-f"><span class="muted">Measure</span><select data-pl="metric">` +
      PL_METRICS.map(([v, l]) => `<option value="${v}"${s.metric === v ? " selected" : ""}>${l}</option>`).join("") + `</select></label>`;
    const pivotSel = `<label class="mt-f"><span class="muted">Columns</span><select data-pl="pivot">` +
      PL_PIVOTS.map(([v, l]) => `<option value="${v}"${s.pivot === v ? " selected" : ""}>${l}</option>`).join("") + `</select></label>`;
    const m = plModel(PL.rows, s);
    const val = x => plValue(x, s.metric);
    const cell = (x, c) => `<td>${plFmt(s.metric, val((c && x.cols.get(c)) || plZero()))}</td>`;
    const head = `<tr><th>Product</th><th>${PL_METRICS.find(x => x[0] === s.metric)[1]}</th><th>Growth %</th>` +
                 m.columns.map(c => `<th>${plEsc(c)}</th>`).join("") + `</tr>`;
    const totalRow = `<tr class="tot"><td>All products</td><td>${plFmt(s.metric, val(m.total))}</td>` +
                     `<td>${plGrowthText(s.metric, val(m.total), val(m.prev))}</td>` + m.columns.map(c => `<td>${plFmt(s.metric, val(m.colTotals.get(c) || plZero()))}</td>`).join("") + `</tr>`;
    let body = "";
    for (const p of m.products) {
      const open = PL.open.has(p.name);
      body += `<tr><td><button data-pl-open="${plEsc(p.name)}">${open ? "▾" : "▸"} ${plEsc(p.name)}</button></td>` +
              `<td>${plFmt(s.metric, val(p.x))}</td><td>${plGrowthText(s.metric, val(p.x), val(p.prev))}</td>` +
              m.columns.map(c => cell(p, c)).join("") + `</tr>`;
      if (open) for (const c of p.cats) {
        body += `<tr class="ins-st"><td>${plEsc(c.name || "(no category)")}</td><td>${plFmt(s.metric, val(c.x))}</td><td></td>` +
                m.columns.map(k => cell(c, k)).join("") + `</tr>`;
      }
    }
    return `<section class="block"><h2>Product Level</h2>` +
      `<p class="sub">Closed sales of the last 62 days, by product. Click a product for its categories. Growth is against the previous period of the same length.</p>` +
      `<div class="mt-bar">${bar}</div><div class="mt-bar">${dates}${metricSel}${pivotSel}</div>` +
      `<div class="scroll"><table class="tbl"><thead>${head}</thead><tbody>${totalRow}${body || `<tr><td colspan="${m.columns.length + 3}" class="muted">No products for these filters.</td></tr>`}</tbody></table></div></section>`;
  }

  async function plFetch(url) {
    const res = await fetch(url, { credentials: "same-origin", cache: "no-store" });
    if (!res.ok) throw new Error(`The product sales could not be read (${res.status}). Run the Product pull and the dashboard build.`);
    return await new Response(res.body.pipeThrough(new DecompressionStream("gzip"))).text();
  }
  async function plLoad() {
    if (PL.rows || PL.loading) return;
    PL.loading = true;
    try {
      PL.rows = plRowsFrom(plParseCsv(await plFetch("data/product_items.csv.gz")));
      const last = PL.rows.reduce((mx, r) => Math.max(mx, r.day), -Infinity);
      if (PL.to === null) PL.to = last;
      if (PL.from === null) PL.from = last - 6;
    } catch (err) { PL.error = err.message; }
    PL.loading = false;
    render();
  }
  function plView() {
    setPeriodBar("product", null, null);
    if (!PL.rows && !PL.error) plLoad();
    return plRender(PL);
  }

  // Filters: each control has data-pl; a product opens with data-pl-open.
  document.addEventListener("change", e => {
    const el = e.target, k = el && el.dataset ? el.dataset.pl : null;
    if (!k) return;
    if (k === "from" || k === "to") { const d = plDayOf(el.value); if (!isNaN(d)) PL[k] = d; }
    else PL[k] = el.value;
    render();
  });
  document.addEventListener("click", e => {
    const b = e.target && e.target.closest ? e.target.closest("[data-pl-open]") : null;
    if (!b) return;
    const k = b.dataset.plOpen;
    if (PL.open.has(k)) PL.open.delete(k); else PL.open.add(k);
    render();
  });

  // The Product Level tab, added to the Sales tabs.
  if (typeof SUBS !== "undefined" && SUBS.sales && !SUBS.sales.some(x => x[0] === "product")) SUBS.sales.push(["product", "Product Level"]);
  const salesPeriodViewPl = salesPeriodView;
  salesPeriodView = function (kind) { return kind === "product" ? plView() : salesPeriodViewPl(kind); };
  window.FB_PRODUCT = { PL, PL_MAX_COLS, plParseCsv, plRowsFrom, plModel, plPass, plValue, plGrowthText, plRender, plLoad, plDayOf, plIsoOf };
})();
