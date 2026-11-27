/* Cancellation (Sales tab): the day chosen on the Daily tab. Closed orders come from the store sales files; cancelled
   orders come from data/cancel_orders.csv.gz (one row per cancelled order). Columns:
   Overall orders (closed + cancelled) | Cancel orders (RDC: cancelled with a listed reason) | Other orders (cancelled with no
   listed reason) | Cancel orders % (RDC divided by overall). The page's filters apply to both kinds of order. */
(function () {
  "use strict";
  const CX = { dim: "brand", chan: "all", open: new Set(), rows: null, loading: false, error: null,
               items: null, itemsError: null, openCat: new Set() };
  const CX_EPOCH = Date.UTC(2025, 0, 1);
  const CX_DIMS = [["brand", "Brand"], ["region", "Region"], ["source", "Source"], ["am", "Area manager"]];
  const cxEsc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#39;" }[c]));
  const cxNorm = s => String(s == null ? "" : s).trim().toLowerCase();
  const cxIso = key => new Date(CX_EPOCH + key * 86400000).toISOString().slice(0, 10);
  const cxNum = v => Math.round(v).toLocaleString("en-IN");
  const cxIdx = (list, name) => { const w = cxNorm(name); return w && list ? list.findIndex(x => cxNorm(x) === w) : -1; };
  const cxName = (list, i, fallback) => (list && i >= 0 && list[i] !== undefined ? list[i] : fallback);
  const cxOverall = g => g.closed + g.rdc + g.other;
  const cxPct = g => (cxOverall(g) ? (g.rdc / cxOverall(g)) * 100 : null);

  // CSV: a quoted field may hold commas and line breaks.
  function cxParseCsv(text) {
    const rows = []; let row = [], field = "", q = false;
    for (let i = 0; i < text.length; i++) {
      const c = text[i];
      if (q) {
        if (c === "\"") { if (text[i + 1] === "\"") { field += "\""; i++; } else q = false; } else field += c;
      } else if (c === "\"") q = true;
      else if (c === ",") { row.push(field); field = ""; }
      else if (c === "\n" || c === "\r") {
        if (c === "\r" && text[i + 1] === "\n") i++;
        row.push(field); rows.push(row); row = []; field = "";
      } else field += c;
    }
    if (field.length || row.length) { row.push(field); rows.push(row); }
    const head = (rows.shift() || []).map(h => h.trim());
    return rows.filter(r => r.length > 1 || String(r[0] || "").trim())
      .map(r => Object.fromEntries(head.map((h, i) => [h, r[i] === undefined ? "" : r[i]])));
  }
  async function cxFetchCsv(url) {
    const res = await fetch(url, { credentials: "same-origin", cache: "no-store" });
    if (!res.ok) throw new Error(`The cancelled orders could not be read (${res.status}). Run the Cancel workflow, then the dashboard build.`);
    const text = await new Response(res.body.pipeThrough(new DecompressionStream("gzip"))).text();
    return cxParseCsv(text);
  }
  async function cxLoad() {
    if (CX.rows || CX.loading) return;
    CX.loading = true;
    try { CX.rows = await cxFetchCsv("data/cancel_orders.csv.gz"); } catch (err) { CX.error = err.message; }
    try { CX.items = await cxFetchCsv("data/cancel_items.csv.gz"); } catch (err) { CX.items = []; CX.itemsError = err.message; }
    CX.loading = false;
    render();
  }

  // A cancelled order as a sales row, with the same indexes, so the page's filters apply to it the same way.
  function cxAsRow(c, key) {
    const st = (M.stores || []).find(s => cxNorm(s.name) === cxNorm(c.Branch));
    return [key, st ? st.i : -1, cxIdx(M.brands, c["Brand Name"]), cxIdx(M.storeTypes, c["Store Type"]),
            cxIdx(M.regions, c.Region), cxIdx(M.sources, c.Source), -1, 0, 0, 0, 1];
  }
  function cxChanOk(r) {
    if (CX.chan === "all") return true;
    const off = M.sources.indexOf("In Store");
    return CX.chan === "offline" ? r[5] === off : r[5] !== off;
  }
  const cxPass = r => salesPass(r) && cxChanOk(r);
  const amOf = st => (st && M.ams && M.ams[st.a] !== undefined ? M.ams[st.a] : "Unassigned");

  // Every order of the day, through the page's filters, as one record each.
  function cxRecords(key) {
    const recs = [];
    const byI = new Map((M.stores || []).map(s => [s.i, s]));
    const byName = new Map((M.stores || []).map(s => [cxNorm(s.name), s]));
    for (const [i, rows] of STORE_ROWS) {
      const st = byI.get(i);
      for (const r of rows) {
        if (r[0] !== key || !cxPass(r)) continue;
        recs.push({ brand: cxName(M.brands, r[2], "Unmatched"), region: cxName(M.regions, r[4], "Unmatched"),
                    source: cxName(M.sources, r[5], "Unmatched"), store: st ? st.name : "Store " + i, am: amOf(st),
                    closed: r[10], rdc: 0, other: 0 });
      }
    }
    const iso = cxIso(key);
    for (const c of CX.rows || []) {
      if (c.Date !== iso) continue;
      const r = cxAsRow(c, key);
      if (!cxPass(r)) continue;
      const st = byName.get(cxNorm(c.Branch));
      const listed = c.Listed === "Yes";
      recs.push({ brand: cxName(M.brands, r[2], c["Brand Name"] || "Unmatched"),
                  region: cxName(M.regions, r[4], c.Region || "Unmatched"),
                  source: cxName(M.sources, r[5], c.Source || "Unmatched"),
                  store: st ? st.name : (c.Branch || "Unmatched"), am: amOf(st),
                  closed: 0, rdc: listed ? 1 : 0, other: listed ? 0 : 1 });
    }
    return recs;
  }
  function cxSum(list, label) {
    const g = { label, closed: 0, rdc: 0, other: 0 };
    for (const x of list) { g.closed += x.closed; g.rdc += x.rdc; g.other += x.other; }
    return g;
  }
  function cxGroups(recs, dim) {
    const m = new Map();
    for (const x of recs) {
      const k = x[dim];
      if (!m.has(k)) m.set(k, []);
      m.get(k).push(x);
    }
    return [...m.entries()].map(([k, list]) => cxSum(list, k)).sort((a, b) => cxOverall(b) - cxOverall(a));
  }
  const cxStoresOf = (recs, am) => cxGroups(recs.filter(x => x.am === am), "store");

  // The products on cancellation: the day's cancelled item lines, by category, then by item. Orders are the distinct
  // cancelled orders that have the item. The page's filters and channel apply.
  function cxProducts(key) {
    const iso = cxIso(key), cats = new Map();
    for (const it of CX.items || []) {
      if (it.Date !== iso || !cxPass(cxAsRow(it, key))) continue;
      const cat = it.Category || "(no category)", name = it["Item Name"] || "(no item)";
      if (!cats.has(cat)) cats.set(cat, { label: cat, items: new Map(), inv: new Set(), rdc: new Set(), other: new Set(), net: 0 });
      const g = cats.get(cat);
      if (!g.items.has(name)) g.items.set(name, { label: name, inv: new Set(), rdc: new Set(), other: new Set(), net: 0 });
      const x = g.items.get(name);
      const net = Number(it["Item Net"]) || 0;
      for (const t of [g, x]) {
        t.inv.add(it["Invoice No"]);
        (it.Listed === "Yes" ? t.rdc : t.other).add(it["Invoice No"]);
        t.net += net;
      }
    }
    const shape = t => ({ label: t.label, orders: t.inv.size, rdcN: t.rdc.size, otherN: t.other.size, net: t.net });
    return [...cats.values()].map(g => ({ ...shape(g), items: [...g.items.values()].map(shape) }))
      .sort((a, b) => b.orders - a.orders);
  }
  function cxProductsSection(key) {
    if (!CX.items) {
      return `<section class="block"><h2>Products on cancellation</h2><p class="muted">${cxEsc(CX.itemsError || "Loading the products.")}</p></section>`;
    }
    const row = (label, o, attr, cls) => `<tr class="${cls || ""}"><td>${attr || ""}${cxEsc(label)}${attr ? "</button>" : ""}</td>` +
      `<td>${cxNum(o.orders)}</td><td>${cxNum(o.rdcN)}</td><td>${cxNum(o.otherN)}</td><td>${cxNum(o.net)}</td></tr>`;
    let body = "";
    for (const g of cxProducts(key)) {
      const open = CX.openCat.has(g.label);
      body += row(`${open ? "▾" : "▸"} ${g.label}`, g, `<button data-cx-cat="${cxEsc(g.label)}">`);
      if (open) for (const x of g.items) body += row(x.label, x, "", "ins-st");
    }
    if (!body) body = `<tr><td colspan="5" class="muted">No cancelled items on this day.</td></tr>`;
    return `<section class="block"><h2>Products on cancellation</h2>` +
      `<p class="sub">The cancelled items of the day, by category. Click a category for its items. Orders = distinct cancelled orders with the item.</p>` +
      `<div class="scroll"><table class="tbl"><thead><tr><th>Category / item</th><th>Cancelled orders</th><th>Cancel (RDC)</th>` +
      `<th>Other</th><th>Cancelled net sales</th></tr></thead><tbody>${body}</tbody></table></div></section>`;
  }
  function cxRow(label, g, attr, cls) {
    const pct = cxPct(g);
    return `<tr class="${cls || ""}"><td>${attr || ""}${cxEsc(label)}${attr ? "</button>" : ""}</td>` +
      `<td>${cxNum(cxOverall(g))}</td><td>${cxNum(g.rdc)}</td><td>${cxNum(g.other)}</td>` +
      `<td>${pct === null ? "—" : pct.toFixed(1) + "%"}</td></tr>`;
  }
  function cxView() {
    const key = PERIOD.key != null ? PERIOD.key : M.last_day;
    setPeriodBar("daily", key, null);
    if (CX.error) return `<section class="block"><h2>Cancellation</h2><p class="muted">${cxEsc(CX.error)}</p></section>`;
    if (!CX.rows) {
      cxLoad();
      return `<section class="block"><h2>Cancellation</h2><p class="muted">Loading the cancelled orders.</p></section>`;
    }
    const loaded = STORE_ROWS.size, all = (M.stores || []).length;
    const note = loaded < all ? `<p class="muted">Loading the store sales: ${loaded} of ${all} stores.</p>` : "";
    const recs = cxRecords(key);
    const groups = cxGroups(recs, CX.dim);
    const total = cxSum(recs, "All");
    const label = (CX_DIMS.find(x => x[0] === CX.dim) || CX_DIMS[0])[1];
    const chips = CX_DIMS.map(([v, l]) => `<button type="button" class="cx-chip${CX.dim === v ? " on" : ""}" data-cx-dim="${v}">${l}</button>`).join("");
    const chans = [["all", "All"], ["offline", "Offline (In Store)"], ["online", "Online"]]
      .map(([v, l]) => `<button type="button" class="cx-chip${CX.chan === v ? " on" : ""}" data-cx-chan="${v}">${l}</button>`).join("");
    let body = cxRow("All", total, "", "tot");
    for (const g of groups) {
      if (CX.dim === "am") {
        const open = CX.open.has(g.label);
        body += cxRow(`${open ? "▾" : "▸"} ${g.label}`, g, `<button data-cx-am="${cxEsc(g.label)}">`);
        if (open) for (const s of cxStoresOf(recs, g.label)) body += cxRow(s.label, s, "", "ins-st");
      } else body += cxRow(g.label, g, "");
    }
    return `<section class="block"><h2>Cancellation: by ${label.toLowerCase()}</h2>` +
      `<p class="sub">The day of the Daily tab. Overall orders = closed + cancelled. Cancel orders (RDC) = cancelled with a listed reason. ` +
      `Other orders = cancelled with no listed reason. Cancel orders % = RDC ÷ overall.</p>${note}` +
      `<div class="cx-chips">${chans}</div><div class="cx-chips">${chips}</div>` +
      `<div class="scroll"><table class="tbl"><thead><tr><th>${label}</th><th>Overall orders</th><th>Cancel orders (RDC)</th>` +
      `<th>Other orders</th><th>Cancel orders %</th></tr></thead><tbody>${body}</tbody></table></div></section>` + cxProductsSection(key);
  }

  // Clicks: the dimension, the channel, an area manager (its stores).
  document.addEventListener("click", e => {
    const t = e.target;
    const d = t && t.closest ? t.closest("[data-cx-dim]") : null;
    if (d) { CX.dim = d.dataset.cxDim; CX.open.clear(); render(); return; }
    const c = t && t.closest ? t.closest("[data-cx-chan]") : null;
    if (c) { CX.chan = c.dataset.cxChan; render(); return; }
    const cat = t && t.closest ? t.closest("[data-cx-cat]") : null;
    if (cat) {
      const k = cat.dataset.cxCat;
      if (CX.openCat.has(k)) CX.openCat.delete(k); else CX.openCat.add(k);
      render();
      return;
    }
    const a = t && t.closest ? t.closest("[data-cx-am]") : null;
    if (a) {
      const k = a.dataset.cxAm;
      if (CX.open.has(k)) CX.open.delete(k); else CX.open.add(k);
      render();
    }
  });

  // Tab and wiring: the Cancellation tab is added to the Sales tabs; every other tab is left as it is.
  if (typeof SUBS !== "undefined" && SUBS.sales && !SUBS.sales.some(x => x[0] === "cancel")) {
    SUBS.sales.push(["cancel", "Cancellation"]);
  }
  const salesPeriodViewCx = salesPeriodView;
  salesPeriodView = function (kind) {
    return kind === "cancel" ? cxView() : salesPeriodViewCx(kind);
  };
  window.FB_CANCEL = { CX, cxView, cxRecords, cxGroups, cxStoresOf, cxParseCsv, cxLoad, cxProducts, cxProductsSection,
                       setRows(rows) { CX.rows = rows; CX.error = null; },
                       setItems(rows) { CX.items = rows; CX.itemsError = null; } };
})();
