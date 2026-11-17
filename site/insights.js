/* ============ Sales: top and bottom stores, and the area manager table (Daily, Weekly and Monthly tabs) ============
   Top and bottom stores come after the session table, with WoW, MoM and YoY growth.
   The area manager table lists each manager. Click a manager to open its stores; click a store
   to see it by brand and source, against last week (last month on the Monthly tab).
   Loaded last. It wraps salesPeriodView(), after the Campaign tab. */
const INS = { loaded: false, loading: false, error: null, amOpen: new Set(), store: null };
const INS_KINDS = ["daily", "weekly", "monthly"];
const insEsc = s => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
const insP = v => (v === null || !isFinite(v) ? "—" : (v >= 0 ? "+" : "−") + Math.abs(v).toFixed(1) + "%");
const insC = v => (v === null || !isFinite(v) ? "var(--ink-2)" : v >= 0 ? "var(--up)" : "var(--down)");
const insG = (cur, prev) => (cur === null || prev === null || !prev ? null : (cur / prev - 1) * 100);
const insRs = v => (v === null ? "—" : "₹" + num(v));

// Every store file, loaded once. The page redraws when they are all in.
async function insLoad() {
  if (INS.loaded || INS.loading || !M) return;
  INS.loading = true;
  try {
    for (const s of M.stores) {
      if (!STORE_ROWS.has(s.i)) {
        const data = await loadGz(`data/stores/${s.i}.json.gz`);
        STORE_ROWS.set(s.i, data.rows);
      }
    }
    INS.loaded = true;
  } catch (err) {
    INS.error = err.message;
  }
  INS.loading = false;
  render();
}

// Comparison periods: last week (WoW), last month (MoM), last year (YoY). The Monthly tab has no week.
function insKeys(kind, key) {
  if (kind === "daily") return { wow: key - 7, mom: shiftMonths(key, -1), yoy: shiftMonths(key, -12) };
  if (kind === "weekly") return { wow: key - 7, mom: mondayOf(shiftMonths(key, -1)), yoy: mondayOf(shiftMonths(key, -12)) };
  return { wow: null, mom: key - 1, yoy: key - 12 };
}

// Net sales (rupees) over a range of days, with the page filters and an optional extra test; null when there are no rows.
function insNet(rows, from, to, extra) {
  let t = 0, any = false;
  for (const r of rows) {
    if (r[0] < from || r[0] > to || !salesPass(r)) continue;
    if (extra && !extra(r)) continue;
    t += r[7]; any = true;
  }
  return any ? t / 100 : null;
}

// One line per store: the current period and its three comparisons.
function insStores(kind, key) {
  const K = insKeys(kind, key);
  const win = k => (k === null ? null : windowOf(kind, k));
  const cur = win(key), w = win(K.wow), m = win(K.mom), y = win(K.yoy);
  const out = [];
  for (const s of M.stores) {
    const rows = STORE_ROWS.get(s.i);
    if (!rows) continue;
    const c = insNet(rows, cur[0], cur[1]);
    if (c === null) continue;
    out.push({
      i: s.i, name: s.name, a: s.a, cur: c,
      wowN: w ? insNet(rows, w[0], w[1]) : null,
      momN: m ? insNet(rows, m[0], m[1]) : null,
      yoyN: y ? insNet(rows, y[0], y[1]) : null
    });
  }
  return out;
}

// Best and worst stores by WoW (MoM on the Monthly tab). Bottom stores come from the stores not already in the top.
function insTopBottom(list, kind) {
  const key = kind === "monthly" ? "momN" : "wowN";
  const g = s => insG(s.cur, s[key]);
  const ranked = list.filter(s => s.cur > 0 && g(s) !== null).map(s => ({ s, g: g(s) })).sort((p, q) => q.g - p.g);
  const top = ranked.slice(0, 5);
  const bottom = ranked.slice(5).slice(-5).reverse();
  return { cmp: kind === "monthly" ? "MoM" : "WoW", top, bottom };
}

// Totals per area manager, with its stores (largest first).
function insAmRows(list) {
  const out = [];
  (M.ams || []).forEach((name, a) => {
    const st = list.filter(s => s.a === a);
    if (!st.length) return;
    const sum = f => st.reduce((t, s) => t + (s[f] || 0), 0);
    out.push({ a, name, cur: sum("cur"), wowN: sum("wowN"), momN: sum("momN"), yoyN: sum("yoyN"),
      stores: st.slice().sort((p, q) => q.cur - p.cur) });
  });
  return out.filter(x => x.cur > 0).sort((p, q) => q.cur - p.cur);
}

const insCells = (cur, wowN, momN, yoyN) =>
  `<td>${insRs(cur)}</td>` +
  `<td style="color:${insC(insG(cur, wowN))}">${insP(insG(cur, wowN))}</td>` +
  `<td style="color:${insC(insG(cur, momN))}">${insP(insG(cur, momN))}</td>` +
  `<td style="color:${insC(insG(cur, yoyN))}">${insP(insG(cur, yoyN))}</td>`;
const insHead = first => `<thead><tr><th>${first}</th><th>Net ₹</th><th>WoW</th><th>MoM</th><th>YoY</th></tr></thead>`;
const insTable = (first, body) => `<div class="scroll"><table class="tbl">${insHead(first)}<tbody>${body}</tbody></table></div>`;
const insStoreRow = x => {
  const s = x.s;
  return `<tr><td>${insEsc(s.name)}</td>${insCells(s.cur, s.wowN, s.momN, s.yoyN)}</tr>`;
};

function insAmTable(list) {
  const ams = insAmRows(list);
  if (!ams.length) return `<p class="muted">No sales by area manager for this period.</p>`;
  const body = ams.map(x => {
    const open = INS.amOpen.has(x.a);
    let html = `<tr class="ins-am"><td><button data-ins-am="${x.a}">${open ? "▾" : "▸"} ${insEsc(x.name)}</button></td>` +
      `${insCells(x.cur, x.wowN, x.momN, x.yoyN)}</tr>`;
    if (open) {
      html += x.stores.map(s => `<tr class="ins-st"><td><button data-ins-store="${s.i}">${insEsc(s.name)}</button></td>` +
        `${insCells(s.cur, s.wowN, s.momN, s.yoyN)}</tr>`).join("");
    }
    return html;
  }).join("");
  return insTable("Area manager", body);
}

// The store opened from the area manager table: net sales by brand and source, with growth against the comparison period.
function insDetail(kind, key) {
  if (INS.store === null) return "";
  const s = M.stores.find(x => x.i === INS.store);
  const rows = STORE_ROWS.get(INS.store);
  if (!s || !rows) return "";
  const K = insKeys(kind, key);
  const cmpKey = kind === "monthly" ? K.mom : K.wow;
  const cur = windowOf(kind, key), cmp = windowOf(kind, cmpKey);
  const sources = M.sources.map((_, j) => j);
  const cell = (b, j) => {
    const f = r => r[2] === b && r[5] === j;
    const c = insNet(rows, cur[0], cur[1], f), p = insNet(rows, cmp[0], cmp[1], f);
    if (c === null) return `<td class="muted">—</td>`;
    return `<td>${insRs(c)}<br><span style="color:${insC(insG(c, p))}">${insP(insG(c, p))}</span></td>`;
  };
  const body = M.brands.map((_, b) => {
    const any = sources.some(j => insNet(rows, cur[0], cur[1], r => r[2] === b && r[5] === j) !== null);
    return any ? `<tr><td>${insEsc(M.brands[b])}</td>${sources.map(j => cell(b, j)).join("")}</tr>` : "";
  }).join("");
  const vs = kind === "monthly" ? "last month" : "last week";
  return `<section class="block"><h2>${insEsc(s.name)}: by brand and source</h2>
    <p class="sub">Net ₹ for this period, with growth against ${vs}. Click the store name again to close.</p>
    <div class="scroll"><table class="tbl"><thead><tr><th>Brand</th>${sources.map(j => `<th>${insEsc(M.sources[j] === "In Store" ? "Offline" : M.sources[j])}</th>`).join("")}</tr></thead>
    <tbody>${body || `<tr><td colspan="${sources.length + 1}" class="muted">No sales for this store in this period.</td></tr>`}</tbody></table></div></section>`;
}

function insBlock(kind, key) {
  if (!M || !M.stores || !M.stores.length) return "";
  if (INS.error) return `<section class="block"><h2>Stores and area managers</h2><p class="muted">${insEsc(INS.error)}</p></section>`;
  if (!INS.loaded) {
    insLoad();
    return `<section class="block"><h2>Top and bottom stores</h2><p class="muted">Loading store data.</p></section>`;
  }
  const list = insStores(kind, key);
  const tb = insTopBottom(list, kind);
  const topRows = tb.top.map(insStoreRow).join("");
  const bottomRows = tb.bottom.map(insStoreRow).join("");
  return `<section class="block"><h2>Top stores</h2><p class="sub">Ranked by ${tb.cmp} growth in net sales.</p>` +
    (topRows ? insTable("Store", topRows) : `<p class="muted">No stores with sales in this period.</p>`) + `</section>` +
    `<section class="block"><h2>Bottom stores</h2><p class="sub">Ranked by ${tb.cmp} growth in net sales.</p>` +
    (bottomRows ? insTable("Store", bottomRows) : `<p class="muted">Fewer than six stores have sales in this period.</p>`) + `</section>` +
    `<section class="block"><h2>By area manager</h2>` +
    `<p class="sub">Click a manager to see its stores. Click a store to see it by brand and source.</p>${insAmTable(list)}</section>` +
    insDetail(kind, key);
}

// Removes an older "By area manager" table from the page, so there is one table (this module\x27s).
function insRemoveOldAm(html) {
  const idx = html.indexOf("By area manager");
  if (idx < 0) return html;
  const start = html.lastIndexOf("<section", idx);
  if (start < 0 || idx - start > 400) return html;
  const end = html.indexOf("</section>", idx);
  if (end < 0) return html;
  return html.slice(0, start) + html.slice(end + 10);
}
// Puts the block straight after the session table; at the end if the page has no session table.
function insPlace(html, block) {
  const idx = html.indexOf("By session");
  if (idx < 0) return html + block;
  const end = html.indexOf("</section>", idx);
  if (end < 0) return html + block;
  return html.slice(0, end + 10) + block + html.slice(end + 10);
}

const salesPeriodViewIns = salesPeriodView;
salesPeriodView = function (kind) {
  const base = salesPeriodViewIns(kind);
  if (!INS_KINDS.includes(kind) || typeof PERIOD === "undefined" || PERIOD.key === null || PERIOD.key === undefined) return base;
  return insPlace(insRemoveOldAm(base), insBlock(kind, PERIOD.key));
};

document.addEventListener("click", e => {
  const am = e.target.closest && e.target.closest("[data-ins-am]");
  if (am) {
    const a = +am.dataset.insAm;
    if (INS.amOpen.has(a)) INS.amOpen.delete(a); else INS.amOpen.add(a);
    render();
    return;
  }
  const st = e.target.closest && e.target.closest("[data-ins-store]");
  if (st) {
    const i = +st.dataset.insStore;
    INS.store = INS.store === i ? null : i;
    render();
  }
});

(function insStyle() {
  if (document.getElementById("ins-style")) return;
  const st = document.createElement("style");
  st.id = "ins-style";
  st.textContent = `.ins-am button,.ins-st button{text-align:left;color:var(--brand);font-weight:600;padding:0}
.ins-st td:first-child{padding-left:18px}`;
  document.head.appendChild(st);
})();
