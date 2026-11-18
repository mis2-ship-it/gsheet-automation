/* ============ Sales: top and bottom stores, area managers, and a store window (Daily, Weekly and Monthly tabs) ============
   Top and bottom stores come after the session table, with WoW, MoM and YoY growth.
   The area manager table lists each manager. Click a manager to open its stores.
   Click a store to open a window with its net sales by brand and source (against last week, or last month on the Monthly tab).
   Loaded last. It wraps salesPeriodView(). Everything is inside one function, so no name can clash with another script. */
(function () {
  const INS = { loaded: false, loading: false, error: null, amOpen: new Set(), store: null, channel: "all", done: 0, total: 0, cache: null };
  const INS_PARALLEL = 8;
  const insLoadingNote = () => `<span id="ins-progress">Loading store data${INS.total ? `: ${INS.done} of ${INS.total} stores` : ""}.</span>`;
  const INS_KINDS = ["daily", "weekly", "monthly"];
  const insEsc = s => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  const insP = v => (v === null || !isFinite(v) ? "—" : (v >= 0 ? "+" : "−") + Math.abs(v).toFixed(1) + "%");
  const insC = v => (v === null || !isFinite(v) ? "var(--ink-2)" : v >= 0 ? "var(--up)" : "var(--down)");
  const insG = (cur, prev) => (cur === null || prev === null || !prev ? null : (cur / prev - 1) * 100);
  const insRs = v => (v === null ? "—" : "₹" + num(v));

  // Every store file, loaded once: several at a time, so the wait is not one file after another.
  function insProgress() {
    const el = document.getElementById("ins-progress");
    if (el) el.textContent = `Loading store data: ${INS.done} of ${INS.total} stores.`;
  }
  async function insLoad() {
    if (INS.loaded || INS.loading || !M) return;
    INS.loading = true;
    INS.error = null;
    const todo = M.stores.filter(s => !STORE_ROWS.has(s.i));
    INS.total = todo.length;
    INS.done = 0;
    let next = 0;
    const worker = async () => {
      while (next < todo.length) {
        const s = todo[next++];
        const data = await loadGz(`data/stores/${s.i}.json.gz`);
        STORE_ROWS.set(s.i, data.rows);
        INS.done++;
        insProgress();
      }
    };
    try {
      await Promise.all(Array.from({ length: Math.min(INS_PARALLEL, todo.length) }, worker));
      INS.loaded = true;
    } catch (err) {
      INS.error = err.message;
    }
    INS.loading = false;
    if (typeof ROLL !== "undefined" && ROLL) render();
  }

  // Start the store files as soon as the page's main data is in, so they are ready when a table is opened.
  (function insPrefetch(tries) {
    if (typeof M !== "undefined" && M && M.stores && typeof ROLL !== "undefined" && ROLL) {
      insLoad();
      return;
    }
    if (tries < 600) setTimeout(() => insPrefetch(tries + 1), 100);
  })(0);

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

  // Month to date: the 1st of the month up to the reference day (the selected day; the end of the week or month on
  // the Weekly and Monthly tabs, but never after the last day of data). MoM and YoY compare the same days last month and last year.
  const insMonthKey = d => (toDate(d).getUTCFullYear() - 2025) * 12 + toDate(d).getUTCMonth();
  const insIsMonthEnd = d => d === monEnd(insMonthKey(d));
  function insBack(ref, back) {
    if (insIsMonthEnd(ref)) return monEnd(insMonthKey(ref) - back);
    return shiftMonths(ref, -back);
  }
  function insMtdWindows(kind, key) {
    const ref0 = kind === "monthly" ? monEnd(insMonthKey(key)) : kind === "weekly" ? key + 6 : key;
    const ref = Math.min(ref0, M.last_day);
    const span = d => [monStart(insMonthKey(d)), d];
    return { cur: span(ref), mom: span(insBack(ref, 1)), yoy: span(insBack(ref, 12)) };
  }

  // One line per store: the current period, its three comparisons, and the month-to-date figures.
  // channel: "all", "offline" (the In Store source) or "online" (every other source).
  function insStores(kind, key, channel) {
    const ck = `${kind}|${key}|${channel}`;
    if (INS.cache && INS.cache.has(ck)) return INS.cache.get(ck);
    const result = insStoresCalc(kind, key, channel);
    if (INS.cache) INS.cache.set(ck, result);
    return result;
  }
  function insStoresCalc(kind, key, channel) {
    const off = M.sources.indexOf("In Store");
    const pick = channel === "offline" ? (r => r[5] === off) : channel === "online" ? (r => r[5] !== off) : null;
    const chNet = (rows, from, to) => insNet(rows, from, to, pick);
    const K = insKeys(kind, key);
    const win = k => (k === null ? null : windowOf(kind, k));
    const cur = win(key), w = win(K.wow), m = win(K.mom), y = win(K.yoy);
    const mt = insMtdWindows(kind, key);
    const out = [];
    for (const s of M.stores) {
      const rows = STORE_ROWS.get(s.i);
      if (!rows) continue;
      const c = chNet(rows, cur[0], cur[1]);
      if (c === null) continue;
      out.push({
        i: s.i, name: s.name, a: s.a, cur: c,
        wowN: w ? chNet(rows, w[0], w[1]) : null,
        momN: m ? chNet(rows, m[0], m[1]) : null,
        yoyN: y ? chNet(rows, y[0], y[1]) : null,
        mtdCur: chNet(rows, mt.cur[0], mt.cur[1]),
        mtdMom: chNet(rows, mt.mom[0], mt.mom[1]),
        mtdYoy: chNet(rows, mt.yoy[0], mt.yoy[1])
      });
    }
    return out;
  }

  // Best and worst stores by WoW (MoM on the Monthly tab). Bottom stores come from the stores not already in the top.
  function insTopBottom(list, kind) {
    const key = kind === "monthly" ? "momN" : "wowN";
    const g = s => insG(s.cur, s[key]);
    const ranked = list.filter(s => s.cur > 0 && g(s) !== null).map(s => ({ s, g: g(s) })).sort((p, q) => q.g - p.g);
    return { cmp: kind === "monthly" ? "MoM" : "WoW", top: ranked.slice(0, 5), bottom: ranked.slice(5).slice(-5).reverse() };
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
  const insTable = (first, body) =>
    `<div class="scroll"><table class="tbl"><thead><tr><th>${first}</th><th>Net ₹</th><th>WoW</th><th>MoM</th><th>YoY</th></tr></thead>` +
    `<tbody>${body}</tbody></table></div>`;
  const insStoreRow = x => {
    const s = x.s;
    return `<tr><td>${insEsc(s.name)}</td>${insCells(s.cur, s.wowN, s.momN, s.yoyN)}</tr>`;
  };

  function insAmTable(kind, key) {
    const list = insStores(kind, key, INS.channel);
    const ch = INS.channel;
    const chBtn = (c, label) => `<button data-ins-channel="${c}" aria-pressed="${ch === c}">${label}</button>`;
    const tabs = `<div class="reg-filter">${chBtn("all", "All")}${chBtn("offline", "Offline (In Store)")}${chBtn("online", "Online (other sources)")}</div>`;
    const hasOffline = M.sources.indexOf("In Store") >= 0;
    if (ch === "offline" && !hasOffline) return tabs + `<p class="muted">The data has no In Store source, so there are no offline sales.</p>`;
    const ams = insAmRows(list);
    if (!ams.length) return tabs + `<p class="muted">No sales by area manager for this period.</p>`;
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
    return tabs + insTable("Area manager", body);
  }

  // The store window: net sales by brand and source, with growth against the comparison period.
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
    return `<h2>${insEsc(s.name)}: by brand and source</h2>` +
      `<p class="sub">Net ₹ for this period, with growth against ${vs}.</p>` +
      `<div class="scroll"><table class="tbl"><thead><tr><th>Brand</th>${sources.map(j => `<th>${insEsc(M.sources[j] === "In Store" ? "Offline" : M.sources[j])}</th>`).join("")}</tr></thead>` +
      `<tbody>${body || `<tr><td colspan="${sources.length + 1}" class="muted">No sales for this store in this period.</td></tr>`}</tbody></table></div>`;
  }

  // The window is drawn over the page, so it is seen wherever the store was clicked. It is refreshed on every redraw.
  function insOverlay(kind, key) {
    let el = document.getElementById("ins-overlay");
    if (INS.store === null) {
      if (el && el.remove) el.remove();
      return;
    }
    if (!el) {
      el = document.createElement("div");
      el.id = "ins-overlay";
      document.body.appendChild(el);
    }
    el.innerHTML = `<div class="ins-backdrop" data-ins-close="1"></div>` +
      `<div class="ins-window" role="dialog" aria-modal="true"><button class="ins-close" data-ins-close="1">Close ×</button>` +
      `${insDetail(kind, key)}</div>`;
  }

  function insBlock(kind, key) {
    if (!M || !M.stores || !M.stores.length) {
      return `<section class="block"><h2>Top and bottom stores</h2><p class="muted">The store list is missing from the data. ` +
        `Rebuild the data with the new build_site.py, then reload.</p></section>`;
    }
    if (INS.error) {
      return `<section class="block"><h2>Top and bottom stores</h2><p class="muted">Store data could not be loaded: ${insEsc(INS.error)}. Reload the page.</p></section>`;
    }
    if (!INS.loaded) {
      insLoad();
      return `<section class="block"><h2>Top and bottom stores</h2><p class="muted">${insLoadingNote()}</p></section>`;
    }
    const list = insStores(kind, key);
    const tb = insTopBottom(list, kind);
    const topRows = tb.top.map(insStoreRow).join("");
    const bottomRows = tb.bottom.map(insStoreRow).join("");
    return `<section class="block"><h2>Top stores</h2>` +
      `<p class="sub">Ranked by ${tb.cmp} growth in net sales. Based on ${list.length} store(s) with sales in this period.</p>` +
      (topRows ? insTable("Store", topRows) : `<p class="muted">No stores with sales in this period.</p>`) + `</section>` +
      `<section class="block"><h2>Bottom stores</h2><p class="sub">Ranked by ${tb.cmp} growth in net sales.</p>` +
      (bottomRows ? insTable("Store", bottomRows) : `<p class="muted">Fewer than six stores have sales in this period.</p>`) + `</section>` +
      `<section class="block"><h2>By area manager</h2>` +
      `<p class="sub">Click a manager to see its stores. Click a store to see it by brand and source.</p>${insAmTable(kind, key)}</section>`;
  }

  // Removes an older "By area manager" table from the page, so there is one table (this one).
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

  // Region and store performance. WoW compares the selected day (or week, or month) with the same period a week earlier.
  // MoM and YoY compare month to date (the 1st to the selected day) with the same days last month and last year.
  const REG = { metric: "all" };
  const REG_LABEL = { wow: "WoW", mom: "MoM", yoy: "YoY" };
  const REG_SPEC = { wow: { base: "cur", prev: "wowN" }, mom: { base: "mtdCur", prev: "mtdMom" }, yoy: { base: "mtdCur", prev: "mtdYoy" } };
  const regSum = (rows, f) => rows.reduce((a, s) => a + (s[f] || 0), 0);
  // Total for a set of stores: the current sales, the month to date, and for each comparison the base and the comparison
  // sales of the stores that have both.
  function regTotal(rows) {
    const t = { cur: regSum(rows, "cur"), mtdCur: regSum(rows, "mtdCur") };
    for (const c of Object.keys(REG_SPEC)) {
      const { base, prev } = REG_SPEC[c];
      const comp = rows.filter(s => s[base] !== null && s[prev] !== null);
      t[c] = { base: comp.length ? regSum(comp, base) : null, prev: comp.length ? regSum(comp, prev) : null };
    }
    return t;
  }
  const regCmpStore = s => {
    const o = {};
    for (const c of Object.keys(REG_SPEC)) o[c] = { prev: s[REG_SPEC[c].prev], g: insG(s[REG_SPEC[c].base], s[REG_SPEC[c].prev]) };
    return o;
  };
  const regCmpTotal = t => {
    const o = {};
    for (const c of Object.keys(REG_SPEC)) o[c] = { prev: t[c].prev, g: insG(t[c].base, t[c].prev) };
    return o;
  };
  // Cells after the store name. A single WoW shows the day's sales; a single MoM or YoY shows the month to date;
  // All shows the day's sales with WoW, then the month to date with MoM and YoY.
  function regCells(day, mtd, cmp, cols) {
    const g = c => `<td>${insRs(cmp[c].prev)}</td><td style="color:${insC(cmp[c].g)}">${insP(cmp[c].g)}</td>`;
    if (cols.length === 1 && cols[0] === "wow") return `<td>${insRs(day)}</td>` + g("wow");
    if (cols.length === 1) return `<td>${insRs(mtd)}</td>` + g(cols[0]);
    return `<td>${insRs(day)}</td>` + g("wow") + `<td>${insRs(mtd)}</td>` + g("mom") + g("yoy");
  }
  function regHead(cols) {
    if (cols.length === 1 && cols[0] === "wow") return ["Sales", "WoW sales", "WoW%"];
    if (cols.length === 1) return ["MTD sales", `${REG_LABEL[cols[0]]} sales`, `${REG_LABEL[cols[0]]}%`];
    return ["Sales", "WoW sales", "WoW%", "MTD sales", "MoM sales", "MoM%", "YoY sales", "YoY%"];
  }
  function regBlock(kind, key) {
    if (!M || !M.stores || !M.stores.length) return "";
    if (!INS.loaded) return `<section class="block"><h2>Region and store performance</h2><p class="muted">${insLoadingNote()}</p></section>`;
    const regionOf = new Map(M.stores.map(s => [s.i, s.region || "Unmapped"]));
    const list = insStores(kind, key).filter(s => s.cur > 0);
    const groups = new Map();
    for (const s of list) {
      const r = regionOf.get(s.i) || "Unmapped";
      if (!groups.has(r)) groups.set(r, []);
      groups.get(r).push(s);
    }
    const cols = REG.metric === "all" ? ["wow", "mom", "yoy"] : [REG.metric];
    const heads = regHead(cols);
    const span = 1 + heads.length;
    const regions = [...groups.entries()]
      .map(([name, rows]) => ({ name, rows: rows.slice().sort((p, q) => q.cur - p.cur), tot: regTotal(rows) }))
      .sort((p, q) => q.tot.cur - p.tot.cur);
    const all = regTotal(list);
    const headRow = `<tr><th>Store</th>${heads.map(h => `<th>${h}</th>`).join("")}</tr>`;
    const body = regions.map(g =>
      `<tr class="reg-head"><td colspan="${span}">${insEsc(g.name)}</td></tr>` +
      g.rows.map(s => `<tr><td>${insEsc(s.name)}</td>${regCells(s.cur, s.mtdCur, regCmpStore(s), cols)}</tr>`).join("") +
      `<tr class="reg-total"><td>Total ${insEsc(g.name)}</td>${regCells(g.tot.cur, g.tot.mtdCur, regCmpTotal(g.tot), cols)}</tr>`
    ).join("") + `<tr class="reg-total"><td>All regions</td>${regCells(all.cur, all.mtdCur, regCmpTotal(all), cols)}</tr>`;
    const btn = (m, label) => `<button data-reg-metric="${m}" aria-pressed="${REG.metric === m}">${label}</button>`;
    return `<section class="block"><h2>Region and store performance</h2>` +
      `<p class="sub">WoW: this period against the same period a week earlier. MoM and YoY: month to date (1st to the selected day) ` +
      `against the same days last month and last year.</p>` +
      `<div class="reg-filter">${btn("all", "All")}${btn("wow", "WoW")}${btn("mom", "MoM")}${btn("yoy", "YoY")}</div>` +
      `<div class="scroll"><table class="tbl"><thead>${headRow}</thead><tbody>${body || `<tr><td colspan="${span}" class="muted">No store sales in this period.</td></tr>`}</tbody></table></div>` +
      `</section>`;
  }

  const salesPeriodViewIns = salesPeriodView;
  salesPeriodView = function (kind) {
    const base = salesPeriodViewIns(kind);
    INS.cache = new Map();
    if (!INS_KINDS.includes(kind) || typeof PERIOD === "undefined" || !PERIOD || PERIOD.key == null) return base;
    let block;
    try {
      block = insBlock(kind, PERIOD.key) + regBlock(kind, PERIOD.key);
      insOverlay(kind, PERIOD.key);
    } catch (err) {
      block = `<section class="block"><h2>Top and bottom stores</h2><p class="muted">This block could not be built: ${insEsc(err.message)}</p></section>`;
    }
    return insPlace(insRemoveOldAm(base), block);
  };

  document.addEventListener("click", e => {
    const ch = e.target.closest && e.target.closest("[data-ins-channel]");
    if (ch) {
      INS.channel = ch.dataset.insChannel;
      render();
      return;
    }
    const rm = e.target.closest && e.target.closest("[data-reg-metric]");
    if (rm) {
      REG.metric = rm.dataset.regMetric;
      render();
      return;
    }
    const close = e.target.closest && e.target.closest("[data-ins-close]");
    if (close) {
      INS.store = null;
      const el = document.getElementById("ins-overlay");
      if (el && el.remove) el.remove();
      render();
      return;
    }
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
.ins-st td:first-child{padding-left:18px}
.reg-filter{display:flex;gap:6px;flex-wrap:wrap;margin:8px 0 10px}
.reg-filter button{padding:6px 12px;border-radius:999px;border:1px solid var(--line);color:var(--ink-2);font-size:13px}
.reg-filter button[aria-pressed="true"]{background:var(--brand);border-color:var(--brand);color:#fff}
.reg-head td{background:var(--bg);font-weight:700;padding-top:12px}
.reg-total td{font-weight:700;border-top:2px solid var(--line)}
#ins-overlay{position:fixed;inset:0;z-index:50}
.ins-backdrop{position:absolute;inset:0;background:rgba(0,0,0,.45)}
.ins-window{position:absolute;left:50%;top:50%;transform:translate(-50%,-50%);width:min(960px,94vw);max-height:86vh;overflow:auto;background:var(--surface);color:var(--ink);border:1px solid var(--line);border-radius:14px;padding:16px 18px}
.ins-close{position:sticky;top:0;float:right;padding:6px 12px;border-radius:999px;border:1px solid var(--line);background:var(--bg);color:var(--ink);font-size:13px}`;
    document.head.appendChild(st);
  })();

  // For checks in the browser console and in the tests.
  window.FB_INS = { INS, insStores, insAmRows, REG, regBlock, insMtdWindows, insLoad, insKeys, insNet, insStores, insTopBottom, insAmRows, insDetail, insBlock, insOverlay, insRemoveOldAm, insPlace };
})();
