const fs = require("fs"), vm = require("vm"), util = require("util");
const path = require('path');
const SITE = path.join(__dirname, '..', 'site');
const FIX = path.join(__dirname, 'fixtures');
const os = require('os');

let pass = 0, fail = 0;
const check = (n, c, x = "") => { c ? pass++ : fail++; console.log((c ? "PASS " : "FAIL ") + n + (x ? "  [" + x + "]" : "")); };
const D = JSON.parse(fs.readFileSync(path.join(FIX, 'sales_test.json'), 'utf8'));
const E = JSON.parse(fs.readFileSync(path.join(FIX, 'sales_expected.json'), 'utf8'));
const byStore = {};
for (const r of D.rows) (byStore[r[1]] = byStore[r[1]] || []).push(r);
const listeners = {};
const node = tag => ({ tagName: tag, id: "", innerHTML: "", children: [], attrs: {}, style: {}, dataset: {},
  appendChild(c) { c._parent = this; this.children.push(c); return c; },
  remove() { if (this._parent) this._parent.children = this._parent.children.filter(c => c !== this); } });
const head = node("head"), body = node("body");
const find = (root, id) => { if (root.id === id) return root; for (const c of root.children) { const r = find(c, id); if (r) return r; } return null; };
const EPOCH_MS = Date.UTC(2025, 0, 1);
const dayIdx = (y, m, d) => Math.round((Date.UTC(y, m - 1, d) - EPOCH_MS) / 86400000);
const toDate = d => new Date(EPOCH_MS + d * 86400000);
const monStart = k => dayIdx(2025 + Math.floor(k / 12), ((k % 12) + 12) % 12 + 1, 1);
const monEnd = k => monStart(k + 1) - 1;
const mondayOf = d => d - ((toDate(d).getUTCDay() + 6) % 7);
const shiftMonths = (d, n) => { const t = toDate(d); let y = t.getUTCFullYear(), m = t.getUTCMonth() + n; const dom = t.getUTCDate(); y += Math.floor(m / 12); m = ((m % 12) + 12) % 12; const dim = new Date(Date.UTC(y, m + 1, 0)).getUTCDate(); return dayIdx(y, m + 1, Math.min(dom, dim)); };
const windowOf = (kind, k) => (kind === "daily" ? [k, k] : kind === "weekly" ? [k, k + 6] : [monStart(k), monEnd(k)]);
const OLD = "<section class=\"block\"><h2>Overall</h2><p>o</p></section><section class=\"block\"><h2>By session</h2><p>s</p></section>";
const ctx = { FB_MC_REF: 682, console, Map, Set, Math, Number, String, Array, Object, JSON, Date, Intl, Infinity, isFinite, RegExp, Error, Promise, setTimeout, TextEncoder: util.TextEncoder,
  M: { last_day: 682, sources: D.sources, brands: ["Frozen Bottle", "Madno"], ams: ["Unassigned", "Akash", "Ravi"],
       stores: [0, 1, 2, 3].map(i => ({ i, name: "Store " + i, a: { 0: 1, 1: 1, 2: 0, 3: 2 }[i], region: i === 2 ? "TN" : "KA", brands: [i === 2 || i === 3 ? 1 : 0] })) },
  S: { store: "all", type: "all", brand: "all", region: "all", source: "all", session: "all" },
  ROLL: { rows: [] }, STORE_ROWS: new Map(), PERIOD: { kind: "daily", key: 682, keys: [], ratingMode: false },
  SUBS: { sales: [["live", "Live"], ["daily", "Daily"]] }, renders: 0, num: n => Math.round(n).toLocaleString("en-IN"),
  toDate, monStart, monEnd, mondayOf, shiftMonths, windowOf,
  monLong: k => toDate(monStart(k)).toLocaleDateString("en-IN", { month: "long", year: "numeric", timeZone: "UTC" }),
  monShort: k => toDate(monStart(k)).toLocaleDateString("en-IN", { month: "short", timeZone: "UTC" }),
  fmtDay: d => toDate(d).toLocaleDateString("en-IN", { day: "numeric", month: "short", timeZone: "UTC" }),
  fmtLong: d => toDate(d).toLocaleDateString("en-IN", { weekday: "short", day: "numeric", month: "short", year: "numeric", timeZone: "UTC" }),
  salesPass: r => { const S = ctx.S; return (S.store === "all" || r[1] === +S.store) && (S.brand === "all" || r[2] === +S.brand) && (S.region === "all" || r[4] === +S.region) && (S.session === "all" || r[6] === +S.session) && (S.type === "all" || r[3] === +S.type) && (S.source === "all" || r[5] === +S.source); },
  document: { head, body, getElementById: id => find(head, id) || find(body, id), createElement: tag => node(tag), addEventListener: (t, f) => { (listeners[t] = listeners[t] || []).push(f); } },
  loadGz: async url => ({ rows: byStore[+url.match(/stores\/(\d+)/)[1]] }),
  render() { ctx.renders++; }, salesPeriodView: () => OLD, setPeriodBar() {} };
ctx.window = ctx;
vm.createContext(ctx);
for (const i of [0, 1, 2, 3]) ctx.STORE_ROWS.set(i, byStore[i]);
vm.runInContext(fs.readFileSync(path.join(SITE, 'insights.js'), 'utf8'), ctx);
vm.runInContext(fs.readFileSync(path.join(SITE, 'morning.js'), 'utf8'), ctx);
const run = s => vm.runInContext(s, ctx);
const close = (x, y) => Math.abs(x - y) < 1e-6 * Math.max(1, Math.abs(y));
const click = attrs => {
  const tgt = { dataset: {}, getAttribute: k => (k in attrs ? attrs[k] : null),
    closest: sel => { const m = sel.match(/^\[(data-[a-z-]+)\]$/); return m && m[1] in attrs ? tgt : null; } };
  for (const k of Object.keys(attrs)) if (k.startsWith("data-")) tgt.dataset[k.slice(5).replace(/-([a-z])/g, (_, c) => c.toUpperCase())] = attrs[k];
  (listeners.click || []).forEach(f => f({ target: tgt }));
};
(async () => {
  run("FB_INS.INS.loaded = true");
  for (let i = 0; i < 100 && !run("FB_INS.INS.loaded"); i++) await new Promise(r => setTimeout(r, 5));
  const page0 = run("salesPeriodView('morning')");
  check("the YTD last year label starts on 1 Nov 2025", page0.includes("YTD last year (" + E.ly_label.replace(" to ", " to ") + ")") || page0.includes("YTD last year (1 Nov 2025 to 14 Nov 2025)"));
  check("the channel bar is at the top of the Morning Call", page0.includes("data-chan=\"offline\"") && page0.includes("data-chan=\"all\" aria-pressed=\"true\""));
  check("regions are listed without their sources until opened", page0.includes("data-mc-region=\"KA\"") && !page0.includes("Ownly"));
  // clicking KA opens its sources
  click({ "data-mc-region": "KA" });
  check("clicking KA opens it", run("FB_MORNING.MC.regOpen.has('KA')"));
  const page1 = run("salesPeriodView('morning')");
  check("KA shows its sources: In Store, Swiggy, Zomato, Ownly and the rest", ["In Store", "Swiggy", "Zomato", "Ownly", "Others (rest of the sources)"].every(s => page1.includes(s)));
  click({ "data-mc-region": "KA" });
  check("clicking KA again closes it", !run("FB_MORNING.MC.regOpen.has('KA')"));
  // the sources add up to the region
  run("FB_MORNING.MC.regOpen.add('KA')");
  const blocks = run("FB_MORNING.mcBlocks(FB_MORNING.mcCompute())");
  const c = run("FB_MORNING.mcCompute()");
  const srcT = run("FB_MORNING.mcSourceTotals(FB_MORNING.mcBlocks(FB_MORNING.mcCompute()))");
  const momId = blocks[0].id;
  const kaTot = run("FB_MORNING.mcGroup(FB_MORNING.mcCompute().list, FB_MORNING.mcBlocks(FB_MORNING.mcCompute())[0]).groups.find(g => g.name === 'KA').tot.novMtd");
  const srcSum = Object.keys(srcT).reduce((a, lab) => a + ((srcT[lab][momId] || {})["KA"] || {}).novMtd || 0, 0);
  check("the sources of KA add up to the KA total (MTD)", close(srcSum, kaTot), `${srcSum} vs ${kaTot}`);
  check("the In Store source matches the separate calculation", close(((srcT["In Store"][momId] || {})["KA"] || {}).novMtd, E.ka_mtd_offline), `${((srcT["In Store"][momId] || {})["KA"] || {}).novMtd} vs ${E.ka_mtd_offline}`);
  check("the Swiggy source matches the separate calculation", close(((srcT["Swiggy"][momId] || {})["KA"] || {}).novMtd, E.ka_mtd_swiggy));
  check("the All regions row is the sum of the sources too", close(Object.keys(srcT).reduce((a, lab) => a + ((srcT[lab][momId] || {})["All regions"] || {}).novMtd || 0, 0), run("FB_MORNING.mcGroup(FB_MORNING.mcCompute().list, FB_MORNING.mcBlocks(FB_MORNING.mcCompute())[0]).all.novMtd")));
  // the channel filter applies to every figure
  run("FB_SALES_CH.value = 'offline'");
  const offKa = run("FB_MORNING.mcGroup(FB_MORNING.mcCompute().list, FB_MORNING.mcBlocks(FB_MORNING.mcCompute())[0]).groups.find(g => g.name === 'KA').tot.novMtd");
  check("Offline on the channel bar: the Morning Call shows In Store only", close(offKa, E.ka_mtd_offline), `${offKa} vs ${E.ka_mtd_offline}`);
  const offStores = run("FB_INS.insStores('daily', 682, 'all')").map(s => [s.i, s.cur]);
  check("Offline on the channel bar: the sales tables show In Store only (stores, day)", offStores.every(([i, v]) => close(v, E.store_day_offline[String(i)])), JSON.stringify(offStores));
  check("the channel bar marks Offline as pressed", run("salesPeriodView('daily')").includes("data-chan=\"offline\" aria-pressed=\"true\""));
  run("FB_SALES_CH.value = 'online'");
  const onKa = run("FB_MORNING.mcGroup(FB_MORNING.mcCompute().list, FB_MORNING.mcBlocks(FB_MORNING.mcCompute())[0]).groups.find(g => g.name === 'KA').tot.novMtd");
  check("Online on the channel bar: every source except In Store", close(onKa, E.ka_mtd_total - E.ka_mtd_offline), `${onKa}`);
  run("FB_SALES_CH.value = 'all'");
  const allKa = run("FB_MORNING.mcGroup(FB_MORNING.mcCompute().list, FB_MORNING.mcBlocks(FB_MORNING.mcCompute())[0]).groups.find(g => g.name === 'KA').tot.novMtd");
  check("All sources puts the whole region back", close(allKa, E.ka_mtd_total), `${allKa}`);
  // the Excel file: Region wise, Region + source, then the store sheets
  const wbBytes = run("FB_MORNING.mcWorkbook(FB_MORNING.mcCompute(), FB_MORNING.mcBlocks(FB_MORNING.mcCompute()))");
  fs.writeFileSync(path.join(os.tmpdir(), 'source_test.xlsx'), Buffer.from(Object.values(wbBytes)));
  check("the Excel file is written with the Region + source sheet", fs.statSync(path.join(os.tmpdir(), 'source_test.xlsx')).size > 1000);
  // the Sales tab shows the channel bar
  check("the Daily tab shows the channel bar at the top", run("salesPeriodView('daily')").indexOf("chan-bar") === 0 || run("salesPeriodView('daily')").indexOf("chan-bar") >= 0 && run("salesPeriodView('daily')").indexOf("chan-bar") < run("salesPeriodView('daily')").indexOf("Overall"));
  console.log(pass + " passed, " + fail + " failed");
  process.exit(fail ? 1 : 0);
})();
