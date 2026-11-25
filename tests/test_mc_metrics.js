const fs = require('fs'), vm = require('vm'), util = require('util');
const path = require('path');
const SITE = path.join(__dirname, '..', 'site');
const FIX = path.join(__dirname, 'fixtures');
const os = require('os');

let pass = 0, fail = 0;
const check = (n, c, x = '') => { c ? pass++ : fail++; console.log((c ? 'PASS ' : 'FAIL ') + n + (x ? '  [' + x + ']' : '')); };
const E = JSON.parse(fs.readFileSync(path.join(FIX, 'mc_metrics_expected.json'), 'utf8'));
const D = JSON.parse(fs.readFileSync(path.join(FIX, 'sales_test.json'), 'utf8'));
const byStore = {};
for (const r of D.rows) (byStore[r[1]] = byStore[r[1]] || []).push(r);
const listeners = {};
const node = tag => ({ tagName: tag, id: '', innerHTML: '', children: [], attrs: {}, style: {}, dataset: {},
  appendChild(c) { c._parent = this; this.children.push(c); return c; },
  remove() { if (this._parent) this._parent.children = this._parent.children.filter(c => c !== this); } });
const head = node('head'), body = node('body');
const find = (root, id) => { if (root.id === id) return root; for (const c of root.children) { const r = find(c, id); if (r) return r; } return null; };
const EPOCH_MS = Date.UTC(2025, 0, 1);
const dayIdx = (y, m, d) => Math.round((Date.UTC(y, m - 1, d) - EPOCH_MS) / 86400000);
const toDate = d => new Date(EPOCH_MS + d * 86400000);
const monStart = k => dayIdx(2025 + Math.floor(k / 12), ((k % 12) + 12) % 12 + 1, 1);
const monEnd = k => monStart(k + 1) - 1;
const mondayOf = d => d - ((toDate(d).getUTCDay() + 6) % 7);
const shiftMonths = (d, n) => { const t = toDate(d); let y = t.getUTCFullYear(), m = t.getUTCMonth() + n; const dom = t.getUTCDate(); y += Math.floor(m / 12); m = ((m % 12) + 12) % 12; const dim = new Date(Date.UTC(y, m + 1, 0)).getUTCDate(); return dayIdx(y, m + 1, Math.min(dom, dim)); };
const windowOf = (kind, k) => (kind === 'daily' ? [k, k] : kind === 'weekly' ? [k, k + 6] : [monStart(k), monEnd(k)]);
const OLD = '<section class="block"><h2>Overall</h2><p>o</p></section><section class="block"><h2>By session</h2><p>s</p></section>';
const ctx = { console, Map, Set, Math, Number, String, Array, Object, JSON, Date, Intl, Infinity, isFinite, RegExp, Error, Promise, setTimeout, TextEncoder: util.TextEncoder,
  M: { last_day: 682, sources: D.sources, brands: ['Frozen Bottle', 'Madno'], ams: ['Unassigned', 'Akash', 'Ravi'],
       stores: [0, 1, 2, 3].map(i => ({ i, name: 'Store ' + i, a: { 0: 1, 1: 1, 2: 0, 3: 2 }[i], region: i === 2 ? 'TN' : 'KA', brands: [i === 2 || i === 3 ? 1 : 0] })) },
  S: { store: 'all', type: 'all', brand: 'all', region: 'all', source: 'all', session: 'all' },
  ROLL: { rows: [] }, STORE_ROWS: new Map(), PERIOD: { kind: 'daily', key: 682, keys: [], ratingMode: false },
  SUBS: { sales: [['live', 'Live'], ['daily', 'Daily']] }, renders: 0, num: n => Math.round(n).toLocaleString('en-IN'),
  toDate, monStart, monEnd, mondayOf, shiftMonths, windowOf,
  monLong: k => toDate(monStart(k)).toLocaleDateString('en-IN', { month: 'long', year: 'numeric', timeZone: 'UTC' }),
  monShort: k => toDate(monStart(k)).toLocaleDateString('en-IN', { month: 'short', timeZone: 'UTC' }),
  fmtDay: d => toDate(d).toLocaleDateString('en-IN', { day: 'numeric', month: 'short', timeZone: 'UTC' }),
  fmtLong: d => toDate(d).toLocaleDateString('en-IN', { weekday: 'short', day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC' }),
  salesPass: r => { const S = ctx.S; return (S.store === 'all' || r[1] === +S.store) && (S.brand === 'all' || r[2] === +S.brand) && (S.region === 'all' || r[4] === +S.region) && (S.session === 'all' || r[6] === +S.session) && (S.type === 'all' || r[3] === +S.type) && (S.source === 'all' || r[5] === +S.source); },
  document: { head, body, getElementById: id => find(head, id) || find(body, id), createElement: tag => node(tag), addEventListener: (t, f) => { (listeners[t] = listeners[t] || []).push(f); } },
  loadGz: async url => ({ rows: byStore[+url.match(/stores\/(\d+)/)[1]] }),
  render() { ctx.renders++; }, salesPeriodView: () => OLD, setPeriodBar() {} };
ctx.window = ctx;
vm.createContext(ctx);
for (const i of [0, 1, 2, 3]) ctx.STORE_ROWS.set(i, byStore[i]);
vm.runInContext(fs.readFileSync(path.join(SITE, 'insights.js'), 'utf8'), ctx);
vm.runInContext(fs.readFileSync(path.join(SITE, 'morning.js'), 'utf8'), ctx);
const run = s => vm.runInContext(s, ctx);
const close = (x, y, tol = 1e-6) => (x === null || x === undefined || y === null || y === undefined) ? (x === y) : Math.abs(x - y) <= tol * Math.max(1, Math.abs(y));
const click = attrs => {
  const tgt = { dataset: {}, getAttribute: k => (k in attrs ? attrs[k] : null),
    closest: sel => { const m = sel.match(/^\[(data-[a-z-]+)\]$/); return m && m[1] in attrs ? tgt : null; } };
  for (const k of Object.keys(attrs)) if (k.startsWith('data-')) tgt.dataset[k.slice(5).replace(/-([a-z])/g, (_, c) => c.toUpperCase())] = attrs[k];
  (listeners.click || []).forEach(f => f({ target: tgt }));
};
(async () => {
  run('FB_INS.INS.loaded = true');
  for (let i = 0; i < 100 && !run('FB_INS.INS.loaded'); i++) await new Promise(r => setTimeout(r, 5));
  const c = run('FB_MORNING.mcCompute()');
  const blocks = run('FB_MORNING.mcBlocks(FB_MORNING.mcCompute())');
  const grp = run('FB_MORNING.mcGroup(FB_MORNING.mcCompute().list, FB_MORNING.mcBlocks(FB_MORNING.mcCompute())[0])');
  const ka = grp.groups.find(g => g.name === 'KA').tot;
  const storeRow = id => c.list.find(s => s.i === id);
  // each metric: store MTD values, and the week-on-week growth
  for (const m of ['net', 'dis', 'orders', 'aov']) {
    run(`FB_MORNING.MC.metric = '${m}'`);
    let okS = true, okW = true;
    for (const id of [0, 1, 2, 3]) {
      const s = storeRow(id);
      const v = run(`FB_MORNING.mcCellVal(FB_MORNING.mcCompute().list.find(s => s.i === ${id}), {k: 'novMtd'}, '${m}')`);
      if (!close(v, E.stores[String(id)][m].mtd)) okS = false;
      const g = run(`FB_MORNING.mcCellVal(FB_MORNING.mcCompute().list.find(s => s.i === ${id}), {k: 'wow'}, '${m}')`);
      if (!close(g, E.stores[String(id)][m].wow, 1e-6)) okW = false;
    }
    check(`${m}: each store's MTD value matches the raw rows`, okS);
    check(`${m}: each store's week-on-week growth matches the raw rows`, okW);
    const kaV = run(`FB_MORNING.mcCellVal(FB_MORNING.mcGroup(FB_MORNING.mcCompute().list, FB_MORNING.mcBlocks(FB_MORNING.mcCompute())[0]).groups.find(g => g.name === 'KA').tot, {k: 'novMtd'}, '${m}')`);
    check(`${m}: the KA region value matches the raw rows (summed, not averaged)`, close(kaV, E.regions.KA[m].mtd), `${kaV} vs ${E.regions.KA[m].mtd}`);
    const kaW = run(`FB_MORNING.mcCellVal(FB_MORNING.mcGroup(FB_MORNING.mcCompute().list, FB_MORNING.mcBlocks(FB_MORNING.mcCompute())[0]).groups.find(g => g.name === 'KA').tot, {k: 'wow'}, '${m}')`);
    check(`${m}: the KA week-on-week matches the raw rows`, close(kaW, E.regions.KA[m].wow, 1e-6), `${kaW} vs ${E.regions.KA[m].wow}`);
    const page = run('salesPeriodView(\'morning\')');
    check(`${m}: the page shows the chosen metric in its note`, page.includes('Showing: ' + ({ net: 'Net Sales', dis: 'Dis %', orders: 'Orders', aov: 'AOV' })[m]));
    check(`${m}: the metric bar shows the four choices with ${m} pressed`, page.includes('data-mc-metric="dis"') && page.includes(`data-mc-metric="${m}" aria-pressed="true"`));
  }
  // clicking the metric buttons changes the page
  click({ 'data-mc-metric': 'orders' });
  check('clicking Orders sets the metric', run('FB_MORNING.MC.metric') === 'orders');
  run("FB_MORNING.MC.metric = 'net'");
  // the area manager table: stores underneath each manager
  const page = run('salesPeriodView(\'morning\')');
  const amAt = page.indexOf('Stores by area manager');
  const akash = page.indexOf('<b>Akash</b>', amAt), ravi = page.indexOf('<b>Ravi</b>', amAt);
  const s0 = page.indexOf('<td>Store 0</td>', amAt), s1 = page.indexOf('<td>Store 1</td>', amAt), s2 = page.indexOf('<td>Store 2</td>', amAt);
  check('the area manager table is on the Morning Call, after the summary', amAt > page.indexOf('Region wise summary') && amAt > 0);
  check('Akash is listed, with its stores (Store 0 and Store 1) underneath', akash > 0 && s0 > akash && s1 > akash && s1 < ravi, `${akash},${s0},${s1},${ravi}`);
  check('Ravi is listed, with its store (Store 3) underneath', ravi > 0 && page.indexOf('<td>Store 3</td>', ravi) > ravi);
  check('the channel bar is above the metric bar and the summary', page.indexOf('chan-bar') < page.indexOf('mc-metric') && page.indexOf('mc-metric') < page.indexOf('Region wise summary'));
  // Excel: the area manager sheet groups the stores so they expand
  const wb = run('FB_MORNING.mcWorkbook(FB_MORNING.mcCompute(), FB_MORNING.mcBlocks(FB_MORNING.mcCompute()))');
  fs.writeFileSync(path.join(os.tmpdir(), 'mc_metrics_test.xlsx'), Buffer.from(Object.values(wb)));
  check('the Excel workbook is written', fs.statSync(path.join(os.tmpdir(), 'mc_metrics_test.xlsx')).size > 1000);
  // Dis % in the Excel file
  run("FB_MORNING.MC.metric = 'dis'");
  const wbD = run('FB_MORNING.mcWorkbook(FB_MORNING.mcCompute(), FB_MORNING.mcBlocks(FB_MORNING.mcCompute()))');
  fs.writeFileSync(path.join(os.tmpdir(), 'mc_metrics_dis.xlsx'), Buffer.from(Object.values(wbD)));
  run("FB_MORNING.MC.metric = 'net'");
  console.log(pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})();
