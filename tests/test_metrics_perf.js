// The Metrics tab: the Sales tab's filters apply, the own filters change on a change event, the charts show values,
// and the uploaded file is kept in the browser across a reload.
const fs = require('fs'), vm = require('vm'), zlib = require('zlib');
let pass = 0, fail = 0;
const check = (n, c, x = '') => { c ? pass++ : fail++; console.log((c ? 'PASS ' : 'FAIL ') + n + (x ? '  [' + x + ']' : '')); };
const gzBuf = fs.readFileSync(require('path').join(__dirname, 'fixtures', 'metrics.json.gz'));
const DATA = JSON.parse(zlib.gunzipSync(gzBuf).toString('utf8'));
const E = JSON.parse(fs.readFileSync(require('path').join(__dirname, 'fixtures', 'metrics_expected.json'), 'utf8'));
const close = (x, y) => Math.abs(x - y) < 1e-6 * Math.max(1, Math.abs(y));
const handlers = {};
const store = new Map();
let loadImpl = async () => DATA;
const storage = { getItem: k => (store.has(k) ? store.get(k) : null), setItem: (k, v) => store.set(k, String(v)), removeItem: k => store.delete(k) };
const ctx = { console, Map, Set, Math, Number, String, Array, Object, JSON, Date, Intl, Infinity, isFinite, RegExp, Error, Promise, setTimeout,
  SUBS: { sales: [['live', 'Live'], ['daily', 'Daily']] }, renders: 0, num: n => Math.round(n).toLocaleString('en-IN'),
  loadGz: async (u) => loadImpl(u), render() { ctx.renders++; }, salesPeriodView: k => 'BASE:' + k, setPeriodBar() {},
  document: { addEventListener: (t, f) => { (handlers[t] = handlers[t] || []).push(f); }, getElementById: () => null, createElement: () => ({}), head: { appendChild() {} } },
  localStorage: storage, Blob, Response, DecompressionStream, TextDecoder, TextEncoder, btoa, atob };
ctx.window = ctx;
// the page's own values: the manifest and the Sales filters
const storesList = [...new Map(DATA.stores.map((n, i) => [n, i])).keys()];
ctx.M = { sources: DATA.sources, brands: DATA.brands, regions: DATA.regions, storeTypes: DATA.storeTypes,
          stores: storesList.map((name, i) => ({ i, name })) };
ctx.S = { store: 'all', type: 'all', brand: 'all', region: 'all', source: 'all', session: 'all' };
vm.createContext(ctx);
vm.runInContext(fs.readFileSync(require('path').join(__dirname, '..', 'site', 'metrics.js'), 'utf8'), ctx);
const run = s => vm.runInContext(s, ctx);
const fire = (type, target) => (handlers[type] || []).forEach(f => f({ target }));
(async () => {
  loadImpl = async () => DATA;
  run("salesPeriodView('metrics')");
  for (let i = 0; i < 100 && !run('FB_MT.MT.data'); i++) await new Promise(r => setTimeout(r, 5));
  run("M.stores = M.stores.map((s, i) => ({ ...s, a: (i % 2) + 1 })); M.ams = { 1: 'Reazul', 2: 'Akash' };");
  const A = run('FB_MT.MT.a'), B = run('FB_MT.MT.b');
  const brandName = b => DATA.brands[b];
  let html = run("salesPeriodView('metrics')");
  check('the performance table is on the page, by brand first', html.includes('Performance by brand') && html.includes(brandName(0)));
  // brand totals equal the raw rows in the period
  const ta = run("FB_MT.mtPerfTotals('brand', FB_MT.MT.a.from, FB_MT.MT.a.to)");
  const rawOrders = (name, p) => DATA.rows.filter(r => DATA.brands[r[2]] === name && r[0] >= p.from && r[0] <= p.to).reduce((t, r) => t + r[7], 0);
  const allBrandsOk = DATA.brands.every(n => { const x = run(`FB_MT.mtPerfTotals('brand', FB_MT.MT.a.from, FB_MT.MT.a.to).get(${JSON.stringify(n)})`); return (x ? x.orders : 0) === rawOrders(n, A); });
  check('each brand\'s orders in period A equal the raw rows', allBrandsOk);
  // a click on a brand row shows its days, for A and for B
  const brand0 = DATA.brands[0];
  fire('click', { closest: sel => (sel === '[data-mtp-key]' ? { dataset: { mtpKey: brand0 } } : null) });
  check('a click opens the brand\'s days', run('FB_MT.MT.perf.open.has(' + JSON.stringify(brand0) + ')'));
  html = run("salesPeriodView('metrics')");
  const dayRows = (html.match(/<tr class="ins-st"><td>[AB] · /g) || []).length;
  check('the days of both periods are listed under the brand', dayRows === (A.to - A.from + 1) + (B.to - B.from + 1), String(dayRows));
  const dsum = run(`FB_MT.mtPerfDays('brand', ${JSON.stringify(brand0)}, FB_MT.MT.a).reduce((t, d) => t + d.orders, 0)`);
  check('the days add up to the brand total for period A', dsum === rawOrders(brand0, A), `${dsum} vs ${rawOrders(brand0, A)}`);
  // area manager and region views
  fire('click', { closest: sel => (sel === '[data-mtp-dim]' ? { dataset: { mtpDim: 'am' } } : null) });
  check('a click on Area manager switches the view and clears the open rows', run('FB_MT.MT.perf.dim') === 'am' && run('FB_MT.MT.perf.open.size') === 0);
  html = run("salesPeriodView('metrics')");
  check('the area managers are shown by name', html.includes('Reazul') && html.includes('Akash') && html.includes('Performance by area manager'));
  const amTotal = run("FB_MT.mtPerfTotals('am', FB_MT.MT.a.from, FB_MT.MT.a.to)");
  const sumAm = run(`[...FB_MT.mtPerfTotals('am', FB_MT.MT.a.from, FB_MT.MT.a.to).values()].reduce((t, x) => t + x.orders, 0)`);
  check('the area managers add up to the Comparison A total', sumAm === DATA.rows.filter(r => r[0] >= A.from && r[0] <= A.to).reduce((t, r) => t + r[7], 0), String(sumAm));
  fire('click', { closest: sel => (sel === '[data-mtp-dim]' ? { dataset: { mtpDim: 'region' } } : null) });
  check('the region view lists the regions', run('FB_MT.MT.perf.dim') === 'region' && run("salesPeriodView('metrics')").includes('Performance by region'));
  console.log(pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})();
