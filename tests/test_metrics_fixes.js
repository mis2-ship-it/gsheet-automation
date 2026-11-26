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
const clickT = (key, m) => (handlers.click || []).forEach(f => f({ target: { closest: sel => (sel === '[data-mtc-toggle]' ? { dataset: { mtcToggle: key, metric: m } } : null) } }));
(async () => {
  loadImpl = async () => DATA;
  run("salesPeriodView('metrics')");
  for (let i = 0; i < 100 && !run('FB_MT.MT.data'); i++) await new Promise(r => setTimeout(r, 5));
  // 1. a click on a metric button hides it; a second click shows it again, and the page shows that
  clickT('m2o', 'C2O%');
  let page = run("salesPeriodView('metrics')");
  check('one click hides C2O% (the button is off, the line is gone)', run('FB_MT.MT.chart.m2o.show["C2O%"]') === false && page.includes('class="mt-chip" aria-pressed="false" data-mtc-toggle="m2o" data-metric="C2O%"'));
  clickT('m2o', 'C2O%');
  page = run("salesPeriodView('metrics')");
  check('a second click shows it again', run('FB_MT.MT.chart.m2o.show["C2O%"]') === true && page.includes('class="mt-chip on" aria-pressed="true" data-mtc-toggle="m2o" data-metric="C2O%"'));
  // 2. the store filter is there even when the file has no store names
  const saved = run('FB_MT.MT.data.stores');
  run('FB_MT.MT.data.stores = undefined');
  page = run("salesPeriodView('metrics')");
  check('the store filter is on the page with no store names in the file, with the reason', page.includes('data-mt="store"') && page.includes('This file has no store names'));
  run('FB_MT.MT.data.stores = ' + JSON.stringify(saved));
  page = run("salesPeriodView('metrics')");
  check('with store names, the filter lists the stores and no reason shows', page.includes('data-mt="store"') && !page.includes('This file has no store names'));
  // 3. the date range in the file, and a warning for a period outside it
  const lo = run('FB_MT.MT.data.dateMin'), hi = run('FB_MT.MT.data.dateMax');
  check('the file\'s dates are shown', page.includes('Data in this file:') && page.includes(' to '), lo + ' to ' + hi);
  check('no warning while both periods are inside the data', !page.includes('is partly outside the data'));
  run('FB_MT.MT.a.from = ' + Math.round((Date.UTC(2026, 9, 29) - Date.UTC(2025, 0, 1)) / 86400000));
  page = run("salesPeriodView('metrics')");
  check('a period starting before the data gets a warning', page.includes('Comparison A (') && page.includes('is partly outside the data in this file'));
  console.log(pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})();
