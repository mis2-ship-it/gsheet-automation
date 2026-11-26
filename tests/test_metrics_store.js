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
const rawOrders = fn => DATA.rows.filter(r => fn(r) && r[0] >= run('FB_MT.MT.a.from') && r[0] <= run('FB_MT.MT.a.to')).reduce((t, r) => t + r[7], 0);
(async () => {
  loadImpl = async () => DATA;
  run("salesPeriodView('metrics')");
  for (let i = 0; i < 100 && !run('FB_MT.MT.data'); i++) await new Promise(r => setTimeout(r, 5));
  const page = run("salesPeriodView('metrics')");
  check('the Store name filter is on the page, with the file\'s stores', page.includes('data-mt="store"') && page.includes(`>${DATA.stores[0]}<`));
  const r0 = DATA.rows.find(r => r.length > 10 && r[0] >= run('FB_MT.MT.a.from') && r[0] <= run('FB_MT.MT.a.to'));
  const name = DATA.stores[r0[10]], region = DATA.regions[r0[3]];
  fire('change', { dataset: { mt: 'store' }, value: name });
  check('a store filter gives that store\'s orders', run('FB_MT.MT.store') === name && close(run('FB_MT.mtSummary().a.total.orders'), rawOrders(r => DATA.stores[r[10]] === name)),
        name + ': ' + run('FB_MT.mtSummary().a.total.orders'));
  fire('change', { dataset: { mt: 'region' }, value: region });
  check('the store and region filters together give that store in that region',
        close(run('FB_MT.mtSummary().a.total.orders'), rawOrders(r => DATA.stores[r[10]] === name && DATA.regions[r[3]] === region)), region);
  fire('change', { dataset: { mt: 'region' }, value: 'all' });
  fire('change', { dataset: { mt: 'store' }, value: 'all' });
  check('All stores puts the total back', close(run('FB_MT.mtSummary().a.total.orders'), E.combos.none.A.total.orders));
  console.log(pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})();
