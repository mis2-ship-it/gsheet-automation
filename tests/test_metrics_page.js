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
const rawOrders = (fn) => DATA.rows.filter(r => fn(r) && r[0] >= run('FB_MT.MT.a.from') && r[0] <= run('FB_MT.MT.a.to')).reduce((t, r) => t + r[7], 0);
(async () => {
  loadImpl = async () => DATA;
  run("salesPeriodView('metrics')");
  for (let i = 0; i < 100 && !run('FB_MT.MT.data'); i++) await new Promise(r => setTimeout(r, 5));
  // 1. the Sales tab's source and region filters
  ctx.S.source = String(DATA.sources.indexOf('Zomato')); ctx.S.region = String(DATA.regions.indexOf('KA'));
  check('the Sales tab source and region filters apply to the Metrics totals (Zomato, KA)',
        close(run('FB_MT.mtSummary().a.total.orders'), E.combos.zomato_ka.A.total.orders), run('FB_MT.mtSummary().a.total.orders') + ' vs ' + E.combos.zomato_ka.A.total.orders);
  ctx.S.source = 'all'; ctx.S.region = 'all';
  // 2. the Sales tab's store type (a name) and source (Swiggy, by index)
  ctx.S.type = 'COCO'; ctx.S.source = String(DATA.sources.indexOf('Swiggy'));
  check('the Sales tab store type and source apply (Swiggy, COCO)', close(run('FB_MT.mtSummary().a.total.orders'), E.combos.swiggy_coco.A.total.orders));
  ctx.S.type = 'all'; ctx.S.source = 'all';
  // 3. the Sales tab's store filter: the store's own orders, from the raw rows
  const sName = DATA.stores.find((s, i) => DATA.rows.some(r => r[10] === i && r[0] >= run('FB_MT.MT.a.from') && r[0] <= run('FB_MT.MT.a.to')));
  const sIdx = storesList.indexOf(sName);
  ctx.S.store = String(sIdx);
  const storeOrders = rawOrders(r => DATA.stores[r[10]] === sName);
  check('the Sales tab store filter gives that store only (' + sName + ')', close(run('FB_MT.mtSummary().a.total.orders'), storeOrders), run('FB_MT.mtSummary().a.total.orders') + ' vs ' + storeOrders);
  ctx.S.store = 'all';
  // 4. the Metrics own filter changes on a change event
  const before = run('FB_MT.mtSummary().a.total.orders');
  fire('change', { dataset: { mt: 'region' }, value: DATA.regions[DATA.regions.indexOf('TN')] || 'TN' });
  const tnOrders = rawOrders(r => DATA.regions[r[3]] === (DATA.regions[DATA.regions.indexOf('TN')] || 'TN'));
  check('a change on the region filter changes the totals', run('FB_MT.MT.region') !== 'all' && run('FB_MT.mtSummary().a.total.orders') === tnOrders, `${before} -> ${run('FB_MT.mtSummary().a.total.orders')} vs ${tnOrders}`);
  check('the page is redrawn after a change', ctx.renders > 0);
  fire('change', { dataset: { mt: 'region' }, value: 'all' });
  // 5. the charts show the values on every point, with a tooltip
  const page = run("salesPeriodView('metrics')");
  check('each chart point has a value label and a tooltip', page.includes('<title>') && page.includes('<circle') && /font-size="10"[^>]*>[0-9.,]+%?</.test(page));
  check('the tooltip gives the series, the period and the value', /<title>M2O% \(A\) · Day 1: [0-9.]+%<\/title>/.test(page));
  // 6. the uploaded file is kept in this browser across a reload
  run("FB_MT.MT.data = null; FB_MT.MT.error = null; FB_MT.MT.savedName = null"); store.clear();
  loadImpl = async () => { throw new TypeError('Failed to fetch'); };
  run('FB_MT.mtLoad()'); await new Promise(r => setTimeout(r, 20));
  check('no server file and nothing saved: the page asks for the file', run('FB_MT.MT.data') === null && /could not reach/.test(run('FB_MT.MT.error')));
  const file = { name: 'metrics.json.gz', arrayBuffer: async () => gzBuf.buffer.slice(gzBuf.byteOffset, gzBuf.byteOffset + gzBuf.byteLength) };
  await run('FB_MT.mtPickFile(FB_FILE)', ctx.FB_FILE = file) ;
  for (let i = 0; i < 100 && !store.get('fb-metrics-file'); i++) await new Promise(r => setTimeout(r, 5));
  check('the uploaded file is saved in this browser', store.has('fb-metrics-file'));
  // a reload: the server has no file, but the saved one is used
  run("FB_MT.MT.data = null; FB_MT.MT.error = null; FB_MT.MT.savedName = null");
  run('FB_MT.mtLoad()'); await new Promise(r => setTimeout(r, 50));
  check('after a reload the saved file is used', run('FB_MT.MT.data') !== null && run('FB_MT.MT.savedName') === 'metrics.json.gz', run('FB_MT.MT.error') || '');
  check('after a reload the totals are back (none filter, same as the file)', close(run('FB_MT.mtSummary().a.total.orders'), E.combos.none.A.total.orders));
  check('the tab says the file is the one saved in this browser, with a Remove button', run("salesPeriodView('metrics')").includes('saved in this browser') && run("salesPeriodView('metrics')").includes('data-mt-forget'));
  fire('click', { closest: sel => (sel === '[data-mt-forget]' ? {} : null) });
  check('Remove deletes the saved file', !store.has('fb-metrics-file') && run('FB_MT.MT.data') === null);
  console.log(pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})();
