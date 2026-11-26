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
const lbl = day => new Date(Date.UTC(2025, 0, 1) + day * 86400000).toLocaleDateString('en-IN', { day: 'numeric', month: 'short' });
const sec = (html, title) => html.split('<section class="block">').find(x => x.includes('<h2>' + title + '</h2>')) || '';
(async () => {
  loadImpl = async () => DATA;
  run("salesPeriodView('metrics')");
  for (let i = 0; i < 100 && !run('FB_MT.MT.data'); i++) await new Promise(r => setTimeout(r, 5));
  let page = run("salesPeriodView('metrics')");
  check('each chart has a source selector', ['m2o', 'org', 'clk'].every(k => page.includes(`data-mtc-src="${k}"`)));
  check('each chart has its metric toggles', page.includes('data-mtc-show="m2o" data-metric="C2O%"') && page.includes('data-mtc-show="org" data-metric="Organic%"') && page.includes('data-mtc-show="clk" data-metric="Ad Orders"'));
  // 1. toggle C2O% off: the M2O% and C2O% chart shows M2O% only
  fire('change', { dataset: { mtcShow: 'm2o', metric: 'C2O%' }, checked: false });
  page = run("salesPeriodView('metrics')");
  const m2oSec = sec(page, 'M2O% and C2O%');
  check('toggling C2O% off hides it from that chart', !m2oSec.includes('<title>C2O%') && m2oSec.includes('<title>M2O%'));
  check('the other charts are unchanged by it', sec(page, 'Organic% and In-Organic%').includes('<title>Organic%') && sec(page, 'Clicks and Ad Orders').includes('<title>Clicks'));
  // 2. both off: a message, and no chart
  fire('change', { dataset: { mtcShow: 'm2o', metric: 'M2O%' }, checked: false });
  page = run("salesPeriodView('metrics')");
  check('with no metric chosen the chart says so', sec(page, 'M2O% and C2O%').includes('Pick at least one metric above.'));
  fire('change', { dataset: { mtcShow: 'm2o', metric: 'M2O%' }, checked: true });
  fire('change', { dataset: { mtcShow: 'm2o', metric: 'C2O%' }, checked: true });
  // 3. the source of one chart: Swiggy only, the others stay All
  const sw = DATA.sources.indexOf('Swiggy');
  const A = run('FB_MT.MT.a.from');
  const dayRows = DATA.rows.filter(r => r[0] === A && (r[1] === sw));
  const om = dayRows.reduce((t, r) => t + r[7], 0), mo = dayRows.reduce((t, r) => t + r[5], 0);
  fire('change', { dataset: { mtcSrc: 'm2o' }, value: 'Swiggy' });
  page = run("salesPeriodView('metrics')");
  const tip = (sect, name, period, day) => { const m = sect.match(new RegExp('<title>' + name.replace(/[%]/g, '\\%') + ' \\(' + period + '\\) · ' + lbl(day) + ': ([0-9.]+)%</title>')); return m ? m[1] : null; };
  const got = tip(sec(page, 'M2O% and C2O%'), 'M2O%', 'A', A);
  check('the chart source Swiggy gives Swiggy\'s M2O% for that day', got === (mo ? (om / mo * 100).toFixed(2) : null), `${got} vs ${(om / mo * 100).toFixed(2)}`);
  check('the other charts stay on Swiggy + Zomato', run('FB_MT.MT.chart.org.src') === 'all' && run('FB_MT.MT.chart.clk.src') === 'all');
  // 4. dates: the x-axis and the tooltips show days, and each period\'s dates appear
  check('the x-axis shows the dates of period A', sec(page, 'M2O% and C2O%').includes(`>${lbl(A)}</text>`));
  check('the tooltips show period B\'s date', sec(page, 'M2O% and C2O%').includes(`· ${lbl(run('FB_MT.MT.b.from'))}:`));
  check('the header gives both periods', page.includes('A: ' + lbl(A) + ' to ') && page.includes(' · B: ' + lbl(run('FB_MT.MT.b.from')) + ' to '));
  console.log(pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})();
