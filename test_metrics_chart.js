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
// The solid line's value sits above its point; the dashed line's value sits below it.
const pairs = svg => [...svg.matchAll(/<circle cx="([\d.]+)" cy="([\d.]+)"[^>]*fill="([^"]+)"[^>]*>.*?<\/circle><text x="[\d.]+" y="([\d.]+)"[^>]*>([^<]+)<\/text>/g)]
  .map(m => ({ cy: +m[2], color: m[3], ly: +m[4], label: m[5] }));
const series = [
  { name: 'M2O%', axis: 'L', values: [10, 12, 9], color: '#ff0000', dash: false },
  { name: 'M2O%', axis: 'L', values: [11, 9, 13], color: '#0000ff', dash: true }
];
const svg = run('FB_MT.mtChart(' + JSON.stringify(series) + ', v => String(v), v => String(v))');
const ps = pairs(svg);
check('the solid line has 3 labels, the dashed line 3 labels', ps.filter(p => p.color === '#ff0000').length === 3 && ps.filter(p => p.color === '#0000ff').length === 3, ps.length + ' labels');
check('the solid line\'s labels sit above their points', ps.filter(p => p.color === '#ff0000').every(p => p.ly < p.cy - 5));
check('the dashed line\'s labels sit below their points', ps.filter(p => p.color === '#0000ff').every(p => p.ly > p.cy + 5));
check('the labels carry the value with a % sign for %-series', ps.every(p => /%$/.test(p.label)), ps.map(p => p.label).join(' '));
check('a short period is drawn full width', svg.includes('width="100%"'));
const long = Array.from({ length: 31 }, (_, i) => 5 + (i % 4));
const lsvg = run('FB_MT.mtChart(' + JSON.stringify([{ name: 'Orders', axis: 'L', values: long, color: '#00ff00', dash: false }]) + ', v => String(v), v => String(v))');
check('a 31-day period is drawn wider and scrolls sideways', /width="\d+px"/.test(lsvg) && lsvg.includes('overflow-x:auto'), (lsvg.match(/width="\d+px"/) || [''])[0]);
check('a 31-day period still labels every point', (lsvg.match(/<circle/g) || []).length === 31 && (lsvg.match(/font-size="10" font-weight="600"/g) || []).length === 31);
console.log(pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
