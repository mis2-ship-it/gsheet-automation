// Top and bottom stores, and the growth tables with a date. Run from the repo root: node tests/test_metrics_growth.js
const fs = require('fs'), vm = require('vm'), path = require('path'), zlib = require('zlib');
let pass = 0, fail = 0;
const check = (n, c, x = '') => { c ? pass++ : fail++; console.log((c ? 'PASS ' : 'FAIL ') + n + (x ? '  [' + x + ']' : '')); };
const gzBuf = fs.readFileSync(path.join(__dirname, 'fixtures', 'metrics.json.gz'));
const DATA = JSON.parse(zlib.gunzipSync(gzBuf).toString('utf8'));
const handlers = {};
const store = new Map();
let loadImpl = async () => DATA;
const ctx = { console, Map, Set, Math, Number, String, Array, Object, JSON, Date, Intl, Infinity, isFinite, RegExp, Error, Promise, setTimeout,
  SUBS: { sales: [['live', 'Live'], ['daily', 'Daily']] }, renders: 0, num: n => Math.round(n).toLocaleString('en-IN'),
  loadGz: async u => loadImpl(u), render() { ctx.renders++; }, salesPeriodView: () => '', setPeriodBar() {},
  document: { addEventListener: (t, f) => { (handlers[t] = handlers[t] || []).push(f); }, getElementById: () => null, createElement: () => ({}), head: { appendChild() {} } },
  localStorage: { getItem: k => (store.has(k) ? store.get(k) : null), setItem: (k, v) => store.set(k, String(v)), removeItem: k => store.delete(k) },
  Blob, Response, DecompressionStream, TextDecoder, TextEncoder, btoa, atob };
ctx.window = ctx;
ctx.M = { sources: DATA.sources, brands: DATA.brands, regions: DATA.regions, storeTypes: DATA.storeTypes,
          stores: [...new Set(DATA.stores)].map((name, i) => ({ i, name, a: (i % 2) + 1 })), ams: { 1: 'Reazul', 2: 'Akash' } };
ctx.S = { store: 'all', type: 'all', brand: 'all', region: 'all', source: 'all', session: 'all' };
vm.createContext(ctx);
vm.runInContext(fs.readFileSync(path.join(__dirname, '..', 'site', 'metrics.js'), 'utf8'), ctx);
const run = s => vm.runInContext(s, ctx);
const fire = (type, target) => (handlers[type] || []).forEach(f => f({ target }));
(async () => {
  run("salesPeriodView('metrics')");
  for (let i = 0; i < 100 && !run('FB_MT.MT.data'); i++) await new Promise(r => setTimeout(r, 5));
  const A = run('FB_MT.MT.a'), B = run('FB_MT.MT.b');
  // top and bottom stores: the expected top store from the raw rows, with at least 10 orders in both periods
  const sum = (name, from, to) => DATA.rows.filter(r => DATA.stores[r[10]] === name && r[0] >= from && r[0] <= to).reduce((t, r) => t + r[7], 0);
  const ch = [...new Set(DATA.stores)].map(n => ({ n, a: sum(n, A.from, A.to), b: sum(n, B.from, B.to) }))
    .filter(x => x.a >= 10 && x.b >= 10).map(x => ({ ...x, c: (x.a / x.b - 1) * 100 })).sort((x, y) => y.c - x.c);
  let html = run("salesPeriodView('metrics')");
  check('the top and bottom stores are on the page, top 5 and bottom 5', html.includes('Top and bottom stores') && html.includes('>Top 5<') && html.includes('>Bottom 5<'));
  const topRows = html.split('>Top 5<')[1].split('>Bottom 5<')[0].split('<tr>').length - 1;
  check('the top list has at most 5 stores', topRows >= 1 && topRows <= 6, String(topRows));
  check('the store with the best growth is the first of the top list', ch.length > 0 && html.split('>Top 5<')[1].includes(ch[0].n), ch.length ? ch[0].n : 'none');
  // the growth tables: the day, the comparisons, the month to date
  const end = run('FB_MT.mtgDataEnd()');
  check('the growth day defaults to the last day of the data (22 Nov)', end === run('FB_MT.mtgDayOf(2026, 11, 22)'));
  const brand0 = DATA.brands[0];
  const dayOrders = (from, to) => DATA.rows.filter(r => DATA.brands[r[2]] === brand0 && r[0] >= from && r[0] <= to).reduce((t, r) => t + r[7], 0);
  const got = (from, to) => run(`(FB_MT.mtPerfTotals('brand', ${from}, ${to}).get(${JSON.stringify(brand0)}) || { orders: 0 }).orders`);
  check('the day column is the orders of that day', got(end, end) === dayOrders(end, end));
  check('the WoW comparison is the same day last week', got(end - 7, end - 7) === dayOrders(end - 7, end - 7));
  const ms = run(`FB_MT.mtgMonthStart(${end})`);
  check('the month to date runs from the 1st to the day', got(ms, end) === dayOrders(ms, end), `${got(ms, end)} vs ${dayOrders(ms, end)}`);
  const pm = run(`FB_MT.mtgPrevMonthSameDay(${end})`);
  check('MoM goes to the same day of last month (22 Nov goes to 22 Oct)', pm === run('FB_MT.mtgDayOf(2026, 10, 22)'));
  html = run("salesPeriodView('metrics')");
  check('the growth tables are on the page for brand, region and area manager',
        html.includes('>Brand<') && html.includes('>Region<') && html.includes('>Area manager<') && html.includes('MTD orders'));
  check('before the data starts, the MoM shows a dash and a note', html.includes('The data starts') && html.includes('<td>—</td>'));
  // picking a date changes the day
  fire('change', { dataset: { mtgDay: '1' }, value: '2026-11-15' });
  check('picking a date changes the growth day', run('FB_MT.MT.gr.day') === run('FB_MT.mtgDayOf(2026, 11, 15)'));
  check('the page follows the picked day', run("salesPeriodView('metrics')").includes('value="2026-11-15"'));
  console.log(pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})();
