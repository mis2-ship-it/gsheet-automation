const fs = require('fs'), vm = require('vm'), zlib = require('zlib');
let pass = 0, fail = 0;
const check = (n, c, x = '') => { c ? pass++ : fail++; console.log((c ? 'PASS ' : 'FAIL ') + n + (x ? '  [' + x + ']' : '')); };
const path = require('path');
const FIX = path.join(__dirname, 'fixtures');
const DATA = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(FIX, 'metrics.json.gz'))).toString('utf8'));
const E = JSON.parse(fs.readFileSync(path.join(FIX, 'metrics_expected.json'), 'utf8'));
const ctx = { console, Map, Set, Math, Number, String, Array, Object, JSON, Date, Intl, Infinity, isFinite, RegExp, Error, Promise, setTimeout,
  SUBS: { sales: [['live', 'Live'], ['daily', 'Daily']] }, renders: 0, num: n => Math.round(n).toLocaleString('en-IN'),
  loadGz: async () => DATA, render() { ctx.renders++; },
  salesPeriodView: k => 'BASE:' + k, setPeriodBar() {},
  document: { addEventListener() {}, getElementById: () => null, createElement: () => ({}), head: { appendChild() {} } } };
ctx.window = ctx;
vm.createContext(ctx);
vm.runInContext(fs.readFileSync(path.join(__dirname, '..', 'site', 'metrics.js'), 'utf8'), ctx);
const run = s => vm.runInContext(s, ctx);
const close = (x, y, tol = 1e-6) => (x === null || x === undefined || y === null || y === undefined) ? (x === y) : Math.abs(x - y) <= tol * Math.max(1, Math.abs(y));
(async () => {
  const first = run("salesPeriodView('metrics')");
  check('the Metrics tab shows a loading note first', first.includes('Loading the metrics'));
  check('other tabs are left alone', run("salesPeriodView('daily')") === 'BASE:daily');
  check('Metrics is a Sales tab', run("SUBS.sales.map(x => x[0]).join(',')").endsWith('metrics'));
  for (let i = 0; i < 200 && !run('FB_MT.MT.data'); i++) await new Promise(r => setTimeout(r, 5));
  check('the default periods: A = the last 7 days, B = the 7 days before', run('FB_MT.MT.a.to') - run('FB_MT.MT.a.from') === 6 && run('FB_MT.MT.b.to') === run('FB_MT.MT.a.from') - 1);
  const combos = { none: {}, zomato_ka: { source: 'Zomato', region: 'KA' }, swiggy_coco: { source: 'Swiggy', type: 'COCO' } };
  for (const [name, f] of Object.entries(combos)) {
    run(`FB_MT.MT.source = '${f.source || 'all'}'; FB_MT.MT.region = '${f.region || 'all'}'; FB_MT.MT.type = '${f.type || 'all'}'; FB_MT.MT.brand = 'all'`);
    const s = run('FB_MT.mtSummary()');
    const want = E.combos[name];
    for (const pk of ['a', 'b']) {
      const P = pk === 'a' ? 'A' : 'B';
      const t = s[pk].total, w = want[P].total;
      const ok = ['menuOpen', 'orders', 'clicks', 'adOrders'].every(k => close(t[k], w[k]));
      check(`${name}: comparison ${P} totals match the raw tabs (menu, orders, clicks, ad orders)`, ok, JSON.stringify([t.menuOpen, w.menuOpen, t.orders, w.orders]));
      check(`${name}: comparison ${P} M2O%, C2O%, In-Organic%, Organic% match`, close(t.m2o, w.m2o, 1e-9) && close(t.c2o, w.c2o, 1e-9) && close(t.inOrg, w.inOrg, 1e-9) && close(t.org, w.org, 1e-9));
      const daysOk = s[pk].days.every((d, i) => close(d.orders, want[P].ordersByDay[i]));
      check(`${name}: comparison ${P} day by day orders match`, daysOk && s[pk].days.length === 7);
    }
    check(`${name}: Organic% = 100 - In-Organic%`, close(s.a.total.org, 100 - s.a.total.inOrg, 1e-9));
  }
  run("FB_MT.MT.source = 'all'; FB_MT.MT.region = 'all'; FB_MT.MT.type = 'all'");
  const page = run("salesPeriodView('metrics')");
  check('the page shows comparison A and B, the three charts and both day tables', page.includes('Comparison A and B') && page.includes('M2O% and C2O%') && page.includes('Organic% and In-Organic%') && page.includes('Clicks and Ad Orders') && page.includes('Comparison A by day') && page.includes('Comparison B by day'));
  check('the charts draw both periods (solid for A, dashed for B)', page.includes('stroke-dasharray') && (page.match(/<path /g) || []).length >= 9);
  check('the day tables have the columns asked for', ['Menu Open', 'Orders', 'M2O%', 'C2O%', 'Clicks', 'Ad Orders', 'Organic%', 'In-Organic%'].every(h => page.includes('<th>' + h + '</th>')));
  check('the filters offer Swiggy and Zomato for the source', page.includes('value="Swiggy"') && page.includes('value="Zomato"'));
  console.log(pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})();
