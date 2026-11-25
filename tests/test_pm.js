const fs = require('fs'), vm = require('vm'), zlib = require('zlib');
let pass = 0, fail = 0;
const check = (n, c, x = '') => { c ? pass++ : fail++; console.log((c ? 'PASS ' : 'FAIL ') + n + (x ? '  [' + x + ']' : '')); };
const PMD = JSON.parse(zlib.gunzipSync(fs.readFileSync('/tmp/pm/product_mix_test.json.gz')).toString('utf8'));
const E = JSON.parse(fs.readFileSync('/tmp/pm/pm_expected.json', 'utf8'));
const EPOCH_MS = Date.UTC(2025, 0, 1);
const fmtDay = d => new Date(EPOCH_MS + d * 86400000).toLocaleDateString('en-IN', { day: 'numeric', month: 'short', timeZone: 'UTC' });
const ctx = { console, Map, Set, Math, Number, String, Array, Object, JSON, Date, Intl, Infinity, isFinite, RegExp, Error, Promise, setTimeout,
  M: { brands: ['Madno', 'Frozen Bottle'], regions: ['KA', 'TN'], sources: ['In Store', 'Swiggy', 'Zomato', 'Ownly', 'Dunzo'] },
  S: { store: 'all', type: 'all', brand: 'all', region: 'all', source: 'all', session: 'all' },
  num: n => Math.round(n).toLocaleString('en-IN'), fmtDay,
  FB_SALES_CH: { value: 'all' }, renders: 0,
  loadGz: async () => PMD, render() { ctx.renders++; },
  salesPeriodView: k => 'BASE:' + k, setPeriodBar() {} };
ctx.window = ctx;
vm.createContext(ctx);
vm.runInContext(fs.readFileSync('/tmp/pm/productmix.js', 'utf8'), ctx);
const run = s => vm.runInContext(s, ctx);
(async () => {
  const first = run("salesPeriodView('morning')");
  check('the product mix shows its loading note first', first.includes('Product mix: week on week') && first.includes('Loading the product mix'));
  check('other tabs are left alone', run("salesPeriodView('daily')") === 'BASE:daily');
  for (let i = 0; i < 100 && !run('FB_PM.PM.data'); i++) await new Promise(r => setTimeout(r, 5));
  const c = run('FB_PM.pmCompute()');
  check('the reference day is the latest day (14 Nov 2026), against 7 Nov', c.refDay - c.prevDay === 7);
  check('total net sales for the day match the separate calculation (all sources)', c.totCur === E.all_totals.cur, `${c.totCur} vs ${E.all_totals.cur}`);
  check('total net sales for the week before match (all sources)', c.totPrev === E.all_totals.prev, `${c.totPrev} vs ${E.all_totals.prev}`);
  const byName = Object.fromEntries(c.rows.map(r => [r.name, r]));
  const perCat = Object.keys(E.all.cur).every(k => byName[k] && byName[k].cur === E.all.cur[k] && byName[k].prev === E.all.prev[k]);
  check('each category matches the separate calculation (day and week before)', perCat);
  check('the mix adds up to 100% for the day', Math.abs(c.rows.reduce((t, r) => t + r.mixCur, 0) - 100) < 0.01);
  check('the total week on week growth is right', Math.abs(c.totWow - (E.all_totals.cur / E.all_totals.prev * 100 - 100)) < 1e-6);
  // channel bar: Offline
  run("FB_SALES_CH.value = 'offline'");
  const off = run('FB_PM.pmCompute()');
  check('Offline on the channel bar: In Store only', off.totCur === E.offline.cur_total && off.totPrev === E.offline.prev_total, `${off.totCur}`);
  run("FB_SALES_CH.value = 'all'");
  // Sales filters: brand Madno, region KA
  run("S.brand = 0; S.region = 0");
  const kam = run('FB_PM.pmCompute()');
  check('the Sales brand and region filters apply to the product mix', kam.totCur === E.ka_madno.cur_total && kam.totPrev === E.ka_madno.prev_total, `${kam.totCur} vs ${E.ka_madno.cur_total}`);
  run("S.brand = 'all'; S.region = 'all'");
  const page = run("salesPeriodView('morning')");
  check('the product mix shows on the Morning Call with its categories', page.includes('Product mix: week on week') && page.includes('Thickshakes') && page.includes('Mix %'));
  console.log(pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})();
