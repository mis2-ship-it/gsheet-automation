const fs = require('fs'), vm = require('vm');
const ctx = { console, SpreadsheetApp: {}, HtmlService: {}, Utilities: { formatDate: (d) => '' }, Session: { getScriptTimeZone: () => 'Asia/Kolkata' } };
vm.createContext(ctx);
vm.runInContext(fs.readFileSync('/tmp/disc/work/Code.gs', 'utf8'), ctx);
let pass = 0, fail = 0;
const check = (n, c, x = '') => { c ? pass++ : fail++; console.log((c ? 'PASS ' : 'FAIL ') + n + (x ? '  [' + x + ']' : '')); };
const row = (month, day, net, cat = 'Thickshakes', ch = 'Offline') => ({ month, day, brand: 'Madno', region: 'KA', source: 'In Store', channel: ch, cat, inv: 1, qty: 1, gross: net + 10, disc: 10, net });
const rows = [
  row('2026-11', '2026-11-05', 1000), row('2026-11', '2026-11-20', 2000), row('2026-11', '2026-11-23', 3000), row('2026-11', '2026-11-24', 9000),
  row('2026-10', '2026-10-05', 100), row('2026-10', '2026-10-23', 200), row('2026-10', '2026-10-24', 900), row('2026-10', '2026-10-31', 800),
  row('2026-09', '2026-09-10', 50), row('2026-09', '2026-09-25', 700),
  row('2026-08', '2026-08-22', 30), row('2026-08', '2026-08-30', 600),
  row('2026-10', '', 5),                                  // an older row with no day: kept for the whole month
];
const all = rows.map(r => Object.assign({}, r, { cat: '(all categories)' }));
const p = { anchor: '2026-11-23', brand: 'all', region: 'all', source: 'all', session: 'all' };
const res = vm.runInContext(`computeCategoryMonths_(${JSON.stringify(rows.concat(all))}, ${JSON.stringify(p)})`, ctx);
check('the spans show the same days in every month', res.months.map(m => m.span).join(' | ') === '1–23 Nov | 1–23 Oct | 1–23 Sep | 1–23 Aug', res.months.map(m => m.span).join(' | '));
const netOf = i => res.totals.all[i].net;
check('current month (Nov): only the days up to 23 Nov count', netOf(0) === 1000 + 2000 + 3000, String(netOf(0)));
check('last month (Oct): only 1–23 Oct count, plus the older no-day row', netOf(1) === 100 + 200 + 5, String(netOf(1)));
check('last 2 months (Sep): only 1–23 Sep counts', netOf(2) === 50, String(netOf(2)));
check('last 3 months (Aug): only 1–23 Aug counts', netOf(3) === 30, String(netOf(3)));
const thick = res.categories.find(c => c.cat === 'Thickshakes');
check('the category row uses the same cut', thick && thick.cells.all[1].net === 305 && thick.cells.all[0].net === 6000, JSON.stringify(thick && thick.cells.all.map(x => x.net)));
// the product monthly spans are unchanged (full months)
const pm = vm.runInContext(`monthSpan_('2026-10', '2026-11-23')`, ctx);
check('the product monthly spans are not changed', pm === '1–31 Oct', pm);
console.log(pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
