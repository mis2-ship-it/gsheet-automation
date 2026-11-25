// Category product rows: last 3 months full, last two months cut to the same day, top 10 items named.
const fs = require('fs'), vm = require('vm'), path = require('path');
let pass = 0, fail = 0;
const check = (n, c, x = '') => { c ? pass++ : fail++; console.log((c ? 'PASS ' : 'FAIL ') + n + (x ? '  [' + x + ']' : '')); };
const CODE = path.join(__dirname, '..', 'apps-script', 'Code.gs');
const ctx = { console, Map, Set, Math, Number, String, Array, Object, JSON, Date, Intl, Infinity, isFinite, RegExp, Error,
  SpreadsheetApp: {}, HtmlService: {}, Utilities: { formatDate: () => '' }, Session: { getScriptTimeZone: () => 'Asia/Kolkata' } };
vm.createContext(ctx);
vm.runInContext(fs.readFileSync(CODE, 'utf8'), ctx);
const row = (month, day, item, net, channel = 'Online') => ({ month, day, brand: 'Madno', region: 'KA',
  source: channel === 'Offline' ? 'In Store' : 'Swiggy', channel, cat: 'Thickshakes', item, inv: 1, qty: 1,
  gross: net + 10, disc: 10, net });
const rows = [];
for (let i = 1; i <= 12; i++) {
  const name = 'Item ' + String(i).padStart(2, '0');
  const n = i * 100;
  rows.push(row('2026-11', '2026-11-10', name, n), row('2026-11', '2026-11-24', name, n));   // day 24 is after the 23rd
  rows.push(row('2026-10', '2026-10-05', name, n), row('2026-10', '2026-10-24', name, n));   // cut: 24 Oct is out
  rows.push(row('2026-09', '2026-09-10', name, n), row('2026-09', '2026-09-25', name, n));   // cut: 25 Sep is out
  rows.push(row('2026-08', '2026-08-22', name, n), row('2026-08', '2026-08-30', name, n));   // full month: both in
}
rows.push(row('2026-11', '2026-11-10', 'Item 12', 50, 'Offline'));   // an offline order
const p = { anchor: '2026-11-23', brand: 'all', region: 'all', source: 'all', session: 'all' };
const cm = vm.runInContext(`attachCategoryItems_(computeCategoryMonths_(${JSON.stringify(rows)}, ${JSON.stringify(p)}), ${JSON.stringify(rows)}, ${JSON.stringify(p)})`, ctx);
check('the spans: the last two months to the 23rd, the last 3 months full', cm.months.map(m => m.span).join(' | ') === '1–23 Nov | 1–23 Oct | 1–23 Sep | 1–31 Aug', cm.months.map(m => m.span).join(' | '));
const cat = cm.categories[0];
// sum of 1..12 times 100 = 7800; the offline 50 is added to November
check('current month (to the 23rd): 7,850 (includes the offline 50)', cat.cells.all[0].net === 7850, String(cat.cells.all[0].net));
check('last month: cut to the 23rd, so 24 Oct is not counted', cat.cells.all[1].net === 7800, String(cat.cells.all[1].net));
check('last 2 months: cut to the 23rd, so 25 Sep is not counted', cat.cells.all[2].net === 7800, String(cat.cells.all[2].net));
check('last 3 months: full month, both days counted', cat.cells.all[3].net === 15600, String(cat.cells.all[3].net));
check('a category opens to 10 named items and one "(other items)" row', cat.items.length === 11 && cat.items[10].name === '(other items)', String(cat.items.length));
check('the top item is the largest by net sales this month (Item 12)', cat.items[0].name === 'Item 12', cat.items[0].name);
check('the other items are the two smallest (Item 01 and 02): 300 this month', cat.items[10].cells.all[0].net === 300, String(cat.items[10].cells.all[0].net));
let recon = true;
for (let i = 0; i < 4; i++) {
  const s = cat.items.reduce((t, it) => t + it.cells.all[i].net, 0);
  if (s !== cat.cells.all[i].net) recon = false;
}
check('the items add up to the category, in every month', recon);
check('the offline sales of the top item are kept under offline', cat.items[0].cells.offline[0].net === 50 && cat.items[0].cells.online[0].net === 1200, JSON.stringify([cat.items[0].cells.offline[0].net, cat.items[0].cells.online[0].net]));
console.log(pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
