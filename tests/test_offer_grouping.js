// Offer types: Zomato by first code, codes under 5 orders left out. Run from the repo root: node tests/test_offer_grouping.js
const fs = require('fs'), vm = require('vm'), path = require('path');
let pass = 0, fail = 0;
const check = (n, c, x = '') => { c ? pass++ : fail++; console.log((c ? 'PASS ' : 'FAIL ') + n + (x ? '  [' + x + ']' : '')); };
const CODE = path.join(__dirname, '..', 'apps-script', 'Code.gs');
const ctx = { console, Math, Date, Number, String, Array, Object, Map, Set, JSON, Infinity, isFinite, RegExp, Error,
  Utilities: { formatDate: () => '' }, Session: { getScriptTimeZone: () => 'Asia/Kolkata' }, SpreadsheetApp: {}, CacheService: {}, PropertiesService: {}, HtmlService: {} };
vm.createContext(ctx);
vm.runInContext(fs.readFileSync(CODE, 'utf8'), ctx);
const run = s => vm.runInContext(s, ctx);
const dn = d => run(`dn_(${JSON.stringify(d)})`);
const CUR = [dn('2026-11-16'), dn('2026-11-22')], PREV = [dn('2026-11-09'), dn('2026-11-15')];
const r = (d, source, label, inv, net) => ({ d, source, label, inv, qty: inv, gross: net + 100, disc: 100, net, lines: inv });
const rows = [
  r('2026-11-17', 'Zomato', 'Gold Discount GOLD', 6, 540),
  r('2026-11-18', 'Zomato', 'Gold Discount GOLD + Menu Discount', 4, 360),
  r('2026-11-18', 'Zomato', 'TRYNEW', 3, 270),
  r('2026-11-10', 'Zomato', 'TRYNEW', 20, 1800),                    // last period only: TRYNEW is 3 this period
  r('2026-11-19', 'Zomato', 'EATSAVE140 + Menu Discount', 5, 450),
  r('2026-11-19', 'Swiggy', '70% Off', 10, 300),
  r('2026-11-19', 'Swiggy', '70% Off + _', 3, 100),
  r('2026-11-19', 'Swiggy', 'Items under Rs X', 4, 300)
];
const offers = run(`offersBySource_(${JSON.stringify(rows)}, ${JSON.stringify(CUR)}, ${JSON.stringify(PREV)}, () => true)`);
check('first code: "Gold Discount GOLD + Menu Discount" counts under "Gold Discount GOLD"', run(`firstCode_("Gold Discount GOLD + Menu Discount")`) === 'Gold Discount GOLD');
check('first code: a single code is itself', run(`firstCode_("ZYUMFOOD")`) === 'ZYUMFOOD');
check('first code: a blank label is "No code"', run(`firstCode_("")`) === 'No code');
const z = offers.Zomato;
check('Zomato shows vouchers, not raw codes', z.map(o => o.type).join('|') === 'Gold Discount GOLD|EATSAVE140', z.map(o => o.type).join('|'));
const gold = z.find(o => o.type === 'Gold Discount GOLD');
check('the voucher totals include its codes (6 + 4 = 10 orders)', gold && gold.cur.inv === 10, gold && String(gold.cur.inv));
check('the codes listed under the voucher: only those with 5+ orders', gold && gold.labels.length === 1 && gold.labels[0].label === 'Gold Discount GOLD', gold && gold.labels.map(x => x.label).join(' ; '));
check('a voucher under 5 orders this period is left out (TRYNEW: 3, though 20 last period)', !z.some(o => o.type === 'TRYNEW'));
const sw = offers.Swiggy;
check('Swiggy: codes under 5 orders are not counted in their offer type', sw.length === 1 && sw[0].type === '70% Off' && sw[0].cur.inv === 10, sw.map(o => o.type + ':' + o.cur.inv).join(' | '));
check('Swiggy: a code under 5 orders is not listed under its offer type', sw[0].labels.length === 1 && sw[0].labels[0].label === '70% Off', sw[0].labels.map(x => x.label).join(' ; '));
check('Swiggy: an offer type made only of small codes disappears', !sw.some(o => o.type === 'Flat Rs Off' || o.type === 'Items under Rs X'));
console.log(pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
