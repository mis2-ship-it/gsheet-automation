// The compare view: Zomato by first code, codes under 5 orders left out, totals from the codes shown.
const fs = require('fs'), vm = require('vm'), path = require('path');
let pass = 0, fail = 0;
const check = (n, c, x = '') => { c ? pass++ : fail++; console.log((c ? 'PASS ' : 'FAIL ') + n + (x ? '  [' + x + ']' : '')); };
const CODE = path.join(__dirname, '..', 'apps-script', 'Code.gs');
const ctx = { console, Math, Date, Number, String, Array, Object, Map, Set, JSON, Infinity, isFinite, RegExp, Error,
  Utilities: { formatDate: () => '' }, Session: { getScriptTimeZone: () => 'Asia/Kolkata' }, SpreadsheetApp: {}, CacheService: {}, PropertiesService: {}, HtmlService: {} };
vm.createContext(ctx);
vm.runInContext(fs.readFileSync(CODE, 'utf8'), ctx);
const run = s => vm.runInContext(s, ctx);
const row = (d, source, label, inv, net) => ({ d, source, label, brand: 'Madno', region: 'KA', session: 'Lunch', inv, qty: inv, gross: net + 100, disc: 100, net, lines: inv });
const rows = [
  row('2026-11-17', 'Zomato', 'Gold Discount GOLD', 6, 540),
  row('2026-11-18', 'Zomato', 'Gold Discount GOLD + Menu Discount', 4, 360),
  row('2026-11-18', 'Zomato', 'TRYNEW', 3, 270),
  row('2026-11-10', 'Zomato', 'TRYNEW', 20, 1800),
  row('2026-11-19', 'Zomato', 'EATSAVE140 + Menu Discount', 5, 450),
  row('2026-11-19', 'Swiggy', '70% Off', 10, 300),
  row('2026-11-19', 'Swiggy', '70% Off + _', 3, 100),
  row('2026-11-19', 'Swiggy', 'Items under Rs X', 4, 300)
];
const res = run(`computeCompare_(${JSON.stringify(rows)}, ${JSON.stringify({ mode: 'week', periods: 2, anchor: '2026-11-22', metric: 'inv' })})`);
const platforms = Object.values(res).find(v => Array.isArray(v) && v.length && v[0] && v[0].source && v[0].types) || [];
const zomato = platforms.find(p => p.source === 'Zomato');
const swiggy = platforms.find(p => p.source === 'Swiggy');
check('the compare view finds both platforms', !!zomato && !!swiggy, Object.keys(res).join(','));
check('Zomato: vouchers by first code, shown when 5+ orders', zomato && zomato.types.map(t => t.type).join('|') === 'Gold Discount GOLD|EATSAVE140', zomato && zomato.types.map(t => t.type).join('|'));
check('Zomato: TRYNEW (3 this period) is left out', zomato && !zomato.types.some(t => t.type === 'TRYNEW'));
check('Zomato total is the vouchers shown (10 + 5 = 15 orders, latest week)', zomato && zomato.total[0] === 15, zomato && String(zomato.total[0]));
check('Swiggy: only the offer type with 5+ orders in the code list', swiggy && swiggy.types.map(t => t.type).join('|') === '70% Off' && swiggy.total[0] === 10, swiggy && swiggy.types.map(t => t.type + ':' + t.values[0]).join('|'));
console.log(pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
