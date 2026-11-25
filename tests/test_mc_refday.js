// The Morning Call reference day is yesterday (the last completed business day, cut-off 05:00 IST).
const fs = require('fs'), vm = require('vm'), path = require('path');
let pass = 0, fail = 0;
const check = (n, c, x = '') => { c ? pass++ : fail++; console.log((c ? 'PASS ' : 'FAIL ') + n + (x ? '  [' + x + ']' : '')); };
const EPOCH_MS = Date.UTC(2025, 0, 1);
const dayOf = (y, m, d) => Math.round((Date.UTC(y, m - 1, d) - EPOCH_MS) / 86400000);
const ctx = { console, Map, Set, Math, Number, String, Array, Object, JSON, Date, Intl, Infinity, isFinite, RegExp, Error,
  SUBS: { sales: [] }, M: { last_day: 700, stores: [], ams: [], brands: [], sources: [], regions: [] }, S: {}, ROLL: {}, STORE_ROWS: new Map(),
  document: { addEventListener() {}, getElementById: () => null, createElement: () => ({}), head: { appendChild() {} } },
  render() {}, salesPeriodView: () => '', setPeriodBar() {}, loadGz: async () => ({}) };
ctx.window = ctx;
vm.createContext(ctx);
vm.runInContext(fs.readFileSync(path.join(__dirname, '..', 'site', 'morning.js'), 'utf8'), ctx);
const run = s => vm.runInContext(s, ctx);
delete ctx.FB_MC_REF;
const at = iso => { ctx.FB_MC_NOW = iso; return run('FB_MORNING.mcRefDay()'); };
// Wed 25 Nov 2026, 10:00 IST: yesterday is Tue 24 Nov
check('at 10:00 on 25 Nov (IST), the reference day is 24 Nov', at('2026-11-25T10:00:00+05:30') === dayOf(2026, 11, 24));
// 03:00 on 25 Nov is still the business day of 24 Nov, so the reference is 23 Nov
check('at 03:00 on 25 Nov, before the 05:00 cut-off, the reference day is 23 Nov', at('2026-11-25T03:00:00+05:30') === dayOf(2026, 11, 23));
// 05:00 exactly starts the new business day
check('at 05:00 on 25 Nov, the reference day is 24 Nov', at('2026-11-25T05:00:00+05:30') === dayOf(2026, 11, 24));
// the clock in UTC is read in IST: 22:00 UTC on 24 Nov is 03:30 IST on 25 Nov
check('the time is read in IST (22:00 UTC 24 Nov is 03:30 IST 25 Nov, so 23 Nov)', at('2026-11-24T22:00:00Z') === dayOf(2026, 11, 23));
// never after the last day of data
ctx.M.last_day = dayOf(2026, 11, 22);
check('never later than the last day of data', at('2026-11-25T10:00:00+05:30') === dayOf(2026, 11, 22));
ctx.M.last_day = 700;
ctx.FB_MC_REF = 682;
check('a fixed reference day (for tests) overrides the clock', run('FB_MORNING.mcRefDay()') === 682);
console.log(pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
