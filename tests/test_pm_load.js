const fs = require('fs'), vm = require('vm'), zlib = require('zlib');
let pass = 0, fail = 0;
const check = (n, c, x = '') => { c ? pass++ : fail++; console.log((c ? 'PASS ' : 'FAIL ') + n + (x ? '  [' + x + ']' : '')); };
const PMD = JSON.parse(zlib.gunzipSync(fs.readFileSync(require('path').join(__dirname, 'fixtures', 'product_mix.json.gz'))).toString('utf8'));
const E = JSON.parse(fs.readFileSync(require('path').join(__dirname, 'fixtures', 'pm_expected.json'), 'utf8'));
let loadImpl = async () => PMD;
const EPOCH_MS = Date.UTC(2025, 0, 1);
const ctx = { console, Map, Set, Math, Number, String, Array, Object, JSON, Date, Intl, Infinity, isFinite, RegExp, Error, Promise, setTimeout,
  M: { brands: ['Madno', 'Frozen Bottle'], regions: ['KA', 'TN'], sources: ['In Store', 'Swiggy', 'Zomato', 'Ownly', 'Dunzo'] },
  S: { store: 'all', type: 'all', brand: 'all', region: 'all', source: 'all', session: 'all' },
  num: n => Math.round(n).toLocaleString('en-IN'), fmtDay: d => new Date(EPOCH_MS + d * 86400000).toLocaleDateString('en-IN', { day: 'numeric', month: 'short', timeZone: 'UTC' }),
  FB_SALES_CH: { value: 'all' }, renders: 0, loadGz: async () => loadImpl(), render() { ctx.renders++; },
  salesPeriodView: k => 'BASE:' + k, setPeriodBar() {},
  document: { addEventListener() {} }, Blob, Response, DecompressionStream, TextDecoder, TextEncoder };
ctx.window = ctx;
vm.createContext(ctx);
vm.runInContext(fs.readFileSync(require('path').join(__dirname, '..', 'site', 'productmix.js'), 'utf8'), ctx);
const run = s => vm.runInContext(s, ctx);
(async () => {
  // a failed fetch
  loadImpl = async () => { throw new TypeError('Failed to fetch'); };
  run('FB_PM.PM.data = null; FB_PM.PM.error = null; FB_PM.PM.loading = false');
  run("salesPeriodView('morning')"); await new Promise(r => setTimeout(r, 20));
  const errA = run('FB_PM.PM.error');
  check('a failed fetch says the file could not be reached, with what to do', errA && errA.includes('could not reach data/product_mix.json.gz') && /load the file below/i.test(errA), errA);
  check('the Morning Call offers the product mix file control when there is no data', run("salesPeriodView('morning')").includes('data-pm-file="1"'));
  // upload the file (gzip), as the page would
  const gz = fs.readFileSync(require('path').join(__dirname, 'fixtures', 'product_mix.json.gz'));
  run('FB_PM.PM.data = null; FB_PM.PM.error = null');
  const parsed = await run(`FB_PM.pmParse(new Uint8Array(${JSON.stringify(Array.from(gz))}).buffer, 'product_mix.json.gz')`);
  run(`FB_PM.pmUse(${JSON.stringify(parsed)})`);
  const c = run('FB_PM.pmCompute()');
  check('the uploaded file gives the same week-on-week totals', c.totCur === E.all_totals.cur && c.totPrev === E.all_totals.prev, `${c.totCur} vs ${E.all_totals.cur}`);
  // a file that is not product mix data
  let bad = ''; try { run('FB_PM.pmUse({ hello: 1 })'); } catch (e) { bad = e.message; }
  check('another file is refused with a clear message', bad.includes('not a product mix file'), bad);
  // the reference day comes from the Morning Call when the page has it
  ctx.FB_MC_REF_DAY = () => Math.round((Date.parse('2026-11-14T00:00:00Z') - EPOCH_MS) / 86400000);
  check('the reference day follows the Morning Call (yesterday)', run('FB_PM.pmCompute()').refDay === Math.round((Date.parse('2026-11-14T00:00:00Z') - EPOCH_MS) / 86400000));
  check('the Morning Call block shows the product mix with the file control at the end', run("salesPeriodView('morning')").includes('Product mix: week on week') && run("salesPeriodView('morning')").includes('data-pm-file="1"'));
  console.log(pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})();
