const fs = require('fs'), vm = require('vm'), zlib = require('zlib');
let pass = 0, fail = 0;
const check = (n, c, x = '') => { c ? pass++ : fail++; console.log((c ? 'PASS ' : 'FAIL ') + n + (x ? '  [' + x + ']' : '')); };
const DATA = JSON.parse(zlib.gunzipSync(fs.readFileSync(require('path').join(__dirname, 'fixtures', 'metrics.json.gz'))).toString('utf8'));
const E = JSON.parse(fs.readFileSync(require('path').join(__dirname, 'fixtures', 'metrics_expected.json'), 'utf8'));
let loadImpl = async () => DATA;
const mk = () => ({ children: [], id: '', appendChild(c) { this.children.push(c); return c; } });
const ctx = { console, Map, Set, Math, Number, String, Array, Object, JSON, Date, Intl, Infinity, isFinite, RegExp, Error, Promise, setTimeout,
  SUBS: { sales: [['live', 'Live'], ['daily', 'Daily']] }, renders: 0, num: n => Math.round(n).toLocaleString('en-IN'),
  loadGz: async (u) => loadImpl(u), render() { ctx.renders++; }, salesPeriodView: k => 'BASE:' + k, setPeriodBar() {},
  document: { addEventListener() {}, getElementById: () => null, createElement: () => ({}), head: { appendChild() {} } },
  Blob, Response, DecompressionStream, TextDecoder, TextEncoder };
ctx.window = ctx;
vm.createContext(ctx);
vm.runInContext(fs.readFileSync(require('path').join(__dirname, '..', 'site', 'metrics.js'), 'utf8'), ctx);
const run = s => vm.runInContext(s, ctx);
(async () => {
  // a failed fetch: the browser never got a response
  loadImpl = async () => { throw new TypeError('Failed to fetch'); };
  run('FB_MT.mtLoad()'); await new Promise(r => setTimeout(r, 20));
  const errA = run('FB_MT.MT.error');
  check('a failed fetch says the file could not be reached, with what to do', errA && errA.includes('could not reach data/metrics.json.gz') && /load the file below/i.test(errA), errA);
  check('the page still offers the file control when there is no data', run("salesPeriodView('metrics')").includes('data-mt-file="1"') && run("salesPeriodView('metrics')").includes('could not reach'));
  // a 404: the server answered but has no file for this login
  run("FB_MT.MT.error = null; FB_MT.MT.data = null");
  loadImpl = async () => { throw new Error('data/metrics.json.gz (404)'); };
  run('FB_MT.mtLoad()'); await new Promise(r => setTimeout(r, 20));
  check('a 404 says the file is not on the server yet', run('FB_MT.MT.error').includes('not on the server yet'), run('FB_MT.MT.error'));
  // upload a gzip file: read the data straight from the file
  const gz = fs.readFileSync(require('path').join(__dirname, 'fixtures', 'metrics.json.gz'));
  run('FB_MT.MT.error = null; FB_MT.MT.data = null');
  const parsed = await run(`FB_MT.mtParse(new Uint8Array(${JSON.stringify(Array.from(gz))}).buffer, 'metrics.json.gz')`);
  run(`FB_MT.mtUse(${JSON.stringify(parsed)})`);
  check('a gzip file loads the data', run('FB_MT.MT.data') !== null && run('FB_MT.MT.data.rows.length') === DATA.rows.length);
  const sumA = run('FB_MT.mtSummary().a.total.orders');
  check('the uploaded file gives the same totals as the server copy', sumA === E.combos.none.A.total.orders, `${sumA} vs ${E.combos.none.A.total.orders}`);
  check('the default periods are set from the file', run('FB_MT.MT.a.to') === Math.max(...DATA.rows.map(r => r[0])));
  // upload a plain JSON file
  run('FB_MT.MT.data = null');
  const plain = await run(`FB_MT.mtParse(new TextEncoder().encode(${JSON.stringify(JSON.stringify(DATA))}).buffer, 'metrics.json')`);
  run(`FB_MT.mtUse(${JSON.stringify(plain)})`);
  check('a plain JSON file loads too', run('FB_MT.MT.data.sources.length') === DATA.sources.length);
  // a file that is not metrics data
  let bad = '';
  try { run('FB_MT.mtUse({ hello: 1 })'); } catch (e) { bad = e.message; }
  check('another file is refused with a clear message', bad.includes('not a metrics file'), bad);
  // the Metrics tab with data shows its charts again
  check('with data, the Metrics tab shows the summary and charts', run("salesPeriodView('metrics')").includes('Comparison A and B'));
  console.log(pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})();
