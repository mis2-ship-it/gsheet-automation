// The Product mix upload is kept in this browser: it survives a refresh, the note shows, and Remove clears it.
const fs = require('fs'), vm = require('vm'), path = require('path');
let pass = 0, fail = 0;
const check = (n, c, x = '') => { c ? pass++ : fail++; console.log((c ? 'PASS ' : 'FAIL ') + n + (x ? '  [' + x + ']' : '')); };
const GZ = fs.readFileSync(path.join(__dirname, 'fixtures', 'product_mix.json.gz'));
const store = new Map();
const handlers = {};
let loadImpl = async () => { throw new TypeError('Failed to fetch'); };
const ctx = { console, Map, Set, Math, Number, String, Array, Object, JSON, Date, Intl, Infinity, isFinite, RegExp, Error, Promise, setTimeout,
  M: { brands: [], regions: [], sources: [] }, S: { store: 'all', type: 'all', brand: 'all', region: 'all', source: 'all', session: 'all' },
  num: n => Math.round(n).toLocaleString('en-IN'), FB_SALES_CH: { value: 'all' }, renders: 0,
  loadGz: async () => loadImpl(), render() { ctx.renders++; },
  localStorage: { getItem: k => (store.has(k) ? store.get(k) : null), setItem: (k, v) => store.set(k, String(v)), removeItem: k => store.delete(k) },
  document: { addEventListener: (t, f) => { (handlers[t] = handlers[t] || []).push(f); } },
  Blob, Response, DecompressionStream, TextDecoder, TextEncoder, btoa, atob };
ctx.window = ctx;
ctx.window.salesPeriodView = () => '';
vm.createContext(ctx);
vm.runInContext(fs.readFileSync(path.join(__dirname, '..', 'site', 'productmix.js'), 'utf8'), ctx);
const run = s => vm.runInContext(s, ctx);
const wait = ms => new Promise(r => setTimeout(r, ms));
(async () => {
  run('FB_PM.PM.data = null; FB_PM.PM.error = null; FB_PM.PM.savedName = null');
  await run('FB_PM.pmLoad()'); await wait(20);
  check('no server file and nothing saved: the page asks for the file', run('FB_PM.PM.data') === null && /could not reach/.test(run('FB_PM.PM.error')));
  const file = { name: 'product_mix.json.gz', arrayBuffer: async () => GZ.buffer.slice(GZ.byteOffset, GZ.byteOffset + GZ.byteLength) };
  await run('FB_PM.pmPickFile(FB_FILE)', ctx.FB_FILE = file); await wait(50);
  check('the uploaded file is saved in this browser', store.has('fb-product-mix-file') && run('FB_PM.PM.data') !== null);
  run('FB_PM.PM.data = null; FB_PM.PM.error = null; FB_PM.PM.savedName = null');
  await run('FB_PM.pmLoad()'); await wait(50);
  check('after a reload the saved file is used', run('FB_PM.PM.data') !== null && run('FB_PM.PM.savedName') === 'product_mix.json.gz', run('FB_PM.PM.error') || '');
  check('the upload control says the file is the one saved in this browser, with Remove', run('FB_PM.pmUploadControl()').includes('saved in this browser') && run('FB_PM.pmUploadControl()').includes('data-pm-forget'));
  (handlers.click || []).forEach(f => f({ target: { closest: sel => (sel === '[data-pm-forget]' ? {} : null) } }));
  check('Remove deletes the saved file and clears the data', !store.has('fb-product-mix-file') && run('FB_PM.PM.data') === null && run('FB_PM.PM.savedName') === null);
  console.log(pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
})();
