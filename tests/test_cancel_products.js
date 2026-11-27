// The Products section of the Cancellation tab: the day's cancelled items by category, then by item.
// Run from the repo root (needs site/index.html for the page's sales filter): node tests/test_cancel_products.js
const fs = require('fs'), vm = require('vm'), path = require('path');
let pass = 0, fail = 0;
const check = (n, c, x = '') => { c ? pass++ : fail++; console.log((c ? 'PASS ' : 'FAIL ') + n + (x ? '  [' + x + ']' : '')); };
const SITE = path.join(__dirname, '..', 'site');
const handlers = {};
const K = Math.round((Date.UTC(2026, 10, 20) - Date.UTC(2025, 0, 1)) / 86400000);
const ctx = {
  console, Math, Number, String, Array, Object, Map, Set, JSON, Date, Infinity, isFinite, RegExp, Error, Promise,
  SUBS: { sales: [['daily', 'Daily']] }, render: () => {}, setPeriodBar: () => {}, salesPeriodView: () => 'BASE',
  M: { brands: ['Frozen Bottle', 'Boba Bar'], regions: ['KA', 'TN'], sources: ['Swiggy', 'In Store'], storeTypes: ['COCO', 'FOFO'],
       stores: [{ i: 0, name: 'Yelahanka', a: 1 }, { i: 1, name: 'Nagavara', a: 1 }, { i: 2, name: 'Vellore', a: 2 }],
       ams: { 1: 'Reazul', 2: 'Akash' }, last_day: K },
  S: { type: 'all', brand: 'all', region: 'all', source: 'all', session: 'all', store: 'all' },
  STORE_ROWS: new Map(), PERIOD: { kind: 'daily', key: K },
  document: { addEventListener: (t, f) => { (handlers[t] = handlers[t] || []).push(f); } }
};
ctx.window = ctx;
vm.createContext(ctx);
const page = fs.readFileSync(path.join(SITE, 'index.html'), 'utf8');
vm.runInContext(page.match(/function salesPass\(r\) \{[\s\S]*?\n\}/)[0], ctx);
vm.runInContext(fs.readFileSync(path.join(SITE, 'cancel.js'), 'utf8'), ctx);
const run = s => vm.runInContext(s, ctx);
const FB = ctx.window.FB_CANCEL;
const d = '2026-11-20', dprev = '2026-11-19';
const it = (date, inv, branch, brand, region, source, listed, name, cat, net) => ({ Date: date, 'Invoice No': inv, Branch: branch,
  'Brand Name': brand, Region: region, Source: source, 'Store Type': 'COCO', Listed: listed, 'Item Name': name, Category: cat,
  'Item Net': String(net), Line: '1' });
FB.setItems([
  it(d, 'A1', 'Yelahanka', 'Frozen Bottle', 'KA', 'Swiggy', 'Yes', 'Belgian Shake', 'Shakes', 380),
  it(d, 'A1', 'Yelahanka', 'Frozen Bottle', 'KA', 'Swiggy', 'Yes', 'Cookie', 'Bakery', 50),
  it(d, 'A2', 'Nagavara', 'Boba Bar', 'KA', 'In Store', 'No', 'Belgian Shake', 'Shakes', 190),
  it(dprev, 'A3', 'Vellore', 'Frozen Bottle', 'TN', 'Swiggy', 'Yes', 'Belgian Shake', 'Shakes', 999),
  it(d, 'A4', 'Vellore', 'Frozen Bottle', 'TN', 'Swiggy', 'Yes', 'Mango Shake', 'Shakes', 200)
]);
const cats = FB.cxProducts(K);
const shakes = cats.find(c => c.label === 'Shakes'), bakery = cats.find(c => c.label === 'Bakery');
check('categories are listed, the one with most orders first', cats[0].label === 'Shakes' && cats.length === 2);
check('Shakes: 3 distinct orders (A1, A2, A4), 2 RDC, 1 other, 770 net', shakes && shakes.orders === 3 && shakes.rdcN === 2 && shakes.otherN === 1 && shakes.net === 770,
      shakes && JSON.stringify(shakes));
const bel = shakes.items.find(x => x.label === 'Belgian Shake'), mango = shakes.items.find(x => x.label === 'Mango Shake');
check('Belgian Shake: 2 orders, 570 net (the day before is not counted)', bel && bel.orders === 2 && bel.net === 570, bel && JSON.stringify(bel));
check('Mango Shake: 1 order, 200 net', mango && mango.orders === 1 && mango.net === 200);
check('Bakery: Cookie, 1 order, 50 net', bakery && bakery.orders === 1 && bakery.items[0].label === 'Cookie' && bakery.net === 50);
// the channel and the page's filters
run("FB_CANCEL.CX.chan = 'offline'");
const off = FB.cxProducts(K);
check('Offline: only the In Store order (A2)', off.length === 1 && off[0].orders === 1 && off[0].otherN === 1 && off[0].net === 190);
run("FB_CANCEL.CX.chan = 'all'; S.brand = 0");
const fbOnly = FB.cxProducts(K).find(c => c.label === 'Shakes');
check('the brand filter takes out Boba Bar (A2): Shakes is 2 orders, 580 net', fbOnly && fbOnly.orders === 2 && fbOnly.net === 580, fbOnly && JSON.stringify(fbOnly));
run("S.brand = 'all'");
// the view: collapsed, then the category opens to its items
let view = run('FB_CANCEL.cxProductsSection(' + K + ')');
check('the products section lists the categories, items hidden', view.includes('Products on cancellation') && view.includes('Shakes') && !view.includes('Belgian Shake'));
(handlers.click || []).forEach(f => f({ target: { closest: s => (s === '[data-cx-cat]' ? { dataset: { cxCat: 'Shakes' } } : null) } }));
view = run('FB_CANCEL.cxProductsSection(' + K + ')');
check('a click on a category shows its items', view.includes('Belgian Shake') && view.includes('Mango Shake'));
// no item file yet: a message, not a blank section
run("FB_CANCEL.CX.items = null; FB_CANCEL.CX.itemsError = 'The cancelled orders could not be read (404).'");
check('a missing item file says so', run('FB_CANCEL.cxProductsSection(' + K + ')').includes('could not be read (404)'));
console.log(pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
