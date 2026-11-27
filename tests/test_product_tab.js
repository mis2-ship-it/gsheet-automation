// The Product Level tab: totals against the raw rows, categories under products, the pivot columns, filters, growth,
// the measures, the top 12 columns with Others, and the view. Run from the repo root: node tests/test_product_tab.js
const fs = require('fs'), vm = require('vm'), path = require('path');
let pass = 0, fail = 0;
const check = (n, c, x = '') => { c ? pass++ : fail++; console.log((c ? 'PASS ' : 'FAIL ') + n + (x ? '  [' + x + ']' : '')); };
const handlers = {};
const branches = [
  { n: 'Yelahanka', b: 'Frozen Bottle', r: 'KA', t: 'COCO', am: 1 }, { n: 'Vellore', b: 'Frozen Bottle', r: 'TN', t: 'FOFO', am: 2 },
  { n: 'Nagavara', b: 'Boba Bar', r: 'KA', t: 'COCO', am: 1 }];
const items = [['Belgian Shake', 'Shakes'], ['Mango Shake', 'Shakes'], ['Cookie', 'Bakery']];
const srcs = ['Swiggy', 'In Store'];
const iso = d => new Date(Date.UTC(2026, 10, 1) + d * 86400000).toISOString().slice(0, 10);
function makeRows(brs, days) {
  const out = [];
  for (let d = 0; d < days; d++) brs.forEach((br, bi) => items.forEach(([it, cat], ii) => srcs.forEach((src, si) => {
    if ((d + bi + ii + si) % 3 === 0) return;
    const orders = ((d * 7 + bi * 3 + ii * 5 + si) % 4) + 1, gross = orders * 200, disc = (orders % 2) * 20 * orders;
    out.push({ Date: iso(d), Branch: br.n, Brand: br.b, Region: br.r, 'Store Type': br.t, Source: src, Category: cat, Item: it,
               Orders: orders, Qty: orders, Gross: gross, Discount: disc, Net: gross - disc });
  })));
  return out;
}
const many = [...branches];
for (let k = 1; k <= 14; k++) many.push({ n: 'S' + String(k).padStart(2, '0'), b: 'Frozen Bottle', r: 'KA', t: 'COCO', am: 1 });
const stores = many.map((br, i) => ({ i, name: br.n, a: br.am }));
const ctx = {
  console, Math, Number, String, Array, Object, Map, Set, JSON, Date, Infinity, isFinite, RegExp, Error, Promise,
  SUBS: { sales: [['daily', 'Daily']] }, render: () => {}, setPeriodBar: () => {}, salesPeriodView: () => 'BASE',
  M: { brands: ['Frozen Bottle', 'Boba Bar'], regions: ['KA', 'TN'], storeTypes: ['COCO', 'FOFO'], sources: srcs,
       stores, ams: { 1: 'Reazul', 2: 'Akash' } },
  document: { addEventListener: (t, f) => { (handlers[t] = handlers[t] || []).push(f); } }
};
ctx.window = ctx;
vm.createContext(ctx);
vm.runInContext(fs.readFileSync(path.join(__dirname, '..', 'site', 'product.js'), 'utf8'), ctx);
const run = s => vm.runInContext(s, ctx);
const FB = ctx.window.FB_PRODUCT;
const objs = makeRows(branches, 20);
const rows = FB.plRowsFrom(objs);
const dayOf = s => FB.plDayOf(s);
const st = over => ({ brand: 'all', region: 'all', type: 'all', store: 'all', am: 'all', metric: 'net', pivot: 'brand',
                      from: dayOf('2026-11-14'), to: dayOf('2026-11-20'), ...over });
const inWin = (o, a, b) => o.Date >= a && o.Date <= b;
const sumOf = (list, f) => list.reduce((t, o) => t + o[f], 0);
const W = objs.filter(o => inWin(o, '2026-11-14', '2026-11-20'));
const belgianRaw = W.filter(o => o.Item === 'Belgian Shake');

// 1. the totals against the raw rows, for the window
let m = FB.plModel(rows, st({}));
check('the total net is the raw sum of the window', m.total.net === sumOf(W, 'Net'), `${m.total.net} vs ${sumOf(W, 'Net')}`);
check('the total orders is the raw sum of the window', m.total.orders === sumOf(W, 'Orders'));
// 2. a product: its net against the raw rows
const belgian = m.products.find(p => p.name === 'Belgian Shake');
check('Belgian Shake net is the raw sum for its rows', belgian && belgian.x.net === sumOf(belgianRaw, 'Net'));
// 3. the categories under a product add up to the product
check('the categories under each product add up to the product (net)', m.products.every(p => p.cats.reduce((t, c) => t + c.x.net, 0) === p.x.net));
// 4. the pivot columns add up to the product
check('the brand columns add up to each product', m.products.every(p => [...p.cols.values()].reduce((t, x) => t + x.net, 0) === p.x.net));
check('the column totals add up to the total', [...m.colTotals.values()].reduce((t, x) => t + x.net, 0) === m.total.net);
// 5. filters: brand, area manager
const mb = FB.plModel(rows, st({ brand: 'Boba Bar' }));
check('the brand filter keeps only that brand', mb.total.net === sumOf(W.filter(o => o.Brand === 'Boba Bar'), 'Net'));
const ma = FB.plModel(rows, st({ am: 'Akash' }));
check('the area manager filter keeps only the stores of that manager (Vellore)', ma.total.orders === sumOf(W.filter(o => o.Branch === 'Vellore'), 'Orders'));
// 6. growth: the previous period is the same number of days before the window
check('the previous period is the 7 days before the window', m.prev.net === sumOf(objs.filter(o => inWin(o, '2026-11-07', '2026-11-13')), 'Net'));
check('growth text: +10.0% from 110 to 100, and points for Dis %', FB.plGrowthText('net', 110, 100) === '+10.0%' && FB.plGrowthText('dispct', 12, 10) === '+2.00 pts');
// 7. the measures
const bd = belgianRaw.reduce((t, o) => ({ g: t.g + o.Gross, d: t.d + o.Discount, n: t.n + o.Net, o: t.o + o.Orders }), { g: 0, d: 0, n: 0, o: 0 });
check('Dis % is discount over gross', Math.abs(FB.plValue(belgian.x, 'dispct') - (bd.d / bd.g) * 100) < 1e-9);
check('AOV is net over orders', Math.abs(FB.plValue(belgian.x, 'aov') - bd.n / bd.o) < 1e-9);
check('the discount measure is the discount total', FB.plValue(belgian.x, 'discount') === bd.d);
// 8. a single day
const day = dayOf('2026-11-18');
const one = FB.plModel(rows, st({ from: day, to: day }));
check('a one-day window gives that day', one.total.net === sumOf(objs.filter(o => o.Date === '2026-11-18'), 'Net'));
// 9. more than 12 stores: the top 12 stay, the rest is Others, and the columns still add up
const rowsMany = FB.plRowsFrom(makeRows(many, 20));
const mm = FB.plModel(rowsMany, st({ pivot: 'branch' }));
check('the store columns show the top 12 and Others', mm.columns.length === 13 && mm.columns[12] === 'Others', String(mm.columns.length));
check('the store columns still add up to the total', [...mm.colTotals.values()].reduce((t, x) => t + x.net, 0) === mm.total.net);
// 10. the view: products closed, then open shows the categories; filters and the measure are on the page
run('FB_PRODUCT.PL.rows = ' + JSON.stringify(rows));
run("FB_PRODUCT.PL.from = FB_PRODUCT.plDayOf('2026-11-14'); FB_PRODUCT.PL.to = FB_PRODUCT.plDayOf('2026-11-20'); FB_PRODUCT.PL.open = new Set()");
let html = run('FB_PRODUCT.plRender(FB_PRODUCT.PL)');
check('the view has the filters and the measure', html.includes('data-pl="brand"') && html.includes('data-pl="am"') && html.includes('data-pl="metric"') && html.includes('data-pl="pivot"'));
check('the products are shown, categories hidden', html.includes('data-pl-open="Belgian Shake"') && !html.includes('ins-st'));
run("FB_PRODUCT.PL.open = new Set(['Belgian Shake'])");
html = run('FB_PRODUCT.plRender(FB_PRODUCT.PL)');
check('a product opens to its categories', html.includes('ins-st') && html.includes('Shakes'));
// the click on a product, and the change of a filter, through the page's handlers
(handlers.click || []).forEach(f => f({ target: { closest: s => (s === '[data-pl-open]' ? { dataset: { plOpen: 'Cookie' } } : null) } }));
check('a click opens the product (state)', run('FB_PRODUCT.PL.open.has("Cookie")'));
(handlers.change || []).forEach(f => f({ target: { dataset: { pl: 'metric' }, value: 'orders' } }));
check('a change of the measure sets it', run('FB_PRODUCT.PL.metric') === 'orders');
// 11. no file: a message
run("FB_PRODUCT.PL.rows = null; FB_PRODUCT.PL.error = 'The product sales could not be read (404).'");
check('a missing file says so', run('FB_PRODUCT.plRender(FB_PRODUCT.PL)').includes('could not be read (404)'));
check('the Product Level tab is in the Sales tabs', run("SUBS.sales.some(x => x[0] === 'product')"));
console.log(pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
