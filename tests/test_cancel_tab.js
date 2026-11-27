// The Cancellation tab: closed and cancelled orders for the Daily tab's day, with the page's filters.
// Run from the repo root: node tests/test_cancel_tab.js
const fs = require('fs'), vm = require('vm'), path = require('path');
let pass = 0, fail = 0;
const check = (n, c, x = '') => { c ? pass++ : fail++; console.log((c ? 'PASS ' : 'FAIL ') + n + (x ? '  [' + x + ']' : '')); };
const SITE = path.join(__dirname, '..', 'site');
const handlers = {};
const K = Math.round((Date.UTC(2026, 10, 20) - Date.UTC(2025, 0, 1)) / 86400000);   // 20 Nov 2026
const ctx = {
  console, Math, Number, String, Array, Object, Map, Set, JSON, Date, Infinity, isFinite, RegExp, Error, Promise,
  SUBS: { sales: [['daily', 'Daily']] }, render: () => {}, setPeriodBar: () => {},
  salesPeriodView: () => 'BASE',
  M: { brands: ['Frozen Bottle', 'Boba Bar'], regions: ['KA', 'TN'], sources: ['Swiggy', 'In Store'], storeTypes: ['COCO', 'FOFO'],
       stores: [{ i: 0, name: 'Yelahanka', a: 1 }, { i: 1, name: 'Nagavara', a: 1 }, { i: 2, name: 'Vellore', a: 2 }],
       ams: { 1: 'Reazul', 2: 'Akash' }, last_day: K },
  S: { type: 'all', brand: 'all', region: 'all', source: 'all', session: 'all', store: 'all' },
  STORE_ROWS: new Map(), PERIOD: { kind: 'daily', key: K },
  document: { addEventListener: (t, f) => { (handlers[t] = handlers[t] || []).push(f); } }
};
ctx.window = ctx;
vm.createContext(ctx);
// the page's own sales filter, as it is in index.html
const html = fs.readFileSync(path.join(SITE, 'index.html'), 'utf8');
vm.runInContext(html.match(/function salesPass\(r\) \{[\s\S]*?\n\}/)[0], ctx);
vm.runInContext(fs.readFileSync(path.join(SITE, 'cancel.js'), 'utf8'), ctx);
const run = s => vm.runInContext(s, ctx);
const FB = ctx.window.FB_CANCEL;
check('the Cancellation tab is in the Sales tabs', vm.runInContext("SUBS.sales.some(x => x[0] === 'cancel')", ctx));
// closed orders for the day, per store: [day, store, brand, type, region, source, session, net, gross, disc, orders]
const sale = (store, brand, type, region, source, orders) => [K, store, brand, type, region, source, 0, orders * 100, orders * 110, 0, orders];
ctx.STORE_ROWS.set(0, [sale(0, 0, 0, 0, 0, 10)]);          // Yelahanka: Frozen Bottle, COCO, KA, Swiggy, 10 closed
ctx.STORE_ROWS.set(1, [sale(1, 1, 0, 0, 1, 4)]);           // Nagavara: Boba Bar, COCO, KA, In Store, 4 closed
ctx.STORE_ROWS.set(2, [sale(2, 0, 0, 1, 0, 6)]);           // Vellore: Frozen Bottle, COCO, TN, Swiggy, 6 closed
// cancelled orders (the rows of the stored file)
const c = (date, branch, brand, region, source, listed, id) => ({ Date: date, Branch: branch, 'Brand Name': brand, Region: region,
  Source: source, 'Store Type': 'COCO', Session: 'Lunch', Listed: listed, 'Invoice No': id });
const d = '2026-11-20', dprev = '2026-11-19';
FB.setRows([
  c(d, 'Yelahanka', 'Frozen Bottle', 'KA', 'Swiggy', 'Yes', '1'), c(d, 'Yelahanka', 'Frozen Bottle', 'KA', 'Swiggy', 'Yes', '2'),
  c(d, 'Yelahanka', 'Frozen Bottle', 'KA', 'Swiggy', 'No', '3'),
  c(d, 'Nagavara', 'Boba Bar', 'KA', 'In Store', 'Yes', '4'),
  c(d, 'Vellore', 'Frozen Bottle', 'TN', 'Swiggy', 'Yes', '5'), c(d, 'Vellore', 'Frozen Bottle', 'TN', 'Swiggy', 'Yes', '6'),
  c(d, 'Vellore', 'Frozen Bottle', 'TN', 'Swiggy', 'Yes', '7'),
  c(dprev, 'Vellore', 'Frozen Bottle', 'TN', 'Swiggy', 'Yes', '8'),           // the day before: not counted
  c(d, 'Mylapore', 'Frozen Bottle', 'KA', 'Swiggy', 'No', '9')                 // a store not in the list
]);
// 1. the CSV reader: a quoted field may hold commas and line breaks
const parsed = FB.cxParseCsv('Invoice No,Reason,Listed\n1,"Item out, of stock",Yes\n2,"two\nlines",No\n');
check('the CSV reader keeps commas and line breaks inside a quoted field', parsed.length === 2 && parsed[0].Reason === 'Item out, of stock' && parsed[1].Reason === 'two\nlines');
// 2. by brand: overall = closed + cancelled; RDC = cancelled with a listed reason; Other = the rest
const recs = FB.cxRecords(K);
const byBrand = FB.cxGroups(recs, 'brand');
const fb = byBrand.find(g => g.label === 'Frozen Bottle'), bb = byBrand.find(g => g.label === 'Boba Bar');
check('Frozen Bottle: overall 23 (16 closed + 7 cancelled)', fb && fb.closed + fb.rdc + fb.other === 23, fb && JSON.stringify(fb));
check('Frozen Bottle: RDC 5, other 2', fb && fb.rdc === 5 && fb.other === 2);
check('Boba Bar: overall 5, RDC 1', bb && bb.closed + bb.rdc + bb.other === 5 && bb.rdc === 1);
check('the day before is not counted', fb && fb.rdc === 5, 'Vellore has 3 RDC cancels on the day, and 1 the day before');
// 3. cancel % = RDC / overall
const cxp = g => (g.rdc / (g.closed + g.rdc + g.other)) * 100;
check('Frozen Bottle cancel % is 21.7', Math.abs(cxp(fb) - 21.739) < 0.01, cxp(fb).toFixed(2));
// 4. by region: TN 9 orders, 3 RDC, 33.3%
const tn = FB.cxGroups(recs, 'region').find(g => g.label === 'TN');
check('TN: overall 9, RDC 3, cancel % 33.3', tn && tn.closed + tn.rdc + tn.other === 9 && tn.rdc === 3 && Math.abs(cxp(tn) - 33.33) < 0.01);
// 5. by area manager: Reazul 18, Akash 9, Unassigned 1 (the store not in the list)
const ams = FB.cxGroups(recs, 'am');
const get = (list, n) => list.find(g => g.label === n);
check('Reazul: overall 18; Akash: overall 9; Unassigned: overall 1', get(ams, 'Reazul') && get(ams, 'Reazul').rdc === 3 && get(ams, 'Akash') && get(ams, 'Akash').rdc === 3 && get(ams, 'Unassigned') && get(ams, 'Unassigned').other === 1);
check('the stores under Reazul: Yelahanka and Nagavara', FB.cxStoresOf(recs, 'Reazul').map(s => s.label).sort().join(',') === 'Nagavara,Yelahanka');
const totalOverall = recs.reduce((t, x) => t + x.closed + x.rdc + x.other, 0);
check('the total is 28, the same by brand and by area manager', totalOverall === 28 && byBrand.reduce((t, g) => t + g.closed + g.rdc + g.other, 0) === 28);
// 6. the channel: offline is In Store only
run("CX_SET = 1");
vm.runInContext("FB_CANCEL.CX.chan = 'offline'", ctx);
const off = FB.cxGroups(FB.cxRecords(K), 'brand');
check('Offline counts only the In Store orders: Boba Bar 5, no Frozen Bottle', off.length === 1 && off[0].label === 'Boba Bar' && off[0].closed + off[0].rdc + off[0].other === 5);
vm.runInContext("FB_CANCEL.CX.chan = 'all'", ctx);
// 7. the page's own filter: a brand filter takes out the other brand, orders and cancels alike
run("S.brand = 1");
const boba = FB.cxGroups(FB.cxRecords(K), 'brand');
check('the brand filter applies to the closed and the cancelled orders', boba.length === 1 && boba[0].label === 'Boba Bar');
run("S.brand = 'all'");
// 8. the view: the columns, the area manager row opens to its stores
let view = run('FB_CANCEL.cxView()');
check('the columns are Overall orders, Cancel orders (RDC), Other orders, Cancel orders %', ['Overall orders', 'Cancel orders (RDC)', 'Other orders', 'Cancel orders %'].every(h => view.includes(`<th>${h}</th>`)));
check('the Daily tab\'s day is used: the view shows the cancel % of Frozen Bottle (21.7%)', view.includes('21.7%'));
vm.runInContext("FB_CANCEL.CX.dim = 'am'", ctx);
view = run('FB_CANCEL.cxView()');
check('area managers are collapsed (no store rows yet)', view.includes('Reazul') && !view.includes('Yelahanka'));
(handlers.click || []).forEach(f => f({ target: { closest: s => (s === '[data-cx-am]' ? { dataset: { cxAm: 'Reazul' } } : null) } }));
view = run('FB_CANCEL.cxView()');
check('a click on an area manager shows its stores', view.includes('Yelahanka') && view.includes('Nagavara') && !view.includes('Vellore'));
(handlers.click || []).forEach(f => f({ target: { closest: s => (s === '[data-cx-dim]' ? { dataset: { cxDim: 'source' } } : null) } }));
check('a click on Source changes the grouping', vm.runInContext("FB_CANCEL.CX.dim", ctx) === 'source' && run('FB_CANCEL.cxView()').includes('Swiggy'));
// 9. the file cannot be read: the message, not a blank tab
vm.runInContext("FB_CANCEL.CX.rows = null; FB_CANCEL.CX.error = 'The cancelled orders could not be read (404).'", ctx);
check('a missing file shows the reason', run('FB_CANCEL.cxView()').includes('could not be read (404)'));
console.log(pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
