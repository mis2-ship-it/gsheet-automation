// The Morning Call layout: area managers collapsed (stores on click), no region + store tables, and the Excel
// Region wise tab stacked as in the snapshot. Run from the repo root: node tests/test_morning_layout.js
const fs = require('fs'), vm = require('vm'), path = require('path');
let pass = 0, fail = 0;
const check = (n, c, x = '') => { c ? pass++ : fail++; console.log((c ? 'PASS ' : 'FAIL ') + n + (x ? '  [' + x + ']' : '')); };
const handlers = {};
const ctx = {
  console, Math, Number, String, Array, Object, Map, Set, JSON, Date, Infinity, isFinite, RegExp, Error, TextEncoder, Uint32Array,
  SUBS: { sales: [] }, render: () => {}, setPeriodBar: () => {}, salesPeriodView: () => '',
  fmtLong: () => 'Wed, 25 Nov, 2026', toDate: d => new Date(d),
  M: {
    stores: [{ i: 0, name: 'Yelahanka', a: 1 }, { i: 1, name: 'Nagavara', a: 1 }, { i: 2, name: 'Vellore', a: 2 }, { i: 3, name: 'Chennai Mall', a: 2 }],
    ams: { 1: 'Reazul', 2: 'Akash' }, sources: ['Swiggy', 'In Store'], brands: ['Frozen Bottle'], regions: ['KA', 'TN'], has_live: false
  },
  document: { addEventListener: (t, f) => { (handlers[t] = handlers[t] || []).push(f); }, getElementById: () => null, createElement: () => ({}), body: { appendChild() {} } }
};
ctx.window = ctx;
vm.createContext(ctx);
vm.runInContext(fs.readFileSync(path.join(__dirname, '..', 'site', 'morning.js'), 'utf8'), ctx);
const FB = ctx.window.FB_MORNING;
check('the Morning Call script loads and exports its functions', !!FB && typeof FB.mcAmBlock === 'function' && typeof FB.mcRegionWiseSheet === 'function');
// two areas, four stores, in two regions; every value carries net, gross, discount and orders
const val = n => ({ n, g: n * 1.1, d: n * 0.1, o: 10 });
const keys = ['octFull', 'octSame', 'novMtd', 'trend', 'dayCur', 'momDayPrev', 'yoyLy', 'yoyTy', 'ytdLy', 'ytdTy', 'wowPrev', 'wowCur', 'l2wPrev', 'ftdPre', 'ftdCur', 'lnPrev', 'lnCur'];
const rec = (i, name, region, base) => { const R = {}, P = {}; keys.forEach((k, j) => { R[k] = val(base + j * 10); P[k] = val(base + j * 7); }); return { i, name, region, R, P }; };
const c = { ref: 694, dom: 25, balance: 5, list: [rec(0, 'Yelahanka', 'KA', 1000), rec(1, 'Nagavara', 'KA', 2000), rec(2, 'Vellore', 'TN', 1500), rec(3, 'Chennai Mall', 'TN', 2500)] };
const cols = [{ k: 'ftdPre', h: 'Pre FTD' }, { k: 'ftdCur', h: 'FTD' }, { k: 'ftd', h: 'Growth %', g: ['ftdCur', 'ftdPre'] }];
const blocks = [{ id: 'ftd', sheet: 'FTD', title: 'FTD vs Pre FTD', sub: '', main: 'ftdCur', sort: 'ftd', cols },
                { id: 'wow', sheet: 'WoW', title: 'WoW (same weekday)', sub: '', main: 'wowCur', sort: 'wow',
                  cols: [{ k: 'wowPrev', h: 'Prev' }, { k: 'wowCur', h: 'Cur' }, { k: 'wow', h: 'Growth %', g: ['wowCur', 'wowPrev'] }] }];
// 1. the area managers: collapsed, the stores hidden
let html = FB.mcAmBlock('top', c, cols);
check('the area managers are listed', html.includes('data-mc-am="top:1"') && html.includes('data-mc-am="top:2"') && html.includes('Reazul') && html.includes('Akash'));
check('the stores are hidden until a manager is clicked', !html.includes('Yelahanka') && !html.includes('Vellore') && !html.includes('ins-st'));
// 2. a click on an area manager shows its stores only
(handlers.click || []).forEach(f => f({ target: { closest: s => (s === '[data-mc-am]' ? { dataset: { mcAm: 'top:1' } } : null) } }));
check('the click opens that manager (MC state)', FB.MC.amOpen.has('top:1'));
html = FB.mcAmBlock('top', c, cols);
check('the opened manager shows its stores, the other manager stays closed', html.includes('Yelahanka') && html.includes('Nagavara') && !html.includes('Vellore'));
check('the open state is per table: the FTD table is still closed', !FB.mcAmBlock('ftd', c, cols).includes('Yelahanka'));
// 3. the Excel "Region wise" tab: the tables one under another
const sheet = FB.mcRegionWiseSheet(c, blocks, null);
const titles = sheet.rows.map(r => (r[0] && r[0].v) || '');
check('the Region wise tab has a title and a row per region in each table', titles.includes('Morning Call: region wise, all tables') && titles.filter(t => t === 'Region').length === 2, String(titles.filter(t => t === 'Region').length));
const band = sheet.rows.findIndex(r => r[1] && r[1].v === 'FTD vs Pre FTD') + 1;
const wowBand = sheet.rows.findIndex(r => r[1] && r[1].v === 'WoW (same weekday)') + 1;
check('the FTD table comes first, then WoW below it', band > 0 && wowBand > band, `FTD row ${band}, WoW row ${wowBand}`);
check('the FTD band is merged across its columns, as the snapshot', sheet.merges.includes(`B${band}:D${band}`), sheet.merges.join(' '));
check('the region rows are All regions, KA and TN, with nothing per store', ['All regions', 'KA', 'TN'].every(n => sheet.rows.some(r => r[0] && r[0].v === n)) && !sheet.rows.some(r => r[0] && r[0].v === 'Yelahanka'));
// 4. the workbook: Region wise, Region + source, Area manager; no store-split sheets
// mcWorkbook builds these three tabs in this order (its source is checked below); the sales-data step is not run here
const summary = FB.mcSummary(c);
const bytes = Buffer.from(FB.mcXlsx([FB.mcRegionWiseSheet(c, blocks, null), FB.mcRegionSourceSheet(c, blocks, null), FB.mcAmSheet(c, summary)])).toString('latin1');
const src = fs.readFileSync(path.join(__dirname, '..', 'site', 'morning.js'), 'utf8');
check('mcWorkbook builds Region wise, Region + source and Area manager, and no store tabs', src.includes('return mcXlsx([mcRegionWiseSheet(c, blocks, null), mcRegionSourceSheet(c, blocks, srcT), mcAmSheet(c, summary)]);'));
check('the workbook has the Region wise, Region + source and Area manager tabs', ['Region wise', 'Region + source', 'Area manager'].every(s => bytes.includes(`name="${s}"`)));
check('the workbook has no store-split tabs', !/name="(FTD|WoW|MoM MTD|YoY|Late Night) Store"/.test(bytes));
console.log(pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
