/* ============ Discount Code tab ============
   Next to Rating on the top row. Each order counts once, under its discount label.
   Filters: Brand, Region, Source, Session (one each). The item table also has Discount Code (one)
   and Category (several, with a search box, e.g. 70%). Money is paise in the data, shown in rupees.
   Loaded last, after calendar.js. */
let DISC = null, DISC_ERR = false, DISC_BUSY = false;
const DS = { kind: "monthly", brand: "all", region: "all", source: "all", session: "all",
             code: "all", cats: null, catSearch: "" };
const EMPTY_D = { inv: 0, qty: 0, gross: 0, disc: 0, net: 0 };
const dInr = p => Math.round(p / 100).toLocaleString("en-IN");
const num_ = n => Math.round(n).toLocaleString("en-IN");
const dPct = (d, g) => (g ? (d / g * 100).toFixed(1) + "%" : "—");
const escD = t => String(t).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
const dFmt = i => toDate(i).toLocaleDateString("en-IN", { day: "numeric", month: "short", year: "numeric", timeZone: "UTC" });
const dLabel = (kind, key) => {
  const w = sumWins(kind, key).cur;
  return w[0] === w[1] ? dFmt(w[0]) : `${dFmt(w[0])} to ${dFmt(w[1])}`;
};
const dGrHtml = (a, b) => {
  if (!b) return "—";
  const g = (a / b - 1) * 100, up = g >= 0;
  return `<span style="color:${up ? "var(--up)" : "var(--down)"}">${up ? "▲" : "▼"} ${Math.abs(g).toFixed(1)}%</span>`;
};

async function ensureDiscounts() {
  if (DISC || DISC_BUSY) return;
  DISC_BUSY = true;
  try { DISC = await loadGz("data/discounts.json.gz"); DISC_ERR = false; }
  catch (err) { DISC_ERR = true; }
  DISC_BUSY = false;
  render();
}

// Windows for the chosen kind, as in the sales tabs (same days last year or last period).
function dPeriod() {
  if (DS.kind !== "mtd") {
    const keys = periodKeys(DS.kind);
    if (!keys.length) return null;
    if (S.sel === null || !keys.includes(S.sel)) S.sel = keys[keys.length - 1];
  }
  return amPeriodWins(DS.kind);
}

function dOk(r) {
  return (DS.brand === "all" || r[1] === +DS.brand) && (DS.region === "all" || r[2] === +DS.region) &&
         (DS.source === "all" || r[3] === +DS.source) && (DS.session === "all" || r[4] === +DS.session);
}
// o = 0 for label rows (invoice count in column 6), o = 1 for item rows (one more column first).
function dAdd(a, r, o) {
  a.inv += r[6 + o]; a.qty += r[7 + o]; a.gross += r[8 + o]; a.disc += r[9 + o]; a.net += r[10 + o];
  return a;
}
function dGroup(rows, wins, keyPos, o, keep) {
  const out = new Map();
  for (const r of rows) {
    if (!wins.some(w => r[0] >= w[0] && r[0] <= w[1])) continue;
    if (!dOk(r) || (keep && !keep(r))) continue;
    const k = r[keyPos];
    out.set(k, dAdd(out.get(k) || { inv: 0, qty: 0, gross: 0, disc: 0, net: 0 }, r, o));
  }
  return out;
}
function dTotal(m) {
  return [...m.values()].reduce((t, a) => ({ inv: t.inv + a.inv, qty: t.qty + a.qty, gross: t.gross + a.gross,
    disc: t.disc + a.disc, net: t.net + a.net }), { inv: 0, qty: 0, gross: 0, disc: 0, net: 0 });
}

function discountView() {
  if (DISC_ERR) {
    return `<section class="empty"><h2>Discount Code</h2><p>The discount data could not be loaded. Usually the sign-in has ended.</p>` +
      `<p><button class="refresh" type="button" data-disc-retry="1">Try again</button></p></section>`;
  }
  if (!DISC) return `<section class="empty"><h2>Discount Code</h2><p>Loading the discount data…</p></section>`;
  const w = dPeriod();
  if (!w) return `<section class="empty"><h2>Discount Code</h2><p>No discount data for this period.</p></section>`;

  const kinds = [["daily", "Daily"], ["weekly", "Weekly"], ["monthly", "Monthly"], ["mtd", "Month to date"]];
  const seg = `<div class="seg4" style="margin:0 0 10px">` + kinds.map(([id, label]) =>
    `<button type="button" data-dkind="${id}" aria-pressed="${DS.kind === id}">${label}</button>`).join("") + `</div>`;
  const keys = DS.kind === "mtd" ? [] : periodKeys(DS.kind);
  const period = keys.length
    ? `<label class="field" style="margin-bottom:10px">Period<select data-dsel>` +
      keys.map(k => `<option value="${k}"${k === S.sel ? " selected" : ""}>${escD(dLabel(DS.kind, k))}</option>`).join("") + `</select></label>`
    : `<p class="sub">${escD(w.note)}</p>`;

  const opt = (list, cur) => `<option value="all">All</option>` + list.map((v, i) =>
    `<option value="${i}"${String(cur) === String(i) ? " selected" : ""}>${escD(v)}</option>`).join("");
  const filters = `<div class="fgrid" style="grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin-bottom:12px">` +
    `<label class="field">Brand<select data-dsf="brand">${opt(DISC.brands, DS.brand)}</select></label>` +
    `<label class="field">Region<select data-dsf="region">${opt(DISC.regions, DS.region)}</select></label>` +
    `<label class="field">Source<select data-dsf="source">${opt(DISC.sources, DS.source)}</select></label>` +
    `<label class="field">Session<select data-dsf="session">${opt(DISC.sessions, DS.session)}</select></label></div>`;

  const cur = dGroup(DISC.rows, w.cur, 5, 0), prev = dGroup(DISC.rows, w.prev, 5, 0);
  const tc = dTotal(cur), tp = dTotal(prev);
  const codes = [...new Set([...cur.keys(), ...prev.keys()])]
    .sort((a, b) => (cur.get(b) || EMPTY_D).net - (cur.get(a) || EMPTY_D).net);
  const line = (name, a, b, cls) => `<tr class="${cls || ""}"><td>${escD(name)}</td><td>${num_(a.inv)}</td><td>${num_(a.qty)}</td>` +
    `<td>${dInr(a.gross)}</td><td>${dInr(a.disc)}</td><td>${dInr(a.net)}</td><td>${dGrHtml(a.net, b.net)}</td>` +
    `<td>${dPct(a.disc, a.gross)}</td><td>${a.inv ? dInr(a.net / a.inv) : "—"}</td></tr>`;
  const codeTable = `<section class="block"><h2>By discount code</h2>` +
    `<p class="sub">${escD(w.c)} against ${escD(w.p)}. Each order counts once, under its discount label.</p>` +
    `<div class="scroll"><table class="tbl"><thead><tr><th>Discount code</th><th>Txn</th><th>Qty</th><th>Gross</th>` +
    `<th>Discount</th><th>Net</th><th>Net growth</th><th>Dis %</th><th>AOV</th></tr></thead><tbody>` +
    line("Total", tc, tp, "tot") +
    codes.map(k => line(DISC.labels[k], cur.get(k) || EMPTY_D, prev.get(k) || EMPTY_D)).join("") +
    `</tbody></table></div></section>`;

  const codeOk = r => DS.code === "all" || r[5] === +DS.code;
  const catOk = r => DS.cats === null || DS.cats.includes(DISC.items[r[6]][1]);
  const it = dGroup(DISC.irows, w.cur, 6, 1, r => codeOk(r) && catOk(r));
  const itemRows = [...it.entries()].sort((a, b) => b[1].net - a[1].net).map(([k, a]) =>
    `<tr><td>${escD(DISC.items[k][0])}</td><td>${escD(DISC.cats[DISC.items[k][1]])}</td><td>${num_(a.inv)}</td>` +
    `<td>${num_(a.qty)}</td><td>${dInr(a.gross)}</td><td>${dInr(a.disc)}</td><td>${dInr(a.net)}</td>` +
    `<td>${dPct(a.disc, a.gross)}</td></tr>`).join("");

  const q = DS.catSearch.trim().toLowerCase();
  const catOn = i => DS.cats === null || DS.cats.includes(i);
  const catMatches = DISC.cats.map((c, i) => [c, i]).filter(([c]) => !q || c.toLowerCase().includes(q));
  const catList = catMatches.slice(0, 300).map(([c, i]) =>
    `<label style="display:flex;gap:6px;align-items:center;padding:3px 0"><input type="checkbox" data-dcat="${i}"${catOn(i) ? " checked" : ""}> ${escD(c)}</label>`).join("") ||
    `<span class="sub">No category matches "${escD(DS.catSearch)}".</span>`;
  const selected = DS.cats === null ? DISC.cats.length : DS.cats.length;
  const codeOpts = `<option value="all">All discount codes</option>` + DISC.labels.map((v, i) =>
    `<option value="${i}"${String(DS.code) === String(i) ? " selected" : ""}>${escD(v)}</option>`).join("");
  const itemsBlock = `<section class="block"><h2>By item</h2>` +
    `<p class="sub">Pick a discount code and one or more categories. Type in the category box to find categories, for example 70%.</p>` +
    `<div class="fgrid" style="grid-template-columns:minmax(0,1fr) minmax(0,2fr);gap:10px">` +
    `<label class="field">Discount code<select data-dcode>${codeOpts}</select></label>` +
    `<div><label class="field">Category<input type="search" data-dcat-search value="${escD(DS.catSearch)}" placeholder="Search category, e.g. 70%"></label>` +
    `<div style="display:flex;gap:8px;align-items:center;margin:6px 0"><button type="button" data-dcat-all>All</button>` +
    `<button type="button" data-dcat-none>None</button><span class="sub">${selected} of ${DISC.cats.length} selected</span></div>` +
    `<div style="max-height:180px;overflow:auto;border:1px solid var(--line);border-radius:8px;padding:6px">${catList}</div></div></div>` +
    `<div class="scroll"><table class="tbl"><thead><tr><th>Item</th><th>Category</th><th>Txn</th><th>Qty</th><th>Gross</th>` +
    `<th>Discount</th><th>Net</th><th>Dis %</th></tr></thead><tbody>` +
    (itemRows || `<tr><td colspan="8" class="sub">No items for this code and these categories.</td></tr>`) +
    `</tbody></table></div></section>`;

  return seg + period + filters + codeTable + itemsBlock;
}

// Only the Discount Code tab uses these: hide the sales sub-tabs, the sales filters and the sales period bar.
function setDiscChrome(on) {
  const sub = document.querySelector("[data-sub]");
  const boxes = [sub && sub.parentElement, document.getElementById("fields"), document.getElementById("periodwrap")];
  for (const el of boxes) if (el) el.style.display = on ? "none" : "";
}

// Adds the Discount Code button right after Rating. It is added again after every redraw of the top row.
function injectDiscountTab() {
  const rating = document.querySelector('[data-top="rating"]');
  if (!rating) return;
  let btn = document.querySelector('[data-top="discount"]');
  if (!btn) {
    btn = rating.cloneNode(true);
    btn.dataset.top = "discount";
    btn.textContent = "Discount Code";
    rating.after(btn);
  }
  document.querySelectorAll("[data-top]").forEach(b => b.setAttribute("aria-pressed", String(b.dataset.top === S.top)));
  rating.parentElement.style.gridTemplateColumns = "repeat(5, minmax(0, 1fr))";
}

const renderDiscBase = render;
render = function () {
  if (!M) return renderDiscBase();
  if (S.top !== "discount") {
    setDiscChrome(false);
    renderDiscBase();
    injectDiscountTab();
    return;
  }
  setDiscChrome(true);
  injectDiscountTab();
  if (!DISC && !DISC_ERR) ensureDiscounts();
  document.getElementById("view").innerHTML = discountView();
};

document.addEventListener("change", e => {
  const t = e.target;
  if (t.dataset.dsf) { DS[t.dataset.dsf] = t.value; return render(); }
  if (t.hasAttribute("data-dsel")) { S.sel = +t.value; return render(); }
  if (t.hasAttribute("data-dcode")) { DS.code = t.value; return render(); }
  if (t.dataset.dcat !== undefined) {
    const i = +t.dataset.dcat;
    const base = DS.cats === null ? DISC.cats.map((_, j) => j) : DS.cats.slice();
    DS.cats = t.checked ? [...new Set([...base, i])] : base.filter(j => j !== i);
    return render();
  }
});
document.addEventListener("input", e => {
  if (!e.target.hasAttribute("data-dcat-search")) return;
  DS.catSearch = e.target.value;
  render();
  const el = document.querySelector("[data-dcat-search]");
  if (el) { el.focus(); el.setSelectionRange(el.value.length, el.value.length); }
});
document.addEventListener("click", e => {
  const b = e.target.closest("[data-dkind],[data-dcat-all],[data-dcat-none],[data-disc-retry]");
  if (!b) return;
  if (b.dataset.dkind) { DS.kind = b.dataset.dkind; S.sel = null; return render(); }
  if (b.hasAttribute("data-dcat-all")) { DS.cats = null; return render(); }
  if (b.hasAttribute("data-dcat-none")) { DS.cats = []; return render(); }
  if (b.hasAttribute("data-disc-retry")) { DISC_ERR = false; return ensureDiscounts(); }
});
