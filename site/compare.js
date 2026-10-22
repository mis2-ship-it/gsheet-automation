/* ============ Sales, monthly: Month A against Month B ============
   Shown above the monthly view, with the same filters. Overall, then brand, region, source and session.
   Loaded after the main page script. It wraps salesPeriodView(). */

const CM = { a: null, b: null };

function monthCompareBlock() {
  const keys = periodKeys("monthly");
  if (!keys.length) return "";
  if (CM.a === null || !keys.includes(CM.a)) CM.a = keys.length > 1 ? keys[keys.length - 2] : keys[0];
  if (CM.b === null || !keys.includes(CM.b)) CM.b = keys[keys.length - 1];
  const A = CM.a, B = CM.b;
  const wa = [monStart(A), monEnd(A)], wb = [monStart(B), monEnd(B)];
  const rows = baseSales();
  const ma = metrics(aggFor(rows, wa[0], wa[1], salesPass));
  const mb = metrics(aggFor(rows, wb[0], wb[1], salesPass));
  const opts = sel => keys.map(k => `<option value="${k}" ${k === sel ? "selected" : ""}>${monLong(k)}</option>`).join("");
  const picker = `<div class="fgrid" style="margin-top:14px">
    <label class="field">Month A<select data-cm="a">${opts(A)}</select></label>
    <label class="field">Month B<select data-cm="b">${opts(B)}</select></label></div>`;

  const defs = [
    ["Gross (₹)", m => m.gross, true, "money"],
    ["Discount (₹)", m => m.disc, false, "money"],
    ["Net (₹)", m => m.net, true, "money"],
    ["Txn", m => m.txn, true, "count"],
    ["AOV (₹)", m => m.aov, true, "money"],
    ["Discount %", m => m.discPct, false, "pct"],
    ["Offline %", m => m.offPct, null, "pct"],
    ["Online %", m => m.onPct, null, "pct"]
  ];
  const fmt = (v, t) => (t === "pct" ? v.toFixed(1) + "%" : num(v));
  const overall = `<div class="scroll"><table class="tbl">
    <thead><tr><th>Parameters</th><th>${monShort(A)}</th><th>${monShort(B)}</th><th>Change</th></tr></thead><tbody>` +
    defs.map(([label, get, good, type]) => {
      const a = get(ma), b = get(mb);
      const chg = type === "pct" ? dlt(pts(b, a), " pts", good) : dlt(rel(b, a), "%", good);
      return `<tr><td>${label}</td><td>${fmt(a, type)}</td><td>${fmt(b, type)}</td><td>${chg}</td></tr>`;
    }).join("") + `</tbody></table></div>`;

  const dimTable = (title, pos) => {
    const ca = aggByDim(rows, wa[0], wa[1], pos, salesPass);
    const cb = aggByDim(rows, wb[0], wb[1], pos, salesPass);
    const keysD = [...new Set([...ca.keys(), ...cb.keys()])]
      .filter(k => (ca.get(k) || EMPTY_A).net || (cb.get(k) || EMPTY_A).net)
      .sort((x, y) => (cb.get(y) || EMPTY_A).net - (cb.get(x) || EMPTY_A).net);
    const line = (name, a, b, cls) => {
      const am = metrics(a), bm = metrics(b);
      return `<tr class="${cls || ""}"><td>${name}</td><td>${num(am.net)}</td><td>${num(bm.net)}</td>
        <td>${dlt(rel(bm.net, am.net), "%", true)}</td><td>${am.discPct.toFixed(1)}%</td><td>${bm.discPct.toFixed(1)}%</td>
        <td>${dlt(pts(bm.discPct, am.discPct), " pts", false)}</td></tr>`;
    };
    const total = line("Total", aggFor(rows, wa[0], wa[1], salesPass), aggFor(rows, wb[0], wb[1], salesPass), "tot");
    const body = total + keysD.map(k => line(nameOf(pos, k), ca.get(k) || EMPTY_A, cb.get(k) || EMPTY_A)).join("");
    return `<section class="block"><h2>By ${title}: ${monShort(A)} against ${monShort(B)}</h2>
      <div class="scroll"><table class="tbl"><thead><tr><th></th><th>${monShort(A)}</th><th>${monShort(B)}</th><th>Growth</th>
      <th>Dis % ${monShort(A)}</th><th>Dis % ${monShort(B)}</th><th>Dis change</th></tr></thead>
      <tbody>${body}</tbody></table></div></section>`;
  };

  return `<section class="block"><h2>Month comparison</h2>
      <p class="sub">Revenue in ₹. The filters above apply. Brackets and changes compare Month B with Month A.</p>${picker}</section>
    <section class="block"><h2>Overall: ${monShort(A)} against ${monShort(B)}</h2>${overall}</section>
    ${dimTable("brand", 2)}${dimTable("region", 4)}${dimTable("source", 5)}${dimTable("session", 6)}`;
}

const salesPeriodViewBase = salesPeriodView;
salesPeriodView = function (kind) {
  const base = salesPeriodViewBase(kind);
  if (kind !== "monthly") return base;
  return monthCompareBlock() + base;
};

document.addEventListener("change", e => {
  const k = e.target.dataset.cm;
  if (!k) return;
  CM[k] = +e.target.value;
  render();
});
