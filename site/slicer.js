/* ============ Summary tables: measure slicer ============
   Each "By brand / region / source / session" table can show Net Rev, Orders, Dis % or AOV.
   Rows are ordered by the chosen measure. The choice stays when you move between periods.
   The Overall table and the AOV and discount buckets are unchanged.
   Loaded after the main page script. It replaces summaryTable(). */

const SLICER = { metric: "net" };
const MEASURES = [
  { id: "net", label: "Net Rev", val: m => m.net, fmt: v => num(v), chg: (c, p) => rel(c, p), unit: "%", good: true },
  { id: "orders", label: "Orders", val: m => m.txn, fmt: v => num(v), chg: (c, p) => rel(c, p), unit: "%", good: true },
  { id: "dis", label: "Dis %", val: m => m.discPct, fmt: v => v.toFixed(1) + "%", chg: (c, p) => pts(c, p), unit: " pts", good: false },
  { id: "aov", label: "AOV", val: m => m.aov, fmt: v => inr(v), chg: (c, p) => rel(c, p), unit: "%", good: true }
];

summaryTable = function (kind, key, pos) {
  const w = sumWins(kind, key);
  const meas = MEASURES.find(x => x.id === SLICER.metric) || MEASURES[0];
  const rows = baseSales();
  const cur = aggByDim(rows, w.cur[0], w.cur[1], pos, salesPass);
  const prev = aggByDim(rows, w.prev[0], w.prev[1], pos, salesPass);
  const keysAll = [...new Set([...cur.keys(), ...prev.keys()])]
    .filter(k => (cur.get(k) || EMPTY_A).net || (prev.get(k) || EMPTY_A).net)
    .sort((a, b) => meas.val(metrics(cur.get(b) || EMPTY_A)) - meas.val(metrics(cur.get(a) || EMPTY_A)));
  const line = (name, c, p, cls) => {
    const cv = meas.val(metrics(c)), pv = meas.val(metrics(p));
    return `<tr class="${cls || ""}"><td>${name}</td><td>${meas.fmt(cv)}</td><td>${meas.fmt(pv)}</td>` +
      `<td>${dlt(meas.chg(cv, pv), meas.unit, meas.good)}</td></tr>`;
  };
  const total = line("Total", aggFor(rows, w.cur[0], w.cur[1], salesPass), aggFor(rows, w.prev[0], w.prev[1], salesPass), "tot");
  const body = total + keysAll.map(k => line(nameOf(pos, k), cur.get(k) || EMPTY_A, prev.get(k) || EMPTY_A)).join("");
  const seg = MEASURES.map(x =>
    `<button data-metric="${x.id}" aria-pressed="${x.id === meas.id}">${x.label}</button>`).join("");
  return `<div class="seg4" role="group" aria-label="Measure">${seg}</div>
    <div class="scroll"><table class="tbl">
    <thead><tr><th></th><th>${w.c} ${meas.label}</th><th>${w.p} ${meas.label}</th><th>${meas.id === "dis" ? "Change" : "Growth %"}</th></tr></thead>
    <tbody>${body}</tbody></table></div>`;
};

document.addEventListener("click", e => {
  const b = e.target.closest("[data-metric]");
  if (!b) return;
  SLICER.metric = b.dataset.metric;
  render();
});
