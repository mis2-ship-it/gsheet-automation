/* ============ Sales, Monthly and Weekly: Latest, Last 2, Last 3 and Last 6 periods ============
   A block at the top of the Monthly and Weekly views. Choose Latest, Last 2, Last 3 or Last 6 periods.
   Each period is a column, oldest on the left. Each figure shows its change against the period before it
   (the first column is compared with the period before the range). The filters above apply.
   Loaded after sales2.js. */

const PR = { monthly: 3, weekly: 3 };
const PR_OPTS = [1, 2, 3, 6];
const PR_LABEL = {
  monthly: n => (n === 1 ? "Latest month" : `Last ${n} months`),
  weekly: n => (n === 1 ? "Latest week" : `Last ${n} weeks`)
};

function periodRangeBlock(kind) {
  const keys = periodKeys(kind);
  if (!keys.length) return "";
  const key = S.sel !== null && keys.includes(S.sel) ? S.sel : keys[keys.length - 1];
  const idx = keys.indexOf(key);
  const n = PR[kind];
  const sel = keys.slice(Math.max(0, idx - n + 1), idx + 1);   // oldest first
  const beforeKey = idx - n >= 0 ? keys[idx - n] : null;
  const rows = baseSales();
  const agg = k => { const [f, t] = windowOf(kind, k); return metrics(aggFor(rows, f, t, salesPass)); };
  const cols = sel.map(k => ({ k, m: agg(k) }));
  const before = beforeKey !== null ? agg(beforeKey) : null;
  const label = k => (kind === "monthly" ? monShort(k) : fmtDay(k));
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
  const body = defs.map(([name, get, good, type]) => {
    const cells = cols.map((c, j) => {
      const prev = j > 0 ? cols[j - 1].m : before;
      const v = get(c.m);
      let g = "";
      if (prev) {
        const pv = get(prev);
        const d = type === "pct" ? pts(v, pv) : rel(v, pv);
        if (d !== null && d !== undefined) g = " " + dlt(d, type === "pct" ? " pts" : "%", good);
      }
      return `<td>${fmt(v, type)}${g}</td>`;
    }).join("");
    return `<tr><td>${name}</td>${cells}</tr>`;
  }).join("");
  const head = cols.map(c => `<th>${label(c.k)}</th>`).join("");
  const opts = PR_OPTS.map(x => `<button data-pr="${x}" data-pr-kind="${kind}" aria-pressed="${PR[kind] === x}">${PR_LABEL[kind](x)}</button>`).join("");
  const span = cols.length ? `${label(cols[0].k)} to ${label(cols[cols.length - 1].k)}` : "";
  return `<section class="block"><h2>${PR_LABEL[kind](n)}</h2>
    <p class="sub">${span}. Each figure shows the change against the period before it. Values in ₹ except Txn and %.</p>
    <div class="seg4 prsel" role="group" aria-label="Period range">${opts}</div>
    <div class="scroll"><table class="tbl"><thead><tr><th>Parameters</th>${head}</tr></thead><tbody>${body}</tbody></table></div></section>`;
}

const salesPeriodViewBase3 = salesPeriodView;
salesPeriodView = function (kind) {
  const base = salesPeriodViewBase3(kind);
  return kind === "monthly" || kind === "weekly" ? periodRangeBlock(kind) + base : base;
};

document.addEventListener("click", e => {
  const b = e.target.closest("[data-pr]");
  if (!b) return;
  PR[b.dataset.prKind] = +b.dataset.pr;
  render();
});
