/* ============ Rating tab: month A against month B ============
   One screen for Swiggy, Zomato and Google. Brand and region tables with the average rating,
   the 1-5 star split, BT (1 and 2 stars) and Awesome (5 stars), rated orders and rated issue orders,
   and the hygiene issue contribution.
   Rated orders = Orders Count of the rows that have a rating.
   Hygiene shares are out of rated issue orders (the same count shown as "Rated issue orders").
   Loaded after the main page script. It replaces ratingView(). */

const RM = { a: null, b: null };
const RAT_LEN = 21;   // row: day, store, brand, type, region, platform, rated, rating_sum_x100, orders, s1..s5, issues, c0..c5
const HYGIENE = ["Quantity Issue", "Quality Issue", "Packing Issue", "Icecream Fully melted", "Wrong Item", "Missing Item"];

const monLabel = k => `${MON[k % 12]}-${String(2025 + Math.floor(k / 12)).slice(2)}`;
function ratingMonths() {
  const set = new Set();
  for (const r of RAT.rows) set.add(DAY_MON[r[0]]);
  return [...set].sort((a, b) => a - b);
}
function rTotals(platform, from, to, test) {
  const t = new Array(RAT_LEN).fill(0);
  for (const r of RAT.rows) {
    if (r[0] < from || r[0] > to || !ratingPass(r, platform) || !test(r)) continue;
    for (let i = 6; i < RAT_LEN; i++) t[i] += r[i];
  }
  return t;
}
function rMetrics(t) {
  const rated = t[6];
  const st = [t[9], t[10], t[11], t[12], t[13]];
  return {
    rated,
    orders: t[8],
    avg: rated ? t[7] / 100 / rated : null,
    star: st.map(s => (rated ? (s / rated) * 100 : null)),
    bt: rated ? ((st[0] + st[1]) / rated) * 100 : null,
    awe: rated ? (st[4] / rated) * 100 : null,
    issues: t[14],
    cats: t.slice(15, 21)
  };
}
// Rows in the order of the sheet: brand block, then region block with each region's brands under it.
function ratingRows(platform, A, B) {
  const wa = [monStart(A), monEnd(A)], wb = [monStart(B), monEnd(B)];
  const rows = [];
  const push = (group, label, indent, test) => {
    const ta = rTotals(platform, wa[0], wa[1], test), tb = rTotals(platform, wb[0], wb[1], test);
    if (indent && !ta[6] && !tb[6]) return;
    rows.push({ group, label, indent, a: rMetrics(ta), b: rMetrics(tb) });
  };
  rows.push({ section: "By brand" });
  push("brand", "Average", 0, () => true);
  M.brands.forEach((name, bi) => push("brand", name, 1, r => r[2] === bi));
  rows.push({ section: "By region" });
  push("region", "Average", 0, () => true);
  M.regions.forEach((name, ri) => {
    push("region", name, 1, r => r[4] === ri);
    M.brands.forEach((bn, bi) => push("region", bn, 2, r => r[4] === ri && r[2] === bi));
  });
  return rows;
}
const f1 = v => (v === null ? "—" : v.toFixed(1) + "%");
const f2 = v => (v === null ? "—" : v.toFixed(2));
const ch = (a, b, unit, good, digits = 1) => (a === null || b === null ? "—" : dlt(b - a, unit, good, digits));
function rTable(head, rows, cells, ncols) {
  const body = rows.map(row => {
    if (row.section) return `<tr><td colspan="${ncols}" style="font-weight:600;color:var(--ink-2);padding-top:14px">${row.section}</td></tr>`;
    const pad = row.indent === 2 ? "padding-left:20px" : row.indent === 1 ? "padding-left:8px" : "";
    const weight = row.indent === 0 ? "font-weight:600" : "";
    return `<tr><td style="${pad};${weight}">${row.label}</td>${cells(row)}</tr>`;
  }).join("");
  return `<div class="scroll"><table class="tbl"><thead>${head}</thead><tbody>${body}</tbody></table></div>`;
}
function ratingView(platformName) {
  setPeriodBar("month", null, null, false);
  if (!M.has_ratings || !RAT) {
    return `<section class="empty"><h2>${platformName} ratings</h2>
      <p>${platformName} ratings are not connected yet. Once the ratings sync runs, this page shows the average rating, the star split, BT and Awesome, rated orders, rated issue orders and the hygiene issues.</p>
      <p class="muted">The filters above are ready and will apply once the data arrives.</p></section>`;
  }
  const platform = M.platforms.indexOf(platformName);
  const months = ratingMonths();
  if (!months.length) return `<p class="muted" style="margin-top:16px">No rating months yet.</p>`;
  if (RM.a === null || !months.includes(RM.a)) RM.a = months.length > 1 ? months[months.length - 2] : months[0];
  if (RM.b === null || !months.includes(RM.b)) RM.b = months[months.length - 1];
  const A = RM.a, B = RM.b, la = monLabel(A), lb = monLabel(B);
  const isGoogle = platformName === "Google";
  const rows = ratingRows(platform, A, B);
  const monthOpts = sel => months.map(k => `<option value="${k}" ${k === sel ? "selected" : ""}>${monLabel(k)}</option>`).join("");
  const picker = `<div class="fgrid" style="margin-top:14px">
    <label class="field">Month A<select data-rm="a">${monthOpts(A)}</select></label>
    <label class="field">Month B<select data-rm="b">${monthOpts(B)}</select></label></div>`;
  const intro = `<p class="sub" style="margin-top:12px">Rated orders are the Orders Count of rows that have a rating. BT is 1 and 2 stars, Awesome is 5 stars. Hygiene shares are out of rated issue orders in ${lb}.</p>`;

  const t1 = rTable(`<tr><th>Average rating</th><th>${la}</th><th>${lb}</th><th>Change</th></tr>`, rows,
    row => `<td>${f2(row.a.avg)}</td><td>${f2(row.b.avg)}</td><td>${ch(row.a.avg, row.b.avg, " pts", true, 2)}</td>`, 4);

  const t2 = rTable(`<tr><th></th><th colspan="5">${la}</th><th colspan="5">${lb}</th></tr>
    <tr><th>Name</th>${[1, 2, 3, 4, 5].map(k => `<th>${k}★</th>`).join("")}${[1, 2, 3, 4, 5].map(k => `<th>${k}★</th>`).join("")}</tr>`, rows,
    row => row.a.star.map(v => `<td>${f1(v)}</td>`).join("") + row.b.star.map(v => `<td>${f1(v)}</td>`).join(""), 11);

  const t3 = rTable(`<tr><th></th><th colspan="3">BT (1–2★)</th><th colspan="3">Awesome (5★)</th></tr>
    <tr><th>Name</th><th>${la}</th><th>${lb}</th><th>Change</th><th>${la}</th><th>${lb}</th><th>Change</th></tr>`, rows,
    row => `<td>${f1(row.a.bt)}</td><td>${f1(row.b.bt)}</td><td>${row.a.bt === null || row.b.bt === null ? "—" : dlt(row.b.bt - row.a.bt, " pts", false)}</td>
            <td>${f1(row.a.awe)}</td><td>${f1(row.b.awe)}</td><td>${row.a.awe === null || row.b.awe === null ? "—" : dlt(row.b.awe - row.a.awe, " pts", true)}</td>`, 7);

  const t4 = rTable(`<tr><th></th><th colspan="3">${isGoogle ? "Reviews" : "Rated orders"}</th><th colspan="3">${isGoogle ? "Issues" : "Rated issue orders"}</th></tr>
    <tr><th>Name</th><th>${la}</th><th>${lb}</th><th>Change</th><th>${la}</th><th>${lb}</th><th>Change</th></tr>`, rows,
    row => `<td>${num(row.a.rated)}</td><td>${num(row.b.rated)}</td><td>${row.a.rated && row.b.rated ? dlt(rel(row.b.rated, row.a.rated), "%", true) : "—"}</td>` +
      (isGoogle ? `<td colspan="3" class="muted">not available</td>`
        : `<td>${num(row.a.issues)}</td><td>${num(row.b.issues)}</td><td>${row.a.issues && row.b.issues ? dlt(rel(row.b.issues, row.a.issues), "%", false) : "—"}</td>`), 7);

  const t5 = isGoogle ? "" : rTable(`<tr><th>Hygiene issue</th>${HYGIENE.map(h => `<th>${h}</th>`).join("")}</tr>
    <tr><th>Share of rated issue orders, ${lb}</th>${HYGIENE.map(() => `<th>% || orders</th>`).join("")}</tr>`, rows,
    row => row.b.cats.map(n => {
      const pct = row.b.issues ? (n / row.b.issues) * 100 : null;
      return `<td>${pct === null ? "—" : pct.toFixed(1) + "% <span class=\"muted\">|| " + num(n) + "</span>"}</td>`;
    }).join(""), 7);

  return picker + intro +
    `<section class="block"><h2>Average rating</h2>${t1}</section>` +
    `<section class="block"><h2>BT and Awesome</h2>${t3}</section>` +
    `<section class="block"><h2>Rated orders and rated issue orders</h2>${t4}</section>` +
    (isGoogle ? "" : `<section class="block"><h2>Hygienic issue contribution</h2>${t5}</section>`);
}
document.addEventListener("change", e => {
  const k = e.target.dataset.rm;
  if (!k) return;
  RM[k] = +e.target.value;
  render();
});
