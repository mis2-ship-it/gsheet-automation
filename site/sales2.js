/* ============ Sales: same-weekday Last Month, Month to date, and the 30-day chart ============
   1. Daily "Last Month" is the same weekday as the selected day, in the previous month, nearest to the same date.
   2. A Month to date tab compares the current month (1st to the last day in the data) with the same days last month.
   3. The 30-day net sales chart has Brand and Source filters, a Net Rev / Offline % / Online % switch, and data labels.
   Loaded after the other sales scripts. */

// 1. Last Month for the daily view: same weekday, nearest to the same date, inside last month
function sameWeekdayLastMonth(key) {
  const dt = toDate(key);
  const y = dt.getUTCFullYear(), mo = dt.getUTCMonth(), day = dt.getUTCDate();
  const pk = (y - 2025) * 12 + mo - 1;
  const lo = monStart(pk), hi = monEnd(pk);
  const t = Math.min(lo + day - 1, hi);
  let off = (((dt.getUTCDay() - toDate(t).getUTCDay()) % 7) + 7) % 7;
  if (off > 3) off -= 7;
  let lm = t + off;
  if (lm > hi) lm -= 7;
  if (lm < lo) lm += 7;
  return lm;
}
const overallColsBase = overallCols;
overallCols = function (kind, key) {
  const c = overallColsBase(kind, key);
  if (kind === "daily") {
    const lm = sameWeekdayLastMonth(key);
    c.wins[3] = [lm, lm];
  }
  return c;
};

// 2. Month to date: this month (1st to the last day in the data) against the same days last month
SUBS.sales.push(["mtd", "Month to date"]);
const monthKeyOfDay = d => (toDate(d).getUTCFullYear() - 2025) * 12 + toDate(d).getUTCMonth();

function mtdBlock() {
  setPeriodBar("mtd", null, null);
  const last = M.last_day;
  const cm = monthKeyOfDay(last);
  const cur = [monStart(cm), last];
  const span = last - monStart(cm);
  const pm = cm - 1;
  const prev = [monStart(pm), Math.min(monStart(pm) + span, monEnd(pm))];
  const rows = baseSales();
  const c = metrics(aggFor(rows, cur[0], cur[1], salesPass));
  const p = metrics(aggFor(rows, prev[0], prev[1], salesPass));
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
  const overall = `<div class="scroll"><table class="tbl"><thead><tr><th>Parameters</th>
    <th>${fmtDay(cur[0])} to ${fmtDay(cur[1])}</th><th>${fmtDay(prev[0])} to ${fmtDay(prev[1])}</th><th>Growth</th></tr></thead><tbody>` +
    defs.map(([label, get, good, type]) => {
      const a = get(c), b = get(p);
      const chg = type === "pct" ? dlt(pts(a, b), " pts", good) : dlt(rel(a, b), "%", good);
      return `<tr><td>${label}</td><td>${fmt(a, type)}</td><td>${fmt(b, type)}</td><td>${chg}</td></tr>`;
    }).join("") + `</tbody></table></div>`;

  const dim = (title, pos) => {
    const ca = aggByDim(rows, cur[0], cur[1], pos, salesPass);
    const cb = aggByDim(rows, prev[0], prev[1], pos, salesPass);
    const ks = [...new Set([...ca.keys(), ...cb.keys()])]
      .filter(k => (ca.get(k) || EMPTY_A).net || (cb.get(k) || EMPTY_A).net)
      .sort((x, y) => (ca.get(y) || EMPTY_A).net - (ca.get(x) || EMPTY_A).net);
    const line = (name, a, b, cls) => {
      const am = metrics(a), bm = metrics(b);
      return `<tr class="${cls || ""}"><td>${name}</td><td>${num(am.net)}</td><td>${num(bm.net)}</td>` +
        `<td>${dlt(rel(am.net, bm.net), "%", true)}</td><td>${am.discPct.toFixed(1)}%</td><td>${bm.discPct.toFixed(1)}%</td>` +
        `<td>${dlt(pts(am.discPct, bm.discPct), " pts", false)}</td></tr>`;
    };
    const total = line("Total", aggFor(rows, cur[0], cur[1], salesPass), aggFor(rows, prev[0], prev[1], salesPass), "tot");
    const body = total + ks.map(k => line(nameOf(pos, k), ca.get(k) || EMPTY_A, cb.get(k) || EMPTY_A)).join("");
    return `<section class="block"><h2>By ${title}</h2><div class="scroll"><table class="tbl"><thead><tr><th></th>
      <th>Month to date</th><th>Last month, same days</th><th>Growth</th><th>Dis %</th><th>Last month Dis %</th><th>Dis change</th></tr></thead>
      <tbody>${body}</tbody></table></div></section>`;
  };
  return `<section class="block"><h2>Month to date, overall</h2>
      <p class="sub">${fmtDay(cur[0])} to ${fmtDay(cur[1])} against ${fmtDay(prev[0])} to ${fmtDay(prev[1])}. Values in ₹ except Txn and %.</p>${overall}</section>` +
    dim("brand", 2) + dim("region", 4) + dim("source", 5) + dim("session", 6);
}

// 3. The 30-day net sales chart: Brand and Source filters, a measure switch, and data labels
const CH = { brand: "all", source: "all", measure: "net" };
const ONLINE_COLOR = "#3B82F6";
const escT = t => String(t).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

function trendDaily(keys, idx, rows, pass) {
  const N = CHART_N.daily;
  const ck = keys.slice(Math.max(0, idx - N + 1), idx + 1);
  const lo = ck[0], hi = ck[ck.length - 1];
  const offIdx = M.sources.indexOf("In Store");
  const cp = r => pass(r) && (CH.brand === "all" || r[2] === +CH.brand) && (CH.source === "all" || r[5] === +CH.source);
  const per = new Map(ck.map(k => [k, { net: 0, src: new Array(M.sources.length).fill(0), off: 0, on: 0 }]));
  for (const r of rows) {
    if (r[0] < lo || r[0] > hi || !cp(r)) continue;
    const a = per.get(r[0]);
    if (!a) continue;
    a.net += r[7];
    a.src[r[5]] += r[7];
    if (r[5] === offIdx) a.off += r[7]; else a.on += r[7];
  }
  const W = 760, H = 190, TOP = 22, bw = W / ck.length;
  const maxNet = Math.max(1, ...ck.map(k => per.get(k).net));
  let body = "";
  ck.forEach((k, i) => {
    const a = per.get(k);
    const x = (i * bw + bw * 0.18).toFixed(1), w = (bw * 0.64).toFixed(1), cx = (i * bw + bw / 2).toFixed(1);
    const op = k === keys[idx] ? 1 : 0.65;
    let y = TOP + H, label;
    if (CH.measure === "net") {
      a.src.forEach((v, j) => {
        if (!v) return;
        const h = (v / maxNet) * H;
        y -= h;
        body += `<rect x="${x}" y="${y.toFixed(1)}" width="${w}" height="${h.toFixed(1)}" fill="${SRC_COLOR[j]}" opacity="${op}"/>`;
      });
      label = (a.net / 1e7).toFixed(1);
    } else {
      const share = a.net ? ((CH.measure === "off" ? a.off : a.on) / a.net) * 100 : 0;
      const h = (share / 100) * H;
      y = TOP + H - h;
      body += `<rect x="${x}" y="${y.toFixed(1)}" width="${w}" height="${h.toFixed(1)}" ` +
        `fill="${CH.measure === "off" ? SRC_COLOR[offIdx] : ONLINE_COLOR}" opacity="${op}"/>`;
      label = share.toFixed(1);
    }
    body += `<text x="${cx}" y="${(y - 5).toFixed(1)}" text-anchor="middle" font-size="9.5" style="fill:var(--ink)">${label}</text>`;
    body += `<rect data-k="${k}" x="${(i * bw).toFixed(1)}" y="0" width="${bw.toFixed(1)}" height="${TOP + H}" fill="transparent"/>`;
  });
  const title = CH.measure === "net" ? "Net sales, last 30 days (labels in ₹ lakh)"
    : CH.measure === "off" ? "Offline share of net sales, last 30 days (labels in %)"
    : "Online share of net sales, last 30 days (labels in %)";
  const brandOpts = `<option value="all">All brands</option>` + M.brands.map((b, i) =>
    `<option value="${i}" ${String(CH.brand) === String(i) ? "selected" : ""}>${escT(b)}</option>`).join("");
  const srcOpts = `<option value="all">All sources</option>` + M.sources.map((s, i) =>
    `<option value="${i}" ${String(CH.source) === String(i) ? "selected" : ""}>${s === "In Store" ? "Offline (in store)" : escT(s)}</option>`).join("");
  const mBtns = [["net", "Net Rev"], ["off", "Offline %"], ["on", "Online %"]].map(([id, l]) =>
    `<button data-chm="${id}" aria-pressed="${CH.measure === id}">${l}</button>`).join("");
  const legend = CH.measure === "net" ? M.sources.map((n, j) =>
    `<span><i class="sw" style="--c:${SRC_COLOR[j]}"></i>${n === "In Store" ? "Offline (in store)" : escT(n)}</span>`).join("") : "";
  const mid = ck[Math.floor(ck.length / 2)];
  return `<section class="block"><h2>${title}</h2>
    <div class="chartctl">
      <label class="field">Brand<select data-ch="brand">${brandOpts}</select></label>
      <label class="field">Source<select data-ch="source">${srcOpts}</select></label>
      <div class="seg3" role="group" aria-label="Measure" style="grid-template-columns:repeat(3,1fr)">${mBtns}</div>
    </div>
    <div class="hourwrap"><svg class="chart" viewBox="0 0 ${W} ${TOP + H}" role="img" aria-label="${title}">${body}</svg></div>
    <div class="axis"><span>${fmtDay(ck[0])}</span><span>${fmtDay(mid)}</span><span>${fmtDay(ck[ck.length - 1])}</span></div>
    <div class="legend">${legend}</div>
    <p class="muted" style="margin-top:8px">Tap a bar to open that day.</p></section>`;
}

const trendBlockBase = trendBlock;
trendBlock = function (kind, keys, idx, rows, pass) {
  return kind === "daily" ? trendDaily(keys, idx, rows, pass) : trendBlockBase(kind, keys, idx, rows, pass);
};
const salesPeriodViewBase2 = salesPeriodView;
salesPeriodView = function (kind) {
  return kind === "mtd" ? mtdBlock() : salesPeriodViewBase2(kind);
};

document.addEventListener("change", e => {
  const k = e.target.dataset.ch;
  if (!k) return;
  CH[k] = e.target.value;
  render();
});
document.addEventListener("click", e => {
  const b = e.target.closest("[data-chm]");
  if (!b) return;
  CH.measure = b.dataset.chm;
  render();
});
