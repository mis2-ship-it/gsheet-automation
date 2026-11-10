/* ============ Sales, monthly: comparison chart (DoD %, WoW %, MoM %, YoY %) ============
   Shown above the monthly view, with the page's filters (type, brand, region, source, session and store).
   Metric: Sales (net ₹), Transactions, Discount (₹) or Dis % (the discount share of gross, shown as a value).
   DoD %  each day against the same weekday last week. Mon–Sun tabs pick the weekdays to follow.
   WoW %  each Mon–Sun week against the week before (shown on its Sunday).
   MoM %  against the previous month, day by day (same day of the month).
   YoY %  against the same month last year, day by day.
   The current month is drawn with its comparable month, and the rest of the month is forecast (dotted line).
   Loaded last, after the other sales scripts. It wraps salesPeriodView(), so the block sits above the monthly view. */
const CHG = { k: null, metric: "sales", cmp: "dod", wd: [0, 1, 2, 3, 4, 5, 6] };
const CHG_METRICS = [["sales", "Sales (net ₹)"], ["txn", "Transactions"], ["disc", "Discount (₹)"], ["discPct", "Dis %"]];
const CHG_CMPS = [["dod", "DoD %"], ["wow", "WoW %"], ["mom", "MoM %"], ["yoy", "YoY %"]];
const CHG_WD = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
// Holidays drawn as a band on the chart. Confirm the dates for each year before relying on them.
const CHG_HOLIDAYS = [{ from: "2026-11-07", to: "2026-11-09", name: "Diwali" }];
// Colours: dark enough to read on white at small sizes.
const CHG_C = { cur: "#1d4ed8", cmp: "#c2410c", text: "#0f172a", axis: "#334155", sub: "#475569", grid: "#cbd5e1", up: "#166534", down: "#b91c1c" };

// Every month with data, the current one included (the last one may still be in progress).
function chgKeys() {
  const out = [];
  for (let k = 0; monStart(k) <= M.last_day; k++) out.push(k);
  return out;
}

// One pass over the rows: each day's totals, with the page's filters applied. Money is in paise in the rows.
function chgDailyMap(lo, hi) {
  const m = new Map();
  for (const r of baseSales()) {
    if (r[0] < lo || r[0] > hi || !salesPass(r)) continue;
    let a = m.get(r[0]);
    if (!a) m.set(r[0], (a = { net: 0, gross: 0, disc: 0, orders: 0 }));
    a.net += r[7]; a.gross += r[8]; a.disc += r[9]; a.orders += r[10];
  }
  return m;
}
// Totals over a range of days. has = false when no day in the range has any sales row (no data is not a zero).
function chgSum(m, from, to) {
  const a = { net: 0, gross: 0, disc: 0, orders: 0, has: false };
  for (let d = from; d <= to; d++) {
    const x = m.get(d);
    if (!x) continue;
    a.has = true; a.net += x.net; a.gross += x.gross; a.disc += x.disc; a.orders += x.orders;
  }
  return a;
}
// The figure for the chosen metric: rupees, a count, or a percentage. null when there is no data.
function chgVal(a, metric) {
  if (!a || !a.has) return null;
  if (metric === "txn") return a.orders;
  if (metric === "disc") return a.disc / 100;
  if (metric === "discPct") return a.gross ? (a.disc / a.gross) * 100 : null;
  return a.net / 100;
}
// Change from prev to cur, as a percentage. null when either side is missing.
function chgChange(cur, prev, metric) {
  if (cur == null || prev == null || !prev) return null;
  return (cur / prev - 1) * 100;
}
// ISO week number of the week that starts on Monday day w (WK 45 = 2–8 Nov 2026).
function chgIsoWeek(w) {
  const thu = w + 3;
  const y = toDate(thu).getUTCFullYear();
  return Math.floor((thu - dayOf(Date.UTC(y, 0, 1))) / 7) + 1;
}
function chgDayOfIso(s) {
  const [y, m, d] = s.split("-").map(Number);
  return dayOf(Date.UTC(y, m - 1, d));
}

// Everything the block shows for one month: the days (actual, comparable and forecast), the weeks, and the totals.
function chgModel(k) {
  const ms = monStart(k), me = monEnd(k), L = me - ms + 1;
  const complete = me <= M.last_day;
  const lastIdx = Math.min(L - 1, M.last_day - ms);           // index of the last day with data in this month
  const pct = CHG.metric === "discPct", metric = CHG.metric, cmp = CHG.cmp;
  const cmpK = cmp === "yoy" ? k - 12 : k - 1;                // the comparable month
  const cms = monStart(cmpK), cmpL = monEnd(cmpK) - cms + 1;
  const m = chgDailyMap(Math.max(0, monStart(k - 12) - 7), Math.min(M.last_day, mondayOf(me) + 6));
  const val = d => (d < 0 || d > M.last_day) ? null : chgVal(chgSum(m, d, d), metric);
  const range = (a, b) => chgVal(chgSum(m, a, b), metric);
  const lastDay = Math.min(me, M.last_day);

  const cur = [], comp = [], wdIdx = [];
  for (let i = 0; i < L; i++) {
    cur.push(i <= lastIdx ? val(ms + i) : null);
    comp.push(i < cmpL ? val(cms + i) : null);
    wdIdx.push((toDate(ms + i).getUTCDay() + 6) % 7);
  }

  // Mon–Sun weeks that start in the month (the last one may run past the last day with data).
  const weeks = [];
  for (let w = mondayOf(ms); w <= lastDay; w += 7) {
    const c = range(w, w + 6), p = range(w - 7, w - 1);
    weeks.push({ w, end: w + 6, cur: c, label: pct ? c : chgChange(c, p, metric), partial: w + 6 > M.last_day, isoWk: chgIsoWeek(w), prevAvail: p != null });
  }

  // Labels: the growth at each point, or the Dis % value for Dis %.
  const labels = [];
  for (let i = 0; i < L; i++) {
    let v = null;
    if (cur[i] != null) {
      if (pct) v = cur[i];
      else if (cmp === "dod") v = chgChange(cur[i], val(ms + i - 7), metric);
      else if (cmp === "mom" || cmp === "yoy") v = chgChange(cur[i], comp[i], metric);
    }
    labels.push(v);
  }
  if (cmp === "wow") {
    labels.fill(null);
    for (const wk of weeks) { const i = wk.end - ms; if (i >= 0 && i < L && !wk.partial) labels[i] = wk.label; }
  }

  // Totals. A month in progress is compared with the same days of the comparable month.
  const curTotal = range(ms, ms + lastIdx);
  const compFull = range(cms, cms + cmpL - 1);
  const compSame = range(cms, cms + Math.min(lastIdx, cmpL - 1));
  const curGrowth = pct ? null : chgChange(curTotal, complete ? compFull : compSame, metric);

  // Forecast for the rest of the month: the comparable month's day pattern, scaled to this month's pace so far.
  const fc = new Array(L).fill(null);
  let fcTotal = null, fcGrowth = null, offset = null;
  if (!complete) {
    if (pct) {
      offset = curTotal != null && compSame != null ? curTotal - compSame : null;
      for (let i = lastIdx + 1; i < L; i++) fc[i] = comp[i] != null && offset != null ? comp[i] + offset : null;
      fcTotal = compFull != null && offset != null ? compFull + offset : null;
    } else {
      let a = 0, b = 0;
      for (let i = 0; i <= lastIdx; i++) {
        if (cur[i] != null) a += cur[i];
        if (i < cmpL && comp[i] != null) b += comp[i];
      }
      const ratio = b > 0 && a > 0 ? a / b : null;
      const avg = a > 0 && lastIdx >= 0 ? a / (lastIdx + 1) : null;
      let rest = 0;
      for (let i = lastIdx + 1; i < L; i++) {
        fc[i] = comp[i] != null && ratio != null ? comp[i] * ratio : avg;
        if (fc[i] != null) rest += fc[i];
      }
      fcTotal = curTotal != null ? curTotal + rest : null;
      fcGrowth = chgChange(fcTotal, compFull, metric);
    }
  }

  // Weekday tabs (DoD): the selected days so far, against the same days a week earlier.
  let selSum = null;
  if (cmp === "dod") {
    const days = [];
    for (let i = 0; i <= lastIdx; i++) if (CHG.wd.includes(wdIdx[i])) days.push(i);
    if (pct) {
      let g = 0, dd = 0;
      for (const i of days) { const a = chgSum(m, ms + i, ms + i); g += a.gross; dd += a.disc; }
      selSum = { days: days.length, pct: g ? (dd / g) * 100 : null };
    } else {
      let c = 0, p = 0, n = 0;
      for (const i of days) {
        const pv = val(ms + i - 7);
        if (cur[i] != null && pv != null) { c += cur[i]; p += pv; n++; }
      }
      selSum = { days: days.length, n, cur: c, prev: p, growth: p ? (c / p - 1) * 100 : null };
    }
  }

  const last = M.last_day;
  const lastInMonth = last >= ms && last <= me;
  return {
    k, ms, me, L, complete, lastIdx, metric, cmp, pct, cmpK, cur, comp, fc, labels, wdIdx, weeks, selSum,
    holidays: CHG_HOLIDAYS.map(h => ({ name: h.name, a: chgDayOfIso(h.from), b: chgDayOfIso(h.to) })),
    curTotal, compFull, compSame, curGrowth, fcTotal, fcGrowth,
    last: lastInMonth ? { d: last, cur: val(last), growth: pct ? null : chgChange(val(last), val(last - 7), metric) } : null
  };
}

const chgFmtVal = (v, metric) => v == null ? "—"
  : metric === "txn" ? Math.round(v).toLocaleString("en-IN")
  : metric === "discPct" ? v.toFixed(1) + "%"
  : "₹" + (v / 100000).toFixed(2) + " L";
const chgFmtChg = v => v == null ? "—" : (v >= 0 ? "+" : "−") + Math.abs(v).toFixed(1) + "%";
const chgColor = v => v == null ? CHG_C.sub : v >= 0 ? CHG_C.up : CHG_C.down;
const chgAxis = (v, metric) => metric === "txn" ? Math.round(v) + ""
  : metric === "discPct" ? v.toFixed(1) + "%"
  : "₹" + (v / 100000).toFixed(1) + "L";

// The daily chart: current month, comparable month (dashed) and forecast (dotted), with the labels.
function chgChart(g) {
  const W = 360, H = 200, L0 = 40, R0 = 10, T0 = 14, B0 = 30;
  const n = g.L;
  const sel = i => g.cmp !== "dod" || CHG.wd.includes(g.wdIdx[i]);
  const series = [];
  for (const arr of [g.cur, g.comp, g.fc]) arr.forEach((v, i) => { if (v != null && sel(i)) series.push(v); });
  if (!series.length) return "<p class=\"chg-note\">No sales for these days with these filters.</p>";
  let lo = Math.min(...series), hi = Math.max(...series);
  if (hi === lo) hi = lo + 1;
  const pad = (hi - lo) * 0.12;
  lo = Math.max(0, lo - pad); hi += pad;
  const x = i => L0 + (n <= 1 ? 0 : i * (W - L0 - R0) / (n - 1));
  const y = v => T0 + (1 - (v - lo) / (hi - lo)) * (H - T0 - B0);
  let s = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="Daily ${g.metric}: current month, comparable month and forecast">`;
  for (const h of g.holidays) {
    const a = Math.max(h.a, g.ms), b = Math.min(h.b, g.me);
    if (a > b) continue;
    const x1 = x(a - g.ms), x2 = x(b - g.ms);
    s += `<rect x="${(x1 - 5).toFixed(1)}" y="${T0}" width="${(x2 - x1 + 10).toFixed(1)}" height="${H - T0 - B0}" fill="#fde68a" opacity=".5"/>`;
    s += `<text x="${((x1 + x2) / 2).toFixed(1)}" y="${T0 + 9}" text-anchor="middle" font-size="8" fill="#78350f">${h.name}</text>`;
  }
  for (let i = 0; i <= 3; i++) {
    const v = lo + (hi - lo) * i / 3;
    s += `<line x1="${L0}" x2="${W - R0}" y1="${y(v).toFixed(1)}" y2="${y(v).toFixed(1)}" stroke="${CHG_C.grid}" stroke-width=".6"/>`;
    s += `<text x="${L0 - 4}" y="${(y(v) + 3).toFixed(1)}" text-anchor="end" font-size="8" fill="${CHG_C.axis}">${chgAxis(v, g.metric)}</text>`;
  }
  const pathOf = arr => {
    const pts = [];
    arr.forEach((v, i) => { if (v != null && sel(i)) pts.push(`${x(i).toFixed(1)},${y(v).toFixed(1)}`); });
    return pts;
  };
  const fcArr = g.fc.slice();
  if (!g.complete && g.cur[g.lastIdx] != null) fcArr[g.lastIdx] = g.cur[g.lastIdx];   // the forecast starts where the actuals end
  const cmpPts = pathOf(g.comp), curPts = pathOf(g.cur), fcPts = pathOf(fcArr);
  if (cmpPts.length > 1) s += `<polyline points="${cmpPts.join(" ")}" fill="none" stroke="${CHG_C.cmp}" stroke-width="1.8" stroke-dasharray="5 3"/>`;
  if (curPts.length > 1) s += `<polyline points="${curPts.join(" ")}" fill="none" stroke="${CHG_C.cur}" stroke-width="2.2"/>`;
  if (fcPts.length > 1) s += `<polyline points="${fcPts.join(" ")}" fill="none" stroke="${CHG_C.cur}" stroke-width="2" stroke-dasharray="1.5 3" stroke-linecap="round"/>`;
  for (let i = 0; i < n; i++) {
    if (!sel(i)) continue;
    if (g.comp[i] != null) s += `<circle cx="${x(i).toFixed(1)}" cy="${y(g.comp[i]).toFixed(1)}" r="1.8" fill="${CHG_C.cmp}"/>`;
    if (g.cur[i] != null) s += `<circle cx="${x(i).toFixed(1)}" cy="${y(g.cur[i]).toFixed(1)}" r="2.6" fill="${CHG_C.cur}"/>`;
    if (g.fc[i] != null) s += `<circle cx="${x(i).toFixed(1)}" cy="${y(g.fc[i]).toFixed(1)}" r="2.4" fill="#fff" stroke="${CHG_C.cur}" stroke-width="1.2"/>`;
  }
  for (let i = 0; i < n; i++) {
    if (!sel(i) || g.labels[i] == null || g.cur[i] == null) continue;
    const ty = i % 2 === 0 ? y(g.cur[i]) - 7 : y(g.cur[i]) + 13;
    const txt = g.pct ? g.labels[i].toFixed(1) + "%" : chgFmtChg(g.labels[i]);
    const col = g.pct ? CHG_C.text : chgColor(g.labels[i]);
    s += `<text x="${x(i).toFixed(1)}" y="${ty.toFixed(1)}" text-anchor="middle" font-size="8" font-weight="700" fill="${col}">${txt}</text>`;
  }
  for (let i = 0; i < n; i++) {
    const dt = toDate(g.ms + i);
    s += `<text x="${x(i).toFixed(1)}" y="${H - 15}" text-anchor="middle" font-size="7.5" fill="${CHG_C.text}">${dt.getUTCDate()}</text>`;
    s += `<text x="${x(i).toFixed(1)}" y="${H - 4}" text-anchor="middle" font-size="7" fill="${CHG_C.sub}">${"MTWTFSS"[g.wdIdx[i]]}</text>`;
  }
  return s + "</svg>";
}

// The whole block: month, metric and comparison choices, Mon–Sun tabs (DoD), the summary, week cards and the chart.
function chgBlock() {
  const keys = chgKeys();
  if (!keys.length) return "";
  if (CHG.k === null || !keys.includes(CHG.k)) CHG.k = keys[keys.length - 1];
  const g = chgModel(CHG.k);
  const seg = (list, current, attr) => list.map(([v, t]) =>
    `<button data-chg="${attr}" data-v="${v}" aria-pressed="${current === v}">${t}</button>`).join("");
  const opts = keys.map(k => {
    const done = monEnd(k) <= M.last_day;
    return `<option value="${k}" ${k === g.k ? "selected" : ""}>${monLong(k)}${done ? "" : " (to date)"}</option>`;
  }).join("");
  const fmt = v => chgFmtVal(v, g.metric);
  const cmpShort = monShort(g.cmpK), cmpName = monLong(g.cmpK);
  const kpi = (label, value, sub, extra = "") =>
    `<div class="chg-kpi ${extra}"><div class="chg-k">${label}</div><div class="chg-v">${value}</div>${sub}</div>`;
  const kpis = [
    kpi(monLong(g.k) + (g.complete ? "" : " (to date)"), fmt(g.curTotal),
      g.pct ? "" : `<div class="chg-d" style="color:${chgColor(g.curGrowth)}">${chgFmtChg(g.curGrowth)} vs ${cmpShort}${g.complete ? "" : " (same days)"}</div>`),
    kpi(`Comparable · ${cmpName}`, fmt(g.compFull),
      g.complete ? "" : `<div class="chg-d">same days: ${fmt(g.compSame)}</div>`)
  ];
  if (!g.complete) kpis.push(kpi("Forecast · month end", fmt(g.fcTotal),
    g.pct ? "" : `<div class="chg-d" style="color:${chgColor(g.fcGrowth)}">${chgFmtChg(g.fcGrowth)} vs ${cmpShort}</div>`, "chg-fc"));
  if (g.last) kpis.push(kpi(`Last day · ${fmtDay(g.last.d)}`, fmt(g.last.cur),
    g.pct || g.last.growth == null ? "" : `<div class="chg-d" style="color:${chgColor(g.last.growth)}">${chgFmtChg(g.last.growth)} DoD</div>`));

  const cards = g.weeks.map((x, i) => `<div class="chg-card">
      <div class="chg-k">WK ${x.isoWk} · ${fmtDay(x.w)}–${fmtDay(x.end)}${x.partial ? " · to date" : ""}</div>
      <div class="chg-v">${fmt(x.cur)}</div>
      ${g.pct ? "" : `<div class="chg-d" style="color:${chgColor(x.label)}">${x.label == null
        ? (i === 0 ? "base week" : "—") : chgFmtChg(x.label) + " WoW"}</div>`}
    </div>`).join("");

  const wdTabs = g.cmp === "dod" ? `<div class="chg-seg chg-wd">${CHG_WD.map((n, i) =>
      `<button data-chg="wd" data-v="${i}" aria-pressed="${CHG.wd.includes(i)}">${n}</button>`).join("")}
      <button data-chg="wdall" data-v="all">All</button></div>` : "";
  let selLine = "";
  if (g.selSum) {
    selLine = g.pct
      ? `<p class="chg-note chg-selline">Dis % on the selected days: <b>${fmt(g.selSum.pct)}</b></p>`
      : `<p class="chg-note chg-selline">${g.selSum.n} selected day(s) with a week-earlier figure: ${fmt(g.selSum.cur)} vs ${fmt(g.selSum.prev)} a week earlier · <b style="color:${chgColor(g.selSum.growth)}">${chgFmtChg(g.selSum.growth)}</b></p>`;
  }
  const legend = `<div class="chg-legend">
      <span><i class="ln cur"></i>${monShort(g.k)}${g.complete ? "" : " to date"}</span>
      <span><i class="ln cmp"></i>${cmpShort} (comparable)</span>
      ${g.complete ? "" : `<span><i class="ln fc"></i>Forecast</span>`}</div>`;
  const note = {
    dod: "DoD: each day against the same weekday last week. Tick the weekdays to follow their growth.",
    wow: "WoW: Mon–Sun weeks, each against the week before (shown on its Sunday).",
    mom: "MoM: each day against the same day of the previous month.",
    yoy: "YoY: each day against the same day last year."
  }[g.cmp];
  const extra = [g.pct ? "Dis % shows the discount as a share of gross, not growth." : "",
    g.complete ? "" : "Forecast: the rest of the month follows the comparable month's daily pattern, scaled to this month's pace so far."]
    .filter(Boolean).join(" ");

  return `<section class="chg">
    <div class="chg-head"><h2>Sales comparison</h2><select data-chg-month>${opts}</select></div>
    <div class="chg-seg">${seg(CHG_METRICS, g.metric, "metric")}</div>
    <div class="chg-seg">${seg(CHG_CMPS, g.cmp, "cmp")}</div>
    ${wdTabs}${selLine}
    <div class="chg-kpis">${kpis.join("")}</div>
    <div class="chg-cards">${cards}</div>
    <div class="chg-chart">${chgChart(g)}</div>
    ${legend}
    <p class="chg-note">${note} ${extra}</p>
  </section>`;
}

(function addChgStyle() {
  if (document.getElementById("chg-style")) return;
  const st = document.createElement("style");
  st.id = "chg-style";
  st.textContent = `.chg{max-width:420px;margin:12px 0 16px;padding:12px;border:1px solid #cbd5e1;border-radius:12px;background:#fff;color:#1e293b}
.chg-head{display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap}
.chg-head h2{font-size:14px;margin:0;color:#0f172a}
.chg-seg{display:flex;gap:5px;flex-wrap:wrap;margin-top:7px}
.chg-seg button{padding:4px 8px;border-radius:14px;border:1px solid #94a3b8;background:transparent;font-size:11px;color:#1e293b}
.chg-seg button[aria-pressed="true"]{background:#1d4ed8;border-color:#1d4ed8;color:#fff}
.chg-wd button{min-width:34px}
.chg-kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(118px,1fr));gap:6px;margin:10px 0 6px}
.chg-kpi{border:1px solid #cbd5e1;border-radius:9px;padding:6px 8px}
.chg-fc{border-style:dashed}
.chg-k{font-size:10.5px;color:#334155}
.chg-v{font-size:14px;font-weight:700;color:#0f172a}
.chg-d{font-size:10.5px;font-weight:600}
.chg-cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(104px,1fr));gap:6px;margin:6px 0}
.chg-card{border:1px solid #cbd5e1;border-radius:9px;padding:6px}
.chg-card .chg-v{font-size:12.5px}
.chg-chart svg{display:block;width:100%;height:auto}
.chg-legend{display:flex;gap:10px;flex-wrap:wrap;font-size:10.5px;color:#0f172a;margin-top:4px;align-items:center}
.chg-legend .ln{display:inline-block;width:18px;height:0;border-top:2.5px solid;margin-right:4px;vertical-align:middle}
.chg-legend .cur{border-color:#1d4ed8}
.chg-legend .cmp{border-color:#c2410c;border-top-style:dashed}
.chg-legend .fc{border-color:#1d4ed8;border-top-style:dotted}
.chg-note{font-size:10.5px;color:#334155;margin:6px 0 0}
.chg-selline{font-size:11px;color:#0f172a}`;
  document.head.appendChild(st);
})();

document.addEventListener("click", e => {
  const b = e.target.closest && e.target.closest("[data-chg]");
  if (!b) return;
  const t = b.dataset.chg, v = b.dataset.v;
  if (t === "metric") CHG.metric = v;
  else if (t === "cmp") CHG.cmp = v;
  else if (t === "wd") {
    const i = +v, on = CHG.wd.includes(i);
    if (on && CHG.wd.length === 1) return;                    // keep at least one weekday
    CHG.wd = on ? CHG.wd.filter(x => x !== i) : [...CHG.wd, i].sort((a, b) => a - b);
  }
  else if (t === "wdall") CHG.wd = [0, 1, 2, 3, 4, 5, 6];
  else return;
  render();
});
document.addEventListener("change", e => {
  if (e.target.dataset && e.target.dataset.chgMonth !== undefined) { CHG.k = +e.target.value; render(); }
});

// Wraps the monthly view: the comparison block goes above it.
const salesPeriodViewChg = salesPeriodView;
salesPeriodView = function (kind) {
  const base = salesPeriodViewChg(kind);
  return kind === "monthly" ? chgBlock() + base : base;
};
