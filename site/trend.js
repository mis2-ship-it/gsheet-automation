/* ============ Sales, monthly: DoD %, WoW %, MoM % and YoY % ============
   Shown above the monthly view, with the page's filters (type, brand, region, source, session and store).
   Metric: Sales (net ₹), Transactions, Discount (₹) or Discount %.
   DoD %  each day against the same weekday last week
   WoW %  each Mon–Sun week against the week before (shown on its Sunday)
   MoM %  each day against the same date last month; the month against the month before
   YoY %  each day against the same date last year; the month against the same month last year
   A month still in progress is compared with the same days of the earlier month, so the figures are fair.
   Loaded last, after the other sales scripts. It wraps salesPeriodView(), so the block sits above the monthly view. */
const CHG = { k: null, metric: "sales", cmp: "dod" };
const CHG_METRICS = [["sales", "Sales (net ₹)"], ["txn", "Transactions"], ["disc", "Discount (₹)"], ["discPct", "Dis %"]];
const CHG_CMPS = [["dod", "DoD %"], ["wow", "WoW %"], ["mom", "MoM %"], ["yoy", "YoY %"]];
// Holidays drawn as a band on the chart. Confirm the dates for each year before relying on them.
const CHG_HOLIDAYS = [{ from: "2026-11-07", to: "2026-11-09", name: "Diwali" }];

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
// Change from prev to cur: a percentage for money and counts, points for Dis %. null when either side is missing.
function chgChange(cur, prev, metric) {
  if (cur == null || prev == null) return null;
  if (metric === "discPct") return cur - prev;
  if (!prev) return null;
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

// Everything the block shows for one month: the days, the Mon–Sun weeks, the month, and the last day.
function chgModel(k) {
  const complete = monEnd(k) <= M.last_day;
  const ms = monStart(k), me = complete ? monEnd(k) : M.last_day;
  const metric = CHG.metric, cmp = CHG.cmp;
  const lastWeekEnd = mondayOf(me) + 6;
  const m = chgDailyMap(Math.max(0, monStart(k - 12) - 7), Math.min(M.last_day, lastWeekEnd));

  const days = [];
  for (let d = ms; d <= me; d++) {
    const cur = chgVal(chgSum(m, d, d), metric);
    let prevDay = null;
    if (cmp === "dod") prevDay = d - 7;
    else if (cmp === "mom") prevDay = shiftMonths(d, -1);
    else if (cmp === "yoy") prevDay = shiftMonths(d, -12);
    const label = prevDay === null ? null : chgChange(cur, chgVal(chgSum(m, prevDay, prevDay), metric), metric);
    days.push({ d, cur, label });
  }

  const weeks = [];
  for (let w = mondayOf(ms); w <= me; w += 7) {
    const cur = chgVal(chgSum(m, w, w + 6), metric);
    const prev = chgVal(chgSum(m, w - 7, w - 1), metric);
    weeks.push({ w, end: w + 6, cur, label: chgChange(cur, prev, metric), partial: w + 6 > M.last_day, isoWk: chgIsoWeek(w) });
  }
  if (cmp === "wow") for (const x of days) { const wk = weeks.find(y => y.end === x.d); x.label = wk ? wk.label : null; }

  // The month against the previous month and the same month last year. A month in progress uses the same days.
  const len = me - ms;
  const window = (start, endFull) => (complete ? [start, endFull] : [start, Math.min(start + len, endFull)]);
  const pm = window(monStart(k - 1), monEnd(k - 1));
  const py = window(monStart(k - 12), monEnd(k - 12));
  const curM = chgVal(chgSum(m, ms, me), metric);
  const momPrev = chgVal(chgSum(m, pm[0], pm[1]), metric);
  const yoyPrev = chgVal(chgSum(m, py[0], py[1]), metric);
  const last = M.last_day;
  const lastInMonth = last >= ms && last <= me;
  const lastCur = lastInMonth ? chgVal(chgSum(m, last, last), metric) : null;
  const lastDod = lastInMonth ? chgChange(lastCur, chgVal(chgSum(m, last - 7, last - 7), metric), metric) : null;

  return {
    k, metric, cmp, complete, ms, me, days, weeks,
    holidays: CHG_HOLIDAYS.map(h => ({ name: h.name, a: chgDayOfIso(h.from), b: chgDayOfIso(h.to) })),
    month: {
      cur: curM,
      mom: { prev: momPrev, label: chgChange(curM, momPrev, metric) },
      yoy: { prev: yoyPrev, label: chgChange(curM, yoyPrev, metric) },
      sameDays: !complete
    },
    last: lastInMonth ? { d: last, cur: lastCur, label: lastDod } : null
  };
}

const chgFmtVal = (v, metric) => v == null ? "—"
  : metric === "txn" ? Math.round(v).toLocaleString("en-IN")
  : metric === "discPct" ? v.toFixed(1) + "%"
  : "₹" + (v / 100000).toFixed(2) + " L";
const chgFmtChg = (v, metric) => v == null ? "—"
  : (v >= 0 ? "+" : "−") + Math.abs(v).toFixed(1) + (metric === "discPct" ? " pts" : "%");
const chgColor = v => v == null ? "#64748b" : v >= 0 ? "#15803d" : "#b91c1c";
const chgAxis = (v, metric) => metric === "txn" ? Math.round(v) + ""
  : metric === "discPct" ? v.toFixed(1) + "%"
  : "₹" + (v / 100000).toFixed(1) + "L";
const CHG_WEEKDAY = ["S", "M", "T", "W", "T", "F", "S"];

// The daily line, with the change written at each point, and holidays as a band.
function chgChart(g) {
  const n = g.days.length;
  const W = 360, H = 210, L = 46, R = 12, T = 16, B = 30;
  const vals = g.days.map(x => x.cur).filter(v => v != null);
  if (!vals.length) return "<p class=\"chg-note\">No sales for this month with these filters.</p>";
  let lo = Math.min(...vals), hi = Math.max(...vals);
  if (hi === lo) hi = lo + 1;
  const pad = (hi - lo) * 0.12;
  lo = Math.max(0, lo - pad); hi += pad;
  const x = i => L + (n <= 1 ? 0 : i * (W - L - R) / (n - 1));
  const y = v => T + (1 - (v - lo) / (hi - lo)) * (H - T - B);
  let s = `<svg viewBox="0 0 ${W} ${H}" width="100%" role="img" aria-label="Daily ${g.metric} with change">`;
  for (const h of g.holidays) {
    const a = Math.max(h.a, g.ms), b = Math.min(h.b, g.me);
    if (a > b) continue;
    const x1 = x(a - g.ms), x2 = x(b - g.ms);
    s += `<rect x="${x1 - 6}" y="${T}" width="${x2 - x1 + 12}" height="${H - T - B}" fill="#fde68a" opacity=".45"/>`;
    s += `<text x="${(x1 + x2) / 2}" y="${T + 10}" text-anchor="middle" font-size="10" fill="#92400e">${h.name}</text>`;
  }
  for (let i = 0; i <= 3; i++) {
    const v = lo + (hi - lo) * i / 3;
    s += `<line x1="${L}" x2="${W - R}" y1="${y(v)}" y2="${y(v)}" stroke="#e2e8f0"/>`;
    s += `<text x="${L - 6}" y="${y(v) + 3}" text-anchor="end" font-size="9" fill="#64748b">${chgAxis(v, g.metric)}</text>`;
  }
  const pts = g.days.map((p, i) => p.cur == null ? null : `${x(i).toFixed(1)},${y(p.cur).toFixed(1)}`).filter(Boolean);
  if (pts.length > 1) s += `<polyline points="${pts.join(" ")}" fill="none" stroke="#2563eb" stroke-width="2"/>`;
  g.days.forEach((p, i) => {
    if (p.cur != null) s += `<circle cx="${x(i).toFixed(1)}" cy="${y(p.cur).toFixed(1)}" r="2.5" fill="#2563eb"/>`;
    if (p.label != null) {
      const above = i % 2 === 0;
      const ty = above ? y(p.cur) - 6 : y(p.cur) + 13;
      s += `<text x="${x(i).toFixed(1)}" y="${ty.toFixed(1)}" text-anchor="middle" font-size="8.5" fill="${chgColor(p.label)}">${chgFmtChg(p.label, g.metric)}</text>`;
    }
    const d = toDate(g.ms + i);
    s += `<text x="${x(i).toFixed(1)}" y="${H - 14}" text-anchor="middle" font-size="8" fill="#334155">${d.getUTCDate()}</text>`;
    s += `<text x="${x(i).toFixed(1)}" y="${H - 3}" text-anchor="middle" font-size="7" fill="#94a3b8">${CHG_WEEKDAY[d.getUTCDay()]}</text>`;
  });
  return s + "</svg>";
}

// The whole block: month picker, metric and comparison choices, the month summary, Mon–Sun week cards and the chart.
function chgBlock() {
  const keys = periodKeys("monthly");
  if (!keys.length) return "";
  if (CHG.k === null || !keys.includes(CHG.k)) CHG.k = keys[keys.length - 1];
  const g = chgModel(CHG.k);
  const seg = (list, current, attr) => list.map(([v, t]) =>
    `<button data-chg="${attr}" data-v="${v}" aria-pressed="${current === v}">${t}</button>`).join("");
  const opts = keys.map(k => `<option value="${k}" ${k === g.k ? "selected" : ""}>${monLong(k)}</option>`).join("");
  const note = {
    dod: "DoD: each day against the same weekday last week.",
    wow: "WoW: Mon–Sun weeks, each against the week before (shown on its Sunday).",
    mom: "MoM: each day against the same date last month.",
    yoy: "YoY: each day against the same date last year."
  }[g.cmp];
  const cards = g.weeks.map((x, i) => `<div class="chg-card">
      <div class="chg-k">WK ${x.isoWk} · ${fmtDay(x.w)}–${fmtDay(x.end)}${x.partial ? " (to date)" : ""}</div>
      <div class="chg-v">${chgFmtVal(x.cur, g.metric)}</div>
      <div class="chg-c" style="color:${chgColor(x.label)}">${x.label == null
        ? (i === 0 ? "base week" : "—") : chgFmtChg(x.label, g.metric) + " WoW"}</div></div>`).join("");
  const mm = g.month;
  const summary = `<div class="chg-sum">
      <div><div class="chg-k">${monLong(g.k)}${mm.sameDays ? " (to date)" : ""}</div>
        <div class="chg-v">${chgFmtVal(mm.cur, g.metric)}</div></div>
      <div><div class="chg-k">MoM${mm.sameDays ? " (same days)" : ""}</div>
        <div class="chg-v" style="color:${chgColor(mm.mom.label)}">${chgFmtChg(mm.mom.label, g.metric)}</div></div>
      <div><div class="chg-k">YoY${mm.sameDays ? " (same days)" : ""}</div>
        <div class="chg-v" style="color:${chgColor(mm.yoy.label)}">${chgFmtChg(mm.yoy.label, g.metric)}</div></div>
      ${g.last ? `<div><div class="chg-k">Last day · DoD</div>
        <div class="chg-v">${chgFmtVal(g.last.cur, g.metric)}</div>
        <div class="chg-v" style="color:${chgColor(g.last.label)}">${chgFmtChg(g.last.label, g.metric)}</div></div>` : ""}
    </div>`;
  return `<section class="chg">
    <div class="chg-head"><h2>Sales comparison</h2>
      <select data-chg-month>${opts}</select></div>
    <div class="chg-seg">${seg(CHG_METRICS, g.metric, "metric")}</div>
    <div class="chg-seg">${seg(CHG_CMPS, g.cmp, "cmp")}</div>
    ${summary}
    <div class="chg-cards">${cards}</div>
    ${chgChart(g)}
    <p class="chg-note">${note} Filters: Type, Brand, Region, Source, Session and Store at the top.</p>
  </section>`;
}

(function addChgStyle() {
  if (document.getElementById("chg-style")) return;
  const st = document.createElement("style");
  st.id = "chg-style";
  st.textContent = `.chg{margin:14px 0 18px;padding:14px;border:1px solid #e2e8f0;border-radius:12px;background:#fff}
.chg-head{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap}
.chg-head h2{font-size:15px;margin:0}
.chg-seg{display:flex;gap:6px;flex-wrap:wrap;margin-top:8px}
.chg-seg button{padding:6px 10px;border-radius:16px;border:1px solid #cbd5e1;background:transparent;font-size:12px}
.chg-seg button[aria-pressed="true"]{background:#1d4ed8;border-color:#1d4ed8;color:#fff}
.chg-sum{display:flex;gap:18px;flex-wrap:wrap;margin:12px 0}
.chg-k{font-size:11px;color:#64748b}
.chg-v{font-size:18px;font-weight:700}
.chg-cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:8px;margin:8px 0}
.chg-card{border:1px solid #e2e8f0;border-radius:10px;padding:8px}
.chg-card .chg-v{font-size:15px}
.chg-note{font-size:11px;color:#64748b;margin:8px 0 0}`;
  document.head.appendChild(st);
})();

document.addEventListener("click", e => {
  const b = e.target.closest && e.target.closest("[data-chg]");
  if (!b) return;
  if (b.dataset.chg === "metric") CHG.metric = b.dataset.v;
  if (b.dataset.chg === "cmp") CHG.cmp = b.dataset.v;
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
