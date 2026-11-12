/* ============ Sales, monthly: comparison chart (Month A vs Month B) ============
   Month A is the month you look at (the latest is the month in progress). Month B is the month it is compared with.
   Both are chosen from the dropdowns. MoM and YoY set Month B to the month before / the same month last year.
   DoD %  each day against the same weekday last week, in each month.
   WoW %  each Mon–Sun week against the week before, shown on its Sunday, in each month.
   MoM / YoY %  each day of Month A against the same day of Month B.
   Both lines carry growth labels. Forecast (dotted), for the rest of Month A:
     the same day of Month B × (1 + Month A's growth so far against the same days of Month B).
   Light (default) or dark, and Mobile / Desktop / Auto layout, are chosen at the top of the page.
   Loaded last. It wraps salesPeriodView(), so the block sits above the monthly view. */
const CHG = { a: null, b: null, cmp: "dod", metric: "sales", wd: [0, 1, 2, 3, 4, 5, 6] };
const CHG_METRICS = [["sales", "Sales (net ₹)"], ["txn", "Transactions"], ["disc", "Discount (₹)"], ["discPct", "Dis %"]];
const CHG_CMPS = [["dod", "DoD %"], ["wow", "WoW %"], ["mom", "MoM %"], ["yoy", "YoY %"]];
const CHG_WD = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
// Holidays drawn as a band on the chart. Confirm the dates for each year before relying on them.
const CHG_HOLIDAYS = [{ from: "2026-11-07", to: "2026-11-09", name: "Diwali" }];
const CHG_PREF = { theme: "fb-sales-theme", layout: "fb-sales-layout" };

/* ---- preferences: light is the default; layout Auto is desktop from 900px wide ---- */
function chgApplyPrefs() {
  const root = document.documentElement;
  let theme = "light", layout = "auto";
  try {
    if (localStorage.getItem(CHG_PREF.theme) === "dark") theme = "dark";
    const l = localStorage.getItem(CHG_PREF.layout);
    if (l === "mobile" || l === "desktop" || l === "auto") layout = l;
  } catch (e) { /* storage blocked: defaults */ }
  root.setAttribute("data-theme", theme);
  root.setAttribute("data-layout", layout);
}
function chgSetPref(kind, value) {
  try { localStorage.setItem(kind === "theme" ? CHG_PREF.theme : CHG_PREF.layout, value); } catch (e) { /* not saved */ }
  document.documentElement.setAttribute("data-" + kind, value);
}
function chgIsWide() {
  const layout = document.documentElement.getAttribute("data-layout");
  if (layout === "desktop") return true;
  if (layout === "mobile") return false;
  return !!(window.matchMedia && window.matchMedia("(min-width: 900px)").matches);
}
// Chart size: the drawing is made at the width it is shown at, so the text stays the same size on screen.
function chgSize() {
  return chgIsWide()
    ? { W: 960, H: 340, L: 66, R: 16, T: 18, B: 40, fAx: 12, fLbl: 12, fDay: 11, fWk: 10 }
    : { W: 360, H: 210, L: 42, R: 10, T: 14, B: 32, fAx: 8, fLbl: 8.5, fDay: 7.5, fWk: 7 };
}
// The Light / Dark and Mobile / Desktop / Auto switch, at the top of the page (above the header).
function chgPrefBar() {
  const old = document.getElementById("fb-prefs");
  if (old && old.parentNode) old.parentNode.removeChild(old);
  const app = document.querySelector(".app");
  if (!app) return;
  const theme = document.documentElement.getAttribute("data-theme"), layout = document.documentElement.getAttribute("data-layout");
  const seg = (name, list, cur) => list.map(([v, t]) =>
    `<button data-pref="${name}" data-v="${v}" aria-pressed="${cur === v}">${t}</button>`).join("");
  const bar = document.createElement("div");
  bar.id = "fb-prefs";
  bar.className = "fb-prefs";
  bar.innerHTML = `<span class="fb-lbl">Theme</span><div class="fb-seg">${seg("theme", [["light", "Light"], ["dark", "Dark"]], theme)}</div>` +
    `<span class="fb-lbl">Layout</span><div class="fb-seg">${seg("layout", [["mobile", "Mobile"], ["desktop", "Desktop"], ["auto", "Auto"]], layout)}</div>`;
  app.insertBefore(bar, app.firstChild);
}

(function chgStyle() {
  if (document.getElementById("chg-style")) return;
  const st = document.createElement("style");
  st.id = "chg-style";
  st.textContent = `html[data-theme="light"]{color-scheme:light;--cmp:root{--cmp:#6D28D9;--holiday:#FDE68A;--holiday-ink:#92400E}
html[data-theme="dark"]{color-scheme:dark;--cmp:#C4B5FD;--holiday:#78350F;--holiday-ink:#FCD34D}
html[data-layout="mobile"] .app{max-width:480px}
html[data-layout="desktop"] .app{max-width:1120px}
@media (prefers-color-scheme:dark){html[data-theme="auto"]{--cmp:#C4B5FD;--holiday:#78350F;--holiday-ink:#FCD34D}}
.fb-prefs{display:flex;flex-wrap:wrap;align-items:center;gap:8px 10px;padding:12px 16px 0;font-size:12px;color:var(--ink-2)}
.fb-lbl{font-weight:600}
.fb-seg{display:inline-flex;gap:3px;padding:3px;border-radius:10px;background:var(--line)}
.fb-seg button{padding:5px 11px;border-radius:8px;font-size:12px;color:var(--ink-2)}
.fb-seg button[aria-pressed="true"]{background:var(--surface);color:var(--ink);font-weight:600}
.chg{margin:14px 16px 18px;padding:14px;border:1px solid var(--line);border-radius:14px;background:var(--surface);color:var(--ink)}
.chg:not(.chg-wide){max-width:420px}
.chg.chg-wide{max-width:none}
.chg-head{display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap}
.chg-head h2{font-size:15px;margin:0;color:var(--ink)}
.chg-months{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-top:10px;font-size:12px;color:var(--ink-2)}
.chg select{background:var(--surface);color:var(--ink);border:1px solid var(--line);border-radius:8px;padding:6px 8px;font-size:13px}
.chg select option{background:var(--surface);color:var(--ink)}
.chg-seg{display:flex;gap:6px;flex-wrap:wrap;margin-top:8px}
.chg-seg button{padding:5px 11px;border-radius:999px;border:1px solid var(--line);color:var(--ink-2);font-size:12px}
.chg-seg button[aria-pressed="true"]{background:var(--brand);border-color:var(--brand);color:#fff}
.chg-wd button{min-width:38px}
.chg-kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:8px;margin:12px 0 8px}
.chg-wide .chg-kpis{grid-template-columns:repeat(4,minmax(0,1fr))}
.chg-kpi{border:1px solid var(--line);border-radius:10px;padding:8px 10px}
.chg-fc{border-style:dashed}
.chg-k{font-size:11px;color:var(--ink-2)}
.chg-v{font-size:16px;font-weight:700;color:var(--ink)}
.chg-d{font-size:11px;font-weight:600}
.chg-cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(104px,1fr));gap:8px;margin:8px 0}
.chg-wide .chg-cards{grid-template-columns:repeat(auto-fit,minmax(150px,1fr))}
.chg-card{border:1px solid var(--line);border-radius:10px;padding:7px 8px}
.chg-card .chg-v{font-size:13px}
.chg-chart svg{display:block;width:100%;height:auto}
.cs-ax{fill:var(--ink-2)}
.cs-day{fill:var(--ink)}
.cs-wk{fill:var(--ink-2)}
.cs-grid{stroke:var(--line);stroke-width:.7}
.cs-band{fill:var(--holiday);opacity:.6}
.cs-holt{fill:var(--holiday-ink)}
.cs-cur{fill:none;stroke:var(--brand);stroke-width:2.4}
.cs-cmp{fill:none;stroke:var(--cmp);stroke-width:2;stroke-dasharray:6 3}
.cs-fc{fill:none;stroke:var(--brand);stroke-width:2;stroke-dasharray:1.5 3;stroke-linecap:round}
.cs-dot-cur{fill:var(--brand)}
.cs-dot-cmp{fill:var(--cmp)}
.cs-dot-fc{fill:var(--surface);stroke:var(--brand);stroke-width:1.4}
.cs-up{fill:var(--up)}
.cs-down{fill:var(--down)}
.cs-val{fill:var(--ink)}
.cs-cmpl{fill:var(--cmp)}
.chg-legend{display:flex;gap:12px;flex-wrap:wrap;font-size:11px;color:var(--ink);margin-top:6px;align-items:center}
.chg-legend i{display:inline-block;width:18px;height:0;border-top:2.5px solid;margin-right:5px;vertical-align:middle}
.chg-legend .lg-cur{border-color:var(--brand)}
.chg-legend .lg-cmp{border-color:var(--cmp);border-top-style:dashed}
.chg-legend .lg-fc{border-color:var(--brand);border-top-style:dotted}
.chg-note{font-size:11px;color:var(--ink-2);margin:8px 0 0;line-height:1.5}
.chg-selline{color:var(--ink)}`;
  document.head.appendChild(st);
})();

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
// Growth from prev to cur, as a percentage. null when either side is missing.
function chgChange(cur, prev) {
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

// Everything the block shows for Month A against Month B.
function chgModel(a, b) {
  const pct = CHG.metric === "discPct", metric = CHG.metric, cmp = CHG.cmp;
  const ms = monStart(a), me = monEnd(a), L = me - ms + 1;
  const complete = me <= M.last_day;
  const lastIdx = Math.min(L - 1, M.last_day - ms);             // index of the last day with data in Month A
  const bms = monStart(b), bme = monEnd(b), bL = bme - bms + 1;
  const bRef = cmp === "yoy" ? b - 12 : b - 1;                  // what Month B is compared with (MoM / YoY)
  const rms = monStart(bRef), rL = monEnd(bRef) - rms + 1;
  const lo = Math.max(0, Math.min(ms, bms, rms) - 21);
  const hi = Math.min(M.last_day, Math.max(me, bme) + 7);
  const m = chgDailyMap(lo, hi);
  const val = d => (d < 0 || d > M.last_day) ? null : chgVal(chgSum(m, d, d), metric);
  const range = (x0, x1) => chgVal(chgSum(m, x0, x1), metric);
  const lastDay = Math.min(me, M.last_day);

  const cur = [], bv = [], wdIdx = [];
  for (let i = 0; i < L; i++) {
    cur.push(i <= lastIdx ? val(ms + i) : null);
    bv.push(i < bL ? val(bms + i) : null);
    wdIdx.push((toDate(ms + i).getUTCDay() + 6) % 7);
  }

  // Mon–Sun weeks of Month A (the last one may run past the last day with data).
  const weeks = [];
  for (let w = mondayOf(ms); w <= lastDay; w += 7) {
    const c = range(w, w + 6), p = range(w - 7, w - 1);
    weeks.push({ w, end: w + 6, cur: c, label: pct ? c : chgChange(c, p), partial: w + 6 > M.last_day, isoWk: chgIsoWeek(w) });
  }

  // Growth labels on Month A.
  const labA = new Array(L).fill(null);
  for (let i = 0; i < L; i++) {
    if (cur[i] == null) continue;
    if (pct) labA[i] = cur[i];
    else if (cmp === "dod") labA[i] = chgChange(cur[i], val(ms + i - 7));
    else if (cmp !== "wow") labA[i] = bv[i] != null ? chgChange(cur[i], bv[i]) : null;
  }
  if (cmp === "wow") for (const wk of weeks) { const i = wk.end - ms; if (i >= 0 && i < L && !wk.partial) labA[i] = wk.label; }

  // Growth labels on Month B, against its own week, day or month before (the same rule as Month A).
  const labB = new Array(L).fill(null);
  for (let i = 0; i < L; i++) {
    if (i >= bL || bv[i] == null) continue;
    if (pct) labB[i] = bv[i];
    else if (cmp === "dod") labB[i] = chgChange(bv[i], val(bms + i - 7));
    else if (cmp !== "wow") labB[i] = i < rL ? chgChange(bv[i], val(rms + i)) : null;
  }
  if (cmp === "wow") {
    const bLast = Math.min(bme, M.last_day);
    for (let w = mondayOf(bms); w + 6 <= bLast; w += 7) {
      const i = w + 6 - bms;
      if (i < 0 || i >= L || i >= bL) continue;
      const c = range(w, w + 6), p = range(w - 7, w - 1);
      labB[i] = pct ? c : chgChange(c, p);
    }
  }

  // Totals. A month in progress is compared with the same days of Month B.
  const curTotal = range(ms, ms + lastIdx);
  const bFull = range(bms, bme);
  const bSame = range(bms, bms + Math.min(lastIdx, bL - 1));
  const curGrowth = pct ? null : chgChange(curTotal, complete ? bFull : bSame);

  // Forecast for the rest of Month A: same day of Month B × (1 + growth so far).
  const fc = new Array(L).fill(null);
  let fcTotal = null, fcGrowth = null, growthSoFar = null, offset = null;
  if (!complete && curTotal != null) {
    if (pct) {
      offset = bSame != null ? curTotal - bSame : null;
      for (let i = lastIdx + 1; i < L; i++) fc[i] = bv[i] != null && offset != null ? bv[i] + offset : null;
      fcTotal = bFull != null && offset != null ? bFull + offset : null;
    } else if (bSame) {
      growthSoFar = (curTotal / bSame - 1) * 100;
      const f = 1 + growthSoFar / 100;
      const avg = curTotal / (lastIdx + 1);                       // only if Month B has no value for a day
      let rest = 0;
      for (let i = lastIdx + 1; i < L; i++) {
        fc[i] = bv[i] != null ? bv[i] * f : avg;
        rest += fc[i];
      }
      fcTotal = curTotal + rest;
      fcGrowth = bFull ? (fcTotal / bFull - 1) * 100 : null;
    }
  }

  // DoD: the selected weekdays so far, against the same days a week earlier.
  let selSum = null;
  if (cmp === "dod") {
    const days = [];
    for (let i = 0; i <= lastIdx; i++) if (CHG.wd.includes(wdIdx[i])) days.push(i);
    if (pct) {
      let gr = 0, dd = 0;
      for (const i of days) { const x = chgSum(m, ms + i, ms + i); gr += x.gross; dd += x.disc; }
      selSum = { pct: gr ? (dd / gr) * 100 : null };
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
  const lastIn = last >= ms && last <= me;
  return {
    a, b, ms, me, L, complete, lastIdx, metric, cmp, pct, bL, cur, bv, fc, labA, labB, wdIdx, weeks, selSum,
    holidays: CHG_HOLIDAYS.map(h => ({ name: h.name, a: chgDayOfIso(h.from), b: chgDayOfIso(h.to) })),
    curTotal, bFull, bSame, curGrowth, fcTotal, fcGrowth, growthSoFar,
    last: lastIn ? { d: last, cur: val(last), growth: pct ? null : chgChange(val(last), val(last - 7)) } : null
  };
}

const chgFmtVal = (v, metric) => v == null ? "—"
  : metric === "txn" ? Math.round(v).toLocaleString("en-IN")
  : metric === "discPct" ? v.toFixed(1) + "%"
  : "₹" + (v / 100000).toFixed(2) + " L";
const chgFmtChg = v => v == null ? "—" : (v >= 0 ? "+" : "−") + Math.abs(v).toFixed(1) + "%";
const chgColorVar = v => v == null ? "var(--ink-2)" : v >= 0 ? "var(--up)" : "var(--down)";
const chgAxis = (v, metric) => metric === "txn" ? Math.round(v) + ""
  : metric === "discPct" ? v.toFixed(1) + "%"
  : "₹" + (v / 100000).toFixed(1) + "L";

// A growth label: green for growth, red for drop (both months). Dis % labels are plain values.
function chgLabel(x, y, v, pct, who, S) {
  const txt = pct ? v.toFixed(1) + "%" : chgFmtChg(v);
  const cls = pct ? "cs-val" : (v >= 0 ? "cs-up" : "cs-down");
  return `<text class="${cls}" x="${x.toFixed(1)}" y="${y.toFixed(1)}" text-anchor="middle" font-size="${S.fLbl}" font-weight="700">${txt}</text>`;
}

// The daily chart: Month A (solid), Month B (dashed), the forecast (dotted), the growth labels and the holidays.
function chgChart(g) {
  const S = chgSize(), W = S.W, H = S.H, L0 = S.L, R0 = S.R, T0 = S.T, B0 = S.B, n = g.L;
  const sel = i => g.cmp !== "dod" || CHG.wd.includes(g.wdIdx[i]);
  const pool = [];
  for (let i = 0; i < n; i++) {
    if (!sel(i)) continue;
    if (g.cur[i] != null) pool.push(g.cur[i]);
    if (g.bv[i] != null) pool.push(g.bv[i]);
    if (g.fc[i] != null) pool.push(g.fc[i]);
  }
  if (!pool.length) return "<p class=\"chg-note\">No sales for these days with these filters.</p>";
  let lo = Math.min(...pool), hi = Math.max(...pool);
  if (hi === lo) hi = lo + 1;
  const pad = (hi - lo) * 0.12;
  lo = Math.max(0, lo - pad); hi += pad;
  const x = i => L0 + (n <= 1 ? 0 : i * (W - L0 - R0) / (n - 1));
  const y = v => T0 + (1 - (v - lo) / (hi - lo)) * (H - T0 - B0);
  let s = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${g.metric} by day: Month A, Month B and forecast">`;
  for (const h of g.holidays) {
    const a = Math.max(h.a, g.ms), b = Math.min(h.b, g.me);
    if (a > b) continue;
    const x1 = x(a - g.ms), x2 = x(b - g.ms);
    s += `<rect class="cs-band" x="${(x1 - 5).toFixed(1)}" y="${T0}" width="${(x2 - x1 + 10).toFixed(1)}" height="${H - T0 - B0}"/>`;
    s += `<text class="cs-holt" x="${((x1 + x2) / 2).toFixed(1)}" y="${T0 + 10}" text-anchor="middle" font-size="${S.fWk + 1}">${h.name}</text>`;
  }
  for (let i = 0; i <= 3; i++) {
    const v = lo + (hi - lo) * i / 3;
    s += `<line class="cs-grid" x1="${L0}" x2="${W - R0}" y1="${y(v).toFixed(1)}" y2="${y(v).toFixed(1)}"/>`;
    s += `<text class="cs-ax" x="${L0 - 6}" y="${(y(v) + 4).toFixed(1)}" text-anchor="end" font-size="${S.fAx}">${chgAxis(v, g.metric)}</text>`;
  }
  const path = arr => {
    const pts = [];
    arr.forEach((v, i) => { if (v != null && sel(i)) pts.push(`${x(i).toFixed(1)},${y(v).toFixed(1)}`); });
    return pts;
  };
  const fcArr = g.fc.slice();
  if (!g.complete && g.cur[g.lastIdx] != null) fcArr[g.lastIdx] = g.cur[g.lastIdx];   // the forecast starts where the actuals end
  const bP = path(g.bv), curP = path(g.cur), fcP = path(fcArr);
  if (bP.length > 1) s += `<polyline class="cs-cmp" points="${bP.join(" ")}"/>`;
  if (curP.length > 1) s += `<polyline class="cs-cur" points="${curP.join(" ")}"/>`;
  if (fcP.length > 1) s += `<polyline class="cs-fc" points="${fcP.join(" ")}"/>`;
  for (let i = 0; i < n; i++) {
    if (!sel(i)) continue;
    if (g.bv[i] != null) s += `<circle class="cs-dot-cmp" cx="${x(i).toFixed(1)}" cy="${y(g.bv[i]).toFixed(1)}" r="2"/>`;
    if (g.cur[i] != null) s += `<circle class="cs-dot-cur" cx="${x(i).toFixed(1)}" cy="${y(g.cur[i]).toFixed(1)}" r="2.8"/>`;
    if (g.fc[i] != null) s += `<circle class="cs-dot-fc" cx="${x(i).toFixed(1)}" cy="${y(g.fc[i]).toFixed(1)}" r="2.6"/>`;
  }
  for (let i = 0; i < n; i++) {
    if (!sel(i)) continue;
    if (g.labA[i] != null && g.cur[i] != null) s += chgLabel(x(i), y(g.cur[i]) - 8, g.labA[i], g.pct, "A", S);
    if (g.labB[i] != null && g.bv[i] != null) s += chgLabel(x(i), y(g.bv[i]) + 15, g.labB[i], g.pct, "B", S);
  }
  for (let i = 0; i < n; i++) {
    const dt = toDate(g.ms + i);
    s += `<text class="cs-day" x="${x(i).toFixed(1)}" y="${(H - 18).toFixed(1)}" text-anchor="middle" font-size="${S.fDay}">${dt.getUTCDate()}</text>`;
    s += `<text class="cs-wk" x="${x(i).toFixed(1)}" y="${(H - 6).toFixed(1)}" text-anchor="middle" font-size="${S.fWk}">${"MTWTFSS"[g.wdIdx[i]]}</text>`;
  }
  return s + "</svg>";
}

// The whole block: the two month choices, metric and comparison, the summary, week cards, the chart and the notes.
function chgBlock() {
  const keys = chgKeys();
  if (!keys.length) return "";
  if (CHG.a === null || !keys.includes(CHG.a)) CHG.a = keys[keys.length - 1];
  if (CHG.b === null || !keys.includes(CHG.b)) CHG.b = keys.includes(CHG.a - 1) ? CHG.a - 1 : keys[0];
  const a = CHG.a, b = CHG.b;
  const g = chgModel(a, b);
  const wide = chgIsWide();
  const monthOpts = sel => keys.map(k => `<option value="${k}" ${k === sel ? "selected" : ""}>${monLong(k)}${monEnd(k) <= M.last_day ? "" : " (to date)"}</option>`).join("");
  const seg = (list, cur, attr) => list.map(([v, t]) =>
    `<button data-chg="${attr}" data-v="${v}" aria-pressed="${cur === v}">${t}</button>`).join("");
  const A = monShort(a), B = monShort(b);
  const fmt = v => chgFmtVal(v, g.metric);
  const kpi = (label, value, sub, extra) =>
    `<div class="chg-kpi${extra || ""}"><div class="chg-k">${label}</div><div class="chg-v">${value}</div>${sub}</div>`;
  const kpis = [
    kpi(`${monLong(a)}${g.complete ? "" : " · to date"}`, fmt(g.curTotal),
      g.pct || g.curGrowth == null ? "" : `<div class="chg-d" style="color:${chgColorVar(g.curGrowth)}">${chgFmtChg(g.curGrowth)} vs ${B}${g.complete ? "" : " (same days)"}</div>`, ""),
    kpi(`Month B · ${monLong(b)}`, fmt(g.bFull),
      g.complete ? "" : `<div class="chg-d" style="color:var(--ink-2)">same days: ${fmt(g.bSame)}</div>`, "")
  ];
  if (!g.complete) kpis.push(kpi("Forecast · month end", fmt(g.fcTotal),
    g.pct || g.fcGrowth == null ? "" : `<div class="chg-d" style="color:${chgColorVar(g.fcGrowth)}">${chgFmtChg(g.fcGrowth)} vs ${B}</div>`, " chg-fc"));
  if (g.last) kpis.push(kpi(`Last day · ${fmtDay(g.last.d)}`, fmt(g.last.cur),
    g.pct || g.last.growth == null ? "" : `<div class="chg-d" style="color:${chgColorVar(g.last.growth)}">${chgFmtChg(g.last.growth)} DoD</div>`, ""));
  const cards = g.weeks.map((x, i) => `<div class="chg-card">
      <div class="chg-k">WK ${x.isoWk} · ${fmtDay(x.w)}–${fmtDay(x.end)}${x.partial ? " · to date" : ""}</div>
      <div class="chg-v">${fmt(x.cur)}</div>
      ${g.pct ? "" : `<div class="chg-d" style="color:${chgColorVar(x.label)}">${x.label == null
        ? (i === 0 ? "base week" : "—") : chgFmtChg(x.label) + " WoW"}</div>`}
    </div>`).join("");
  const wdTabs = g.cmp === "dod" ? `<div class="chg-seg chg-wd">${CHG_WD.map((n, i) =>
      `<button data-chg="wd" data-v="${i}" aria-pressed="${CHG.wd.includes(i)}">${n}</button>`).join("")}
      <button data-chg="wdall" data-v="all">All</button></div>` : "";
  let selLine = "";
  if (g.selSum) {
    selLine = g.pct
      ? `<p class="chg-note chg-selline">Dis % on the selected days: <b>${fmt(g.selSum.pct)}</b></p>`
      : `<p class="chg-note chg-selline">${g.selSum.n} selected day(s) with a week-earlier figure: ${fmt(g.selSum.cur)} vs ${fmt(g.selSum.prev)} a week earlier · <b style="color:${chgColorVar(g.selSum.growth)}">${chgFmtChg(g.selSum.growth)}</b></p>`;
  }
  const legend = `<div class="chg-legend">
      <span><i class="lg-cur"></i>${A}${g.complete ? "" : " to date"} (Month A)</span>
      <span><i class="lg-cmp"></i>${B} (Month B)</span>
      ${g.complete ? "" : `<span><i class="lg-fc"></i>Forecast</span>`}</div>`;
  const cmpNote = {
    dod: "DoD: each day against the same weekday last week, in each month. Tick the weekdays to follow their growth.",
    wow: "WoW: Mon–Sun weeks, each against the week before, shown on its Sunday, in each month.",
    mom: "MoM: each day of Month A against the same day of Month B. Month B's own labels are against the month before it.",
    yoy: "YoY: each day of Month A against the same day of Month B. Month B's own labels are against the same month a year earlier."
  }[g.cmp];
  let fcNote = "";
  if (!g.complete) {
    if (g.pct) fcNote = `Forecast (dotted) for Dis %: the same day of ${B} plus the to-date difference in points.`;
    else if (g.growthSoFar == null) fcNote = "Forecast needs sales in both months so far.";
    else fcNote = `Forecast (dotted): each remaining day = the same day of ${B} × (1 ${g.growthSoFar >= 0 ? "+" : "−"} ${Math.abs(g.growthSoFar).toFixed(1)}%). ` +
      `${Math.abs(g.growthSoFar).toFixed(1)}% is ${A}'s growth so far against the same days of ${B}.`;
  }
  return `<section class="chg${wide ? " chg-wide" : ""}">
    <div class="chg-head"><h2>Sales comparison</h2></div>
    <div class="chg-months"><span>Month A</span><select data-chg-a>${monthOpts(a)}</select>
      <span>compared with Month B</span><select data-chg-b>${monthOpts(b)}</select></div>
    <div class="chg-seg">${seg(CHG_METRICS, g.metric, "metric")}</div>
    <div class="chg-seg">${seg(CHG_CMPS, g.cmp, "cmp")}</div>
    ${wdTabs}${selLine}
    <div class="chg-kpis">${kpis.join("")}</div>
    <div class="chg-cards">${cards}</div>
    <div class="chg-chart">${chgChart(g)}</div>
    ${legend}
    <p class="chg-note">${cmpNote}</p>
    ${fcNote ? `<p class="chg-note">${fcNote}</p>` : ""}
  </section>`;
}

document.addEventListener("click", e => {
  const pref = e.target.closest && e.target.closest("[data-pref]");
  if (pref) {
    chgSetPref(pref.dataset.pref, pref.dataset.v);
    chgPrefBar();
    render();
    return;
  }
  const b = e.target.closest && e.target.closest("[data-chg]");
  if (!b) return;
  const t = b.dataset.chg, v = b.dataset.v;
  if (t === "metric") CHG.metric = v;
  else if (t === "cmp") {
    CHG.cmp = v;
    const keys = chgKeys();
    if (v === "mom" && keys.includes(CHG.a - 1)) CHG.b = CHG.a - 1;       // MoM: Month B is the month before Month A
    if (v === "yoy" && keys.includes(CHG.a - 12)) CHG.b = CHG.a - 12;     // YoY: Month B is the same month last year
  }
  else if (t === "wd") {
    const i = +v, on = CHG.wd.includes(i);
    if (on && CHG.wd.length === 1) return;                                 // keep at least one weekday
    CHG.wd = on ? CHG.wd.filter(x => x !== i) : [...CHG.wd, i].sort((p, q) => p - q);
  }
  else if (t === "wdall") CHG.wd = [0, 1, 2, 3, 4, 5, 6];
  else return;
  render();
});
document.addEventListener("change", e => {
  const d = e.target.dataset;
  if (!d) return;
  if (d.chgA !== undefined) { CHG.a = +e.target.value; render(); }
  else if (d.chgB !== undefined) { CHG.b = +e.target.value; render(); }
});
if (window.matchMedia) {
  const mq = window.matchMedia("(min-width: 900px)");
  const again = () => { if (document.documentElement.getAttribute("data-layout") === "auto") render(); };
  if (mq.addEventListener) mq.addEventListener("change", again);
  else if (mq.addListener) mq.addListener(again);
}

// Start: the saved light or dark mode and layout, the switch at the top, then the block.
chgApplyPrefs();
chgPrefBar();

// Wraps the monthly view: the comparison block goes above it.
const salesPeriodViewChg = salesPeriodView;
salesPeriodView = function (kind) {
  const base = salesPeriodViewChg(kind);
  return kind === "monthly" ? chgBlock() + base : base;
};
if (typeof render === "function") render();
