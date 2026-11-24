/* ============ Sales: Campaign tab ============
   Offline campaign days, from data/campaigns.json (built from data/campaign_days.csv).
   Each campaign day is compared with the same weekday last week (the day 7 days earlier).
   Each campaign month is also compared with the other days of that month.
   Campaign Month A and Month B are chosen from the dropdowns. Loaded after trend.js. */
const CAMP = { days: null, names: null, error: null, loading: false, a: null, b: null, name: null };   // name: null = all campaigns
if (typeof SUBS !== "undefined" && SUBS.sales && !SUBS.sales.some(x => x[0] === "campaign")) {
  SUBS.sales.push(["campaign", "Campaign"]);
}

const campMonthOf = d => { const dt = toDate(d); return (dt.getUTCFullYear() - 2025) * 12 + dt.getUTCMonth(); };
const cpPct = v => (v == null || !isFinite(v) ? "—" : (v >= 0 ? "+" : "−") + Math.abs(v).toFixed(1) + "%");
const cpRs = v => (v == null ? "—" : "₹" + Math.round(v).toLocaleString("en-IN"));
const cpShort = v => (v == null ? "" : v >= 100000 ? "₹" + (v / 100000).toFixed(1) + "L" : "₹" + Math.round(v / 1000) + "k");
const cpEsc = s => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
const cpWide = () => (typeof chgIsWide === "function" ? chgIsWide() : (window.innerWidth || 400) >= 900);
const cpColour = v => (v == null ? "var(--ink-2)" : v >= 0 ? "var(--up)" : "var(--down)");
const cpWeekday = d => toDate(d).toLocaleDateString("en-IN", { weekday: "short", timeZone: "UTC" });

async function campLoad() {
  if (CAMP.days || CAMP.loading) return;
  CAMP.loading = true;
  let j = null;
  try {
    j = await loadJson("data/campaigns.json");
  } catch (err) {
    j = null;   // the server sent a web page instead of the file
  }
  if (j && Array.isArray(j.days)) {
    CAMP.source = "served";
  } else if (typeof window !== "undefined" && window.CAMP_CALENDAR) {
    j = window.CAMP_CALENDAR;
    CAMP.source = "built-in";
  } else {
    j = null;
    CAMP.source = "none";
  }
  CAMP.names = j ? j.names : [];
  CAMP.days = j ? j.days : [];
  CAMP.error = null;
  CAMP.loading = false;
  render();
}

// Sales per day, for the same rows and filters the other Sales tabs use (paise).
function campDayMap(lo, hi) {
  const m = new Map();
  for (const r of baseSales()) {
    if (r[0] < lo || r[0] > hi || !salesPass(r)) continue;
    let a = m.get(r[0]);
    if (!a) m.set(r[0], (a = { net: 0, gross: 0, disc: 0, orders: 0 }));
    a.net += r[7]; a.gross += r[8]; a.disc += r[9]; a.orders += r[10];
  }
  return m;
}
// Totals over days from..to (paise). n = how many of those days have sales rows.
function campSum(map, from, to) {
  const a = { net: 0, gross: 0, disc: 0, orders: 0, n: 0 };
  for (let d = from; d <= to; d++) {
    const x = map.get(d);
    if (!x) continue;
    a.n++; a.net += x.net; a.gross += x.gross; a.disc += x.disc; a.orders += x.orders;
  }
  return a;
}

// The campaign day before d (in the current discount-code filter) that has sales: its date and sales (rupees).
function campPrevCampaign(d, map) {
  const days = CAMP.days
    .filter(([x, n]) => (CAMP.name === null || CAMP.names[n] === CAMP.name) && x < d)
    .map(([x]) => x)
    .sort((p, q) => q - p);
  for (const x of days) {
    const cur = campSum(map, x, x);
    if (cur.n > 0) return { d: x, net: cur.net / 100 };
  }
  return null;
}

// Everything shown for one campaign month. Money is in rupees here.
function campModel(k, map) {
  const ms = monStart(k), lastDay = Math.min(monEnd(k), M.last_day);
  const inMonth = CAMP.days.filter(([d]) => campMonthOf(d) === k);
  const camp = inMonth
    .filter(([d, n]) => CAMP.name === null || CAMP.names[n] === CAMP.name)
    .map(([d, n]) => ({ d, name: CAMP.names[n] || "Offline campaign" }))
    .sort((p, q) => p.d - q.d);
  // every campaign day of the month is left out of the "other days" baseline, whichever code it has
  const isCamp = new Set(inMonth.map(([d]) => d));
  const rows = camp.map(c => {
    const cur = campSum(map, c.d, c.d), prev = campSum(map, c.d - 7, c.d - 7);
    const hasData = cur.n > 0;
    const prevOk = prev.n > 0 && prev.net > 0;
    const last = campPrevCampaign(c.d, map);
    const net = hasData ? cur.net / 100 : null;
    return {
      d: c.d, name: c.name, hasData,
      net,
      orders: cur.orders, gross: cur.gross, disc: cur.disc,
      prev: prevOk ? prev.net / 100 : null,
      growth: hasData && prevOk ? (cur.net / prev.net - 1) * 100 : null,
      dispct: cur.gross ? (cur.disc / cur.gross) * 100 : null,
      lastD: last ? last.d : null,
      lastNet: last ? last.net : null,
      growthLast: hasData && last && last.net ? (net / last.net - 1) * 100 : null
    };
  });
  const withData = rows.filter(r => r.hasData);
  const pairs = withData.filter(r => r.prev !== null);
  const n = withData.length;
  const campNet = withData.reduce((s, r) => s + r.net, 0);
  const campAvg = n ? campNet / n : null;
  const pairCur = pairs.reduce((s, r) => s + r.net, 0);
  const pairPrev = pairs.reduce((s, r) => s + r.prev, 0);
  const lift = pairs.length ? (pairCur / pairPrev - 1) * 100 : null;
  const prevAvg = pairs.length ? pairPrev / pairs.length : null;
  const campGross = withData.reduce((s, r) => s + r.gross, 0);
  const campDisc = withData.reduce((s, r) => s + r.disc, 0);
  // the other days of the month that have sales (not campaign days)
  let oNet = 0, oGross = 0, oDisc = 0, oN = 0;
  for (let d = ms; d <= lastDay; d++) {
    if (isCamp.has(d)) continue;
    const x = map.get(d);
    if (!x) continue;
    oN++; oNet += x.net; oGross += x.gross; oDisc += x.disc;
  }
  const otherAvg = oN ? oNet / 100 / oN : null;
  const graded = rows.filter(r => r.growth !== null);
  const best = graded.length ? graded.reduce((p, q) => (q.growth > p.growth ? q : p)) : null;
  const worst = graded.length ? graded.reduce((p, q) => (q.growth < p.growth ? q : p)) : null;
  const withLast = withData.filter(r => r.lastNet !== null);
  const lastCur = withLast.reduce((s, r) => s + r.net, 0);
  const lastSum = withLast.reduce((s, r) => s + r.lastNet, 0);
  return {
    k, rows, n, pairs: pairs.length, campAvg, prevAvg, lift,
    total: campNet, pairCur, pairPrev, lastCur, lastSum,
    growthLastTotal: lastSum ? (lastCur / lastSum - 1) * 100 : null,
    dispctCamp: campGross ? (campDisc / campGross) * 100 : null,
    otherAvg, otherN: oN,
    baseLift: campAvg != null && otherAvg ? (campAvg / otherAvg - 1) * 100 : null,
    dispctCamp: campGross ? (campDisc / campGross) * 100 : null,
    dispctOther: oGross ? (oDisc / oGross) * 100 : null,
    best, worst
  };
}

// Charts. Bars use the page colours; growth labels are green for growth and red for drop.
function cpSize() { return cpWide() ? { W: 900, H: 250, fs: 12 } : { W: 360, H: 210, fs: 9 }; }
function cpNoData(text) { return `<p class="muted">${text}</p>`; }

function cpDayChart(A) {
  const rows = A.rows.filter(r => r.hasData);
  if (!rows.length) return cpNoData("No sales yet on the campaign days of this month.");
  const S = cpSize(), W = S.W, H = S.H, L0 = 8, R0 = 8, T0 = 22, B0 = 38;
  const max = Math.max(...rows.map(r => Math.max(r.net, r.prev || 0)), 1) * 1.15;
  const band = (W - L0 - R0) / rows.length;
  const bw = Math.min(band * 0.36, 26);
  const y = v => T0 + (1 - v / max) * (H - T0 - B0);
  let s = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="Campaign days against the same days last week">`;
  rows.forEach((r, i) => {
    const cx = L0 + band * i + band / 2;
    if (r.prev !== null) {
      s += `<rect x="${(cx - bw - 1).toFixed(1)}" y="${y(r.prev).toFixed(1)}" width="${bw.toFixed(1)}" height="${(H - B0 - y(r.prev)).toFixed(1)}" style="fill:var(--ink-2);opacity:.45"/>`;
    }
    s += `<rect x="${(cx + 1).toFixed(1)}" y="${y(r.net).toFixed(1)}" width="${bw.toFixed(1)}" height="${(H - B0 - y(r.net)).toFixed(1)}" style="fill:var(--brand)"/>`;
    if (r.growth !== null) {
      const top = y(Math.max(r.net, r.prev || 0)) - 5;
      s += `<text x="${(cx + 1 + bw / 2).toFixed(1)}" y="${top.toFixed(1)}" text-anchor="middle" font-size="${S.fs}" font-weight="700" style="fill:${cpColour(r.growth)}">${cpPct(r.growth)}</text>`;
    }
    s += `<text x="${cx.toFixed(1)}" y="${H - 20}" text-anchor="middle" font-size="${S.fs - 1}" style="fill:var(--ink)">${fmtDay(r.d)}</text>`;
    s += `<text x="${cx.toFixed(1)}" y="${H - 6}" text-anchor="middle" font-size="${S.fs - 2}" style="fill:var(--ink-2)">${cpWeekday(r.d)}</text>`;
  });
  s += `</svg>`;
  return s + `<div class="chg-legend"><span><i class="lg-cur"></i>Campaign day</span><span><i style="display:inline-block;width:10px;height:10px;background:var(--ink-2);opacity:.45;margin-right:5px"></i>Same day last week</span></div>`;
}

function cpCompareChart(A, B, la, lb) {
  const cats = [["Campaign days", "campAvg"], ["Same days last week", "prevAvg"], ["Other days in month", "otherAvg"]];
  const vals = cats.flatMap(([, k]) => [A[k], B[k]]).filter(v => v != null);
  if (!vals.length) return cpNoData("Not enough sales to compare the two months yet.");
  const S = cpSize(), W = S.W, H = S.H, L0 = 8, R0 = 8, T0 = 22, B0 = 34;
  const max = Math.max(...vals) * 1.2;
  const groupW = (W - L0 - R0) / cats.length, bw = Math.min(groupW * 0.28, 44);
  const y = v => T0 + (1 - v / max) * (H - T0 - B0);
  let s = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="Month A against Month B, per day">`;
  cats.forEach(([label, key], i) => {
    const gx = L0 + groupW * i + groupW / 2;
    [[A, "var(--brand)", -bw - 2], [B, "var(--cmp)", 2]].forEach(([m, fill, dx]) => {
      const v = m[key];
      if (v == null) return;
      s += `<rect x="${(gx + dx).toFixed(1)}" y="${y(v).toFixed(1)}" width="${bw.toFixed(1)}" height="${(H - B0 - y(v)).toFixed(1)}" style="fill:${fill}"/>`;
      s += `<text x="${(gx + dx + bw / 2).toFixed(1)}" y="${(y(v) - 4).toFixed(1)}" text-anchor="middle" font-size="${S.fs - 1}" style="fill:var(--ink)">${cpShort(v)}</text>`;
    });
    s += `<text x="${gx.toFixed(1)}" y="${H - 8}" text-anchor="middle" font-size="${S.fs}" style="fill:var(--ink)">${label}</text>`;
  });
  s += `</svg>`;
  const liftLine = A.campAvg != null && B.campAvg != null
    ? `Average per campaign day: ${cpEsc(la)} ${cpRs(A.campAvg)}, ${cpEsc(lb)} ${cpRs(B.campAvg)} (${cpPct((A.campAvg / B.campAvg - 1) * 100)} for ${cpEsc(la)}).`
    : "";
  return s + `<div class="chg-legend"><span><i style="display:inline-block;width:10px;height:10px;background:var(--brand);margin-right:5px"></i>${cpEsc(la)} (Month A)</span><span><i style="display:inline-block;width:10px;height:10px;background:var(--cmp);margin-right:5px"></i>${cpEsc(lb)} (Month B)</span></div>
    <p class="muted" style="margin-top:6px">${liftLine}</p>`;
}

function cpTimelineChart(list) {
  const pts = list.filter(m => m.lift !== null);
  if (!pts.length) return cpNoData("No campaign month has sales to compare with last week yet.");
  const S = cpSize(), W = S.W, H = S.H + 30, L0 = 8, R0 = 8, T0 = 16, B0 = 46;
  const lim = Math.max(...pts.map(m => Math.abs(m.lift)), 5) * 1.15;
  const band = (W - L0 - R0) / pts.length, bw = Math.min(band * 0.6, 28);
  const zero = T0 + (H - T0 - B0) / 2;
  const y = v => zero - (v / lim) * ((H - T0 - B0) / 2);
  let s = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="Campaign lift against last week, by campaign month">`;
  s += `<line x1="${L0}" x2="${W - R0}" y1="${zero.toFixed(1)}" y2="${zero.toFixed(1)}" style="stroke:var(--line)"/>`;
  pts.forEach((m, i) => {
    const cx = L0 + band * i + band / 2;
    const top = Math.min(y(m.lift), zero), h = Math.abs(y(m.lift) - zero);
    s += `<rect x="${(cx - bw / 2).toFixed(1)}" y="${top.toFixed(1)}" width="${bw.toFixed(1)}" height="${Math.max(h, 1).toFixed(1)}" style="fill:${cpColour(m.lift)}"/>`;
    const ly = m.lift >= 0 ? top - 4 : top + h + S.fs;
    s += `<text x="${cx.toFixed(1)}" y="${ly.toFixed(1)}" text-anchor="middle" font-size="${S.fs - 2}" style="fill:${cpColour(m.lift)}">${cpPct(m.lift)}</text>`;
    s += `<text transform="translate(${(cx + 3).toFixed(1)},${H - B0 + 10}) rotate(-45)" font-size="${S.fs - 2}" style="fill:var(--ink-2)">${monShort(m.k)}</text>`;
  });
  return s + `</svg>`;
}

function cpTable(A) {
  if (!A.rows.length) return cpNoData("No campaign days in this month.");
  const body = A.rows.map(r => r.hasData
    ? `<tr><td>${fmtLong(r.d)}</td><td>${cpEsc(r.name)}</td><td>${cpRs(r.net)}</td><td>${r.prev === null ? "—" : cpRs(r.prev)}</td>` +
      `<td style="color:${cpColour(r.growth)}">${cpPct(r.growth)}</td>` +
      `<td>${r.lastNet === null ? "—" : `${cpRs(r.lastNet)}<br><span class="muted">${fmtDay(r.lastD)}</span>`}</td>` +
      `<td style="color:${cpColour(r.growthLast)}">${cpPct(r.growthLast)}</td>` +
      `<td>${r.dispct === null ? "—" : r.dispct.toFixed(1) + "%"}</td><td>${num(r.orders)}</td></tr>`
    : `<tr><td>${fmtLong(r.d)}</td><td>${cpEsc(r.name)}</td><td colspan="7" class="muted">No sales yet</td></tr>`).join("");
  const ordersTotal = A.rows.filter(r => r.hasData).reduce((s, r) => s + r.orders, 0);
  const total = `<tr class="cp-total"><td><b>Total</b></td><td></td><td><b>${cpRs(A.total)}</b></td>` +
    `<td>${A.pairPrev ? cpRs(A.pairPrev) : "—"}</td><td style="color:${cpColour(A.lift)}">${cpPct(A.lift)}</td>` +
    `<td>${A.lastSum ? cpRs(A.lastSum) : "—"}</td><td style="color:${cpColour(A.growthLastTotal)}">${cpPct(A.growthLastTotal)}</td>` +
    `<td>${A.dispctCamp === null ? "—" : A.dispctCamp.toFixed(1) + "%"}</td><td>${num(ordersTotal)}</td></tr>`;
  return `<div class="scroll"><table class="tbl"><thead><tr><th>Date</th><th>Campaign</th><th>Sales</th><th>Same day last week</th>` +
    `<th>Growth</th><th>Last campaign sales</th><th>Growth vs last campaign</th><th>Dis %</th><th>Orders</th></tr></thead>` +
    `<tbody>${body}${total}</tbody></table></div>` +
    `<p class="muted" style="margin-top:6px">Last-week totals cover the campaign days that have a figure a week earlier. ` +
    `Last campaign sales is the sales on the previous campaign day in this discount code (or any code, if All campaigns).</p>`;
}

// Written points for the page, from the numbers above. Only what the data shows.
function cpInsights(A, B, la, lb, timeline) {
  const out = [];
  if (!A.n) return [`${cpEsc(la)}: no campaign day with sales data yet.`];
  out.push(`${cpEsc(la)}: ${A.n} campaign day(s) with sales, averaging ${cpRs(A.campAvg)} a day. ` +
    (A.lift === null ? "There is no same-weekday figure last week to compare." :
      `Against the same weekdays last week, sales are ${cpPct(A.lift)} (${A.pairs} day(s) compared).`));
  if (A.baseLift !== null) out.push(`The campaign days average ${cpPct(A.baseLift)} against the other ${A.otherN} day(s) of ${cpEsc(la)}.`);
  if (A.best && A.worst && A.best.d !== A.worst.d) {
    out.push(`Best campaign day: ${fmtLong(A.best.d)} (${cpPct(A.best.growth)} on the same weekday last week). ` +
      `Weakest: ${fmtLong(A.worst.d)} (${cpPct(A.worst.growth)}).`);
  }
  if (A.dispctCamp !== null && A.dispctOther !== null) {
    out.push(`Discount on campaign days is ${A.dispctCamp.toFixed(1)}%, against ${A.dispctOther.toFixed(1)}% on the other days of ${cpEsc(la)}.`);
  }
  if (B.n && A.campAvg !== null && B.campAvg !== null) {
    out.push(`${cpEsc(lb)} against ${cpEsc(la)}: average per campaign day ${cpPct((A.campAvg / B.campAvg - 1) * 100)}. ` +
      `Lift against last week: ${cpEsc(la)} ${cpPct(A.lift)}, ${cpEsc(lb)} ${cpPct(B.lift)}.`);
  }
  const lifts = timeline.filter(m => m.lift !== null);
  if (lifts.length > 1) {
    const avg = lifts.reduce((s, m) => s + m.lift, 0) / lifts.length;
    const up = lifts.filter(m => m.lift > 0).length;
    out.push(`Across ${lifts.length} campaign months, the lift against last week averages ${cpPct(avg)}. ` +
      `${up} of ${lifts.length} months were above last week.`);
  }
  return out;
}

// Top tiles for Month A: total campaign-day sales, average per campaign day, and growth on the last campaign (Month B).
function cpTop(A, B, la, lb) {
  const tile = (label, value, sub, colour) =>
    `<div class="cp-card"><div class="kl">${label}</div><div class="cp-big"${colour ? ` style="color:${colour}"` : ""}>${value}</div><div class="cmp">${sub}</div></div>`;
  const g = A.campAvg !== null && B.campAvg ? (A.campAvg / B.campAvg - 1) * 100 : null;
  return `<div class="cp-tiles">` +
    tile("Total campaign days sales", cpRs(A.total), `${A.n} campaign day(s) with sales`) +
    tile("Average per campaign day", cpRs(A.campAvg), `${cpEsc(lb)}: ${cpRs(B.campAvg)}`) +
    tile("Growth on last campaign", cpPct(g), `${cpEsc(la)} average against ${cpEsc(lb)}`, cpColour(g)) +
    `</div>`;
}

function cpCard(m, title) {
  const row = (label, value, colour) => `<div class="cp-row"><span>${label}</span><b${colour ? ` style="color:${colour}"` : ""}>${value}</b></div>`;
  return `<div class="cp-card"><div class="kl">${title}</div><div class="cp-big">${monLong(m.k)}</div>
    ${row("Campaign days with sales", num(m.n))}
    ${row("Average per campaign day", cpRs(m.campAvg))}
    ${row("Lift vs same weekday last week", cpPct(m.lift), cpColour(m.lift))}
    ${row("Campaign days vs other days", cpPct(m.baseLift), cpColour(m.baseLift))}
    ${row("Discount %, campaign days", m.dispctCamp === null ? "—" : m.dispctCamp.toFixed(1) + "%")}
  </div>`;
}

function campaignView() {
  setPeriodBar("campaign", null, null);
  const note = CAMP.source === "built-in"
    ? `<p class="muted">Showing the built-in campaign calendar (data/campaigns.json is not being served). Refresh it from the CSV with make_campaign_calendar.py.</p>`
    : "";
  return note + campaignViewBody();
}
function campaignViewBody() {
  if (!CAMP.days) {
    campLoad();
    return `<section class="empty"><h2>Campaign</h2><p class="muted">Loading the campaign days.</p></section>`;
  }
  if (!CAMP.days.length) {
    return `<section class="empty"><h2>Campaign</h2>
      <p>No campaign days are loaded yet. Put the dates in <b>data/campaign_days.csv</b> and run the update.</p>
      ${CAMP.error ? `<p class="muted">${cpEsc(CAMP.error)}</p>` : ""}</section>`;
  }
  const inFilter = ([d, n]) => CAMP.name === null || CAMP.names[n] === CAMP.name;
  const days = CAMP.days.filter(inFilter);
  if (!days.length) return `<section class="empty"><h2>Campaign</h2><p>No campaign days for this discount code.</p></section>`;
  const months = [...new Set(days.map(([d]) => campMonthOf(d)))].sort((x, y) => x - y);
  // the defaults are the latest campaign months that have sales (a future campaign month has none yet)
  const withSales = months.filter(k => days.some(([d]) => campMonthOf(d) === k && d <= M.last_day));
  const pick = withSales.length ? withSales : months;
  if (CAMP.a === null || !months.includes(CAMP.a)) CAMP.a = pick[pick.length - 1];
  if (CAMP.b === null || !months.includes(CAMP.b)) CAMP.b = pick.length > 1 ? pick[pick.length - 2] : pick[pick.length - 1];
  const lo = Math.max(0, monStart(months[0]) - 14), hi = Math.min(M.last_day, monEnd(months[months.length - 1]) + 7);
  const map = campDayMap(lo, hi);
  const A = campModel(CAMP.a, map), B = campModel(CAMP.b, map);
  const timeline = months.map(k => campModel(k, map));
  const la = monLong(CAMP.a), lb = monLong(CAMP.b);
  const opts = sel => months.map(k =>
    `<option value="${k}" ${k === sel ? "selected" : ""}>${monLong(k)} · ${days.filter(([d]) => campMonthOf(d) === k).length} day(s)</option>`).join("");
  const codes = [...new Set(CAMP.days.map(([, n]) => CAMP.names[n] || "Offline campaign"))].sort();
  const codeOpts = [`<option value="" ${CAMP.name === null ? "selected" : ""}>All campaigns</option>`]
    .concat(codes.map(c => `<option value="${cpEsc(c)}" ${CAMP.name === c ? "selected" : ""}>${cpEsc(c)}</option>`)).join("");
  const insights = cpInsights(A, B, la, lb, timeline);
  return `
    <section class="block"><h2>${cpEsc(la)}: totals</h2>${cpTop(A, B, la, lb)}</section>
    <section class="block">
      <h2>Campaign months</h2>
      <p class="sub">Campaign days against the same weekday last week, and against the other days of the same month.</p>
      <div class="cp-pick">
        <label class="field">Discount code<select data-camp="name">${codeOpts}</select></label>
        <label class="field">Campaign Month A<select data-camp="a">${opts(CAMP.a)}</select></label>
        <label class="field">Campaign Month B<select data-camp="b">${opts(CAMP.b)}</select></label>
      </div>
      <div class="cp-grid">${cpCard(A, "Month A")}${cpCard(B, "Month B")}</div>
    </section>
    <section class="block"><h2>Insights</h2><ul class="cp-ins">${insights.map(t => `<li>${t}</li>`).join("")}</ul></section>
    <section class="block"><h2>${cpEsc(la)}: each campaign day</h2>${cpDayChart(A)}</section>
    <section class="block"><h2>Month A against Month B</h2>${cpCompareChart(A, B, la, lb)}</section>
    <section class="block"><h2>Every campaign month: lift against last week</h2>${cpTimelineChart(timeline)}</section>
    <section class="block"><h2>${cpEsc(la)}: campaign days</h2>${cpTable(A)}</section>
    <p class="note">Campaign days come from data/campaign_days.csv. Growth compares each campaign day with the same weekday a week earlier, using the same filters as the other Sales tabs.</p>`;
}

// Page styles for this tab (added once).
(function campStyle() {
  if (document.getElementById("camp-style")) return;
  const st = document.createElement("style");
  st.id = "camp-style";
  st.textContent = `.cp-pick{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin:12px 0}
.cp-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.cp-tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px;margin:12px 0}
.cp-total td{border-top:2px solid var(--line)}
.cp-card{background:var(--surface);border:1px solid var(--line);border-radius:14px;padding:12px;font-size:13px}
.cp-big{font-family:var(--serif);font-size:20px;margin:2px 0 8px}
.cp-row{display:flex;justify-content:space-between;gap:8px;padding:4px 0;border-top:1px solid var(--line);color:var(--ink-2)}
.cp-row b{color:var(--ink);font-variant-numeric:tabular-nums}
.cp-ins{padding-left:18px;font-size:14px;line-height:1.5}
.cp-ins li{margin:6px 0}
svg{display:block;width:100%;height:auto}`;
  document.head.appendChild(st);
})();

document.addEventListener("change", e => {
  const d = e.target.dataset;
  if (!d || d.camp === undefined) return;
  if (d.camp === "a") CAMP.a = +e.target.value;
  if (d.camp === "b") CAMP.b = +e.target.value;
  if (d.camp === "name") { CAMP.name = e.target.value === "" ? null : e.target.value; CAMP.a = null; CAMP.b = null; }
  render();
});

// The Campaign tab is handled here; every other Sales tab goes to the page as before.
const salesPeriodViewCamp = salesPeriodView;
salesPeriodView = function (kind) {
  return kind === "campaign" ? campaignView() : salesPeriodViewCamp(kind);
};
