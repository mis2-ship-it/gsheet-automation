/* ============ Live tab: today's business day by hour ============
   Business day: 9 AM to 5 AM. Comparisons use the same clock hours last week, complete hours only.
   The hour in progress is shown on its own. The rest of today is forecast from last week's
   hour-by-hour pattern, scaled by how today's complete hours compare with last week's.
   Loaded after the main page script. It replaces salesLiveView() and uses the helpers in index.html. */

let HOURLY = null, HOURLY_LOADING = null;
const DAY_HOURS = 20;                   // business hours: 0 = 9 AM ... 19 = 4 AM
const LIVE_BRAND = "#2D6A6E", LIVE_LW = "#5E6D6A";

function ensureHourly() {
  if (HOURLY) return Promise.resolve(HOURLY);
  if (!HOURLY_LOADING) {
    HOURLY_LOADING = loadGz("data/hourly.json.gz")
      .then(d => (HOURLY = d.rows))
      .catch(() => (HOURLY = []));
  }
  return HOURLY_LOADING;
}

// Hourly row: [day, store, brand, type, region, source, session, hour, net_paise, gross_paise, disc_paise, orders]
function hourlyPass(r) {
  if (S.type !== "all" && r[3] !== +S.type) return false;
  if (S.brand !== "all" && r[2] !== +S.brand) return false;
  if (S.region !== "all" && r[4] !== +S.region) return false;
  if (S.source !== "all" && r[5] !== +S.source) return false;
  if (S.session !== "all" && r[6] !== +S.session) return false;
  if (S.store !== "all" && r[1] !== +S.store) return false;
  return true;
}
const blankA = () => ({ net: 0, gross: 0, disc: 0, orders: 0, netIn: 0 });
function addRow(a, r) {
  a.net += r[8]; a.gross += r[9]; a.disc += r[10]; a.orders += r[11];
  if (r[5] === 0) a.netIn += r[8];
}
function hourTotals(day) {
  const out = Array.from({ length: DAY_HOURS }, blankA);
  for (const r of HOURLY) {
    if (r[0] !== day || r[7] >= DAY_HOURS || !hourlyPass(r)) continue;
    addRow(out[r[7]], r);
  }
  return out;
}
function sumHours(arr, from, to) {
  const a = blankA();
  for (let h = from; h < to; h++) {
    const x = arr[h];
    a.net += x.net; a.gross += x.gross; a.disc += x.disc; a.orders += x.orders; a.netIn += x.netIn;
  }
  return a;
}
function hourlyDim(day, cut, pos) {
  const m = new Map();
  for (const r of HOURLY) {
    if (r[0] !== day || r[7] >= cut || !hourlyPass(r)) continue;
    const k = r[pos];
    let a = m.get(k);
    if (!a) m.set(k, a = blankA());
    addRow(a, r);
  }
  return m;
}
const hourName = h => {
  const hr = (9 + h) % 24;
  return `${hr % 12 === 0 ? 12 : hr % 12} ${hr < 12 ? "AM" : "PM"}`;
};

// Where the data was pulled, in India time. Before 9 AM or from 5 AM to 9 AM there is no open business day.
function liveClock() {
  const dt = new Date(M.live_as_of);
  const parts = new Intl.DateTimeFormat("en-GB", { timeZone: "Asia/Kolkata", hour: "2-digit", minute: "2-digit", hourCycle: "h23" }).formatToParts(dt);
  const hh = +parts.find(p => p.type === "hour").value;
  const mm = +parts.find(p => p.type === "minute").value;
  const label0 = dt.toLocaleTimeString("en-IN", { timeZone: "Asia/Kolkata", hour: "numeric", minute: "2-digit" });
  const label = label0.replace(/\bam\b/, "AM").replace(/\bpm\b/, "PM");
  const active = hh >= 9 || hh < 5;
  return { active, cut: active ? (hh - 9 + 24) % 24 : 0, frac: mm / 60, label };
}

function salesLiveView() {
  setPeriodBar("live", null, null);
  if (!M.has_live) {
    return `<section class="empty"><h2>Live sales</h2>
      <p>Live sales are not connected yet. Once the live sync runs, today's business day appears here with the same filters.</p></section>`;
  }
  if (HOURLY === null) {
    ensureHourly().then(() => render());
    return `<p class="muted" style="margin-top:16px">Loading live data…</p>`;
  }
  const clock = liveClock();
  if (!clock.active) {
    return `<section class="empty"><h2>Business day not started</h2>
      <p>The business day runs from 9 AM to 5 AM. Today's live numbers start at 9 AM. Completed days are in the Daily tab.</p></section>`;
  }

  const day = M.live_day, cut = clock.cut, frac = clock.frac;
  const today = hourTotals(day), lw = hourTotals(day - 7);
  const tC = sumHours(today, 0, cut), lC = sumHours(lw, 0, cut), lDay = sumHours(lw, 0, DAY_HOURS);
  const inProg = cut < DAY_HOURS ? today[cut] : blankA();

  // Forecast: last week's same hours, scaled by today's complete hours against last week's.
  const gNet = lC.net > 0 ? tC.net / lC.net : 1;
  const gTxn = lC.orders > 0 ? tC.orders / lC.orders : 1;
  const rest = h => (h === cut ? 1 - frac : 1);       // share of the hour still to come
  const fNet = [], fTxn = [];
  let fNetSum = 0, fTxnSum = 0;
  for (let h = cut; h < DAY_HOURS; h++) {
    fNet[h] = lw[h].net * gNet * rest(h);
    fTxn[h] = lw[h].orders * gTxn * rest(h);
    fNetSum += fNet[h]; fTxnSum += fTxn[h];
  }
  const eodNet = tC.net + inProg.net + fNetSum;
  const eodTxn = tC.orders + inProg.orders + fTxnSum;

  const mT = metrics(tC), mL = metrics(lC);
  const card = (label, value, delta) => `<div class="kpi"><div class="kl">${label}</div><div class="kv">${value}</div><div class="cmp">${delta}</div></div>`;
  const kpis = `<div class="kgrid">
    ${card("Net sales, complete hours", inr(mT.net), dlt(rel(mT.net, mL.net), "%", true))}
    ${card("Txn, complete hours", num(tC.orders), dlt(rel(tC.orders, lC.orders), "%", true))}
    ${card("AOV, complete hours", inr(mT.aov), dlt(rel(mT.aov, mL.aov), "%", true))}
    ${card("Discount, complete hours", mT.discPct.toFixed(1) + "%", dlt(pts(mT.discPct, mL.discPct), " pts", false))}
    ${card("Net, end of day forecast", inr(eodNet / 100), dlt(rel(eodNet, lDay.net), "%", true))}
    ${card("Txn, end of day forecast", num(eodTxn), dlt(rel(eodTxn, lDay.orders), "%", true))}
  </div>`;

  // Hour-by-hour table
  const rows = [];
  for (let h = 0; h < DAY_HOURS; h++) {
    const done = h < cut, now = h === cut;
    const act = done ? today[h].net : now ? inProg.net : null;
    const g = done && lw[h].net ? rel(today[h].net, lw[h].net) : null;
    rows.push(`<tr><td>${hourName(h)}${now ? " (in progress)" : ""}</td>
      <td>${act === null ? "—" : num(act / 100)}</td>
      <td>${num(lw[h].net / 100)}</td>
      <td>${g === null ? "—" : dlt(g, "%", true)}</td>
      <td>${h >= cut ? num(fNet[h] / 100) + (now ? ' <span class="muted">rest of hour</span>' : "") : "—"}</td></tr>`);
  }
  rows.push(`<tr class="tot"><td>Total</td><td>${num((tC.net + inProg.net) / 100)}</td><td>${num(lDay.net / 100)}</td><td></td><td>${num(fNetSum / 100)}</td></tr>`);
  const table = `<div class="scroll"><table class="tbl">
    <thead><tr><th>Hour</th><th>Today</th><th>Last week</th><th>Growth</th><th>Forecast</th></tr></thead>
    <tbody>${rows.join("")}</tbody></table></div>`;

  // Chart: today (solid), in-progress hour (lighter), forecast (pale), last week (dashed line)
  const W = 340, H = 170, bw = W / DAY_HOURS, w = bw * 0.7;
  let maxV = 1;
  for (let h = 0; h < DAY_HOURS; h++) {
    maxV = Math.max(maxV, today[h].net, lw[h].net, (fNet[h] || 0) + (h === cut ? inProg.net : 0));
  }
  maxV *= 1.1;
  const yOf = v => H - (v / maxV) * H;
  const rect = (x, top, bottom, fill, op) =>
    `<rect x="${x.toFixed(1)}" y="${top.toFixed(1)}" width="${w.toFixed(1)}" height="${Math.max(0, bottom - top).toFixed(1)}" fill="${fill}" opacity="${op}"/>`;
  let bars = "";
  const pts2 = [];
  for (let h = 0; h < DAY_HOURS; h++) {
    const x = h * bw + bw * 0.15;
    if (h < cut) bars += rect(x, yOf(today[h].net), H, LIVE_BRAND, 1);
    if (h === cut) {
      bars += rect(x, yOf(inProg.net), H, LIVE_BRAND, 0.55);
      bars += rect(x, yOf(inProg.net + (fNet[h] || 0)), yOf(inProg.net), LIVE_BRAND, 0.2);
    }
    if (h > cut) bars += rect(x, yOf(fNet[h] || 0), H, LIVE_BRAND, 0.2);
    pts2.push(`${(h * bw + bw / 2).toFixed(1)},${yOf(lw[h].net).toFixed(1)}`);
  }
  const chart = `<svg class="chart" viewBox="0 0 ${W} ${H}" role="img" aria-label="Hourly sales today and last week">
    ${bars}<polyline points="${pts2.join(" ")}" fill="none" stroke="${LIVE_LW}" stroke-width="2" stroke-dasharray="4 3"/></svg>
    <div class="axis"><span>9 AM</span><span>${hourName(10)}</span><span>4 AM</span></div>
    <div class="legend"><span><i class="sw" style="--c:${LIVE_BRAND}"></i>Today</span>
      <span><i class="sw" style="--c:${LIVE_BRAND};opacity:.25"></i>Forecast</span>
      <span><i class="sw" style="--c:${LIVE_LW}"></i>Last week (dashed line)</span></div>`;

  // Brand, region, source and session, complete hours only
  const dimTable = (title, pos) => {
    if (cut === 0) return `<section class="block"><h2>By ${title}</h2><p class="muted">No complete hour yet.</p></section>`;
    const cur = hourlyDim(day, cut, pos), prev = hourlyDim(day - 7, cut, pos);
    const keysD = [...new Set([...cur.keys(), ...prev.keys()])]
      .filter(k => (cur.get(k) || blankA()).net || (prev.get(k) || blankA()).net)
      .sort((a, b) => (cur.get(b) || blankA()).net - (cur.get(a) || blankA()).net);
    const line = (name, c, p, cls) => {
      const cm = metrics(c), pm = metrics(p);
      return `<tr class="${cls || ""}"><td>${name}</td><td>${num(cm.net)}</td><td>${num(pm.net)}</td>
        <td>${dlt(rel(cm.net, pm.net), "%", true)}</td><td>${cm.discPct.toFixed(1)}%</td><td>${pm.discPct.toFixed(1)}%</td>
        <td>${dlt(pts(cm.discPct, pm.discPct), " pts", false)}</td></tr>`;
    };
    const body = line("Total", tC, lC, "tot") + keysD.map(k => line(nameOf(pos, k), cur.get(k) || blankA(), prev.get(k) || blankA())).join("");
    return `<section class="block"><h2>By ${title}</h2>
      <p class="sub">Revenue in ₹, 9 AM to ${hourName(cut)}. Brackets show change against last week, same hours.</p>
      <div class="scroll"><table class="tbl"><thead><tr><th></th><th>Today Rev</th><th>LW Rev</th><th>Growth %</th><th>Today Dis %</th><th>LW Dis %</th><th>Dis Change %</th></tr></thead>
      <tbody>${body}</tbody></table></div></section>`;
  };

  const banner = `<p class="sub" style="margin-top:14px">Data as of ${clock.label} IST. Business day ${fmtLong(day)}.
    Comparisons use complete hours. The hour in progress is shown separately, and the rest of today is a forecast from last week's pattern, scaled by today's growth so far.</p>`;
  return banner + kpis +
    `<section class="block"><h2>Hour by hour</h2>${chart}${table}</section>` +
    dimTable("brand", 2) + dimTable("region", 4) + dimTable("source", 5) + dimTable("session", 6);
}
