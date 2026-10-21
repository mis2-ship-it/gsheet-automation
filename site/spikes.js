/* ============ Sales: Spike Days ============
   A sub-tab under Sales, laid out like Monthly. Spike days come from the master calendar (2025 to 2027).
   Each spike day is compared with the same event last year, on its own date that year.
   An event is listed under the month where it starts. Days after yesterday are not counted yet.
   The Brand, Region, Source, Session and Store filters apply. Loaded after store.js. */

const SPIKE_EVENTS = [
  {"m": "January", "n": "New Year's Day", "t": "Global Public Holiday", "d": {"2025": ["2025-01-01", "2025-01-01"], "2026": ["2026-01-01", "2026-01-01"], "2027": ["2027-01-01", "2027-01-01"]}},
  {"m": "January", "n": "Makar Sankranti / Pongal / Lohri / Magh Bihu", "t": "India (KA, TN, PB, AS, AP, TS, GJ)", "d": {"2025": ["2025-01-14", "2025-01-15"], "2026": ["2026-01-14", "2026-01-15"], "2027": ["2027-01-14", "2027-01-15"]}},
  {"m": "January", "n": "Republic Day", "t": "India National Holiday", "d": {"2025": ["2025-01-26", "2025-01-26"], "2026": ["2026-01-26", "2026-01-26"], "2027": ["2027-01-26", "2027-01-26"]}},
  {"m": "February", "n": "Vasant Panchami / Saraswati Puja", "t": "India (WB, BH, UP, PB, KA)", "d": {"2025": ["2025-02-02", "2025-02-02"], "2026": ["2026-01-23", "2026-01-23"], "2027": ["2027-02-11", "2027-02-11"]}},
  {"m": "February", "n": "Thaipusam", "t": "India (TN) / MY / SG", "d": {"2025": ["2025-02-11", "2025-02-11"], "2026": ["2026-02-01", "2026-02-01"], "2027": ["2027-01-22", "2027-01-22"]}},
  {"m": "February", "n": "Valentine's Day", "t": "Global Commercial Festival", "d": {"2025": ["2025-02-14", "2025-02-14"], "2026": ["2026-02-14", "2026-02-14"], "2027": ["2027-02-14", "2027-02-14"]}},
  {"m": "February", "n": "Chhatrapati Shivaji Maharaj Jayanti", "t": "India Regional (Maharashtra)", "d": {"2025": ["2025-02-19", "2025-02-19"], "2026": ["2026-02-19", "2026-02-19"], "2027": ["2027-02-19", "2027-02-19"]}},
  {"m": "February", "n": "Maha Shivaratri", "t": "India National / Regional", "d": {"2025": ["2025-02-26", "2025-02-26"], "2026": ["2026-02-15", "2026-02-15"], "2027": ["2027-03-06", "2027-03-06"]}},
  {"m": "March", "n": "Ramadan Begins", "t": "Global Islamic Month", "d": {"2025": ["2025-03-01", "2025-03-01"], "2026": ["2026-02-18", "2026-02-18"], "2027": ["2027-02-08", "2027-02-08"]}},
  {"m": "March", "n": "Holi (Festival of Colors) / Lathmar Holi", "t": "India (North, West, East)", "d": {"2025": ["2025-03-14", "2025-03-14"], "2026": ["2026-03-03", "2026-03-03"], "2027": ["2027-03-22", "2027-03-22"]}},
  {"m": "March", "n": "Ugadi / Gudi Padwa / Cheti Chand", "t": "India Regional (KA, AP, TS, MH)", "d": {"2025": ["2025-03-30", "2025-03-30"], "2026": ["2026-03-19", "2026-03-19"], "2027": ["2027-04-07", "2027-04-07"]}},
  {"m": "March", "n": "Mother's Day (UK)", "t": "UK / Ireland", "d": {"2025": ["2025-03-30", "2025-03-30"], "2026": ["2026-03-15", "2026-03-15"], "2027": ["2027-03-14", "2027-03-14"]}},
  {"m": "March", "n": "Eid al-Fitr", "t": "Global Islamic Festival", "d": {"2025": ["2025-03-31", "2025-03-31"], "2026": ["2026-03-20", "2026-03-20"], "2027": ["2027-03-10", "2027-03-10"]}},
  {"m": "April", "n": "Vishu / Poila Baisakh / Bohag Bihu / Pana Sankranti / Tamil Puthandu", "t": "India Regional (KL, WB, AS, OR, TN)", "d": {"2025": ["2025-04-14", "2025-04-15"], "2026": ["2026-04-14", "2026-04-15"], "2027": ["2027-04-14", "2027-04-15"]}},
  {"m": "April", "n": "Good Friday & Easter", "t": "Global Christian Holiday", "d": {"2025": ["2025-04-18", "2025-04-20"], "2026": ["2026-04-03", "2026-04-05"], "2027": ["2027-03-26", "2027-03-28"]}},
  {"m": "April", "n": "Akshaya Tritiya", "t": "India National / Regional Peak", "d": {"2025": ["2025-04-30", "2025-04-30"], "2026": ["2026-04-19", "2026-04-19"], "2027": ["2027-05-08", "2027-05-08"]}},
  {"m": "May", "n": "Labor Day / May Day", "t": "Global / Europe / Asia", "d": {"2025": ["2025-05-01", "2025-05-01"], "2026": ["2026-05-01", "2026-05-01"], "2027": ["2027-05-01", "2027-05-01"]}},
  {"m": "May", "n": "Basava Jayanti", "t": "India Regional (Karnataka)", "d": {"2025": ["2025-05-01", "2025-05-01"], "2026": ["2026-04-20", "2026-04-20"], "2027": ["2027-05-09", "2027-05-09"]}},
  {"m": "May", "n": "Mother's Day (US / Global)", "t": "US, APAC, EU Holiday", "d": {"2025": ["2025-05-11", "2025-05-11"], "2026": ["2026-05-10", "2026-05-10"], "2027": ["2027-05-09", "2027-05-09"]}},
  {"m": "June", "n": "Eid al-Adha (Bakrid)", "t": "Global Islamic Festival", "d": {"2025": ["2025-06-06", "2025-06-06"], "2026": ["2026-05-27", "2026-05-27"], "2027": ["2027-05-16", "2027-05-16"]}},
  {"m": "June", "n": "Ratha Yatra", "t": "India Regional (Odisha, WB)", "d": {"2025": ["2025-06-27", "2025-06-27"], "2026": ["2026-07-16", "2026-07-16"], "2027": ["2027-07-05", "2027-07-05"]}},
  {"m": "June", "n": "Father's Day", "t": "Global Holiday", "d": {"2025": ["2025-06-15", "2025-06-15"], "2026": ["2026-06-21", "2026-06-21"], "2027": ["2027-06-20", "2027-06-20"]}},
  {"m": "July", "n": "Bonalu / Guru Purnima", "t": "India National / Regional", "d": {"2025": ["2025-07-10", "2025-07-10"], "2026": ["2026-06-29", "2026-06-29"], "2027": ["2027-07-18", "2027-07-18"]}},
  {"m": "August", "n": "Varamahalakshmi Vrata", "t": "India Regional (KA, AP, TS, TN)", "d": {"2025": ["2025-08-08", "2025-08-08"], "2026": ["2026-08-21", "2026-08-21"], "2027": ["2027-08-13", "2027-08-13"]}},
  {"m": "August", "n": "Raksha Bandhan", "t": "India Cultural Festival", "d": {"2025": ["2025-08-09", "2025-08-09"], "2026": ["2026-08-28", "2026-08-28"], "2027": ["2027-08-17", "2027-08-17"]}},
  {"m": "August", "n": "Independence Day (India)", "t": "India National Holiday", "d": {"2025": ["2025-08-15", "2025-08-15"], "2026": ["2026-08-15", "2026-08-15"], "2027": ["2027-08-15", "2027-08-15"]}},
  {"m": "August", "n": "Janmashtami (Krishna Jayanti)", "t": "India National / Regional", "d": {"2025": ["2025-08-16", "2025-08-16"], "2026": ["2026-09-04", "2026-09-04"], "2027": ["2027-08-24", "2027-08-24"]}},
  {"m": "August", "n": "Onam", "t": "India Regional (Kerala)", "d": {"2025": ["2025-09-05", "2025-09-05"], "2026": ["2026-08-26", "2026-08-26"], "2027": ["2027-09-12", "2027-09-12"]}},
  {"m": "August", "n": "Ganesh Chaturthi", "t": "India (MH, KA, AP, TS, TN, GJ)", "d": {"2025": ["2025-08-27", "2025-08-27"], "2026": ["2026-09-14", "2026-09-14"], "2027": ["2027-09-04", "2027-09-04"]}},
  {"m": "October", "n": "Gandhi Jayanti", "t": "India National Holiday", "d": {"2025": ["2025-10-02", "2025-10-02"], "2026": ["2026-10-02", "2026-10-02"], "2027": ["2027-10-02", "2027-10-02"]}},
  {"m": "October", "n": "Navratri / Durga Puja / Mysuru Dasara", "t": "India (WB, KA, GJ, North, East)", "d": {"2025": ["2025-09-22", "2025-10-02"], "2026": ["2026-10-11", "2026-10-20"], "2027": ["2027-09-30", "2027-10-09"]}},
  {"m": "October", "n": "Dussehra / Vijayadashami / Ayudha Puja", "t": "India National / Regional", "d": {"2025": ["2025-10-02", "2025-10-02"], "2026": ["2026-10-20", "2026-10-20"], "2027": ["2027-10-09", "2027-10-09"]}},
  {"m": "October", "n": "Karwa Chauth", "t": "India Regional (North India)", "d": {"2025": ["2025-10-09", "2025-10-09"], "2026": ["2026-10-29", "2026-10-29"], "2027": ["2027-10-18", "2027-10-18"]}},
  {"m": "October", "n": "Diwali / Deepavali / Kali Puja", "t": "India / Global Hindu Festival", "d": {"2025": ["2025-10-20", "2025-10-20"], "2026": ["2026-11-08", "2026-11-08"], "2027": ["2027-10-29", "2027-10-29"]}},
  {"m": "October", "n": "Halloween", "t": "US / EU / Global", "d": {"2025": ["2025-10-31", "2025-10-31"], "2026": ["2026-10-31", "2026-10-31"], "2027": ["2027-10-31", "2027-10-31"]}},
  {"m": "November", "n": "Kannada Rajyotsava", "t": "India Regional (Karnataka)", "d": {"2025": ["2025-11-01", "2025-11-01"], "2026": ["2026-11-01", "2026-11-01"], "2027": ["2027-11-01", "2027-11-01"]}},
  {"m": "November", "n": "Singles' Day (11.11)", "t": "Global Mega Shopping Event", "d": {"2025": ["2025-11-11", "2025-11-11"], "2026": ["2026-11-11", "2026-11-11"], "2027": ["2027-11-11", "2027-11-11"]}},
  {"m": "November", "n": "Thanksgiving", "t": "US Public Holiday", "d": {"2025": ["2025-11-27", "2025-11-27"], "2026": ["2026-11-26", "2026-11-26"], "2027": ["2027-11-25", "2027-11-25"]}},
  {"m": "November", "n": "Black Friday", "t": "Global Mega Peak", "d": {"2025": ["2025-11-28", "2025-11-28"], "2026": ["2026-11-27", "2026-11-27"], "2027": ["2027-11-26", "2027-11-26"]}},
  {"m": "December", "n": "Christmas Eve & Christmas Day", "t": "Global Public Holiday", "d": {"2025": ["2025-12-24", "2025-12-25"], "2026": ["2026-12-24", "2026-12-25"], "2027": ["2027-12-24", "2027-12-25"]}},
  {"m": "December", "n": "Boxing Day & Year-End Clearance", "t": "UK / AU / CA / Global", "d": {"2025": ["2025-12-26", "2025-12-31"], "2026": ["2026-12-26", "2026-12-31"], "2027": ["2027-12-26", "2027-12-31"]}},
  {"m": "December", "n": "New Year's Eve", "t": "Global Public Holiday", "d": {"2025": ["2025-12-31", "2025-12-31"], "2026": ["2026-12-31", "2026-12-31"], "2027": ["2027-12-31", "2027-12-31"]}}
];

const SP = { year: null, month: null };
const SP_YEARS = [2025, 2026, 2027];
const spDay = iso => Math.round((Date.parse(iso + "T00:00:00Z") - EPOCH) / DAY);
const spMonthOf = i => toDate(i).getUTCMonth();
const spMonthName = i => new Date(Date.UTC(2026, i, 1)).toLocaleDateString("en-IN", { month: "long", timeZone: "UTC" });
const spMonthName2 = i => spMonthName(i);
const escS = t => String(t).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
const spAvail = () => Math.min(M.last_day, yesterdayIdx());

function spDefaults(avail) {
  if (SP.year === null) {
    const years = SP_YEARS.filter(y => SPIKE_EVENTS.some(e => spDay(e.d[String(y)][0]) <= avail));
    SP.year = years.length ? years[years.length - 1] : 2026;
  }
  if (SP.month === null) {
    const starts = SPIKE_EVENTS.map(e => spDay(e.d[String(SP.year)][0])).filter(s => s <= avail);
    SP.month = starts.length ? spMonthOf(Math.max(...starts)) : 0;
  }
}
// Events that start in a given month of the selected year.
function spEvents(year, month) {
  return SPIKE_EVENTS.filter(e => spMonthOf(spDay(e.d[String(year)][0])) === month);
}
// This year's window for an event: the full range, and the part that is available so far.
function spWins(ev, year, avail) {
  const [s, e] = ev.d[String(year)].map(spDay);
  return { full: [s, e], cur: s <= avail ? [s, Math.min(e, avail)] : null };
}
// The same festival last year, on the date the calendar gives for last year. Same number of days as this year's available part.
function spPrev(ev, year, w) {
  const k = String(year - 1);
  if (!ev.d[k]) return null;
  const ps = spDay(ev.d[k][0]);
  const len = w.cur ? w.cur[1] - w.cur[0] + 1 : w.full[1] - w.full[0] + 1;
  const win = [ps, ps + len - 1];
  return win[1] <= M.last_day ? win : null;
}
// Merge overlapping or touching windows, so a day is counted once.
function spMerge(list) {
  const a = list.filter(Boolean).map(w => w.slice()).sort((x, y) => x[0] - y[0]);
  const out = [];
  for (const w of a) {
    const last = out[out.length - 1];
    if (last && w[0] <= last[1] + 1) last[1] = Math.max(last[1], w[1]);
    else out.push(w);
  }
  return out;
}
const spAdd = (x, y) => { const r = Object.assign({}, x); for (const k of Object.keys(y)) r[k] = (r[k] || 0) + y[k]; return r; };
function spAggWins(rows, wins, pass) {
  return wins.reduce((acc, w) => spAdd(acc, aggFor(rows, w[0], w[1], pass)), Object.assign({}, EMPTY_A));
}
function spDimWins(rows, wins, pos, pass) {
  const out = new Map();
  for (const w of wins) {
    for (const [k, a] of aggByDim(rows, w[0], w[1], pos, pass)) {
      out.set(k, out.has(k) ? spAdd(out.get(k), a) : Object.assign({}, a));
    }
  }
  return out;
}
// A group of events (one month, or the whole year), with this year and last year figures.
function spGroup(events, year, avail, rows) {
  const wins = events.map(ev => spWins(ev, year, avail));
  const prevs = events.map((ev, i) => spPrev(ev, year, wins[i]));
  const curWins = spMerge(wins.map(w => w.cur));
  const prevWins = spMerge(prevs);
  const C = spAggWins(rows, curWins, salesPass), P = spAggWins(rows, prevWins, salesPass);
  return { n: events.length, wins, prevs, curWins, prevWins, C, P, cm: metrics(C), pm: metrics(P), hasCur: wins.some(w => w.cur) };
}

function spikeBlock() {
  setPeriodBar("spike", null, null);
  const avail = spAvail();
  spDefaults(avail);
  const year = SP.year, month = SP.month, py = year - 1;
  const rows = baseSales();
  const gr = (a, b) => (b ? dlt(rel(a, b), "%", true) : "—");
  const yOpts = SP_YEARS.map(y => `<option value="${y}" ${y === year ? "selected" : ""}>${y}</option>`).join("");
  const mOpts = Array.from({ length: 12 }, (_, i) =>
    `<option value="${i}" ${i === month ? "selected" : ""}>${spMonthName2(i)}</option>`).join("");
  const controls = `<div class="chartctl">
    <label class="field">Year<select data-sp="year">${yOpts}</select></label>
    <label class="field">Month<select data-sp="month">${mOpts}</select></label></div>`;

  // Overview: one row per month of the selected year, then the full year.
  const monthRow = (label, g, attr) => {
    if (!g.n) return `<tr ${attr}><td>${label}</td><td>0</td><td colspan="6" class="muted">No spike days</td></tr>`;
    const pyOk = g.prevs.some(Boolean);
    return `<tr ${attr}><td>${label}</td><td>${g.n}</td>` +
      `<td>${g.hasCur ? num(g.cm.net) : "not yet"}</td><td>${pyOk ? num(g.pm.net) : "—"}</td>` +
      `<td>${g.hasCur && pyOk ? gr(g.cm.net, g.pm.net) : "—"}</td>` +
      `<td>${g.hasCur ? num(g.cm.txn) : "—"}</td><td>${pyOk ? num(g.pm.txn) : "—"}</td>` +
      `<td>${g.hasCur && pyOk ? gr(g.cm.txn, g.pm.txn) : "—"}</td></tr>`;
  };
  const monthBody = Array.from({ length: 12 }, (_, m) => {
    const sel = m === month ? "background:var(--line);" : "";
    return monthRow(spMonthName2(m), spGroup(spEvents(year, m), year, avail, rows), `data-spm="${m}" style="cursor:pointer;${sel}"`);
  }).join("") + monthRow("Full year", spGroup(SPIKE_EVENTS, year, avail, rows), `class="tot"`);
  const overview = `<section class="block"><h2>Spike days by month, ${year} against ${py}</h2>
    <p class="sub">Each spike day is compared with the same festival last year, on the date the calendar gives for that year. Days after yesterday are not counted yet. Tap a month to see its spike days.</p>
    <div class="scroll"><table class="tbl"><thead><tr><th>Month</th><th>Spike days</th><th>Net ${year}</th><th>Net ${py}</th><th>Growth</th>` +
    `<th>Orders ${year}</th><th>Orders ${py}</th><th>Growth</th></tr></thead><tbody>${monthBody}</tbody></table></div></section>`;

  // Detail for the selected month.
  const events = spEvents(year, month);
  const title = `Spike days, ${spMonthName2(month)} ${year} against ${spMonthName2(month)} ${py}`;
  if (!events.length) {
    return controls + overview + `<section class="block"><h2>${title}</h2><p class="muted">No spike days start in this month.</p></section>`;
  }
  const g = spGroup(events, year, avail, rows);
  const span = w => `${fmtDay(w[0])}${w[1] !== w[0] ? " – " + fmtDay(w[1]) : ""}`;
  const evRows = events.map((ev, i) => {
    const w = g.wins[i], p = g.prevs[i];
    const c = w.cur ? metrics(aggFor(rows, w.cur[0], w.cur[1], salesPass)) : null;
    const q = p ? metrics(aggFor(rows, p[0], p[1], salesPass)) : null;
    const dates = w.cur
      ? span(w.full) + (w.cur[1] < w.full[1] ? ` <span class="muted">(to ${fmtDay(w.cur[1])})</span>` : "")
      : `${span(w.full)} <span class="muted">not yet</span>`;
    return `<tr><td>${escS(ev.n)}<div class="muted" style="font-size:11px">${escS(ev.t)}</div></td><td>${dates}</td>` +
      `<td>${c ? num(c.net) : "—"}</td><td>${q ? num(q.net) : "—"}</td><td>${c && q ? gr(c.net, q.net) : "—"}</td>` +
      `<td>${c ? num(c.txn) : "—"}</td><td>${q ? num(q.txn) : "—"}</td><td>${c && q ? gr(c.txn, q.txn) : "—"}</td>` +
      `<td>${c ? num(c.aov) : "—"}</td><td>${q ? num(q.aov) : "—"}</td></tr>`;
  }).join("");
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
  const overall = `<div class="scroll"><table class="tbl"><thead><tr><th>Parameters</th><th>${year}</th><th>${py}</th><th>Growth</th></tr></thead><tbody>` +
    defs.map(([label, get, good, type]) => {
      const a = get(g.cm), b = get(g.pm);
      const chg = type === "pct" ? dlt(pts(a, b), " pts", good) : (b ? dlt(rel(a, b), "%", good) : "—");
      return `<tr><td>${label}</td><td>${fmt(a, type)}</td><td>${fmt(b, type)}</td><td>${chg}</td></tr>`;
    }).join("") + `</tbody></table></div>`;
  const dim = (name, pos) => {
    const ca = spDimWins(rows, g.curWins, pos, salesPass);
    const cb = spDimWins(rows, g.prevWins, pos, salesPass);
    const ks = [...new Set([...ca.keys(), ...cb.keys()])]
      .filter(k => (ca.get(k) || EMPTY_A).net || (cb.get(k) || EMPTY_A).net)
      .sort((x, y) => (ca.get(y) || EMPTY_A).net - (ca.get(x) || EMPTY_A).net);
    const line = (label, a, b, cls) => {
      const am = metrics(a), bm = metrics(b);
      return `<tr class="${cls || ""}"><td>${escS(label)}</td><td>${num(am.net)}</td><td>${num(bm.net)}</td>` +
        `<td>${gr(am.net, bm.net)}</td><td>${am.discPct.toFixed(1)}%</td><td>${bm.discPct.toFixed(1)}%</td>` +
        `<td>${dlt(pts(am.discPct, bm.discPct), " pts", false)}</td></tr>`;
    };
    const body = line("Total", g.C, g.P, "tot") + ks.map(k => line(nameOf(pos, k), ca.get(k) || EMPTY_A, cb.get(k) || EMPTY_A)).join("");
    return `<section class="block"><h2>By ${name}</h2><div class="scroll"><table class="tbl"><thead><tr><th></th>` +
      `<th>${year}</th><th>${py}</th><th>Growth</th><th>Dis %</th><th>${py} Dis %</th><th>Dis change</th></tr></thead>` +
      `<tbody>${body}</tbody></table></div></section>`;
  };
  const detail = `<section class="block"><h2>${title}</h2>
      <p class="sub">Each festival is compared with the same festival last year, on the date the calendar gives for that year. Values in ₹ except Txn and %.</p>
      <div class="scroll"><table class="tbl"><thead><tr><th>Spike day</th><th>Dates ${year}</th><th>Net ${year}</th><th>Net ${py}</th><th>Growth</th>` +
      `<th>Orders ${year}</th><th>Orders ${py}</th><th>Growth</th><th>AOV ${year}</th><th>AOV ${py}</th></tr></thead>` +
      `<tbody>${evRows}</tbody></table></div></section>` +
    `<section class="block"><h2>Spike days in ${spMonthName2(month)}, overall</h2><p class="sub">All spike days of the month together, with each day counted once.</p>${overall}</section>` +
    dim("brand", 2) + dim("region", 4) + dim("source", 5) + dim("session", 6);
  return controls + overview + detail;
}

// Top tab after KPT and O2D.
SUBS.spike = [];
const renderHeaderKpt = renderHeader;
renderHeader = function () {
  renderHeaderKpt();
  const l1 = document.getElementById("l1");
  if (l1 && !l1.querySelector('[data-top="spike"]')) {
    l1.insertAdjacentHTML("beforeend", `<button data-top="spike" aria-pressed="${S.top === "spike"}">Spike Days</button>`);
  }
};
const renderFiltersBase = renderFilters;
renderFilters = function () {
  if (S.top !== "spike") return renderFiltersBase();
  const top = S.top;
  S.top = "sales";                       // the spike tab uses the same filters as Sales (Brand, Region, Source, Session, Store)
  try { renderFiltersBase(); } finally { S.top = top; }
};
const renderSpikeBase = render;
render = function () {
  if (S.top !== "spike") return renderSpikeBase();
  if (!M) return;
  renderHeader();
  renderFilters();
  setPeriodBar("spike", null, null);
  renderPeriodBar();
  document.getElementById("view").innerHTML = spikeBlock();
  renderNote();
};
document.addEventListener("click", e => {
  if (e.target.closest('[data-top="spike"]')) { S.sel = null; render(); return; }
  const row = e.target.closest("[data-spm]");
  if (row) { SP.month = +row.dataset.spm; render(); }
});
document.addEventListener("change", e => {
  const k = e.target.dataset.sp;
  if (!k) return;
  SP[k] = +e.target.value;
  render();
});
