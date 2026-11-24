/* ============ Morning Call: product mix, week on week ============
   Category sales for the reference day, against the same weekday a week earlier: the net sales, each category's share
   of the total (mix %), the change in that share, and the week on week growth. The data is product_mix.json.gz, built
   from the discount product rows. The Sales brand, region and source filters apply, and so does the channel bar at the
   top of the Morning Call. Product rows have no store, so the store and store-type filters do not apply here.
   Loaded after morning.js. It wraps salesPeriodView() for the Morning Call tab only. */
(function () {
  const PM = { data: null, loading: false, error: null };
  const pmEsc = s => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  const pmOk = v => v !== null && v !== undefined && isFinite(v);
  const pmG = (c, p) => (pmOk(c) && pmOk(p) && p ? (c / p - 1) * 100 : null);
  const pmP = v => (pmOk(v) ? (v >= 0 ? "+" : "−") + Math.abs(v).toFixed(1) + "%" : "—");
  const pmPts = v => (pmOk(v) ? (v >= 0 ? "+" : "−") + Math.abs(v).toFixed(2) + " pts" : "—");
  const pmC = v => (pmOk(v) ? (v >= 0 ? "var(--up)" : "var(--down)") : "var(--ink-2)");
  const pmRs = paise => (pmOk(paise) ? "₹" + num(paise / 100) : "—");
  const PM_EPOCH = Date.UTC(2025, 0, 1);

  async function pmLoad() {
    if (PM.data || PM.loading) return;
    PM.loading = true;
    try {
      PM.data = await loadGz("data/product_mix.json.gz");
    } catch (err) {
      PM.error = err.message;
    }
    PM.loading = false;
    render();
  }

  // Does a product row pass the Sales filters and the channel bar? Rows are matched by name.
  function pmPass(d, b, rg, s) {
    const S = window.S || {};
    const brandName = d.brands[b], regionName = d.regions[rg], sourceName = d.sources[s];
    if (S.brand !== undefined && S.brand !== "all" && M.brands[+S.brand] !== brandName) return false;
    if (S.region !== undefined && S.region !== "all" && M.regions[+S.region] !== regionName) return false;
    if (S.source !== undefined && S.source !== "all" && M.sources[+S.source] !== sourceName) return false;
    const ch = window.FB_SALES_CH ? window.FB_SALES_CH.value : "all";
    if (ch === "offline" && sourceName !== "In Store") return false;
    if (ch === "online" && sourceName === "In Store") return false;
    return true;
  }

  // Net sales by category for the reference day and the same weekday a week earlier.
  function pmCompute() {
    const d = PM.data;
    const refDay = Math.round((Date.parse(d.ref + "T00:00:00Z") - PM_EPOCH) / 86400000);
    const prevDay = refDay - 7;
    const n = d.categories.length;
    const zero = () => d.categories.map(() => ({ net: 0, gross: 0, disc: 0, qty: 0, orders: 0 }));
    const cur = zero(), prev = zero();
    for (const r of d.rows) {
      const [day, b, rg, s, c, net, gross, disc, qty, orders] = r;
      if (day !== refDay && day !== prevDay) continue;
      if (!pmPass(d, b, rg, s)) continue;
      const acc = day === refDay ? cur : prev;
      acc[c].net += net; acc[c].gross += gross; acc[c].disc += disc; acc[c].qty += qty; acc[c].orders += orders;
    }
    const total = arr => arr.reduce((t, x) => t + x.net, 0);
    const totCur = total(cur), totPrev = total(prev);
    const rows = d.categories.map((name, i) => {
      const mixCur = totCur ? cur[i].net / totCur * 100 : null;
      const mixPrev = totPrev ? prev[i].net / totPrev * 100 : null;
      return { name, cur: cur[i].net, prev: prev[i].net, mixCur, mixPrev,
               mixChange: pmOk(mixCur) && pmOk(mixPrev) ? mixCur - mixPrev : null, wow: pmG(cur[i].net, prev[i].net) };
    }).filter(x => x.cur > 0 || x.prev > 0).sort((a, b) => b.cur - a.cur);
    return { refDay, prevDay, rows, totCur, totPrev, totWow: pmG(totCur, totPrev) };
  }

  function pmBlock() {
    if (!PM.data) {
      if (!PM.error) pmLoad();
      return `<section class="block"><h2>Product mix: week on week</h2>` +
        (PM.error ? `<p class="muted">Product mix data could not be loaded: ${pmEsc(PM.error)}</p>`
                  : `<p class="muted">Loading the product mix.</p>`) + `</section>`;
    }
    const c = pmCompute();
    const rows = c.rows.map(x => `<tr><td>${pmEsc(x.name)}</td><td>${pmRs(x.cur)}</td>` +
      `<td>${pmOk(x.mixCur) ? x.mixCur.toFixed(1) + "%" : "—"}</td><td>${pmRs(x.prev)}</td>` +
      `<td>${pmOk(x.mixPrev) ? x.mixPrev.toFixed(1) + "%" : "—"}</td>` +
      `<td style="color:${pmC(x.wow)}">${pmP(x.wow)}</td><td style="color:${pmC(x.mixChange)}">${pmPts(x.mixChange)}</td></tr>`).join("");
    const total = `<tr class="reg-total"><td>Total</td><td>${pmRs(c.totCur)}</td><td>100.0%</td><td>${pmRs(c.totPrev)}</td>` +
      `<td>100.0%</td><td style="color:${pmC(c.totWow)}">${pmP(c.totWow)}</td><td>—</td></tr>`;
    const head = `<tr><th>Category</th><th>Net sales, ${pmEsc(fmtDay(c.refDay))}</th><th>Mix %</th>` +
      `<th>Net sales, ${pmEsc(fmtDay(c.prevDay))}</th><th>Mix %</th><th>WoW %</th><th>Mix change</th></tr>`;
    return `<section class="block"><h2>Product mix: week on week</h2>` +
      `<p class="sub">Category sales for ${pmEsc(fmtDay(c.refDay))} against the same weekday a week earlier. Mix = the category's share of the total. ` +
      `The Sales filters and the channel bar above apply.</p>` +
      `<div class="scroll"><table class="tbl"><thead>${head}</thead><tbody>${total}${rows}</tbody></table></div></section>`;
  }

  // Shown on the Morning Call only, under the other tables.
  const salesPeriodViewPm = salesPeriodView;
  salesPeriodView = function (kind) {
    const base = salesPeriodViewPm(kind);
    if (kind !== "morning") return base;
    return base + pmBlock();
  };

  window.FB_PM = { PM, pmCompute, pmPass };
})();
