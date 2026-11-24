/* ============ Sales: net sales by source, as a column chart, on the Daily, Weekly and Monthly tabs ============
   Daily: the last 30 days. Weekly: the last 12 weeks (Monday to Sunday). Monthly: the last 12 months.
   Each bar is split by source, with the total above it, each source's value (₹ lakh) inside its segment when it is
   tall enough, and a label under it (the weekday and date; the week number and start date; the month and year).
   A week or month that is still running is marked * (to date). The page's Sales filters apply.
   The older chart with the same heading is hidden on that tab. Loaded last. It wraps render(). */
(function () {
  const L30_TABS = {
    daily:   { n: 30, bw: 34, title: "Net sales, last 30 days (labels in ₹ lakh)", old: /^Net sales, last 30 days/,
               sub: "Each bar is a day, split by source. The total is above each bar." },
    weekly:  { n: 12, bw: 62, title: "Net sales, last 12 weeks (labels in ₹ lakh)", old: /^Net sales, last 12 weeks/,
               sub: "Each bar is a week (Monday to Sunday), split by source. * This week is to date." },
    monthly: { n: 12, bw: 62, title: "Net sales, last 12 months (labels in ₹ lakh)", old: /^Net sales, last 12 months/,
               sub: "Each bar is a month, split by source. * This month is to date." }
  };
  const l30Lakh = paise => (paise / 1e7).toFixed(1);
  const l30Esc = s => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  const l30Month = d => (toDate(d).getUTCFullYear() - 2025) * 12 + toDate(d).getUTCMonth();
  const l30Dm = d => toDate(d).toLocaleDateString("en-IN", { day: "numeric", month: "short", timeZone: "UTC" });

  // The key of each bar: the day, the Monday of the week, or the month.
  function l30Keys(tab) {
    const end = M.last_day, n = L30_TABS[tab].n;
    if (tab === "daily") return Array.from({ length: n }, (_, i) => end - (n - 1) + i);
    if (tab === "weekly") { const last = mondayOf(end); return Array.from({ length: n }, (_, i) => last - 7 * (n - 1 - i)); }
    const k = l30Month(end);
    return Array.from({ length: n }, (_, i) => k - (n - 1) + i);
  }
  // The first and last day of a bar, within the data.
  function l30Span(tab, key) {
    const end = M.last_day;
    if (tab === "daily") return [key, key];
    if (tab === "weekly") return [key, Math.min(key + 6, end)];
    return [monStart(key), Math.min(monEnd(key), end)];
  }
  // Which bar a day belongs to.
  function l30Bucket(tab, d) {
    if (tab === "daily") return d;
    if (tab === "weekly") return mondayOf(d);
    return l30Month(d);
  }
  // The sales of each source and the total (paise) for each bar, with the page's filters.
  function l30Data(tab) {
    const keys = l30Keys(tab);
    const bars = keys.map(k => ({ key: k, span: l30Span(tab, k), src: M.sources.map(() => 0), total: 0 }));
    const at = new Map(keys.map((k, i) => [k, i]));
    for (const r of baseSales()) {
      const i = at.get(l30Bucket(tab, r[0]));
      if (i === undefined || !salesPass(r)) continue;
      bars[i].src[r[5]] += r[7];
      bars[i].total += r[7];
    }
    return bars;
  }
  // ISO week number of a day.
  function l30Iso(d) {
    const t = new Date(toDate(d));
    t.setUTCDate(t.getUTCDate() + 4 - (t.getUTCDay() || 7));
    const ys = new Date(Date.UTC(t.getUTCFullYear(), 0, 1));
    return Math.ceil(((t - ys) / 86400000 + 1) / 7);
  }
  // The two labels under a bar, and whether the bar is still running.
  function l30Label(tab, b) {
    if (tab === "daily") {
      return { l1: toDate(b.key).toLocaleDateString("en-IN", { weekday: "short", timeZone: "UTC" }), l2: l30Dm(b.key), partial: false };
    }
    if (tab === "weekly") {
      const partial = b.span[1] < b.key + 6;
      return { l1: `W${l30Iso(b.key)}${partial ? "*" : ""}`, l2: l30Dm(b.key), partial };
    }
    const partial = b.span[1] < monEnd(b.key);
    const first = toDate(monStart(b.key));
    return { l1: first.toLocaleDateString("en-IN", { month: "short", timeZone: "UTC" }) + (partial ? "*" : ""),
             l2: String(first.getUTCFullYear()), partial };
  }

  function l30Svg(tab, bars) {
    const bw = L30_TABS[tab].bw, n = bars.length, H = 230, T = 22, B = 46, L = 6;
    const W = L + n * bw + 6;
    const max = Math.max(...bars.map(x => x.total), 1) * 1.12;
    const y = v => T + (1 - v / max) * (H - T - B);
    let s = `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" role="img" aria-label="Net sales by source">`;
    bars.forEach((x, i) => {
      const cx = L + i * bw + bw / 2, w = bw - 8;
      const lab = l30Label(tab, x);
      let base = H - B;
      x.src.forEach((v, j) => {
        if (!v) return;
        const h = (v / max) * (H - T - B);
        const top = base - h;
        s += `<rect x="${(cx - w / 2).toFixed(1)}" y="${top.toFixed(1)}" width="${w}" height="${h.toFixed(1)}" style="fill:${SRC_COLOR[j % SRC_COLOR.length]}"/>`;
        if (h >= 13) s += `<text x="${cx}" y="${(top + h / 2 + 3).toFixed(1)}" text-anchor="middle" font-size="8" style="fill:#fff">${l30Lakh(v)}</text>`;
        base = top;
      });
      s += `<text x="${cx}" y="${(y(x.total) - 6).toFixed(1)}" text-anchor="middle" font-size="9" font-weight="700" style="fill:var(--ink)">${l30Lakh(x.total)}</text>`;
      s += `<text x="${cx}" y="${H - 28}" text-anchor="middle" font-size="9" font-weight="700" style="fill:var(--ink)">${l30Esc(lab.l1)}</text>`;
      s += `<text x="${cx}" y="${H - 16}" text-anchor="middle" font-size="8" style="fill:var(--ink-2)">${l30Esc(lab.l2)}</text>`;
    });
    return s + "</svg>";
  }

  function l30Block(tab) {
    const bars = l30Data(tab);
    const legend = M.sources.map((n, j) => `<span><i class="sw" style="--c:${SRC_COLOR[j % SRC_COLOR.length]}"></i>${l30Esc(n === "In Store" ? "Offline (In Store)" : n)}</span>`).join("");
    const block = document.createElement("section");
    block.className = "block l30-block";
    block.innerHTML = `<h2>${L30_TABS[tab].title}</h2><p class="sub">${L30_TABS[tab].sub}</p>` +
      `<div class="l30-scroll">${l30Svg(tab, bars)}</div><div class="legend">${legend}</div>`;
    return block;
  }

  // Hides the older chart with the same heading on this tab, and puts the new chart after the session table.
  function l30Apply(tab) {
    const view = document.getElementById("view");
    if (!view) return;
    for (const el of Array.from(document.querySelectorAll("h1,h2,h3,p,span,div"))) {
      if (el.closest && el.closest(".l30-block")) continue;
      if (el.children.length === 0 && L30_TABS[tab].old.test((el.textContent || "").trim())) {
        const box = el.closest("section") || el.parentElement;
        if (box && !box.classList.contains("l30-block")) box.style.display = "none";
      }
    }
    const block = l30Block(tab);
    const sess = Array.from(view.querySelectorAll("h2")).find(h => h.textContent.trim() === "By session");
    const anchor = sess ? (sess.closest("section") || sess) : null;
    if (anchor) anchor.insertAdjacentElement("afterend", block);
    else view.appendChild(block);
  }

  // Wrap the page's render: the chart is redrawn after each redraw of the Daily, Weekly and Monthly tabs.
  const renderBaseL30 = render;
  render = function () {
    renderBaseL30();
    if (typeof S === "undefined" || S.top !== "sales" || !M || !ROLL) return;
    if (!L30_TABS[S.sub]) return;
    try {
      l30Apply(S.sub);
    } catch (err) {
      console.error("sales chart:", err);
    }
  };

  (function l30Style() {
    if (document.getElementById("l30-style")) return;
    const st = document.createElement("style");
    st.id = "l30-style";
    st.textContent = `.l30-scroll{overflow-x:auto;margin-top:8px}.l30-scroll svg{display:block}`;
    document.head.appendChild(st);
  })();

  window.FB_L30 = { l30Data, l30Svg, l30Label, l30Iso, l30Days: () => l30Data("daily") };
})();
