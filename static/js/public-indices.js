// public-indices.js — BRidge public list
(function () {
  'use strict';

  // ---- tiny helpers
  const $  = (sel) => document.querySelector(sel);
  const $$ = (sel) => Array.from(document.querySelectorAll(sel));

function toUSDlb(v, unit){
  const n = Number(v);
  if (!Number.isFinite(n)) return null;
  const u = String(unit || '').toLowerCase();
  if (u.includes('/lb')) return n;
  if (u.includes('/ton')) return n / 2000;
  return n > 100 ? n / 2000 : n; // heuristic fallback
}
const fmt = (n, dp = 4) =>
  (n == null || Number.isNaN(+n))
    ? "-"
    : Number(n).toLocaleString(undefined, { minimumFractionDigits: dp, maximumFractionDigits: dp });

async function jget(path) {
  const r = await fetch(path, {
    headers: { "Accept": "application/json" },
    credentials: "omit",
    cache: "no-store"
  });
  if (!r.ok) throw new Error(`${path} -> ${r.status}`);
  return r.json();
}

// ---- polite copy deterrents (public only; cosmetic)
document.addEventListener("contextmenu", (e) => e.preventDefault());
document.addEventListener("selectstart", (e) => e.preventDefault());
document.addEventListener("copy", (e) => {
  e.preventDefault();
  e.clipboardData.setData("text/plain", "BRidge Indices — https://scrapfutures.com/indices");
});

// ---- skeleton helper (no external CSS; still CSP-safe because it’s not inline <script>)
function showSkeleton(tbody, cols) {
  tbody.innerHTML = "";
  for (let i = 0; i < 6; i++) {
    const tr = document.createElement("tr");
    for (let c = 0; c < cols; c++) {
      const td = document.createElement("td");
      td.innerHTML = '<span class="s" style="display:inline-block;width:100%;height:1em;background:linear-gradient(90deg,#152036,#0e1524,#152036);background-size:200% 100%;animation:sh 1.1s linear infinite;border-radius:4px"></span>';
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
}
(function injectKeyframes(){
  const css = "@keyframes sh{0%{background-position:200% 0}100%{background-position:-200% 0}}";
  const style = document.createElement("style");
  style.textContent = css;
  document.head.appendChild(style);
})();

// ---- main loader
async function loadPublicIndices() {
  const tbody     = $("#tbody");
  const updatedEl = $("#lastUpdated");
  const modeEl    = $("#modePill");
  const statusEl  = $("#statusText");

  showSkeleton(tbody, 5);

  try {
    // 1) universe
    const uni = await jget("/public/indices/universe");

    // Accept: array OR {tickers:[...]} OR {items:[...]}
    const raw = Array.isArray(uni) ? uni
              : (Array.isArray(uni?.tickers) ? uni.tickers
              : (Array.isArray(uni?.items) ? uni.items : []));

    let symbols = [...new Set(
      raw
        .map(d => {
          if (typeof d === "string") return d.trim();
          return String(d.symbol || d.ticker || "").trim();
        })
        .filter(Boolean)
        .filter(s => !/\s/.test(s))
    )].sort((a, b) => a.localeCompare(b));

    // 2) seed rows
    tbody.innerHTML = symbols.map(sym => `
      <tr data-sym="${sym}">
        <td class="mono" style="font-weight:800;">${sym}</td>
        <td class="last muted">…</td>
        <td class="delta muted">…</td>
        <td class="updated muted">…</td>
        <td class="right"><a class="btn" href="/index/${encodeURIComponent(sym)}">View</a></td>
      </tr>
    `).join("");

    // 3) fill Last / Δ / Updated from ONE daily.json call
    let metaSet = false;

    let payload = null;
    try {
      payload = await jget("/public/indices/daily.json");
    } catch (e) {
      console.warn("daily.json failed", e);
    }

    if (payload) {
      const rowsAll = Array.isArray(payload?.rows) ? payload.rows
                  : Array.isArray(payload?.data) ? payload.data
                  : Array.isArray(payload) ? payload
                  : [];

      const blendedRows = rowsAll.filter(
        r => r && String(r.region || '').trim().toLowerCase() === 'blended'
      );

      const rows = (blendedRows.length ? blendedRows : rowsAll)
        .map(r => ({
          sym: String(r.symbol || r.ticker || '').trim(),
          dt:  (r.as_of_date || r.dt || r.date || '').toString(),
          unit: r.unit || payload?.unit || 'USD/ton',
          raw:  (r.index_price_per_ton ?? r.avg_price ?? r.price ?? r.close_price ?? r.close ?? r.value)
        }))
        .filter(r => r.sym && r.dt && !/\s/.test(r.sym));

      // max date for header
      let maxDate = null;
      for (const r of rows) {
        if (!maxDate || new Date(r.dt) > new Date(maxDate)) maxDate = r.dt;
      }

      // build series per symbol
      const bySym = new Map();
      for (const r of rows) {
        const v = toUSDlb(r.raw, r.unit);
        if (v == null) continue;
        if (!bySym.has(r.sym)) bySym.set(r.sym, []);
        bySym.get(r.sym).push({ dt: r.dt, value: v });
      }
      for (const [k, arr] of bySym.entries()) {
        arr.sort((a,b)=> new Date(a.dt) - new Date(b.dt));
      }

      const liveSymbols = Array.from(bySym.keys())
        .filter(s => s && !/\s/.test(s))
        .sort((a, b) => a.localeCompare(b));

      if (liveSymbols.length) {
        const liveMap = new Map(
          liveSymbols.map(sym => [String(sym).toUpperCase(), sym])
        );

        const filtered = symbols.filter(sym =>
          liveMap.has(String(sym).toUpperCase())
        );

        symbols = filtered.length
          ? filtered.map(sym => liveMap.get(String(sym).toUpperCase()) || sym)
          : liveSymbols;

        tbody.innerHTML = symbols.map(sym => `
          <tr data-sym="${sym}">
            <td class="mono" style="font-weight:800;">${sym}</td>
            <td class="last muted">…</td>
            <td class="delta muted">…</td>
            <td class="updated muted">…</td>
            <td class="right"><a class="btn" href="/index/${encodeURIComponent(sym)}">View</a></td>
          </tr>
        `).join("");
      }

      // header pills
      if (modeEl) modeEl.textContent = "Mode: Public";
      if (updatedEl && maxDate) updatedEl.textContent = `Index Date: ${maxDate}`;
      metaSet = true;

      // fill table
      for (const sym of symbols) {
        const row = tbody.querySelector(`tr[data-sym="${CSS.escape(sym)}"]`);
        if (!row) continue;

        const series = bySym.get(sym) || [];
        const n = series.length;
        const last = n ? series[n-1].value : null;
        const prev = n > 1 ? series[n-2].value : null;
        const delta = (last != null && prev != null) ? last - prev : null;
        const when = n ? series[n-1].dt : (maxDate || null);

        row.querySelector(".last").textContent = fmt(last);

        const dEl = row.querySelector(".delta");
        if (delta == null) {
          dEl.textContent = "-";
          dEl.className = "delta muted";
        } else {
          dEl.textContent = (delta >= 0 ? "+" : "") + fmt(delta);
          dEl.className = "delta " + (delta >= 0 ? "pos" : "neg");
        }

        row.querySelector(".updated").textContent = when || "—";
      }
    }

    // header/status
    const nowUtc = new Date().toISOString().replace("T", " ").slice(0, 16);
    if (updatedEl) {
      const prior = updatedEl.textContent || "";
      if (prior.startsWith("Index Date:")) updatedEl.textContent = `${prior} · Updated: ${nowUtc} (UTC)`;
      else updatedEl.textContent = "Updated: " + nowUtc + " (UTC)";
    }
    if (modeEl && !metaSet) modeEl.textContent = "Mode: Public";
    if (statusEl)  statusEl.textContent  = "Live";
  } catch (e) {
    tbody.innerHTML = `<tr><td colspan="5" class="muted">Error loading indices.</td></tr>`;
    if (statusEl) statusEl.textContent = "Degraded";
  }
}

// ---- filter (client-side)
function filterRows() {
  const q = ($("#filterInput").value || "").trim().toUpperCase();
  $("#tbody").querySelectorAll("tr").forEach(tr => {
    const sym = (tr.getAttribute("data-sym") || "").toUpperCase();
    tr.style.display = (!q || sym.includes(q)) ? "" : "none";
  });
}

// ---- wire & init
document.getElementById("refreshBtn")?.addEventListener("click", loadPublicIndices);
document.getElementById("filterInput")?.addEventListener("input", filterRows);
window.addEventListener("DOMContentLoaded", loadPublicIndices);

})();