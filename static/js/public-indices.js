// public-indices.js — BRidge public list

// ---- tiny helpers
const $  = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));
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
    const defs = await jget("/public/indices/universe");   // [{symbol,...}]
    const symbols = (Array.isArray(defs) ? defs : [])
      .map(d => String(d.symbol || "").trim())
      .filter(Boolean)
      .sort((a, b) => a.localeCompare(b));

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

    // 3) fill Last / Δ / Updated (sequential = friendly to backend)
    let metaSet = false
    for (const sym of symbols) {
      try {
        // Public mirror detail endpoint (indices_daily) — returns {latest, history, subscriber, delay_days, index_date}
        const data = await jget(`/public/index/${encodeURIComponent(sym)}/data?region=blended`);

        // Set global page labels once, from any successful response
        if (!metaSet && data) {
          const isSub = !!data.subscriber;
          const delay = (data.delay_days ?? null);
          const idxDate = (data.index_date ?? null);

          // mode pill
          modePill.textContent = isSub ? "Mode: Subscriber" : `Mode: Public${delay != null ? ` (T-${delay})` : ""}`;

          // last updated pill shows the effective index date
          if (idxDate) lastUpdated.textContent = `Index Date: ${idxDate}`;
          metaSet = true;
        }

        // Normalize series from the response
        const seriesRaw = (data && Array.isArray(data.history)) ? data.history
                        : (data && Array.isArray(data.rows)) ? data.rows
                        : [];

        // Ensure asc by date
        const series = seriesRaw
          .filter(x => x && x.as_of_date && x.index_price_per_ton != null)
          .sort((a,b) => new Date(a.as_of_date) - new Date(b.as_of_date));

        const n = series.length;
        const last = n ? +series[n-1].index_price_per_ton : null;
        const prev = n > 1 ? +series[n-2].index_price_per_ton : null;
        const when = n ? (series[n-1].as_of_date || null) : null;

        const row = tbody.querySelector(`tr[data-sym="${CSS.escape(sym)}"]`);
        if (!row) continue;

        // last
        const lastEl = row.querySelector(".last");
        lastEl.textContent = fmt(last);

        // delta
        const dEl = row.querySelector(".delta");
        const delta = (last != null && prev != null) ? last - prev : null;
        if (delta == null) {
          dEl.textContent = "-";
          dEl.className = "delta muted";
        } else {
          dEl.textContent = (delta >= 0 ? "+" : "") + fmt(delta);
          dEl.className = "delta " + (delta >= 0 ? "pos" : "neg");
        }

        // updated
        row.querySelector(".updated").textContent = when || "—";
      } catch {
        // keep skeleton for that row if it fails
      }
    }

    // header/status
    const nowUtc = new Date().toISOString().replace("T", " ").slice(0, 16);
    if (updatedEl) updatedEl.textContent = "Updated: " + nowUtc + " (UTC)";
    if (modeEl)    modeEl.textContent    = "Mode: Public";
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
