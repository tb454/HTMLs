// public-indices.js (public mirror)
(function () {
  // deterrents (not real protection, but fine for casuals)
  document.addEventListener("contextmenu", (e) => e.preventDefault());
  document.addEventListener("selectstart", (e) => e.preventDefault());
  document.addEventListener("copy", (e) => {
    e.preventDefault();
    e.clipboardData.setData("text/plain", "BRidge Indices — https://bridge.scrapfutures.com/indices");
  });

  const fmt = (n) => (n === null || n === undefined) ? "-" : (typeof n === "number" ? n.toFixed(4) : String(n));
  const tbody = document.getElementById("tbody");
  const filterInput = document.getElementById("filterInput");
  const lastUpdated = document.getElementById("lastUpdated");
  const modePill = document.getElementById("modePill");

  let rows = [];

  function render() {
    const q = (filterInput.value || "").trim().toUpperCase();
    const filtered = !q ? rows : rows.filter(r => (r.ticker || "").toUpperCase().includes(q));

    if (!filtered.length) {
      tbody.innerHTML = `<tr><td colspan="5" class="muted">No matches.</td></tr>`;
      return;
    }

    tbody.innerHTML = filtered.map(r => {
      const d = (typeof r.delta === "number") ? r.delta : null;
      const deltaCls = (d === null) ? "muted" : (d >= 0 ? "delta pos" : "delta neg");
      const deltaText = (d === null) ? "-" : (d >= 0 ? "+" + d.toFixed(4) : d.toFixed(4));

      return `
        <tr>
          <td style="font-weight:800;">${r.ticker || "-"}</td>
          <td>${fmt(r.last)}</td>
          <td><span class="${deltaCls}">${deltaText}</span></td>
          <td class="muted">${r.updated || "-"}</td>
          <td class="right"><a class="btn" href="/index/${encodeURIComponent(r.ticker)}">View</a></td>
        </tr>
      `;
    }).join("");
  }

  async function load() {
    tbody.innerHTML = `<tr><td colspan="5" class="muted">Loading…</td></tr>`;
    const res = await fetch("/api/public/indices", { headers: { "Accept": "application/json" } });
    const data = await res.json();
    rows = Array.isArray(data?.items) ? data.items : (Array.isArray(data) ? data : []);
    lastUpdated.textContent = "Updated: " + (data?.server_time || "—");
    modePill.textContent = "Mode: " + (data?.mode || "Public");
    render();
  }

  document.getElementById("refreshBtn").addEventListener("click", load);
  filterInput.addEventListener("input", render);

  load().catch(err => {
    tbody.innerHTML = `<tr><td colspan="5" class="muted">Error loading indices.</td></tr>`;
    console.error(err);
  });
})();
