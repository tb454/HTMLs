let limit  = +(new URLSearchParams(location.search).get("limit")  || 25);
let offset = +(new URLSearchParams(location.search).get("offset") || 0);

function buildQuery(extra={}) {
  const p = new URLSearchParams({limit, offset, ...extra});
  return "/contracts?" + p.toString();
}

async function loadContracts(extraFilters = {}) {
  // If a previous load is in-flight, abort it to avoid piling renders
  if (_contractsLoading && _contractsCtl) {
    _contractsCtl.abort();
  }
  _contractsLoading = true;
  _contractsCtl = new AbortController();

  try {
    const res = await fetch(buildQuery(extraFilters), {
      credentials: "same-origin",
      signal: _contractsCtl.signal
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();

    // Clear Contracts table body if present (prevents row stacking if renderer appends)
    const _ctb = document.querySelector("#contracts-table tbody");
    if (_ctb) _ctb.innerHTML = "";

    renderContractsTable(data.items || data);

    const count = (data.items?.length ?? data.length ?? 0);
  } catch (err) {
    if (err?.name !== "AbortError") console.error(err);
  } finally {
    _contractsLoading = false;
  }
}

let _contractsCtl = null;
let _contractsLoading = false;

function activeFilters(){
  // read your filter inputs here, e.g. status/date/material
  return {
    status: document.getElementById("statusFilter")?.value || "",
    material_code: document.getElementById("materialFilter")?.value || ""
  };
}

document.addEventListener("DOMContentLoaded", () => {
  if (!document.getElementById("contracts-table")) return;
  if (typeof window.renderContractsTable !== "function") return; // avoid admin IIFE
  loadContracts(activeFilters());
}, { once: true });

