let limit  = +(new URLSearchParams(location.search).get("limit")  || 25);
let offset = +(new URLSearchParams(location.search).get("offset") || 0);

function buildQuery(extra={}) {
  const p = new URLSearchParams({limit, offset, ...extra});
  return "/contracts?" + p.toString();
}

async function loadContracts(extraFilters={}) {
  const res = await fetch(buildQuery(extraFilters), { credentials: "same-origin" });
  const data = await res.json();
  renderContractsTable(data.items || data);
  document.getElementById("prevBtn").disabled = (offset <= 0);
  document.getElementById("nextBtn").disabled = (data.items?.length ?? data.length) < limit;
}

document.getElementById("prevBtn").onclick = () => { offset = Math.max(0, offset - limit); loadContracts(activeFilters()); };
document.getElementById("nextBtn").onclick = () => { offset = offset + limit; loadContracts(activeFilters()); };

function activeFilters(){
  // read your filter inputs here, e.g. status/date/material
  return {
    status: document.getElementById("statusFilter")?.value || "",
    material_code: document.getElementById("materialFilter")?.value || ""
  };
}
loadContracts(activeFilters());
