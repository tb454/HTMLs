// static/js/yard_settings.js

let currentYardId = null;
let yardMap = new Map();
let pricingRules = [];
let hedgeRules = [];

function setText(id, text) {
  const el = document.getElementById(id);
  if (el) el.textContent = text || "";
}

async function apiGet(url) {
  const res = await fetch(url, { credentials: "include" });
  if (!res.ok) {
    const msg = await res.text().catch(() => res.statusText);
    throw new Error(`GET ${url} failed: ${res.status} ${msg}`);
  }
  return res.json();
}

async function apiPost(url, body) {
  const res = await fetch(url, {
    method: "POST",
    credentials: "include",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {})
  });
  if (!res.ok) {
    const msg = await res.text().catch(() => res.statusText);
    throw new Error(`POST ${url} failed: ${res.status} ${msg}`);
  }
  return res.json();
}

async function apiPut(url, body) {
  const res = await fetch(url, {
    method: "PUT",
    credentials: "include",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {})
  });
  if (!res.ok) {
    const msg = await res.text().catch(() => res.statusText);
    throw new Error(`PUT ${url} failed: ${res.status} ${msg}`);
  }
  return res.json();
}

// ---------- Yard selector + profile ----------

async function loadYards() {
  setText("yardSelectorStatus", "Loading yards...");
  try {
    const data = await apiGet("/yards");
    const select = document.getElementById("selectYard");
    select.innerHTML = "";

    yardMap.clear();
    data.forEach(y => yardMap.set(y.id, y));

    if (!data.length) {
      const opt = document.createElement("option");
      opt.value = "";
      opt.textContent = "No yards yet – create one";
      select.appendChild(opt);
      currentYardId = null;
      clearYardProfileForm();
      setText("yardSelectorStatus", "No yards found. Click 'New Yard' to create.");
      return;
    }

    data.forEach(y => {
      const opt = document.createElement("option");
      opt.value = y.id;
      opt.textContent = y.name || y.yard_code || y.id;
      select.appendChild(opt);
    });

    // keep current selection if possible
    if (currentYardId && yardMap.has(currentYardId)) {
      select.value = currentYardId;
    } else {
      currentYardId = data[0].id;
      select.value = currentYardId;
    }

    setText("yardSelectorStatus", `Loaded ${data.length} yard(s).`);
    await onYardChanged();
  } catch (err) {
    console.error(err);
    setText("yardSelectorStatus", `Error loading yards: ${err.message}`);
  }
}

function clearYardProfileForm() {
  document.getElementById("yard_id").value = "";
  document.getElementById("yard_code").value = "";
  document.getElementById("yard_name").value = "";
  document.getElementById("yard_city").value = "";
  document.getElementById("yard_region").value = "";
  document.getElementById("yard_currency").value = "USD";
  document.getElementById("yard_default_hedge_ratio").value = "0.60";
}

async function onYardChanged() {
  const select = document.getElementById("selectYard");
  const id = select.value || "";
  currentYardId = id || null;

  if (!currentYardId) {
    clearYardProfileForm();
    pricingRules = [];
    hedgeRules = [];
    renderPricingRules();
    renderHedgeRules();
    renderHedgeRecs([]);
    setText("yardProfileStatus", "No yard selected.");
    return;
  }

  try {
    setText("yardProfileStatus", "Loading yard profile...");
    const yard = await apiGet(`/yards/${currentYardId}`);
    document.getElementById("yard_id").value = yard.id;
    document.getElementById("yard_code").value = yard.yard_code || "";
    document.getElementById("yard_name").value = yard.name || "";
    document.getElementById("yard_city").value = yard.city || "";
    document.getElementById("yard_region").value = yard.region || "";
    document.getElementById("yard_currency").value = yard.default_currency || "USD";
    document.getElementById("yard_default_hedge_ratio").value =
      yard.default_hedge_ratio != null ? yard.default_hedge_ratio : 0.6;
    setText("yardProfileStatus", "");

    await Promise.all([loadPricingRules(), loadHedgeRules(), loadHedgeRecs()]);
  } catch (err) {
    console.error(err);
    setText("yardProfileStatus", `Error loading yard: ${err.message}`);
  }
}

async function saveYardProfile() {
  const id = document.getElementById("yard_id").value || null;
  const body = {
    yard_code: document.getElementById("yard_code").value.trim(),
    name: document.getElementById("yard_name").value.trim(),
    city: document.getElementById("yard_city").value.trim(),
    region: document.getElementById("yard_region").value.trim(),
    default_currency: document.getElementById("yard_currency").value.trim() || "USD",
    default_hedge_ratio: parseFloat(document.getElementById("yard_default_hedge_ratio").value || "0.6")
  };

  try {
    setText("yardProfileStatus", "Saving yard profile...");

    let saved;
    if (id) {
      saved = await apiPut(`/yards/${id}`, body);
    } else {
      saved = await apiPost("/yards", body);
      currentYardId = saved.id;
      document.getElementById("yard_id").value = saved.id;
    }

    setText("yardProfileStatus", "Saved.");
    await loadYards();
  } catch (err) {
    console.error(err);
    setText("yardProfileStatus", `Error saving yard: ${err.message}`);
  }
}

// ---------- Pricing Rules ----------

async function loadPricingRules() {
  pricingRules = [];
  if (!currentYardId) {
    renderPricingRules();
    return;
  }
  setText("pricingStatus", "Loading pricing rules...");
  try {
    const data = await apiGet(`/yards/${currentYardId}/pricing_rules`);
    pricingRules = data || [];
    renderPricingRules();
    setText("pricingStatus", "");
  } catch (err) {
    console.error(err);
    setText("pricingStatus", `Error: ${err.message}`);
  }
}

function renderPricingRules() {
  const tbody = document.querySelector("#tblPricingRules tbody");
  if (!tbody) return;
  tbody.innerHTML = "";

  if (!pricingRules.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 7;
    td.className = "text-muted small";
    td.textContent = currentYardId
      ? "No pricing rules yet. Add one below."
      : "Select a yard to view pricing rules.";
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }

  pricingRules.forEach(r => {
    const tr = document.createElement("tr");

    const tdMat = document.createElement("td");
    tdMat.textContent = r.material;
    tr.appendChild(tdMat);

    const tdFormula = document.createElement("td");
    tdFormula.textContent = r.formula;
    tr.appendChild(tdFormula);

    const tdLMin = document.createElement("td");
    tdLMin.textContent = (r.loss_min_pct != null ? r.loss_min_pct : 0).toString();
    tr.appendChild(tdLMin);

    const tdLMax = document.createElement("td");
    tdLMax.textContent = (r.loss_max_pct != null ? r.loss_max_pct : 0).toString();
    tr.appendChild(tdLMax);

    const tdMinMargin = document.createElement("td");
    tdMinMargin.textContent = (r.min_margin_usd_ton != null ? r.min_margin_usd_ton : 0).toString();
    tr.appendChild(tdMinMargin);

    const tdActive = document.createElement("td");
    tdActive.textContent = r.active ? "Yes" : "No";
    tr.appendChild(tdActive);

    const tdActions = document.createElement("td");
    tdActions.className = "text-end";

    const btnEdit = document.createElement("button");
    btnEdit.type = "button";
    btnEdit.className = "btn btn-sm btn-outline-secondary me-1";
    btnEdit.textContent = "Edit";
    btnEdit.addEventListener("click", () => populatePricingForm(r));

    tdActions.appendChild(btnEdit);
    tr.appendChild(tdActions);

    tbody.appendChild(tr);
  });
}

function populatePricingForm(r) {
  document.getElementById("pricing_rule_id").value = r.id;
  document.getElementById("pricing_material").value = r.material || "";
  document.getElementById("pricing_formula").value = r.formula || "REF";
  document.getElementById("pricing_loss_min").value =
    r.loss_min_pct != null ? r.loss_min_pct : 0;
  document.getElementById("pricing_loss_max").value =
    r.loss_max_pct != null ? r.loss_max_pct : 0.08;
  document.getElementById("pricing_min_margin").value =
    r.min_margin_usd_ton != null ? r.min_margin_usd_ton : 0;
  document.getElementById("pricing_active").checked = !!r.active;
}

function clearPricingForm() {
  document.getElementById("pricing_rule_id").value = "";
  document.getElementById("pricing_material").value = "";
  document.getElementById("pricing_formula").value = "REF - 0.10";
  document.getElementById("pricing_loss_min").value = "0.00";
  document.getElementById("pricing_loss_max").value = "0.08";
  document.getElementById("pricing_min_margin").value = "0";
  document.getElementById("pricing_active").checked = true;
}

async function savePricingRule() {
  if (!currentYardId) {
    setText("pricingStatus", "Select a yard first.");
    return;
  }
  const id = document.getElementById("pricing_rule_id").value || null;
  const body = {
    material: document.getElementById("pricing_material").value.trim(),
    formula: document.getElementById("pricing_formula").value.trim() || "REF",
    loss_min_pct: parseFloat(document.getElementById("pricing_loss_min").value || "0"),
    loss_max_pct: parseFloat(document.getElementById("pricing_loss_max").value || "0"),
    min_margin_usd_ton: parseFloat(document.getElementById("pricing_min_margin").value || "0"),
    active: document.getElementById("pricing_active").checked
  };

  try {
    setText("pricingStatus", "Saving rule...");

    if (id) {
      await apiPut(`/yard_pricing_rules/${id}`, body);
    } else {
      await apiPost(`/yards/${currentYardId}/pricing_rules`, body);
    }

    clearPricingForm();
    await loadPricingRules();
    setText("pricingStatus", "Saved.");
  } catch (err) {
    console.error(err);
    setText("pricingStatus", `Error saving rule: ${err.message}`);
  }
}

// ---------- Hedge Rules ----------

async function loadHedgeRules() {
  hedgeRules = [];
  if (!currentYardId) {
    renderHedgeRules();
    return;
  }
  setText("hedgeStatus", "Loading hedge rules...");
  try {
    const data = await apiGet(`/yards/${currentYardId}/hedge_rules`);
    hedgeRules = data || [];
    renderHedgeRules();
    setText("hedgeStatus", "");
  } catch (err) {
    console.error(err);
    setText("hedgeStatus", `Error: ${err.message}`);
  }
}

function renderHedgeRules() {
  const tbody = document.querySelector("#tblHedgeRules tbody");
  if (!tbody) return;
  tbody.innerHTML = "";

  if (!hedgeRules.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 6;
    td.className = "text-muted small";
    td.textContent = currentYardId
      ? "No hedge rules yet. Add one below."
      : "Select a yard to view hedge rules.";
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }

  hedgeRules.forEach(r => {
    const tr = document.createElement("tr");

    const tdMat = document.createElement("td");
    tdMat.textContent = r.material;
    tr.appendChild(tdMat);

    const tdMinTons = document.createElement("td");
    tdMinTons.textContent = (r.min_tons != null ? r.min_tons : 0).toString();
    tr.appendChild(tdMinTons);

    const tdTarget = document.createElement("td");
    tdTarget.textContent = (r.target_hedge_ratio != null ? r.target_hedge_ratio : 0).toString();
    tr.appendChild(tdTarget);

    const tdRoot = document.createElement("td");
    tdRoot.textContent = r.futures_symbol_root || "";
    tr.appendChild(tdRoot);

    const tdAuto = document.createElement("td");
    tdAuto.textContent = r.auto_hedge ? "Yes" : "No";
    tr.appendChild(tdAuto);

    const tdActions = document.createElement("td");
    tdActions.className = "text-end";

    const btnEdit = document.createElement("button");
    btnEdit.type = "button";
    btnEdit.className = "btn btn-sm btn-outline-secondary me-1";
    btnEdit.textContent = "Edit";
    btnEdit.addEventListener("click", () => populateHedgeForm(r));

    tdActions.appendChild(btnEdit);
    tr.appendChild(tdActions);

    tbody.appendChild(tr);
  });
}

function populateHedgeForm(r) {
  document.getElementById("hedge_rule_id").value = r.id;
  document.getElementById("hedge_material").value = r.material || "";
  document.getElementById("hedge_symbol_root").value = r.futures_symbol_root || "";
  document.getElementById("hedge_min_tons").value =
    r.min_tons != null ? r.min_tons : 0;
  document.getElementById("hedge_target_ratio").value =
    r.target_hedge_ratio != null ? r.target_hedge_ratio : 0.6;
  document.getElementById("hedge_auto").checked = !!r.auto_hedge;
}

function clearHedgeForm() {
  document.getElementById("hedge_rule_id").value = "";
  document.getElementById("hedge_material").value = "";
  document.getElementById("hedge_symbol_root").value = "";
  document.getElementById("hedge_min_tons").value = "40";
  document.getElementById("hedge_target_ratio").value = "0.60";
  document.getElementById("hedge_auto").checked = false;
}

async function saveHedgeRule() {
  if (!currentYardId) {
    setText("hedgeStatus", "Select a yard first.");
    return;
  }
  const id = document.getElementById("hedge_rule_id").value || null;
  const body = {
    material: document.getElementById("hedge_material").value.trim(),
    futures_symbol_root: document.getElementById("hedge_symbol_root").value.trim(),
    min_tons: parseFloat(document.getElementById("hedge_min_tons").value || "0"),
    target_hedge_ratio: parseFloat(document.getElementById("hedge_target_ratio").value || "0"),
    auto_hedge: document.getElementById("hedge_auto").checked
  };

  try {
    setText("hedgeStatus", "Saving hedge rule...");

    if (id) {
      await apiPut(`/yard_hedge_rules/${id}`, body);
    } else {
      await apiPost(`/yards/${currentYardId}/hedge_rules`, body);
    }

    clearHedgeForm();
    await loadHedgeRules();
    setText("hedgeStatus", "Saved.");
  } catch (err) {
    console.error(err);
    setText("hedgeStatus", `Error saving hedge rule: ${err.message}`);
  }
}

// ---------- Hedge Recommendations Preview ----------

async function loadHedgeRecs() {
  if (!currentYardId) {
    renderHedgeRecs([]);
    setText("hedgeRecsStatus", "Select a yard to view recommendations.");
    return;
  }
  setText("hedgeRecsStatus", "Loading hedge recommendations...");
  try {
    const data = await apiGet(`/hedge/recommendations?yard_id=${encodeURIComponent(currentYardId)}`);
    renderHedgeRecs(data || []);
    setText("hedgeRecsStatus", data.length ? "" : "No hedge recommendations for current yard.");
  } catch (err) {
    console.error(err);
    renderHedgeRecs([]);
    setText("hedgeRecsStatus", `Error: ${err.message}`);
  }
}

function renderHedgeRecs(rows) {
  const tbody = document.querySelector("#tblHedgeRecs tbody");
  if (!tbody) return;
  tbody.innerHTML = "";

  if (!rows || !rows.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 7;
    td.className = "text-muted small";
    td.textContent = currentYardId
      ? "No hedge recommendations available."
      : "Select a yard to view hedge recommendations.";
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }

  rows.forEach(r => {
    const tr = document.createElement("tr");

    const tdMat = document.createElement("td");
    tdMat.textContent = r.material;
    tr.appendChild(tdMat);

    const tdOnHand = document.createElement("td");
    tdOnHand.textContent = r.on_hand_tons.toFixed(2);
    tr.appendChild(tdOnHand);

    const tdTarget = document.createElement("td");
    tdTarget.textContent = r.target_hedge_ratio.toFixed(2);
    tr.appendChild(tdTarget);

    const tdCur = document.createElement("td");
    tdCur.textContent = r.current_hedged_tons.toFixed(2);
    tr.appendChild(tdCur);

    const tdMore = document.createElement("td");
    tdMore.textContent = r.hedge_more_tons.toFixed(2);
    tr.appendChild(tdMore);

    const tdLots = document.createElement("td");
    tdLots.textContent = r.hedge_more_lots.toString();
    tr.appendChild(tdLots);

    const tdRoot = document.createElement("td");
    tdRoot.textContent = r.futures_symbol_root || "";
    tr.appendChild(tdRoot);

    tbody.appendChild(tr);
  });
}

// ---------- Wire up events on ready ----------

document.addEventListener("DOMContentLoaded", () => {
  const sel = document.getElementById("selectYard");
  if (sel) sel.addEventListener("change", onYardChanged);

  const btnRefreshYards = document.getElementById("btnRefreshYards");
  if (btnRefreshYards) btnRefreshYards.addEventListener("click", loadYards);

  const btnNewYard = document.getElementById("btnNewYard");
  if (btnNewYard) btnNewYard.addEventListener("click", () => {
    currentYardId = null;
    const select = document.getElementById("selectYard");
    if (select) select.value = "";
    clearYardProfileForm();
    pricingRules = [];
    hedgeRules = [];
    renderPricingRules();
    renderHedgeRules();
    renderHedgeRecs([]);
    setText("yardProfileStatus", "Creating new yard – fill form and Save.");
  });

  const btnSaveYardProfile = document.getElementById("btnSaveYardProfile");
  if (btnSaveYardProfile) btnSaveYardProfile.addEventListener("click", saveYardProfile);

  const btnNewPricingRule = document.getElementById("btnNewPricingRule");
  if (btnNewPricingRule) btnNewPricingRule.addEventListener("click", () => {
    clearPricingForm();
    setText("pricingStatus", "");
  });

  const btnSavePricingRule = document.getElementById("btnSavePricingRule");
  if (btnSavePricingRule) btnSavePricingRule.addEventListener("click", savePricingRule);

  const btnNewHedgeRule = document.getElementById("btnNewHedgeRule");
  if (btnNewHedgeRule) btnNewHedgeRule.addEventListener("click", () => {
    clearHedgeForm();
    setText("hedgeStatus", "");
  });

  const btnSaveHedgeRule = document.getElementById("btnSaveHedgeRule");
  if (btnSaveHedgeRule) btnSaveHedgeRule.addEventListener("click", saveHedgeRule);

  const btnRefreshHedgeRecs = document.getElementById("btnRefreshHedgeRecs");
  if (btnRefreshHedgeRecs) btnRefreshHedgeRecs.addEventListener("click", loadHedgeRecs);

  // initial load
  loadYards();
});
