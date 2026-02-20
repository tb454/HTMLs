// BR-INDEX SNAPSHOT + MATERIALS TABLE


// Constants and state
const MATERIALS_PAGE_SIZE = 50;
let materialsPage = 1;
let BRINDEX_ROWS = [];
let BRINDEX_LAST_FETCH_MS = 0;
const BRINDEX_TTL_MS = 25000; // refetch if older than 25s

function _bleach(s){  
  return String(s || '').replace(/(COMEX|LME|CME)/ig, 'BR-Index');
}

const SYMBOL_LABELS = {
  'BR-CU':'Base Copper',
  'BR-AL':'Base Aluminum',
  'BR-HMS':'HMS',
  'BR-PS':'Plate & Structural',
  'BR-SHRED':'Shred',
  'BR-SS304':'Stainless 304',
  'BR-SS316':'Stainless 316',
  'BR-ZN':'Base Zinc',
  'BR-NI':'Base Nickel',
  'BR-PB':'Base Lead'
};

function _arr(x){
  return Array.isArray(x) ? x : (x?.items || x?.rows || x?.data || []);
}

async function _getJSON(url){
  // Use the unified request wrapper from seller.html so auth/401/CSRF behavior is consistent
  if (typeof window.api !== 'function') {
    throw new Error('api() not loaded yet');
  }
  const r = await window.api(url, {
    method: 'GET',
    cache: 'no-store',
    headers: { 'Accept': 'application/json' }
  });
  return await r.json();
}

async function _loadUniverseDefs(){
  const data = await _getJSON('/indices/universe');
  const arr  = _arr(data);
  return arr.filter(d => d && d.symbol && (d.enabled == null || d.enabled === true));
}

async function _loadHistory(sym){
  const data = await _getJSON(`/indices/history?symbol=${encodeURIComponent(sym)}`);
  return _arr(data);
}

function _lastPrevFromHistory(rows){
  const series = (rows || [])
    .filter(r => r && (r.dt || r.as_of_date))
    .map(r => ({
      dt: (r.dt || r.as_of_date),
      v: +(r.close_price ?? r.close ?? r.value ?? 0)
    }))
    .filter(p => p.dt && Number.isFinite(p.v))
    .sort((a,b)=> new Date(a.dt) - new Date(b.dt));

  if (!series.length) return { last_lb:null, prev_lb:null, dt:null };

  const last = series[series.length - 1];
  const prev = series.length > 1 ? series[series.length - 2] : null;

  return {
    last_lb: last.v,
    prev_lb: prev ? prev.v : null,
    dt: last.dt
  };
}

// Build BRINDEX_ROWS from bridge_index_definitions + bridge_index_history
async function _ensureBRIndexRows(force=false){
  const now = Date.now();
  if (!force && BRINDEX_ROWS.length && (now - BRINDEX_LAST_FETCH_MS) < BRINDEX_TTL_MS) return;

  const defs = await _loadUniverseDefs();
  const out = [];

  // sequential (64 symbols) to avoid rate-limiting issues
  for (const d of defs){
    const sym = d.symbol;
    let hist = [];
    try { hist = await _loadHistory(sym); } catch { hist = []; }

    const { last_lb, prev_lb, dt } = _lastPrevFromHistory(hist);
    if (last_lb == null) continue;

    const last_ton  = Number(last_lb) * 2000.0;
    const prev_ton  = (prev_lb == null ? null : Number(prev_lb) * 2000.0);
    const delta_ton = (prev_ton == null ? null : (last_ton - prev_ton));

    out.push({
      symbol: sym,

      name: _bleach(d.notes || d.name || SYMBOL_LABELS[sym] || ''),

      category: '',

      last_ton,
      delta_ton,
      dt
    });
  }

  BRINDEX_ROWS = out;
  BRINDEX_LAST_FETCH_MS = now;
}

// ----------------------------------------
// TOP 10 MARKET SNAPSHOT (USD/ton)
// ----------------------------------------
async function loadMarketSnapshot() {
  const tbody = document.getElementById('market-snapshot-body');
  if (!tbody) return;

  try {
    await _ensureBRIndexRows(false);

    const top10 = [...BRINDEX_ROWS]
      .filter(r => Number.isFinite(r.last_ton))
      .sort((a,b) => b.last_ton - a.last_ton)
      .slice(0, 10);

    if (!top10.length){
      tbody.innerHTML = `<tr><td colspan="3" class="text-muted text-center py-2">No BR-Index rows yet.</td></tr>`;
      return;
    }

    tbody.innerHTML = top10.map(r => `
      <tr>
        <td>${r.symbol}</td>
        <td>${_bleach(r.name) || '—'}</td>
        <td class="text-end">${r.last_ton.toFixed(2)}</td>
      </tr>
    `).join('');

  } catch (err) {
    console.error('Error loading BR-Index snapshot:', err);
    tbody.innerHTML = `<tr><td colspan="3" class="text-muted text-center py-2">Snapshot unavailable.</td></tr>`;
  }
}

// ----------------------------------------
// FULL MATERIALS BENCHMARK TABLE
// ----------------------------------------
async function loadMaterialBenchmarks(page = 1) {
  const table = document.getElementById('metals-market-table');
  const tbody = table?.querySelector('tbody');
  if (!tbody) return;

  try {
    await _ensureBRIndexRows(false);

    // sort stable (optional)
    const rows = [...BRINDEX_ROWS].sort((a,b) => String(a.symbol).localeCompare(String(b.symbol)));

    if (!rows.length) {
      tbody.innerHTML = `<tr><td colspan="9" class="text-muted text-center py-2">No BR-Index rows yet.</td></tr>`;
      return;
    }

    const fmt = (n, dp=2) =>
      Number(n || 0).toLocaleString(undefined, { minimumFractionDigits: dp, maximumFractionDigits: dp });

    tbody.innerHTML = rows.map(r => {
      const last = Number.isFinite(r.last_ton) ? r.last_ton : null;
      const chg  = (r.delta_ton == null || !Number.isFinite(r.delta_ton)) ? null : r.delta_ton;

      return `
        <tr>
          <td>${_bleach(r.symbol)}</td>               
          <td>${last == null ? '—' : fmt(last, 2)}</td>
          <td>${chg == null ? '—' : (chg >= 0 ? '+' : '') + fmt(chg, 2)}</td>
          <td>—</td>
          <td>—</td>
          <td>—</td>
          <td>—</td>
          <td>${_bleach(r.dt || '—')}</td>
        </tr>
      `;
    }).join('');

    // If the old 5-col table still exists in HTML, blank it so it can’t leak anything.
    const oldBody = document.getElementById('materials-table-body');
    if (oldBody) oldBody.innerHTML = '';
    const oldLabel = document.getElementById('materials-page-label');
    if (oldLabel) oldLabel.textContent = '';

  } catch (err) {
    console.error('Error loading BR-Index overview:', err);
    tbody.innerHTML = `<tr><td colspan="9" class="text-muted text-center py-2">BR-Index unavailable.</td></tr>`;
  }
}

// ----------------------------------------
// PAGING BUTTONS 
// ----------------------------------------
function initMaterialBenchmarksPaging() {
  const prevBtn = document.getElementById('materials-prev');
  const nextBtn = document.getElementById('materials-next');

  if (prevBtn) {
    prevBtn.onclick = () => loadMaterialBenchmarks(materialsPage - 1);
  }
  if (nextBtn) {
    nextBtn.onclick = () => loadMaterialBenchmarks(materialsPage + 1);
  }
}

// Force-refresh helper
async function refreshBRIndex(force = true) {
  try {
    await _ensureBRIndexRows(!!force);
  } catch {}
  try { await loadMarketSnapshot(); } catch {}
  try { await loadMaterialBenchmarks(materialsPage); } catch {}
}

// seller.html needs these:
window.loadMarketSnapshot = loadMarketSnapshot;
window.loadMaterialBenchmarks = loadMaterialBenchmarks;
window.initMaterialBenchmarksPaging = initMaterialBenchmarksPaging;
window.refreshBRIndex = refreshBRIndex;
