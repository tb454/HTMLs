// ========================================
// BR-INDEX SNAPSHOT + MATERIALS TABLE
// ========================================

// Constants and state
const MATERIALS_PAGE_SIZE = 50;
let materialsPage = 1;
let BRINDEX_ROWS = [];
let BRINDEX_LAST_FETCH_MS = 0;
const BRINDEX_TTL_MS = 25000; // refetch if older than 25s

function _arr(x){
  return Array.isArray(x) ? x : (x?.items || x?.rows || x?.data || []);
}

async function _getJSON(url){
  const r = await fetch(url, {
    credentials: 'include',
    cache: 'no-store',
    headers: { 'Accept': 'application/json' }
  });
  if (!r.ok) throw new Error(`HTTP ${r.status} for ${url}`);
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
      // Use notes if present; otherwise show base_symbol/method as “material-ish”
      name: (d.notes || d.name || d.base_symbol || sym),
      category: (d.base_symbol || d.method || ''),
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
        <td>${r.name || '—'}</td>
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
  const tbody = document.getElementById('materials-table-body');
  const label = document.getElementById('materials-page-label');
  if (!tbody) return;

  try {
    await _ensureBRIndexRows(false);

    const total = BRINDEX_ROWS.length;
    const maxPage = Math.max(1, Math.ceil(total / MATERIALS_PAGE_SIZE));
    materialsPage = Math.min(Math.max(page, 1), maxPage);

    const start = (materialsPage - 1) * MATERIALS_PAGE_SIZE;
    const end   = Math.min(start + MATERIALS_PAGE_SIZE, total);
    const rows  = BRINDEX_ROWS.slice(start, end);

    if (!rows.length){
      tbody.innerHTML = `<tr><td colspan="5" class="text-muted text-center py-2">No BR-Index rows yet.</td></tr>`;
      if (label) label.textContent = '';
      return;
    }

    tbody.innerHTML = rows.map(r => {
      const chg = r.delta_ton;
      const chgStr = (chg == null || !Number.isFinite(chg)) ? '—' : (chg >= 0 ? '+' : '') + chg.toFixed(2);
      return `
        <tr>
          <td>${r.symbol}</td>
          <td>${r.name || '—'}</td>
          <td>${r.category || '—'}</td>
          <td class="text-end">${Number(r.last_ton).toFixed(2)}</td>
          <td class="text-end">${chgStr}</td>
        </tr>
      `;
    }).join('');

    if (label) label.textContent = `Showing ${start + 1}–${end} of ${total} materials`;

    const prevBtn = document.getElementById('materials-prev');
    const nextBtn = document.getElementById('materials-next');
    if (prevBtn) prevBtn.disabled = (materialsPage <= 1);
    if (nextBtn) nextBtn.disabled = (materialsPage >= maxPage);

  } catch (err) {
    console.error('Error loading BR-Index materials:', err);
    tbody.innerHTML = `<tr><td colspan="5" class="text-muted text-center py-2">BR-Index unavailable.</td></tr>`;
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

// Force-refresh helper (called by seller.html)
async function refreshBRIndex(force = true) {
  try {
    await _ensureBRIndexRows(!!force);
  } catch {}
  try { await loadMarketSnapshot(); } catch {}
  try { await loadMaterialBenchmarks(materialsPage); } catch {}
}

window.refreshBRIndex = refreshBRIndex;
