// ========================================
// BR-INDEX SNAPSHOT + MATERIALS TABLE
// ========================================

// one clean definition — no duplicates
const MATERIALS_PAGE_SIZE = 50;
let materialsPage = 1;
let BRINDEX_ROWS = [];

// ----------------------------------------
// TOP 10 MARKET SNAPSHOT (USD/ton)
// ----------------------------------------
async function loadMarketSnapshot() {
  const tbody = document.getElementById('market-snapshot-body');
  if (!tbody) return;

  try {
    const resp = await fetch('/api/br-index/current', { credentials: 'include' });
    const rows = await resp.json();

    const top10 = rows
      .map(r => ({
        symbol: r.instrument_code,
        name: r.material_canonical,
        last_ton: Number(r.px_avg) * 2000
      }))
      .sort((a, b) => b.last_ton - a.last_ton)
      .slice(0, 10);

    tbody.innerHTML = top10.map(r => `
      <tr>
        <td>${r.symbol}</td>
        <td>${r.name}</td>
        <td class="text-end">${r.last_ton.toFixed(2)}</td>
      </tr>
    `).join('');

  } catch (err) {
    console.error('Error loading BR-Index snapshot:', err);
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
    // fetch once
    if (BRINDEX_ROWS.length === 0) {
      const resp = await fetch('/api/br-index/current', { credentials: 'include' });
      const data = await resp.json();

      BRINDEX_ROWS = data.map(r => ({
        symbol: r.instrument_code,
        name: r.material_canonical,
        category: r.core_code,
        last_ton: Number(r.px_avg) * 2000,
        last_lb: Number(r.px_avg)
      }));
    }

    const total = BRINDEX_ROWS.length;
    const maxPage = Math.max(1, Math.ceil(total / MATERIALS_PAGE_SIZE));

    materialsPage = Math.min(Math.max(page, 1), maxPage);

    const start = (materialsPage - 1) * MATERIALS_PAGE_SIZE;
    const end = Math.min(start + MATERIALS_PAGE_SIZE, total);
    const rows = BRINDEX_ROWS.slice(start, end);

    tbody.innerHTML = rows.map(r => `
      <tr>
        <td>${r.symbol}</td>
        <td>${r.name}</td>
        <td>${r.category}</td>
        <td class="text-end">${r.last_ton.toFixed(2)}</td>
        <td class="text-end text-muted">${r.last_lb.toFixed(4)} / lb</td>
      </tr>
    `).join('');

    if (label) {
      label.textContent = `Showing ${start + 1}–${end} of ${total} materials`;
    }

    const prevBtn = document.getElementById('materials-prev');
    const nextBtn = document.getElementById('materials-next');

    if (prevBtn) prevBtn.disabled = (materialsPage <= 1);
    if (nextBtn) nextBtn.disabled = (materialsPage >= maxPage);

  } catch (err) {
    console.error('Error loading BR-Index materials:', err);
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
