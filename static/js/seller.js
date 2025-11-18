const MATERIALS_PAGE_SIZE = 50;
let materialsPage = 1;
let materialsTotal = 0;

// Top-10 Market Snapshot card
async function loadMarketSnapshot() {
  const tbody = document.getElementById('market-snapshot-body');
  if (!tbody) return;

  try {
    // top 10 from BR-Index
    const resp = await fetch('/benchmarks/materials?page=1&page_size=10', {
      credentials: 'include'
    });
    if (!resp.ok) {
      console.error('Failed to load market snapshot', resp.status);
      return;
    }

    const payload = await resp.json();
    const rows = Array.isArray(payload) ? payload : (payload.items || []);

    tbody.innerHTML = '';

    rows.forEach(row => {
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${row.symbol}</td>
        <td>${row.name}</td>
        <td class="text-end">${Number(row.last).toFixed(2)}</td>
      `;
      tbody.appendChild(tr);
    });
  } catch (err) {
    console.error('Error fetching market snapshot', err);
  }
}

// BR-Index table (50 at a time)
async function loadMaterialBenchmarks(page = 1) {
  const tbody = document.getElementById('materials-table-body');
  const label = document.getElementById('materials-page-label');
  const prevBtn = document.getElementById('materials-prev');
  const nextBtn = document.getElementById('materials-next');
  if (!tbody) return;

  try {
    const resp = await fetch(`/benchmarks/materials?page=${page}&page_size=${MATERIALS_PAGE_SIZE}`, {
      credentials: 'include'
    });
    if (!resp.ok) {
      console.error('Failed to load material benchmarks', resp.status);
      return;
    }

    const payload = await resp.json();
    const rows = Array.isArray(payload) ? payload : (payload.items || []);

    materialsPage = Array.isArray(payload) ? page : (payload.page || page);
    materialsTotal = Array.isArray(payload) ? rows.length : (payload.total || rows.length);

    tbody.innerHTML = '';

    rows.forEach(row => {
      const tr = document.createElement('tr');

      let changeText = '—';
      if (row.change !== null && row.change !== undefined) {
        const val = Number(row.change);
        const sign = val > 0 ? '+' : (val < 0 ? '−' : '');
        changeText = `${sign}${Math.abs(val).toFixed(2)}`;
      }

      tr.innerHTML = `
        <td>${row.symbol}</td>
        <td>${row.name}</td>
        <td>${row.category}</td>
        <td class="text-end">${Number(row.last).toFixed(2)}</td>
        <td class="text-end">${changeText}</td>
      `;
      tbody.appendChild(tr);
    });

    if (label) {
      if (!materialsTotal) {
        label.textContent = 'No materials found';
      } else {
        const from = (materialsPage - 1) * MATERIALS_PAGE_SIZE + 1;
        const to = Math.min(materialsPage * MATERIALS_PAGE_SIZE, materialsTotal);
        label.textContent = `Showing ${from}–${to} of ${materialsTotal} materials`;
      }
    }

    const lastPage = Math.max(1, Math.ceil((materialsTotal || 1) / MATERIALS_PAGE_SIZE));
    if (prevBtn) prevBtn.disabled = materialsPage <= 1;
    if (nextBtn) nextBtn.disabled = materialsPage >= lastPage;

  } catch (err) {
    console.error('Error fetching material benchmarks', err);
  }
}

function initMaterialBenchmarksPaging() {
  const prevBtn = document.getElementById('materials-prev');
  const nextBtn = document.getElementById('materials-next');

  if (prevBtn) {
    prevBtn.addEventListener('click', () => {
      if (materialsPage > 1) {
        loadMaterialBenchmarks(materialsPage - 1);
      }
    });
  }

  if (nextBtn) {
    nextBtn.addEventListener('click', () => {
      const lastPage = Math.max(1, Math.ceil((materialsTotal || 1) / MATERIALS_PAGE_SIZE));
      if (materialsPage < lastPage) {
        loadMaterialBenchmarks(materialsPage + 1);
      }
    });
  }
}
