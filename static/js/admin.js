// /static/js/admin.js 

// ---- API base (single source of truth)
window.endpoint = (function () {
  const isLocal = location.hostname === 'localhost' || location.hostname === '127.0.0.1';  
  const localPort = 8000; // <--- change if yours differs
  return isLocal ? `http://localhost:${localPort}` : location.origin;
})();

(function () {
  const $ = sel => document.querySelector(sel);
  const $$ = sel => Array.from(document.querySelectorAll(sel));

  // Utility: simple JSON fetch with credentials
  async function jget(url) {
    const full = url.startsWith('http') ? url : (window.endpoint + url);
    const r = await fetch(full, { credentials: 'include' });
    if (!r.ok) throw new Error(full + ' -> ' + r.status);
    return r.json();
  }

  // ----------------------------
  // Tabs (if you use Bootstrap tabs or custom nav)
  // ----------------------------
  function wireTabs() {
    const links = $$('.nav-link[data-target]');
    links.forEach(a => {
      a.addEventListener('click', (e) => {
        e.preventDefault();
        const target = a.getAttribute('data-target');
        $$('.nav-link').forEach(x => x.classList.remove('active'));
        a.classList.add('active');
        $$('.tab-pane').forEach(p => p.classList.add('d-none'));
        $(target)?.classList.remove('d-none');
      });
    });
    // default: first tab active
    if (links.length) links[0].click();
  }

  // ----------------------------
  // Contracts table
  // ----------------------------
  let contractsPage = 1;
  const PAGE_SIZE = 25;

  async function loadContracts() {
    const tbody = $('#contracts-tbody');
    if (!tbody) return;

    const params = new URLSearchParams({ page: String(contractsPage), page_size: String(PAGE_SIZE) });
    const rows = await jget('/contracts?' + params.toString());

    tbody.innerHTML = rows.items.map(r => `
      <tr>
        <td>${r.id}</td>
        <td>${r.material || '-'}</td>
        <td>${r.qty_tons ?? '-'}</td>
        <td>${r.price_per_ton ?? '-'}</td>
        <td>${r.status || '-'}</td>
        <td>${r.created_at || '-'}</td>
      </tr>
    `).join('') || `<tr><td colspan="6" class="text-muted">No contracts found.</td></tr>`;

    renderPager('#contracts-pager', rows.page, rows.total_pages, (p)=>{ contractsPage=p; loadContracts(); });
  }

  // ----------------------------
  // BOLs table
  // ----------------------------
  let bolsPage = 1;

  async function loadBOLs() {
    const tbody = $('#bols-tbody');
    if (!tbody) return;

    const params = new URLSearchParams({ page: String(bolsPage), page_size: String(PAGE_SIZE) });
    const rows = await jget('/bols?' + params.toString());

    tbody.innerHTML = rows.items.map(r => `
      <tr>
        <td>${r.id}</td>
        <td>${r.contract_id ?? '-'}</td>
        <td>${r.carrier || '-'}</td>
        <td>${r.status || '-'}</td>
        <td>
          <a class="btn btn-sm btn-outline-secondary" href="${window.endpoint}/bol/${r.id}/pdf" target="_blank">PDF</a>
        </td>
      </tr>
    `).join('') || `<tr><td colspan="5" class="text-muted">No BOLs found.</td></tr>`;

    renderPager('#bols-pager', rows.page, rows.total_pages, (p)=>{ bolsPage=p; loadBOLs(); });
  }

  // ----------------------------
  // Pager renderer
  // ----------------------------
  function renderPager(containerSel, page, total, onGo) {
    const el = $(containerSel);
    if (!el) return;
    const prevDisabled = page <= 1 ? 'disabled' : '';
    const nextDisabled = page >= total ? 'disabled' : '';
    el.innerHTML = `
      <div class="d-flex gap-2 align-items-center">
        <button class="btn btn-sm btn-outline-secondary" ${prevDisabled} data-go="${page-1}">Prev</button>
        <span class="small">Page ${page} / ${total || 1}</span>
        <button class="btn btn-sm btn-outline-secondary" ${nextDisabled} data-go="${page+1}">Next</button>
      </div>`;
    el.querySelectorAll('button[data-go]').forEach(b=>{
      b.addEventListener('click', ()=>{
        const p = Number(b.getAttribute('data-go'));
        if (p>=1 && p<=Math.max(1,total)) onGo(p);
      });
    });
  }

  // ----------------------------
  // Chart (optional, safe-load)
  // ----------------------------
  async function loadAdminChart() {
    const canvas = $('#contracts-chart');
    if (!canvas || typeof Chart === 'undefined') return; // Chart.js blocked? skip silently.
    const data = await jget('/analytics/contracts_by_day');
    const labels = data.map(d => d.day);
    const values = data.map(d => d.count);

    const ctx = canvas.getContext('2d');
    new Chart(ctx, {
      type: 'line',
      data: { labels, datasets: [{ label: 'Contracts/day', data: values }] },
      options: { responsive: true, animation: false }
    });
  }

  // ----------------------------
  // INIT
  // ----------------------------
  async function init() {
    try {
      wireTabs();
      await Promise.all([ loadContracts(), loadBOLs() ]);
      // await loadAdminChart(); // disabled (draw bolsByDay later on page)
    } catch (e) {
      console.error(e);
    }
  }

  // Defer guarantees DOMContentLoaded already fired; still guard if bundled elsewhere
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
