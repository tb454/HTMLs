// ----------------------
// onReady + loader + XSRF warmup
// ----------------------
function onReady(fn){
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', fn);
  } else {
    try { fn(); } catch (e) { console.error(e); }
  }
}

// Warm cookies / XSRF
onReady(async () => {
  try { await fetch('/health', { credentials: 'include' }); } catch {}
});

// Loader clearing
window.hideLoading = function(){
  try { document.documentElement.classList.remove('loading'); } catch {}
  try { document.body.classList.add('loaded'); } catch {}
};
setTimeout(() => {
  try { window.hideLoading && window.hideLoading(); } catch {}
}, 2500);
window.addEventListener('DOMContentLoaded', () => {
  try { window.hideLoading && window.hideLoading(); } catch {}
});

// Endpoint bootstrap
window.endpoint = window.endpoint || (
  location.origin.includes('localhost')
    ? 'http://localhost:8000'
    : location.origin
);

// ----------------------
// Shared helpers (same pattern as other pages)
// ----------------------
function _cookie(name){
  const m = document.cookie.match(
    new RegExp('(?:^|; )' + name.replace(/([.*+?^${}()|[\]\\])/g, '\\$1') + '=([^;]*)')
  );
  return m ? decodeURIComponent(m[1]) : '';
}

function toast(msg, ms = 2800){
  const t = document.getElementById('toast');
  if (!t){
    alert(typeof msg === 'string' ? msg : (msg?.message || 'Error'));
    return;
  }
  const text = typeof msg === 'string' ? msg : (msg?.message || 'Error');
  t.textContent = text;
  t.classList.add('show');
  try { t.focus(); } catch {}
  setTimeout(() => t.classList.remove('show'), ms);
}

async function api(path, opts = {}){
  const url = path.startsWith('http') ? path : `${window.endpoint}${path}`;
  const method = (opts.method || 'GET').toUpperCase();
  const headers = { Accept: 'application/json', ...(opts.headers || {}) };
  if (!/^(GET|HEAD|OPTIONS)$/.test(method)) {
    headers['X-CSRF'] = _cookie('XSRF-TOKEN') || headers['X-CSRF'];
  }
  const res = await fetch(url, { ...opts, credentials: 'include', headers });
  if (res.status === 401){
    const next = encodeURIComponent(location.pathname + location.search);
    location.href = `/static/bridge-login.html?next=${next}`;
    throw new Error('Unauthorized');
  }
  if (!res.ok){
    let m = 'Request failed';
    try { m = (await res.clone().json()).detail || m; }
    catch { try { m = await res.clone().text() || m; } catch {} }
    toast(m);
    throw new Error(m);
  }
  return res;
}

// Health badge
onReady(async () => {
  const el = document.querySelector('[data-health-badge]');
  if (!el) return;
  try {
    let ok = false;
    try { ok = (await api('/health')).ok; } catch {}
    if (!ok) { try { ok = (await api('/healthz')).ok; } catch {} }
    if (ok){
      el.textContent = '● Live';
      el.dataset.ok = '1';
    } else {
      el.textContent = '● Degraded';
      el.dataset.ok = '0';
    }
  }catch{
    el.textContent = '● Degraded';
    el.dataset.ok = '0';
  }
});

// ----------------------
// Page-specific logic: Compliance Members / Alerts / Cases
// ----------------------
(function(){
  const $ = id => document.getElementById(id);
  let currentCaseId = null;
  let caseDataMap = {};   // case_id -> case object

  // ---- Compliance Members ----
  async function loadMembers(){
    const tb = $('#compTbl')?.querySelector('tbody');
    if (!tb) return;
    tb.innerHTML = `<tr><td colspan="5" class="text-muted text-center py-2">Loading…</td></tr>`;
    try{
      const res = await api('/compliance/members');
      const rows = await res.json();
      if (!rows.length){
        tb.innerHTML = `<tr><td colspan="5" class="text-muted text-center py-2">No members yet.</td></tr>`;
        return;
      }
      const filterVal = ($('#filterUser')?.value || '').trim().toLowerCase();
      const filtered = filterVal
        ? rows.filter(r => (r.username || '').toLowerCase().includes(filterVal))
        : rows;
      if (!filtered.length){
        tb.innerHTML = `<tr><td colspan="5" class="text-muted text-center py-2">No matches for filter.</td></tr>`;
        return;
      }
      tb.innerHTML = filtered.map(r => `
        <tr data-username="${r.username}">
          <td>${r.username}</td>
          <td>${r.kyc_passed ? '✅' : '❌'}</td>
          <td>${r.aml_passed ? '✅' : '❌'}</td>
          <td>${r.bsa_risk}</td>
          <td>${r.sanctions_screened ? '✅' : '❌'}</td>
        </tr>
      `).join('');

      tb.querySelectorAll('tr[data-username]').forEach(tr => {
        tr.addEventListener('click', () => {
          openMemberModal(tr.getAttribute('data-username'));
        });
      });
    }catch(e){
      tb.innerHTML = `<tr><td colspan="5" class="text-muted text-center py-2">Failed: ${e.message || 'error'}</td></tr>`;
    }
  }

  function openMemberModal(username){
    const id = 'memberCompModal';
    let modalEl = document.getElementById(id);
    if (!modalEl){
      modalEl = document.createElement('div');
      modalEl.id = id;
      modalEl.className = 'modal fade';
      modalEl.tabIndex = -1;
      modalEl.innerHTML = `
        <div class="modal-dialog">
          <div class="modal-content">
            <div class="modal-header">
              <h5 class="modal-title">Compliance Flags</h5>
              <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
            </div>
            <div class="modal-body">
              <div class="mb-2 small text-muted" id="memberCompUser"></div>
              <div class="form-check">
                <input class="form-check-input" type="checkbox" id="mcKyc">
                <label class="form-check-label" for="mcKyc">KYC Passed</label>
              </div>
              <div class="form-check">
                <input class="form-check-input" type="checkbox" id="mcAml">
                <label class="form-check-label" for="mcAml">AML Passed</label>
              </div>
              <div class="form-check">
                <input class="form-check-input" type="checkbox" id="mcSan">
                <label class="form-check-label" for="mcSan">Sanctions Screened</label>
              </div>
              <div class="form-check">
                <input class="form-check-input" type="checkbox" id="mcBoi">
                <label class="form-check-label" for="mcBoi">BOI Collected</label>
              </div>
              <div class="mt-2">
                <label class="form-label" for="mcRisk">BSA Risk</label>
                <select id="mcRisk" class="form-select form-select-sm">
                  <option value="low">low</option>
                  <option value="medium">medium</option>
                  <option value="high">high</option>
                </select>
              </div>
            </div>
            <div class="modal-footer">
              <button type="button" class="btn btn-secondary btn-sm" data-bs-dismiss="modal">Cancel</button>
              <button type="button" class="btn btn-primary btn-sm" id="mcSave">Save</button>
            </div>
          </div>
        </div>
      `;
      document.body.appendChild(modalEl);
    }

    const userLabel = modalEl.querySelector('#memberCompUser');
    if (userLabel) userLabel.textContent = `User: ${username}`;

    api('/compliance/members')
      .then(res => res.json())
      .then(rows => {
        const row = rows.find(r => r.username === username);
        if (!row) return;
        modalEl.querySelector('#mcKyc').checked = !!row.kyc_passed;
        modalEl.querySelector('#mcAml').checked = !!row.aml_passed;
        modalEl.querySelector('#mcSan').checked = !!row.sanctions_screened;
        modalEl.querySelector('#mcBoi').checked = !!row.boi_collected;
        modalEl.querySelector('#mcRisk').value = row.bsa_risk || 'low';
      }).catch(()=>{});

    modalEl.querySelector('#mcSave').onclick = async () => {
      try{
        const body = {
          kyc_passed: modalEl.querySelector('#mcKyc').checked,
          aml_passed: modalEl.querySelector('#mcAml').checked,
          sanctions_screened: modalEl.querySelector('#mcSan').checked,
          boi_collected: modalEl.querySelector('#mcBoi').checked,
          bsa_risk: modalEl.querySelector('#mcRisk').value,
        };
        await api(`/compliance/members/${encodeURIComponent(username)}`, {
          method: 'PATCH',
          headers: { 'Content-Type':'application/json' },
          body: JSON.stringify(body),
        });
        toast('Compliance flags updated.');
        bootstrap.Modal.getInstance(modalEl).hide();
        loadMembers().catch(()=>{});
      }catch(e){
        // toast handled by api()
      }
    };

    const modal = new bootstrap.Modal(modalEl);
    modal.show();
  }

  // ---- Alerts ----
  async function loadAlerts(){
    const tb = $('#alertTbl')?.querySelector('tbody');
    if (!tb) return;
    tb.innerHTML = `<tr><td colspan="4" class="text-muted text-center py-2">Loading…</td></tr>`;
    try{
      const res = await api('/surveil/alerts?limit=50');
      const rows = await res.json();
      const severityFilter = ($('#alertSeverityFilter')?.value || '').toLowerCase();
      const filtered = severityFilter
        ? rows.filter(r => (r.severity || '').toLowerCase() === severityFilter)
        : rows;
      if (!filtered.length){
        tb.innerHTML = `<tr><td colspan="4" class="text-muted text-center py-2">No alerts.</td></tr>`;
        return;
      }
      tb.innerHTML = filtered.map(r => `
        <tr>
          <td>${r.rule}</td>
          <td>${r.subject}</td>
          <td>${r.severity}</td>
          <td>${r.created_at}</td>
        </tr>
      `).join('');
    }catch(e){
      tb.innerHTML = `<tr><td colspan="4" class="text-muted text-center py-2">Failed: ${e.message || 'error'}</td></tr>`;
    }
  }

  // ---- Cases ----
  async function loadCases(){
    const tb = $('#caseTbl')?.querySelector('tbody');
    if (!tb) return;
    tb.innerHTML = `<tr><td colspan="5" class="text-muted text-center py-2">Loading…</td></tr>`;
    try{
      const statusFilter = $('#caseStatusFilter')?.value || '';
      const q = statusFilter ? `?status=${encodeURIComponent(statusFilter)}` : '';
      const res = await api(`/surveil/cases${q}`);
      const rows = await res.json();
      caseDataMap = {};
      if (!rows.length){
        tb.innerHTML = `<tr><td colspan="5" class="text-muted text-center py-2">No cases.</td></tr>`;
        return;
      }
      tb.innerHTML = rows.map(r => {
        caseDataMap[r.case_id] = r;
        return `
          <tr data-case="${r.case_id}">
            <td>${r.case_id}</td>
            <td>${r.rule}</td>
            <td>${r.subject}</td>
            <td>${r.status}</td>
            <td>${r.opened_at}</td>
          </tr>
        `;
      }).join('');

      tb.querySelectorAll('tr[data-case]').forEach(tr => {
        tr.addEventListener('click', () => {
          const id = tr.getAttribute('data-case');
          openCase(id);
        });
      });
    }catch(e){
      tb.innerHTML = `<tr><td colspan="5" class="text-muted text-center py-2">Failed: ${e.message || 'error'}</td></tr>`;
    }
  }

  function openCase(caseId){
    currentCaseId = caseId;
    const row = caseDataMap[caseId];
    const headerEl = $('#caseDetailHeader');
    const statusEl = $('#caseStatus');
    const notesEl  = $('#caseNotes');
    if (!row || !headerEl || !statusEl || !notesEl) return;
    headerEl.textContent = `Case ${caseId} — ${row.rule} / ${row.subject}`;
    statusEl.value = row.status || 'open';
    notesEl.value  = row.notes || '';
  }

  async function saveCase(){
    if (!currentCaseId) return toast('Select a case first');
    try{
      const body = {
        status: $('#caseStatus').value || 'open',
        notes: $('#caseNotes').value || null,
      };
      await api(`/surveil/cases/${encodeURIComponent(currentCaseId)}`, {
        method: 'PATCH',
        headers: { 'Content-Type':'application/json' },
        body: JSON.stringify(body),
      });
      toast('Case updated');
      await loadCases();
      openCase(currentCaseId);
    }catch(e){
      // toast handled by api()
    }
  }

  onReady(() => {
    // Members
    $('#reloadMembers')?.addEventListener('click', () => loadMembers().catch(()=>{}));
    $('#filterUser')?.addEventListener('input', () => loadMembers().catch(()=>{}));
    loadMembers().catch(()=>{});

    // Alerts
    $('#reloadAlerts')?.addEventListener('click', () => loadAlerts().catch(()=>{}));
    $('#alertSeverityFilter')?.addEventListener('change', () => loadAlerts().catch(()=>{}));
    loadAlerts().catch(()=>{});

    // Cases
    $('#reloadCases')?.addEventListener('click', () => loadCases().catch(()=>{}));
    $('#caseStatusFilter')?.addEventListener('change', () => loadCases().catch(()=>{}));
    $('#caseSave')?.addEventListener('click', () => saveCase().catch(()=>{}));
    loadCases().catch(()=>{});
  });
})();
