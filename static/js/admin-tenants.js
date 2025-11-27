// /static/js/admin-tenants.js
(function () {
  // ------------------------
  // Basic DOM helpers
  // ------------------------
  function onReady(fn) {
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', fn);
    } else {
      try {
        fn();
      } catch (e) {
        console.error(e);
      }
    }
  }

  // Warm cookies / XSRF
  onReady(async () => {
    try {
      await fetch('/health', { credentials: 'include' });
    } catch { /* ignore */ }
  });

  // Loader clearing
  window.hideLoading = function () {
    try { document.documentElement.classList.remove('loading'); } catch { /* ignore */ }
    try { document.body.classList.add('loaded'); } catch { /* ignore */ }
  };
  setTimeout(() => {
    try { window.hideLoading && window.hideLoading(); } catch { /* ignore */ }
  }, 2500);
  window.addEventListener('DOMContentLoaded', () => {
    try { window.hideLoading && window.hideLoading(); } catch { /* ignore */ }
  });

  // Endpoint bootstrap
  const endpoint = window.endpoint || (location.origin.includes('localhost')
    ? 'http://localhost:8000'
    : location.origin);
  window.endpoint = endpoint;

  const $ = (id) => document.getElementById(id);
  const fmtInt = (n) => Number(n || 0).toLocaleString();

  let orgList = [];
  let currentOrg = '';

  // ------------------------
  // Cookie / toast / API
  // ------------------------
  function _cookie(name) {
    const m = document.cookie.match(
      new RegExp('(?:^|; )' + name.replace(/([.*+?^${}()|[\]\\])/g, '\\$1') + '=([^;]*)')
    );
    return m ? decodeURIComponent(m[1]) : '';
  }

  function toast(msg, ms = 2800) {
    const t = document.getElementById('toast');
    if (!t) {
      alert(typeof msg === 'string' ? msg : (msg?.message || 'Error'));
      return;
    }
    const text = typeof msg === 'string' ? msg : (msg?.message || 'Error');
    t.textContent = text;
    t.classList.add('show');
    try { t.focus(); } catch { /* ignore */ }
    setTimeout(() => t.classList.remove('show'), ms);
  }

  async function api(path, opts = {}) {
    const url = path.startsWith('http') ? path : `${endpoint}${path}`;
    const method = (opts.method || 'GET').toUpperCase();
    const headers = { Accept: 'application/json', ...(opts.headers || {}) };
    if (!/^(GET|HEAD|OPTIONS)$/.test(method)) {
      headers['X-CSRF'] = _cookie('XSRF-TOKEN') || headers['X-CSRF'];
    }
    const res = await fetch(url, { ...opts, credentials: 'include', headers });
    if (res.status === 401) {
      const next = encodeURIComponent(location.pathname + location.search);
      location.href = `/static/bridge-login.html?next=${next}`;
      throw new Error('Unauthorized');
    }
    if (!res.ok) {
      let m = 'Request failed';
      try {
        m = (await res.clone().json()).detail || m;
      } catch {
        try {
          m = (await res.clone().text()) || m;
        } catch {
          /* ignore */
        }
      }
      toast(m);
      throw new Error(m);
    }
    return res;
  }

  // ------------------------
  // Health badge
  // ------------------------
  onReady(async () => {
    const el = document.querySelector('[data-health-badge]');
    if (!el) return;
    try {
      let ok = false;
      try { ok = (await api('/health')).ok; } catch { /* ignore */ }
      if (!ok) {
        try { ok = (await api('/healthz')).ok; } catch { /* ignore */ }
      }
      if (ok) {
        el.textContent = '● Live';
        el.dataset.ok = '1';
      } else {
        el.textContent = '● Degraded';
        el.dataset.ok = '0';
      }
    } catch {
      el.textContent = '● Degraded';
      el.dataset.ok = '0';
    }
  });

  // ------------------------
  // Applications
  // ------------------------
  async function loadApplications() {
    const tb = $('#appTbl')?.querySelector('tbody');
    if (!tb) return;
    tb.innerHTML = `<tr><td colspan="6" class="text-muted text-center py-2">Loading…</td></tr>`;
    try {
      const reviewedFilter = $('#appReviewedFilter')?.value;
      let url = '/onboarding/applications';
      if (reviewedFilter === 'true') url += '?reviewed=true';
      if (reviewedFilter === 'false') url += '?reviewed=false';

      const res = await api(url);
      const rows = await res.json();
      if (!rows.length) {
        tb.innerHTML = `<tr><td colspan="6" class="text-muted text-center py-2">No applications.</td></tr>`;
        return;
      }
      tb.innerHTML = rows.map(r => `
        <tr data-app-id="${r.id}">
          <td>${r.created_at}</td>
          <td>${r.org_name}</td>
          <td>${r.entity_type}</td>
          <td>${r.role}</td>
          <td>${r.plan}</td>
          <td>${r.is_reviewed ? '✅' : '❌'}</td>
        </tr>
      `).join('');

      tb.querySelectorAll('tr[data-app-id]').forEach(tr => {
        tr.addEventListener('click', () => {
          const id = tr.getAttribute('data-app-id');
          openApplicationModal(id);
        });
      });
    } catch (e) {
      tb.innerHTML = `<tr><td colspan="6" class="text-muted text-center py-2">Failed: ${e.message || 'error'}</td></tr>`;
    }
  }

  function openApplicationModal(appId) {
    const id = 'appModal';
    let modalEl = document.getElementById(id);
    if (!modalEl) {
      modalEl = document.createElement('div');
      modalEl.id = id;
      modalEl.className = 'modal fade';
      modalEl.tabIndex = -1;
      modalEl.innerHTML = `
        <div class="modal-dialog modal-lg">
          <div class="modal-content">
            <div class="modal-header">
              <h5 class="modal-title">Application Review</h5>
              <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
            </div>
            <div class="modal-body">
              <div id="appModalBody" class="small"></div>
              <div class="form-check mt-3">
                <input class="form-check-input" type="checkbox" id="appReviewedChk">
                <label class="form-check-label" for="appReviewedChk">
                  Mark as reviewed
                </label>
              </div>
              <div class="mt-2">
                <label class="form-label" for="appNotes">Notes</label>
                <textarea id="appNotes" class="form-control form-control-sm" rows="4"
                  placeholder="KYC/AML outcome, risk flags, onboarding decisions..."></textarea>
              </div>
            </div>
            <div class="modal-footer">
              <button type="button" class="btn btn-secondary btn-sm" data-bs-dismiss="modal">Close</button>
              <button type="button" class="btn btn-primary btn-sm" id="appSaveBtn">Save</button>
            </div>
          </div>
        </div>
      `;
      document.body.appendChild(modalEl);
    }

    api('/onboarding/applications')
      .then(res => res.json())
      .then(rows => {
        const r = rows.find(x => x.id === appId);
        const body = modalEl.querySelector('#appModalBody');
        const chk = modalEl.querySelector('#appReviewedChk');
        const txt = modalEl.querySelector('#appNotes');
        if (!r || !body || !chk || !txt) return;
        body.innerHTML = `
          <div><strong>Org:</strong> ${r.org_name}</div>
          <div><strong>Type/Role:</strong> ${r.entity_type} / ${r.role}</div>
          <div><strong>Plan:</strong> ${r.plan}</div>
          <div><strong>Volume (tons/m):</strong> ${fmtInt(r.monthly_volume_tons)}</div>
          <div><strong>Contact:</strong> ${r.contact_name} &lt;${r.email}&gt; ${r.phone || ''}</div>
          <div><strong>Created:</strong> ${r.created_at}</div>
        `;
        chk.checked = !!r.is_reviewed;
        txt.value = r.notes || '';
      })
      .catch(() => { /* ignore */ });

    const saveBtn = modalEl.querySelector('#appSaveBtn');
    if (saveBtn) {
      saveBtn.onclick = async () => {
        try {
          const chk = modalEl.querySelector('#appReviewedChk');
          const txt = modalEl.querySelector('#appNotes');
          const body = {
            is_reviewed: !!chk.checked,
            notes: txt.value || null
          };
          await api(`/onboarding/applications/${encodeURIComponent(appId)}`, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
          });
          toast('Application updated');
          bootstrap.Modal.getInstance(modalEl).hide();
          loadApplications().catch(() => { /* ignore */ });
        } catch (e) {
          // toast already shown
        }
      };
    }

    const modal = new bootstrap.Modal(modalEl);
    modal.show();
  }

  // ------------------------
  // Signups
  // ------------------------
  async function loadSignups() {
    const tb = $('#signupTbl')?.querySelector('tbody');
    if (!tb) return;
    tb.innerHTML = `<tr><td colspan="5" class="text-muted text-center py-2">Loading…</td></tr>`;
    try {
      const status = $('#signupStatusFilter')?.value || '';
      const url = status ? `/tenants/signups?status=${encodeURIComponent(status)}` : '/tenants/signups';
      const res = await api(url);
      const rows = await res.json();
      if (!rows.length) {
        tb.innerHTML = `<tr><td colspan="5" class="text-muted text-center py-2">No signups.</td></tr>`;
        return;
      }
      tb.innerHTML = rows.map(r => `
        <tr>
          <td>${r.created_at}</td>
          <td>${r.yard_name}</td>
          <td>${r.contact_name} &lt;${r.email}&gt;</td>
          <td>${r.plan}</td>
          <td>${r.status}</td>
        </tr>
      `).join('');
    } catch (e) {
      tb.innerHTML = `<tr><td colspan="5" class="text-muted text-center py-2">Failed: ${e.message || 'error'}</td></tr>`;
    }
  }

  // ------------------------
  // Orgs & Invites
  // ------------------------
  async function loadOrgs() {
    const sel = $('#orgSelect');
    if (!sel) return;
    sel.innerHTML = `<option value="">(select org)</option>`;
    try {
      const res = await api('/orgs');
      orgList = await res.json();
      orgList.forEach(o => {
        const opt = document.createElement('option');
        opt.value = o.org;
        opt.textContent = o.display_name || o.org;
        sel.appendChild(opt);
      });
    } catch (e) {
      console.warn('loadOrgs failed', e);
    }
  }

  async function loadInvites() {
    const member = $('#orgSelect')?.value || '';
    const tb = $('#inviteTbl')?.querySelector('tbody');
    const orgField = $('#inviteOrg');
    if (orgField) orgField.value = member || '';
    if (!tb) return;
    if (!member) {
      tb.innerHTML = `<tr><td colspan="4" class="text-muted text-center py-2">Select an org.</td></tr>`;
      return;
    }
    tb.innerHTML = `<tr><td colspan="4" class="text-muted text-center py-2">Loading…</td></tr>`;
    try {
      const res = await api(`/invites?member=${encodeURIComponent(member)}`);
      const rows = await res.json();
      if (!rows.length) {
        tb.innerHTML = `<tr><td colspan="4" class="text-muted text-center py-2">No invites yet.</td></tr>`;
        return;
      }
      tb.innerHTML = rows.map(r => `
        <tr>
          <td>${r.email}</td>
          <td>${r.role_req}</td>
          <td>${r.created_at}</td>
          <td>${r.accepted_at || '—'}</td>
        </tr>
      `).join('');
    } catch (e) {
      tb.innerHTML = `<tr><td colspan="4" class="text-muted text-center py-2">Failed: ${e.message || 'error'}</td></tr>`;
    }
  }

  async function createInvite() {
    const member = $('#orgSelect')?.value || '';
    const email = $('#inviteEmail')?.value.trim() || '';
    const role = $('#inviteRole')?.value || 'buyer';
    if (!member) return toast('Select an org first');
    if (!email) return toast('Enter an email');
    try {
      await api('/invites', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, member, role_req: role })
      });
      toast('Invite created');
      $('#inviteEmail').value = '';
      await loadInvites();
    } catch (e) {
      // toast already shown
    }
  }

  // ------------------------
  // Wire it all up
  // ------------------------
  onReady(() => {
    // Applications
    $('#reloadApps')?.addEventListener('click', () => loadApplications().catch(() => { /* ignore */ }));
    $('#appReviewedFilter')?.addEventListener('change', () => loadApplications().catch(() => { /* ignore */ }));
    loadApplications().catch(() => { /* ignore */ });

    // Signups
    $('#reloadSignups')?.addEventListener('click', () => loadSignups().catch(() => { /* ignore */ }));
    $('#signupStatusFilter')?.addEventListener('change', () => loadSignups().catch(() => { /* ignore */ }));
    loadSignups().catch(() => { /* ignore */ });

    // Orgs + invites
    loadOrgs().then(() => loadInvites()).catch(() => { /* ignore */ });
    $('#orgSelect')?.addEventListener('change', () => loadInvites().catch(() => { /* ignore */ }));
    $('#reloadInvites')?.addEventListener('click', () => loadInvites().catch(() => { /* ignore */ }));
    $('#inviteCreate')?.addEventListener('click', () => createInvite().catch(() => { /* ignore */ }));
  });
})();
