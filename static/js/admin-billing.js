// ----------------------
// Helpers & boot
// ----------------------

// Small onReady helper (same pattern as other pages)
function onReady(fn){
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', fn);
  } else {
    try { fn(); } catch (e) { console.error(e); }
  }
}

// Mint XSRF-TOKEN early
onReady(async () => {
  try {
    await fetch('/health', { credentials: 'include' });
  } catch {}
});

// Remove loading class if you use it
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
// Toast + API helper
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

// Health badge for this page
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
  } catch {
    el.textContent = '● Degraded';
    el.dataset.ok = '0';
  }
});

// ----------------------
// Billing page logic
// ----------------------
(function(){
  const fmtUSD = new Intl.NumberFormat(undefined, {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 2,
  });

  let currentMember = null;
  const $ = id => document.getElementById(id);

  async function loadPlans(){
    const tb = $('#planTbl')?.querySelector('tbody');
    if (!tb) return;
    tb.innerHTML = `<tr><td colspan="4" class="text-muted text-center py-2">Loading…</td></tr>`;
    try{
      const res = await api('/billing/plans');
      const rows = await res.json();
      if (!rows.length){
        tb.innerHTML = `<tr><td colspan="4" class="text-muted text-center py-2">No plans configured.</td></tr>`;
        return;
      }
      tb.innerHTML = rows.map(r => `
        <tr>
          <td>${r.plan_code}</td>
          <td>${r.name}</td>
          <td>${fmtUSD.format(r.price_usd)}</td>
          <td>${r.active ? 'Yes' : 'No'}</td>
        </tr>
      `).join('');
    }catch(e){
      tb.innerHTML = `<tr><td colspan="4" class="text-muted text-center py-2">Failed: ${e.message || 'error'}</td></tr>`;
    }
  }

  async function loadMembersSummary(){
    const tb = $('#memberTbl')?.querySelector('tbody');
    if (!tb) return;
    tb.innerHTML = `<tr><td colspan="5" class="text-muted text-center py-2">Loading…</td></tr>`;
    try{
      const [usageRes, guarRes] = await Promise.all([
        api('/analytics/usage_by_member_current_cycle'),
        api('/billing/guaranty_fund'),
      ]);
      const usage = await usageRes.json();
      const guar  = await guarRes.json();
      const guarMap = {};
      guar.forEach(g => { guarMap[g.member] = g.contribution_usd; });

      if (!usage.length){
        tb.innerHTML = `<tr><td colspan="5" class="text-muted text-center py-2">No usage yet in this cycle.</td></tr>`;
        return;
      }
      tb.innerHTML = usage.map(u => {
        const g = guarMap[u.member] || 0;
        return `
          <tr data-member="${u.member}">
            <td>${u.member}</td>
            <td>${u.plan_code || ''}</td>
            <td>${u.bols_month ?? 0}</td>
            <td>${u.contracts_month ?? 0}</td>
            <td>${g ? fmtUSD.format(g) : '—'}</td>
          </tr>`;
      }).join('');

      tb.querySelectorAll('tr[data-member]').forEach(tr => {
        tr.addEventListener('click', () => {
          const member = tr.getAttribute('data-member');
          setMember(member);
        });
      });

    }catch(e){
      tb.innerHTML = `<tr><td colspan="5" class="text-muted text-center py-2">Failed: ${e.message || 'error'}</td></tr>`;
    }
  }

  async function setMember(member){
    currentMember = member;
    $('#detailMember').textContent = member ? `Member: ${member}` : '';
    $('#detailStatus').textContent = 'Loading…';
    await Promise.all([
      loadMemberPlan(member),
      loadMemberGuarantyAndDefaults(member),
      loadPrefs(member),
      loadInvoices(member),
    ]).catch(() => {});
    $('#detailStatus').textContent = '';
  }

  async function loadMemberPlan(member){
    const el = $('#detailPlan');
    if (!el) return;
    el.textContent = '—';
    try{
      const res = await api(`/billing/member_plan?member=${encodeURIComponent(member)}`);
      const row = await res.json();
      if (!row){
        el.textContent = 'No plan recorded';
      } else {
        el.textContent = `${row.plan_code} (since ${row.effective_date})`;
      }
    }catch(e){
      el.textContent = `Error: ${e.message || 'failed'}`;
    }
  }

  async function loadMemberGuarantyAndDefaults(member){
    const elG = $('#detailGuaranty');
    const elD = $('#detailDefaults');
    if (elG) elG.textContent = '—';
    if (elD) elD.textContent = '—';
    try{
      const [gRes, dRes] = await Promise.all([
        api('/billing/guaranty_fund'),
        api(`/billing/defaults?member=${encodeURIComponent(member)}`),
      ]);
      const gRows = await gRes.json();
      const dRows = await dRes.json();
      const g = gRows.find(r => r.member === member);
      if (g && elG){
        elG.textContent = fmtUSD.format(g.contribution_usd);
      }
      if (dRows.length && elD){
        const total = dRows.reduce((acc, r) => acc + Number(r.amount_usd || 0), 0);
        elD.textContent = `${dRows.length} event(s), total ${fmtUSD.format(total)}`;
      } else if (elD){
        elD.textContent = 'None';
      }
    }catch(e){
      if (elG) elG.textContent = 'Error';
      if (elD) elD.textContent = 'Error';
    }
  }

  async function loadPrefs(member){
    const dayEl = $('#prefDay');
    const tzEl  = $('#prefTz');
    const ccEl  = $('#prefCc');
    const aEl   = $('#prefAutopay');
    const acEl  = $('#prefAutoCharge');
    if (!dayEl || !tzEl || !ccEl || !aEl || !acEl) return;
    dayEl.value = '';
    tzEl.value  = '';
    ccEl.value  = '';
    aEl.checked = false;
    acEl.checked= false;
    try{
      const res = await api(`/billing/preferences?member=${encodeURIComponent(member)}`);
      const p = await res.json();
      if (!p) return;
      dayEl.value = p.billing_day || '';
      tzEl.value  = p.timezone || '';
      ccEl.value  = (p.invoice_cc_emails || []).join(', ');
      aEl.checked = !!p.autopay;
      acEl.checked = !!p.auto_charge;
    }catch(e){
      // ignore; prefs may not exist yet
    }
  }

  async function savePrefs(){
    if (!currentMember) return toast('Select a member first');
    const body = {
      billing_day: $('#prefDay').value ? parseInt($('#prefDay').value,10) : null,
      invoice_cc_emails: $('#prefCc').value
        ? $('#prefCc').value.split(',').map(s => s.trim()).filter(Boolean)
        : [],
      autopay: $('#prefAutopay').checked,
      timezone: $('#prefTz').value || null,
      auto_charge: $('#prefAutoCharge').checked,
    };
    try{
      await api(`/billing/preferences?member=${encodeURIComponent(currentMember)}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      toast('Preferences saved');
      await loadPrefs(currentMember);
    }catch(e){
      // toast already shown by api()
    }
  }

  async function loadInvoices(member){
    const tb = $('#invoiceTbl')?.querySelector('tbody');
    const linesTb = $('#invoiceLinesTbl')?.querySelector('tbody');
    if (!tb || !linesTb) return;
    tb.innerHTML = `<tr><td colspan="3" class="text-muted text-center py-2">Loading…</td></tr>`;
    linesTb.innerHTML = `<tr><td colspan="2" class="text-muted text-center py-2">Select an invoice.</td></tr>`;
    try{
      const res = await api(`/billing/invoices?member=${encodeURIComponent(member)}`);
      const rows = await res.json();
      if (!rows.length){
        tb.innerHTML = `<tr><td colspan="3" class="text-muted text-center py-2">No invoices yet.</td></tr>`;
        return;
      }
      tb.innerHTML = rows.map(r => `
        <tr data-invoice="${r.invoice_id}">
          <td>${r.period_start} – ${r.period_end}</td>
          <td>${fmtUSD.format(r.total)}</td>
          <td>${r.status}</td>
        </tr>
      `).join('');

      tb.querySelectorAll('tr[data-invoice]').forEach(tr => {
        tr.addEventListener('click', () => {
          const id = tr.getAttribute('data-invoice');
          loadInvoiceLines(id);
        });
      });

    }catch(e){
      tb.innerHTML = `<tr><td colspan="3" class="text-muted text-center py-2">Failed: ${e.message || 'error'}</td></tr>`;
    }
  }

  async function loadInvoiceLines(invoiceId){
    const tb = $('#invoiceLinesTbl')?.querySelector('tbody');
    if (!tb) return;
    tb.innerHTML = `<tr><td colspan="2" class="text-muted text-center py-2">Loading…</td></tr>`;
    try{
      const res = await api(`/billing/invoices/${encodeURIComponent(invoiceId)}/lines`);
      const rows = await res.json();
      if (!rows.length){
        tb.innerHTML = `<tr><td colspan="2" class="text-muted text-center py-2">No line items.</td></tr>`;
        return;
      }
      tb.innerHTML = rows.map(r => `
        <tr>
          <td>${r.event_type || ''}${r.description ? ' — ' + r.description : ''}</td>
          <td>${fmtUSD.format(r.amount)} ${r.currency}</td>
        </tr>
      `).join('');
    }catch(e){
      tb.innerHTML = `<tr><td colspan="2" class="text-muted text-center py-2">Failed: ${e.message || 'error'}</td></tr>`;
    }
  }

  onReady(() => {
    loadPlans().catch(() => {});
    loadMembersSummary().catch(() => {});
    const reload = document.getElementById('reloadPlans');
    if (reload) reload.addEventListener('click', () => loadPlans().catch(() => {}));
    const save = document.getElementById('prefsSave');
    if (save) save.addEventListener('click', savePrefs);
  });
})();
