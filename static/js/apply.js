// Unified API base
window.endpoint = window.endpoint || (location.origin.includes('localhost') ? 'http://localhost:8000' : location.origin);

// --- TEMP: Free mode (no payment required) ---
const FREE_MODE = true; // flip to false when 3 For Free ends
function isFreeMode(){
  return !!FREE_MODE || window.BRIDGE_FREE_MODE === '1' || localStorage.getItem('BRIDGE_FREE_MODE') === '1';
}

function _cookie(name){
  const m = document.cookie.match(new RegExp('(?:^|; )'+name.replace(/([.*+?^${}()|[\]\\])/g,'\\$1')+'=([^;]*)'));
  return m ? decodeURIComponent(m[1]) : '';
}

function toast(msg, ms=2800){
  const t=document.getElementById('toast'); if(!t){ alert(msg); return; }
  t.textContent=typeof msg==='string'?msg:(msg?.message||'');
  t.style.display='block';
  try { setTimeout(()=>t.style.display='none', ms); } catch {}
}

async function api(path, opts={}){
  const url = path.startsWith('http') ? path : `${window.endpoint}${path}`;
  const res = await fetch(url, {
    ...opts,
    headers:{ Accept:'application/json', 'X-CSRF': _cookie('XSRF-TOKEN'), ...(opts.headers||{}) },
    credentials: 'include'
  });
  if(res.status===401){
    const next = encodeURIComponent(location.pathname + location.search);
    location.href = `/static/bridge-login.html?next=${next}`;
    throw new Error('Unauthorized');
  }
  if(!res.ok){
    let m='Request failed';
    try{ m=(await res.clone().json()).detail||m; }catch{ try{ m=await res.clone().text()||m; }catch{} }
    throw new Error(m);
  }
  return res;
}

// DOM helpers
const $ = (id)=>document.getElementById(id);
const qs = new URLSearchParams(location.search);
function setVal(id,v){ const el=$(id); if(el!=null) el.value = v; }
function show(kind,msg){
  const box=$('alert');
  box.className = 'alert ' + (kind==='ok' ? 'alert-success' : 'alert-danger');
  box.textContent = msg;
  box.classList.remove('d-none');
  window.scrollTo({top:0,behavior:'smooth'});
}

// Health badge
(async ()=>{
  const el = document.querySelector('[data-health]');
  if (!el) return;
  try {
    const r = await api('/health', { headers:{Accept:'application/json'} });
    if (r && r.ok) { el.textContent = '● Live'; el.setAttribute('data-ok','1'); return; }
  } catch {}
  el.textContent = '● Degraded';
})();

// Prefill referral/utm
setVal('ref_code', qs.get('ref') || '');
const utm = {
  utm_source:  qs.get('utm_source')  || null,
  utm_campaign:qs.get('utm_campaign')|| null,
  utm_medium:  qs.get('utm_medium')  || null
};

// Draft autosave/restore
const KEY='bridge_apply_draft_v1';
const form=$('applyForm');
document.getElementById('refreshPmBtn')?.addEventListener('click', refreshPmBadge);
form.addEventListener('input', ()=> {
  const data = Object.fromEntries(new FormData(form).entries());
  localStorage.setItem(KEY, JSON.stringify(data));
});
(function restore(){
  const raw=localStorage.getItem(KEY); if(!raw) return;
  try{
    const data=JSON.parse(raw);
    Object.entries(data).forEach(([k,v])=>{
      const byName = form.querySelector(`[name="${CSS.escape(k)}"]`);
      if (byName) { byName.value = v; return; }
      const byId = document.getElementById(k);
      if (byId) byId.value = v;
    });
  }catch{}
})();
function updateSubmitDisabled(){
  const submit = document.getElementById('submitBtn');
  if (!submit) return;
  const ok = form.checkValidity()
    && document.getElementById('agree')?.checked
    && document.getElementById('feesAgree')?.checked;
  submit.disabled = !ok;
}

// keep button state current in FREE mode
['input','change'].forEach(evt => {
  form.addEventListener(evt, () => { if (isFreeMode()) updateSubmitDisabled(); });
});
document.getElementById('agree')?.addEventListener('change', () => { if (isFreeMode()) updateSubmitDisabled(); });
document.getElementById('feesAgree')?.addEventListener('change', () => { if (isFreeMode()) updateSubmitDisabled(); });

// ---- Payment Method flow (Stripe Checkout in setup mode) ----
async function pmStatus(member){
  const res = await api(`/billing/pm/status?member=${encodeURIComponent(member)}`);
  return await res.json();
}
async function pmStart(member, email){
  const res = await api('/billing/pm/setup_session', {
    method: 'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ member, email })
  });
  const j = await res.json();
  if (j.url) { location.href = j.url; return; }
  throw new Error('Could not start payment method setup.');
}
function memberKeyFromForm(){
  const org = (document.getElementById('org_name')?.value || '').trim();
  const mail = (document.getElementById('email')?.value || '').trim();
  return org || mail || '';
}
async function refreshPmBadge(){
  const member = memberKeyFromForm();
  const plan = (document.getElementById('plan')?.value || '').trim();

  // FREE MODE: no payment required, keep UI visible but disabled
  if (isFreeMode()) {
    const pmEl = document.getElementById('pmStatus');
    if (pmEl) pmEl.textContent = 'Payment method not required during free access.';
    const hint = document.getElementById('subHint');
    if (hint) hint.textContent = 'Subscriptions are disabled during free access.';
    const subBtn = document.getElementById('startSubBtn');
    if (subBtn) subBtn.disabled = true;

    // Submit enabled purely by validity + required checkboxes
    updateSubmitDisabled();
    return;
  }

  // PAID MODE
  if(!member){ return; }
  try{
    const s = await pmStatus(member);
    const hasPm = !!s.has_default;
    document.getElementById('pmStatus').textContent = hasPm
      ? `Payment method on file: ${s.pm || 'default'}`
      : 'No payment method yet.';
    document.getElementById('submitBtn').disabled = !hasPm;

    const subBtn = document.getElementById('startSubBtn');
    const canStart = hasPm && !!plan;
    if (subBtn) subBtn.disabled = !canStart;
    const hint = document.getElementById('subHint');
    if (hint) hint.textContent =
      canStart ? 'Ready to start subscription.' : 'Choose a plan and add a payment method to enable “Start Subscription”.';
  }catch{
    document.getElementById('submitBtn').disabled = true;
    const subBtn = document.getElementById('startSubBtn');
    if (subBtn) subBtn.disabled = true;
  }
}

// Wire buttons (no inline handlers)
document.getElementById('startSubBtn')?.addEventListener('click', async (ev)=>{
  if (isFreeMode()) { toast('Subscriptions are disabled during free access.'); return; }
  const btn = ev.currentTarget;
  const member = memberKeyFromForm();
  const plan = (document.getElementById('plan').value || '').trim().toLowerCase();
  if (!member || !plan) { toast('Set Organization and Plan first'); return; }
  btn.disabled = true; const old=btn.textContent; btn.textContent='Starting…';
  try {
    await startSubscription(member, plan);
  } catch (e) {
    toast(e.message || 'Could not start subscription checkout.');
    btn.disabled = false; 
    btn.textContent = old;
  }
});
document.getElementById('addPmBtn')?.addEventListener('click', async ()=>{
  if (isFreeMode()) { toast('Payment method collection is disabled during free access.'); return; }
  const member = memberKeyFromForm();
  const email  = (document.getElementById('email').value || '').trim();
  if(!member || !email){ toast('Enter Organization and Email first'); return; }
  try{ await pmStart(member, email); }
  catch(e){ toast(e.message || 'Could not start payment method setup'); }
});

// Locale save button (replacing inline onclick)
document.getElementById('locale-save')?.addEventListener('click', ()=>{
  try { BRIDGE_LOCALE.save(); } catch {}
});

// Finalize PM / Subscription on load
(async ()=>{
  const pmOk   = qs.get('pm') === 'ok';
  const pmSess = qs.get('sess');
  if (isFreeMode()) {
    const lbl = document.getElementById('payment_method_lbl');
    if (lbl) lbl.textContent = 'Payment Method (temporarily not required)';
    updateSubmitDisabled();
  }
  
  if (pmOk && pmSess) {
    const member = memberKeyFromForm() || (qs.get('member') || '');
    try {
      await api(`/billing/pm/finalize_from_session?sess=${encodeURIComponent(pmSess)}&member=${encodeURIComponent(member)}`);
      toast('✓ Payment method saved');
    } catch (e) {
      console.warn('PM finalize failed', e);
      toast('Payment method saved (finalization deferred)');
    }
  } else if (pmOk) {
    toast('✓ Payment method saved');
  }

  const subOk = qs.get('sub') === 'ok';
  const subSession = qs.get('session');
  if (subOk && subSession) {
    const member = memberKeyFromForm() || (qs.get('member') || '');
    try {
      const res = await api(`/billing/subscribe/finalize_from_session?sess=${encodeURIComponent(subSession)}&member=${encodeURIComponent(member)}`);
      const j = await res.json();
      toast(`✓ Subscription active — ${j.plan || 'plan'}`);
    } catch (e) {
      console.warn('Subscription finalize failed', e);
      toast('Subscription recorded. If status is not updated, refresh.');
    }
  }

  await refreshPmBadge();
})();

// Submit handler (PM required)
form.addEventListener('submit', async (e)=>{
  e.preventDefault();
  $('alert').classList.add('d-none');

  // Honeypot
  if ((document.getElementById('fax_hp')?.value || '').trim()) {
    show('err','Submission blocked.'); 
    return;
  }

  if(!form.checkValidity()){
    const bad=form.querySelector(':invalid'); bad?.focus();
    show('err','Please fix the highlighted fields.'); return;
  }
  if(!$('agree').checked){ show('err','Please accept the Terms & Privacy.'); return; }

  const member = memberKeyFromForm();
  if(!member){ show('err','Organization or Email is required.'); return; }

  if (!isFreeMode()) {
    try{
      const s = await pmStatus(member);
      if(!s.has_default){
        show('err','Add a payment method to continue.'); toast('Payment method required'); return;
      }
    }catch{
      show('err','Could not verify payment method. Try again.'); return;
    }
  }

  const fd = new FormData(form);
  const payload = {
    entity_type: (fd.get('entity_type') || '').trim() || null,
    role:        (fd.get('role') || '').trim() || null,
    org_name:    (fd.get('org_name') || fd.get('organization') || '').trim() || null,
    ein:         (fd.get('ein') || '').trim() || null,
    address:     (fd.get('street-address') || fd.get('address') || '').trim() || null,
    city:        (fd.get('address-level2') || fd.get('city') || '').trim() || null,
    region:      (fd.get('address-level1') || fd.get('region') || '').trim() || null,
    website:     (fd.get('website') || '').trim() || null,
    monthly_volume_tons: fd.get('monthly_volume_tons') ? parseInt(fd.get('monthly_volume_tons'),10) : null,
    contact_name: (fd.get('name') || fd.get('contact_name') || '').trim() || null,
    email:        (fd.get('email') || '').trim() || null,
    phone:        (fd.get('tel') || fd.get('phone') || '').trim() || null,
    ref_code:     (fd.get('ref_code') || '').trim() || null,
    materials_buy: (fd.get('materials_buy') || '').trim() || null,
    materials_sell:(fd.get('materials_sell') || '').trim() || null,
    lanes:         (fd.get('lanes') || '').trim() || null,
    compliance_notes:(fd.get('compliance_notes') || '').trim() || null,
    plan:           (fd.get('plan') || '').trim() || null,
    notes:          (fd.get('notes') || '').trim() || null,
    utm_source:     qs.get('utm_source')  || null,
    utm_campaign:   qs.get('utm_campaign')|| null,
    utm_medium:     qs.get('utm_medium')  || null
  };

  const btn=$('submitBtn'); btn.disabled=true; const old=btn.textContent; btn.textContent='Submitting…';
  try{
    const idem = (self.crypto?.randomUUID?.() || String(Date.now()) + Math.random());
    await api('/public/apply', {
      method:'POST',
      headers:{'Content-Type':'application/json','Idempotency-Key': idem},
      body: JSON.stringify(payload)
    });
    show('ok',"Thanks! Your application has been received. We'll email you after review.");
    toast('✓ Application submitted');
    localStorage.removeItem('bridge_apply_draft_v1'); form.reset(); $('agree').checked=false;
    await refreshPmBadge();
  }catch(err){
    show('err', err?.message || 'Could not submit application.');
  }finally{
    btn.disabled=false; btn.textContent=old;
  }
});

// Subscription start
async function startSubscription(member, plan) {
  const res = await api('/billing/subscribe/checkout', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({ member, plan })
  });
  const j = await res.json();
  if (j.url) { location.href = j.url; return; }
  throw new Error('Could not start subscription checkout.');
}

// Keep PM/Subscription readiness current
['org_name','email','plan'].forEach(id=>{
  const el = document.getElementById(id);
  if (!el) return;
  el.addEventListener('blur', refreshPmBadge);
  el.addEventListener('change', refreshPmBadge);
});

// Refresh on back/forward cache
window.addEventListener('pageshow', () => { try { refreshPmBadge(); } catch {} });
