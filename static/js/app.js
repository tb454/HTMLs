// endpoint + toast + fetch helper + small utils
window.endpoint = window.endpoint || (location.origin.includes('localhost') ? 'http://localhost:8000' : location.origin);
function toast(msg, ms=2800){ const el = document.getElementById('toast'); if(!el){alert(msg);return;} el.textContent = msg; el.setAttribute('data-show','1'); setTimeout(()=>el.removeAttribute('data-show'), ms); }
function _cookie(name){
  return document.cookie.split('; ').reduce((acc, c) => {
    const [k, ...v] = c.split('=');
    return k === name ? decodeURIComponent(v.join('=')) : acc;
  }, null);
}

async function api(path, opts = {}) {
  const url = path.startsWith('http') ? path : `${window.endpoint}${path}`;
  const method = (opts.method || 'GET').toUpperCase();
  const headers = {
    Accept: 'application/json',
    ...(opts.headers || {})
  };

  // Attach CSRF for unsafe methods (matches backend’s expectations)
  if (!['GET','HEAD','OPTIONS'].includes(method)) {
    const tok = _cookie('XSRF-TOKEN');
    if (tok) headers['X-CSRF'] = tok;
  }

  const res = await fetch(url, {
    credentials: 'same-origin',
    ...opts,
    headers
  });

  if (res.status === 401) {
    // Same behavior you had before
    const next = encodeURIComponent(location.pathname + location.search);
    location.href = `/static/bridge-login.html?next=${next}`;
    throw new Error('Unauthorized');
  }
  if (!res.ok) {
    let m = 'Request failed';
    try { m = (await res.clone().json()).detail || m; }
    catch { m = await res.text() || m; }
    toast(m);
    throw new Error(m);
  }
  return res;
}

const $ = sel => document.querySelector(sel);
function setBtnBusy(btn, busy){ btn?.setAttribute('data-busy', busy ? '1' : '0'); }
function csvDownload(filename, rows){
  if(!rows?.length){ toast('No data.'); return; }
  const headers = Object.keys(rows[0]||{});
  const csv = [headers.join(',')].concat(rows.map(r=>headers.map(h=>JSON.stringify(r[h] ?? '')).join(','))).join('\n');
  const blob = new Blob([csv], {type:'text/csv'}); const url = URL.createObjectURL(blob);
  const a = document.createElement('a'); a.href = url; a.download = filename; a.click(); URL.revokeObjectURL(url);
}

// Abortable fetch timeout (default 8s)
function _withTimeout(ms = 8000) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), ms);
  return { controller, cancel: () => clearTimeout(id) };
}

// JSON convenience wrapper that never hangs UI
async function apiJSON(path, opts = {}) {
  const { controller, cancel } = _withTimeout(opts.timeout || 8000);
  try {
    const res = await api(path, { ...opts, signal: controller.signal });
    // Some endpoints may return empty bodies
    const ct = res.headers.get('content-type') || '';
    if (ct.includes('application/json')) return res.json();
    return null;
  } finally {
    cancel();
  }
}

// ---- app.js (add once globally) ----
function showLoading(){ document.documentElement.classList.add('loading'); }
function hideLoading(){
  document.documentElement.classList.remove('loading');
  const f = document.querySelector('footer');
  if (f) f.classList.remove('invisible');
}
// last resort, never let UI hang > 15s
setTimeout(hideLoading, 15000);

async function startMarketDataWS() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  return new Promise((resolve) => {
    const ws = new WebSocket(`${proto}://${location.host}/md/ws`);
    let settled = false;
    const cleanup = () => { if (!settled) { settled = true; resolve(); } };
    const timer = setTimeout(() => { try { ws.close(); } catch {} cleanup(); }, 5000);
    ws.onopen = () => { clearTimeout(timer); settled = true; resolve(); };
    ws.onerror = cleanup;
    ws.onclose = cleanup;
    ws.onmessage = (ev) => { /* optional live updates */ };
  });
}

function safe(fn){ try { return fn(), true; } catch(e){ console.warn(e); return false; } }

async function initAdmin() {
  showLoading();
    // kick off everything in parallel and NEVER await a single long-poll
    const tasks = [
      apiJSON('/analytics/prices_over_time?material=Shred%20Steel&window=1M')
        .then(d => safe(() => drawPriceChart(d))).catch(e => console.warn('prices_over_time', e)),
      apiJSON('/analytics/tons_by_yard_this_month')
        .then(rows => safe(() => renderYardTons(rows))).catch(e => console.warn('tons_by_yard', e)),
      apiJSON('/admin/futures/products')
        .then(p => safe(() => renderFuturesProducts((p && p.products) ? p.products : (Array.isArray(p) ? p : []))))
        .catch(e => console.warn('futures/products', e)),
      apiJSON('/bols?limit=100&offset=0')
        .then(rows => safe(() => renderBOLs(rows || []))).catch(e => console.warn('bols', e)),
      // do not block on ws
      startMarketDataWS().catch(() => console.info('WS optional; continuing')),
    ];
    await Promise.allSettled(tasks);
  hideLoading();
}

window.addEventListener('DOMContentLoaded', () => {
  initAdmin().catch(err => { console.error(err); hideLoading(); });
});
// ---- static/js/app.js (or your admin boot file) ----
async function startMarketDataWS() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  return new Promise((resolve, reject) => {
    let opened = false;
    const ws = new WebSocket(`${proto}://${location.host}/md/ws`);
    ws.onopen  = () => { opened = true; resolve(); };
    ws.onerror = () => { if (!opened) reject(new Error('ws failed')); };
    ws.onclose = () => {}; // optional: reconnect later
    ws.onmessage = (ev) => { /* update tickers */ };
  });
}

async function initAdmin() {
  showLoading();

  const tasks = [
    // analytics
    apiJSON('/analytics/prices_over_time?material=Shred%20Steel&window=1M')
      .then(data => safe(() => drawPriceChart(data)))
      .catch(err => console.warn('prices_over_time', err)),

    apiJSON('/analytics/tons_by_yard_this_month')
      .then(rows => safe(() => renderYardTons(rows)))
      .catch(err => console.warn('tons_by_yard', err)),

    // products (handle old/new shapes gracefully)
    apiJSON('/admin/futures/products')
      .then(payload => {
        const products = payload?.products ?? payload ?? [];
        safe(() => renderFuturesProducts(products));
      })
      .catch(err => console.warn('futures products', err)),

    // BOLs table
    apiJSON('/bols?limit=100&offset=0')
      .then(rows => safe(() => renderBOLs(rows || [])))
      .catch(err => console.warn('bols', err)),

    // optional websocket — do NOT block ready
    startMarketDataWS().catch(() => console.info('WS optional; continuing')),
  ];

  await Promise.allSettled(tasks);
}

// kick it
window.addEventListener('DOMContentLoaded', () => {
  initAdmin().finally(hideLoading);
});

function drawPriceChart(data){
  if (!Array.isArray(data) || !data.length) { /* show placeholder */ return; }
  // ... draw chart ...
}

function renderYardTons(rows){
  if (!Array.isArray(rows)) rows = [];
  // ... build table ...
}

function renderFuturesProducts(products){
  if (!Array.isArray(products)) products = [];
  // ... list products ...
}

function renderBOLs(rows){
  if (!Array.isArray(rows)) rows = [];
  // ... render BOL table ...
}
