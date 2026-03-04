// indices.js — BR-Index mode (bridge_index_definitions + bridge_index_history)

// ---------- tiny helpers ----------
const $ = (sel) => document.querySelector(sel);

// ---- array + price helpers (indices_daily is usually USD/ton; UI wants USD/lb) ----
function _arr(x){
  if (!x) return [];
  if (Array.isArray(x)) return x;
  if (Array.isArray(x.items)) return x.items;
  if (Array.isArray(x.rows)) return x.rows;
  if (Array.isArray(x.data)) return x.data;
  if (Array.isArray(x.history)) return x.history;
  if (Array.isArray(x.series)) return x.series;
  if (Array.isArray(x.tickers)) return x.tickers;
  if (Array.isArray(x.indices)) return x.indices;
  return [];
}

function toUSDlb(v, unit){
  const n = Number(v);
  if (!Number.isFinite(n)) return null;
  const u = String(unit || '').toLowerCase();

  if (u.includes('/lb')) return n;
  if (u.includes('/ton')) return n / 2000;

  // heuristic fallback: big numbers are almost always $/ton
  return n > 100 ? n / 2000 : n;
}

// Pull official “public mirror” payload for a symbol (indices_daily-backed)
async function getPublicIndexData(sym){
  return await getJSON(`/public/index/${encodeURIComponent(sym)}/data?region=blended`);
}

function setBusy(btn, busy){ btn?.setAttribute('data-busy', busy ? '1' : '0'); }

function csvDownload(filename, rows){
  if(!rows?.length){ toast?.('No data.'); return; }
  const headers = Object.keys(rows[0]||{});
  const csv = [headers.join(',')].concat(
    rows.map(r=>headers.map(h=>JSON.stringify(r[h] ?? '')).join(','))
  ).join('\n');
  const blob = new Blob([csv], {type:'text/csv'});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a'); a.href = url; a.download = filename; a.click();
  URL.revokeObjectURL(url);
}

function showSkeleton(tbody, cols){
  tbody.innerHTML='';
  for(let i=0;i<5;i++){
    const tr=document.createElement('tr');
    tr.className='skeleton-row';
    tr.innerHTML = Array.from({length:cols},()=>'<td><span class="skeleton"></span></td>').join('');
    tbody.appendChild(tr);
  }
}

// Safe JSON fetcher (works whether app.js has apiJSON or only api)
async function getJSON(path, opts){
  // If you already have apiJSON in app.js, use it.
  if (typeof window.apiJSON === 'function') return window.apiJSON(path, opts);

  // If you have api() and it returns a Response, parse it.
  if (typeof window.api === 'function') {
    const r = await window.api(path, opts);
    // Some app.js implementations already return parsed JSON.
    if (r && typeof r === 'object' && !(r instanceof Response) && !('ok' in r)) return r;
    if (r instanceof Response) {
      if (!r.ok) throw new Error(`HTTP ${r.status} ${r.statusText}`);
      return await r.json();
    }
  }

  // Raw fetch fallback
  const res = await fetch(path, {
    credentials: 'include',
    headers: { 'Accept': 'application/json' },
    ...opts
  });
  if(!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
  return await res.json();
}

// ---- indices_daily public cache (single fetch) ----
let __dailyBySymbol = null;   // Map(symbol -> [{dt,value}...])
let __dailyMaxDate  = null;

function _rowsFromDailyPayload(payload){
  // daily.json might be array, or {rows:[]}, or {data:[]}
  const arr = _arr(payload?.rows || payload?.data || payload);
  return Array.isArray(arr) ? arr : [];
}

async function ensureDailyCache(){
  if (__dailyBySymbol) return { bySymbol: __dailyBySymbol, maxDate: __dailyMaxDate };

  const payload = await getJSON('/public/indices/daily.json');
  const rowsAll = _rowsFromDailyPayload(payload);

  // Keep only blended rows with a symbol + date
  const rows = rowsAll
    .filter(r => r && String(r.region || '').toLowerCase() === 'blended')
    .map(r => ({
      sym: String(r.symbol || r.sym || r.ticker || r.material || '').trim(),
      dt:  (r.as_of_date || r.dt || r.date || '').toString(),
      unit: r.unit || payload?.unit || 'USD/ton',
      raw:  (r.index_price_per_ton ?? r.avg_price ?? r.price ?? r.close_price ?? r.close ?? r.value)
    }))
    .filter(r => r.sym && r.dt);

  const bySymbol = new Map();
  let maxDate = null;

  for (const r of rows){
    const v = toUSDlb(r.raw, r.unit);
    if (v == null) continue;

    if (!bySymbol.has(r.sym)) bySymbol.set(r.sym, []);
    bySymbol.get(r.sym).push({ dt: r.dt, value: v });

    if (!maxDate || new Date(r.dt) > new Date(maxDate)) maxDate = r.dt;
  }

  // sort each series asc
  for (const [k, arr] of bySymbol.entries()){
    arr.sort((a,b)=> new Date(a.dt) - new Date(b.dt));
  }

  __dailyBySymbol = bySymbol;
  __dailyMaxDate  = maxDate;
  return { bySymbol, maxDate };
}

// DPR-aware lightweight line chart
function drawLine(canvas, series, options={}){
  const ctx = canvas.getContext('2d');
  const dpr = Math.max(1, Math.min(3, window.devicePixelRatio || 1));
  const cssW = canvas.clientWidth, cssH = canvas.clientHeight;
  canvas.width  = Math.floor(cssW * dpr);
  canvas.height = Math.floor(cssH * dpr);
  ctx.setTransform(dpr,0,0,dpr,0,0);

  const W = cssW, H = cssH;
  ctx.clearRect(0,0,W,H);
  if(!series.length) return;

  const pad = 28;
  const xs = series.map(d=>new Date(d.dt).getTime());
  const ys = series.map(d=>+d.value);
  const minX = Math.min(...xs), maxX = Math.max(...xs);
  const minY = Math.min(...ys), maxY = Math.max(...ys);
  const rangeY = maxY - minY || 1;
  const y0 = minY - rangeY*0.05, y1 = maxY + rangeY*0.05;

  const xPix = t => pad + (W-2*pad)*( (t-minX)/(maxX-minX || 1) );
  const yPix = v => H-pad - (H-2*pad)*( (v-y0)/(y1-y0 || 1) );

  // axes
  ctx.strokeStyle = '#e5e7eb'; ctx.lineWidth=1;
  ctx.beginPath(); ctx.moveTo(pad, pad); ctx.lineTo(pad, H-pad); ctx.lineTo(W-pad, H-pad); ctx.stroke();

  // grid + labels
  ctx.fillStyle = '#6b7280'; ctx.font='12px system-ui';
  const ticks = 5;
  for(let i=0;i<=ticks;i++){
    const val = y0 + (y1-y0)*i/ticks;
    const y = yPix(val);
    ctx.strokeStyle = '#f3f4f6';
    ctx.beginPath(); ctx.moveTo(pad,y); ctx.lineTo(W-pad,y); ctx.stroke();
    ctx.fillText(val.toFixed(4), 6, y+3);
  }

  // main series
  ctx.strokeStyle = options.color || '#111827'; ctx.lineWidth=2;
  ctx.beginPath();
  series.forEach((d,idx)=>{
    const x=xPix(new Date(d.dt).getTime()), y=yPix(d.value);
    if(idx===0) ctx.moveTo(x,y); else ctx.lineTo(x,y);
  });
  ctx.stroke();

  // optional CI band
  if(options.ci){
    const lows  = options.ci.map(d=>({dt:d.dt, value:d.low}));
    const highs = options.ci.map(d=>({dt:d.dt, value:d.high}));
    ctx.fillStyle = 'rgba(37,99,235,.12)';
    ctx.beginPath();
    highs.forEach((d,idx)=>{
      const x=xPix(new Date(d.dt).getTime()), y=yPix(d.value);
      if(idx===0) ctx.moveTo(x,y); else ctx.lineTo(x,y);
    });
    for(let i=lows.length-1;i>=0;i--){
      const d=lows[i];
      const x=xPix(new Date(d.dt).getTime()), y=yPix(d.value);
      ctx.lineTo(x,y);
    }
    ctx.closePath(); ctx.fill();
  }
}

// ---------- state ----------
let universe = [];
let selectedSymbol = null;
let historyDays = 30;
let showForecast = false;

// ---------- UI bits ----------
window.addEventListener('DOMContentLoaded', async () => {
  try {
    await (window.api ? window.api('/health') : fetch('/health'));
    const b = document.querySelector('[data-health-badge]');
    if (b) { b.textContent = '● Live'; b.setAttribute('data-ok','1'); }
  } catch (e) {
    console.error('health check failed', e);
  }
});

async function getLastFromHistory(sym){
  try{
    const data = await getPublicIndexData(sym);
    const unitDefault = data?.unit || 'USD/ton';

    const hist = _arr(data?.history);
    const seriesAll = hist
      .filter(r => r && (r.as_of_date || r.dt || r.date))
      .map(r => {
        const dt = (r.as_of_date || r.dt || r.date);
        const raw =
          r.index_price_per_ton ??
          r.avg_price ??
          r.price ??
          r.settle_price ??
          r.close_price ??
          r.close ??
          r.value;
        const unit = r.unit || unitDefault;
        return { dt, value: toUSDlb(raw, unit) };
      })
      .filter(p => p.dt && Number.isFinite(p.value))
      .sort((a,b)=> new Date(a.dt) - new Date(b.dt));

    const n = seriesAll.length;
    const last = n ? seriesAll[n-1].value : null;
    const prev = n > 1 ? seriesAll[n-2].value : null;
    const delta = (last!=null && prev!=null) ? (last - prev) : null;
    const updated = n ? seriesAll[n-1].dt : null;

    return { last, delta, updated };
  } catch(e){
    // BRIX_* (or anything with no blended history) will land here or return empty -> show '-'
    console.warn('getLastFromHistory failed', sym, e);
    return { last:null, delta:null, updated:null };
  }
}

async function loadUniverse(){
  const tbody = $('#universeTbl tbody');
  showSkeleton(tbody, 5);

  try{
    const rows = await getJSON('/public/indices/universe');
    const arr = _arr(rows);

    // Accept either array of strings or objects: {symbol,...} from indices_universe
    const defs = arr
      .map(d => {
        if (typeof d === 'string') return { symbol: d.trim() };
        return { symbol: String(d.symbol || d.ticker || d.name || '').trim() };
      })
      .filter(d => d.symbol);

    universe = defs.map(d => ({ symbol: d.symbol }));

    // filter
    const q = ($('#filterInput')?.value || '').trim().toLowerCase();
    const filter = (row) => !q || (row.symbol||'').toLowerCase().includes(q);

    tbody.innerHTML = '';
    for (const row of universe.filter(filter)){
      const sym = row.symbol;
      const tr = document.createElement('tr');
      tr.setAttribute('data-sym', sym);
      tr.innerHTML = `
        <td><b>${sym}</b></td>
        <td class="muted">…</td>
        <td class="muted">…</td>
        <td class="muted">…</td>
        <td><button class="btn" data-view="${sym}">View</button></td>
      `;
      tbody.appendChild(tr);
    }

    if (!tbody.children.length){
      tbody.innerHTML = `<tr><td colspan="5" class="muted">No rows match your filter.</td></tr>`;
    }

    // Fill Last/Δ/Updated from ONE daily.json cache (no per-symbol HTTP calls)
    (async function fillUniverseMetrics(){
      let bySymbol = new Map();
      let maxDate = null;

      try {
        const cache = await ensureDailyCache();
        bySymbol = cache.bySymbol || new Map();
        maxDate  = cache.maxDate || null;
      } catch (e) {
        console.warn('ensureDailyCache failed', e);
      }

      const rowsToFill = Array.from(tbody.querySelectorAll('tr[data-sym]'));
      for (const tr of rowsToFill){
        const sym = tr.getAttribute('data-sym');
        const series = bySymbol.get(sym) || [];
        const n = series.length;

        const last = n ? series[n-1].value : null;
        const prev = n > 1 ? series[n-2].value : null;
        const delta = (last!=null && prev!=null) ? (last - prev) : null;
        const updated = n ? series[n-1].dt : (maxDate || null);

        tr.cells[1].innerText = (last!=null ? (+last).toFixed(4) : '-');
        tr.cells[2].innerText = (delta!=null ? (delta>0?'+':'') + (+delta).toFixed(4) : '-');
        tr.cells[2].className = (delta>0?'positive':delta<0?'negative':'muted');
        tr.cells[3].innerText = (updated || '-');
      }
    })();

    const note = document.getElementById('detailNote');
    if (note){
      note.textContent =
         'Indices Daily mode. USD/lb. Universe from indices_universe; history from indices_daily (public mirror).';
    }

  } catch(e){
    console.error(e);
    tbody.innerHTML = `<tr><td colspan="5" class="muted">Failed to load index universe.</td></tr>`;
    toast?.('Could not load index universe');
  }
}

async function viewSymbol(sym){
  selectedSymbol = sym;

  const title = $('#detailTitle'); if (title) title.textContent = sym;

  // Pull full history from daily.json cache first (reliable), fallback to /public/index if needed
  let histRows = [];
  let unitDefault = 'USD/ton';

  try {
    const cache = await ensureDailyCache();
    const series = cache.bySymbol.get(sym) || [];
    // Convert to the same shape your mapper expects
    histRows = series.map(p => ({ as_of_date: p.dt, price: p.value, unit: 'USD/lb' }));
    unitDefault = 'USD/lb';
  } catch {}

  if (!histRows.length) {
    const data = await getPublicIndexData(sym);
    unitDefault = data?.unit || 'USD/ton';
    histRows = _arr(data?.history);
  }

  // Accept either {dt, close_price} (bridge_index_history) or {dt, close} (fallback)
  const seriesAll = (histRows || [])
    .filter(r => r && (r.as_of_date || r.dt || r.date))
    .map(r => {
      const dt = (r.as_of_date || r.dt || r.date);
      const raw =
        r.index_price_per_ton ??
        r.avg_price ??
        r.price ??
        r.settle_price ??
        r.close_price ??
        r.close ??
        r.value;
      const unit = r.unit || unitDefault;
      return { dt, value: toUSDlb(raw, unit) };
    })
    .filter(p => p.dt && Number.isFinite(p.value))
    .sort((a,b)=> new Date(a.dt) - new Date(b.dt));

  const series = seriesAll.slice(-historyDays);

  drawLine($('#chart'), series, { color:'#111827' });

  // provenance + delta
  try {
    const last = seriesAll.length ? seriesAll[seriesAll.length - 1].value : null;
    const prev = seriesAll.length > 1 ? seriesAll[seriesAll.length - 2].value : null;
    const delta = (last!=null && prev!=null) ? (last - prev) : null;
    const updated = seriesAll.length ? seriesAll[seriesAll.length - 1].dt : null;

    const asofLine   = document.getElementById('asofLine');
    const methodLine = document.getElementById('methodLine');
    const hashLine   = document.getElementById('hashLine');

    if (asofLine)   asofLine.textContent = `As of: ${updated || '—'}`;
    if (methodLine) methodLine.textContent = `Method: indices_daily (as_of_date/price → USD/lb)`;

    if (hashLine) {
      const enc = new TextEncoder();
      const data = JSON.stringify(seriesAll);
      const buf = await crypto.subtle.digest('SHA-256', enc.encode(data));
      const hash = Array.from(new Uint8Array(buf)).map(b=>b.toString(16).padStart(2,'0')).join('').slice(0,12);
      hashLine.textContent = `Hash: ${hash}`;
    }

    // Update row in universe table
    const all = Array.from(document.querySelectorAll('#universeTbl tbody tr'));
    const rowEl = all.find(r => (r.cells?.[0]?.innerText || '').trim() === sym);
    if (rowEl){
      rowEl.cells[1].innerText = (last!=null ? (+last).toFixed(4) : '-');
      rowEl.cells[2].innerText = (delta!=null ? (delta>0?'+':'') + delta.toFixed(4) : '-');
      rowEl.cells[2].className = (delta>0?'positive':delta<0?'negative':'muted');
      rowEl.cells[3].innerText = (updated || '-');
    }
  } catch {}

  // Forecast overlay (optional)
  const fc = $('#forecastChart');
  if (fc) {
    if (!showForecast) {
      fc.classList.add('hidden');
    } else {
      try {
        const f = await getJSON(`/forecasts/latest?symbol=${encodeURIComponent(sym)}&horizon_days=90`);
        // Expect array of {dt, predicted_price, confidence_low, confidence_high} or similar
        const ci = (f || []).map(r => ({
          dt: r.forecast_date || r.dt,
          low: +(r.confidence_low ?? r.low ?? r.p10 ?? 0),
          high:+(r.confidence_high ?? r.high ?? r.p90 ?? 0)
        })).filter(x=>x.dt);
        const fseries = (f || []).map(r => ({
          dt: r.forecast_date || r.dt,
          value: +(r.predicted_price ?? r.value ?? 0)
        })).filter(x=>x.dt);

        fc.classList.remove('hidden');
        drawLine(fc, fseries, { color:'#2563eb', ci });
      } catch (e) {
        console.warn('forecast overlay unavailable', e);
        fc.classList.add('hidden');
      }
    }
  }
}

// ---------- interactions ----------
document.getElementById('universeTbl')?.addEventListener('click', (e)=>{
  const b = e.target.closest('button[data-view]');
  if(b){ viewSymbol(b.getAttribute('data-view')); }
});

document.querySelectorAll('button[data-days]')?.forEach(b=>{
  b.onclick = ()=>{
    document.querySelectorAll('button[data-days]').forEach(x=>x.setAttribute('aria-pressed','false'));
    b.setAttribute('aria-pressed','true');
    historyDays = +b.dataset.days;
    if(selectedSymbol) viewSymbol(selectedSymbol);
  };
});

document.getElementById('toggleForecast')?.addEventListener('click', (ev)=>{
  showForecast = !showForecast;
  ev.currentTarget.setAttribute('aria-pressed', showForecast ? 'true' : 'false');
  if(selectedSymbol) viewSymbol(selectedSymbol);
});

document.getElementById('exportHistory')?.addEventListener('click', async ()=>{
  if(!selectedSymbol) return toast?.('Select a ticker first.');
  try {
    const data = await getPublicIndexData(selectedSymbol);
    const rows = _arr(data?.history);
    csvDownload(`${selectedSymbol}_history.csv`, rows);
  } catch(e) {
    console.warn('exportHistory failed', e);
    toast?.('Export failed (no data).');
  }
});

document.getElementById('exportForecast')?.addEventListener('click', async ()=>{
  if(!selectedSymbol) return toast?.('Select a ticker first.');
  const rows = await getJSON(`/forecasts/latest?symbol=${encodeURIComponent(selectedSymbol)}&horizon_days=90`);
  csvDownload(`${selectedSymbol}_forecast90.csv`, rows);
});

// public downloads
(function ensureEndpoint(){ window.endpoint = window.endpoint || location.origin; })();
document.getElementById('dlDailyJson')?.addEventListener('click', ()=>{
  window.location = `${window.endpoint}/public/indices/daily.json`;
});
document.getElementById('dlDailyCsv')?.addEventListener('click', ()=>{
  window.location = `${window.endpoint}/public/indices/daily.csv`;
});

// filter + refresh
function filterUniverseRows(){
  const q = ($('#filterInput')?.value || '').trim().toLowerCase();
  document.querySelectorAll('#universeTbl tbody tr[data-sym]').forEach(tr => {
    const sym = (tr.getAttribute('data-sym') || '').toLowerCase();
    tr.style.display = (!q || sym.includes(q)) ? '' : 'none';
  });
}
document.getElementById('filterInput')?.addEventListener('input', filterUniverseRows);
document.getElementById('refreshAll')?.addEventListener('click', loadUniverse);

// init
loadUniverse().then(()=>{
  const first = universe[0]?.symbol;
  if(first) viewSymbol(first);
});
