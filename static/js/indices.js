// Health badge using api() from app.js
window.addEventListener('DOMContentLoaded', async () => {
  try {
    await api('/health');  // if this throws, we never mark Live
    const b = document.querySelector('[data-health-badge]');
    if (b) {
      b.textContent = '● Live';
      b.setAttribute('data-ok', '1');
    }
  } catch (e) {
    console.error('health check failed', e);
  }
});

// Shortcuts
function setBusy(btn, busy){ btn?.setAttribute('data-busy', busy ? '1' : '0'); }

function csvDownload(filename, rows){
  if(!rows?.length){ toast('No data.'); return; }
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
    ctx.strokeStyle = '#f3f4f6'; ctx.beginPath(); ctx.moveTo(pad,y); ctx.lineTo(W-pad,y); ctx.stroke();
    ctx.fillText(val.toFixed(3), 6, y+3);
  }

  // main series
  ctx.strokeStyle = options.color || '#111827'; ctx.lineWidth=2;
  ctx.beginPath();
  series.forEach((d,idx)=>{ const x=xPix(new Date(d.dt).getTime()), y=yPix(d.value); if(idx===0) ctx.moveTo(x,y); else ctx.lineTo(x,y); });
  ctx.stroke();

  // CI band (forecast)
  if(options.ci){
    const lows  = options.ci.map(d=>({dt:d.dt, value:d.low}));
    const highs = options.ci.map(d=>({dt:d.dt, value:d.high}));
    ctx.fillStyle = 'rgba(37,99,235,.12)';
    ctx.beginPath();
    highs.forEach((d,idx)=>{ const x=xPix(new Date(d.dt).getTime()), y=yPix(d.value); if(idx===0) ctx.moveTo(x,y); else ctx.lineTo(x,y); });
    for(let i=lows.length-1;i>=0;i--){ const d=lows[i]; const x=xPix(new Date(d.dt).getTime()), y=yPix(d.value); ctx.lineTo(x,y); }
    ctx.closePath(); ctx.fill();
  }
}

function sanitizeUniverseRows(rows){
  return (rows||[]).map(r=>{
    const o = {...r};
    delete o.method;
    delete o.factor;
    delete o.base_symbol;
    delete o.reference_source;
    delete o.reference_symbol;
    delete o.source;
    return o;
  });
}
function bleachExchangeText(root=document){
  const re = /(COMEX|LME|CME)/ig;
  root.querySelectorAll('#universeTbl td, #universeTbl th, #detailNote, #methodLine, [title], [aria-label], [data-title]').forEach(el=>{
    ['title','aria-label','data-title'].forEach(a=>{
      const v = el.getAttribute?.(a); if(v && re.test(v)) el.setAttribute(a, v.replace(re,'BR-Index'));
    });
    if (re.test(el.textContent)) el.textContent = el.textContent.replace(re,'BR-Index');
  });
}

// state
let universe = [];
let selectedSymbol = null;
let historyDays = 30;
let showForecast = false;

async function getJSON(url) {
  const data = await apiJSON(url);
  return (data == null ? [] : data);
}

async function loadUniverse(){
  const tbody = $('#universeTbl tbody');
  showSkeleton(tbody, 5);

  try{
    // ✅ BR-Index universe (vendor quotes only)
    const rows = await getJSON('/api/br-index/current');

    universe = (rows || []).map(r => {
      const last = (r.px_median ?? r.px_avg ?? r.px_min ?? null);
      return {
        symbol: r.instrument_code,
        last:   (last != null ? +last : null),
      };
    });

    // filter
    const q = ($('#filterInput')?.value || '').trim().toLowerCase();
    const filter = (row) => !q || (row.symbol||'').toLowerCase().includes(q);

    tbody.innerHTML = '';
    for (const row of universe.filter(filter)){
      const sym = row.symbol;

      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td><b>${sym}</b></td>
        <td>${row.last!=null ? (+row.last).toFixed(4) : '-'}</td>
        <td class="muted">-</td>
        <td class="muted">-</td>
        <td><button class="btn" data-view="${sym}">View</button></td>
      `;
      tbody.appendChild(tr);
    }

    if (!tbody.children.length){
      tbody.innerHTML = `<tr><td colspan="5" class="muted">No rows match your filter.</td></tr>`;
    }

    // BR-only note
    const note = document.getElementById('detailNote');
    if (note){
      note.textContent =
        'BR-Index (vendor quotes only). USD/lb. No CME/LME shown. Loaded ' +
        new Date().toISOString().slice(0,19).replace('T',' ') + 'Z';
    }

    // Disable COMEX/LME-facing buttons + forecast UI in this dashboard
    try {
      ['pullRefs','runIndices','runForecasts','toggleForecast','dlDailyJson','dlDailyCsv']
        .forEach(id => {
          const el = document.getElementById(id);
          if (el) {
            el.disabled = true;
            el.style.opacity = '0.45';
            el.title = 'Disabled in BR-Index mode (vendor quotes only).';
          }
        });
      showForecast = false;
      const fc = document.getElementById('forecastChart');
      if (fc) fc.classList.add('hidden');
    } catch {}

    bleachExchangeText(document);

  } catch(e){
    console.error(e);
    tbody.innerHTML = `<tr><td colspan="5" class="muted">Failed to load BR-Index universe.</td></tr>`;
    toast('Could not load BR-Index universe');
  }
}

async function viewSymbol(sym){
  selectedSymbol = sym;

  const title = $('#detailTitle'); if (title) title.textContent = sym;
  const note  = $('#detailNote');
  if (note) note.textContent = 'BR-Index history (vendor quotes). USD/lb.';

  // ✅ BR-Index history (vendor quotes only)
  const histRows = await getJSON(`/api/br-index/history?instrument=${encodeURIComponent(sym)}&days=${historyDays}`);

  const series = (histRows || [])
    .filter(r => r && r.sheet_date)
    .map(r => ({
      dt: r.sheet_date,
      value: +(r.px_median ?? r.px_avg ?? r.px_min ?? 0)
    }));

  drawLine($('#chart'), series, {color:'#111827'});

  // Update provenance + delta
  try {
    const last = series.length ? series[series.length - 1].value : null;
    const prev = series.length > 1 ? series[series.length - 2].value : null;
    const delta = (last!=null && prev!=null) ? (last - prev) : null;
    const updated = series.length ? series[series.length - 1].dt : null;

    const asofLine   = document.getElementById('asofLine');
    const methodLine = document.getElementById('methodLine');
    const hashLine   = document.getElementById('hashLine');

    if (asofLine)   asofLine.textContent = `As of: ${updated || '—'} (sheet_date)`;
    if (methodLine) methodLine.textContent = `Method: vendor_quotes → vendor_material_map → scrap_instrument`;

    if (hashLine) {
      const enc = new TextEncoder();
      const data = JSON.stringify(series);
      const buf = await crypto.subtle.digest('SHA-256', enc.encode(data));
      const hash = Array.from(new Uint8Array(buf)).map(b=>b.toString(16).padStart(2,'0')).join('').slice(0,12);
      hashLine.textContent = `Hash: ${hash}`;
    }

    // Update row display in the table (nice polish)
    const all = Array.from(document.querySelectorAll('#universeTbl tbody tr'));
    const rowEl = all.find(r => (r.cells?.[0]?.innerText || '').trim() === sym);
    if (rowEl){
      rowEl.cells[1].innerText = (last!=null ? (+last).toFixed(4) : '-');
      rowEl.cells[2].innerText = (delta!=null ? (delta>0?'+':'') + delta.toFixed(4) : '-');
      rowEl.cells[2].className = (delta>0?'positive':delta<0?'negative':'muted');
      rowEl.cells[3].innerText = (updated || '-');
    }
  } catch {}

  // BR-only dashboard: no forecast overlay
  const fc = $('#forecastChart');
  if (fc) fc.classList.add('hidden');
}

// interactions
document.getElementById('universeTbl')?.addEventListener('click', (e)=>{
  const b = e.target.closest('button[data-view]');
  if(b){ viewSymbol(b.getAttribute('data-view')); }
});

// Days toggle
document.querySelectorAll('button[data-days]')?.forEach(b=>{
  b.onclick = ()=>{
    document.querySelectorAll('button[data-days]').forEach(x=>x.setAttribute('aria-pressed','false'));
    b.setAttribute('aria-pressed','true');
    historyDays = +b.dataset.days; if(selectedSymbol) viewSymbol(selectedSymbol);
  };
});

// Forecast toggle
document.getElementById('toggleForecast')?.addEventListener('click', (ev)=>{
  showForecast = !showForecast;
  ev.currentTarget.setAttribute('aria-pressed', showForecast ? 'true' : 'false');
  if(selectedSymbol) viewSymbol(selectedSymbol);
});

// exports
document.getElementById('exportUniverse')?.addEventListener('click', async ()=>{
  const rows = await getJSON('/api/br-index/current');
  const out = (rows||[]).map(r=>({symbol:r.instrument_code}));
  csvDownload('br_index_universe.csv', out);
});

document.getElementById('exportHistory')?.addEventListener('click', async ()=>{
  if(!selectedSymbol) return toast('Select a ticker first.');
  const rows = await getJSON(`/api/br-index/history?instrument=${encodeURIComponent(selectedSymbol)}&days=365`);
  csvDownload(`${selectedSymbol}_history.csv`, rows);
});

document.getElementById('exportForecast')?.addEventListener('click', async ()=>{
  if(!selectedSymbol) return toast('Select a ticker first.');
  const rows = await getJSON(`/forecasts/latest?symbol=${encodeURIComponent(selectedSymbol)}&horizon_days=90`);
  csvDownload(`${selectedSymbol}_forecast90.csv`, rows);
});

// public downloads (ensure window.endpoint exists via app.js; fall back if not)
(function ensureEndpoint(){ window.endpoint = window.endpoint || location.origin; })();
document.getElementById('dlDailyJson')?.addEventListener('click', ()=>{
  window.location = `${window.endpoint}/public/indices/daily.json`;
});
document.getElementById('dlDailyCsv')?.addEventListener('click', ()=>{
  window.location = `${window.endpoint}/public/indices/daily.csv`;
});

// admin buttons
function onceBusy(btn, fn){
  if(btn.dataset.busy === "1") return;
  setBusy(btn, true);
  Promise.resolve(fn()).finally(()=> setBusy(btn, false));
}
document.getElementById('runIndices')?.addEventListener('click', e =>
  onceBusy(e.target, () => api('/indices/run',{method:'POST'}).then(()=> { toast('✓ Indices built'); if(selectedSymbol) viewSymbol(selectedSymbol); }))
);
document.getElementById('runForecasts')?.addEventListener('click', e =>
  onceBusy(e.target, () => api('/forecasts/run',{method:'POST'}).then(()=> { toast('✓ Forecasts complete'); if(selectedSymbol) viewSymbol(selectedSymbol); }))
);

// filter
document.getElementById('filterInput')?.addEventListener('input', ()=>{ loadUniverse(); });

// init
document.getElementById('refreshAll')?.addEventListener('click', loadUniverse);
loadUniverse().then(()=>{
  bleachExchangeText(document);
  const first = universe[0]?.symbol; if(first) viewSymbol(first);
});

// simple sortable columns
(function makeSortable(){
  const table = document.getElementById('universeTbl');
  if (!table) return;
  const ths = table.querySelectorAll('thead th');
  ths.forEach((th, idx) => {
    th.style.cursor='pointer'; th.title='Click to sort';
    let dir = 1;
    th.addEventListener('click', ()=>{
      const rows = Array.from(table.tBodies[0].rows);
      const isNum = rows.every(r => !isNaN(parseFloat(r.cells[idx].innerText.replace(/[,$]/g,''))) || r.cells[idx].innerText.trim()==='-');
      rows.sort((a,b)=>{
        const av = a.cells[idx].innerText.trim(), bv = b.cells[idx].innerText.trim();
        if (av==='-' || bv==='-') return av==='-' && bv==='-' ? 0 : av==='-' ? 1 : -1;
        if (isNum) return (parseFloat(av.replace(/[,$+]/g,'')) - parseFloat(bv.replace(/[,$+]/g,''))) * dir;
        return av.localeCompare(bv, undefined, {numeric:true, sensitivity:'base'}) * dir;
      });
      dir *= -1; rows.forEach(r=>table.tBodies[0].appendChild(r));
    });
  });
})();

// Pull reference prices now
document.getElementById('pullRefs')?.addEventListener('click', e =>
  onceBusy(e.target, () =>
    api('/reference_prices/pull_now_all', { method:'POST' })
      .then(() => { toast('✓ Reference prices pulled'); if (selectedSymbol) viewSymbol(selectedSymbol); })
  )
);
