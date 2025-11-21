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
})();

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

// state
let universe = [];
let selectedSymbol = null;
let historyDays = 30;
let showForecast = false;

async function getJSON(url){  
  return api(url);
}

async function loadUniverse(){
  const tbody = $('#universeTbl tbody');
  showSkeleton(tbody, 8);
  try{
    universe = await getJSON('/indices/universe?page=1&page_size=500');
    const note = document.getElementById('detailNote');
    if (note){
      note.textContent = 'History (USD/lb); toggle forecast overlay to view 7/30/90d. Universe loaded ' +
        new Date().toISOString().slice(0,19).replace('T',' ') + 'Z';
    }
    tbody.innerHTML = '';

    // apply filter if any
    const q = ($('#filterInput')?.value || '').trim().toLowerCase();
    const filter = (row) => {
      if(!q) return true;
      const sym = (row.symbol||'').toLowerCase();
      const meth = (row.method||'').toLowerCase();
      return sym.includes(q) || meth.includes(q);
    };

    for (const row of universe.filter(filter)){
      const sym = row.symbol;
      let last=null, prev=null, updated=null;
      try{
        const hist = await getJSON(`/indices/history?symbol=${encodeURIComponent(sym)}`);
        if(hist.length){
          last = hist[hist.length-1]?.close_price;
          if(hist.length>1) prev = hist[hist.length-2]?.close_price;
          updated = hist[hist.length-1]?.dt;
        }
      }catch(e){}
      const delta = (last!=null && prev!=null) ? (last - prev) : null;
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td><b>${sym}</b></td>
        <td>${row.method}</td>
        <td>${(+row.factor).toFixed(4)}</td>
        <td>${row.base_symbol}</td>
        <td>${last!=null ? (+last).toFixed(4) : '-'}</td>
        <td class="${delta>0?'positive':delta<0?'negative':''}">${delta!=null ? (delta>0?'+':'')+delta.toFixed(4) : '-'}</td>
        <td class="muted">${updated || '-'}</td>
        <td><button class="btn" data-view="${sym}">View</button></td>
      `;
      tbody.appendChild(tr);
    }
    if (!tbody.children.length){
      tbody.innerHTML = `<tr><td colspan="8" class="muted">No rows match your filter.</td></tr>`;
    }
  }catch(e){
    tbody.innerHTML = `<tr><td colspan="8" class="muted">Failed to load universe.</td></tr>`;
    toast('Could not load index universe');
  }
}

async function viewSymbol(sym){
  selectedSymbol = sym;
  const title = $('#detailTitle'); if (title) title.textContent = sym;
  const note  = $('#detailNote');  if (note)  note.textContent = 'History (USD/lb); toggle forecast overlay to view 7/30/90d.';

  // history for chart
  const histAll = await getJSON(`/indices/history?symbol=${encodeURIComponent(sym)}`);
  const trim = histAll.slice(-historyDays).map(r=>({dt:r.dt, value:+r.close_price}));
  drawLine($('#chart'), trim, {color:'#111827'});

  // forecast overlay
  if(showForecast){
    const fc = await getJSON(`/forecasts/latest?symbol=${encodeURIComponent(sym)}&horizon_days=90`);
    const series = fc.map(r=>({dt:r.forecast_date, value:+r.predicted_price}));
    const ci = fc.map(r=>({dt:r.forecast_date, low:+(r.conf_low ?? r.predicted_price), high:+(r.conf_high ?? r.predicted_price)}));
    $('#forecastChart').classList.remove('hidden');   // was style.display = ''
    drawLine($('#forecastChart'), series, {color:'#2563eb', ci});
  } else {
    $('#forecastChart').classList.add('hidden');      // was style.display = 'none'
  }

  // provenance (as-of, method, integrity hash)
  try {
    const latest = histAll[histAll.length-1];
    const asof = latest ? latest.dt : null;
    const uniRow = (universe || []).find(u => u.symbol === sym);
    const method = uniRow ? uniRow.method : '—';

    const enc = new TextEncoder();
    const data = JSON.stringify(histAll.slice(-historyDays));
    const buf = await crypto.subtle.digest('SHA-256', enc.encode(data));
    const hash = Array.from(new Uint8Array(buf)).map(b=>b.toString(16).padStart(2,'0')).join('').slice(0,12);

    const asofLine   = document.getElementById('asofLine');
    const methodLine = document.getElementById('methodLine');
    const hashLine   = document.getElementById('hashLine');
    if (asofLine)   asofLine.textContent   = `As of: ${asof || '—'} (UTC)`;
    if (methodLine) methodLine.textContent = `Method: ${method}`;
    if (hashLine)   hashLine.textContent   = `Hash: ${hash}`;
  } catch {}
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
document.getElementById('exportUniverse')?.addEventListener('click', ()=>{
  const rows = universe.map(r=>({symbol:r.symbol, method:r.method, factor:r.factor, base_symbol:r.base_symbol}));
  csvDownload('bridge_index_universe.csv', rows);
});
document.getElementById('exportHistory')?.addEventListener('click', async ()=>{
  if(!selectedSymbol) return toast('Select a ticker first.');
  const rows = await getJSON(`/indices/history?symbol=${encodeURIComponent(selectedSymbol)}`);
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
