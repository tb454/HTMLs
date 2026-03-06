// indices.js - internal BR index dashboard (not public mirror viewer)
(function () {
  'use strict';

  const $ = (sel) => document.querySelector(sel);

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

  function setBusy(btn, busy){ btn?.setAttribute('data-busy', busy ? '1' : '0'); }

  function csvDownload(filename, rows){
    if(!rows?.length){ toast?.('No data.'); return; }
    const headers = Object.keys(rows[0] || {});
    const csv = [headers.join(',')].concat(
      rows.map(r => headers.map(h => JSON.stringify(r[h] ?? '')).join(','))
    ).join('\n');
    const blob = new Blob([csv], { type:'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  }

  function showSkeleton(tbody, cols){
    tbody.innerHTML = '';
    for(let i=0;i<5;i++){
      const tr = document.createElement('tr');
      tr.className = 'skeleton-row';
      tr.innerHTML = Array.from({length:cols},()=>'<td><span class="skeleton"></span></td>').join('');
      tbody.appendChild(tr);
    }
  }

  async function getJSON(path, opts){
    if (typeof window.apiJSON === 'function') return window.apiJSON(path, opts);

    if (typeof window.api === 'function') {
      const r = await window.api(path, opts);
      if (r && typeof r === 'object' && !(r instanceof Response) && !('ok' in r)) return r;
      if (r instanceof Response) {
        if (!r.ok) throw new Error(`HTTP ${r.status} ${r.statusText}`);
        return await r.json();
      }
    }

    const res = await fetch(path, {
      credentials: 'include',
      headers: { 'Accept': 'application/json' },
      ...opts
    });
    if(!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
    return await res.json();
  }

  async function postJSON(path, opts = {}){
    if (typeof window.apiJSON === 'function') {
      return await window.apiJSON(path, { method:'POST', ...opts });
    }

    if (typeof window.api === 'function') {
      const r = await window.api(path, { method:'POST', ...opts });
      if (r instanceof Response) {
        if (!r.ok) throw new Error(`HTTP ${r.status} ${r.statusText}`);
        return await r.json();
      }
      return r;
    }

    const res = await fetch(path, {
      method: 'POST',
      credentials: 'include',
      headers: { 'Accept':'application/json', ...(opts.headers || {}) },
      ...opts
    });
    if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
    return await res.json();
  }

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

    const xPix = t => pad + (W-2*pad) * ((t-minX)/(maxX-minX || 1));
    const yPix = v => H-pad - (H-2*pad) * ((v-y0)/(y1-y0 || 1));

    ctx.strokeStyle = '#e5e7eb';
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(pad, pad);
    ctx.lineTo(pad, H-pad);
    ctx.lineTo(W-pad, H-pad);
    ctx.stroke();

    ctx.fillStyle = '#6b7280';
    ctx.font = '12px system-ui';
    const ticks = 5;
    for(let i=0;i<=ticks;i++){
      const val = y0 + (y1-y0)*i/ticks;
      const y = yPix(val);
      ctx.strokeStyle = '#f3f4f6';
      ctx.beginPath();
      ctx.moveTo(pad,y);
      ctx.lineTo(W-pad,y);
      ctx.stroke();
      ctx.fillText(val.toFixed(4), 6, y+3);
    }

    ctx.strokeStyle = options.color || '#111827';
    ctx.lineWidth = 2;
    ctx.beginPath();
    series.forEach((d,idx)=>{
      const x = xPix(new Date(d.dt).getTime());
      const y = yPix(d.value);
      if(idx===0) ctx.moveTo(x,y); else ctx.lineTo(x,y);
    });
    ctx.stroke();

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
      ctx.closePath();
      ctx.fill();
    }
  }

  let universe = [];
  let selectedSymbol = null;
  let selectedLast = null;
  let selectedUpdated = null;
  let historyDays = 30;
  let showForecast = false;

  function asIsoOrNow(v){
    if (!v) return new Date().toISOString();
    const d = new Date(v);
    return Number.isNaN(d.getTime()) ? new Date().toISOString() : d.toISOString();
  }

  function buildIndexedFormula(sym, refLb, basisTon){
    const ref = Number(refLb);
    const basis = Number(basisTon || 0);
    const left = Number.isFinite(ref) ? `${sym} @ ${ref.toFixed(4)} USD/lb` : `${sym}`;
    if (!Number.isFinite(basis) || basis === 0) return `${left} × 2000`;
    return `${left} × 2000 ${basis >= 0 ? '+' : '-'} ${Math.abs(basis).toFixed(2)} USD/ton`;
  }

  function syncIndexContractCard(){
    const symEl     = document.getElementById('idxRefSymbol');
    const refEl     = document.getElementById('idxRefPrice');
    const basisEl   = document.getElementById('idxBasisUsdTon');
    const priceEl   = document.getElementById('idxPricePerTon');
    const formulaEl = document.getElementById('idxFormula');
    const metaEl    = document.getElementById('indexContractMeta');

    if (symEl) symEl.value = selectedSymbol || '';

    if (metaEl) {
      if (!selectedSymbol) {
        metaEl.textContent = 'Select a BR symbol first.';
      } else {
        metaEl.textContent =
          `Selected: ${selectedSymbol}` +
          (selectedLast != null ? ` · Ref ${Number(selectedLast).toFixed(4)} USD/lb` : '') +
          (selectedUpdated ? ` · As of ${selectedUpdated}` : '');
      }
    }

    if (selectedLast != null && refEl && !refEl.dataset.userEdited) {
      refEl.value = Number(selectedLast).toFixed(4);
    }

    const ref = Number(refEl?.value);
    const basis = Number(basisEl?.value || 0);

    if (Number.isFinite(ref) && priceEl) {
      priceEl.value = (ref * 2000 + (Number.isFinite(basis) ? basis : 0)).toFixed(2);
    }

    if (formulaEl) {
      formulaEl.value = buildIndexedFormula(selectedSymbol || '', refEl?.value, basisEl?.value);
    }
  }

  window.addEventListener('DOMContentLoaded', async () => {
    try {
      await (window.api ? window.api('/health') : fetch('/health'));
      const b = document.querySelector('[data-health-badge]');
      if (b) {
        b.textContent = '● Live';
        b.setAttribute('data-ok','1');
      }
    } catch (e) {
      console.error('health check failed', e);
    }
  });

  async function getLastFromHistory(sym){
    try{
      const histRows = await getJSON(`/indices/history?symbol=${encodeURIComponent(sym)}`);
      const seriesAll = (histRows || [])
        .filter(r => r && (r.dt || r.as_of_date || r.date))
        .map(r => ({
          dt: (r.dt || r.as_of_date || r.date),
          value: +(r.close_price ?? r.close ?? r.value ?? 0)
        }))
        .filter(p => p.dt && Number.isFinite(p.value))
        .sort((a,b)=> new Date(a.dt) - new Date(b.dt));

      const n = seriesAll.length;
      const last = n ? seriesAll[n-1].value : null;
      const prev = n > 1 ? seriesAll[n-2].value : null;
      const delta = (last!=null && prev!=null) ? (last - prev) : null;
      const updated = n ? seriesAll[n-1].dt : null;

      return { last, delta, updated };
    } catch(e){
      console.warn('getLastFromHistory failed', sym, e);
      return { last:null, delta:null, updated:null };
    }
  }

  async function loadUniverse(){
    const tbody = $('#universeTbl tbody');
    showSkeleton(tbody, 5);

    try{
      const rows = await getJSON('/indices/universe');
      const arr = Array.isArray(rows) ? rows : (rows?.items || rows?.rows || rows?.data || []);
      const defs = arr.filter(r => r && r.symbol && (r.enabled == null || r.enabled === true));

      universe = defs.map(d => ({
        symbol: d.symbol,
        method: d.method,
        factor: d.factor,
        base_symbol: d.base_symbol
      }));

      const q = ($('#filterInput')?.value || '').trim().toLowerCase();
      const filter = (row) => !q || (row.symbol || '').toLowerCase().includes(q);

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

      const rowsToFill = Array.from(tbody.querySelectorAll('tr[data-sym]'));
      for (const tr of rowsToFill){
        const sym = tr.getAttribute('data-sym');
        const { last, delta, updated } = await getLastFromHistory(sym);

        tr.cells[1].innerText = (last!=null ? (+last).toFixed(4) : '-');
        tr.cells[2].innerText = (delta!=null ? (delta>0?'+':'') + (+delta).toFixed(4) : '-');
        tr.cells[2].className = (delta>0?'positive':delta<0?'negative':'muted');
        tr.cells[3].innerText = (updated || '-');
      }

      const note = document.getElementById('detailNote');
      if (note){
        note.textContent =
          'BR-Index mode. USD/lb. Universe from bridge_index_definitions; history from bridge_index_history.';
      }

    } catch(e){
      console.error(e);
      tbody.innerHTML = `<tr><td colspan="5" class="muted">Failed to load index universe.</td></tr>`;
      toast?.('Could not load index universe');
    }
  }

  async function viewSymbol(sym){
    selectedSymbol = sym;

    const title = $('#detailTitle');
    if (title) title.textContent = sym;

    const note = $('#detailNote');
    if (note) note.textContent = 'BR-Index history (internal). USD/lb.';

    const histRows = await getJSON(`/indices/history?symbol=${encodeURIComponent(sym)}`);

    const seriesAll = (histRows || [])
      .filter(r => r && (r.dt || r.as_of_date || r.date))
      .map(r => ({
        dt: (r.dt || r.as_of_date || r.date),
        value: +(r.close_price ?? r.close ?? r.value ?? 0)
      }))
      .filter(p => p.dt && Number.isFinite(p.value))
      .sort((a,b)=> new Date(a.dt) - new Date(b.dt));

    const series = seriesAll.slice(-historyDays);
    drawLine($('#chart'), series, { color:'#111827' });

    try {
      const last = seriesAll.length ? seriesAll[seriesAll.length - 1].value : null;
      const prev = seriesAll.length > 1 ? seriesAll[seriesAll.length - 2].value : null;
      const delta = (last!=null && prev!=null) ? (last - prev) : null;
      const updated = seriesAll.length ? seriesAll[seriesAll.length - 1].dt : null;

      const asofLine   = document.getElementById('asofLine');
      const methodLine = document.getElementById('methodLine');
      const hashLine   = document.getElementById('hashLine');

      if (asofLine)   asofLine.textContent = `As of: ${updated || '—'}`;
      if (methodLine) methodLine.textContent = `Method: bridge_index_history (dt/close_price)`;

      if (hashLine) {
        const enc = new TextEncoder();
        const data = JSON.stringify(seriesAll);
        const buf = await crypto.subtle.digest('SHA-256', enc.encode(data));
        const hash = Array.from(new Uint8Array(buf)).map(b=>b.toString(16).padStart(2,'0')).join('').slice(0,12);
        hashLine.textContent = `Hash: ${hash}`;
      }

      const all = Array.from(document.querySelectorAll('#universeTbl tbody tr'));
      const rowEl = all.find(r => (r.cells?.[0]?.innerText || '').trim() === sym);
      if (rowEl){
        rowEl.cells[1].innerText = (last!=null ? (+last).toFixed(4) : '-');
        rowEl.cells[2].innerText = (delta!=null ? (delta>0?'+':'') + delta.toFixed(4) : '-');
        rowEl.cells[2].className = (delta>0?'positive':delta<0?'negative':'muted');
        rowEl.cells[3].innerText = (updated || '-');
      }

      selectedLast = last;
      selectedUpdated = updated || null;
      syncIndexContractCard();
    } catch {}

    const fc = $('#forecastChart');
    if (fc) {
      if (!showForecast) {
        fc.classList.add('hidden');
      } else {
        try {
          const f = await getJSON(`/forecasts/latest?symbol=${encodeURIComponent(sym)}&horizon_days=90`);
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

  document.getElementById('exportUniverse')?.addEventListener('click', async ()=>{
    const rows = await getJSON('/indices/universe');
    const arr = Array.isArray(rows) ? rows : (rows?.items || rows?.rows || rows?.data || []);
    const out = arr.map(r => ({
      symbol: r.symbol,
      method: r.method,
      factor: r.factor,
      base_symbol: r.base_symbol
    }));
    csvDownload('bridge_index_universe.csv', out);
  });

  document.getElementById('exportHistory')?.addEventListener('click', async ()=>{
    if(!selectedSymbol) return toast?.('Select a ticker first.');
    const rows = await getJSON(`/indices/history?symbol=${encodeURIComponent(selectedSymbol)}`);
    csvDownload(`${selectedSymbol}_history.csv`, rows);
  });

  document.getElementById('exportForecast')?.addEventListener('click', async ()=>{
    if(!selectedSymbol) return toast?.('Select a ticker first.');
    const rows = await getJSON(`/forecasts/latest?symbol=${encodeURIComponent(selectedSymbol)}&horizon_days=90`);
    csvDownload(`${selectedSymbol}_forecast90.csv`, rows);
  });

  (function ensureEndpoint(){ window.endpoint = window.endpoint || location.origin; })();

  document.getElementById('dlDailyJson')?.addEventListener('click', ()=>{
    window.location = `${window.endpoint}/public/indices/daily.json`;
  });

  document.getElementById('dlDailyCsv')?.addEventListener('click', ()=>{
    window.location = `${window.endpoint}/public/indices/daily.csv`;
  });

  document.getElementById('pullRefs')?.addEventListener('click', async (e)=>{
    const btn = e.currentTarget;
    if (btn?.dataset.busy === '1') return;
    setBusy(btn, true);
    try {
      await postJSON('/reference_prices/pull_now_all');
      toast?.('Reference prices pulled.');
      if (selectedSymbol) viewSymbol(selectedSymbol);
    } catch (err) {
      console.warn('pullRefs failed', err);
      toast?.('Pull refs failed.');
    } finally {
      setBusy(btn, false);
    }
  });

  document.getElementById('runIndices')?.addEventListener('click', async (e)=>{
    const btn = e.currentTarget;
    if (btn?.dataset.busy === '1') return;
    setBusy(btn, true);
    try {
      await postJSON('/indices/run');
      toast?.('Indices built.');
      await loadUniverse();
      if (selectedSymbol) viewSymbol(selectedSymbol);
    } catch (err) {
      console.warn('runIndices failed', err);
      toast?.('Build indices failed.');
    } finally {
      setBusy(btn, false);
    }
  });

  document.getElementById('runForecasts')?.addEventListener('click', async (e)=>{
    const btn = e.currentTarget;
    if (btn?.dataset.busy === '1') return;
    setBusy(btn, true);
    try {
      await postJSON('/forecasts/run');
      toast?.('Forecasts run complete.');
      if (selectedSymbol) viewSymbol(selectedSymbol);
    } catch (err) {
      console.warn('runForecasts failed', err);
      toast?.('Run forecasts failed.');
    } finally {
      setBusy(btn, false);
    }
  });

  document.getElementById('copyIndexIntoPrice')?.addEventListener('click', ()=>{
    syncIndexContractCard();
    toast?.('Selected index copied into pricing.');
  });

  document.getElementById('idxRefPrice')?.addEventListener('input', (e)=>{
    e.currentTarget.dataset.userEdited = '1';
    syncIndexContractCard();
  });

  document.getElementById('idxBasisUsdTon')?.addEventListener('input', syncIndexContractCard);

  document.getElementById('createIndexedContract')?.addEventListener('click', async ()=>{
    const seller = (document.getElementById('idxSeller')?.value || '').trim();
    const buyer  = (document.getElementById('idxBuyer')?.value || '').trim();
    const material = (document.getElementById('idxMaterial')?.value || '').trim();
    const weight_tons = Number(document.getElementById('idxTons')?.value);
    const price_per_ton = Number(document.getElementById('idxPricePerTon')?.value);
    const reference_price = Number(document.getElementById('idxRefPrice')?.value);
    const pricing_formula = (document.getElementById('idxFormula')?.value || '').trim();
    const basis = Number(document.getElementById('idxBasisUsdTon')?.value || 0);

    if (!selectedSymbol) return toast?.('Select a BR symbol first.');
    if (!seller || !buyer || !material) return toast?.('Fill seller, buyer, and material.');
    if (!Number.isFinite(weight_tons) || weight_tons <= 0) return toast?.('Enter valid tons.');
    if (!Number.isFinite(price_per_ton) || price_per_ton <= 0) return toast?.('Enter valid $/ton.');
    if (!Number.isFinite(reference_price) || reference_price <= 0) return toast?.('Missing reference price.');

    const body = {
      seller,
      buyer,
      material,
      weight_tons,
      price_per_ton,
      currency: 'USD',
      pricing_formula: pricing_formula || buildIndexedFormula(selectedSymbol, reference_price, basis),
      reference_symbol: selectedSymbol,
      reference_price,
      reference_source: 'BR-Index internal',
      reference_timestamp: asIsoOrNow(selectedUpdated)
    };

    try {
      await postJSON('/contracts', {
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': (window.crypto?.randomUUID?.() || String(Date.now()))
        },
        body: JSON.stringify(body)
      });
      toast?.(`Contract created from ${selectedSymbol}.`);
    } catch (e) {
      console.warn('createIndexedContract failed', e);
      toast?.(e?.message || 'Indexed contract create failed');
    }
  });

  function filterUniverseRows(){
    const q = ($('#filterInput')?.value || '').trim().toLowerCase();
    document.querySelectorAll('#universeTbl tbody tr[data-sym]').forEach(tr => {
      const sym = (tr.getAttribute('data-sym') || '').toLowerCase();
      tr.style.display = (!q || sym.includes(q)) ? '' : 'none';
    });
  }

  document.getElementById('filterInput')?.addEventListener('input', filterUniverseRows);
  document.getElementById('refreshAll')?.addEventListener('click', loadUniverse);

  loadUniverse().then(()=>{
    const first = universe[0]?.symbol;
    if(first) viewSymbol(first);
  });

})();