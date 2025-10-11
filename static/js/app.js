// endpoint + toast + fetch helper + small utils
window.endpoint = window.endpoint || (location.origin.includes('localhost') ? 'http://localhost:8000' : location.origin);
function toast(msg, ms=2800){ const el = document.getElementById('toast'); if(!el){alert(msg);return;} el.textContent = msg; el.setAttribute('data-show','1'); setTimeout(()=>el.removeAttribute('data-show'), ms); }
async function api(path, opts={}) {
  const url = path.startsWith('http') ? path : `${window.endpoint}${path}`;
  const res = await fetch(url, { ...opts, headers: { Accept:'application/json', ...(opts.headers||{}) }});
  if (res.status === 401) { const next = encodeURIComponent(location.pathname + location.search); location.href = `/static/bridge-login.html?next=${next}`; throw new Error('Unauthorized'); }
  if (!res.ok) { let m='Request failed'; try{ m=(await res.clone().json()).detail||m; }catch{ m=await res.text()||m; } toast(m); throw new Error(m); }
  return res;
}
const $ = sel => document.querySelector(sel);
function setBusy(btn, busy){ btn?.setAttribute('data-busy', busy ? '1' : '0'); }
function csvDownload(filename, rows){
  if(!rows?.length){ toast('No data.'); return; }
  const headers = Object.keys(rows[0]||{});
  const csv = [headers.join(',')].concat(rows.map(r=>headers.map(h=>JSON.stringify(r[h] ?? '')).join(','))).join('\n');
  const blob = new Blob([csv], {type:'text/csv'}); const url = URL.createObjectURL(blob);
  const a = document.createElement('a'); a.href = url; a.download = filename; a.click(); URL.revokeObjectURL(url);
}
