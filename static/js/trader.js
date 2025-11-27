// /static/js/trader.js
// Trader page: BR-Index chart, quotes, order book, ticket, positions & RFQs.

(function () {
  // helpers
  function _cookie(name) {
    const m = document.cookie.match(
      new RegExp('(?:^|; )' + name.replace(/([.*+?^${}()|[\\]\\])/g, '\\$1') + '=([^;]*)')
    );
    return m ? decodeURIComponent(m[1]) : '';
  }

  async function api(path, opts = {}) {
    const url = path.startsWith('http') ? path : path; // relative → same origin
    const method = (opts.method || 'GET').toUpperCase();
    const headers = { Accept: 'application/json', ...(opts.headers || {}) };

    // CSRF for unsafe methods
    if (!/^(GET|HEAD|OPTIONS)$/.test(method)) {
      headers['X-CSRF'] = _cookie('XSRF-TOKEN') || headers['X-CSRF'];
    }

    const res = await fetch(url, {
      credentials: 'include',
      ...opts,
      headers
    });

    if (res.status === 401) {
      const next = encodeURIComponent(location.pathname + location.search);
      location.href = `/static/bridge-login.html?next=${next}`;
      throw new Error('Unauthorized');
    }

    if (!res.ok) {
      let msg = 'Request failed';
      try {
        msg = (await res.clone().json()).detail || msg;
      } catch {
        try {
          msg = (await res.clone().text()) || msg;
        } catch (_) {}
      }
      throw new Error(msg);
    }

    try {
      return await res.json();
    } catch {
      return {};
    }
  }

  const symbolSel = document.getElementById('symbolSel');
  const limitSel = document.getElementById('limitSel');
  const loadBtn = document.getElementById('loadBtn');
  const matInput = document.getElementById('matInput');
  const quoteBtn = document.getElementById('quoteBtn');
  const quoteOut = document.getElementById('quoteOut');
  const quoteErr = document.getElementById('quoteErr');
  const idxMeta = document.getElementById('idxMeta');
  const chartCanvas = document.getElementById('idxChart');
  const ctx = chartCanvas ? chartCanvas.getContext('2d') : null;

  let chart;
  // Order ticket elements
  const ordSymbol = document.getElementById('ordSymbol');
  const ordSide = document.getElementById('ordSide');
  const ordTif = document.getElementById('ordTif');
  const ordPrice = document.getElementById('ordPrice');
  const ordQty = document.getElementById('ordQty');
  const ordMsg = document.getElementById('ordMsg');
  const ordSubmitBtn = document.getElementById('ordSubmitBtn');

  function setOrdMsg(msg, ok) {
    if (!ordMsg) return;
    ordMsg.style.color = ok ? '#0a7b34' : '#b00020';
    ordMsg.textContent = msg || '';
  }

  let ordersLoaded = false;
  let positionsLoaded = false;
  let rfqsLoaded = false;

  function drawChart(points) {
    if (!ctx) return;
    const labels = points.map((p) => p.ts || p.time || p.date || p.dt || '');
    const data = points.map((p) => +(p.price ?? p.close_price ?? 0));

    if (chart) {
      chart.destroy();
    }
    chart = new Chart(ctx, {
      type: 'line',
      data: { labels, datasets: [{ label: 'Price', data }] },
      options: { responsive: true, animation: false, scales: { y: { beginAtZero: false } } }
    });
  }

  async function loadUniverse() {
    if (!symbolSel) return;
    try {
      const syms = await api('/indices/universe');
      symbolSel.innerHTML = (syms || [])
        .map((row) => {
          const sym = typeof row === 'string' ? row : row.symbol;
          return `<option value="${sym}">${sym}</option>`;
        })
        .join('');
    } catch (e) {
      console.error(e);
      symbolSel.innerHTML = `<option disabled>Failed to load</option>`;
    }
  }

  async function loadHistory() {
    if (!symbolSel || !limitSel || !idxMeta) return;
    const symbol = symbolSel.value;
    const limit = +limitSel.value || 500;
    idxMeta.textContent = '';
    try {
      const rows = await api(
        `/indices/history?symbol=${encodeURIComponent(symbol)}&limit=${limit}`
      );
      const data = (rows || []).slice().reverse(); // ascending for chart aesthetics
      drawChart(data);
      if (rows && rows.length) {
        const last = rows[0];
        const px = last.close_price ?? last.price;
        const ts = last.dt ?? last.date ?? last.ts ?? '';
        idxMeta.textContent = `Latest: ${px} @ ${ts}`;
      } else {
        idxMeta.textContent = 'No data for this symbol yet.';
      }
    } catch (e) {
      console.error(e);
      idxMeta.textContent = 'Failed to load history.';
    }
  }

  async function getQuote() {
    if (!matInput || !quoteOut || !quoteErr) return;
    quoteOut.textContent = '';
    quoteErr.textContent = '';
    const material = (matInput.value || '').trim();
    if (!material) {
      quoteErr.textContent = 'Enter a material name.';
      return;
    }
    try {
      const q = await api(
        `/pricing/quote?category=${encodeURIComponent('Misc Product')}` +
          `&material=${encodeURIComponent(material)}`
      );
      const px = Number(q.price_per_lb);
      quoteOut.textContent = `${q.material}: $${px.toFixed(4)} per lb (source: ${q.source})`;
    } catch (e) {
      quoteErr.textContent =
        'No quote found (try a different material or ensure reference/vendor data is loaded).';
    }
  }

  async function loadBook() {
    const symbolRootInput = document.getElementById('bookSymbolRoot');
    const symRoot = (symbolRootInput && symbolRootInput.value || '').trim();
    const fallback = (symbolSel && symbolSel.value || '').trim();
    const sym = symRoot || fallback;
    if (!sym) return;
    try {
      const book = await api(`/trader/book?symbol=${encodeURIComponent(sym)}`);
      const bidsBody = document.querySelector('#bookBids tbody');
      const asksBody = document.querySelector('#bookAsks tbody');
      function renderSide(rows, body) {
        if (!body) return;
        if (!rows || !rows.length) {
          body.innerHTML = `<tr><td colspan="2" style="opacity:.6; padding:4px;">No depth</td></tr>`;
          return;
        }
        body.innerHTML = rows
          .map(
            (r) => `
            <tr>
              <td>${r.price}</td>
              <td style="text-align:right;">${r.qty_lots}</td>
            </tr>
          `
          )
          .join('');
      }
      renderSide(book.bids || [], bidsBody);
      renderSide(book.asks || [], asksBody);
    } catch (e) {
      console.error(e);
    }
  }

  async function submitOrder() {
    if (!ordQty) return;

    const symFromTicket = (ordSymbol && ordSymbol.value || '').trim();
    const symFromBook = (document.getElementById('bookSymbolRoot')?.value || '').trim();
    const sym = symFromTicket || symFromBook || (symbolSel && symbolSel.value || '').trim();

    const side = (ordSide && ordSide.value) || 'buy';
    const tif = (ordTif && ordTif.value) || 'day';
    const qty = parseFloat(ordQty.value || '0');

    if (!sym) {
      setOrdMsg('Enter a symbol (e.g., CU-SHRED-1M).', false);
      return;
    }
    if (!qty || qty <= 0) {
      setOrdMsg('Qty must be > 0.', false);
      return;
    }

    const priceRaw = ordPrice && ordPrice.value;
    const body = {
      symbol: sym,
      side: side,
      qty_lots: qty,
      tif: tif
    };
    if (priceRaw !== '' && priceRaw != null) {
      body.price = Number(priceRaw);
    }

    try {
      setOrdMsg('Submitting…', true);

      const j = await api('/clob/orders', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key':
            (crypto.randomUUID && crypto.randomUUID()) || String(Date.now())
        },
        body: JSON.stringify(body)
      });

      const oid = j.order_id || j.id || 'ok';
      setOrdMsg(`Order accepted (ID: ${oid})`, true);

      // Keep symbol fields in sync
      if (!symFromTicket && ordSymbol) ordSymbol.value = sym;
      const bookSymInput = document.getElementById('bookSymbolRoot');
      if (bookSymInput && !bookSymInput.value) bookSymInput.value = sym;

      // Refresh panes
      loadBook().catch(() => {});
      loadOrders().catch(() => {});
      loadPositions().catch(() => {});
      loadRfqs().catch(() => {});
    } catch (e) {
      console.error(e);
      setOrdMsg('Order failed: ' + (e.message || e), false);
    }
  }

  async function loadOrders() {
    const body = document.querySelector('#ordersTbl tbody');
    if (!body) return;

    if (!ordersLoaded && !body.children.length) {
      body.innerHTML =
        `<tr><td colspan="5" style="text-align:center; padding:6px; opacity:.6;">Loading…</td></tr>`;
    }

    try {
      const rows = await api('/trader/orders');
      if (!rows.length) {
        body.innerHTML =
          `<tr><td colspan="5" style="text-align:center; padding:6px; opacity:.6;">No open orders.</td></tr>`;
        ordersLoaded = true;
        return;
      }

      body.innerHTML = rows
        .map(
          (r) => `
          <tr>
            <td>${r.symbol_root || r.symbol || ''}</td>
            <td>${(r.side || '').toUpperCase()}</td>
            <td>${r.price}</td>
            <td>${r.qty_open ?? r.qty ?? ''}</td>
            <td>${r.status || ''}</td>
          </tr>
        `
        )
        .join('');
      ordersLoaded = true;
    } catch (e) {
      body.innerHTML =
        `<tr><td colspan="5" style="text-align:center; padding:6px; opacity:.6;">Failed: ${e.message || 'error'}</td></tr>`;
      ordersLoaded = true;
    }
  }

  async function loadPositions() {
    const body = document.querySelector('#posTbl tbody');
    if (!body) return;

    if (!positionsLoaded && !body.children.length) {
      body.innerHTML =
        `<tr><td colspan="2" style="text-align:center; padding:6px; opacity:.6;">Loading…</td></tr>`;
    }

    try {
      const rows = await api('/trader/positions');
      if (!rows.length) {
        body.innerHTML =
          `<tr><td colspan="2" style="text-align:center; padding:6px; opacity:.6;">No positions.</td></tr>`;
        positionsLoaded = true;
        return;
      }
      body.innerHTML = rows
        .map(
          (r) => `
          <tr>
            <td>${r.symbol_root || r.symbol || ''}</td>
            <td>${r.net_lots ?? r.net_qty ?? 0}</td>
          </tr>
        `
        )
        .join('');
      positionsLoaded = true;
    } catch (e) {
      body.innerHTML =
        `<tr><td colspan="2" style="text-align:center; padding:6px; opacity:.6;">Failed: ${e.message || 'error'}</td></tr>`;
      positionsLoaded = true;
    }
  }

  async function loadRfqs() {
    const body = document.querySelector('#rfqTbl tbody');
    if (!body) return;

    if (!rfqsLoaded && !body.children.length) {
      body.innerHTML =
        `<tr><td colspan="4" style="text-align:center; padding:6px; opacity:.6;">Loading…</td></tr>`;
    }

    try {
      const rows = await api('/rfqs?scope=mine');
      if (!rows.length) {
        body.innerHTML =
          `<tr><td colspan="4" style="text-align:center; padding:6px; opacity:.6;">No RFQs.</td></tr>`;
        rfqsLoaded = true;
        return;
      }
      body.innerHTML = rows
        .map(
          (r) => `
          <tr>
            <td>${r.symbol || ''}</td>
            <td>${(r.side || '').toUpperCase()}</td>
            <td>${r.quantity_lots ?? r.qty_lots ?? ''}</td>
            <td>${r.expires_at || ''}</td>
          </tr>
        `
        )
        .join('');
      rfqsLoaded = true;
    } catch (e) {
      body.innerHTML =
        `<tr><td colspan="4" style="text-align:center; padding:6px; opacity:.6;">Failed: ${e.message || 'error'}</td></tr>`;
      rfqsLoaded = true;
    }
  }

  // events & init
  function init() {
    if (loadBtn) loadBtn.addEventListener('click', loadHistory);
    if (quoteBtn) quoteBtn.addEventListener('click', getQuote);

    document.getElementById('bookLoadBtn')?.addEventListener('click', loadBook);

    if (ordSubmitBtn) {
      ordSubmitBtn.addEventListener('click', function (ev) {
        ev.preventDefault();
        submitOrder().catch(() => {});
      });
    }

    // Seed ticket symbol from current selection if empty
    if (ordSymbol && !ordSymbol.value && symbolSel && symbolSel.value) {
      ordSymbol.value = symbolSel.value;
    }

    // Initial loads
    loadUniverse()
      .then(async () => {
        if (symbolSel && symbolSel.options.length) {
          await loadHistory();
        }
      })
      .catch(() => {});

    loadBook().catch(() => {});
    loadOrders().catch(() => {});
    loadPositions().catch(() => {});
    loadRfqs().catch(() => {});

    // Lightweight auto-refresh every ~45s when visible
    function tick() {
      if (document.hidden) return;
      loadOrders().catch(() => {});
      loadPositions().catch(() => {});
      loadRfqs().catch(() => {});
    }
    setInterval(tick, 45000);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
