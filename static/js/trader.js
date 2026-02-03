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
  const ordAccount = document.getElementById('ordAccount');

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
      const uni = await api('/public/indices/universe?region=blended&limit=5000');
      const list = (uni && uni.tickers) ? uni.tickers : (uni || []);
      symbolSel.innerHTML = list
        .map((t) => `<option value="${t}">${t}</option>`)
        .join('');
    } catch (e) {
      console.error(e);
      symbolSel.innerHTML = `<option disabled>Failed to load</option>`;
    }
  }

  async function loadHistory() {
    if (!symbolSel || !limitSel || !idxMeta) return;
    const ticker = symbolSel.value;
    const limit = +limitSel.value || 500;
    idxMeta.textContent = '';
    try {
      const out = await api(`/public/indices/history?ticker=${encodeURIComponent(ticker)}&region=blended`);
      const rows = (out.rows || out || []);
      const use = rows.slice(-limit);
      // adapt to drawChart’s expected fields
      const pts = use.map(r => ({ date: r.as_of_date, price: r.index_price_per_ton ?? r.price_per_ton ?? r.avg_price }));
      // draw
      if (typeof Chart !== 'undefined') {
        const labels = pts.map(p => p.date);
        const data = pts.map(p => +p.price);
        if (chart) chart.destroy();
        chart = new Chart(ctx, {
          type: 'line',
          data: { labels, datasets: [{ label: `${ticker} (USD/ton)`, data }] },
          options: { responsive: true, animation: false, scales: { y: { beginAtZero: false } } }
        });
      } else {
        drawChart(pts);
      }
      if (pts.length) {
        const last = pts[pts.length - 1];
        idxMeta.textContent = `Latest: $${(+last.price).toFixed(2)} @ ${last.date}`;
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
      const q = await api(`/pricing/quote?material=${encodeURIComponent(material)}`);
      const perLb = q.price_per_lb ?? q.price ?? null;
      if (perLb != null) {
        const perTon = (+perLb * 2000).toFixed(2);
        quoteOut.textContent = `${q.material || material}: ~$${perTon}/ton`;
      } else if (q.price_per_ton != null) {
        quoteOut.textContent = `${q.material || material}: $${(+q.price_per_ton).toFixed(2)}/ton`;
      } else {
        quoteOut.textContent = 'No quote available yet.';
      }
    } catch (e) {
      quoteErr.textContent = 'No quote found (try another name or load vendor/index data).';
    }
  }

    async function resolveListingId(symbolRoot) {
    const products = await api('/admin/futures/products');
    const prod = (products || []).find(p => (p.symbol_root || '').toUpperCase() === symbolRoot.toUpperCase());
    if (!prod) throw new Error('Unknown product');
    const series = await api(`/admin/futures/series?product_id=${encodeURIComponent(prod.id)}&status=Listed`);
    if (!series.length) throw new Error('No Listed series');
    series.sort((a,b) => new Date(a.expiry_date) - new Date(b.expiry_date));
    return series[0].id;
  }
  
  async function loadBook() {
    const symbolRootInput = document.getElementById('bookSymbolRoot');
    const root = ((symbolRootInput && symbolRootInput.value) || '').trim()
      || ((symbolSel && symbolSel.value) || '').trim();
    if (!root) return;
    try {
      const listingId = await resolveListingId(root);
      const book = await api(`/trade/book?listing_id=${encodeURIComponent(listingId)}&depth=10`);
      const bidsBody = document.querySelector('#bookBids tbody');
      const asksBody = document.querySelector('#bookAsks tbody');

      function renderSide(rows, body) {
        if (!body) return;
        if (!rows || !rows.length) {
          body.innerHTML = `<tr><td colspan="2" style="opacity:.6; padding:4px;">No depth</td></tr>`;
          return;
        }
        body.innerHTML = rows.map(r =>
          `<tr><td>$${(+r.price).toFixed(2)}</td><td style="text-align:right;">${(+r.qty).toFixed(2)}</td></tr>`
        ).join('');
      }

      renderSide(book.bids || [], bidsBody);
      renderSide(book.asks || [], asksBody);

      // remember active listing/root if you add WS later
      loadBook._activeListing = listingId;
      loadBook._activeRoot = root.toUpperCase();
    } catch (e) {
      const bidsBody = document.querySelector('#bookBids tbody');
      const asksBody = document.querySelector('#bookAsks tbody');
      if (bidsBody) bidsBody.innerHTML = `<tr><td colspan="2" style="opacity:.6; padding:4px;">No book</td></tr>`;
      if (asksBody) asksBody.innerHTML = `<tr><td colspan="2" style="opacity:.6; padding:4px;">No book</td></tr>`;
      console.error(e);
    }
  }

  async function submitOrder() {
    if (!ordQty) return;

    const symFromTicket = (ordSymbol && ordSymbol.value || '').trim();
    const symFromBook = (document.getElementById('bookSymbolRoot')?.value || '').trim();
    const root = symFromTicket || symFromBook || (symbolSel && symbolSel.value || '').trim();

    const side = ((ordSide && ordSide.value) || 'buy').toUpperCase();
    const tif  = ((ordTif && ordTif.value) || 'day').toUpperCase();
    const qty  = parseFloat(ordQty.value || '0');
    const account_id = (ordAccount && ordAccount.value || '').trim();

    if (!root) { setOrdMsg('Enter a symbol root (e.g., CU-SHRED-1M).', false); return; }
    if (!(qty > 0)) { setOrdMsg('Qty must be > 0.', false); return; }
    if (!account_id) { setOrdMsg('Account ID required.', false); return; }

    try {
      setOrdMsg('Submitting…', true);

      const listing_id = await resolveListingId(root);
      const priceStr = (ordPrice && ordPrice.value || '').trim();
      const body = {
        account_id,
        listing_id,
        side,
        qty,
        order_type: priceStr ? 'LIMIT' : 'MARKET',
        tif
      };
      if (priceStr) body.price = parseFloat(priceStr);

      const j = await api('/trade/orders', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': (crypto.randomUUID && crypto.randomUUID()) || String(Date.now())
        },
        body: JSON.stringify(body)
      });

      const oid = j.id || j.order_id || 'ok';
      setOrdMsg(`Order accepted (ID: ${oid})`, true);

      // keep fields in sync
      if (!symFromTicket && ordSymbol) ordSymbol.value = root;
      const bookSymInput = document.getElementById('bookSymbolRoot');
      if (bookSymInput && !bookSymInput.value) bookSymInput.value = root;

      // refresh panes
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
