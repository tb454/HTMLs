// static/js/trader.js

(function () {
  const symbolInput   = document.getElementById("symbolInput");
  const symbolForm    = document.getElementById("symbolForm");
  const bidsBody      = document.getElementById("bidsBody");
  const asksBody      = document.getElementById("asksBody");
  const bookMeta      = document.getElementById("bookMeta");

  const orderForm     = document.getElementById("orderForm");
  const orderSymbol   = document.getElementById("orderSymbol");
  const orderSide     = document.getElementById("orderSide");
  const orderTif      = document.getElementById("orderTif");
  const orderPrice    = document.getElementById("orderPrice");
  const orderQty      = document.getElementById("orderQty");
  const orderStatus   = document.getElementById("orderStatus");
  const refreshAllBtn = document.getElementById("refreshAllBtn");

  const positionsBody = document.getElementById("positionsBody");
  const futOrdersBody = document.getElementById("futOrdersBody");

  function setStatus(el, msg, ok = true) {
    if (!el) return;
    el.innerHTML = "";
    const div = document.createElement("div");
    div.className = ok ? "alert alert-success py-1 px-2 mb-0" : "alert alert-danger py-1 px-2 mb-0";
    div.textContent = msg;
    el.appendChild(div);
  }

  function clearTableBody(tbody, colSpan) {
    if (!tbody) return;
    tbody.innerHTML = "";
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = colSpan;
    td.className = "text-muted small";
    td.textContent = "No data";
    tr.appendChild(td);
    tbody.appendChild(tr);
  }

  async function fetchJSON(url, opts = {}) {
    const res = await fetch(url, {
      credentials: "include",
      ...opts,
      headers: {
        "Accept": "application/json",
        ...(opts.headers || {}),
      },
    });
    if (!res.ok) {
      let detail = res.statusText;
      try {
        const j = await res.json();
        detail = j.detail || j.error || detail;
      } catch (_) {}
      throw new Error(`HTTP ${res.status}: ${detail}`);
    }
    return res.json();
  }

  // ---- BOOK ----
  async function loadBook() {
    const sym = (symbolInput.value || "").trim();
    if (!sym) return;
    try {
      bookMeta.textContent = "Loading order book...";
      const data = await fetchJSON(`/trader/book?symbol=${encodeURIComponent(sym)}`);
      renderBook(data);
      bookMeta.textContent = `Symbol: ${data.symbol}`;
    } catch (err) {
      console.error(err);
      clearTableBody(bidsBody, 2);
      clearTableBody(asksBody, 2);
      bookMeta.textContent = `Error loading book: ${err.message}`;
    }
  }

  function renderBook(data) {
    const bids = data.bids || [];
    const asks = data.asks || [];

    // bids
    bidsBody.innerHTML = "";
    if (!bids.length) {
      clearTableBody(bidsBody, 2);
    } else {
      bids.forEach((lvl) => {
        const tr = document.createElement("tr");
        const tdP = document.createElement("td");
        const tdQ = document.createElement("td");
        tdP.textContent = lvl.price.toFixed(2);
        tdQ.textContent = lvl.qty_lots != null ? Number(lvl.qty_lots).toFixed(2) : "-";
        tdQ.className = "text-end";
        tr.appendChild(tdP);
        tr.appendChild(tdQ);
        bidsBody.appendChild(tr);
      });
    }

    // asks
    asksBody.innerHTML = "";
    if (!asks.length) {
      clearTableBody(asksBody, 2);
    } else {
      asks.forEach((lvl) => {
        const tr = document.createElement("tr");
        const tdP = document.createElement("td");
        const tdQ = document.createElement("td");
        tdP.textContent = lvl.price.toFixed(2);
        tdQ.textContent = lvl.qty_lots != null ? Number(lvl.qty_lots).toFixed(2) : "-";
        tdQ.className = "text-end";
        tr.appendChild(tdP);
        tr.appendChild(tdQ);
        asksBody.appendChild(tr);
      });
    }
  }

  // ---- POSITIONS ----
  async function loadPositions() {
    try {
      const rows = await fetchJSON("/trader/positions");
      positionsBody.innerHTML = "";
      if (!rows.length) {
        clearTableBody(positionsBody, 2);
        return;
      }
      rows.forEach((r) => {
        const tr = document.createElement("tr");
        const tdSym = document.createElement("td");
        const tdNet = document.createElement("td");
        tdSym.textContent = r.symbol_root;
        tdNet.textContent = Number(r.net_lots || 0).toFixed(2);
        tdNet.className = "text-end";
        tr.appendChild(tdSym);
        tr.appendChild(tdNet);
        positionsBody.appendChild(tr);
      });
    } catch (err) {
      console.error(err);
      clearTableBody(positionsBody, 2);
    }
  }

  // ---- FUTURES ORDERS ----
  async function loadFuturesOrders() {
    try {
      const rows = await fetchJSON("/trader/orders");
      futOrdersBody.innerHTML = "";
      if (!rows.length) {
        clearTableBody(futOrdersBody, 4);
        return;
      }
      rows.forEach((r) => {
        const tr = document.createElement("tr");
        const tdRoot = document.createElement("td");
        const tdSide = document.createElement("td");
        const tdPx   = document.createElement("td");
        const tdQ    = document.createElement("td");

        tdRoot.textContent = r.symbol_root || r.symbol || "";
        tdSide.textContent = (r.side || "").toUpperCase();
        tdPx.textContent   = r.price != null ? Number(r.price).toFixed(2) : "-";
        tdQ.textContent    = r.qty_open != null ? Number(r.qty_open).toFixed(2) : "-";

        tdPx.className = "text-end";
        tdQ.className  = "text-end";

        tr.appendChild(tdRoot);
        tr.appendChild(tdSide);
        tr.appendChild(tdPx);
        tr.appendChild(tdQ);
        futOrdersBody.appendChild(tr);
      });
    } catch (err) {
      console.error(err);
      clearTableBody(futOrdersBody, 4);
    }
  }

  // ---- CLOB ORDER SUBMIT ----
  async function submitClobOrder(ev) {
    ev.preventDefault();
    const sym = (orderSymbol.value || "").trim();
    const side = orderSide.value || "buy";
    const tif  = orderTif.value || "day";

    if (!sym) {
      setStatus(orderStatus, "Symbol is required", false);
      return;
    }

    const qty = parseFloat(orderQty.value || "0");
    if (!qty || qty <= 0) {
      setStatus(orderStatus, "Qty must be > 0", false);
      return;
    }

    // LIMIT vs MARKET (for MARKET, leave price undefined)
    const priceRaw = orderPrice.value;
    const body = {
      symbol: sym,
      side: side,
      price: priceRaw !== "" ? Number(priceRaw) : undefined,
      qty_lots: qty,
      tif: tif,
    };

    try {
      setStatus(orderStatus, "Submitting order…", true);
      const res = await fetchJSON("/clob/orders", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });

      const id = res.order_id || res.id || "(unknown)";
      setStatus(orderStatus, `Order accepted (ID: ${id})`, true);

      // Reload book + positions (and futures orders if any)
      await Promise.all([
        loadBook(),
        loadPositions(),
        loadFuturesOrders(),
      ]);
    } catch (err) {
      console.error(err);
      setStatus(orderStatus, `Order failed: ${err.message}`, false);
    }
  }

  async function refreshAll() {
    await Promise.all([
      loadBook(),
      loadPositions(),
      loadFuturesOrders(),
    ]);
  }

  function init() {
    if (!symbolForm) return;

    symbolForm.addEventListener("submit", function (ev) {
      ev.preventDefault();
      loadBook();
    });

    orderForm.addEventListener("submit", submitClobOrder);
    if (refreshAllBtn) {
      refreshAllBtn.addEventListener("click", function () {
        refreshAll();
      });
    }

    // sync ticket symbol w/ left pane
    orderSymbol.value = symbolInput.value;

    // initial load
    refreshAll().catch(console.error);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
