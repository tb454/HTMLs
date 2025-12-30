// public-index-detail.js
(function () {
  document.addEventListener("contextmenu", (e) => e.preventDefault());
  document.addEventListener("selectstart", (e) => e.preventDefault());
  document.addEventListener("copy", (e) => {
    e.preventDefault();
    e.clipboardData.setData("text/plain", "BRidge Indices — https://bridge.scrapfutures.com/indices");
  });

  const ticker = decodeURIComponent(location.pathname.split("/").pop() || "");
  const elTicker = document.getElementById("tickerText");
  const elUpdated = document.getElementById("updatedText");
  const elLast = document.getElementById("lastText");
  const elDelta = document.getElementById("deltaText");
  const elUnit = document.getElementById("unitText");
  const elMode = document.getElementById("modeText");

  elTicker.textContent = ticker || "—";

  function fmt(n) {
    if (n === null || n === undefined) return "—";
    if (typeof n === "number") return n.toFixed(4);
    return String(n);
  }

  function draw(series) {
    const c = document.getElementById("chart");
    const ctx = c.getContext("2d");
    ctx.clearRect(0, 0, c.width, c.height);

    if (!Array.isArray(series) || series.length < 2) {
      ctx.font = "600 18px system-ui";
      ctx.fillStyle = getComputedStyle(document.body).color;
      ctx.fillText("Not enough data to chart.", 20, 60);
      return;
    }

    const values = series.map(p => +p.value).filter(v => !Number.isNaN(v));
    const minV = Math.min(...values), maxV = Math.max(...values);
    const pad = 40;
    const W = c.width - pad * 2, H = c.height - pad * 2;

    const x = (i) => pad + (i / (series.length - 1)) * W;
    const y = (v) => pad + (1 - ((v - minV) / (maxV - minV || 1))) * H;

    ctx.globalAlpha = 0.25;
    ctx.strokeStyle = "#fff";
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.moveTo(pad, pad);
    ctx.lineTo(pad, c.height - pad);
    ctx.lineTo(c.width - pad, c.height - pad);
    ctx.stroke();
    ctx.globalAlpha = 1;

    ctx.strokeStyle = "#fff";
    ctx.lineWidth = 3;
    ctx.beginPath();
    series.forEach((p, i) => {
      const xv = x(i);
      const yv = y(+p.value);
      if (i === 0) ctx.moveTo(xv, yv);
      else ctx.lineTo(xv, yv);
    });
    ctx.stroke();

    ctx.font = "600 14px system-ui";
    ctx.fillStyle = "#fff";
    ctx.globalAlpha = 0.75;
    ctx.fillText("max " + maxV.toFixed(4), pad, pad - 10);
    ctx.fillText("min " + minV.toFixed(4), pad, c.height - pad + 24);
    ctx.globalAlpha = 1;
  }

  async function load() {
    const res = await fetch(`/api/public/index/${encodeURIComponent(ticker)}`, { headers: { "Accept": "application/json" } });
    if (!res.ok) throw new Error("Failed");
    const data = await res.json();

    elUpdated.textContent = "Updated: " + (data.updated || "—");
    elLast.textContent = fmt(data.last);
    elUnit.textContent = data.unit || "—";

    const d = (typeof data.delta === "number") ? data.delta : null;
    elDelta.textContent = (d === null) ? "—" : (d >= 0 ? "+" + d.toFixed(4) : d.toFixed(4));
    elDelta.className = "value " + (d === null ? "" : (d >= 0 ? "delta pos" : "delta neg"));

    elMode.textContent = "Mode: " + (data.mode || "Public");
    draw(data.series || []);
  }

  document.getElementById("refreshBtn").addEventListener("click", () => load().catch(console.error));
  load().catch(err => {
    console.error(err);
    elUpdated.textContent = "Updated: error";
  });
})();
