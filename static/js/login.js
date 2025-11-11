// static/js.login.js

// Same-origin by default
const endpoint = (window.ENDPOINT && typeof window.ENDPOINT === "string")
  ? window.ENDPOINT
  : ""; // "" means same-origin

// Eagerly mint XSRF-TOKEN so the SPA can echo X-CSRF immediately (harmless for /login which is exempt)
(async () => {
  try {
    await fetch(joinURL(endpoint, "/health"), { credentials: "include" });
  } catch (_) {}
})();

// Collapse accidental double slashes
function joinURL(base, path) {
  const b = (base || "").replace(/\/+$/, "");
  const p = (path || "").replace(/^\/+/, "/");
  return b + p;
}

// Basic cookie reader for optional CSRF
function getCookie(name) {
  return document.cookie
    .split(";")
    .map(s => s.trim())
    .find(s => s.startsWith(name + "="))
    ?.split("=")[1];
}

// Tiny fetch wrapper
async function api(path, opts = {}) {
  const url = joinURL(endpoint, path);
  const headers = new Headers(opts.headers || {});
  if (!headers.has("Content-Type")) headers.set("Content-Type", "application/json");
  // Optional CSRF if you ever gate /login; your middleware sets XSRF-TOKEN on /health
  const xsrf = getCookie("XSRF-TOKEN");
  if (xsrf && !headers.has("X-CSRF")) headers.set("X-CSRF", xsrf);

  const res = await fetch(url, {
    method: opts.method || "GET",
    headers,
    body: opts.body,
    credentials: "include", 
  });
  return res;
}

// ?next= handling + loading state + show/hide password
const qs   = new URLSearchParams(location.search);
const next = qs.get("next");
const loginBtn = document.querySelector('#loginForm button[type="submit"]');

(function () {
  const t = document.getElementById("togglePw"),
        p = document.getElementById("password");
  if (t && p) {
    t.addEventListener("click", () => {
      const show = p.type === "password";
      p.type = show ? "text" : "password";
      t.textContent = show ? "Hide" : "Show";
      p.focus();
    });
  }
})();

document.getElementById("loginForm").addEventListener("submit", async function (e) {
  e.preventDefault();
  const errBox = document.getElementById("error");
  const okBox  = document.getElementById("success");
  errBox.style.display = "none"; okBox.style.display = "none";

  const username = document.getElementById("username").value.trim();
  const password = document.getElementById("password").value;

  // Disable during request
  const old = loginBtn.textContent;
  loginBtn.disabled = true; loginBtn.textContent = "Signing in…";

  try {
    const res = await api("/login" + (next ? `?next=${encodeURIComponent(next)}` : ""), {
      method: "POST",
      body: JSON.stringify({ username, password })
    });

    if (!res.ok) {
      let msg = "Invalid credentials. Try again.";
      try { const j = await res.json(); if (j?.detail) msg = j.detail; } catch {}
      errBox.textContent = msg; errBox.style.display = "block";
      return;
    }

    const data = await res.json();
    // normalize role + persist minimal state
    let role = (data.role || "").toLowerCase();
    if (role === "yard") role = "seller";
    localStorage.setItem("bridgeUser", JSON.stringify({ role }));

    // Prefer server-provided redirect
    if (data.redirect) { window.location.href = data.redirect; return; }

    // If server didn't redirect, honor ?next= first
    if (next) { window.location.href = next; return; }

    // Fallback by role
    window.location.href = role === "admin" ? "/admin"
                         : role ? `/${role}` : "/buyer";
  } catch (err) {
    errBox.textContent = "Network error. Please try again.";
    errBox.style.display = "block";
    console.error(err);
  } finally {
    loginBtn.disabled = false; loginBtn.textContent = old;
  }
});
