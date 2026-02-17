// /static/js/login.js 

// Same-origin by default
const endpoint = (window.ENDPOINT && typeof window.ENDPOINT === "string")
  ? window.ENDPOINT
  : ""; // "" means same-origin

// Eagerly mint XSRF-TOKEN so the SPA can echo X-CSRF immediately (harmless for /login which is exempt)
(async () => {
  try {
    await fetch(joinURL(endpoint, "/healthz"), { credentials: "include" });
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

// Role → homepage map 
const ROLE_HOME = {
  admin:  "/admin",
  seller: "/seller",
  buyer:  "/buyer",
};

function _roleAllowsNext(role, path) {
  const p = String(path || "");
  // hard deny anything weird
  if (!p.startsWith("/") || p.startsWith("//")) return false;

  // admin can go anywhere in-app
  if (role === "admin") return true;

  // sellers: allow seller + shared pages
  if (role === "seller") {
    return (
      p === "/seller" || p.startsWith("/seller/") ||
      p === "/trader" || p.startsWith("/trader/") ||
      p === "/broker" || p.startsWith("/broker/") ||
      p === "/mill"   || p.startsWith("/mill/") ||
      p === "/indices-dashboard" || p.startsWith("/indices") ||
      p.startsWith("/public") ||
      p.startsWith("/legal") ||
      p === "/pricing" || p.startsWith("/pricing")
    );
  }

  // buyers: allow buyer + shared pages
  if (role === "buyer") {
    return (
      p === "/buyer"  || p.startsWith("/buyer/") ||
      p === "/trader" || p.startsWith("/trader/") ||
      p === "/broker" || p.startsWith("/broker/") ||
      p === "/mill"   || p.startsWith("/mill/") ||
      p === "/indices-dashboard" || p.startsWith("/indices") ||
      p.startsWith("/public") ||
      p.startsWith("/legal") ||
      p === "/pricing" || p.startsWith("/pricing")
    );
  }

  return false;
}

function safeInternalNext(role, nxt) {
  if (!nxt) return null;
  if (!nxt.startsWith("/") || nxt.startsWith("//")) return null;
  return _roleAllowsNext(role, nxt) ? nxt : null;
}

// ?next= handling + loading state + show/hide password
const qs   = new URLSearchParams(location.search);
const next = qs.get("next");
const loginBtn = document.querySelector('#loginForm button[type="submit"]');

// show/hide password
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
  errBox.classList.add("hidden");
  okBox.classList.add("hidden");

  const username = document.getElementById("username").value.trim();
  const password = document.getElementById("password").value;

  // Disable during request
  const old = loginBtn.textContent;
  loginBtn.disabled = true; loginBtn.textContent = "Signing in…";

  try {
    const res = await api("/login", {
      method: "POST",
      body: JSON.stringify({ username, password })
    });

    if (!res.ok) {
      let msg = "Invalid credentials. Try again.";
      try {
        const j = await res.json();
        if (j?.message) msg = j.message;
        else if (j?.detail) msg = j.detail;
      } catch {}
      errBox.textContent = msg; errBox.classList.remove("hidden");
      return;
    }

    const data = await res.json();

    // normalize role
    let role = (data.role || "").toLowerCase();
    if (role === "yard") role = "seller";
    if (!ROLE_HOME[role]) role = "buyer"; // hard fallback

    // --- derive org/buyer key from login identifier ---
    const rawUser = username; // what they typed into the login box

    const orgGuess = (function (u) {
      if (!u) return "";
      const lower = u.toLowerCase();

      // Hard-coded mappings for your real yards/buyers
      if (lower === "anthony.priority" || lower === "anthony@priorityrecycling.com") {
        return "Priority Recycling";
      }
      if (lower === "alex.farnsworth" || lower === "alex@farnsworthmetals.com") {
        return "Farnsworth Metals";
      }

      // Fallback: turn "anthony.priority" or "anthony_priority" into "Anthony Priority"
      const base = u.split("@")[0].replace(/[._-]+/g, " ").trim();
      if (!base) return u;
      return base.replace(/\b\w/g, c => c.toUpperCase());
    })(rawUser);

    const bridgeUser = {
      username: rawUser,
      role,
      // The buyer/member/org key the buyer dashboard will use:
      buyer:  orgGuess || rawUser,
      member: orgGuess || rawUser,
      org:    orgGuess || rawUser
    };

    try {
      localStorage.setItem("bridgeUser", JSON.stringify(bridgeUser));
    } catch {}

    // Target resolution order:
    // 1) ?next= (internal only)
    let target = safeInternalNext(role, next);

    // 2) server-provided redirect (internal only)
    if (!target && typeof data.redirect === "string") {
      const safe = safeInternalNext(role, data.redirect);
      if (safe) target = safe;
    }

    // 3) strict role map (.html pages)
    if (!target) target = ROLE_HOME[role];

    window.location.assign(target);

  } catch (err) {
    errBox.textContent = "Network error. Please try again.";
    errBox.classList.remove("hidden");
    console.error(err);
  } finally {
    loginBtn.disabled = false; loginBtn.textContent = old;
  }
});
