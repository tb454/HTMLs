// static/js.login.js
    // Same-origin calls; leave blank in prod so /login hits this service
    const endpoint = ""; // e.g. "" in prod, or "http://localhost:10000" for local dev

    // Collapse accidental double slashes
    function joinURL(base, path) {
      const b = (base || "").replace(/\/+$/,"");
      const p = (path || "").replace(/^\/+/,"/");
      return b + p;
    }

    async function api(path, opts = {}) {
      const url = joinURL(endpoint, path);
      const res = await api("/login", {
        method: "POST",
        body: JSON.stringify({ username, password })
      });
        return res;
      }

    // ?next= handling + loading state + show/hide password
    const qs   = new URLSearchParams(location.search);
    const next = qs.get('next');
    const loginBtn = document.querySelector('#loginForm button[type="submit"]');

    (function(){
      const t=document.getElementById('togglePw'), p=document.getElementById('password');
      if(t && p){ t.addEventListener('click', ()=>{ const s=p.type==='password'; p.type=s?'text':'password'; t.textContent=s?'Hide':'Show'; p.focus();});}
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
