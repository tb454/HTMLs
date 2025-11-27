// /static/js/trader-auth.js
// Guards the Trader view: only admin/buyer/seller/yard roles allowed.

(async () => {
  try {
    // warm CSRF + session
    await fetch('/health', { credentials: 'include' });

    const r = await fetch('/me', {
      credentials: 'include',
      headers: { 'Accept': 'application/json' }
    });
    if (!r.ok) throw new Error('no session');
    const me = await r.json(); // { username, role }

    const role = (me.role || '').toLowerCase();
    const allowed = ['admin', 'buyer', 'seller', 'yard'];

    if (!role || !allowed.includes(role)) {
      const next = encodeURIComponent('/trader');
      location.href = `/static/bridge-login.html?next=${next}`;
      return;
    }
    // allowed → do nothing, user stays on Trader
  } catch {
    const next = encodeURIComponent('/trader');
    location.href = `/static/bridge-login.html?next=${next}`;
  }
})();
