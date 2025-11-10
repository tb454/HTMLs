// /static/js/locale.js
(async function () {
  // --- helpers ------------------------------------------------------------
  const $ = (sel, root=document) => root.querySelector(sel);
  const $$ = (sel, root=document) => Array.from(root.querySelectorAll(sel));
  const endpoint = ""; // same-origin

  async function getJSON(path, init) {
    const r = await fetch(endpoint + path, { credentials: "same-origin", ...init });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return r.json();
  }

  // apply {key: translatedText} over any [data-i18n="key"] node
  function applyI18n(dict) {
    $$("[data-i18n]").forEach(el => {
      const k = el.getAttribute("data-i18n");
      if (k && dict[k]) el.textContent = dict[k];
    });
    // title/placeholders (optional)
    $$("[data-i18n-title]").forEach(el => {
      const k = el.getAttribute("data-i18n-title");
      if (k && dict[k]) el.title = dict[k];
    });
    $$("[data-i18n-ph]").forEach(el => {
      const k = el.getAttribute("data-i18n-ph");
      if (k && dict[k]) el.placeholder = dict[k];
    });
  }

  function cookie(name) {
    const m = document.cookie.match(new RegExp("(?:^|; )" + name + "=([^;]*)"));
    return m ? decodeURIComponent(m[1]) : null;
  }
  // i18n sticky + safe t()
  (function initTranslator(){
    const langCookie = document.cookie.match(/(?:^|; )LANG=([^;]+)/)?.[1];
    const _currentLang = (langCookie || 'en').split('-')[0];

    // expose a safe translator that prefers page bundle → english → key/def
    if (!window.t) {
      window.t = (k, def) => (window.STRINGS?.[_currentLang]?.[k]
                          ?? window.STRINGS?.en?.[k]
                          ?? def ?? k);
    }
  })();  
  
  async function hydrateLocale() {
    // 1) pull current strings
    const { lang, strings } = await getJSON("/i18n/strings");
    // keep a global copy for window.t()
    window.STRINGS = window.STRINGS || {};
    window.STRINGS[lang] = strings;
    if (!window.STRINGS.en && lang !== 'en') {
      // opportunistic: fallback will still work even if en is absent
      try { const en = await getJSON("/i18n/strings?lang=en"); window.STRINGS.en = en.strings; } catch {}
    }
    applyI18n(strings);

    // 2) set selectors if present
    const langSel = $("#lang-select");
    if (langSel) {
      langSel.value = (langSel.value || lang);
      if (!langSel.__wired) {
        langSel.addEventListener("change", () => BRIDGE_LOCALE.save());
        langSel.__wired = true;
      }
    }

    const tzSel = $("#tz-select");
    if (tzSel) {
      // Try TZ cookie first; fallback to server-reported tz
      let tz = cookie("TZ");
      if (!tz) {
        try {
          const t = await getJSON("/time/sync");
          tz = t.tz || "UTC";
        } catch { tz = "UTC"; }
      }
      tzSel.value = tz;
    }

    // 3) example: render a local time UI element, if present
    const timeBox = $("#local-time");
    if (timeBox) {
      try {
        const t = await getJSON("/time/sync");
        // prefer server’s formatted string if provided
        timeBox.textContent = t.local_display || (t.local || t.utc);
      } catch {
        // ignore
      }
    }
  }

  async function saveLocale() {
    const langSel = $("#lang-select");
    const tzSel   = $("#tz-select");
    const body = {
      lang: langSel ? langSel.value : undefined,
      tz: tzSel ? tzSel.value : undefined
    };
    await fetch("/prefs/locale", {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body)
    });
    // hard refresh so CSP-nonced templates re-render with cookies in place
    location.reload();
  }

  // expose save for buttons
  window.BRIDGE_LOCALE = { save: saveLocale };

  // kick off
  try { await hydrateLocale(); } catch {}
})();
