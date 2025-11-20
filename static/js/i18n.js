// static/js/i18n.js

// Simple cookie reader for CSRF, etc.
function getCookie(name) {
  const match = document.cookie.match(new RegExp("(^|; )" + name + "=([^;]*)"));
  return match ? decodeURIComponent(match[2]) : null;
}

// Apply translations to any element with data-i18n-key="<key>"
function applyTranslations(strings) {
  if (!strings || typeof strings !== "object") return;
  document.querySelectorAll("[data-i18n-key]").forEach(el => {
    const key = el.getAttribute("data-i18n-key");
    const val = strings[key];
    if (typeof val === "string") {
      el.textContent = val;
    }
  });
}

// Fetch current language strings from backend
async function loadI18nStrings() {
  try {
    const resp = await fetch("/i18n/strings", { credentials: "include" });
    if (!resp.ok) return;
    const data = await resp.json();
    applyTranslations(data.strings || {});
  } catch (e) {
    console.warn("i18n strings load failed", e);
  }
}

// Handle language dropdown changes
async function initLanguageSelector() {
  const select = document.getElementById("lang-select");
  if (!select) return;

  select.addEventListener("change", async () => {
    const lang = select.value || "en";
    const csrf = getCookie("XSRF-TOKEN");

    try {
      await fetch("/prefs/locale", {
        method: "POST",
        credentials: "include",
        headers: {
          "Content-Type": "application/json",
          ...(csrf ? { "X-CSRF": csrf } : {})
        },
        body: JSON.stringify({ lang })
      });
      // Reload strings for the new language
      await loadI18nStrings();
    } catch (e) {
      console.warn("Failed to update locale", e);
    }
  });
}

document.addEventListener("DOMContentLoaded", () => {
  // 1) Load initial strings for whatever the server picked
  loadI18nStrings();
  // 2) Wire the dropdown if present
  initLanguageSelector();
});
