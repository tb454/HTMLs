// specs-accordion.js
// Generic accordion handler (supports nested accordions too)

document.addEventListener("DOMContentLoaded", () => {
  const buttons = document.querySelectorAll("button.accordion");

  buttons.forEach((btn) => {
    btn.addEventListener("click", () => {
      btn.classList.toggle("active");

      const panel = btn.nextElementSibling;
      if (!panel) return;

      panel.classList.toggle("open");
    });
  });
});
