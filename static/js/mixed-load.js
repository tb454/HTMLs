// /static/js/mixed-load.js
const TOTAL_WEIGHT = 20000;

function recalculate() {
  const rows = document.querySelectorAll('#contractTable tr');
  let rawTotal = 0;
  let totalPercent = 0;

  rows.forEach((row) => {
    const percentInput = row.cells[1]?.querySelector('input');
    const recoveredInput = row.cells[2]?.querySelector('input');
    const priceInput = row.cells[3]?.querySelector('input');
    const valueCell = row.querySelector('.totalValue');

    if (!percentInput || !recoveredInput || !priceInput || !valueCell) return;

    const percent = parseFloat(percentInput.value) || 0;
    const price = parseFloat(priceInput.value) || 0;
    const recovered = (percent / 100) * TOTAL_WEIGHT;
    const total = recovered * price;

    recoveredInput.value = recovered.toFixed(2);
    valueCell.textContent = `$${total.toFixed(2)}`;

    rawTotal += total;
    totalPercent += percent;
  });

  const warning = document.getElementById('warning');
  if (warning) {
    warning.textContent =
      totalPercent > 100
        ? '⚠️ Total % of Load exceeds 100%. Please adjust.'
        : '';
  }

  const marginInput = document.getElementById('marginInput');
  const feeInput = document.getElementById('feeInput');

  const marginPercent = marginInput ? parseFloat(marginInput.value) || 0 : 0;
  const contractFee = feeInput ? parseFloat(feeInput.value) || 0 : 0;

  const afterMargin = rawTotal * (1 - marginPercent / 100);
  const finalValue = afterMargin + contractFee;

  const rawValueEl = document.getElementById('rawValue');
  const afterMarginEl = document.getElementById('afterMargin');
  const finalValueEl = document.getElementById('finalValue');

  if (rawValueEl) rawValueEl.textContent = rawTotal.toFixed(2);
  if (afterMarginEl) afterMarginEl.textContent = afterMargin.toFixed(2);
  if (finalValueEl) finalValueEl.textContent = finalValue.toFixed(2);
}

function makeRowTemplate() {
  return `
    <td>
      <input
        type="text"
        name="material[]"
        value="New Material"
        placeholder="Material"
        title="Material name"
        aria-label="Material name"
      />
    </td>
    <td>
      <input
        type="number"
        name="percent[]"
        value="0"
        placeholder="% of load"
        title="% of load"
        aria-label="% of load"
      />
    </td>
    <td>
      <input
        type="number"
        name="recovered[]"
        value="0"
        readonly
        aria-label="Recovered weight in pounds"
        title="Recovered weight in pounds"
      />
    </td>
    <td>
      <input
        type="number"
        step="0.01"
        name="price[]"
        value="0.00"
        placeholder="Price/lb"
        title="Market price per pound"
        aria-label="Market price per pound"
      />
    </td>
    <td class="totalValue">$0.00</td>
    <td>
      <button
        type="button"
        data-action="remove-row"
        style="color: red; border: none; background: none; font-weight: bold; cursor: pointer;"
        aria-label="Remove material row"
        title="Remove material row"
      >
        ✖
      </button>
    </td>
  `;
}

function addRow() {
  const table = document.getElementById('contractTable');
  if (!table) return;
  const row = table.insertRow();
  row.innerHTML = makeRowTemplate();
  bindRowInputs(row);
  recalculate();
}

function bindRowInputs(row) {
  if (!row) return;
  // Any editable input in the contract table reacts to changes
  row
    .querySelectorAll('input[type="text"], input[type="number"]:not([readonly])')
    .forEach((input) => {
      input.addEventListener('input', recalculate);
      input.addEventListener('change', recalculate);
    });
}

document.addEventListener('DOMContentLoaded', () => {
  // Bind existing rows
  document.querySelectorAll('#contractTable tr').forEach(bindRowInputs);

  // Margin / fee inputs
  const marginInput = document.getElementById('marginInput');
  const feeInput = document.getElementById('feeInput');
  marginInput?.addEventListener('input', recalculate);
  feeInput?.addEventListener('input', recalculate);

  // Add material button
  const addBtn = document.getElementById('addMaterialBtn');
  if (addBtn) {
    addBtn.addEventListener('click', (e) => {
      e.preventDefault();
      addRow();
    });
  }

  // Row delete handler (event delegation)
  const table = document.getElementById('contractTable');
  if (table) {
    table.addEventListener('click', (e) => {
      const btn = e.target.closest('[data-action="remove-row"]');
      if (!btn) return;
      const row = btn.closest('tr');
      if (row && row.parentElement) {
        row.parentElement.removeChild(row);
        recalculate();
      }
    });
  }

  // Initial calculation
  recalculate();
});
