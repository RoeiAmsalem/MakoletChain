# MakoletChain mobile patterns

## Pattern A — Page wrapper
```css
@media (max-width: 768px) {
  body, main { padding: 0.75rem !important; }
  h1 { font-size: 1.25rem !important; }
  h2 { font-size: 1.1rem !important; }
  h3 { font-size: 1rem !important; }
}
```

## Pattern B — KPI tiles
```css
.kpi-grid {
  display: grid;
  grid-template-columns: repeat(6, 1fr);
  gap: 1rem;
}
@media (max-width: 768px) {
  .kpi-grid { grid-template-columns: repeat(2, 1fr); gap: 0.5rem; }
  .kpi-tile { padding: 0.75rem !important; }
  .kpi-value { font-size: 1.25rem !important; }
  .kpi-label { font-size: 0.7rem !important; }
}
```

## Pattern C — Chart container
Ensure the canvas parent has explicit height and position:relative:
```html
<div style="position: relative; height: 200px; width: 100%;">
  <canvas id="chart-x"></canvas>
</div>
```

Chart.js options MUST include:
```javascript
options: {
  responsive: true,
  maintainAspectRatio: false,
  ...
}
```

## Pattern D — Donut legend responsive
```javascript
const isMobile = () => window.matchMedia('(max-width: 768px)').matches;
plugins: {
  legend: {
    position: isMobile() ? 'bottom' : 'right',
    labels: {
      color: '#ffffff',
      font: { size: isMobile() ? 12 : 16, weight: '600' },
      padding: isMobile() ? 8 : 14,
      boxWidth: isMobile() ? 12 : 18,
      boxHeight: isMobile() ? 12 : 18,
      usePointStyle: true
    }
  }
}
```

On window resize, call chart.update() to re-apply.

## Pattern E — Tables (scrollable)
Wrap existing table:
```html
<div style="overflow-x: auto; -webkit-overflow-scrolling: touch; margin: 0 -0.75rem; padding: 0 0.75rem;">
  <table style="min-width: 600px;"> ... </table>
</div>
```

## Pattern E2 — Tables (stacked on mobile)
Requires data-label="..." on every `<td>`:
```css
@media (max-width: 768px) {
  table.stack-mobile, table.stack-mobile thead, table.stack-mobile tbody,
  table.stack-mobile th, table.stack-mobile td, table.stack-mobile tr { display: block; }
  table.stack-mobile thead { display: none; }
  table.stack-mobile tr { border-bottom: 1px solid #475569; padding: 0.75rem 0; }
  table.stack-mobile td { padding: 0.25rem 0; border: none; }
  table.stack-mobile td::before {
    content: attr(data-label) ": ";
    font-weight: 600;
    color: #94a3b8;
    margin-left: 0.5rem;
  }
}
```

## Pattern F — Modals
```css
.modal { position: fixed; inset: 0; background: rgba(0,0,0,0.7); display: none; align-items: center; justify-content: center; z-index: 500; padding: 0.5rem; }
.modal-box { background: #1e293b; border: 1px solid #334155; border-radius: 12px; padding: 1.75rem; width: 100%; max-width: 480px; max-height: 90vh; overflow-y: auto; }
@media (max-width: 768px) {
  .modal-box { padding: 1.25rem; max-height: 95vh; }
  .modal-box input, .modal-box select { font-size: 16px !important; }
}
```

Note: iOS zooms inputs if font-size < 16px. Always 16px on mobile.

## Pattern G — Employee/card grid
```css
.emp-grid { display: grid; grid-template-columns: repeat(5, 1fr); gap: 1rem; }
@media (max-width: 768px) {
  .emp-grid { grid-template-columns: 1fr !important; gap: 0.75rem; }
  .emp-card { padding: 0.75rem !important; }
}
```

## Pattern H — Buttons
```css
button, .btn {
  min-height: 44px;
  min-width: 44px;
  padding: 0.65rem 1rem;
  touch-action: manipulation;
}
```

## Pattern I — Click-toggle tooltips
```css
.chart-info { position: relative; display: inline-block; cursor: pointer; }
.chart-info::after {
  content: attr(data-tooltip);
  position: absolute;
  top: 100%;
  inset-inline-start: 0;
  margin-top: 0.5rem;
  background: #1e293b;
  border: 1px solid #475569;
  border-radius: 8px;
  padding: 0.75rem 1rem;
  font-size: 0.85rem;
  color: #e2e8f0;
  width: min(260px, calc(100vw - 2rem));
  z-index: 100;
  display: none;
  white-space: normal;
  line-height: 1.5;
  direction: rtl;
  text-align: right;
  box-shadow: 0 4px 12px rgba(0,0,0,0.4);
}
.chart-info:hover::after, .chart-info.open::after { display: block; }
```

JavaScript (add once per page):
```javascript
document.querySelectorAll('.chart-info').forEach(el => {
  el.addEventListener('click', e => {
    e.stopPropagation();
    document.querySelectorAll('.chart-info.open').forEach(o => { if (o !== el) o.classList.remove('open'); });
    el.classList.toggle('open');
  });
});
document.addEventListener('click', () => {
  document.querySelectorAll('.chart-info.open').forEach(el => el.classList.remove('open'));
});
```

## Pattern J — Banners
```css
@media (max-width: 768px) {
  .banner-row { flex-direction: column !important; align-items: stretch !important; gap: 0.5rem !important; }
  .banner-row button { width: 100% !important; }
  .banner-title { font-size: 0.95rem !important; }
}
```

## Pattern K — Navigation bar
```css
@media (max-width: 768px) {
  .top-nav { padding: 0.5rem 0.75rem !important; gap: 0.5rem !important; }
  .top-nav .logo { font-size: 0.9rem !important; }
  .top-nav select, .top-nav button { font-size: 0.85rem !important; padding: 0.4rem 0.75rem !important; }
}
```
If still crowded, hide the logo text and keep only the icon on mobile.

## Pattern L — Inputs
```css
input[type="number"], input[type="text"], input[type="email"], select, textarea {
  font-size: 16px;
}
@media (max-width: 768px) {
  input, select, textarea { font-size: 16px !important; }
}
```
