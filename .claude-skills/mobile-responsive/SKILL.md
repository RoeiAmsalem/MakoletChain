---
name: mobile-responsive
description: Mobile-responsive rules for every MakoletChain page. Encodes our actual breakpoints, RTL/Chart.js gotchas, chart-wrapper sizing rules (the network "labels outside card" bug taught us), touch-target sizes, and table behavior. Use this skill BEFORE writing any new page, chart, or table — and re-check against it before declaring a page done.
---

# Mobile-Responsive Skill

Primary user: a store owner on a phone (target device width ~390px, e.g. iPhone 12/13/14). Desktop is where Roei builds; mobile is where it gets used. If it breaks on a 390px viewport, it's broken.

## Breakpoints

We use **one** mobile breakpoint everywhere:

```
@media (max-width: 768px) { /* mobile styles */ }
```

- `768px` — primary mobile breakpoint. Use this for everything: KPI grid reflow, table font shrink, chart heights, touch sizes, nav drawer.
- `480px` — only on auth pages (`login`, `forgot_password`, `reset_password`) for tighter padding. Don't introduce this elsewhere.
- `1440px` — only on auth pages for "large screen" widening. Don't introduce this elsewhere.

**Test viewports**: 390px (primary), 768px (tablet boundary), desktop. If a layout is fine at 390 and desktop, 768 is fine too — but verify the breakpoint flip.

## Design Tokens (NEVER introduce new colors)

Use only what's in `static/css/style.css :root`:

| Purpose            | Token                                                      |
|--------------------|------------------------------------------------------------|
| Page bg            | `var(--bg)` `#0f172a`                                      |
| Card / surface     | `var(--surface)` / `var(--card)` `#1e293b`                 |
| Card border        | `var(--card-border)` `#334155`                             |
| Accent             | `var(--accent)` `#6366f1`                                  |
| Profit / income    | `var(--profit)` `#22c55e` + `--profit-bg` + `--profit-border` |
| Loss / expense     | `var(--loss)` `#ef4444` + `--loss-bg` + `--loss-border`   |
| Fixed expenses     | `var(--fixed)` `#818cf8` + `--fixed-bg` + `--fixed-border` |
| Salary             | `var(--salary)` `#3b82f6` + `--salary-bg` + `--salary-border` |
| Text primary       | `var(--text-primary)` `#f1f5f9`                            |
| Text secondary     | `var(--text-secondary)` `#94a3b8`                          |
| Text muted         | `var(--text-muted)` `#64748b`                              |
| Border radius      | `var(--radius)` `12px`                                     |
| Shadow             | `var(--shadow)`                                            |

If you find yourself reaching for `#cbd5e1`, `#e2e8f0`, `#93c5fd`, `#86efac`, `#f87171`, `#fca5a5` — check first whether a token already covers it (most do).

## RTL Hebrew (BASE: `dir="rtl"`)

- `<html dir="rtl">` is set in `base.html` and on auth pages. Pages that extend `base.html` inherit RTL — **do not re-set it**.
- Text-align default is right. Use `text-align: left` only for numeric/LTR data (₪ amounts, dates, IDs). Use `direction: ltr` for currency cells so the ₪ sign stays on the left of the number.
- Margin/padding logical-side: prefer `padding-inline-start/end`. If you must use physical sides (legacy), remember: in RTL, `right` is the *start*, `left` is the *end*.

### Chart.js + RTL — read carefully

Chart.js doesn't understand `dir="rtl"`. We've shipped 5 separate fixes for this (commits `85057b8`, `82e0d98`, `90baee4`, `966b00f`, `ac82f39`). Rules learned the hard way:

1. **Don't reverse the axis** to flip a chart for RTL. Reverse the *data array* before passing to Chart.js (`82e0d98`).
2. **Restore default axes** after reversing data — don't double-flip (`90baee4`).
3. **Horizontal bar charts**: label-to-bar gap needs `y.ticks.padding` + `layout.padding` together. Just one isn't enough (`966b00f`).
4. **Legend dot/label pairing**: when reversing data for RTL, the legend can desync — keep dot-label pairing correct (`ac82f39`).
5. **Bar width**: with default categories Chart.js gets bar width right; don't override unless you've checked both LTR and RTL renders.

When in doubt, render the chart in both LTR (temporarily set `dir="ltr"` on the canvas wrapper in devtools) and RTL — anything that's mirrored unexpectedly is the bug.

## Chart Cards — the wrapper height rule

**The bug we shipped**: a 320px `.chart-card` with a 320px-tall canvas had no room for the section title + subtitle + card padding, so labels fell outside the card (`2192b0a`).

### Rule: canvas height < card content area

```
card content area  =  card height  −  (heading + subtitle + top/bottom padding)
canvas height      <  card content area
```

For our default `.chart-card { min-height: 320px; max-height: 380px; }` with one `<h2 class="section-title">` and one `.section-sub` and `1.25rem` padding, the canvas wrapper must be **≤ 260px** (default `.chart-wrapper` value) or **≤ 240px** (default `.network-chart-wrap` value when there's also a subtitle).

### Safe defaults

| Use case                                     | Wrapper height (desktop) | Wrapper height (≤768px) |
|---------------------------------------------|--------------------------|--------------------------|
| Standard chart card (no subtitle)            | `260px` (`.chart-wrapper`) | `200px`                 |
| Network chart card (title + subtitle)        | `240px` (`.network-chart-wrap`) | `240px` (keep — labels need room) |
| Hourly-sales mini chart                      | `180px`                  | `280px` (taller mobile, less data crowding) |

### Always

- `.chart-wrapper { position: relative; }` (Chart.js requires positioned parent for responsive sizing)
- Set wrapper height explicitly. **Never** let it auto-grow — Chart.js will resize on every paint and oscillate.
- In Chart.js options: `maintainAspectRatio: false` (we already do this; if you forget, the canvas ignores wrapper height).
- If a chart has long x-axis labels (branch names, dates), test at 390px. Truncate or rotate before letting them overflow.

## Touch Targets

Minimum tappable sizes on mobile (`≤768px`):

- **Primary action buttons** (save, add, submit): `min-height: 44px`
- **Secondary action buttons** (edit, filter chips): `min-height: 40px`
- **Icon buttons** (delete, close, expand): `min-width: 40px; min-height: 40px` (use 44x44 for modal close, PDF nav, alert dismiss)
- All tappable elements get `touch-action: manipulation;` to suppress the 300ms tap delay

Already-consistent patterns to copy: `templates/sales.html:58-62`, `templates/employees.html:161`, `templates/admin_branches.html:40`.

## Tables

We always scroll wide tables horizontally — **never stack rows** on mobile.

### Pattern

```html
<div style="overflow-x:auto">
  <table class="data-table"> ... </table>
</div>
```

```css
@media (max-width: 768px) {
  .data-table { min-width: 500px; font-size: 0.85rem; }
  .data-table thead th, .data-table tbody td { padding: 10px 8px; }
}
```

The `min-width: 500px` forces the table to keep its column structure at 390px; the wrapper provides side-scroll. Don't try to make a 5-column table readable in 390px without scrolling.

For grouped tables (goods page supplier accordions), the `<details>` summary stays full-width; the inner `.sg-body` is `overflow-x: auto`.

## KPI Tiles

`.cards-grid` default: `repeat(6, 1fr)` on desktop. On mobile (`≤768px`):

- Default: collapse to `1fr` (single column, stacked).
- 2-up override for image-light tiles (goods page): `grid-template-columns: 1fr 1fr` with `padding: 0.75rem` and `font-size: 1.25rem` for the value.

Tile content rules:

- `.kpi-value` shrinks from `~2rem` → `1.6rem` (style.css:599) or `1.25rem` (goods 2-up).
- Don't allow tile content to wrap awkwardly. If a label is `> 12 chars`, check on a real 390px viewport.

## Nav / Header

The hamburger (`#mobile-nav`) opens at `≤768px`. Hide desktop nav elements on mobile via `.hide-mobile { display: none !important; }`. The month switcher gets a mobile-specific variant (`.month-switcher-mobile`).

## Body / Overflow Guard

On pages with charts or wide content, add to the page's `<style>`:

```css
html, body { overflow-x: hidden; max-width: 100vw; }
```

(`templates/index.html:6`, `templates/employees.html:116` already do this.) This prevents a stray wide element from creating horizontal page scroll while still allowing intentional `overflow-x:auto` containers to scroll.

## Verification Checklist (before declaring a page done)

Run through this **every time** you add or substantially modify a page. Skipping = shipping the bug.

- [ ] Open at **390px width** (Chrome devtools → toolbar → iPhone 12 Pro, or `window.resizeTo(390, 844)` via Chrome MCP). Does anything overflow horizontally?
- [ ] Open at **desktop** (≥1280px). Does the layout still feel right (not stretched, not tiny in the center)?
- [ ] Every chart canvas sits **fully inside its card** at both widths. No axis label or legend item is clipped.
- [ ] Every tappable element on mobile is **≥40px tall** (≥44px for primary/modal-close).
- [ ] All wide tables are wrapped in `overflow-x:auto`; tapping a row doesn't fire on the wrong row when scrolled.
- [ ] RTL: section titles, KPI labels, Hebrew text all render right-aligned; numeric / currency cells are `direction: ltr`.
- [ ] No new color literals — all colors are tokens from `:root`.
- [ ] No new breakpoint — only `@media (max-width: 768px)` (unless it's an auth page, which can also use 480px / 1440px).
- [ ] `html, body { overflow-x: hidden; max-width: 100vw; }` is set on pages with charts or wide tables.

If verification can't be done visually (no browser available), say so plainly. Type-checking and tests do not catch mobile bugs.

## When to break the rules

- A chart genuinely needs more vertical room (e.g. a stacked breakdown across 6 branches with a legend) → raise the wrapper height AND the `.chart-card` `max-height` together. Never raise canvas without raising card.
- A new auth/marketing page can use 480px / 1440px breakpoints.
- A genuine new color used semantically (new status type) → add a token to `:root` first, then use it. Don't sprinkle hex literals.
