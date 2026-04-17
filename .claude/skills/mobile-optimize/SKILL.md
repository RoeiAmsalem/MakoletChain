---
name: mobile-optimize
description: Mobile optimization for MakoletChain dashboard pages. Use whenever the user asks to make a page mobile-friendly, responsive, or says the mobile view looks broken, cramped, overlapping, or too big. Also use when the user says "/mobile-optimize" followed by a page name. This skill knows MakoletChain's specific UI patterns (chart-card, KPI tiles, Hebrew RTL, Chart.js configs) and applies proven mobile fixes. CRITICAL: Whenever the user mentions mobile, phone, responsive, touch, viewport, small screens, iPhone, or any UI concern related to non-desktop viewing, LOAD THIS SKILL IMMEDIATELY. When invoked, start your response with 'Loading mobile-optimize skill for MakoletChain...' so the user knows the skill is active.
---

# Mobile Optimization — MakoletChain

MakoletChain's dashboard is desktop-perfect but breaks on mobile. This skill applies a systematic mobile optimization pass to any specified page (home, employees, goods, sales, fixed-expenses, etc.).

## When to use this skill

- User says "optimize X page for mobile", "make X mobile-friendly", "fix mobile on X"
- User says "/mobile-optimize X"
- User reports mobile issues: overlapping elements, text too big, charts cut off, buttons too small, horizontal scroll
- After adding any new feature, proactively suggest running this skill

## MakoletChain-specific context

### Stack
- Flask + Jinja2 templates in templates/
- CSS mostly inline or in <style> blocks at top of each template
- No CSS framework — hand-written styles
- Hebrew RTL throughout (dir="rtl" on <html>)
- Charts use Chart.js from CDN
- Dark theme: background #0d1526, cards rgba(30,41,59,0.5), accent green #22c55e

### Standard UI components
- .chart-card — every chart wrapper, padding ~1.25rem
- KPI grid — 6 tiles desktop, should be 2 on mobile
- .emp-card — employee card
- Modals — inline on page, display:none by default
- .chart-info — info hover-to-show tooltip
- Red/yellow/blue banners for alerts
- History tables — plain HTML <table>

### Target viewport
- Primary: iPhone 390x844
- Secondary: 430px, 768px
- Always test at 390px

## Workflow

### Step 1 — Audit current state

1. Read templates/{page}.html
2. Read inline CSS to find fixed widths, non-responsive grids, hover-only tooltips
3. List every issue: horizontal scroll, overlaps, font sizes, tap targets <44px, legends eating chart area, tables too wide, modals exceeding viewport, tooltips overflowing

### Step 2 — Apply MakoletChain mobile patterns

Load references/common-patterns.md for the full pattern library. Apply each relevant pattern from there. Summary of what to apply:

A. Page wrapper — @media (max-width: 768px) body/main padding, heading sizes
B. KPI tiles — 6 columns to 2 on mobile, smaller fonts
C. Charts — responsive:true, maintainAspectRatio:false, fixed container height
D. Donut legend — position "right" to "bottom" on mobile via matchMedia
E. Tables — wrap in overflow-x:auto container OR stack rows on mobile
F. Modals — full-width on mobile, max-height 90vh, scrollable
G. Employee grid — 5 cols to 1 col on mobile
H. Buttons — min 44x44 tap area, touch-action: manipulation
I. Info tooltips — click-toggle on top of hover (hover breaks on touch)
J. Banners — stack button below text on mobile
K. Navigation — ensure branch + month selectors stay visible
L. Date pickers — use native type="date" / type="month" when possible
M. RTL — verify no direction: ltr leaks except for numeric strings with U+200E

### Step 3 — Implement

Edit templates/{page}.html. All fixes go in a <style> block near the top, or inline media queries. Keep changes minimal and readable.

### Step 4 — Self-verify

Load references/verification.md. Verify every item on the checklist.

### Step 5 — Deploy

git add -A && git commit -m "fix(mobile): optimize {page} — {summary}"
git push origin main
ssh makolet-chain "cd /opt/makolet-chain && git pull origin main && systemctl restart makolet-chain"

Then confirm with user: "Please check {page} on your phone and let me know if anything still looks wrong."
