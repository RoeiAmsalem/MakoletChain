# Mobile verification checklist

After applying changes, check every item:

## Structural (must pass all)
- [ ] No horizontal scroll at 390px width (scroll bar at bottom of page = fail)
- [ ] No elements overlapping at 390px
- [ ] All KPI tiles visible without cropping
- [ ] All charts render and are readable
- [ ] All buttons have minimum 44x44px tap area
- [ ] All modals fit within viewport (no cut-off bottom)

## Typography (must pass all)
- [ ] Body text minimum 14px
- [ ] Input fields 16px (prevents iOS zoom-on-focus)
- [ ] Headings scaled down appropriately (not 2rem on mobile)
- [ ] No text truncated with ...

## Charts (must pass all)
- [ ] Chart containers have explicit height (200-220px typical)
- [ ] Donut legends below the chart on mobile, not right
- [ ] Bar chart labels don't overlap
- [ ] Line chart Y-axis labels readable
- [ ] Tooltip widths fit viewport

## Interactive (must pass all)
- [ ] Info tooltips open on tap (not just hover)
- [ ] Tap dismiss works when clicking outside tooltip
- [ ] All form fields accessible with on-screen keyboard
- [ ] Date pickers use native type="date" or type="month"
- [ ] Delete/edit buttons on cards are tappable (not too small)

## RTL (must pass all)
- [ ] Hebrew text reads right-to-left correctly
- [ ] Time strings (e.g. "6:30-8:30") use U+200E LTR mark
- [ ] Icons on correct side (info icon left-of-text in RTL = start side)

## Performance (nice to have)
- [ ] Page loads in under 2s on 4G
- [ ] No layout shift after initial render
- [ ] Images/charts don't resize-flash on load

## Final confirmation
- [ ] Tested at 390px width (iPhone)
- [ ] Tested at 768px width (tablet)
- [ ] Tested in RTL mode
- [ ] User confirmed page looks good on their phone
