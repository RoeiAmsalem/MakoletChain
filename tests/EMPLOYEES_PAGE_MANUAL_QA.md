# Employees Page — Manual QA Checklist

## UI Tests (Chrome, desktop + mobile 390px)
- [ ] Page loads without JS errors in console
- [ ] All 3 KPI tiles show correct values
- [ ] Hours chart (horizontal bars) renders with correct employee names
- [ ] Salary donut chart renders with legend on right
- [ ] Labor cost % line chart renders with 10% benchmark line
- [ ] Charts are side-by-side on desktop, stacked on mobile (768px breakpoint)
- [ ] Mobile responsive at 390px — no horizontal scroll, all content readable
- [ ] Employee cards grid adapts to screen width
- [ ] Loading state: spinner visible while data loads
- [ ] Empty state: "אין עובדים" message when no employees exist

## Banner Tests
- [ ] Red banner shows for unknown employees, count correct
- [ ] Red banner disappears after adding employee via "הוסף עובד"
- [ ] Blue banner shows for CSV-only employees
- [ ] Yellow banner shows for discrepancies
- [ ] All banners hide when no items remain

## Modal Tests
- [ ] Add employee modal opens, all fields accessible
- [ ] Edit modal pre-fills name, role, rate correctly
- [ ] Escape key closes modal
- [ ] Add-as-new modal opens from red banner button
- [ ] Form validates: empty name shows error, zero rate shows error
- [ ] After save: modal closes, toast shows, list refreshes

## Delete Tests
- [ ] Delete shows confirmation dialog
- [ ] After delete: employee disappears, count updates
- [ ] Deleted employee's hours remain in history table

## Concurrency Tests
- [ ] Click delete twice fast on same employee — no crash, no duplicate action
- [ ] Open two tabs on same employee page — edit in one, other still works
- [ ] Run agent while editing employee — no data corruption

## Edge Cases
- [ ] Employee with 0 hours — card renders, salary shows "—"
- [ ] 20+ employees — grid layout holds, charts render correctly
- [ ] Hebrew + English mixed name — displays correctly
- [ ] Very long name (50+ chars) — card doesn't overflow

## Data Integrity (verify in DB after each action)
- [ ] Delete employee: active=0 (soft delete), aliases removed, hours preserved
- [ ] Reactivate via add-new: active=1, rate/role updated, aliases added
- [ ] Add unknown + rename + re-run agent: alias prevents re-flagging
