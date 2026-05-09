# Aviv Employer Report Agent

## Why we're switching

The current `aviv_employees.py` uses `POST /employees/sales?type=all` which returns
per-employee shift data. The "employer's report" (one of Aviv BI's 102 reports) is
expected to be more accurate and comprehensive — it's the same report branch managers
download manually from Aviv BI.

## New endpoint

```
GET https://bi1.aviv-pos.co.il:8443/avivbi/v2/reports?branch={aviv_branch_id}
```

- Returns 404 when POS is offline (after store hours)
- Returns 200 with a list of available reports when store is live
- `aviv_branch_id` is obtained from the login response (not stored in DB)

## Schedule (future)

| Day       | Times        |
|-----------|-------------|
| Sun-Thu   | 16:00, 23:30 |
| Friday    | 20:00        |
| Saturday  | 23:30        |

Scheduler wiring will happen AFTER the parser is implemented.

## Current status

| Component              | Status |
|------------------------|--------|
| Auth (login/refresh)   | Done (reused from existing agent) |
| `fetch_report_list()`  | Done (handles 404 gracefully) |
| `find_employer_report_id()` | TODO — need live response to identify report |
| `fetch_employer_report()` | TODO — need to know download URL pattern |
| `parse_employer_report()` | TODO — need sample Excel/PDF file |
| `update_employee_hours()` | TODO — matching logic ready via `_employee_matching.py` |
| `run_for_branch()`     | TODO — wire after all above done |
| Scheduler wiring       | TODO — do NOT wire until agent is functional |

## Shared matching logic

Employee name matching was extracted from `gmail_agent.py` into
`agents/_employee_matching.py`. Both the current agent and the new one import from there:

- `match_employee_name(name, db_employees, branch_name, branch_id)` -> `(id, confidence, name, rate)`
- `_clean_name(name, branch_name)` -> stripped name (removes store suffixes)

## How to capture the live response

1. Open store during business hours (Sun-Thu 06:30-23:30)
2. Run: `python3 scripts/probe_aviv_reports.py` (already exists)
3. Or use Network tab in Aviv BI web app (bi-aviv.web.app) when viewing reports
4. Save the JSON response to understand the report list structure
5. Download one employer report to see the file format (likely Excel)
6. Implement `find_employer_report_id()` and `parse_employer_report()` based on actual data

## Files

- `agents/aviv_employees_report.py` — skeleton agent
- `agents/_employee_matching.py` — shared matching logic
- `scripts/test_aviv_employees_report_skeleton.py` — smoke tests
- `scripts/probe_aviv_reports.py` — API probe script (pre-existing)
