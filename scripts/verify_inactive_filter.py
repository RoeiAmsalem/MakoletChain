"""Verify the /admin/users 'hide inactive' display filter on staging.

The filter is client-side JS, so this checks: (1) admin-only gating on the
page, (2) the page renders and ships the filter controls + default-hide logic,
(3) the API still exposes the `active` flag the filter depends on.

Run on the server:  PYTHONPATH=. venv/bin/python scripts/verify_inactive_filter.py
"""
import sqlite3
import sys

from app import app, DB_PATH

PASS, FAIL = [], []


def check(label, cond, detail=""):
    (PASS if cond else FAIL).append(label)
    print(f"{'PASS' if cond else 'FAIL'} — {label}" + (f" :: {detail}" if detail else ""))


con = sqlite3.connect(DB_PATH); con.row_factory = sqlite3.Row
admin = con.execute("SELECT id FROM users WHERE role='admin' AND active=1 LIMIT 1").fetchone()
mgr = con.execute("SELECT id FROM users WHERE role='manager' LIMIT 1").fetchone()
con.close()

with app.test_client() as c:
    # Anonymous → redirect to login.
    r = c.get("/admin/users")
    check("anonymous redirected (not 200)", r.status_code in (301, 302), f"status={r.status_code}")

    # Non-admin manager → 403.
    with c.session_transaction() as s:
        s["user_id"] = mgr["id"]; s["user_role"] = "manager"
    r = c.get("/admin/users")
    check("non-admin (manager) gets 403 on /admin/users", r.status_code == 403, f"status={r.status_code}")

    # Admin → 200 + filter controls + default-hide logic present.
    with c.session_transaction() as s:
        s["user_id"] = admin["id"]; s["user_role"] = "admin"
    r = c.get("/admin/users")
    html = r.get_data(as_text=True)
    check("admin gets 200", r.status_code == 200, f"status={r.status_code}")
    check("page has 'הצג מושבתים' toggle", "הצג מושבתים" in html)
    check("page has show-inactive checkbox", 'id="show-inactive"' in html)
    check("default-hide filter present (filters on active)",
          "SHOW_INACTIVE ? ALL_USERS : ALL_USERS.filter(u => u.active)" in html)
    check("SHOW_INACTIVE defaults to false (hidden by default)",
          "let SHOW_INACTIVE = false;" in html)
    check("reactivate path intact (setActive present)", "function setActive(" in html)

    # API still exposes the active flag the filter reads.
    r = c.get("/api/admin/users")
    data = r.get_json() or []
    check("API returns users with 'active' field", bool(data) and all("active" in u for u in data),
          f"n={len(data)}")

print(f"\n{len(PASS)}/{len(PASS)+len(FAIL)} passed")
if FAIL:
    print("FAILURES: " + "; ".join(FAIL))
    sys.exit(1)
