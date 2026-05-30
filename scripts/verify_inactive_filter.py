"""Verify the /admin/users two-section layout (active vs inactive) on staging.

Sections are rendered client-side, so this checks: (1) admin-only gating,
(2) the page ships both section headers + the active/inactive render split,
(3) the deactivate/reactivate data flow that MOVES a card between the two
lists actually flips the `active` flag the split reads. Creates and removes
its own throwaway user.

Run on the server:  PYTHONPATH=. venv/bin/python scripts/verify_inactive_filter.py
"""
import sqlite3
import sys

from app import app, DB_PATH

PASS, FAIL = [], []


def check(label, cond, detail=""):
    (PASS if cond else FAIL).append(label)
    print(f"{'PASS' if cond else 'FAIL'} — {label}" + (f" :: {detail}" if detail else ""))


def db():
    c = sqlite3.connect(DB_PATH); c.row_factory = sqlite3.Row
    return c


con = db()
admin = con.execute("SELECT id FROM users WHERE role='admin' AND active=1 LIMIT 1").fetchone()
mgr = con.execute("SELECT id FROM users WHERE role='manager' LIMIT 1").fetchone()
con.close()

TEST_EMAIL = "section_move_test@example.com"

with app.test_client() as c:
    # Gating ------------------------------------------------------------------
    r = c.get("/admin/users")
    check("anonymous redirected (not 200)", r.status_code in (301, 302), f"status={r.status_code}")
    with c.session_transaction() as s:
        s["user_id"] = mgr["id"]; s["user_role"] = "manager"
    r = c.get("/admin/users")
    check("non-admin (manager) gets 403", r.status_code == 403, f"status={r.status_code}")

    # Structure ---------------------------------------------------------------
    with c.session_transaction() as s:
        s["user_id"] = admin["id"]; s["user_role"] = "admin"
    r = c.get("/admin/users")
    html = r.get_data(as_text=True)
    check("admin gets 200", r.status_code == 200, f"status={r.status_code}")
    check("has 'חשבונות פעילים' section", "חשבונות פעילים" in html)
    check("has 'חשבונות מושבתים' section + count span", "חשבונות מושבתים" in html and 'id="inactive-count"' in html)
    check("has active + inactive containers", 'id="active-users-container"' in html and 'id="inactive-users-container"' in html)
    check("render splits on active flag",
          "ALL_USERS.filter(u => u.active)" in html and "ALL_USERS.filter(u => !u.active)" in html)
    check("old toggle removed", "SHOW_INACTIVE" not in html and "show-inactive" not in html)
    check("reactivate path intact (setActive present)", "function setActive(" in html)

    # Move data flow: create → deactivate → reactivate ------------------------
    con = db(); con.execute("DELETE FROM users WHERE email=?", (TEST_EMAIL,)); con.commit(); con.close()
    r = c.post("/api/admin/users", json={"name": "Section Move", "email": TEST_EMAIL,
                                         "password": "test123", "role": "manager"})
    tid = (r.get_json() or {}).get("user_id")
    check("created throwaway test user (lands active)", r.status_code == 201)

    def active_of(uid):
        users = c.get("/api/admin/users").get_json() or []
        return next((u["active"] for u in users if u["id"] == uid), None)

    check("new user is in ACTIVE group (active=1)", active_of(tid) == 1)
    r = c.post(f"/api/admin/users/{tid}/active", json={"active": 0})
    check("deactivate → moves to INACTIVE group (active=0)",
          r.status_code == 200 and active_of(tid) == 0)
    r = c.post(f"/api/admin/users/{tid}/active", json={"active": 1})
    check("reactivate → moves back to ACTIVE group (active=1)",
          r.status_code == 200 and active_of(tid) == 1)

con = db(); con.execute("DELETE FROM users WHERE email=?", (TEST_EMAIL,)); con.commit(); con.close()

print(f"\n{len(PASS)}/{len(PASS)+len(FAIL)} passed")
if FAIL:
    print("FAILURES: " + "; ".join(FAIL))
    sys.exit(1)
