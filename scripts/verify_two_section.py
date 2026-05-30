"""Verify the /admin/users two-section layout (active + deactivated-with-count).

The page renders client-side: the server HTML carries both static section
headers (חשבונות פעילים / חשבונות מושבתים (<count>)) and the JS filters
/api/admin/users on `active` to place each card. So this verifier asserts BOTH:
  * the two section scaffolding renders, admin-only; and
  * the `active` flag in /api/admin/users (which drives placement) moves a
    throwaway user between sections on deactivate/reactivate.
Creates + removes its own throwaway user. Read-only against real users.

Run on the server:  venv/bin/python scripts/verify_two_section.py
"""
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import app, DB_PATH

PASS, FAIL = [], []


def check(label, cond, detail=""):
    (PASS if cond else FAIL).append(label)
    print(f"{'PASS' if cond else 'FAIL'} — {label}" + (f" :: {detail}" if detail else ""))


def db():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


con = db()
admin = con.execute("SELECT id FROM users WHERE role='admin' AND active=1 LIMIT 1").fetchone()
mgr = con.execute("SELECT id FROM users WHERE role='manager' AND active=1 LIMIT 1").fetchone()
admin_id = admin["id"] if admin else None
con.close()
check("an admin account exists to act as", admin_id is not None)

TEST_EMAIL = "twosection_test@example.com"


def feed_active(client, email):
    """Return the `active` flag for `email` from /api/admin/users (drives section)."""
    for u in client.get("/api/admin/users").get_json() or []:
        if (u.get("email") or "").lower() == email.lower():
            return u.get("active")
    return None


with app.test_client() as c:
    with c.session_transaction() as s:
        s["user_id"] = admin_id
        s["user_role"] = "admin"

    # 1) Server render: both sections + count span, admin-only.
    r = c.get("/admin/users")
    html = r.get_data(as_text=True)
    check("/admin/users renders 200 for admin", r.status_code == 200, f"status={r.status_code}")
    check("active section header present (חשבונות פעילים)", "חשבונות פעילים" in html)
    check("deactivated section header present (חשבונות מושבתים)", "חשבונות מושבתים" in html)
    check("deactivated-count element present (id=inactive-count)", 'id="inactive-count"' in html)

    # 2) Demo CEO placement — report its actual active flag (0 -> deactivated section).
    ceo_active = feed_active(c, "demo@makoletchain.com")
    check("Demo CEO present in users feed", ceo_active is not None, "demo@makoletchain.com not found")
    print(f"   -> Demo CEO active flag = {ceo_active} "
          f"({'DEACTIVATED section' if ceo_active == 0 else 'ACTIVE section'})")

    # 3) Throwaway move flow: create -> active section.
    con = db(); con.execute("DELETE FROM users WHERE email=?", (TEST_EMAIL,)); con.commit(); con.close()
    r = c.post("/api/admin/users", json={"name": "TwoSection Test", "email": TEST_EMAIL,
                                         "password": "test123", "role": "manager"})
    tid = (r.get_json() or {}).get("user_id")
    check("created throwaway user", r.status_code == 201 and tid, f"status={r.status_code}")
    check("throwaway starts in ACTIVE section (active=1)", feed_active(c, TEST_EMAIL) == 1)

    # deactivate -> moves to deactivated section.
    r = c.post(f"/api/admin/users/{tid}/active", json={"active": 0})
    check("deactivate returns 200", r.status_code == 200, f"status={r.status_code}")
    check("throwaway MOVED to deactivated section (active=0)", feed_active(c, TEST_EMAIL) == 0)

    # reactivate -> moves back up.
    r = c.post(f"/api/admin/users/{tid}/active", json={"active": 1})
    check("reactivate returns 200", r.status_code == 200, f"status={r.status_code}")
    check("throwaway MOVED back to active section (active=1)", feed_active(c, TEST_EMAIL) == 1)

    # 4) admin-only.
    with c.session_transaction() as s:
        s["user_id"] = mgr["id"] if mgr else 999999
        s["user_role"] = "manager"
    check("non-admin (manager) blocked from /admin/users (403)",
          c.get("/admin/users").status_code == 403)

# Cleanup throwaway.
con = db(); con.execute("DELETE FROM users WHERE email=?", (TEST_EMAIL,)); con.commit()
inactive_total = con.execute("SELECT COUNT(*) n FROM users WHERE active=0").fetchone()["n"]
con.close()
print(f"\n[state] deactivated users now: {inactive_total} (this is the inactive-count the page shows)")

print(f"\n{len(PASS)}/{len(PASS)+len(FAIL)} passed")
if FAIL:
    print("FAILURES: " + "; ".join(FAIL))
    sys.exit(1)
