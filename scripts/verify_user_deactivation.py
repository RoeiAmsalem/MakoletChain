"""Verify the /admin/users deactivate/reactivate feature on staging.

Exercises the REAL Flask routes via test_client + session_transaction (so
_admin_required and the self-deactivation block run exactly as in prod), and
asserts DB state with a direct sqlite connection. Creates and removes its own
throwaway test user — leaves no trace. Read-only against real users.

Run on the server:  venv/bin/python scripts/verify_user_deactivation.py
"""
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import app, DB_PATH


def db():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


PASS, FAIL = [], []


def check(label, cond, detail=""):
    (PASS if cond else FAIL).append(label)
    print(f"{'PASS' if cond else 'FAIL'} — {label}" + (f" :: {detail}" if detail else ""))


# Column + baseline ------------------------------------------------------------
con = db()
cols = [r["name"] for r in con.execute("PRAGMA table_info(users)")]
check("users.active column exists", "active" in cols, f"cols={cols}")
inactive_before = con.execute("SELECT COUNT(*) n FROM users WHERE active=0").fetchone()["n"]
total_users = con.execute("SELECT COUNT(*) n FROM users").fetchone()["n"]
check("no existing user is inactive (migration locked nobody out)",
      inactive_before == 0, f"{inactive_before} inactive of {total_users}")

admin = con.execute("SELECT id, email FROM users WHERE role='admin' AND active=1 LIMIT 1").fetchone()
check("an admin account exists to act as", admin is not None)
admin_id = admin["id"] if admin else None
con.close()

TEST_EMAIL = "deactivation_test@example.com"

with app.test_client() as c:
    # Act as admin for setup + most tests.
    with c.session_transaction() as s:
        s["user_id"] = admin_id
        s["user_role"] = "admin"

    # Clean any leftover fixture, then create a fresh test manager via the API.
    con = db(); con.execute("DELETE FROM users WHERE email=?", (TEST_EMAIL,)); con.commit(); con.close()
    r = c.post("/api/admin/users", json={
        "name": "Deactivation Test", "email": TEST_EMAIL,
        "password": "test123", "role": "manager"})
    check("created throwaway test user", r.status_code == 201, f"status={r.status_code} body={r.get_json()}")
    target_id = (r.get_json() or {}).get("user_id")

    # 1) Self-deactivation blocked server-side.
    r = c.post(f"/api/admin/users/{admin_id}/active", json={"active": 0})
    body = r.get_json() or {}
    check("admin CANNOT deactivate own account (403 + clear msg)",
          r.status_code == 403 and "own account" in (body.get("error") or ""),
          f"status={r.status_code} body={body}")
    con = db()
    still_active = con.execute("SELECT active FROM users WHERE id=?", (admin_id,)).fetchone()["active"]
    con.close()
    check("admin's own account still active after blocked attempt", still_active == 1)

    # 2) Deactivate the test user → DB active=0, login query rejects it.
    r = c.post(f"/api/admin/users/{target_id}/active", json={"active": 0})
    check("admin deactivated test user (200)", r.status_code == 200, f"status={r.status_code}")
    con = db()
    row = con.execute("SELECT active FROM users WHERE id=?", (target_id,)).fetchone()
    check("test user active=0 in DB", row["active"] == 0)
    # The login path is: SELECT * FROM users WHERE LOWER(email)=? AND active=1.
    login_hit = con.execute(
        "SELECT id FROM users WHERE LOWER(email)=? AND active=1", (TEST_EMAIL,)).fetchone()
    check("login query returns NO row for deactivated user (login blocked)", login_hit is None)
    con.close()

    # 3) Reactivate → DB active=1, login query finds it again.
    r = c.post(f"/api/admin/users/{target_id}/active", json={"active": 1})
    check("admin reactivated test user (200)", r.status_code == 200, f"status={r.status_code}")
    con = db()
    row = con.execute("SELECT active FROM users WHERE id=?", (target_id,)).fetchone()
    login_hit = con.execute(
        "SELECT id FROM users WHERE LOWER(email)=? AND active=1", (TEST_EMAIL,)).fetchone()
    con.close()
    check("test user active=1 after reactivate", row["active"] == 1)
    check("login query finds reactivated user again", login_hit is not None)

    # 4) Non-admin (manager) hitting the endpoint → 403.
    with c.session_transaction() as s:
        s["user_id"] = target_id
        s["user_role"] = "manager"
    r = c.post(f"/api/admin/users/{target_id}/active", json={"active": 0})
    check("non-admin (manager) gets 403 on the endpoint", r.status_code == 403, f"status={r.status_code}")

    # 5) CEO hitting the endpoint → 403 (operator-only surface).
    with c.session_transaction() as s:
        s["user_role"] = "ceo"
    r = c.post(f"/api/admin/users/{target_id}/active", json={"active": 0})
    check("non-admin (ceo) gets 403 on the endpoint", r.status_code == 403, f"status={r.status_code}")

# Cleanup fixture.
con = db(); con.execute("DELETE FROM users WHERE email=?", (TEST_EMAIL,)); con.commit(); con.close()

print(f"\n{len(PASS)}/{len(PASS)+len(FAIL)} passed")
if FAIL:
    print("FAILURES: " + "; ".join(FAIL))
    sys.exit(1)
