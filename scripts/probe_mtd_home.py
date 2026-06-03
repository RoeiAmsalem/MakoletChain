#!/usr/bin/env python3
"""Diagnose why the P/L toggle may not show: login as the temp admin, inspect / and /api/summary."""
import sqlite3, requests
from werkzeug.security import generate_password_hash
import app as A

BASE = "http://localhost:8081"
EMAIL, PW, BRANCH = "mtd-shot@test.local", "shot12345", 126


def main():
    db = sqlite3.connect(A.DB_PATH, timeout=30); db.row_factory = sqlite3.Row
    db.execute("DELETE FROM user_branches WHERE user_id IN (SELECT id FROM users WHERE email=?)", (EMAIL,))
    db.execute("DELETE FROM users WHERE email=?", (EMAIL,))
    cur = db.execute("INSERT INTO users (name,email,password_hash,role,active) VALUES (?,?,?,'admin',1)",
                     ("MTD Shot", EMAIL, generate_password_hash(PW)))
    db.execute("INSERT INTO user_branches (user_id,branch_id) VALUES (?,?)", (cur.lastrowid, BRANCH)); db.commit()
    try:
        s = requests.Session()
        r = s.post(f"{BASE}/login", data={"email": EMAIL, "password": PW}, allow_redirects=True)
        html = r.text
        print("final URL:", r.url, "status:", r.status_code)
        print("is network template:", "home_network" in html or "כל הסניפים" in html)
        print("has #pl-mode-toggle:", "pl-mode-toggle" in html)
        print("has top-controls:", "top-controls" in html)
        api = s.get(f"{BASE}/api/summary?month={A._now_il().strftime('%Y-%m')}&branch_id={BRANCH}").json()
        print("summary: branch", api.get("branch_id"), "income", api.get("income"),
              "mtd_applicable", api.get("mtd_applicable"), "fixed", api.get("fixed"),
              "fixed_mtd", api.get("fixed_mtd"))
    finally:
        db.execute("DELETE FROM user_branches WHERE user_id IN (SELECT id FROM users WHERE email=?)", (EMAIL,))
        db.execute("DELETE FROM users WHERE email=?", (EMAIL,)); db.commit(); db.close()


if __name__ == "__main__":
    main()
