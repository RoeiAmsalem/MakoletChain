#!/usr/bin/env python3
"""Screenshot the מלא / עד היום P/L toggle in both modes (home + fixed-expenses).

Self-contained: creates a temporary branch-126 manager, drives the LOCAL Flask
app (http://localhost:8081, bypassing nginx basic-auth) with Playwright, captures
both modes, then deletes the temp user. Run on the staging server with its venv:

    cd /opt/makolet-chain-staging && PYTHONPATH=. venv/bin/python scripts/screenshot_mtd_toggle.py

PNGs are written to /tmp/mtd_*.png.
"""
import sqlite3
from werkzeug.security import generate_password_hash
from playwright.sync_api import sync_playwright

import app as A

BASE = "http://localhost:8081"
BRANCH = 126
EMAIL = "mtd-shot@test.local"
PASSWORD = "shot12345"
OUT = "/tmp"


def make_user(db):
    # admin role so the "תצוגה על סניף / כל הרשת" control renders alongside the
    # P/L toggle; branch 126 assigned so the home shows that branch's KPIs.
    db.execute("DELETE FROM user_branches WHERE user_id IN (SELECT id FROM users WHERE email=?)", (EMAIL,))
    db.execute("DELETE FROM users WHERE email=?", (EMAIL,))
    cur = db.execute(
        "INSERT INTO users (name, email, password_hash, role, active) VALUES (?,?,?,'admin',1)",
        ("MTD Shot", EMAIL, generate_password_hash(PASSWORD)),
    )
    uid = cur.lastrowid
    db.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (?,?)", (uid, BRANCH))
    db.commit()
    return uid


def drop_user(db):
    db.execute("DELETE FROM user_branches WHERE user_id IN (SELECT id FROM users WHERE email=?)", (EMAIL,))
    db.execute("DELETE FROM users WHERE email=?", (EMAIL,))
    db.commit()


def login(pg):
    pg.goto(f"{BASE}/login", wait_until="domcontentloaded")
    pg.fill("input[name=email]", EMAIL)
    pg.fill("input[name=password]", PASSWORD)
    pg.click("button[type=submit]")
    pg.wait_for_url(f"{BASE}/", wait_until="domcontentloaded")


def shoot():
    with sync_playwright() as p:
        b = p.chromium.launch(args=["--no-sandbox"])

        # ---------- DESKTOP ----------
        pg = b.new_page(viewport={"width": 1180, "height": 1000})
        login(pg)
        pg.wait_for_selector("#pl-mode-mtd", state="visible", timeout=15000)
        pg.wait_for_timeout(1200)
        # top control row — both pills side by side
        pg.locator(".top-controls").screenshot(path=f"{OUT}/mtd_toprow_full.png")
        pg.locator("#kpi-section").screenshot(path=f"{OUT}/mtd_home_full.png")
        pg.click("#pl-mode-mtd")
        pg.wait_for_timeout(600)
        pg.locator(".top-controls").screenshot(path=f"{OUT}/mtd_toprow_mtd.png")
        pg.locator("#kpi-section").screenshot(path=f"{OUT}/mtd_home_mtd.png")

        # fixed-expenses page (own toggle)
        pg.goto(f"{BASE}/fixed-expenses", wait_until="domcontentloaded")
        pg.wait_for_selector("#pl-mode-mtd", state="visible", timeout=15000)
        pg.wait_for_timeout(1000)
        pg.screenshot(path=f"{OUT}/mtd_fixed_full.png", clip={"x": 0, "y": 0, "width": 1180, "height": 360})
        pg.click("#pl-mode-mtd")
        pg.wait_for_timeout(600)
        pg.screenshot(path=f"{OUT}/mtd_fixed_mtd.png", clip={"x": 0, "y": 0, "width": 1180, "height": 360})

        # ---------- MOBILE 390px ----------
        mctx = b.new_context(viewport={"width": 390, "height": 844}, device_scale_factor=2)
        mp = mctx.new_page()
        login(mp)
        mp.wait_for_selector("#pl-mode-mtd", state="visible", timeout=15000)
        mp.wait_for_timeout(1200)
        mp.screenshot(path=f"{OUT}/mtd_mobile_full.png", clip={"x": 0, "y": 0, "width": 390, "height": 520})
        mp.click("#pl-mode-mtd")
        mp.wait_for_timeout(600)
        mp.screenshot(path=f"{OUT}/mtd_mobile_mtd.png", clip={"x": 0, "y": 0, "width": 390, "height": 520})

        b.close()


def main():
    db = sqlite3.connect(A.DB_PATH, timeout=30)
    db.row_factory = sqlite3.Row
    try:
        make_user(db)
        shoot()
        print("screenshots written to /tmp/mtd_*.png")
    finally:
        drop_user(db)
        db.close()


if __name__ == "__main__":
    main()
