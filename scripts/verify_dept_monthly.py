"""Verify the /api/department-sales-monthly computation against the DB.

Replicates the endpoint's exact logic (equal-weight average of daily
dept-share-of-Z percentages over qualifying days) and prints, per branch,
the per-day breakdown plus the resulting tile numbers so they can be
eyeballed against the live endpoint. Read-only.

Run on staging:
  /opt/makolet-chain-staging/venv/bin/python \
    /opt/makolet-chain-staging/scripts/verify_dept_monthly.py 2026-05
"""
import sqlite3
import sys
import os

MONTH = sys.argv[1] if len(sys.argv) > 1 else '2026-05'
DB = os.environ.get('DB_PATH') or os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'db', 'makolet_chain.db')
CODES = [5, 83, 2]
LABELS = {5: 'חלב', 83: 'סיגריות', 2: 'ירקות'}

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

branches = [r['branch_id'] for r in conn.execute(
    "SELECT DISTINCT branch_id FROM z_department_sales "
    "WHERE strftime('%Y-%m', date)=? ORDER BY branch_id", (MONTH,))]

for bid in branches:
    day_rows = conn.execute(
        "SELECT ds.date AS date, ds.amount AS z FROM daily_sales ds "
        "WHERE ds.branch_id=? AND strftime('%Y-%m', ds.date)=? "
        "AND ds.amount > 0 AND ds.source NOT IN ('live_provisional','provisional') "
        "AND EXISTS (SELECT 1 FROM z_department_sales z "
        "            WHERE z.branch_id=ds.branch_id AND z.date=ds.date) "
        "ORDER BY ds.date ASC", (bid, MONTH)).fetchall()
    qualifying = {r['date']: r['z'] for r in day_rows}

    dept_rows = conn.execute(
        "SELECT date, dept_code, amount FROM z_department_sales "
        "WHERE branch_id=? AND strftime('%Y-%m', date)=? "
        "AND dept_code IN (5,83,2)", (bid, MONTH)).fetchall()
    by_code = {c: {} for c in CODES}
    for r in dept_rows:
        if r['date'] in qualifying:
            by_code[r['dept_code']][r['date']] = r['amount']

    print(f"\n=== branch {bid} — {MONTH} — {len(qualifying)} qualifying day(s) ===")
    for c in CODES:
        per_day = by_code[c]
        total = 0.0
        pct_sum = 0.0
        parts = []
        for date, z in qualifying.items():
            amt = per_day.get(date, 0.0) or 0.0
            total += amt
            pct = (amt / z * 100) if z else 0
            pct_sum += pct
            parts.append(f"{date}:{amt:.0f}/{z:.0f}={pct:.1f}%")
        avg = round(pct_sum / len(qualifying), 1) if qualifying else None
        print(f"  dept {c:>2} ({LABELS[c]}): avg_pct={avg}  total=₪{round(total,2)}")
        for p in parts:
            print(f"        {p}")
conn.close()
