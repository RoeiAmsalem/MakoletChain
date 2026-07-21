"""Run the billing sweep once against this install's DB (source='auto', so the
skip path is eligible) and print the result dict + metered call count.

Staging/prod verification for the payment-resolutions skip fix (migration 041):
run twice — first full (builds resolutions), second must be skipped=1, 1 call.

Usage: venv/bin/python3 scripts/run_billing_sweep_once.py
"""
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app as app_module  # noqa: E402

conn = sqlite3.connect(app_module.DB_PATH, timeout=30)
conn.row_factory = sqlite3.Row
with app_module.app.app_context():
    res = app_module._run_billing_sync_logged(conn, 'auto')
run = conn.execute(
    "SELECT started_at, api_calls, skipped, unmatched, payments_seen, "
    "paid_managers FROM billing_sync_runs ORDER BY id DESC LIMIT 1").fetchone()
print('result:', res)
print('logged:', dict(run) if run else None)
conn.close()
