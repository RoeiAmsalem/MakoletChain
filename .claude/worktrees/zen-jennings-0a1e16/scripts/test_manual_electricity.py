#!/usr/bin/env python3
"""Tests for manual electricity entry feature."""

import json
import os
import sqlite3
import sys
import tempfile
import shutil

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')

passed = 0
failed = 0


def check(name, condition):
    global passed, failed
    if condition:
        print(f"  ✅ {name}")
        passed += 1
    else:
        print(f"  ❌ {name}")
        failed += 1


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def get_test_client():
    """Create a Flask test client with a logged-in session."""
    os.environ.setdefault('SECRET_KEY', 'test-secret')
    from app import app
    app.config['TESTING'] = True
    client = app.test_client()
    return client, app


def login_as_admin(client):
    """Log in as admin user via session."""
    with client.session_transaction() as sess:
        sess['user_id'] = 1
        sess['user_role'] = 'admin'
        sess['user_name'] = 'Admin'
        sess['branch_id'] = 126
        sess['user_branches'] = [126, 127]


print("\n🔌 Manual Electricity Tests\n")

# ── Test 1: Schema migration applied ────────────────────────────
print("1. Schema checks")
db = get_db()
# Check electricity_source column exists
branch = db.execute("SELECT electricity_source FROM branches LIMIT 1").fetchone()
check("branches.electricity_source column exists", branch is not None)

# Check month column on electricity_invoices
cols = [r[1] for r in db.execute("PRAGMA table_info(electricity_invoices)").fetchall()]
check("electricity_invoices.month column exists", 'month' in cols)

# Check IEC branches have electricity_source='iec'
iec_branches = db.execute(
    "SELECT id, electricity_source FROM branches WHERE iec_token IS NOT NULL"
).fetchall()
for b in iec_branches:
    check(f"Branch {b['id']} with IEC token has electricity_source='iec'", b['electricity_source'] == 'iec')

# Check non-IEC branches have NULL
non_iec = db.execute(
    "SELECT id, electricity_source FROM branches WHERE iec_token IS NULL AND active = 1"
).fetchall()
for b in non_iec:
    check(f"Branch {b['id']} without IEC token has electricity_source=NULL", b['electricity_source'] is None)
db.close()

# ── Test 2: API endpoints via Flask test client ─────────────────
print("\n2. API endpoint tests")
client, app = get_test_client()
login_as_admin(client)

# Find a branch to test with — use one without IEC
db = get_db()
test_branch = db.execute(
    "SELECT id FROM branches WHERE iec_token IS NULL AND active = 1 LIMIT 1"
).fetchone()
db.close()

if test_branch:
    test_bid = test_branch['id']
    print(f"   Using branch {test_bid} for testing")

    # Set session to this branch
    with client.session_transaction() as sess:
        sess['branch_id'] = test_bid

    # Test: GET /api/electricity/status
    resp = client.get('/api/electricity/status')
    check("GET /api/electricity/status returns 200", resp.status_code == 200)
    status_data = json.loads(resp.data)
    check("Status shows source=null for unconfigured branch", status_data['source'] is None)

    # Test: POST manual entry on unconfigured branch (should work and auto-set source)
    resp = client.post('/api/electricity/manual',
                       data=json.dumps({'month': '2026-05', 'amount': 3000.00}),
                       content_type='application/json')
    check("POST /api/electricity/manual returns 200", resp.status_code == 200)

    # Verify row was created
    db = get_db()
    row = db.execute(
        "SELECT * FROM electricity_invoices WHERE branch_id = ? AND source = 'manual' AND month = '2026-05'",
        (test_bid,)
    ).fetchone()
    check("Manual entry created in DB", row is not None)
    check("Manual entry has correct amount", row and row['amount'] == 3000.00)

    # Verify branch electricity_source auto-set to 'manual'
    branch = db.execute("SELECT electricity_source FROM branches WHERE id = ?", (test_bid,)).fetchone()
    check("Branch electricity_source auto-set to 'manual'", branch['electricity_source'] == 'manual')
    db.close()

    # Test: PUT manual entry update
    if row:
        entry_id = row['id']
        resp = client.put(f'/api/electricity/manual/{entry_id}',
                          data=json.dumps({'amount': 3500.00}),
                          content_type='application/json')
        check("PUT /api/electricity/manual/<id> returns 200", resp.status_code == 200)

        db = get_db()
        updated = db.execute("SELECT amount FROM electricity_invoices WHERE id = ?", (entry_id,)).fetchone()
        check("Manual entry updated to new amount", updated and updated['amount'] == 3500.00)
        db.close()

    # Test: GET /api/electricity/status after manual entry
    resp = client.get('/api/electricity/status')
    status_data = json.loads(resp.data)
    check("Status shows source='manual' after entry", status_data['source'] == 'manual')

    # Test: GET /api/electricity/history
    resp = client.get('/api/electricity/history')
    check("GET /api/electricity/history returns 200", resp.status_code == 200)
    history = json.loads(resp.data)
    check("History contains the manual entry", any(h['source'] == 'manual' for h in history))

    # Test: get_electricity_for_month returns manual entry
    with app.app_context():
        from app import get_electricity_for_month, get_db as app_get_db
        db = app_get_db()
        elec = get_electricity_for_month(test_bid, 2026, 5, db)
        check("get_electricity_for_month returns manual entry", elec['amount'] == 3500.00)
        check("get_electricity_for_month source is 'manual'", elec['source'] == 'manual')

        # Test: missing month returns manual_missing
        elec_missing = get_electricity_for_month(test_bid, 2026, 1, db)
        check("Missing month returns source='manual_missing'", elec_missing['source'] == 'manual_missing')
        check("Missing month returns amount=0", elec_missing['amount'] == 0)

    # Test: source switch manual -> iec
    resp = client.post('/api/electricity/source',
                       data=json.dumps({'source': 'iec'}),
                       content_type='application/json')
    check("POST /api/electricity/source returns 200", resp.status_code == 200)

    db = get_db()
    branch = db.execute("SELECT electricity_source FROM branches WHERE id = ?", (test_bid,)).fetchone()
    check("Branch source switched to 'iec'", branch['electricity_source'] == 'iec')

    # Verify old manual rows still exist (future-only)
    manual_rows = db.execute(
        "SELECT COUNT(*) as cnt FROM electricity_invoices WHERE branch_id = ? AND source = 'manual'",
        (test_bid,)
    ).fetchone()
    check("Manual rows preserved after source switch", manual_rows['cnt'] > 0)
    db.close()

    # Test: trying manual entry on IEC-mode branch returns 409
    resp = client.post('/api/electricity/manual',
                       data=json.dumps({'month': '2026-06', 'amount': 2000.00}),
                       content_type='application/json')
    check("Manual entry on IEC-mode branch returns 409", resp.status_code == 409)

    # Test: source switch back to manual
    resp = client.post('/api/electricity/source',
                       data=json.dumps({'source': 'manual'}),
                       content_type='application/json')
    check("Source switch back to manual works", resp.status_code == 200)

    # Test: invalid source
    resp = client.post('/api/electricity/source',
                       data=json.dumps({'source': 'invalid'}),
                       content_type='application/json')
    check("Invalid source returns 400", resp.status_code == 400)

    # Test: PUT on IEC entry should fail
    db = get_db()
    iec_row = db.execute(
        "SELECT id FROM electricity_invoices WHERE branch_id != ? AND source = 'iec_api' LIMIT 1",
        (test_bid,)
    ).fetchone()
    db.close()
    if iec_row:
        resp = client.put(f'/api/electricity/manual/{iec_row["id"]}',
                          data=json.dumps({'amount': 999}),
                          content_type='application/json')
        # Might be 403 or 404 (different branch), either is correct
        check("PUT on IEC entry returns error", resp.status_code in (403, 404))

    # ── Cleanup ──────────────────────────────────────────────
    print("\n3. Cleanup")
    db = get_db()
    db.execute("DELETE FROM electricity_invoices WHERE branch_id = ? AND source = 'manual'", (test_bid,))
    db.execute("UPDATE branches SET electricity_source = NULL WHERE id = ?", (test_bid,))
    db.commit()
    db.close()
    check("Test data cleaned up", True)

else:
    print("   ⚠️  No unconfigured branch found for testing — skipping API tests")

# ── Summary ──────────────────────────────────────────────────────
print(f"\n{'='*50}")
print(f"Results: {passed} passed, {failed} failed")
if failed:
    print("❌ Some tests failed!")
    sys.exit(1)
else:
    print("✅ All tests passed!")
    sys.exit(0)
