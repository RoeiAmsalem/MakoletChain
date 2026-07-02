"""Run the read-only SUMIT billing sync once from the shell — the exact code
path behind the /admin/billing רענן סטטוס button (_run_billing_sync).

READ-ONLY vs SUMIT; writes only manager_billing.last_paid_date / last_status /
updated_at. Prints the sync summary dict."""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from app import app, get_db, _run_billing_sync  # noqa: E402

with app.test_request_context():
    result = _run_billing_sync(get_db())
    print(result)
    sys.exit(0 if result.get('connected') and not result.get('error') else 1)
