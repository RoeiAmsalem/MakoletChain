"""Read-only probe: /api/sales wolt slice + revenue-total reconciliation.

Calls /api/sales through the Flask test client (the exact tile code path)
for the given branches, prints total / wolt / pct, and reconciles the API
total against a direct daily_sales SUM — proving Wolt changed no totals.

Usage: venv/bin/python scripts/probe_wolt_sales.py [--json] [--month YYYY-MM] [branch_id ...]
Defaults to 9001 9015 126, current month.
"""
import argparse
import json

from app import app, get_db


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--json', action='store_true')
    ap.add_argument('--month', default=None)
    ap.add_argument('branch_ids', nargs='*', type=int,
                    default=[9001, 9015, 126])
    args = ap.parse_args()

    with app.test_client() as client:
        for bid in args.branch_ids:
            with client.session_transaction() as s:
                s['user_id'] = 0
                s['user_name'] = 'probe'
                s['user_role'] = 'admin'
                s['user_email'] = 'probe@local'
                s['user_branches'] = []
                s['branch_id'] = bid
            qs = f'?month={args.month}' if args.month else ''
            resp = client.get(f'/api/sales{qs}')
            if resp.status_code != 200:
                print(f"[{bid}] HTTP {resp.status_code}")
                continue
            d = resp.get_json()
            if args.json:
                print(json.dumps({'branch_id': bid, 'sales': d},
                                 ensure_ascii=False))
                continue
            with app.app_context():
                db = get_db()
                month = args.month
                if not month:
                    from app import _now_il
                    month = _now_il().strftime('%Y-%m')
                raw = db.execute(
                    "SELECT COALESCE(SUM(amount),0) FROM daily_sales "
                    "WHERE branch_id=? AND strftime('%Y-%m', date)=?",
                    (bid, month)).fetchone()[0]
            w = d['wolt']
            w_s = f"{w['amount']:,.2f} ({w['pct']}%)" if w else 'none'
            recon = 'OK' if abs(d['total'] - raw) < 0.01 else \
                f'MISMATCH raw={raw:,.2f}'
            print(f"[{bid}] total {d['total']:,.2f} | wolt {w_s} | "
                  f"recon vs daily_sales SUM: {recon}")


if __name__ == '__main__':
    main()
