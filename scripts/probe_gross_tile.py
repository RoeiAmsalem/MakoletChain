"""Read-only probe: what the home profit tile shows for רווח גולמי.

Calls /api/summary through the Flask test client with an admin session —
the exact code path the tile uses (including today's-live income handling) —
and prints gross ₪ + %, plus the operating profit for comparison.

Usage: venv/bin/python scripts/probe_gross_tile.py [--json] [branch_id ...]
Defaults to 9018 9015, current month. --json prints the raw /api/summary
response per branch instead of the one-line summary.
"""
import json
import sys

from app import app, get_db


def main():
    argv = sys.argv[1:]
    as_json = '--json' in argv
    branch_ids = [int(a) for a in argv if a != '--json'] or [9018, 9015]
    with app.test_client() as client:
        for bid in branch_ids:
            with client.session_transaction() as s:
                s['user_id'] = 0
                s['user_name'] = 'probe'
                s['user_role'] = 'admin'
                s['user_email'] = 'probe@local'
                s['user_branches'] = []
                s['branch_id'] = bid
            resp = client.get('/api/summary')
            if resp.status_code != 200:
                print(f"[{bid}] HTTP {resp.status_code}")
                continue
            d = resp.get_json()
            if as_json:
                print(json.dumps({'branch_id': bid, 'summary': d},
                                 ensure_ascii=False))
                continue
            with app.app_context():
                name = get_db().execute(
                    'SELECT name FROM branches WHERE id=?', (bid,)
                ).fetchone()
            name = name['name'] if name else '?'
            gross = d.get('gross')
            gross_pct = d.get('gross_pct')
            gross_s = (f"{gross:,.0f} ({gross_pct}%)"
                       if gross is not None else "—")
            print(f"[{bid}] {name}: income {d['income']:,.0f} | "
                  f"goods {d['goods']:,.0f} | GROSS {gross_s} | "
                  f"operating {d['profit']:,.0f}")


if __name__ == '__main__':
    main()
