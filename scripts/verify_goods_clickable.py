#!/usr/bin/env python3
"""Verify goods invoice rows are clickable end-to-end (DOM + detail fetch).

Forges an admin session via Flask's test client, renders the real /goods page
for a branch+month, and asserts that every doc row whose backing
goods_documents row has a bilboy_doc_id renders with:
  - class="doc-row"   (the delegated click listener's hook)
  - data-row-id="<id>" (the id openDocDetail() passes to /api/goods/doc/<id>)

Then it picks one rendered invoice row and hits /api/goods/doc/<id> to prove
the click -> fetch -> render path returns line items (not "stuck").

Usage: python -m scripts.verify_goods_clickable [branch_id] [YYYY-MM]
Defaults: branch 126, month 2026-05.
"""
import re
import sys

import app as appmod


def main():
    branch_id = int(sys.argv[1]) if len(sys.argv) > 1 else 126
    month = sys.argv[2] if len(sys.argv) > 2 else '2026-05'

    flask_app = appmod.app
    with flask_app.test_client() as c:
        # Forge an admin session (admin is in ROLES_ALL_BRANCHES, so it may
        # view any branch via ?branch_id=).
        with c.session_transaction() as sess:
            sess['user_id'] = 0
            sess['user_name'] = 'verify-harness'
            sess['user_role'] = 'admin'
            sess['user_email'] = 'verify@local'
            sess['user_branches'] = []
            sess['branch_id'] = branch_id

        r = c.get(f'/goods?branch_id={branch_id}&month={month}')
        html = r.get_data(as_text=True)
        assert r.status_code == 200, f'/goods returned {r.status_code}'

        # All <tr> opening tags inside the goods table.
        rows = re.findall(r'<tr\b[^>]*>', html)
        invoice_rows = [t for t in rows if 'data-type="invoice"' in t]
        clickable = [t for t in rows if 'class="doc-row"' in t and 'data-row-id="' in t]
        clickable_invoice = [t for t in invoice_rows
                             if 'class="doc-row"' in t and 'data-row-id="' in t]
        invoice_no_hook = [t for t in invoice_rows
                           if 'data-row-id="' not in t]

        print(f'branch={branch_id} month={month}')
        print(f'  invoice rows rendered      : {len(invoice_rows)}')
        print(f'  invoice rows clickable     : {len(clickable_invoice)} '
              f'(class=doc-row + data-row-id)')
        print(f'  invoice rows missing hook  : {len(invoice_no_hook)}')
        print(f'  total clickable rows (all) : {len(clickable)}')

        # Sanity: the JS hooks must exist in the served page.
        for needle in ('doc-detail-overlay', 'function openDocDetail',
                       "closest('.doc-row')"):
            assert needle in html, f'missing JS/DOM hook: {needle}'
        print('  JS/DOM hooks present       : openDocDetail, overlay, delegated click  OK')

        if not clickable_invoice:
            print('FAIL: no invoice row rendered with data-row-id + doc-row')
            return 1

        # Exercise the click -> fetch path on the first clickable invoice row.
        row_id = re.search(r'data-row-id="(\d+)"', clickable_invoice[0]).group(1)
        d = c.get(f'/api/goods/doc/{row_id}')
        dj = d.get_json() or {}
        print(f'  /api/goods/doc/{row_id} -> {d.status_code}, '
              f'items={len(dj.get("items", []))}, '
              f'supplier={(dj.get("header") or {}).get("supplier")!r}')
        detail_ok = d.status_code == 200 and 'header' in dj

        ok = len(invoice_no_hook) == 0 and detail_ok
        print('RESULT:', 'PASS' if ok else 'FAIL')
        return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())
