"""Empirical cancel-semantics matrix — run ONCE against a DISPOSABLE test
subscription before the cancel button ships to prod.

The SUMIT spec documents nothing about double-cancel, bogus ids, or
cancelling an id that belongs to another customer (docs/billing_cancel_spec.md
→ "Idempotency / double-cancel — UNDOCUMENTED"). This script answers all
three empirically, using the sanctioned utils.sumit recurring path.

USAGE (after Roei creates a ₪1 RECURRING test sub and we probe its ids):
    venv/bin/python scripts/cancel_empirical_probe.py \
        --customer <test SUMIT customer id> --item <test recurring item id> --yes

It WILL cancel the given item (that is the point). Guards:
- refuses to run without --yes
- refuses REAL payers' customer/item ids (hard denylist below)
- everything else is read-only listing around the one cancel
"""
import argparse
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from dotenv import load_dotenv  # noqa: E402
load_dotenv(os.path.join(ROOT, '.env'))

from utils import sumit  # noqa: E402

# REAL paying managers' SUMIT objects (probe 2026-08-07) — never touchable
# from this script, whatever arguments are passed.
REAL_CUSTOMERS = {2126101197, 2227741440, 2137873115}   # גל / יובל / דניס
REAL_ITEMS = {2126101210, 2227741450, 2137873119}


def show(label, res):
    print(f'\n── {label}')
    print(json.dumps(res, ensure_ascii=False, default=str)[:600])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--customer', type=int, required=True)
    ap.add_argument('--item', type=int, required=True)
    ap.add_argument('--yes', action='store_true',
                    help='required: this script CANCELS the given item')
    args = ap.parse_args()

    if args.customer in REAL_CUSTOMERS or args.item in REAL_ITEMS:
        sys.exit('REFUSED: that customer/item belongs to a REAL paying manager.')
    if not args.yes:
        sys.exit('Add --yes to confirm cancelling this TEST subscription.')

    print(f'target: customer {args.customer}, item {args.item}')
    items = sumit.list_recurring_for_customer(args.customer, include_inactive=True)
    show('items BEFORE', items)
    live = [it for it in items if sumit.recurring_item_is_live(it)
            and it.get('ID') == args.item]
    if not live:
        sys.exit('target item is not live on that customer — wrong ids?')

    # (a) bogus item id first — must NOT cancel anything
    show('(a) cancel BOGUS item id 1',
         sumit.cancel_recurring(args.customer, 1))

    # (b) foreign pairing: OUR item id under a bogus customer — expect refusal.
    #     Uses an id that is certainly not a customer (1). If SUMIT were to
    #     accept this, the customer field is decorative and our server-side
    #     resolution is the ONLY isolation layer.
    show('(b) cancel with WRONG customer (1) + real test item',
         sumit.cancel_recurring(1, args.item))
    after_b = sumit.list_recurring_for_customer(args.customer, include_inactive=True)
    still_live = any(it.get('ID') == args.item and sumit.recurring_item_is_live(it)
                     for it in after_b)
    print(f'   item still live after (b): {still_live}  '
          f'(True = customer field IS enforced)')

    # (c) the real cancel
    show('(c) REAL cancel', sumit.cancel_recurring(args.customer, args.item))
    after_c = sumit.list_recurring_for_customer(args.customer, include_inactive=True)
    show('items AFTER cancel (IncludeInactive=true — note the Status value: '
         'Cancelled=1 vs CancelledByCustomer=13)', after_c)

    # (d) double-cancel — the idempotency answer
    show('(d) DOUBLE cancel (same ids again)',
         sumit.cancel_recurring(args.customer, args.item))

    print(f'\ntotal SUMIT calls: {sumit.call_count()}')


if __name__ == '__main__':
    sumit.reset_call_count()
    main()
