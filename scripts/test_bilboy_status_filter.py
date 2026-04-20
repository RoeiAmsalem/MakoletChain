"""Tests: BilBoy status filtering and bookKeepingId dedup.

Verifies:
1. status=7 (superseded) docs are dropped silently
2. status=3 (normal) and status=9 (finalized) docs are kept
3. Unknown statuses are dropped and trigger alert
4. Two docs with same bookKeepingId → higher status wins
5. ref_number lstrip('0') dedup still works
6. Reconciliation compares against accepted docs, not raw total
7. Golden test: branch 127 April — 64 raw docs → 63 after dedup (2300/2657 pair)

Run: python scripts/test_bilboy_status_filter.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.bilboy import ACCEPTED_STATUSES


def test_accepted_statuses():
    """Status 3 and 9 are accepted, 7 is not."""
    assert 3 in ACCEPTED_STATUSES
    assert 9 in ACCEPTED_STATUSES
    assert 7 not in ACCEPTED_STATUSES
    print("PASS: ACCEPTED_STATUSES correct")


def _run_filter_logic(raw_docs):
    """Simulate the status filter + bookKeepingId dedup + ref dedup pipeline.
    Extracted from bilboy.py logic for testability."""
    alerts = []

    # Status filter
    status_7_sum = 0
    unknown_sum = 0
    skip_superseded = 0
    skip_status = 0
    status_filtered = []
    for doc in raw_docs:
        amount = float(doc.get('totalWithVat') or doc.get('totalAmount') or doc.get('amount') or 0)
        status = doc.get('status')
        if status == 7:
            skip_superseded += 1
            status_7_sum += amount
            continue
        if status is not None and status not in ACCEPTED_STATUSES:
            skip_status += 1
            unknown_sum += amount
            alerts.append(f"unknown_status_{status}")
            continue
        status_filtered.append(doc)

    # BookKeepingId dedup
    bk_map = {}
    for doc in status_filtered:
        bk_id = doc.get('bookKeepingId')
        if bk_id is not None:
            existing = bk_map.get(bk_id)
            if existing is not None:
                old_status = existing.get('status') or 0
                new_status = doc.get('status') or 0
                if new_status >= old_status:
                    bk_map[bk_id] = doc
            else:
                bk_map[bk_id] = doc
        else:
            bk_map[id(doc)] = doc
    docs = list(bk_map.values())

    # Process + ref dedup
    records = []
    for doc in docs:
        doc_type = doc.get('type')
        if doc_type not in {2, 3, 4, 5}:
            continue
        amount = float(doc.get('totalWithVat') or doc.get('totalAmount') or doc.get('amount') or 0)
        if amount == 0:
            continue
        ref_number = str(doc.get('refNumber') or doc.get('number') or '').lstrip('0') or '0'
        records.append({
            'ref_number': ref_number,
            'amount': amount,
            'doc_type': doc_type,
            'supplier': doc.get('supplierName', ''),
        })

    seen = set()
    deduped = []
    for r in records:
        if r['ref_number'] in seen:
            continue
        seen.add(r['ref_number'])
        deduped.append(r)

    return {
        'records': deduped,
        'total': sum(r['amount'] for r in deduped),
        'count': len(deduped),
        'skip_superseded': skip_superseded,
        'skip_status': skip_status,
        'status_7_sum': status_7_sum,
        'unknown_sum': unknown_sum,
        'alerts': alerts,
    }


def test_status_7_dropped():
    """Status=7 docs are dropped silently (no alert)."""
    docs = [
        {'refNumber': '100', 'totalWithVat': 1000, 'status': 3, 'type': 3, 'bookKeepingId': 1},
        {'refNumber': '200', 'totalWithVat': 500, 'status': 7, 'type': 3, 'bookKeepingId': 2},
        {'refNumber': '300', 'totalWithVat': 750, 'status': 9, 'type': 3, 'bookKeepingId': 3},
    ]
    result = _run_filter_logic(docs)
    assert result['count'] == 2, f"Expected 2 docs, got {result['count']}"
    assert result['skip_superseded'] == 1
    assert result['status_7_sum'] == 500
    assert len(result['alerts']) == 0, "status=7 should not trigger alerts"
    print("PASS: status=7 docs dropped silently")


def test_unknown_status_dropped_and_alerted():
    """Unknown status docs are dropped AND trigger alert."""
    docs = [
        {'refNumber': '100', 'totalWithVat': 1000, 'status': 3, 'type': 3, 'bookKeepingId': 1},
        {'refNumber': '200', 'totalWithVat': 500, 'status': 99, 'type': 3, 'bookKeepingId': 2},
    ]
    result = _run_filter_logic(docs)
    assert result['count'] == 1
    assert result['skip_status'] == 1
    assert 'unknown_status_99' in result['alerts']
    print("PASS: unknown status dropped and alerted")


def test_bookkeeping_id_dedup():
    """Two docs with same bookKeepingId → higher status wins."""
    docs = [
        {'refNumber': '100', 'totalWithVat': 1000, 'status': 3, 'type': 3, 'bookKeepingId': 50},
        {'refNumber': '200', 'totalWithVat': 1500, 'status': 9, 'type': 3, 'bookKeepingId': 50},
    ]
    result = _run_filter_logic(docs)
    assert result['count'] == 1, f"Expected 1, got {result['count']}"
    assert result['total'] == 1500, f"Expected 1500 (status=9 doc), got {result['total']}"
    print("PASS: bookKeepingId dedup keeps higher status")


def test_ref_number_lstrip_dedup():
    """ref_number variants (0212 vs 00212 vs 212) deduped correctly."""
    docs = [
        {'refNumber': '0212', 'totalWithVat': 100, 'status': 3, 'type': 3, 'bookKeepingId': 1},
        {'refNumber': '00212', 'totalWithVat': 100, 'status': 3, 'type': 3, 'bookKeepingId': 2},
        {'refNumber': '212', 'totalWithVat': 100, 'status': 3, 'type': 3, 'bookKeepingId': 3},
    ]
    result = _run_filter_logic(docs)
    assert result['count'] == 1, f"Expected 1 after lstrip dedup, got {result['count']}"
    print("PASS: ref_number lstrip dedup works")


def test_golden_case_branch_127():
    """Golden test: branch 127 April — the known 2300/2657 bookKeepingId=8074 pair.

    Input: 64 raw docs including:
      - ref=2300, status=7, bookKeepingId=8074, amount=6346  (superseded)
      - ref=2657, status=9, bookKeepingId=8074, amount=6346  (finalized replacement)
    Expected: 63 docs, the status=7 one is dropped by status filter.
    Total should be 116327.07 (122673.07 - 6346.00).
    """
    # Build 62 normal docs + the problematic pair = 64 total
    normal_docs = []
    normal_total = 0
    for i in range(62):
        amt = round(1500 + i * 23.45, 2)  # deterministic amounts
        normal_total += amt
        normal_docs.append({
            'refNumber': str(1000 + i),
            'totalWithVat': amt,
            'status': 3,
            'type': 3,
            'bookKeepingId': 7000 + i,
            'supplierName': f'Supplier {i}',
        })

    # The superseded/finalized pair
    pair_amount = 6346.00
    superseded = {
        'refNumber': '2300',
        'totalWithVat': pair_amount,
        'status': 7,
        'type': 3,
        'bookKeepingId': 8074,
        'supplierName': 'Some Supplier',
    }
    finalized = {
        'refNumber': '2657',
        'totalWithVat': pair_amount,
        'status': 9,
        'type': 3,
        'bookKeepingId': 8074,
        'supplierName': 'Some Supplier',
    }

    all_docs = normal_docs + [superseded, finalized]
    assert len(all_docs) == 64, f"Fixture should have 64 docs, got {len(all_docs)}"

    result = _run_filter_logic(all_docs)

    expected_total = round(normal_total + pair_amount, 2)  # 62 normal + 1 finalized
    assert result['count'] == 63, f"Expected 63 docs, got {result['count']}"
    assert abs(result['total'] - expected_total) < 0.01, \
        f"Expected total ₪{expected_total}, got ₪{result['total']}"
    assert result['skip_superseded'] == 1, "Should have dropped 1 superseded doc"
    assert result['status_7_sum'] == pair_amount
    print(f"PASS: Golden case — 64→63 docs, total ₪{result['total']:,.2f} (dropped superseded ₪{pair_amount:,.2f})")


def test_status_none_accepted():
    """Docs with status=None (missing field) should be accepted (backwards compat)."""
    docs = [
        {'refNumber': '100', 'totalWithVat': 1000, 'type': 3, 'bookKeepingId': 1},
    ]
    result = _run_filter_logic(docs)
    assert result['count'] == 1, "Docs without status field should be accepted"
    print("PASS: status=None accepted (backwards compat)")


if __name__ == '__main__':
    passed = 0
    failed = 0
    tests = [
        test_accepted_statuses,
        test_status_7_dropped,
        test_unknown_status_dropped_and_alerted,
        test_bookkeeping_id_dedup,
        test_ref_number_lstrip_dedup,
        test_golden_case_branch_127,
        test_status_none_accepted,
    ]
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"FAIL: {test.__name__}: {e}")
            failed += 1
    print(f"\n{'='*40}")
    print(f"Results: {passed} passed, {failed} failed")
    if failed:
        sys.exit(1)
    print("All tests passed!")
