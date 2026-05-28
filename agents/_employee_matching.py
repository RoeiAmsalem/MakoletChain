"""Shared employee name-matching logic.

Extracted from gmail_agent.py so both gmail_agent and aviv_employees_report
can reuse the same matching without duplication.
"""

import logging
import os
import sqlite3

log = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')


def _clean_name(name: str, branch_name: str = '') -> str:
    """Strip branch/store name suffixes from employee name."""
    store_words = ['איינשטיין', 'אינשטיין', 'einstein']
    if branch_name:
        store_words.append(branch_name.strip())
        store_words.extend(branch_name.strip().split())

    words = name.strip().split()
    while words and any(w.lower() == words[-1].lower() for w in store_words):
        words.pop()
    return ' '.join(words).strip()


def strip_store_suffix(name: str, branch_name: str = '') -> str:
    """Public wrapper for the suffix-strip step used during matching.

    Used by the unmatched-path of aviv_employees_report so that pending rows
    are stored with the same cleaned name the matcher used internally —
    otherwise a name like 'זכאי זיני תיכון' gets stored verbatim into
    employee_match_pending while 'זכאי זיני' is what the manager sees in
    every other UI.
    """
    return _clean_name(name, branch_name)


def _check_alias(csv_name: str, branch_id: int, db_employees: list):
    """Check employee_aliases table for a match. Returns (emp_id, confidence, name, rate) or None."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.row_factory = sqlite3.Row
        alias = conn.execute(
            '''SELECT ea.employee_id FROM employee_aliases ea
               JOIN employees e ON e.id = ea.employee_id
               WHERE ea.branch_id=? AND ea.alias_name=? AND e.active=1''',
            (branch_id, csv_name.strip())
        ).fetchone()
        conn.close()
        if alias:
            emp_id = alias['employee_id']
            for emp in db_employees:
                if emp['id'] == emp_id:
                    return (emp_id, 'exact', emp['name'], emp['hourly_rate'])
    except Exception:
        pass
    return None


def match_employee_name(csv_name: str, db_employees: list, branch_name: str = '', branch_id: int = 0) -> tuple:
    """Match CSV/Aviv employee name to DB employee.

    Returns (employee_id, confidence, matched_db_name, hourly_rate)
    confidence: 'exact', 'high', 'low', 'none'
    """
    # Check aliases first
    if branch_id:
        alias_match = _check_alias(csv_name, branch_id, db_employees)
        if alias_match:
            return alias_match

    # Clean the CSV name: strip branch suffixes
    cleaned = _clean_name(csv_name, branch_name)

    best_match = None
    best_score = 0.0

    for emp in db_employees:
        db_name = emp['name'].strip()
        db_clean = _clean_name(db_name, branch_name)

        # Exact match after cleaning
        if cleaned == db_clean:
            return (emp['id'], 'exact', db_name, emp['hourly_rate'])

        # One contains the other (handles "עידן" matching "עידן בקון")
        # But require the shorter name to be at least 2 words OR an exact first-name match
        csv_words_check = cleaned.split()
        db_words_check = db_clean.split()
        if cleaned.startswith(db_clean) or db_clean.startswith(cleaned):
            shorter_len = min(len(csv_words_check), len(db_words_check))
            # Only accept if first names match exactly
            if csv_words_check and db_words_check and csv_words_check[0] == db_words_check[0]:
                return (emp['id'], 'exact', db_name, emp['hourly_rate'])

        csv_words = cleaned.split()
        db_words = db_clean.split()
        if not csv_words or not db_words:
            continue

        # First name matches
        if csv_words[0] == db_words[0]:
            overlap = len(set(csv_words) & set(db_words))
            score = overlap / max(len(csv_words), len(db_words))
            if score > best_score:
                best_score = score
                best_match = emp

        # First + last name match (ignore middle names)
        if len(db_words) >= 2:
            first, last = db_words[0], db_words[-1]
            if first in csv_words and last in csv_words:
                score = 0.8
                if score > best_score:
                    best_score = score
                    best_match = emp

        if len(csv_words) >= 2:
            first, last = csv_words[0], csv_words[-1]
            if first in db_words and last in db_words:
                score = 0.8
                if score > best_score:
                    best_score = score
                    best_match = emp

    if best_match:
        if best_score >= 0.5:
            return (best_match['id'], 'high', best_match['name'], best_match['hourly_rate'])
        else:
            return (best_match['id'], 'low', best_match['name'], best_match['hourly_rate'])

    return (None, 'none', None, 0)
