"""Shared text helpers."""
import re


def clean_supplier_name(name) -> str:
    """Normalize a supplier name to ONE canonical form.

    BilBoy returns some supplier names with a trailing newline (also \\r / \\t /
    stray spaces), so the raw value 'מרינה ...\\n' and the trimmed 'מרינה ...'
    would otherwise be treated as two different suppliers. This strips
    leading/trailing whitespace and collapses any internal whitespace run
    (incl. newlines/tabs) to a single space. None/empty-safe → returns ''.

    This is the single source of truth for supplier-name normalization — used at
    write time (agents/bilboy.py), in the roster build (agents/supplier_roster.py),
    and as the grouping key in _goal_data so two raw variants merge into one row.
    """
    if name is None:
        return ''
    return re.sub(r'\s+', ' ', str(name)).strip()
