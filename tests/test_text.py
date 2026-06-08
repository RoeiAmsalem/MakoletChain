"""Tests for utils.text.clean_supplier_name — the single supplier-name normalizer."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from utils.text import clean_supplier_name


def test_strips_trailing_newline():
    # the actual BilBoy bug: a trailing \n made 'name' and 'name\n' two suppliers
    assert clean_supplier_name('מרינה פטריות הגליל בע"מ\n') == 'מרינה פטריות הגליל בע"מ'


def test_strips_cr_lf_tab_and_trims():
    assert clean_supplier_name('\r\n  תנובה בע"מ \t') == 'תנובה בע"מ'


def test_collapses_internal_whitespace():
    assert clean_supplier_name('a  b\tc\nd') == 'a b c d'


def test_none_and_empty_safe():
    assert clean_supplier_name(None) == ''
    assert clean_supplier_name('') == ''
    assert clean_supplier_name('   \n\t ') == ''


def test_idempotent():
    once = clean_supplier_name('שטראוס   מצונן\n')
    assert clean_supplier_name(once) == once == 'שטראוס מצונן'
