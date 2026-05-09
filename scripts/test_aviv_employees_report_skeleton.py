"""Smoke tests for the Aviv employer-report agent skeleton.

Verifies:
1. Imports work
2. fetch_report_list handles 404 correctly
3. Unimplemented functions raise NotImplementedError
4. Extracted matching function produces same results as before
"""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


class TestSkeletonImports(unittest.TestCase):
    def test_import_agent_module(self):
        from agents import aviv_employees_report
        self.assertTrue(hasattr(aviv_employees_report, 'fetch_report_list'))
        self.assertTrue(hasattr(aviv_employees_report, 'find_employer_report_id'))
        self.assertTrue(hasattr(aviv_employees_report, 'run_for_branch'))

    def test_import_matching_module(self):
        from agents._employee_matching import match_employee_name, _clean_name
        self.assertTrue(callable(match_employee_name))
        self.assertTrue(callable(_clean_name))


class TestFetchReportList(unittest.TestCase):
    @patch('agents.aviv_employees_report.requests.get')
    def test_404_returns_empty_list(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_get.return_value = mock_resp

        from agents.aviv_employees_report import fetch_report_list
        result = fetch_report_list(3, 'fake-token')
        self.assertEqual(result, [])

    @patch('agents.aviv_employees_report.requests.get')
    def test_200_returns_json(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [{'id': 1, 'name': 'test'}]
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        from agents.aviv_employees_report import fetch_report_list
        result = fetch_report_list(3, 'fake-token')
        self.assertEqual(result, [{'id': 1, 'name': 'test'}])


class TestNotImplemented(unittest.TestCase):
    def test_find_employer_report_id_raises(self):
        from agents.aviv_employees_report import find_employer_report_id
        with self.assertRaises(NotImplementedError):
            find_employer_report_id([])

    def test_fetch_employer_report_raises(self):
        from agents.aviv_employees_report import fetch_employer_report
        with self.assertRaises(NotImplementedError):
            fetch_employer_report(3, 1, 'token')

    def test_parse_employer_report_raises(self):
        from agents.aviv_employees_report import parse_employer_report
        with self.assertRaises(NotImplementedError):
            parse_employer_report(b'')

    def test_update_employee_hours_raises(self):
        from agents.aviv_employees_report import update_employee_hours
        with self.assertRaises(NotImplementedError):
            update_employee_hours(126, [], None)

    def test_run_for_branch_raises(self):
        from agents.aviv_employees_report import run_for_branch
        with self.assertRaises(NotImplementedError):
            run_for_branch(126)


class TestMatchingFunction(unittest.TestCase):
    """Verify the extracted matching function produces correct results."""

    def setUp(self):
        self.db_employees = [
            {'id': 1, 'name': 'עידן בקון', 'hourly_rate': 35.0},
            {'id': 2, 'name': 'יוסי כהן', 'hourly_rate': 40.0},
            {'id': 3, 'name': 'דניאל לוי', 'hourly_rate': 38.0},
        ]

    def test_exact_match(self):
        from agents._employee_matching import match_employee_name
        emp_id, conf, name, rate = match_employee_name('עידן בקון', self.db_employees)
        self.assertEqual(emp_id, 1)
        self.assertEqual(conf, 'exact')
        self.assertEqual(rate, 35.0)

    def test_branch_suffix_stripped(self):
        from agents._employee_matching import match_employee_name
        emp_id, conf, name, rate = match_employee_name(
            'עידן בקון איינשטיין', self.db_employees, branch_name='איינשטיין')
        self.assertEqual(emp_id, 1)
        self.assertEqual(conf, 'exact')

    def test_first_name_prefix_match(self):
        from agents._employee_matching import match_employee_name
        emp_id, conf, name, rate = match_employee_name('עידן', self.db_employees)
        self.assertEqual(emp_id, 1)
        self.assertEqual(conf, 'exact')

    def test_no_match(self):
        from agents._employee_matching import match_employee_name
        emp_id, conf, name, rate = match_employee_name('שלום עולם', self.db_employees)
        self.assertIsNone(emp_id)
        self.assertEqual(conf, 'none')

    def test_clean_name(self):
        from agents._employee_matching import _clean_name
        self.assertEqual(_clean_name('עידן בקון איינשטיין'), 'עידן בקון')
        self.assertEqual(_clean_name('עידן בקון'), 'עידן בקון')
        self.assertEqual(_clean_name('דניאל לוי einstein'), 'דניאל לוי')


if __name__ == '__main__':
    unittest.main()
