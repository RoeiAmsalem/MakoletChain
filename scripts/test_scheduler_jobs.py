"""Regression test for scheduler.py job registration.

Guards against re-introduction of the legacy aviv_employees agent (removed
2026-05-28). Its 23:45 run used to clobber aviv_employees_report rows
inserted at 23:30. The new aviv_employees_report jobs must remain registered.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


class TestSchedulerJobs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import scheduler  # noqa: F401  — registers jobs as a side effect of import
        cls.job_ids = {j.id for j in scheduler.scheduler.get_jobs()}

    def test_old_aviv_employees_jobs_not_registered(self):
        self.assertNotIn('aviv_employees', self.job_ids,
                         'Old 23:45 aviv_employees job must be unscheduled '
                         '(it clobbered aviv_employees_report rows on 2026-05-10).')
        self.assertNotIn('aviv_employees_midday', self.job_ids,
                         'Old 15:00 aviv_employees_midday job must be unscheduled.')

    def test_new_aviv_report_jobs_registered(self):
        for job_id in ('aviv_report_weekday_afternoon',
                       'aviv_report_weekday_night',
                       'aviv_report_friday',
                       'aviv_report_saturday'):
            self.assertIn(job_id, self.job_ids,
                          f'aviv_employees_report job {job_id} missing.')


if __name__ == '__main__':
    unittest.main()
