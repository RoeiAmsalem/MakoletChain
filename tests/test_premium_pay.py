"""Payroll-grade premium pay: overtime + Shabbat/chag brackets (cumulative method).

Hand-calculated cases asserted EXACTLY. Rates:
  weekday  regular 100% | OT first-2 125% | OT rest 150%
  Shabbat  regular 150% | OT first-2 175% | OT rest 200%
"""
import os
import sys
import unittest
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from agents.shift_classify import premium_pay_for_month  # noqa: E402
from scripts.seed_demo_shifts import DEMO_SHIFTS, SHABBAT_WINDOWS_MAY, DEMO_EMPLOYEES  # noqa: E402

# May 2026 Haifa windows as datetimes (from the demo seeder's verified data).
MAY_WINDOWS = [(datetime.strptime(c, '%Y-%m-%d %H:%M:%S'),
                datetime.strptime(h, '%Y-%m-%d %H:%M:%S'))
               for _d, c, h, _ih, _l in SHABBAT_WINDOWS_MAY]


def _shift(date, s, e):
    return {'shift_date': date, 'start_ts': f'{date} {s}:00',
            'end_ts': f'{date} {e}:00', 'hours': 0, 'is_open': False}


def _cost(shifts, rate=100.0, windows=None):
    return premium_pay_for_month(shifts, rate, windows or [])


class TestCanonicalCases(unittest.TestCase):
    def test_8h_weekday(self):
        r = _cost([_shift('2026-05-04', '09:00', '17:00')])
        self.assertEqual(r['cost'], 800.0)               # 8×100%
        self.assertEqual(r['buckets'], {100: 8.0})

    def test_10h_weekday(self):
        r = _cost([_shift('2026-05-04', '08:00', '18:00')])
        self.assertEqual(r['cost'], 1050.0)              # 8×100 + 2×125
        self.assertEqual(r['buckets'], {100: 8.0, 125: 2.0})

    def test_12h_weekday(self):
        r = _cost([_shift('2026-05-04', '08:00', '20:00')])
        self.assertEqual(r['cost'], 1350.0)              # 8×100 + 2×125 + 2×150
        self.assertEqual(r['buckets'], {100: 8.0, 125: 2.0, 150: 2.0})

    def test_6h_saturday(self):
        # 05-16 inside window 05-15 19:01 → 05-16 20:13.
        r = _cost([_shift('2026-05-16', '10:00', '16:00')], windows=MAY_WINDOWS)
        self.assertEqual(r['cost'], 900.0)               # 6×150%
        self.assertEqual(r['buckets'], {150: 6.0})

    def test_11h_saturday_cumulative(self):
        # 05-30 inside window 05-29 19:11 → 05-30 20:24: 8×150 + 2×175 + 1×200.
        r = _cost([_shift('2026-05-30', '09:00', '20:00')], windows=MAY_WINDOWS)
        self.assertEqual(r['buckets'], {150: 8.0, 175: 2.0, 200: 1.0})
        self.assertEqual(r['cost'], 8*150 + 2*175 + 1*200)   # 1750

    def test_mixed_day_friday_into_shabbat(self):
        # Fri 14:00–24:00, candle 19:11. 14–22 regular (5h11m @100 + 2h49m @150),
        # 22–24 OT first-2 tier, all Shabbat → 175%.
        r = premium_pay_for_month(
            [{'shift_date': '2026-05-29', 'start_ts': '2026-05-29 14:00:00',
              'end_ts': '2026-05-30 00:00:00', 'hours': 0, 'is_open': False}],
            100.0, MAY_WINDOWS)
        self.assertAlmostEqual(r['buckets'][100], 5 + 11/60, places=3)   # 14:00–19:11
        self.assertAlmostEqual(r['buckets'][150], 2 + 49/60, places=3)   # 19:11–22:00
        self.assertAlmostEqual(r['buckets'][175], 2.0, places=3)         # 22:00–24:00 OT+Shabbat
        self.assertAlmostEqual(r['cost'], (5+11/60)*100 + (2+49/60)*150 + 2*175, places=1)

    def test_multi_shift_day_overtime_is_daily(self):
        # Two 5h shifts same day = 10h day → first 8 regular, last 2 OT (not per-shift).
        r = _cost([_shift('2026-05-04', '06:00', '11:00'),
                   _shift('2026-05-04', '15:00', '20:00')])
        self.assertEqual(r['buckets'], {100: 8.0, 125: 2.0})
        self.assertEqual(r['cost'], 1050.0)

    def test_global_n_a(self):
        # Sanity: a Saturday 8h shift priced as Shabbat regular when windows apply.
        r = _cost([_shift('2026-05-16', '10:00', '18:00')], windows=MAY_WINDOWS)
        self.assertEqual(r['buckets'], {150: 8.0})


class TestDemoEmployeeMonthlyTotals(unittest.TestCase):
    """Exact monthly premium per demo hourly employee (the live staging data)."""

    def _emp_cost(self, name, rate):
        shifts = [_shift(d, s, e) for d, s, e, _dow in DEMO_SHIFTS[name]]
        return premium_pay_for_month(shifts, rate, MAY_WINDOWS)['cost']

    def test_yoav(self):
        # 8h(8.0) + 6h(6.0) + 10h(10.5) = 24.5 units × ₪38
        self.assertEqual(self._emp_cost('יואב לוי', 38.0), round(24.5 * 38, 2))

    def test_maya(self):
        # 12h(13.5) + 5h(5.0) + Sat6h(9.0) + chag6h(9.0) = 36.5 units × ₪42
        self.assertEqual(self._emp_cost('מאיה כהן', 42.0), round(36.5 * 42, 2))

    def test_daniel(self):
        # 6h(6.0) + Fri6h(7.40833) + Sat11h(17.5) = 30.90833 units × ₪40
        cost = self._emp_cost('דניאל פרץ', 40.0)
        self.assertAlmostEqual(cost, round((6 + (3+11/60) + (2+49/60)*1.5 + 17.5) * 40, 2), places=1)
        self.assertEqual(cost, 1236.33)

    def test_paid_hours_reconcile(self):
        # Premium paid_hours equals the sum of worked shift hours (no double-count).
        for name, salary_type, rate, _g, _role in DEMO_EMPLOYEES:
            if salary_type != 'hourly':
                continue
            shifts = [_shift(d, s, e) for d, s, e, _dow in DEMO_SHIFTS[name]]
            r = premium_pay_for_month(shifts, rate, MAY_WINDOWS)
            span = sum((datetime.strptime(x['end_ts'], '%Y-%m-%d %H:%M:%S')
                        - datetime.strptime(x['start_ts'], '%Y-%m-%d %H:%M:%S')).total_seconds()/3600
                       for x in shifts)
            self.assertAlmostEqual(r['paid_hours'], span, places=4)
            self.assertAlmostEqual(sum(r['buckets'].values()), span, places=4)


if __name__ == '__main__':
    unittest.main()
