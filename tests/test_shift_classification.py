"""Overtime + Shabbat/chag shift classification (display only).

Covers the pure logic in agents/shift_classify.py and the Hebcal window parser
in agents/shabbat_times.py — no network.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from agents.shift_classify import classify_shifts, _shabbat_overlap_hours
from agents.shabbat_times import _pair_windows


def _shift(date, start, end, hours, is_open=False):
    return {'shift_date': date, 'start_ts': start, 'end_ts': end,
            'hours': hours, 'is_open': is_open}


class TestOvertimeDaily(unittest.TestCase):
    def test_single_long_shift_splits_at_8(self):
        s = _shift('2026-05-04', '2026-05-04 08:00:00', '2026-05-04 17:30:00', 9.0)
        classify_shifts([s], [])
        self.assertEqual(s['regular_hours'], 8.0)
        self.assertEqual(s['overtime_hours'], 1.0)

    def test_under_8_no_overtime(self):
        s = _shift('2026-05-04', '2026-05-04 08:00:00', '2026-05-04 14:00:00', 6.0)
        classify_shifts([s], [])
        self.assertEqual(s['regular_hours'], 6.0)
        self.assertEqual(s['overtime_hours'], 0.0)

    def test_two_shifts_same_day_cumulative(self):
        # 5h morning + 5h evening = 10h day → first 8 regular, last 2 overtime.
        a = _shift('2026-05-04', '2026-05-04 06:00:00', '2026-05-04 11:00:00', 5.0)
        b = _shift('2026-05-04', '2026-05-04 15:00:00', '2026-05-04 20:00:00', 5.0)
        classify_shifts([b, a], [])  # unsorted input — function sorts by start
        self.assertEqual((a['regular_hours'], a['overtime_hours']), (5.0, 0.0))
        self.assertEqual((b['regular_hours'], b['overtime_hours']), (3.0, 2.0))

    def test_regular_plus_overtime_equals_hours(self):
        s = _shift('2026-05-04', '2026-05-04 08:00:00', '2026-05-04 20:30:00', 12.5)
        classify_shifts([s], [])
        self.assertAlmostEqual(s['regular_hours'] + s['overtime_hours'], 12.5, places=4)

    def test_open_shift_all_zero(self):
        s = _shift('2026-05-04', '2026-05-04 08:00:00', None, 0.0, is_open=True)
        classify_shifts([s], [])
        self.assertEqual((s['regular_hours'], s['overtime_hours'], s['shabbat_hours']),
                         (0.0, 0.0, 0.0))


class TestShabbatOverlap(unittest.TestCase):
    def setUp(self):
        from datetime import datetime
        # Fri 2026-05-29 19:14 → Sat 2026-05-30 20:25 (Haifa-ish).
        self.win = [(datetime(2026, 5, 29, 19, 14), datetime(2026, 5, 30, 20, 25))]

    def test_partial_friday_evening(self):
        # Fri 16:00–21:00; candle-lighting 19:14 → 1h46m inside Shabbat.
        s = _shift('2026-05-29', '2026-05-29 16:00:00', '2026-05-29 21:00:00', 5.0)
        classify_shifts([s], self.win)
        self.assertAlmostEqual(s['shabbat_hours'], 1 + 46/60, places=2)

    def test_full_saturday_shift(self):
        s = _shift('2026-05-30', '2026-05-30 09:00:00', '2026-05-30 15:00:00', 6.0)
        classify_shifts([s], self.win)
        self.assertAlmostEqual(s['shabbat_hours'], 6.0, places=2)

    def test_outside_window_zero(self):
        s = _shift('2026-05-27', '2026-05-27 09:00:00', '2026-05-27 15:00:00', 6.0)
        classify_shifts([s], self.win)
        self.assertEqual(s['shabbat_hours'], 0.0)

    def test_shabbat_and_overtime_overlap(self):
        # 9h Saturday shift fully inside Shabbat: regular 8 + OT 1 (partition),
        # shabbat 9 (orthogonal overlay — can coincide with both).
        s = _shift('2026-05-30', '2026-05-30 09:00:00', '2026-05-30 18:00:00', 9.0)
        classify_shifts([s], self.win)
        self.assertEqual(s['regular_hours'], 8.0)
        self.assertEqual(s['overtime_hours'], 1.0)
        self.assertAlmostEqual(s['shabbat_hours'], 9.0, places=2)

    def test_shabbat_capped_at_hours(self):
        # Wall-clock 10h inside window but only 8 reported worked (breaks) →
        # shabbat capped at the reported 8.
        s = _shift('2026-05-30', '2026-05-30 09:00:00', '2026-05-30 19:00:00', 8.0)
        classify_shifts([s], self.win)
        self.assertEqual(s['shabbat_hours'], 8.0)


class TestGlobalEmployee(unittest.TestCase):
    def test_global_not_classified(self):
        s = _shift('2026-05-30', '2026-05-30 09:00:00', '2026-05-30 21:00:00', 12.0)
        classify_shifts([s], [], is_global=True)
        self.assertEqual(s['regular_hours'], 12.0)   # plain
        self.assertEqual(s['overtime_hours'], 0.0)
        self.assertEqual(s['shabbat_hours'], 0.0)


class TestHebcalWindowParsing(unittest.TestCase):
    def test_friday_shabbat_window(self):
        items = [
            {'category': 'candles', 'date': '2026-05-29T19:14:00+03:00'},
            {'category': 'havdalah', 'date': '2026-05-30T20:25:00+03:00'},
        ]
        wins = _pair_windows(items)
        self.assertEqual(len(wins), 1)
        self.assertEqual(wins[0]['date'], '2026-05-29')
        self.assertEqual(wins[0]['is_holiday'], 0)
        self.assertEqual(wins[0]['label'], 'שבת')

    def test_holiday_non_friday_flagged(self):
        # Shavuot eve on a Thursday → holiday window.
        items = [
            {'category': 'holiday', 'yomtov': True, 'title': 'Shavuot'},
            {'category': 'candles', 'date': '2026-05-21T19:08:00+03:00'},  # Thu
            {'category': 'havdalah', 'date': '2026-05-22T20:18:00+03:00'},
        ]
        wins = _pair_windows(items)
        self.assertEqual(len(wins), 1)
        self.assertEqual(wins[0]['is_holiday'], 1)

    def test_no_havdalah_no_window(self):
        items = [{'category': 'candles', 'date': '2026-05-29T19:14:00+03:00'}]
        self.assertEqual(_pair_windows(items), [])


if __name__ == '__main__':
    unittest.main()
