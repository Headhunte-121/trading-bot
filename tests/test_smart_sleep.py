import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
import sys
import os

# Ensure shared is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from shared.smart_sleep import get_market_status, SLEEP_ACTIVE, SLEEP_PASSIVE

class TestSmartSleep(unittest.TestCase):
    @patch('shared.smart_sleep.datetime')
    def test_market_open(self, mock_datetime):
        # Mock time: Monday 10:00 AM NY time (2023-10-23 is Monday)
        from zoneinfo import ZoneInfo
        ny_tz = ZoneInfo("America/New_York")

        # We need a class that behaves like datetime but has now() mocked
        # But since we patch 'shared.smart_sleep.datetime', we can just set return_value of now

        # Create a real datetime object for the return value
        mock_now = datetime(2023, 10, 23, 10, 0, 0, tzinfo=ny_tz)

        # Configure the mock to return this object when now(tz) is called
        mock_datetime.now.return_value = mock_now

        # We also need side_effect to handle replace() calls on the returned object?
        # No, the returned object is a real datetime (or close to it).
        # Wait, if mock_datetime.now returns a real datetime object, then .replace() works on it.
        # But 'is_weekday = ny_time.weekday()' works.

        status = get_market_status()
        self.assertTrue(status['is_open'], "Market should be open on Monday 10 AM")
        self.assertEqual(status['sleep_seconds'], SLEEP_ACTIVE)

    @patch('shared.smart_sleep.datetime')
    def test_market_closed_weekend(self, mock_datetime):
        from zoneinfo import ZoneInfo
        ny_tz = ZoneInfo("America/New_York")
        # 2023-10-21 is Saturday
        mock_now = datetime(2023, 10, 21, 10, 0, 0, tzinfo=ny_tz)

        mock_datetime.now.return_value = mock_now

        status = get_market_status()
        self.assertFalse(status['is_open'], "Market should be closed on Saturday")
        self.assertEqual(status['sleep_seconds'], SLEEP_PASSIVE)

    @patch('shared.smart_sleep.datetime')
    def test_market_closed_evening(self, mock_datetime):
        from zoneinfo import ZoneInfo
        ny_tz = ZoneInfo("America/New_York")
        # Monday 18:00
        mock_now = datetime(2023, 10, 23, 18, 0, 0, tzinfo=ny_tz)

        mock_datetime.now.return_value = mock_now

        status = get_market_status()
        self.assertFalse(status['is_open'], "Market should be closed at 6 PM")
        self.assertEqual(status['sleep_seconds'], SLEEP_PASSIVE)

    @patch('shared.smart_sleep.datetime')
    def test_market_edge_case_open(self, mock_datetime):
        from zoneinfo import ZoneInfo
        ny_tz = ZoneInfo("America/New_York")
        # Monday 09:30:00
        mock_now = datetime(2023, 10, 23, 9, 30, 0, tzinfo=ny_tz)
        mock_datetime.now.return_value = mock_now

        status = get_market_status()
        self.assertTrue(status['is_open'], "Market should be open at 9:30 AM")

    @patch('shared.smart_sleep.datetime')
    def test_market_edge_case_close(self, mock_datetime):
        from zoneinfo import ZoneInfo
        ny_tz = ZoneInfo("America/New_York")
        # Monday 15:59:59
        mock_now = datetime(2023, 10, 23, 15, 59, 59, tzinfo=ny_tz)
        mock_datetime.now.return_value = mock_now

        status = get_market_status()
        self.assertTrue(status['is_open'], "Market should be open at 15:59:59")

    @patch('shared.smart_sleep.datetime')
    def test_market_edge_case_closed_exact(self, mock_datetime):
        from zoneinfo import ZoneInfo
        ny_tz = ZoneInfo("America/New_York")
        # Monday 16:00:00
        mock_now = datetime(2023, 10, 23, 16, 0, 0, tzinfo=ny_tz)
        mock_datetime.now.return_value = mock_now

        status = get_market_status()
        self.assertFalse(status['is_open'], "Market should be closed at 16:00:00")

if __name__ == '__main__':
    unittest.main()
