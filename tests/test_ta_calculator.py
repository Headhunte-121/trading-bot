import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
import numpy as np
import sys
import os

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from processor.ta_calculator import TACalculator

class TestTACalculator(unittest.TestCase):
    def setUp(self):
        self.calc = TACalculator()
        # Mock connection to avoid real DB calls
        self.calc.conn = MagicMock()

    @patch('processor.ta_calculator.pd.read_sql_query')
    def test_fetch_daily_sma_200_cache(self, mock_read_sql):
        # Setup mock return for first call
        dates = pd.date_range(end=pd.Timestamp.now(), periods=250)
        df = pd.DataFrame({'timestamp': dates, 'close': np.random.randn(250) + 100})
        # Reverse to mimic SQL DESC
        mock_return = df.iloc[::-1].reset_index(drop=True)
        mock_read_sql.return_value = mock_return

        # First call - should hit DB
        sma = self.calc.fetch_daily_sma_200('AAPL')

        self.assertIsNotNone(sma)
        self.assertTrue(mock_read_sql.called)

        # Reset mock
        mock_read_sql.reset_mock()

        # Second call - should hit cache
        sma2 = self.calc.fetch_daily_sma_200('AAPL')
        self.assertEqual(sma, sma2)
        self.assertFalse(mock_read_sql.called)

    @patch('processor.ta_calculator.pd.read_sql_query')
    def test_process_symbol_filters_date(self, mock_read_sql):
        """Test that process_symbol filters output to only the latest date"""
        self.calc.fetch_daily_sma_200 = MagicMock(return_value=150.0)

        # Mock intraday data spanning 2 days
        today = pd.Timestamp.now().normalize()
        yesterday = today - pd.Timedelta(days=1)

        # 60 points yesterday, 60 points today
        dates_yesterday = pd.date_range(start=yesterday + pd.Timedelta(hours=10), periods=60, freq='5min')
        dates_today = pd.date_range(start=today + pd.Timedelta(hours=10), periods=60, freq='5min')
        dates = dates_yesterday.union(dates_today) # 120 rows

        # Random price walk
        close_prices = np.cumsum(np.random.randn(120)) + 100

        df = pd.DataFrame({
            'timestamp': dates.strftime('%Y-%m-%d %H:%M:%S'),
            'open': close_prices,
            'high': close_prices + 1,
            'low': close_prices - 1,
            'close': close_prices,
            'volume': 1000
        })
        # SQL returns DESC
        mock_read_sql.return_value = df.iloc[::-1].reset_index(drop=True)

        # Run process
        result = self.calc.process_symbol('AAPL')

        self.assertIsNotNone(result)

        # Result should only contain rows from "today"
        # Today has 60 rows.
        # SMA 50 needs 50 rows history.
        # We have 120 rows history total.
        # So "today" rows (indices 60-119) should all have valid SMA 50 because they have 60 rows of history before them (yesterday).
        # So we expect 60 rows in output.
        self.assertEqual(len(result), 60)

        # Verify timestamp dates
        result_dates = pd.to_datetime(result['timestamp']).dt.date
        unique_dates = result_dates.unique()
        self.assertEqual(len(unique_dates), 1)
        # Should be the max date from input (today)
        self.assertEqual(unique_dates[0], dates.date.max())

if __name__ == '__main__':
    unittest.main()
