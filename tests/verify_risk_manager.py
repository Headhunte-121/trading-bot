import sqlite3
import os
import sys
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta, timezone
import math

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the module under test
import execution.risk_manager

DB_PATH = "tests/test_risk.db"

class TestRiskManager(unittest.TestCase):

    def setUp(self):
        # Setup temporary database
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)

        self.conn = sqlite3.connect(DB_PATH)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()

        # Create minimal tables
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS trade_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                timestamp TEXT,
                signal_type TEXT,
                status TEXT,
                size REAL
            )
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS market_data (
                symbol TEXT,
                timestamp TEXT,
                timeframe TEXT,
                close REAL
            )
        """)
        self.conn.commit()

    def tearDown(self):
        self.conn.close()
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)

    def test_calculate_position_size(self):
        # Access the function directly
        calc_func = execution.risk_manager.calculate_position_size

        # Test 1% of 100k account / price
        price = 100.0
        # Expected: floor(100000 * 0.01 / 100) = 10
        self.assertEqual(calc_func(price), 10)

        price = 50.0
        # Expected: floor(100000 * 0.01 / 50) = 20
        self.assertEqual(calc_func(price), 20)

        # Edge case: price 0 or negative
        self.assertEqual(calc_func(0), 0)
        self.assertEqual(calc_func(-10), 0)

    @patch('execution.risk_manager.get_db_connection')
    def test_end_to_end_sizing(self, mock_get_db):
        mock_get_db.side_effect = lambda: sqlite3.connect(DB_PATH)

        # Insert a pending signal (FRESH)
        timestamp = datetime.now(timezone.utc).isoformat()
        self.cursor.execute("INSERT INTO trade_signals (symbol, timestamp, signal_type, status) VALUES (?, ?, ?, ?)",
                            ('AAPL', timestamp, 'BUY', 'PENDING'))
        signal_id = self.cursor.lastrowid

        # Insert corresponding market data
        self.cursor.execute("INSERT INTO market_data (symbol, timestamp, timeframe, close) VALUES (?, ?, ?, ?)",
                            ('AAPL', timestamp, '5m', 150.0))
        self.conn.commit()

        # Run risk manager
        from io import StringIO
        captured_output = StringIO()
        original_stdout = sys.stdout
        sys.stdout = captured_output

        try:
            execution.risk_manager.run_risk_manager()
        except Exception as e:
            sys.stdout = original_stdout
            self.fail(f"run_risk_manager raised exception: {e}")

        sys.stdout = original_stdout

        # Verify the signal was updated
        check_conn = sqlite3.connect(DB_PATH)
        check_cursor = check_conn.cursor()
        check_cursor.execute("SELECT size, status FROM trade_signals WHERE id = ?", (signal_id,))
        row = check_cursor.fetchone()
        check_conn.close()

        self.assertIsNotNone(row)
        size, status = row
        self.assertEqual(status, 'SIZED')

        expected_size = 6
        self.assertEqual(size, expected_size)

    @patch('execution.risk_manager.get_db_connection')
    def test_stale_signal_expiration(self, mock_get_db):
        mock_get_db.side_effect = lambda: sqlite3.connect(DB_PATH)

        # Insert a STALE signal (2 hours old)
        stale_time = (datetime.now(timezone.utc) - timedelta(minutes=120)).isoformat()

        self.cursor.execute("INSERT INTO trade_signals (symbol, timestamp, signal_type, status) VALUES (?, ?, ?, ?)",
                            ('MSFT', stale_time, 'BUY', 'PENDING'))
        signal_id = self.cursor.lastrowid

        # Insert corresponding market data
        self.cursor.execute("INSERT INTO market_data (symbol, timestamp, timeframe, close) VALUES (?, ?, ?, ?)",
                            ('MSFT', stale_time, '5m', 300.0))
        self.conn.commit()

        # Run risk manager
        from io import StringIO
        captured_output = StringIO()
        original_stdout = sys.stdout
        sys.stdout = captured_output

        try:
            execution.risk_manager.run_risk_manager()
        except Exception as e:
            sys.stdout = original_stdout
            self.fail(f"run_risk_manager raised exception: {e}")

        sys.stdout = original_stdout

        # Verify the signal was EXPIRED
        check_conn = sqlite3.connect(DB_PATH)
        check_cursor = check_conn.cursor()
        check_cursor.execute("SELECT size, status FROM trade_signals WHERE id = ?", (signal_id,))
        row = check_cursor.fetchone()
        check_conn.close()

        self.assertIsNotNone(row)
        size, status = row
        self.assertEqual(status, 'EXPIRED')
        self.assertIsNone(size) # Size should not be updated (null or whatever default)

    @patch('execution.risk_manager.get_db_connection')
    def test_diverse_signal_types(self, mock_get_db):
        """Tests that RiskManager processes signals other than 'BUY' (e.g. VWAP_SCALP, TREND_BUY)."""
        mock_get_db.side_effect = lambda: sqlite3.connect(DB_PATH)

        timestamp = datetime.now(timezone.utc).isoformat()

        # Insert signals with different types
        self.cursor.execute("INSERT INTO trade_signals (symbol, timestamp, signal_type, status) VALUES (?, ?, ?, ?)",
                            ('NVDA', timestamp, 'VWAP_SCALP', 'PENDING'))
        id1 = self.cursor.lastrowid

        self.cursor.execute("INSERT INTO trade_signals (symbol, timestamp, signal_type, status) VALUES (?, ?, ?, ?)",
                            ('AMD', timestamp, 'TREND_BUY', 'PENDING'))
        id2 = self.cursor.lastrowid

        # Insert corresponding market data
        self.cursor.execute("INSERT INTO market_data (symbol, timestamp, timeframe, close) VALUES (?, ?, ?, ?)",
                            ('NVDA', timestamp, '5m', 200.0))
        self.cursor.execute("INSERT INTO market_data (symbol, timestamp, timeframe, close) VALUES (?, ?, ?, ?)",
                            ('AMD', timestamp, '5m', 100.0))
        self.conn.commit()

        # Run risk manager
        from io import StringIO
        captured_output = StringIO()
        original_stdout = sys.stdout
        sys.stdout = captured_output

        try:
            execution.risk_manager.run_risk_manager()
        except Exception as e:
            sys.stdout = original_stdout
            self.fail(f"run_risk_manager raised exception: {e}")

        sys.stdout = original_stdout

        # Verify signals were sized
        check_conn = sqlite3.connect(DB_PATH)
        check_cursor = check_conn.cursor()

        check_cursor.execute("SELECT status FROM trade_signals WHERE id = ?", (id1,))
        self.assertEqual(check_cursor.fetchone()[0], 'SIZED')

        check_cursor.execute("SELECT status FROM trade_signals WHERE id = ?", (id2,))
        self.assertEqual(check_cursor.fetchone()[0], 'SIZED')

        check_conn.close()

if __name__ == '__main__':
    unittest.main()
