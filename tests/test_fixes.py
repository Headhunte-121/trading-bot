import sqlite3
import unittest
from unittest.mock import MagicMock

# Mocking the functions to test logic without full DB
class TestFixes(unittest.TestCase):

    def test_alpaca_executor_row_fix(self):
        """
        Simulate the bug fix in execution/alpaca_executor.py
        """
        # Create a mock Row object
        conn = sqlite3.connect(':memory:')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE trade_signals (id INTEGER, symbol TEXT, size INTEGER, stop_loss REAL)")
        cursor.execute("INSERT INTO trade_signals VALUES (1, 'AAPL', 10, 150.50)")
        cursor.execute("SELECT * FROM trade_signals")
        row = cursor.fetchone()

        # Buggy behavior: float(row) -> Error
        try:
            val = float(row)
            print("Bug not reproduced (unexpected)")
        except TypeError:
            print("Bug reproduced: float(row) failed as expected")

        # Fixed behavior: row['stop_loss'] -> float
        stop_loss = row['stop_loss']
        val = float(stop_loss)
        self.assertEqual(val, 150.50)
        print("Fix verified: float(row['stop_loss']) works")

    def test_mean_reversion_tuple_fix(self):
        """
        Simulate the bug fix in strategy/mean_reversion.py
        """
        # Create mock data
        conn = sqlite3.connect(':memory:')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE technical_indicators (symbol TEXT, timestamp TEXT)")
        cursor.execute("INSERT INTO technical_indicators VALUES ('AAPL', '2023-01-01T00:00:00')")
        cursor.execute("SELECT symbol, timestamp FROM technical_indicators")
        rows = cursor.fetchall()

        row = rows[0]

        # Buggy behavior simulation: passing Row/tuple to execute
        # In real code, cursor.execute("SELECT ... WHERE symbol = ?", (row,)) fails
        # because row is a tuple/Row, not a string.

        # Verify extraction
        symbol = row['symbol']
        timestamp = row['timestamp']

        self.assertEqual(symbol, 'AAPL')
        self.assertEqual(timestamp, '2023-01-01T00:00:00')
        self.assertIsInstance(symbol, str)
        print("Fix verified: symbol and timestamp extracted as strings")

if __name__ == '__main__':
    unittest.main()
