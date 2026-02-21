import unittest
from unittest.mock import patch, MagicMock
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Mock alpaca_trade_api BEFORE importing alpaca_executor
mock_alpaca_module = MagicMock()
sys.modules["alpaca_trade_api"] = mock_alpaca_module
sys.modules["alpaca_trade_api.rest"] = mock_alpaca_module.rest

from execution.alpaca_executor import process_signals

class TestAlpacaCryptoLogic(unittest.TestCase):

    @patch('execution.alpaca_executor.get_db_connection')
    def test_process_signals_crypto_rounding(self, mock_get_db):
        # Mock DB
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Mock Signals
        # 1. BTC/USD (Standard Crypto, Price > $1) -> 4 decimals
        # Size = 0.54321 -> Expect 0.5432

        # 2. PEPE/USD (Micro Cap, Price < $1) -> 8 decimals
        # Size = 10000.123456789 -> Expect 10000.12345679

        # 3. AAPL (Equity) -> Int
        # Size = 10.9 -> Expect 10

        mock_cursor.fetchall.return_value = [
            {'id': 1, 'symbol': 'BTC/USD', 'size': 0.54321, 'signal_type': 'TREND_BUY', 'atr': 100.0},
            {'id': 2, 'symbol': 'PEPE/USD', 'size': 50000.123456789, 'signal_type': 'VWAP_SCALP', 'atr': 0.0001},
            {'id': 3, 'symbol': 'AAPL', 'size': 10.9, 'signal_type': 'DEEP_VALUE_BUY', 'atr': 5.0}
        ]

        # Mock API
        mock_api = MagicMock()
        mock_order = MagicMock()
        mock_order.id = 'order_123'
        mock_api.submit_order.return_value = mock_order

        # Mock Order Status (Filled immediately)
        mock_filled_order = MagicMock()
        mock_filled_order.status = 'filled'
        mock_filled_order.filled_qty = '0' # Placeholder, logic uses this for logging but submit_order check is key
        mock_api.get_order.return_value = mock_filled_order

        # Run
        process_signals(mock_api)

        # Assertions
        # Check calls to submit_order
        calls = mock_api.submit_order.call_args_list

        # We expect 6 calls (3 Buys + 3 Trailing Stops)
        # We only care about the Buy calls first to check Qty logic

        buy_calls = [c for c in calls if c.kwargs.get('side') == 'buy']

        self.assertEqual(len(buy_calls), 3)

        # Check BTC/USD
        btc_call = next(c for c in buy_calls if c.kwargs['symbol'] == 'BTC/USD')
        self.assertEqual(btc_call.kwargs['qty'], 0.5432)
        self.assertIsInstance(btc_call.kwargs['qty'], float)

        # Check PEPE/USD
        # Logic: If estimated_price < 1.0 -> 8 decimals.
        # Estimated Price = 1000 / Size.
        # 1000 / 50000 = 0.02 (< 1.0) -> Expect 8 decimals
        pepe_call = next(c for c in buy_calls if c.kwargs['symbol'] == 'PEPE/USD')
        self.assertEqual(pepe_call.kwargs['qty'], 50000.12345679)

        # Check AAPL
        aapl_call = next(c for c in buy_calls if c.kwargs['symbol'] == 'AAPL')
        self.assertEqual(aapl_call.kwargs['qty'], 10)
        self.assertIsInstance(aapl_call.kwargs['qty'], int)

if __name__ == '__main__':
    unittest.main()
