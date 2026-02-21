import unittest
from unittest.mock import patch, MagicMock, call
import os
import sys

# Mock alpaca_trade_api before it's imported by alpaca_executor
mock_alpaca = MagicMock()
sys.modules["alpaca_trade_api"] = mock_alpaca
sys.modules["alpaca_trade_api.rest"] = mock_alpaca.rest

# Ensure the project root is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from execution.alpaca_executor import get_alpaca_api, process_sized_signals, process_submitted_signals

class TestAlpacaExecutor(unittest.TestCase):

    @patch('execution.alpaca_executor.os.getenv')
    @patch('execution.alpaca_executor.REST')
    def test_get_alpaca_api_success(self, mock_rest, mock_getenv):
        # Configure mock_getenv to return values for all required env vars
        env_vars = {
            "APCA_API_KEY_ID": "test_key",
            "APCA_API_SECRET_KEY": "test_secret",
            "APCA_API_BASE_URL": "https://paper-api.alpaca.markets"
        }
        mock_getenv.side_effect = lambda k: env_vars.get(k)

        # Call the function
        api = get_alpaca_api()

        # Assertions
        mock_rest.assert_called_once_with("test_key", "test_secret", "https://paper-api.alpaca.markets")
        self.assertEqual(api, mock_rest.return_value)

    @patch('execution.alpaca_executor.os.getenv')
    @patch('execution.alpaca_executor.REST')
    def test_get_alpaca_api_missing_env_vars(self, mock_rest, mock_getenv):
        # Scenario 1: All missing
        mock_getenv.side_effect = None
        mock_getenv.return_value = None
        api = get_alpaca_api()
        self.assertIsNone(api)
        mock_rest.assert_not_called()

        # Scenario 2: One missing
        mock_getenv.reset_mock()
        env_vars = {
            "APCA_API_KEY_ID": "test_key",
            "APCA_API_SECRET_KEY": "test_secret",
            "APCA_API_BASE_URL": None
        }
        mock_getenv.side_effect = lambda k: env_vars.get(k)
        api = get_alpaca_api()
        self.assertIsNone(api)
        mock_rest.assert_not_called()

    @patch('execution.alpaca_executor.os.getenv')
    @patch('execution.alpaca_executor.REST')
    def test_get_alpaca_api_exception(self, mock_rest, mock_getenv):
        # Configure mock_getenv to return values
        env_vars = {
            "APCA_API_KEY_ID": "test_key",
            "APCA_API_SECRET_KEY": "test_secret",
            "APCA_API_BASE_URL": "https://paper-api.alpaca.markets"
        }
        mock_getenv.side_effect = lambda k: env_vars.get(k)

        # Configure mock_rest to raise an exception
        mock_rest.side_effect = Exception("Initialization failed")

        # Call the function
        api = get_alpaca_api()

        # Assertions
        self.assertIsNone(api)

    def test_process_sized_signals_success(self):
        # Mock API
        mock_api = MagicMock()
        mock_order = MagicMock()
        mock_order.id = "order_123"
        mock_api.submit_order.return_value = mock_order

        # Mock DB
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # Setup mock data
        mock_cursor.fetchall.return_value = [
            {'id': 1, 'symbol': 'AAPL', 'size': 10, 'signal_type': 'TREND_BUY'}
        ]

        # Call function
        process_sized_signals(mock_api, mock_conn)

        # Assertions
        mock_cursor.execute.assert_any_call("UPDATE trade_signals SET status = 'SUBMITTED', order_id = ? WHERE id = ?", ("order_123", 1))
        mock_api.submit_order.assert_called_with(
            symbol='AAPL', qty=10, side='buy', type='market', time_in_force='gtc'
        )

    def test_process_sized_signals_failure(self):
        # Mock API
        mock_api = MagicMock()
        mock_api.submit_order.side_effect = Exception("API Error")

        # Mock DB
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            {'id': 1, 'symbol': 'AAPL', 'size': 10, 'signal_type': 'TREND_BUY'}
        ]

        # Call function
        process_sized_signals(mock_api, mock_conn)

        # Assertions
        mock_cursor.execute.assert_any_call("UPDATE trade_signals SET status = 'FAILED' WHERE id = ?", (1,))

    def test_process_submitted_signals_filled(self):
        # Mock API
        mock_api = MagicMock()
        mock_order = MagicMock()
        mock_order.status = 'filled'
        mock_order.filled_qty = '10'
        mock_order.filled_avg_price = '150.0'
        mock_api.get_order.return_value = mock_order

        mock_stop_order = MagicMock()
        mock_stop_order.id = "stop_123"
        mock_api.submit_order.return_value = mock_stop_order

        # Mock DB
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            {'id': 1, 'symbol': 'AAPL', 'order_id': 'order_123', 'signal_type': 'TREND_BUY', 'atr': 2.0}
        ]

        # Call function
        process_submitted_signals(mock_api, mock_conn)

        # Assertions
        # Check if trade logged (this logic is inside log_trade, verify execute call)
        # Check if trailing stop submitted
        mock_api.submit_order.assert_called()
        # Check status update
        mock_cursor.execute.assert_any_call("UPDATE trade_signals SET status = 'EXECUTED' WHERE id = ?", (1,))

    def test_process_submitted_signals_canceled(self):
        # Mock API
        mock_api = MagicMock()
        mock_order = MagicMock()
        mock_order.status = 'canceled'
        mock_api.get_order.return_value = mock_order

        # Mock DB
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            {'id': 1, 'symbol': 'AAPL', 'order_id': 'order_123', 'signal_type': 'TREND_BUY', 'atr': 2.0}
        ]

        # Call function
        process_submitted_signals(mock_api, mock_conn)

        # Assertions
        mock_cursor.execute.assert_any_call("UPDATE trade_signals SET status = 'FAILED' WHERE id = ?", (1,))

    def test_process_submitted_signals_partial(self):
        # Mock API
        mock_api = MagicMock()
        mock_order = MagicMock()
        mock_order.status = 'partially_filled'
        mock_api.get_order.return_value = mock_order

        # Mock DB
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            {'id': 1, 'symbol': 'AAPL', 'order_id': 'order_123', 'signal_type': 'TREND_BUY', 'atr': 2.0}
        ]

        # Call function
        process_submitted_signals(mock_api, mock_conn)

        # Assertions
        calls = mock_cursor.execute.call_args_list
        update_calls = [c for c in calls if "UPDATE trade_signals" in c[0][0]]
        self.assertEqual(len(update_calls), 0)

if __name__ == '__main__':
    unittest.main()
