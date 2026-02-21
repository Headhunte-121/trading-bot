import unittest
from unittest.mock import patch, MagicMock, call
import os
import sys

# Ensure the project root is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Mock alpaca_trade_api before it's imported by alpaca_executor
mock_alpaca = MagicMock()
sys.modules["alpaca_trade_api"] = mock_alpaca
sys.modules["alpaca_trade_api.rest"] = mock_alpaca.rest

# Mock shared modules to avoid side effects
sys.modules["shared.db_utils"] = MagicMock()
sys.modules["shared.smart_sleep"] = MagicMock()

from execution.alpaca_executor import AlpacaExecutor

class TestAlpacaExecutor(unittest.TestCase):

    @patch('execution.alpaca_executor.REST')
    @patch('execution.alpaca_executor.os.getenv')
    def setUp(self, mock_getenv, mock_rest):
        # Setup env vars for init
        mock_getenv.side_effect = lambda k: "test_val" if "APCA" in k else None

        # Mock API instance
        self.mock_api_instance = MagicMock()
        mock_rest.return_value = self.mock_api_instance

        self.executor = AlpacaExecutor()
        self.mock_api = self.executor.api

        # Reset mocks
        self.mock_api.reset_mock()

    def test_process_sized_signals_success(self):
        # Mock DB
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # Mock Data
        mock_cursor.fetchall.return_value = [
            {'id': 1, 'symbol': 'AAPL', 'size': 10, 'signal_type': 'TREND_BUY'}
        ]

        # Mock API Response
        mock_order = MagicMock()
        mock_order.id = "order_123"
        self.mock_api.submit_order.return_value = mock_order

        # Call
        self.executor.process_sized_signals(mock_conn)

        # Assert
        self.mock_api.submit_order.assert_called_with(
            symbol='AAPL', qty=10.0, side='buy', type='market', time_in_force='gtc'
        )
        mock_cursor.execute.assert_any_call(
            "UPDATE trade_signals SET status = 'SUBMITTED', order_id = ? WHERE id = ?",
            ("order_123", 1)
        )

    def test_process_submitted_signals_filled_success(self):
        # Mock DB
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # Mock Data
        mock_cursor.fetchall.return_value = [
            {'id': 1, 'symbol': 'AAPL', 'order_id': 'order_123', 'signal_type': 'TREND_BUY', 'atr': 2.0}
        ]

        # Mock API Response: Get Order
        mock_order_status = MagicMock()
        mock_order_status.status = 'filled'
        mock_order_status.filled_qty = '10'
        mock_order_status.filled_avg_price = '150.0'
        self.mock_api.get_order.return_value = mock_order_status

        # Mock API Response: Submit Stop
        mock_stop_order = MagicMock()
        mock_stop_order.id = "stop_123"
        self.mock_api.submit_order.return_value = mock_stop_order

        # Call
        self.executor.process_submitted_signals(mock_conn)

        # Assert
        # 1. Get Order
        self.mock_api.get_order.assert_called_with("order_123")
        # 2. Log Trade (Insert) - implied by log_trade logic, tricky to assert exact insert without mocking log_trade
        # 3. Submit Stop
        # Multiplier for TREND_BUY is 3.0. ATR is 2.0. Trail price = 6.0
        self.mock_api.submit_order.assert_called_with(
            symbol='AAPL', qty=10.0, side='sell', type='trailing_stop', time_in_force='gtc', trail_price=6.0
        )
        # 4. Update status
        mock_cursor.execute.assert_any_call("UPDATE trade_signals SET status = 'EXECUTED' WHERE id = ?", (1,))

    def test_circuit_breaker_activates(self):
        # Force failures
        self.executor.failure_count = 0
        self.executor.circuit_breaker_tripped = False

        # Simulate 3 failures
        error_msg = "500 Server Error"
        for _ in range(3):
            self.executor._check_circuit_breaker(Exception(error_msg))

        self.assertTrue(self.executor.circuit_breaker_tripped)
        self.assertEqual(self.executor.failure_count, 3)

    def test_retry_logic_failure(self):
        # Test that _submit_trailing_stop retries 3 times then fails
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # Force submit_order to fail always
        self.mock_api.submit_order.side_effect = Exception("API Error")

        # Patch time.sleep to speed up test
        with patch('execution.alpaca_executor.time.sleep') as mock_sleep:
            self.executor._submit_trailing_stop(mock_conn, 1, 'AAPL', 10, 2.0, 'TREND_BUY')

        # Assert called 3 times
        self.assertEqual(self.mock_api.submit_order.call_count, 3)
        # Assert status updated to EXECUTED_NO_STOP
        mock_cursor.execute.assert_called_with(
            "UPDATE trade_signals SET status = 'EXECUTED_NO_STOP' WHERE id = ?", (1,)
        )

if __name__ == '__main__':
    unittest.main()
