import unittest
from unittest.mock import patch, MagicMock
import os
import sys

# Mock alpaca_trade_api before it's imported by alpaca_executor
mock_alpaca = MagicMock()
sys.modules["alpaca_trade_api"] = mock_alpaca
sys.modules["alpaca_trade_api.rest"] = mock_alpaca.rest
# We want to be able to patch REST in execution.alpaca_executor
# So we need to make sure that when 'from alpaca_trade_api.rest import REST' is called,
# it gets something we can track or patch.

# Ensure the project root is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from execution.alpaca_executor import get_alpaca_api

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

if __name__ == '__main__':
    unittest.main()
