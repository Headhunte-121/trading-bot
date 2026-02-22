import unittest
from unittest.mock import patch, MagicMock
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.db_utils import get_db_connection

class TestDBUtils(unittest.TestCase):
    @patch('shared.db_utils.psycopg2.connect')
    def test_get_db_connection(self, mock_connect):
        # Setup mock
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # Call function
        conn = get_db_connection()

        # Verify
        self.assertIsNotNone(conn)
        self.assertEqual(conn, mock_conn)
        mock_connect.assert_called_once()

        # Check env vars usage (defaults)
        kwargs = mock_connect.call_args[1]
        self.assertEqual(kwargs['host'], 'postgres_db')
        self.assertEqual(kwargs['user'], 'quant_user')

    @patch('shared.db_utils.psycopg2.connect')
    def test_connection_failure(self, mock_connect):
        # Setup mock to raise exception
        import psycopg2
        mock_connect.side_effect = psycopg2.OperationalError("Connection failed")

        # Call function
        conn = get_db_connection(log_error=False)

        # Verify
        self.assertIsNone(conn)
        # Should retry 3 times
        self.assertEqual(mock_connect.call_count, 3)

if __name__ == '__main__':
    unittest.main()
