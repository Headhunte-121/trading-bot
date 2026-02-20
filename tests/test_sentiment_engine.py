import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class TestSentimentEngine(unittest.TestCase):

    def setUp(self):
        # Create a clean environment for each test
        self.mock_transformers = MagicMock()
        self.mock_torch = MagicMock()
        self.mock_bnb = MagicMock()

        self.modules_patcher = patch.dict(sys.modules, {
            'transformers': self.mock_transformers,
            'torch': self.mock_torch,
            'bitsandbytes': self.mock_bnb
        })
        self.modules_patcher.start()

        # Now we can import the module under test safely
        from processor import sentiment_engine
        self.sentiment_engine = sentiment_engine

    def tearDown(self):
        self.modules_patcher.stop()
        # Remove the module from sys.modules so it's re-imported next time if needed
        if 'processor.sentiment_engine' in sys.modules:
            del sys.modules['processor.sentiment_engine']

    @patch('processor.sentiment_engine.get_db_connection')
    @patch('processor.sentiment_engine.load_llm')
    @patch('processor.sentiment_engine.time.sleep')
    @patch('processor.sentiment_engine.update_db_batch')
    def test_loop_handles_empty_llm_results(self, mock_update, mock_sleep, mock_load_llm, mock_get_db):
        # Setup mocks
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Mock rows: 2 items
        row1 = {'id': 1, 'symbol': 'TEST1', 'headline': 'Headline 1'}
        row2 = {'id': 2, 'symbol': 'TEST2', 'headline': 'Headline 2'}

        # mock fetchall returns rows once, then empty list
        mock_cursor.fetchall.side_effect = [[row1, row2], []]

        # Mock LLM to return empty list (failure)
        mock_llm = MagicMock()
        mock_llm.return_value = []
        mock_load_llm.return_value = mock_llm

        # Mock sleep to raise exception to break infinite loop
        mock_sleep.side_effect = InterruptedError("Stop Loop")

        try:
            self.sentiment_engine.main()
        except InterruptedError:
            pass

        # Assertions
        # Verify update_db_batch called with 0s for both rows
        self.assertTrue(mock_update.called)
        args, _ = mock_update.call_args
        updates = args[1]
        self.assertEqual(len(updates), 2)
        self.assertEqual(updates[0], (1, 0.0, 0.0, 0))
        self.assertEqual(updates[1], (2, 0.0, 0.0, 0))

if __name__ == '__main__':
    unittest.main()
