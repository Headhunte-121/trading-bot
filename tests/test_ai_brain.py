import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class TestAIBrain(unittest.TestCase):

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
        from processor import ai_brain
        self.ai_brain = ai_brain

    def tearDown(self):
        self.modules_patcher.stop()
        if 'processor.ai_brain' in sys.modules:
            del sys.modules['processor.ai_brain']

    @patch('processor.ai_brain.get_db_connection')
    @patch('processor.ai_brain.load_llm')
    @patch('processor.ai_brain.time.sleep')
    def test_news_loop_handles_failure(self, mock_sleep, mock_load_llm, mock_get_db):
        """Test that news processing updates DB even on LLM failure"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Mock rows: 2 items for news
        row1 = {'id': 1, 'symbol': 'TEST1', 'headline': 'Headline 1'}

        # Scenario:
        # 1. process_news_batch called -> returns True (work done)
        # 2. process_chart_batch called -> returns False (no work)
        # 3. loop continues
        # 4. process_news_batch called -> returns False (no work)
        # 5. process_chart_batch called -> returns False
        # 6. sleep 900 -> raise InterruptedError to stop test

        # Mock fetchall behavior:
        # Call 1 (News): [row1]
        # Call 2 (Charts): []
        # Call 3 (News): []
        # Call 4 (Charts): []
        mock_cursor.fetchall.side_effect = [[row1], [], [], []]

        # Mock LLM failure (returns empty list or raises)
        mock_llm = MagicMock()
        mock_llm.side_effect = Exception("Inference Failed")
        mock_load_llm.return_value = mock_llm

        mock_sleep.side_effect = InterruptedError("Stop Loop")

        try:
            self.ai_brain.main()
        except InterruptedError:
            pass

        # Verify DB updated for news item even though LLM failed
        # Logic: updates.append((row['id'], final_score=0, relevance=0, urgency=0))
        # query = "UPDATE raw_news SET sentiment_score = ?, relevance = ?, urgency = ? WHERE id = ?"
        # params = [(0.0, 0.0, 0, 1)]

        # Check executemany calls
        # We expect at least one call to update raw_news
        found_update = False
        for call in mock_conn.executemany.call_args_list:
            args, _ = call
            query = args[0]
            if "UPDATE raw_news" in query:
                params = args[1]
                self.assertEqual(len(params), 1)
                self.assertEqual(params[0], (0.0, 0.0, 0, 1))
                found_update = True
                break

        self.assertTrue(found_update, "Database was not updated for failed news item")

    @patch('processor.ai_brain.get_db_connection')
    @patch('processor.ai_brain.load_llm')
    @patch('processor.ai_brain.time.sleep')
    def test_sleep_duration(self, mock_sleep, mock_load_llm, mock_get_db):
        """Test that sleep is called with 900s when idle"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # No work at all
        mock_cursor.fetchall.return_value = []

        mock_llm = MagicMock()
        mock_load_llm.return_value = mock_llm

        # First sleep call should be the long sleep
        mock_sleep.side_effect = InterruptedError("Stop Loop")

        try:
            self.ai_brain.main()
        except InterruptedError:
            pass

        # Check that 900 was called at some point
        mock_sleep.assert_any_call(900)

if __name__ == '__main__':
    unittest.main()
