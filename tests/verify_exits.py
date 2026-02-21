
import unittest
from unittest.mock import MagicMock, patch
import sqlite3
import os
import sys
import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from strategy.trend_following import evaluate_exits
from execution.risk_manager import RiskManager
from execution.alpaca_executor import AlpacaExecutor
# from shared.db_utils import get_db_connection # Not needed here directly except for patch target verification

# Mock Database Path
DB_PATH = 'tests/test_exits.db'

def setup_db():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Create tables
    cursor.execute("""
        CREATE TABLE market_data (
            symbol TEXT, timestamp TEXT, timeframe TEXT, close REAL,
            volume REAL,
            PRIMARY KEY (symbol, timestamp, timeframe)
        )
    """)
    cursor.execute("""
        CREATE TABLE technical_indicators (
            symbol TEXT, timestamp TEXT, timeframe TEXT,
            sma_50 REAL, sma_200 REAL, rsi_14 REAL,
            PRIMARY KEY (symbol, timestamp, timeframe)
        )
    """)
    cursor.execute("""
        CREATE TABLE ai_predictions (
            symbol TEXT, timestamp TEXT, ensemble_pct_change REAL,
            UNIQUE(symbol, timestamp)
        )
    """)
    cursor.execute("""
        CREATE TABLE trade_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, timestamp TEXT, signal_type TEXT,
            status TEXT, size REAL, stop_loss REAL, atr REAL, order_id TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE executed_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, timestamp TEXT, price REAL, qty REAL, side TEXT, signal_type TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE system_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, service_name TEXT, log_level TEXT, message TEXT
        )
    """)

    conn.commit()
    conn.close()

class TestAIExits(unittest.TestCase):
    def setUp(self):
        setup_db()

        # Helper to create connection
        def create_conn():
            c = sqlite3.connect(DB_PATH)
            c.row_factory = sqlite3.Row
            return c

        # Patch get_db_connection in all utilized modules
        self.patchers = [
            patch('shared.db_utils.get_db_connection', side_effect=create_conn),
            patch('execution.risk_manager.get_db_connection', side_effect=create_conn),
            patch('execution.alpaca_executor.get_db_connection', side_effect=create_conn),
            patch('strategy.trend_following.get_db_connection', side_effect=create_conn)
        ]

        for p in self.patchers:
            p.start()

        self.conn = create_conn()
        self.timestamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        self.conn.execute("INSERT INTO market_data (symbol, timestamp, timeframe, close) VALUES ('AAPL', ?, '5m', 150.0)", (self.timestamp,))
        self.conn.execute("INSERT INTO technical_indicators (symbol, timestamp, timeframe, sma_50, sma_200, rsi_14) VALUES ('AAPL', ?, '5m', 155.0, 140.0, 30.0)", (self.timestamp,))
        self.conn.execute("INSERT INTO ai_predictions (symbol, timestamp, ensemble_pct_change) VALUES ('AAPL', ?, -0.6)", (self.timestamp,))
        self.conn.commit()

    def tearDown(self):
        for p in self.patchers:
            p.stop()
        self.conn.close()
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)

    @patch('strategy.trend_following.get_alpaca_api')
    def test_evaluate_exits_take_profit(self, mock_get_api):
        """Test generating TAKE_PROFIT_EXIT signal"""
        print("\nTesting TAKE_PROFIT_EXIT generation...")

        mock_api = MagicMock()
        mock_get_api.return_value = mock_api

        # Mock Position: Profit > 1%
        mock_pos = MagicMock()
        mock_pos.symbol = 'AAPL'
        mock_pos.unrealized_plpc = '0.02' # 2% profit
        mock_api.list_positions.return_value = [mock_pos]

        # Run Strategy
        evaluate_exits(self.conn.cursor())
        # evaluate_exits now commits internally via cursor.connection.commit()

        # Verify Signal
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM trade_signals WHERE symbol='AAPL'")
        signal = cursor.fetchone()

        self.assertIsNotNone(signal, "Signal was not generated/inserted")
        self.assertEqual(signal['signal_type'], 'TAKE_PROFIT_EXIT')
        self.assertEqual(signal['status'], 'PENDING')
        print("✅ TAKE_PROFIT_EXIT signal generated successfully.")

    @patch('strategy.trend_following.get_alpaca_api')
    def test_evaluate_exits_panic_exit(self, mock_get_api):
        """Test generating PANIC_EXIT signal"""
        print("\nTesting PANIC_EXIT generation...")

        mock_api = MagicMock()
        mock_get_api.return_value = mock_api

        # Mock Position: Loss < 0%
        mock_pos = MagicMock()
        mock_pos.symbol = 'AAPL'
        mock_pos.unrealized_plpc = '-0.02' # 2% loss
        mock_api.list_positions.return_value = [mock_pos]

        # Run Strategy
        evaluate_exits(self.conn.cursor())

        # Verify Signal
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM trade_signals WHERE symbol='AAPL'")
        signal = cursor.fetchone()

        self.assertIsNotNone(signal, "Signal was not generated/inserted")
        self.assertEqual(signal['signal_type'], 'PANIC_EXIT')
        print("✅ PANIC_EXIT signal generated successfully.")

    def test_risk_manager_sizing(self):
        """Test Risk Manager handling of Exit signals"""
        print("\nTesting Risk Manager sizing...")

        # Insert a Pending Exit Signal
        self.conn.execute("""
            INSERT INTO trade_signals (symbol, timestamp, signal_type, status, size)
            VALUES ('AAPL', ?, 'PANIC_EXIT', 'PENDING', NULL)
        """, (self.timestamp,))
        self.conn.commit()

        # Run Risk Manager
        rm = RiskManager()
        rm.process_pending_signals()

        # Verify
        cursor = self.conn.cursor()
        cursor.execute("SELECT status, size FROM trade_signals WHERE symbol='AAPL'")
        row = cursor.fetchone()

        self.assertIsNotNone(row)
        self.assertEqual(row['status'], 'SIZED')
        self.assertEqual(row['size'], 0)
        print("✅ Risk Manager correctly sized Exit signal (Size 0, Status SIZED).")

    @patch('execution.alpaca_executor.REST')
    def test_alpaca_executor_execution(self, mock_rest):
        """Test Alpaca Executor executing Exit signals"""
        print("\nTesting Alpaca Executor execution...")

        # Insert a SIZED Exit Signal
        self.conn.execute("""
            INSERT INTO trade_signals (symbol, timestamp, signal_type, status, size)
            VALUES ('AAPL', ?, 'PANIC_EXIT', 'SIZED', 0)
        """, (self.timestamp,))
        self.conn.commit()

        # Setup Mock API
        mock_api = mock_rest.return_value

        mock_order = MagicMock()
        mock_order.id = 'order_123'
        mock_order.symbol = 'AAPL'
        mock_api.list_orders.return_value = [mock_order]

        mock_pos = MagicMock()
        mock_pos.qty = '10'
        mock_api.get_position.return_value = mock_pos

        mock_sell_order = MagicMock()
        mock_sell_order.id = 'sell_order_999'
        mock_api.submit_order.return_value = mock_sell_order

        # Mock environment variables for Executor init
        with patch.dict(os.environ, {
            'APCA_API_KEY_ID': 'test',
            'APCA_API_SECRET_KEY': 'test',
            'APCA_API_BASE_URL': 'http://test'
        }):
            executor = AlpacaExecutor()
            # executor.process_sized_signals takes a connection
            executor.process_sized_signals(self.conn)

        # Verify Actions
        mock_api.list_orders.assert_called()
        mock_api.cancel_order.assert_called_with('order_123')
        mock_api.get_position.assert_called_with('AAPL')
        mock_api.submit_order.assert_called_with(
            symbol='AAPL', qty=10.0, side='sell', type='market', time_in_force='gtc'
        )

        # Verify DB Update
        cursor = self.conn.cursor()
        cursor.execute("SELECT status FROM trade_signals WHERE symbol='AAPL'")
        status = cursor.fetchone()[0]
        self.assertEqual(status, 'EXECUTED')
        print("✅ Alpaca Executor correctly executed Exit signal (Cancel -> Sell -> Update DB).")

if __name__ == '__main__':
    unittest.main()
