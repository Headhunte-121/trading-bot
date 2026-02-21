import sqlite3
import os
import math
import sys
import time

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection
from shared.smart_sleep import get_sleep_seconds

# Configuration
ACCOUNT_SIZE = 100000
POSITION_SIZE_PCT = 0.01 # 1% of Account Equity per trade

def calculate_position_size(close_price):
    """
    Calculates the position size (number of shares).
    Logic: Position Value = Account Size * 1%
    """
    if close_price <= 0:
        return 0

    target_position_value = ACCOUNT_SIZE * POSITION_SIZE_PCT

    # Calculate shares
    shares = math.floor(target_position_value / close_price)

    return shares

def run_risk_manager():
    print("Starting Risk Manager...")

    conn = get_db_connection()
    conn.row_factory = sqlite3.Row # Consistent row access
    cursor = conn.cursor()

    try:
        # Find PENDING BUY signals and their corresponding close price
        # We need the close price from market_data (most recent 5m candle)
        # JOIN with market_data based on symbol and timestamp
        query = """
            SELECT ts.id, ts.symbol, ts.timestamp, md.close
            FROM trade_signals ts
            JOIN market_data md ON ts.symbol = md.symbol AND ts.timestamp = md.timestamp
            WHERE ts.status = 'PENDING' AND ts.signal_type = 'BUY'
            AND md.timeframe = '5m'
        """

        cursor.execute(query)
        pending_signals = cursor.fetchall()

        print(f"Found {len(pending_signals)} pending BUY signals.")

        for signal in pending_signals:
            signal_id = signal['id']
            symbol = signal['symbol']
            timestamp = signal['timestamp']
            close_price = signal['close']

            if close_price is None:
                print(f"Skipping signal {signal_id} for {symbol}: No market data found.")
                continue

            size = calculate_position_size(close_price)

            if size > 0:
                # Update signal with size
                # We don't set stop_loss price here because Alpaca handles Trailing Stop %
                # But we can store it for reference if we wanted, but the prompt says just SIZED.
                update_query = """
                    UPDATE trade_signals
                    SET size = ?, status = 'SIZED'
                    WHERE id = ?
                """
                cursor.execute(update_query, (size, signal_id))
                conn.commit() # Commit immediately
                print(f"✅ Sized signal {signal_id}: Symbol={symbol}, Price={close_price:.2f}, Size={size} shares.")
            else:
                 print(f"Skipping signal {signal_id}: Calculated size is 0 (Price: {close_price}).")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    while True:
        run_risk_manager()

        sleep_sec = get_sleep_seconds()
        print(f"💤 Risk Manager Sleeping for {sleep_sec} seconds...")
        time.sleep(sleep_sec)
