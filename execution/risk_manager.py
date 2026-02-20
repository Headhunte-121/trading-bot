import sqlite3
import os
import math
import sys

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection

# Configuration
ACCOUNT_SIZE = 100000
RISK_PCT = 0.01          # Risk 1% of account per trade ($1,000)
STOP_LOSS_PCT = 0.02     # Stop Loss 2% below entry
MAX_POSITION_SIZE_PCT = 0.20 # Cap max position size to 20% of account ($20,000) to prevent over-leveraging on tight stops

def calculate_position_size(close_price):
    """
    Calculates the position size (number of shares) and stop loss price.

    Logic:
    - Risk Amount = Account Size * Risk % ($1,000)
    - Stop Loss Distance = Close Price * Stop Loss %
    - Shares = floor(Risk Amount / Stop Loss Distance) -> Integer shares
    - Stop Loss Price = Close Price - Stop Loss Distance
    - Applies MAX_POSITION_SIZE_PCT cap.

    Returns:
        (size, stop_loss_price)
    """
    if close_price <= 0:
        return 0, 0.0

    risk_amount = ACCOUNT_SIZE * RISK_PCT
    stop_loss_distance = close_price * STOP_LOSS_PCT

    # Avoid division by zero
    if stop_loss_distance == 0:
        return 0, 0.0

    # limit risk-based sizing
    shares = math.floor(risk_amount / stop_loss_distance)

    # Cap position size based on max allocation
    max_shares_allocation = math.floor((ACCOUNT_SIZE * MAX_POSITION_SIZE_PCT) / close_price)

    if shares > max_shares_allocation:
        print(f"⚠️ Capping position size from {shares} to {max_shares_allocation} (Max Allocation Rule)")
        shares = max_shares_allocation

    stop_loss_price = close_price - stop_loss_distance

    return shares, stop_loss_price

def run_risk_manager():
    print("Starting Risk Manager...")

    conn = get_db_connection()
    conn.row_factory = sqlite3.Row # Consistent row access
    cursor = conn.cursor()

    try:
        # Find PENDING BUY signals and their corresponding close price
        # Using an INNER JOIN to only process signals with valid market data
        query = """
            SELECT ts.id, ts.symbol, ts.timestamp, md.close
            FROM trade_signals ts
            JOIN market_data md ON ts.symbol = md.symbol AND ts.timestamp = md.timestamp
            WHERE ts.status = 'PENDING' AND ts.signal_type = 'BUY'
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
                print(f"Skipping signal {signal_id} for {symbol} at {timestamp}: No market data found.")
                continue

            size, stop_loss = calculate_position_size(close_price)

            if size > 0:
                update_query = """
                    UPDATE trade_signals
                    SET size = ?, stop_loss = ?, status = 'SIZED'
                    WHERE id = ?
                """
                cursor.execute(update_query, (size, stop_loss, signal_id))
                print(f"✅ Processed signal {signal_id}: Symbol={symbol}, Price={close_price}, Size={size}, StopLoss={stop_loss:.4f}")
            else:
                 print(f"Skipping signal {signal_id}: Calculated size is 0 (Price: {close_price}).")

        conn.commit()
        print("Risk Manager completed successfully.")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_risk_manager()
