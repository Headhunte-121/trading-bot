import sqlite3
import os

# Configuration
ACCOUNT_SIZE = 100000
RISK_PCT = 0.01
STOP_LOSS_PCT = 0.02

# Database Path
DB_NAME = "trade_history.db"
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, DB_NAME)

def calculate_position_size(close_price):
    """
    Calculates the position size (number of shares) and stop loss price.

    Logic:
    - Risk Amount = Account Size * Risk % ($1,000)
    - Stop Loss Distance = Close Price * Stop Loss %
    - Shares = Risk Amount / Stop Loss Distance
    - Alternatively: Position Value = Risk Amount / Stop Loss % ($50,000)
    - Shares = Position Value / Close Price

    Returns:
        (size, stop_loss_price)
    """
    if close_price <= 0:
        return 0.0, 0.0

    risk_amount = ACCOUNT_SIZE * RISK_PCT
    stop_loss_distance = close_price * STOP_LOSS_PCT

    # Avoid division by zero
    if stop_loss_distance == 0:
        return 0.0, 0.0

    shares = risk_amount / stop_loss_distance
    stop_loss_price = close_price - stop_loss_distance

    return shares, stop_loss_price

def run_risk_manager():
    print("Starting Risk Manager...")

    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
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
            signal_id, symbol, timestamp, close_price = signal

            # Additional check, though INNER JOIN should handle it mostly
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
                print(f"Processed signal {signal_id}: Symbol={symbol}, Price={close_price}, Size={size:.4f}, StopLoss={stop_loss:.4f}")
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
