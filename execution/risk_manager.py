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
CRYPTO_NOTIONAL_LIMIT = 1000.00 # Hard cap for Crypto

def calculate_position_size(symbol, close_price):
    """
    Calculates the position size.
    Equities: Shares (int) based on 1% risk.
    Crypto: Quantity (float) based on fixed $1000 notional.
    """
    if close_price <= 0:
        return 0

    # Crypto Sizing (/USD)
    if "/USD" in symbol:
        # Notional Sizing
        # qty = Target Value / Price
        size = CRYPTO_NOTIONAL_LIMIT / close_price
        return size # Returns float

    # Equity Sizing (Standard)
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
        # Find PENDING signals and their corresponding close price
        # We need the close price from market_data (most recent 5m candle)
        # JOIN with market_data based on symbol and timestamp
        query = """
            SELECT ts.id, ts.symbol, ts.timestamp, md.close
            FROM trade_signals ts
            JOIN market_data md ON ts.symbol = md.symbol AND ts.timestamp = md.timestamp
            WHERE ts.status = 'PENDING'
            AND md.timeframe = '5m'
        """

        cursor.execute(query)
        pending_signals = cursor.fetchall()

        if len(pending_signals) > 0:
            print(f"Found {len(pending_signals)} pending signals.")

        for signal in pending_signals:
            signal_id = signal['id']
            symbol = signal['symbol']
            timestamp = signal['timestamp']
            close_price = signal['close']

            if close_price is None:
                print(f"Skipping signal {signal_id} for {symbol}: No market data found.")
                continue

            size = calculate_position_size(symbol, close_price)

            if size > 0:
                # Update signal with size
                update_query = """
                    UPDATE trade_signals
                    SET size = ?, status = 'SIZED'
                    WHERE id = ?
                """
                cursor.execute(update_query, (size, signal_id))
                conn.commit() # Commit immediately
                print(f"✅ Sized signal {signal_id}: Symbol={symbol}, Price={close_price:.2f}, Size={size}")
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
