import sqlite3
import os
import sys
import datetime
import time

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection
from shared.smart_sleep import get_sleep_seconds

def run_strategy():
    """
    Executes the Dual-Model Ensemble Strategy.
    Conditions:
    1. Trend: Close > SMA 200
    2. Pullback: 35 < RSI < 55
    3. AI: Ensemble Predicted % Change > 0.5%
    """
    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        print("Running Ensemble Strategy...")

        # Lookback window (e.g. last 60 minutes of candles)
        lookback_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=60)
        lookback_iso = lookback_time.strftime('%Y-%m-%dT%H:%M:%SZ')

        # Updated Query for Ensemble Table
        query = """
            SELECT
                m.symbol,
                m.timestamp,
                m.close,
                t.sma_200,
                t.rsi_14,
                p.ensemble_pct_change,
                p.small_predicted_price,
                p.large_predicted_price
            FROM market_data m
            JOIN technical_indicators t ON m.symbol = t.symbol AND m.timestamp = t.timestamp
            JOIN ai_predictions p ON m.symbol = p.symbol AND m.timestamp = p.timestamp
            WHERE
                m.timeframe = '5m'
                AND m.timestamp >= ?
                AND m.close > t.sma_200
                AND t.rsi_14 > 35
                AND t.rsi_14 < 55
                AND p.ensemble_pct_change > 0.5
            ORDER BY m.timestamp DESC
        """

        cursor.execute(query, (lookback_iso,))
        candidates = cursor.fetchall()

        if not candidates:
            print("No candidates found matching strategy criteria.")
            return

        print(f"Found {len(candidates)} potential signals.")

        for row in candidates:
            symbol = row['symbol']
            timestamp = row['timestamp']
            close = row['close']
            sma_200 = row['sma_200']
            rsi = row['rsi_14']
            pct_change = row['ensemble_pct_change']

            # Check for duplicate signal
            cursor.execute(
                "SELECT id FROM trade_signals WHERE symbol = ? AND timestamp = ?",
                (symbol, timestamp)
            )
            if cursor.fetchone():
                continue # Already signaled

            # Generate Signal
            print(f"⭐⭐ BUY SIGNAL: {symbol} @ {timestamp} | Close: {close:.2f} > SMA: {sma_200:.2f} | RSI: {rsi:.2f} | Ensemble AI: +{pct_change:.2f}% ⭐⭐")

            cursor.execute("""
                INSERT INTO trade_signals
                (symbol, timestamp, signal_type, status, size, stop_loss)
                VALUES (?, ?, 'BUY', 'PENDING', NULL, NULL)
            """, (symbol, timestamp))

            conn.commit()
            print(f"✅ Signal logged for {symbol}.")

    except Exception as e:
        print(f"Strategy Engine Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    while True:
        run_strategy()

        sleep_sec = get_sleep_seconds()
        print(f"💤 Strategy Sleeping for {sleep_sec} seconds...")
        time.sleep(sleep_sec)
