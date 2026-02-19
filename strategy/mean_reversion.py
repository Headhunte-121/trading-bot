import sqlite3
import os
import sys
import datetime

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection

def run_mean_reversion():
    """
    Executes the mean reversion strategy logic.
    """
    print("Running Mean Reversion Strategy...")
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # CHANGED: Cutoff time is now 5 minutes instead of 1 hour!
        # This prevents it from ignoring fresh signals.
        cutoff_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=5)
        cutoff_iso = cutoff_time.strftime('%Y-%m-%dT%H:%M:%SZ')

        # Step 1: Find candidates
        query_candidates = """
            SELECT t.symbol, t.timestamp
            FROM technical_indicators t
            JOIN market_data m ON t.symbol = m.symbol AND t.timestamp = m.timestamp
            LEFT JOIN trade_signals s ON t.symbol = s.symbol AND t.timestamp = s.timestamp
            WHERE m.close < t.lower_bb
            AND t.rsi_14 < 30
            AND s.id IS NULL
            AND t.timestamp <= ?
        """

        cursor.execute(query_candidates, (cutoff_iso,))
        candidates = cursor.fetchall()

        print(f"Found {len(candidates)} potential candidates.")

        for row in candidates:
            if isinstance(row, sqlite3.Row):
                symbol = row
                timestamp = row
            else:
                symbol = row
                timestamp = row

            # Step 2: Check news sentiment (Look back 12 hours instead of 5 for more context)
            query_news = """
                SELECT AVG(sentiment_score), COUNT(*)
                FROM raw_news
                WHERE symbol = ?
                AND timestamp > datetime(?, '-12 hours')
                AND timestamp <= ?
            """

            cursor.execute(query_news, (symbol, timestamp, timestamp))
            result = cursor.fetchone()

            avg_sentiment = result
            count = result

            # Step 3: Insert signal if conditions met
            # condition: avg sentiment > 0 (News must be positive to buy the dip!)
            if count > 0 and avg_sentiment is not None and avg_sentiment > 0:
                print(f"⭐⭐ BUY SIGNAL DETECTED for {symbol} at {timestamp} (Sentiment: {avg_sentiment:.2f}) ⭐⭐")
                insert_query = """
                    INSERT INTO trade_signals (symbol, timestamp, signal_type, status, size, stop_loss)
                    VALUES (?, ?, 'BUY', 'PENDING', NULL, NULL)
                """
                cursor.execute(insert_query, (symbol, timestamp))
            else:
                print(f"Ignored {symbol} at {timestamp}. RSI was low, but sentiment was negative ({avg_sentiment}).")

        conn.commit()
        print("Strategy cycle completed.")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_mean_reversion()
