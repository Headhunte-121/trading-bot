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
    conn.row_factory = sqlite3.Row # CRITICAL: Enable column access by name
    cursor = conn.cursor()

    try:
        # Cutoff time is 5 minutes to ensure fresh data
        cutoff_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=5)
        cutoff_iso = cutoff_time.strftime('%Y-%m-%dT%H:%M:%SZ')

        # Step 1: Find candidates
        query_candidates = """
            SELECT t.symbol, t.timestamp, t.rsi_14, t.lower_bb, m.close
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
            # FIX: Explicitly convert Row objects to strings for the next query
            symbol = str(row['symbol'])
            timestamp = str(row['timestamp'])

            # Step 2: Check news sentiment
            query_news = """
                SELECT AVG(sentiment_score), COUNT(*)
                FROM raw_news
                WHERE symbol = ?
                AND timestamp > datetime(?, '-12 hours')
                AND timestamp <= ?
            """

            cursor.execute(query_news, (symbol, timestamp, timestamp))
            result = cursor.fetchone()

            avg_sentiment = result[0]
            count = result[1]

            # Step 3: Insert signal if conditions met
            # Condition: Sentiment > 0.3 (Strong Positive)
            if count > 0 and avg_sentiment is not None and avg_sentiment > 0.3:
                print(f"⭐⭐ BUY SIGNAL DETECTED for {symbol} at {timestamp} (Sentiment: {avg_sentiment:.2f}) ⭐⭐")
                
                cursor.execute("""
                    INSERT INTO trade_signals (symbol, timestamp, signal_type, status, size, stop_loss)
                    VALUES (?, ?, 'BUY', 'PENDING', NULL, NULL)
                """, (symbol, timestamp))
            elif count > 0:
                print(f"Ignored {symbol}. RSI low, but sentiment ({avg_sentiment:.2f}) not convincing.")

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
