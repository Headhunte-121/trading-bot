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
        # Define cutoff time to avoid look-ahead bias (current partial candle)
        # Assuming 1h interval, we exclude data from the last hour.
        cutoff_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)
        cutoff_iso = cutoff_time.strftime('%Y-%m-%dT%H:%M:%SZ')

        # Step 1: Find candidates
        # Conditions:
        # - market_data.close < technical_indicators.lower_bb
        # - technical_indicators.rsi_14 < 30
        # - No existing signal in trade_signals for this symbol/timestamp
        # - timestamp < cutoff_iso (Ensure candle is completed)

        query_candidates = """
            SELECT t.symbol, t.timestamp
            FROM technical_indicators t
            JOIN market_data m ON t.symbol = m.symbol AND t.timestamp = m.timestamp
            LEFT JOIN trade_signals s ON t.symbol = s.symbol AND t.timestamp = s.timestamp
            WHERE m.close < t.lower_bb
            AND t.rsi_14 < 30
            AND s.id IS NULL
            AND t.timestamp < ?
        """

        cursor.execute(query_candidates, (cutoff_iso,))
        candidates = cursor.fetchall()

        print(f"Found {len(candidates)} potential candidates.")

        for row in candidates:
            # Handle tuple/Row
            if isinstance(row, sqlite3.Row):
                symbol = row['symbol']
                timestamp = row['timestamp']
            else:
                symbol = row[0]
                timestamp = row[1]

            # Step 2: Check news sentiment
            # Average sentiment score over the last 5 hours relative to the candidate timestamp

            query_news = """
                SELECT AVG(sentiment_score), COUNT(*)
                FROM raw_news
                WHERE symbol = ?
                AND timestamp > datetime(?, '-5 hours')
                AND timestamp <= ?
            """

            cursor.execute(query_news, (symbol, timestamp, timestamp))
            result = cursor.fetchone()

            avg_sentiment = result[0]
            count = result[1]

            # Step 3: Insert signal if conditions met
            # condition: avg sentiment > 0

            if count > 0 and avg_sentiment is not None and avg_sentiment > 0:
                print(f"Generating BUY signal for {symbol} at {timestamp} (Sentiment: {avg_sentiment:.2f})")
                insert_query = """
                    INSERT INTO trade_signals (symbol, timestamp, signal_type, status, size, stop_loss)
                    VALUES (?, ?, 'BUY', 'PENDING', NULL, NULL)
                """
                cursor.execute(insert_query, (symbol, timestamp))

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
