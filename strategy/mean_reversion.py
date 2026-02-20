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
        # We only look for signals in the last hour to prevent processing old history
        # but ensure we catch recent missed signals.
        lookback_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)
        lookback_iso = lookback_time.strftime('%Y-%m-%dT%H:%M:%SZ')

        # Step 1: Find candidates
        # We look for RSI < 30 and Close < Lower Bollinger Band
        # signal must not already exist
        query_candidates = """
            SELECT t.symbol, t.timestamp, t.rsi_14, t.lower_bb, m.close
            FROM technical_indicators t
            JOIN market_data m ON t.symbol = m.symbol AND t.timestamp = m.timestamp
            LEFT JOIN trade_signals s ON t.symbol = s.symbol AND t.timestamp = s.timestamp
            WHERE m.close < t.lower_bb
            AND t.rsi_14 < 30
            AND s.id IS NULL
            AND t.timestamp >= ?
            ORDER BY t.timestamp ASC
        """

        cursor.execute(query_candidates, (lookback_iso,))
        candidates = cursor.fetchall()

        print(f"Found {len(candidates)} potential candidates in the last hour.")

        for row in candidates:
            symbol = row['symbol']
            timestamp = row['timestamp']

            # Parse timestamp to calculate news window
            try:
                ts_dt = datetime.datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            except ValueError:
                # Handle potential non-ISO strings if any
                print(f"Skipping {symbol} due to invalid timestamp: {timestamp}")
                continue

            start_ts_dt = ts_dt - datetime.timedelta(hours=12)
            start_ts_iso = start_ts_dt.strftime('%Y-%m-%dT%H:%M:%SZ')

            # Step 2: Check news sentiment (last 12 hours relative to the candle time)
            query_news = """
                SELECT AVG(sentiment_score) as avg_score, COUNT(*) as count
                FROM raw_news
                WHERE symbol = ?
                AND timestamp > ?
                AND timestamp <= ?
            """

            cursor.execute(query_news, (symbol, start_ts_iso, timestamp))
            result = cursor.fetchone()

            avg_sentiment = result['avg_score']
            count = result['count']

            # Step 3: Insert signal if conditions met
            # Condition: Sentiment > 0.3 (Strong Positive)
            if count > 0 and avg_sentiment is not None and avg_sentiment > 0.3:
                print(f"⭐⭐ BUY SIGNAL DETECTED for {symbol} at {timestamp} (Sentiment: {avg_sentiment:.2f}) ⭐⭐")
                
                cursor.execute("""
                    INSERT INTO trade_signals (symbol, timestamp, signal_type, status, size, stop_loss)
                    VALUES (?, ?, 'BUY', 'PENDING', NULL, NULL)
                """, (symbol, timestamp))
            elif count > 0:
                print(f"Ignored {symbol} at {timestamp}. RSI low, but sentiment ({avg_sentiment:.2f}) not convincing.")
            else:
                 # No news, ignore or maybe take a risk? Strategy says we need sentiment.
                 pass

        conn.commit()
        print("Strategy cycle completed.")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    run_mean_reversion()
