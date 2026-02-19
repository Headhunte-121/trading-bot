import sqlite3
import os

# Default path relative to this script
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "trade_history.db")

def run_mean_reversion(db_path=DB_PATH):
    """
    Executes the mean reversion strategy logic.
    """
    conn = None
    try:
        # Ensure the directory exists if we are using the default path or a path that implies a directory structure
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
             os.makedirs(db_dir, exist_ok=True)

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Step 1: Find candidates
        # Conditions:
        # - market_data.close < technical_indicators.lower_bb
        # - technical_indicators.rsi_14 < 30
        # - No existing signal in trade_signals for this symbol/timestamp

        query_candidates = """
            SELECT t.symbol, t.timestamp
            FROM technical_indicators t
            JOIN market_data m ON t.symbol = m.symbol AND t.timestamp = m.timestamp
            LEFT JOIN trade_signals s ON t.symbol = s.symbol AND t.timestamp = s.timestamp
            WHERE m.close < t.lower_bb
            AND t.rsi_14 < 30
            AND s.id IS NULL
        """

        cursor.execute(query_candidates)
        candidates = cursor.fetchall()

        for symbol, timestamp in candidates:
            # Step 2: Check news sentiment
            # Average sentiment score over the last 5 hours relative to the candidate timestamp

            # SQLite datetime modifier: datetime(timestamp, '-5 hours')
            # We assume timestamp is in a format SQLite understands (ISO8601)

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
            # condition: avg sentiment > 0 (and implies count > 0, since avg of nothing is NULL)

            if count > 0 and avg_sentiment is not None and avg_sentiment > 0:
                print(f"Generating BUY signal for {symbol} at {timestamp}")
                insert_query = """
                    INSERT INTO trade_signals (symbol, timestamp, signal_type, status, size, stop_loss)
                    VALUES (?, ?, 'BUY', 'PENDING', NULL, NULL)
                """
                cursor.execute(insert_query, (symbol, timestamp))

        conn.commit()

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    run_mean_reversion()
