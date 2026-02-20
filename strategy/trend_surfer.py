import sqlite3
import os
import sys
import datetime

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection

def run_trend_surfer():
    """
    Executes the Trend Surfer strategy logic.
    """
    print("Running Trend Surfer Strategy...")
    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        # Lookback for candidates (last 60 mins)
        lookback_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=60)
        lookback_iso = lookback_time.strftime('%Y-%m-%dT%H:%M:%SZ')

        # Step 1: Find candidates (Math Filter)
        # Price > SMA 200 AND 35 < RSI < 55
        query_candidates = """
            SELECT t.symbol, t.timestamp, t.rsi_14, t.sma_200, m.close, m.volume, t.lower_bb
            FROM technical_indicators t
            JOIN market_data m ON t.symbol = m.symbol AND t.timestamp = m.timestamp
            WHERE m.close > t.sma_200
            AND t.rsi_14 > 35
            AND t.rsi_14 < 55
            AND t.timestamp >= ?
            ORDER BY t.timestamp DESC
        """

        cursor.execute(query_candidates, (lookback_iso,))
        candidates = cursor.fetchall()

        print(f"Found {len(candidates)} candidates passing Math Filter.")

        for row in candidates:
            symbol = row['symbol']
            timestamp = row['timestamp']
            close = row['close']
            sma_200 = row['sma_200']
            rsi = row['rsi_14']

            # Check if we already have a signal for this candle
            cursor.execute("SELECT id FROM trade_signals WHERE symbol = ? AND timestamp = ?", (symbol, timestamp))
            if cursor.fetchone():
                continue # Already signaled

            # Check if we have an analysis request
            cursor.execute("SELECT status, ai_prediction, ai_confidence, ai_reasoning FROM chart_analysis_requests WHERE symbol = ? AND timestamp = ?", (symbol, timestamp))
            analysis = cursor.fetchone()

            if not analysis:
                # Step 2: Request Analysis
                print(f"Requesting AI Analysis for {symbol} @ {timestamp}...")

                technical_summary = (
                    f"Price: {close:.2f}\n"
                    f"SMA 200: {sma_200:.2f} (Trend: BULLISH)\n"
                    f"RSI: {rsi:.2f} (Status: HEALTHY DIP)\n"
                    f"Volume: {row['volume']}\n"
                    f"Bollinger Lower Band: {row['lower_bb']:.2f}"
                )

                cursor.execute("""
                    INSERT INTO chart_analysis_requests (symbol, timestamp, technical_summary)
                    VALUES (?, ?, ?)
                """, (symbol, timestamp, technical_summary))
                conn.commit()

            elif analysis['status'] == 'COMPLETED':
                # Step 3: Check AI Decision
                prediction = analysis['ai_prediction']
                confidence = analysis['ai_confidence']

                if prediction == 'BULLISH' and confidence > 0.5:
                    # Step 4: Check News Sentiment Shield
                    # "News effectively doesn't matter, UNLESS it is terrible."
                    # Logic: Sentiment Score > -0.2

                    # Fetch aggregated sentiment for last 24h
                    ts_dt = datetime.datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                    start_ts = (ts_dt - datetime.timedelta(hours=24)).strftime('%Y-%m-%dT%H:%M:%SZ')

                    cursor.execute("""
                        SELECT AVG(sentiment_score) as avg_score
                        FROM raw_news
                        WHERE symbol = ?
                        AND timestamp >= ? AND timestamp <= ?
                    """, (symbol, start_ts, timestamp))

                    news_res = cursor.fetchone()
                    avg_sentiment = news_res['avg_score'] if news_res['avg_score'] is not None else 0.0

                    if avg_sentiment > -0.2:
                        print(f"⭐⭐ BUY SIGNAL: {symbol} @ {timestamp} | Chart: {prediction} ({confidence:.2f}) | News: {avg_sentiment:.2f} ⭐⭐")

                        cursor.execute("""
                            INSERT INTO trade_signals (symbol, timestamp, signal_type, status, size, stop_loss)
                            VALUES (?, ?, 'BUY', 'PENDING', NULL, NULL)
                        """, (symbol, timestamp))
                        conn.commit()
                    else:
                        print(f"⛔ BLOCKED: {symbol} - News Sentiment too low ({avg_sentiment:.2f})")
                else:
                    # AI said Bearish or Neutral
                    pass

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    run_trend_surfer()
