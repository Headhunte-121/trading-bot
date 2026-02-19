import pandas as pd
import pandas_ta as ta
import sqlite3
import os
import sys

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection

def calculate_indicators():
    """Calculates RSI and Lower Bollinger Band for each symbol and updates the database using a sliding window."""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Get list of symbols
        cursor.execute("SELECT DISTINCT symbol FROM market_data")
        symbols = [row[0] for row in cursor.fetchall()]

        print(f"Processing {len(symbols)} symbols: {symbols}")

        for symbol in symbols:
            # Get last processed timestamp
            cursor.execute("SELECT MAX(timestamp) FROM technical_indicators WHERE symbol = ?", (symbol,))
            result = cursor.fetchone()
            last_ts = result[0] if result else None

            df = pd.DataFrame()

            if last_ts:
                # Fetch lookback (200 rows) for warm-up + new data
                # Using 2 separate queries to avoid complex UNION syntax issues and ensure correct sorting
                query_lookback = """
                    SELECT * FROM market_data
                    WHERE symbol = ? AND timestamp <= ?
                    ORDER BY timestamp DESC LIMIT 200
                """
                df_lookback = pd.read_sql_query(query_lookback, conn, params=(symbol, last_ts))

                query_new = """
                    SELECT * FROM market_data
                    WHERE symbol = ? AND timestamp > ?
                    ORDER BY timestamp ASC
                """
                df_new = pd.read_sql_query(query_new, conn, params=(symbol, last_ts))

                if df_new.empty:
                    # No new data to process
                    continue

                # Combine: lookback (reversed to be ASC) + new
                df_lookback = df_lookback.iloc[::-1] # Reverse to chronological order
                df = pd.concat([df_lookback, df_new])
            else:
                # Full load if no history
                query = "SELECT * FROM market_data WHERE symbol = ? ORDER BY timestamp ASC"
                df = pd.read_sql_query(query, conn, params=(symbol,))

            if df.empty or len(df) < 14:
                print(f"Not enough data for {symbol}. Skipping.")
                continue

            # Ensure columns are float for calculation
            # SQLite might return strings if declared as REAL but inserted as text? No, REAL is float.
            # But safe to cast.
            # pandas-ta needs 'close'

            # Calculate RSI (14)
            try:
                rsi = df.ta.rsi(length=14)
            except Exception as e:
                print(f"RSI error for {symbol}: {e}")
                continue

            # Calculate Bollinger Bands (20, 2)
            try:
                bb = df.ta.bbands(length=20, std=2)
            except Exception as e:
                print(f"BB error for {symbol}: {e}")
                continue

            if rsi is None or bb is None:
                continue

            # Find Lower Band column
            bbl_col = next((col for col in bb.columns if col.startswith('BBL')), None)
            if not bbl_col:
                print(f"Lower BB column not found for {symbol}. Columns: {bb.columns}")
                continue

            df['rsi_14'] = rsi
            df['lower_bb'] = bb[bbl_col]

            # Filter for rows to insert
            if last_ts:
                # Only keep rows strictly newer than last_ts
                # We use string comparison which works for ISO8601
                df_to_insert = df[df['timestamp'] > last_ts].copy()
            else:
                df_to_insert = df.copy()

            # Drop NaNs in target columns (warm-up period results might be NaN)
            df_to_insert = df_to_insert.dropna(subset=['rsi_14', 'lower_bb'])

            if df_to_insert.empty:
                continue

            # Prepare data for insertion
            results = []
            for _, row in df_to_insert.iterrows():
                results.append((
                    row['symbol'],
                    row['timestamp'],
                    float(row['rsi_14']),
                    float(row['lower_bb'])
                ))

            print(f"Inserting {len(results)} new rows for {symbol}...")

            # Use executemany for batch insertion
            try:
                cursor.executemany("""
                    INSERT OR REPLACE INTO technical_indicators (symbol, timestamp, rsi_14, lower_bb)
                    VALUES (?, ?, ?, ?)
                """, results)
                conn.commit()
            except sqlite3.OperationalError as e:
                 print(f"Insertion failed for {symbol}: {e}")

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    calculate_indicators()
