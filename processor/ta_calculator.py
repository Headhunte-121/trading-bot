import sqlite3
import pandas as pd
import pandas_ta as ta
import os

# Define the path to the database
DB_NAME = "trade_history.db"
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, DB_NAME)

def calculate_indicators():
    """Calculates RSI and Lower Bollinger Band for each symbol and updates the database."""
    print(f"Connecting to database at {DB_PATH}...")

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=15)

        # Read market data
        # We need enough history for calculation. The user said "Pull ALL historical data".
        query = "SELECT symbol, timestamp, close FROM market_data ORDER BY timestamp ASC"
        df = pd.read_sql_query(query, conn)

        if df.empty:
            print("No market data found.")
            return

        # Initialize lists to store results
        results = []

        # Process each symbol independently
        symbols = df['symbol'].unique()
        print(f"Processing {len(symbols)} symbols: {symbols}")

        for symbol in symbols:
            # Create a copy to avoid SettingWithCopyWarning
            symbol_df = df[df['symbol'] == symbol].copy()

            # Ensure timestamp is sorted
            symbol_df = symbol_df.sort_values('timestamp')

            if len(symbol_df) < 14:
                print(f"Not enough data for {symbol} (need at least 14 rows). Skipping.")
                continue

            # Calculate RSI (14)
            # This returns a Series named 'RSI_14' by default
            rsi = symbol_df.ta.rsi(length=14)
            if rsi is None:
                 print(f"RSI calculation failed for {symbol}")
                 continue

            # Calculate Bollinger Bands (20, 2)
            # This returns a DataFrame with multiple columns
            bb = symbol_df.ta.bbands(length=20, std=2)
            if bb is None:
                print(f"BB calculation failed for {symbol}")
                continue

            # Find the Lower Band column (starts with BBL)
            bbl_col = next((col for col in bb.columns if col.startswith('BBL')), None)

            if not bbl_col:
                print(f"Could not find Lower Bollinger Band column for {symbol}. Columns: {bb.columns}")
                continue

            # Merge indicators back to the symbol_df
            # We use the index to align (pandas handles this)
            symbol_df['rsi_14'] = rsi
            symbol_df['lower_bb'] = bb[bbl_col]

            # Filter for rows where both indicators are valid (not NaN)
            valid_rows = symbol_df.dropna(subset=['rsi_14', 'lower_bb'])

            if valid_rows.empty:
                print(f"No valid indicator data for {symbol} after filtering NaNs.")
                continue

            # Prepare data for insertion
            # The table expects: symbol, timestamp, rsi_14, lower_bb
            for _, row in valid_rows.iterrows():
                results.append((
                    row['symbol'],
                    row['timestamp'],
                    float(row['rsi_14']),
                    float(row['lower_bb'])
                ))

        if not results:
            print("No valid data to insert.")
            return

        print(f"Inserting {len(results)} rows into technical_indicators...")

        cursor = conn.cursor()

        # Use INSERT OR REPLACE to handle composite primary keys
        cursor.executemany("""
            INSERT OR REPLACE INTO technical_indicators (symbol, timestamp, rsi_14, lower_bb)
            VALUES (?, ?, ?, ?)
        """, results)

        conn.commit()
        print("Data inserted successfully.")

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    calculate_indicators()
