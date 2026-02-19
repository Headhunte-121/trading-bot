import sys
import os
import sqlite3
import time
import yfinance as yf
import pandas as pd

# Go up one directory to access the shared folder
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from shared.schema import setup_database

def fetch_market_data(symbols, period="7d", interval="1h"):
    """
    Fetches market data for given symbols using yfinance.
    """
    data = {}
    for symbol in symbols:
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(period=period, interval=interval)
            if not df.empty:
                data[symbol] = df
            else:
                print(f"Warning: No data found for {symbol}")
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
    return data

def save_to_db(data, db_path):
    """
    Inserts market data into the database.
    Handles SQLite locking errors with retries.
    """
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        for symbol, df in data.items():
            for index, row in df.iterrows():
                timestamp = index.isoformat()
                open_price = row['Open']
                high_price = row['High']
                low_price = row['Low']
                close_price = row['Close']
                volume = row['Volume']

                retries = 5
                while retries > 0:
                    try:
                        cursor.execute('''
                            INSERT OR REPLACE INTO market_data
                            (symbol, timestamp, open, high, low, close, volume)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (symbol, timestamp, open_price, high_price, low_price, close_price, volume))
                        break
                    except sqlite3.OperationalError as e:
                        if "database is locked" in str(e):
                            retries -= 1
                            time.sleep(1)
                        else:
                            raise e
                if retries == 0:
                    print(f"Failed to insert data for {symbol} at {timestamp} due to database lock.")

        conn.commit()
        print("Data successfully inserted into database.")

    except Exception as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # Initialize database
    setup_database()

    # Define database path relative to this script
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DB_PATH = os.path.join(BASE_DIR, "data", "trade_history.db")

    symbols = ['AAPL', 'MSFT']
    print(f"Fetching data for {symbols}...")
    market_data = fetch_market_data(symbols)

    if market_data:
        print(f"Saving data to {DB_PATH}...")
        save_to_db(market_data, DB_PATH)
    else:
        print("No data fetched.")
