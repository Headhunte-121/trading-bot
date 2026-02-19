import sys
import os
import time
import yfinance as yf
import pandas as pd
import sqlite3
import datetime

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection, DB_PATH
from shared.schema import setup_database

def fetch_market_data(symbols, period="7d", interval="1h"):
    """
    Fetches market data for given symbols using yfinance.
    """
    data = {}
    for symbol in symbols:
        try:
            ticker = yf.Ticker(symbol)
            # Fetch data
            df = ticker.history(period=period, interval=interval)
            if not df.empty:
                data[symbol] = df
            else:
                print(f"Warning: No data found for {symbol}")
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
    return data

def save_to_db(data):
    """
    Inserts market data into the database.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        for symbol, df in data.items():
            for index, row in df.iterrows():
                # Ensure UTC timestamp
                # yfinance index is usually tz-aware
                if index.tzinfo is None:
                    # If naive, assume UTC or localize to UTC?
                    # Stocks are usually in exchange time. But we want UTC.
                    # Safety fallback: assume UTC.
                    ts_utc = index.tz_localize(datetime.timezone.utc)
                else:
                    ts_utc = index.tz_convert(datetime.timezone.utc)

                timestamp = ts_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

                open_price = row['Open']
                high_price = row['High']
                low_price = row['Low']
                close_price = row['Close']
                volume = row['Volume']

                # With WAL mode and increased timeout, explicit retries are less critical but still safe.
                # We rely on get_db_connection's timeout (30s).
                try:
                    cursor.execute('''
                        INSERT OR REPLACE INTO market_data
                        (symbol, timestamp, open, high, low, close, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (symbol, timestamp, open_price, high_price, low_price, close_price, volume))
                except sqlite3.OperationalError as e:
                    print(f"Failed to insert {symbol} at {timestamp}: {e}")

        conn.commit()
        print("Data successfully inserted into database.")

    except Exception as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # Initialize database (idempotent)
    setup_database()

    symbols = ['AAPL', 'MSFT']
    print(f"Fetching data for {symbols}...")
    market_data = fetch_market_data(symbols)

    if market_data:
        print(f"Saving data to {DB_PATH}...")
        save_to_db(market_data)
    else:
        print("No data fetched.")
