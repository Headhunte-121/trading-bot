import sys
import os
import yfinance as yf
import pandas as pd
import sqlite3
import datetime

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection, DB_PATH
from shared.schema import setup_database

# CHANGED: period to "5d" and interval to "5m" for fast, 5-minute candles
def fetch_market_data(symbols, period="5d", interval="5m"):
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
                data = df
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
                if index.tzinfo is None:
                    ts_utc = index.tz_localize(datetime.timezone.utc)
                else:
                    ts_utc = index.tz_convert(datetime.timezone.utc)

                timestamp = ts_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

                open_price = row
                high_price = row
                low_price = row
                close_price = row
                volume = row

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
    setup_database()

    symbols = ['NVDA', 'AAPL', 'MSFT', 'AMZN', 'GOOGL', 'GOOG', 'META', 'AVGO', 'TSLA', 'BRK.B', 'WMT', 'LLY', 'JPM', 'XOM', 'V', 'JNJ', 'MU', 'MA', 'ORCL', 'COST', 'ABBV', 'BAC', 'HD', 'CVX', 'PG', 'CAT', 'GE', 'KO', 'AMD', 'NFLX', 'PLTR', 'CSCO', 'MRK', 'LRCX', 'AMAT', 'PM', 'MS', 'RTX', 'GS', 'WFC', 'UNH', 'IBM', 'TMUS', 'MCD', 'AXP', 'LIN', 'GEV', 'PEP', 'INTC', 'VZ']
    print(f"Fetching 5-minute data for {symbols}...")
    market_data = fetch_market_data(symbols)

    if market_data:
        print(f"Saving data to {DB_PATH}...")
        save_to_db(market_data)
    else:
        print("No data fetched.")
