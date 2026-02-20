import sys
import os
import yfinance as yf
import pandas as pd
import sqlite3
import datetime
import time

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection, DB_PATH
from shared.schema import setup_database

def fetch_market_data(symbols, period="5d", interval="5m"):
    """
    Fetches market data for given symbols using yfinance.
    Returns a Dictionary: { 'AAPL': dataframe, 'MSFT': dataframe }
    """
    data_dict = {} # Renamed for clarity
    
    for symbol in symbols:
        try:
            print(f"📉 Downloading {symbol}...")
            ticker = yf.Ticker(symbol)
            # Fetch data
            df = ticker.history(period=period, interval=interval)
            
            if not df.empty:
                # CRITICAL FIX: Save to the dictionary with the symbol as the key
                # Do NOT write 'data = df', that deletes everything else!
                data_dict[symbol] = df
            else:
                print(f"⚠️ Warning: No data found for {symbol}")
            
            # Small sleep to be polite to Yahoo API and prevent timeouts
            time.sleep(0.5) 
            
        except Exception as e:
            print(f"❌ Error fetching data for {symbol}: {e}")
            
    return data_dict

def save_to_db(data_dict):
    """
    Inserts market data into the database.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Loop through the dictionary items
        for symbol, df in data_dict.items():
            print(f"💾 Saving {len(df)} rows for {symbol}...")
            
            for index, row in df.iterrows():
                # Ensure UTC timestamp
                if index.tzinfo is None:
                    ts_utc = index.tz_localize(datetime.timezone.utc)
                else:
                    ts_utc = index.tz_convert(datetime.timezone.utc)

                timestamp = ts_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

                # Handle yfinance sometimes missing columns or having different casing
                try:
                    open_price = row.get('Open', 0.0)
                    high_price = row.get('High', 0.0)
                    low_price = row.get('Low', 0.0)
                    close_price = row.get('Close', 0.0)
                    volume = row.get('Volume', 0)

                    cursor.execute('''
                        INSERT OR REPLACE INTO market_data
                        (symbol, timestamp, open, high, low, close, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (symbol, timestamp, open_price, high_price, low_price, close_price, volume))
                except Exception as row_error:
                    continue # Skip bad rows

        conn.commit()
        print("✅ Data batch successfully inserted into database.")

    except Exception as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    setup_database()

    # NOTE: Changed 'BRK.B' to 'BRK-B' because Yahoo Finance uses a dash, not a dot.
    symbols = [
        'NVDA', 'AAPL', 'MSFT', 'AMZN', 'GOOGL', 'GOOG', 'META', 'AVGO', 'TSLA', 'BRK-B', 
        'WMT', 'LLY', 'JPM', 'XOM', 'V', 'JNJ', 'MU', 'MA', 'ORCL', 'COST', 
        'ABBV', 'BAC', 'HD', 'CVX', 'PG', 'CAT', 'GE', 'KO', 'AMD', 'NFLX', 
        'PLTR', 'CSCO', 'MRK', 'LRCX', 'AMAT', 'PM', 'MS', 'RTX', 'GS', 'WFC', 
        'UNH', 'IBM', 'TMUS', 'MCD', 'AXP', 'LIN', 'GEV', 'PEP', 'INTC', 'VZ'
    ]
    
    print(f"🚀 Fetching 5-minute data for {len(symbols)} symbols...")
    market_data = fetch_market_data(symbols)

    # CRITICAL FIX: Checking truthiness of a Dictionary, not a DataFrame
    if market_data:
        print(f"Saving data to {DB_PATH}...")
        save_to_db(market_data)
    else:
        print("❌ No data fetched for any symbol.")
