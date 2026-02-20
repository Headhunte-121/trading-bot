import sys
import os
import yfinance as yf
import pandas as pd
import pandas_ta as ta
import sqlite3
import datetime
import time

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection, DB_PATH
from shared.schema import setup_database
from shared.config import SYMBOLS

def fetch_market_data(symbols, period="5d", interval="5m"):
    """
    Fetches market data for given symbols using yfinance.
    Returns a Dictionary: { 'AAPL': dataframe, 'MSFT': dataframe }
    """
    data_dict = {} # Renamed for clarity
    
    for symbol in symbols:
        try:
            print(f"📉 Downloading {symbol} (5m)...")
            ticker = yf.Ticker(symbol)
            # Fetch data
            df = ticker.history(period=period, interval=interval)
            
            if not df.empty:
                data_dict[symbol] = df
            else:
                print(f"⚠️ Warning: No 5m data found for {symbol}")
            
            # Small sleep to be polite to Yahoo API and prevent timeouts
            time.sleep(0.2)
            
        except Exception as e:
            print(f"❌ Error fetching 5m data for {symbol}: {e}")
            
    return data_dict

def fetch_daily_data(symbols):
    """
    Fetches 1 year of daily data to calculate SMA 200.
    Returns a Dictionary: { 'AAPL': dataframe, ... }
    """
    data_dict = {}
    for symbol in symbols:
        try:
            print(f"☀️ Downloading {symbol} (Daily)...")
            ticker = yf.Ticker(symbol)
            # Fetch 1 year of history for SMA 200
            df = ticker.history(period="1y", interval="1d")

            if not df.empty and len(df) >= 200:
                # Calculate SMA 200
                df['sma_200'] = df.ta.sma(length=200)
                data_dict[symbol] = df
            else:
                print(f"⚠️ Warning: Not enough daily data for {symbol} (Need 200, got {len(df)})")

            time.sleep(0.2)
        except Exception as e:
            print(f"❌ Error fetching Daily data for {symbol}: {e}")
    return data_dict

def save_to_db(data_dict):
    """
    Inserts 5-minute market data into the database using batch execution.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Loop through the dictionary items
        for symbol, df in data_dict.items():
            print(f"💾 Saving {len(df)} 5m rows for {symbol}...")
            
            rows_to_insert = []
            for index, row in df.iterrows():
                try:
                    # Ensure UTC timestamp
                    if index.tzinfo is None:
                        ts_utc = index.tz_localize(datetime.timezone.utc)
                    else:
                        ts_utc = index.tz_convert(datetime.timezone.utc)

                    timestamp = ts_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

                    # Handle yfinance sometimes missing columns or having different casing
                    open_price = row.get('Open', 0.0)
                    high_price = row.get('High', 0.0)
                    low_price = row.get('Low', 0.0)
                    close_price = row.get('Close', 0.0)
                    volume = row.get('Volume', 0)

                    rows_to_insert.append((symbol, timestamp, open_price, high_price, low_price, close_price, volume))

                except Exception as row_error:
                    print(f"Error processing row for {symbol}: {row_error}")
                    continue

            if rows_to_insert:
                cursor.executemany('''
                    INSERT OR REPLACE INTO market_data
                    (symbol, timestamp, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', rows_to_insert)

        conn.commit()
        print("✅ 5m Data batch successfully inserted.")

    except Exception as e:
        print(f"Database error (5m): {e}")
    finally:
        if conn:
            conn.close()

def save_daily_to_db(data_dict):
    """
    Inserts Daily market data (with SMA 200) into the database.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        for symbol, df in data_dict.items():
            # Filter for rows that have SMA 200 (first 199 will be NaN)
            df_valid = df.dropna(subset=['sma_200'])

            if df_valid.empty:
                continue

            # We only really need the LATEST daily candle to determine the trend for today,
            # but saving the last few helps with history/charts if needed.
            # Let's save the last 5 days to be safe and efficient.
            df_recent = df_valid.tail(5)

            print(f"💾 Saving {len(df_recent)} Daily rows for {symbol}...")

            rows_to_insert = []
            for index, row in df_recent.iterrows():
                try:
                    # Daily data index is usually just Date (YYYY-MM-DD) or Timestamp at 00:00
                    # We'll store it as YYYY-MM-DD string for 'date' column
                    date_str = index.strftime('%Y-%m-%d')

                    close_price = row.get('Close', 0.0)
                    sma_200 = row.get('sma_200', 0.0)

                    rows_to_insert.append((symbol, date_str, close_price, sma_200))

                except Exception as row_error:
                    print(f"Error processing daily row for {symbol}: {row_error}")
                    continue

            if rows_to_insert:
                cursor.executemany('''
                    INSERT OR REPLACE INTO market_data_daily
                    (symbol, date, close, sma_200)
                    VALUES (?, ?, ?, ?)
                ''', rows_to_insert)

        conn.commit()
        print("✅ Daily Data (SMA 200) batch successfully inserted.")

    except Exception as e:
        print(f"Database error (Daily): {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    setup_database()
    
    # 1. Fetch Daily Data (SMA 200) - The "Big Picture"
    # We do this first so we have the trend established.
    print(f"\n🚀 Fetching Daily data for {len(SYMBOLS)} symbols...")
    daily_data = fetch_daily_data(SYMBOLS)
    if daily_data:
        save_daily_to_db(daily_data)
    else:
        print("❌ No daily data fetched.")

    # 2. Fetch 5-minute Data - The "Execution"
    print(f"\n🚀 Fetching 5-minute data for {len(SYMBOLS)} symbols...")
    market_data = fetch_market_data(SYMBOLS)

    if market_data:
        save_to_db(market_data)
    else:
        print("❌ No 5m data fetched.")
