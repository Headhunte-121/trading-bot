import sys
import os
import yfinance as yf
import pandas as pd
import sqlite3
import datetime
import time

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection, log_system_event
from shared.config import SYMBOLS
from shared.smart_sleep import get_sleep_seconds

def get_last_timestamp(cursor, symbol, timeframe):
    """
    Returns the latest timestamp for a symbol and timeframe from the database.
    """
    try:
        cursor.execute(
            "SELECT MAX(timestamp) FROM market_data WHERE symbol = ? AND timeframe = ?",
            (symbol, timeframe)
        )
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception:
        return None

def fetch_and_store(symbol, timeframe, period, interval, limit=None):
    """
    Fetches market data for a symbol and timeframe, handling redundancy.
    limit: Optional integer to keep only the last N rows before insertion.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        last_ts = get_last_timestamp(cursor, symbol, timeframe)

        # Determine fetch strategy
        df = pd.DataFrame()

        if last_ts:
            # Parse timestamp to check if we need to fetch
            last_dt = datetime.datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
            start_date = last_dt.strftime('%Y-%m-%d')

            # Use 'start' parameter for updates
            # yfinance history with start date fetches from 00:00 of that date
            print(f"🔄 Updating {symbol} ({timeframe}). Last: {last_ts}...")
            df = yf.Ticker(symbol).history(start=start_date, interval=interval)
        else:
            # Full fetch if no history
            print(f"📜 Initial Fetch {symbol} ({timeframe})...")
            df = yf.Ticker(symbol).history(period=period, interval=interval)

        if df.empty:
            print(f"⚠️ No data returned for {symbol} ({timeframe}). Market holiday or halt?")
            return

        # Optional: Limit rows (e.g. strict intraday update)
        if limit and len(df) > limit:
            df = df.tail(limit)

        # Prepare for DB
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

                rows_to_insert.append((symbol, timestamp, timeframe, open_price, high_price, low_price, close_price, volume))

            except Exception as row_error:
                continue

        if rows_to_insert:
            # INSERT OR IGNORE to handle overlaps
            cursor.executemany('''
                INSERT OR IGNORE INTO market_data
                (symbol, timestamp, timeframe, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', rows_to_insert)
            conn.commit()
            print(f"✅ Saved {len(rows_to_insert)} rows for {symbol} ({timeframe}).")
            log_system_event("MarketHarvester", "INFO", f"Ingested {len(rows_to_insert)} rows for {symbol} ({timeframe})")

    except Exception as e:
        print(f"❌ Error fetching {symbol} ({timeframe}): {e}")
        log_system_event("MarketHarvester", "ERROR", f"Error fetching {symbol} ({timeframe}): {str(e)}")
    finally:
        conn.close()

def initial_sync():
    """
    Runs once at startup to fetch 1 year of 1d data and ensure SMA 200 data is available.
    """
    print("🚀 Starting Initial Sync (Daily Data for SMA 200)...")
    log_system_event("MarketHarvester", "INFO", "Starting Initial Sync (Daily Data for SMA 200)")
    for symbol in SYMBOLS:
        # Fetch 2 years of daily data to be safe for SMA 200 calculation
        fetch_and_store(symbol, "1d", "2y", "1d")
        time.sleep(0.2)
    print("✅ Initial Sync Complete.")
    log_system_event("MarketHarvester", "INFO", "Initial Sync Complete")

def intraday_sync():
    """
    Runs every 5 minutes to fetch recent 5m data.
    """
    print("🔄 Running Intraday Sync (5m)...")
    for symbol in SYMBOLS:
        # Fetch 1 day of 5m data (covers today's market hours).
        # We limit to last 5 candles if updating, just to be lean as requested,
        # but sticking to period="1d" is safest network-wise.
        # However, passing limit=5 respects the "fetch only last 3-5 candles" directive
        # for processing/insertion, even if yfinance fetches the day.
        fetch_and_store(symbol, "5m", "1d", "5m", limit=5)
        time.sleep(0.2)
    print("✅ Intraday Sync Cycle Complete.")

def main():
    print("🚀 Starting Smart Market Harvester (Dual-Mode)...")
    
    # 1. Initial Sync (One-time)
    initial_sync()

    # 2. Intraday Loop
    while True:
        intraday_sync()

        # Smart Sleep
        sleep_sec = get_sleep_seconds()
        print(f"💤 Sleeping for {sleep_sec} seconds...")
        time.sleep(sleep_sec)

if __name__ == "__main__":
    main()
