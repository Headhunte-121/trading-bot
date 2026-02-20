import sys
import os
import yfinance as yf
import pandas as pd
import sqlite3
import datetime
import time

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection
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

def fetch_and_store(symbol, timeframe, period, interval):
    """
    Fetches market data for a symbol and timeframe, handling redundancy.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        last_ts = get_last_timestamp(cursor, symbol, timeframe)

        # Determine fetch strategy
        fetch_period = period
        if last_ts:
            # Parse timestamp to check if we need to fetch
            last_dt = datetime.datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
            now_dt = datetime.datetime.now(datetime.timezone.utc)
            
            delta = now_dt - last_dt
            
            # If 5m data is older than 5 mins, fetch. If 1d data is older than 1 day, fetch.
            # But yfinance 'period' is not precise enough for "missing candles only" easily without start/end.
            # However, yfinance handles 'start' parameter.
            # Let's use 'start' if we have a last timestamp.
            
            # buffer: start from last_ts to ensure we get the latest (overlaps are handled by INSERT OR IGNORE)
            start_date = last_dt.strftime('%Y-%m-%d')
            
            # yfinance download with start date is often cleaner for "update"
            # BUT yf.Ticker.history with start/end is also good.
            # Let's stick to period if the gap is huge, or use start if gap is small.
            # Actually, the user requirement says: "If a timestamp exists, calculate the delta to now. Only fetch the specific days/hours missing."
            
            # If delta is small (e.g. < 1 day for 5m), we might just fetch '1d' period to be safe and fast.
            # If delta is large, use start/end.

            # Simplification: Always use 'start' if last_ts exists.
            # However, yfinance requires 'start' to be YYYY-MM-DD.
            # If we pass start=today, we get today's data.

            # Let's try to use the 'start' parameter.
            # Note: yfinance `history` method accepts `start` as string or datetime.

            print(f"🔄 Updating {symbol} ({timeframe}). Last: {last_ts}...")
            # We add 1 second to avoid fetching the exact same last candle if possible,
            # but yfinance resolution is minute/day.
            # Overlap is fine due to INSERT OR IGNORE.
            df = yf.Ticker(symbol).history(start=start_date, interval=interval)
        else:
            print(f"📜 Initial Fetch {symbol} ({timeframe})...")
            df = yf.Ticker(symbol).history(period=period, interval=interval)

        if df.empty:
            print(f"⚠️ No data returned for {symbol} ({timeframe}). Market holiday or halt?")
            return

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

    except Exception as e:
        print(f"❌ Error fetching {symbol} ({timeframe}): {e}")
    finally:
        conn.close()

def main():
    print("🚀 Starting Smart Market Harvester...")
    
    while True:
        # Loop through symbols
        print(f"\n🔄 Cycle Start: Processing {len(SYMBOLS)} symbols...")

        for symbol in SYMBOLS:
            # 1. Fetch Daily (1d) - Period '1y' for history or 'start' for update
            fetch_and_store(symbol, "1d", "1y", "1d")

            # Anti-Ban Sleep
            time.sleep(0.2)

            # 2. Fetch Intraday (5m) - Period '5d' for history or 'start' for update
            fetch_and_store(symbol, "5m", "5d", "5m")

            # Anti-Ban Sleep
            time.sleep(0.2)

        print("✅ Cycle Complete.")

        # Smart Sleep
        sleep_sec = get_sleep_seconds()
        print(f"💤 Sleeping for {sleep_sec} seconds...")
        time.sleep(sleep_sec)

if __name__ == "__main__":
    main()
