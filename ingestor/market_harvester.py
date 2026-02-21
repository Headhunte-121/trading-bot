import sys
import os
import yfinance as yf
import pandas as pd
import sqlite3
import datetime
import time
import threading

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection, log_system_event
from shared.config import SYMBOLS, CRYPTO_SYMBOLS
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

        # Translation for yfinance (BTC/USD -> BTC-USD)
        yf_symbol = symbol.replace("/", "-")

        if last_ts:
            # Parse timestamp to check if we need to fetch
            last_dt = datetime.datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
            start_date = last_dt.strftime('%Y-%m-%d')

            # Use 'start' parameter for updates
            # yfinance history with start date fetches from 00:00 of that date
            # print(f"🔄 Updating {symbol} ({timeframe}). Last: {last_ts}...")
            df = yf.Ticker(yf_symbol).history(start=start_date, interval=interval)
        else:
            # Full fetch if no history
            # print(f"📜 Initial Fetch {symbol} ({timeframe})...")
            df = yf.Ticker(yf_symbol).history(period=period, interval=interval)

        if df.empty:
            # print(f"⚠️ No data returned for {symbol} ({timeframe}). Market holiday or halt?")
            return False

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
            # print(f"✅ Saved {len(rows_to_insert)} rows for {symbol} ({timeframe}).")
            log_system_event("MarketHarvester", "INFO", f"Ingested {len(rows_to_insert)} rows for {symbol} ({timeframe})")
            return True

    except Exception as e:
        print(f"❌ Error fetching {symbol} ({timeframe}): {e}")
        log_system_event("MarketHarvester", "ERROR", f"Error fetching {symbol} ({timeframe}): {str(e)}")
        return False
    finally:
        conn.close()
    return False

def get_hot_list():
    """
    Returns a set of symbols that are either currently held or sold in the last 30 minutes.
    """
    hot_list = set()
    conn = get_db_connection()
    try:
        query_all = "SELECT symbol, side, qty, timestamp FROM executed_trades ORDER BY timestamp ASC"
        df_trades = pd.read_sql_query(query_all, conn)

        if not df_trades.empty:
            # Calculate holdings
            holdings = {}
            for _, row in df_trades.iterrows():
                sym = row['symbol']
                qty = row['qty'] if row['qty'] else 0
                if row['side'] == 'BUY':
                    holdings[sym] = holdings.get(sym, 0) + qty
                elif row['side'] == 'SELL':
                    holdings[sym] = holdings.get(sym, 0) - qty

            # Add held symbols to hot list
            for sym, qty in holdings.items():
                if qty > 0.0001: # Floating point tolerance
                    hot_list.add(sym)

            # Check for Sold in last 30 minutes
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            cutoff = now_utc - datetime.timedelta(minutes=30)

            # Ensure proper datetime parsing
            df_trades['dt'] = pd.to_datetime(df_trades['timestamp'], utc=True)

            recent_sells = df_trades[
                (df_trades['side'] == 'SELL') &
                (df_trades['dt'] > cutoff)
            ]

            for sym in recent_sells['symbol'].unique():
                hot_list.add(sym)

    except Exception as e:
        # print(f"⚠️ Error calculating Hot List: {e}")
        log_system_event("MarketHarvester", "WARNING", f"Error calculating Hot List: {str(e)}")
    finally:
        conn.close()

    return hot_list

def initial_sync():
    """
    Runs once at startup to fetch 1 year of 1d data and ensure SMA 200 data is available.
    Also fetches SPY daily data.
    """
    print("🚀 Starting Initial Sync (Daily Data for SMA 200)...")
    log_system_event("MarketHarvester", "INFO", "Starting Initial Sync (Daily Data for SMA 200)")

    # Sync SYMBOLS
    for symbol in SYMBOLS:
        # Fetch 2 years of daily data to be safe for SMA 200 calculation
        fetch_and_store(symbol, "1d", "2y", "1d")
        time.sleep(0.1)

    # Sync SPY (Daily)
    print("🇺🇸 Syncing SPY Daily Data...")
    fetch_and_store("SPY", "1d", "2y", "1d")

    # Sync CRYPTO (Daily)
    print("₿ Syncing Crypto Daily Data...")
    for symbol in CRYPTO_SYMBOLS:
        fetch_and_store(symbol, "1d", "2y", "1d")
        time.sleep(0.1)

    print("✅ Initial Sync Complete.")
    log_system_event("MarketHarvester", "INFO", "Initial Sync Complete")

def intraday_sync_equities():
    """
    Runs every 5 minutes (or adaptive loop) to fetch recent data.
    Implements Eagle Eye: 1m for Hot List, 5m for others.
    """
    print("🔄 Running Intraday Sync (Eagle Eye Mode)...")

    hot_list = get_hot_list()
    if hot_list:
        print(f"🔥 Hot List (1m Fetch): {', '.join(hot_list)}")

    count_5m = 0
    count_1m = 0

    # 1. Fetch SPY (5m) - Essential for Macro Filter
    fetch_and_store("SPY", "5m", "1d", "5m", limit=5)

    for symbol in SYMBOLS:
        # Determine timeframe
        if symbol in hot_list:
            # Fetch 1m for Hot List (High Frequency Monitoring)
            if fetch_and_store(symbol, "1m", "1d", "1m", limit=10):
                count_1m += 1

            # ALSO fetch 5m for Strategy compatibility
            if fetch_and_store(symbol, "5m", "1d", "5m", limit=5):
                count_5m += 1

        else:
            # Standard 5m fetch
            if fetch_and_store(symbol, "5m", "1d", "5m", limit=5):
                count_5m += 1

        time.sleep(0.1)

    print(f"✅ Synced: {count_5m} symbols (5m), {count_1m} symbols (1m).")
    log_system_event("MarketHarvester", "INFO", f"Synced: {count_5m} symbols (5m), {count_1m} symbols (1m)")

def run_equities_loop():
    print("🚀 Starting Equities Harvester Loop...")
    while True:
        intraday_sync_equities()
        # Smart Sleep
        sleep_sec = get_sleep_seconds()
        print(f"💤 Equities Sleeping for {sleep_sec} seconds...")
        time.sleep(sleep_sec)

def run_crypto_loop():
    print("🚀 Starting Crypto Harvester Loop (24/7)...")
    while True:
        print("₿ Running Crypto Sync...")
        count_crypto = 0
        for symbol in CRYPTO_SYMBOLS:
            # Crypto is always 5m for now as per requirements
            if fetch_and_store(symbol, "5m", "1d", "5m", limit=5):
                count_crypto += 1
            time.sleep(0.1)

        print(f"✅ Crypto Synced: {count_crypto} symbols.")
        log_system_event("MarketHarvester", "INFO", f"Crypto Synced: {count_crypto} symbols")

        time.sleep(300) # Strict 5 minute loop

def main():
    print("🚀 Starting Smart Market Harvester (Dual-Mode)...")
    
    # 1. Initial Sync (One-time)
    initial_sync()

    # 2. Start Threads
    t1 = threading.Thread(target=run_equities_loop, daemon=True)
    t2 = threading.Thread(target=run_crypto_loop, daemon=True)

    t1.start()
    t2.start()

    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping Harvester...")

if __name__ == "__main__":
    main()
