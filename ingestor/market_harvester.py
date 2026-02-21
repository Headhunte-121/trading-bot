"""
Service: Market Harvester (Data Ingestor)
Role: Fetches market data (1m/5m/1d) from Yahoo Finance and syncs it to the local database.
Dependencies: yfinance, pandas, shared.db_utils
"""
import sys
import os
import yfinance as yf
import pandas as pd
import datetime
import time

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection, log_system_event
from shared.config import SYMBOLS
from shared.smart_sleep import get_sleep_seconds, smart_sleep


def get_last_timestamp(cursor, symbol, timeframe):
    """
    Returns the latest timestamp for a symbol and timeframe from the database.

    Args:
        cursor: Database cursor.
        symbol (str): Ticker symbol.
        timeframe (str): Timeframe (e.g., '5m', '1d').

    Returns:
        str: ISO timestamp or None.
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
    Fetches market data for a symbol and timeframe from Yahoo Finance and stores it.
    Handles rate limits with a retry mechanism.

    Args:
        symbol (str): Ticker symbol.
        timeframe (str): Timeframe identifier for DB (e.g., '5m').
        period (str): yfinance period (e.g., '2y', '5d').
        interval (str): yfinance interval (e.g., '1d', '5m').
        limit (int, optional): Max rows to keep before insertion.

    Returns:
        bool: True if successful, False otherwise.
    """
    conn = get_db_connection()
    if not conn:
        return False
    cursor = conn.cursor()

    try:
        last_ts = get_last_timestamp(cursor, symbol, timeframe)
        df = pd.DataFrame()

        # Retry logic for yfinance
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if last_ts:
                    # Incremental Sync
                    # last_ts is in ISO format: YYYY-MM-DDTHH:MM:SSZ
                    last_dt = datetime.datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
                    start_date = last_dt.strftime('%Y-%m-%d')
                    df = yf.Ticker(symbol).history(start=start_date, interval=interval)
                else:
                    # Full Fetch
                    df = yf.Ticker(symbol).history(period=period, interval=interval)

                if not df.empty:
                    break  # Success

            except Exception:
                if attempt < max_retries - 1:
                    time.sleep(1)  # Wait before retry
                else:
                    return False

        if df.empty:
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

            except Exception:
                continue

        if rows_to_insert:
            # INSERT OR IGNORE to handle overlaps
            cursor.executemany('''
                INSERT OR IGNORE INTO market_data
                (symbol, timestamp, timeframe, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', rows_to_insert)
            conn.commit()
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
    Generates a 'Hot List' of symbols to monitor at higher resolution (1m).

    Logic:
    - Includes currently held assets (Quantity > 0).
    - Includes assets sold within the last 30 minutes.

    Returns:
        set: Set of symbol strings.
    """
    hot_list = set()
    conn = get_db_connection()
    if not conn:
        return hot_list

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
                if qty > 0.0001:  # Floating point tolerance
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
        log_system_event("MarketHarvester", "WARNING", f"Error calculating Hot List: {str(e)}")
    finally:
        conn.close()

    return hot_list


def initial_sync():
    """
    Runs once at startup to fetch 2 years of Daily (1d) data.
    This ensures that the SMA 200 (Daily) can be calculated immediately.
    """
    print("🚀 Starting Initial Sync (Daily Data for SMA 200)...")
    log_system_event("MarketHarvester", "INFO", "Starting Initial Sync (Daily Data for SMA 200)")

    # Sync SPY (Daily) - Macro Benchmark
    print("🇺🇸 Syncing SPY Daily Data...")
    fetch_and_store("SPY", "1d", "2y", "1d")
    time.sleep(0.5)

    # Sync SYMBOLS
    count = 0
    for symbol in SYMBOLS:
        # Fetch 2 years of daily data to be safe for SMA 200 calculation
        if fetch_and_store(symbol, "1d", "2y", "1d"):
            count += 1
        time.sleep(0.5)  # Rate limiting

    print(f"✅ Initial Sync Complete. {count} symbols synced.")
    log_system_event("MarketHarvester", "INFO", f"Initial Sync Complete. {count} symbols synced.")


def intraday_sync():
    """
    Executes the main data fetching loop.
    Implements 'Eagle Eye' resolution:
    - Hot List symbols -> Fetched at 1m AND 5m resolution.
    - Standard symbols -> Fetched at 5m resolution only.
    """
    print("🔄 Running Intraday Sync (Eagle Eye Mode)...")

    hot_list = get_hot_list()
    if hot_list:
        print(f"🔥 Hot List (1m Fetch): {', '.join(hot_list)}")

    count_5m = 0
    count_1m = 0

    # 1. Fetch SPY (5m) - Essential for Macro Filter
    fetch_and_store("SPY", "5m", "5d", "5m", limit=None)
    time.sleep(0.5)

    for symbol in SYMBOLS:
        # Determine timeframe strategy
        if symbol in hot_list:
            # Fetch 1m for Hot List (High Frequency Monitoring)
            if fetch_and_store(symbol, "1m", "5d", "1m", limit=None):
                count_1m += 1
            time.sleep(0.5)

            # ALSO fetch 5m for Strategy compatibility
            if fetch_and_store(symbol, "5m", "5d", "5m", limit=None):
                count_5m += 1
            time.sleep(0.5)

        else:
            # Standard 5m fetch
            if fetch_and_store(symbol, "5m", "5d", "5m", limit=None):
                count_5m += 1
            time.sleep(0.5)

    print(f"✅ Synced: {count_5m} symbols (5m), {count_1m} symbols (1m).")
    log_system_event("MarketHarvester", "INFO", f"Synced: {count_5m} symbols (5m), {count_1m} symbols (1m)")


def main():
    print("🚀 Starting Smart Market Harvester (State-Aware)...")
    
    # 1. Initial Sync (One-time)
    initial_sync()

    # 2. Intraday Loop
    while True:
        # Check sleep status BEFORE starting a sync cycle
        sleep_sec = get_sleep_seconds()

        if sleep_sec >= 3600:
             print(f"💤 Market Closed/Sleeping. Waiting {sleep_sec}s...")
             smart_sleep(sleep_sec)
        else:
             # Market Open or Force Awake
             intraday_sync()

             # Post-sync sleep
             post_sync_sleep = get_sleep_seconds()
             print(f"💤 Cycle Complete. Sleeping {post_sync_sleep}s...")
             smart_sleep(post_sync_sleep)


if __name__ == "__main__":
    main()
