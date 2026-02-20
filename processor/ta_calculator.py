import pandas as pd
import pandas_ta as ta
import sqlite3
import os
import sys
import time

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection
from shared.config import SYMBOLS
from shared.smart_sleep import get_sleep_seconds

def calculate_technical_indicators():
    """
    Calculates technical indicators (Daily SMA 200, 5m RSI 14, 5m SMA 50)
    and writes them to the technical_indicators table.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        print("📊 Starting Technical Analysis Processor...")
        count = 0

        for symbol in SYMBOLS:
            # 1. Fetch Daily Data & Calculate SMA 200
            # We need enough history for SMA 200 (at least 200 days)
            query_daily = """
                SELECT timestamp, close
                FROM market_data
                WHERE symbol = ? AND timeframe = '1d'
                ORDER BY timestamp ASC
            """
            df_daily = pd.read_sql_query(query_daily, conn, params=(symbol,))

            daily_sma_map = {}
            if not df_daily.empty and len(df_daily) >= 200:
                # Calculate SMA 200
                df_daily['sma_200'] = df_daily.ta.sma(length=200, close='close')

                # Create a map: date_str -> sma_200
                # timestamp in DB is ISO 8601 (YYYY-MM-DDTHH:MM:SSZ)
                # We extract YYYY-MM-DD
                for _, row in df_daily.iterrows():
                    if pd.notna(row['sma_200']):
                        ts_str = row['timestamp']
                        date_str = ts_str[:10]
                        daily_sma_map[date_str] = row['sma_200']
            else:
                print(f"⚠️ Not enough daily data for {symbol} (SMA 200). Skipping.")
                continue

            # 2. Fetch 5m Data (Intraday)
            # We need enough history for RSI 14 and SMA 50 (at least 50 periods)
            # Let's fetch last 500 rows to be safe and efficient
            query_intraday = """
                SELECT timestamp, close
                FROM market_data
                WHERE symbol = ? AND timeframe = '5m'
                ORDER BY timestamp ASC
            """
            # Fetching all might be slow if history grows.
            # Ideally we only process new data.
            # But indicators need context (previous values or window).
            # We can fetch last X rows.
            # But to ensure we update ALL recent rows, let's just fetch last 1000.
            # If we want to backfill, we might need more.
            # For now, let's fetch last 2000 rows.

            # Actually, we should probably check what is already in technical_indicators
            # and only process new + lookback.
            # Query MAX(timestamp) from technical_indicators for this symbol.
            cursor.execute("SELECT MAX(timestamp) FROM technical_indicators WHERE symbol = ?", (symbol,))
            last_processed_ts = cursor.fetchone()[0]

            if last_processed_ts:
                # Fetch lookback (e.g. 200 rows) + new data
                query_intraday = """
                    SELECT timestamp, close
                    FROM market_data
                    WHERE symbol = ? AND timeframe = '5m'
                    AND timestamp >= (
                        SELECT timestamp FROM market_data
                        WHERE symbol = ? AND timeframe = '5m'
                        AND timestamp <= ?
                        ORDER BY timestamp DESC LIMIT 1 OFFSET 200
                    )
                    ORDER BY timestamp ASC
                """
                # This query is complex. Simpler: fetch last 200 before last_processed + all after.
                # Actually, simpler approach: Just fetch last 1000 rows.
                # If gap is huge, we might miss some, but for a running bot, this is fine.
                query_intraday = """
                    SELECT * FROM (
                        SELECT timestamp, close
                        FROM market_data
                        WHERE symbol = ? AND timeframe = '5m'
                        ORDER BY timestamp DESC
                        LIMIT 1000
                    ) ORDER BY timestamp ASC
                """
                df_intraday = pd.read_sql_query(query_intraday, conn, params=(symbol,))
            else:
                # Full fetch (limit 5000 to be safe)
                query_intraday = """
                    SELECT * FROM (
                        SELECT timestamp, close
                        FROM market_data
                        WHERE symbol = ? AND timeframe = '5m'
                        ORDER BY timestamp DESC
                        LIMIT 5000
                    ) ORDER BY timestamp ASC
                """
                df_intraday = pd.read_sql_query(query_intraday, conn, params=(symbol,))

            if df_intraday.empty or len(df_intraday) < 50:
                continue

            # 3. Calculate Intraday Indicators
            try:
                df_intraday['rsi_14'] = df_intraday.ta.rsi(length=14, close='close')
                df_intraday['sma_50'] = df_intraday.ta.sma(length=50, close='close')
                # Lower BB? Schema has it. Let's calc it.
                bb = df_intraday.ta.bbands(length=20, std=2, close='close')
                if bb is not None:
                    # columns: BBL_20_2.0, BBM_20_2.0, BBU_20_2.0
                    bbl_col = [c for c in bb.columns if c.startswith('BBL')][0]
                    df_intraday['lower_bb'] = bb[bbl_col]
                else:
                    df_intraday['lower_bb'] = None
            except Exception as e:
                print(f"Error calculating indicators for {symbol}: {e}")
                continue

            # 4. Merge Daily SMA 200
            # Logic: For each row, get date_str, lookup in daily_sma_map.
            # Forward fill: If today's SMA 200 isn't ready (market open), use yesterday's.
            # But map has dates.
            # We can just use the map. get(date_str) -> if None, get(prev_date_str)?
            # Easier: Sort daily map keys, use bisect or just get closest previous date.
            # Or rely on the fact that we have daily data up to yesterday or today.

            # Efficient way:
            # We iterate and lookup.

            rows_to_insert = []

            # Get sorted daily dates
            sorted_dates = sorted(daily_sma_map.keys())

            for idx, row in df_intraday.iterrows():
                ts_str = row['timestamp']
                date_str = ts_str[:10]

                # Check map
                sma_200 = daily_sma_map.get(date_str)

                if sma_200 is None:
                    # Fallback to last available daily SMA if current date missing
                    # (e.g. today's daily candle not closed/formed yet, or simple mismatch)
                    # We find the latest date in sorted_dates <= date_str
                    # Simple linear scan backwards or bisect.
                    # Since we are processing typically "now", we can just look at the last element of sorted_dates
                    if sorted_dates:
                        last_avail_date = sorted_dates[-1]
                        if last_avail_date < date_str:
                             sma_200 = daily_sma_map[last_avail_date]

                if pd.isna(row['rsi_14']) or pd.isna(row['sma_50']):
                    continue

                # We allow sma_200 to be None if really no history, but preferably not.

                rows_to_insert.append((
                    symbol,
                    ts_str,
                    float(row['rsi_14']),
                    float(row['sma_50']),
                    float(sma_200) if sma_200 else None,
                    float(row['lower_bb']) if pd.notna(row['lower_bb']) else None
                ))

            if rows_to_insert:
                # Batch insert
                cursor.executemany("""
                    INSERT OR REPLACE INTO technical_indicators
                    (symbol, timestamp, rsi_14, sma_50, sma_200, lower_bb)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, rows_to_insert)
                conn.commit()
                # print(f"✅ Updated technicals for {symbol} ({len(rows_to_insert)} rows).")
                count += 1

        print(f"✅ {count} symbols calculated.")

    except Exception as e:
        print(f"❌ TA Processor Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    while True:
        calculate_technical_indicators()
        sleep_sec = get_sleep_seconds()
        print(f"💤 TA Sleeping for {sleep_sec} seconds...")
        time.sleep(sleep_sec)
