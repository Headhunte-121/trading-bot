import pandas as pd
import pandas_ta as ta
import sqlite3
import os
import sys
import time

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection, log_system_event
from shared.config import SYMBOLS
from shared.smart_sleep import get_sleep_seconds

def calculate_technical_indicators():
    """
    Calculates technical indicators (Daily SMA 200, 5m RSI 14, 5m SMA 50, VWAP, ATR, Vol SMA)
    and writes them to the technical_indicators table.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # print("📊 Starting Technical Analysis Processor...")
        count = 0

        # Process SYMBOLS and SPY (for Macro Filter)
        # Use set to avoid duplicates if SPY is already in SYMBOLS (unlikely per config, but safe)
        all_symbols = list(set(SYMBOLS + ['SPY']))

        for symbol in all_symbols:
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
                for _, row in df_daily.iterrows():
                    if pd.notna(row['sma_200']):
                        ts_str = row['timestamp']
                        date_str = ts_str[:10]
                        daily_sma_map[date_str] = row['sma_200']

            # Note: If no daily data, we continue but sma_200 will be None.
            # SPY might only have 5m data if Harvester just started and only did intraday sync for SPY?
            # Harvester's initial_sync fetches 1d for SPY too.

            # 2. Fetch 5m Data (Intraday)
            # Fetch high, low, close, volume for VWAP/ATR
            # Fetch enough for VWAP (needs start of day), ATR (14), SMA (50)
            # VWAP resets daily. We need at least from start of current day.
            # Safest is to fetch last 3000 rows (approx 10 trading days of 5m candles).
            query_intraday = """
                SELECT * FROM (
                    SELECT timestamp, open, high, low, close, volume
                    FROM market_data
                    WHERE symbol = ? AND timeframe = '5m'
                    ORDER BY timestamp DESC
                    LIMIT 3000
                ) ORDER BY timestamp ASC
            """
            df_intraday = pd.read_sql_query(query_intraday, conn, params=(symbol,))

            if df_intraday.empty or len(df_intraday) < 50:
                continue

            # Set datetime index for pandas_ta (crucial for VWAP anchor)
            df_intraday['datetime'] = pd.to_datetime(df_intraday['timestamp'])
            df_intraday.set_index('datetime', inplace=True)

            # 3. Calculate Intraday Indicators
            try:
                # RSI 14
                df_intraday['rsi_14'] = df_intraday.ta.rsi(length=14, close='close')

                # SMA 50
                df_intraday['sma_50'] = df_intraday.ta.sma(length=50, close='close')

                # ATR 14
                df_intraday['atr_14'] = df_intraday.ta.atr(length=14)

                # Volume SMA 20
                df_intraday['volume_sma_20'] = df_intraday.ta.sma(close='volume', length=20)

                # VWAP (Anchor 'D' for Daily reset)
                # pandas_ta should handle the reset if index is datetime
                vwap = df_intraday.ta.vwap(anchor='D')
                if vwap is not None:
                    if isinstance(vwap, pd.DataFrame):
                         # If it returns a DF, take the first column (usually VWAP_D)
                         df_intraday['vwap'] = vwap.iloc[:, 0]
                    else:
                         df_intraday['vwap'] = vwap
                else:
                    df_intraday['vwap'] = None

                # Bollinger Bands (Lower)
                bb = df_intraday.ta.bbands(length=20, std=2, close='close')
                if bb is not None:
                    # columns: BBL_20_2.0, BBM_20_2.0, BBU_20_2.0
                    bbl_col = [c for c in bb.columns if c.startswith('BBL')][0]
                    df_intraday['lower_bb'] = bb[bbl_col]
                else:
                    df_intraday['lower_bb'] = None

            except Exception as e:
                # print(f"Error calculating indicators for {symbol}: {e}")
                log_system_event("TA_Calculator", "ERROR", f"Error calculating indicators for {symbol}: {str(e)}")
                continue

            # 4. Merge & Prepare Insert
            rows_to_insert = []
            sorted_dates = sorted(daily_sma_map.keys())

            for idx, row in df_intraday.iterrows():
                # idx is datetime index now
                ts_str = row['timestamp'] # Original string
                date_str = ts_str[:10]

                # Lookup Daily SMA 200
                sma_200 = daily_sma_map.get(date_str)
                if sma_200 is None and sorted_dates:
                    # Fallback to previous available
                    # Quick approximate: compare with last available
                    if sorted_dates[-1] < date_str:
                        sma_200 = daily_sma_map[sorted_dates[-1]]

                # Ensure essential values exist
                if pd.isna(row['rsi_14']) or pd.isna(row['sma_50']):
                    continue

                # Prepare row
                # Schema: symbol, timestamp, timeframe, rsi_14, sma_50, sma_200, lower_bb, vwap, atr_14, volume_sma_20

                rows_to_insert.append((
                    symbol,
                    ts_str,
                    '5m', # Fixed timeframe
                    float(row['rsi_14']),
                    float(row['sma_50']),
                    float(sma_200) if sma_200 else None,
                    float(row['lower_bb']) if pd.notna(row['lower_bb']) else None,
                    float(row['vwap']) if pd.notna(row['vwap']) else None,
                    float(row['atr_14']) if pd.notna(row['atr_14']) else None,
                    float(row['volume_sma_20']) if pd.notna(row['volume_sma_20']) else None
                ))

            if rows_to_insert:
                # Batch insert
                cursor.executemany("""
                    INSERT OR REPLACE INTO technical_indicators
                    (symbol, timestamp, timeframe, rsi_14, sma_50, sma_200, lower_bb, vwap, atr_14, volume_sma_20)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, rows_to_insert)
                conn.commit()
                count += 1

        print(f"✅ {count} symbols calculated.")
        log_system_event("TA_Calculator", "INFO", f"Calculated indicators for {count} symbols.")

    except Exception as e:
        print(f"❌ TA Processor Error: {e}")
        log_system_event("TA_Calculator", "ERROR", f"Critical Error: {str(e)}")
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
