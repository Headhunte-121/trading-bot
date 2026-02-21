import pandas as pd
import pandas_ta as ta
import sqlite3
import os
import sys
import time
from datetime import datetime, timezone
import numpy as np

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection, log_system_event
from shared.config import SYMBOLS
from shared.smart_sleep import get_sleep_seconds, smart_sleep

class TACalculator:
    def __init__(self):
        # Cache for Daily SMA 200: {symbol: {'date': 'YYYY-MM-DD', 'value': float}}
        self._daily_cache = {}
        self.conn = None

    def get_connection(self):
        if self.conn is None:
            self.conn = get_db_connection()
        return self.conn

    def close_connection(self):
        if self.conn:
            try:
                self.conn.close()
            except:
                pass
            self.conn = None

    def fetch_daily_sma_200(self, symbol):
        """
        Fetches or calculates the latest Daily SMA 200 for a symbol.
        Uses an in-memory cache to avoid re-querying daily data within the same trading day.
        """
        today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        # Check cache
        cached = self._daily_cache.get(symbol)
        if cached and cached['date'] == today_str:
            return cached['value']

        try:
            # Query last 300 daily records (buffer for SMA 200 calculation)
            query = """
                SELECT close
                FROM market_data
                WHERE symbol = ? AND timeframe = '1d'
                ORDER BY timestamp DESC
                LIMIT 300
            """
            df = pd.read_sql_query(query, self.get_connection(), params=(symbol,))

            if len(df) < 200:
                return None

            # Data is in DESC order, reverse it for calculation
            df = df.iloc[::-1].reset_index(drop=True)

            # Calculate SMA 200
            sma_series = df.ta.sma(length=200, close='close')

            if sma_series is not None and not sma_series.empty:
                last_sma = sma_series.iloc[-1]
                # Update cache
                self._daily_cache[symbol] = {'date': today_str, 'value': last_sma}
                return last_sma

            return None

        except Exception as e:
            log_system_event("TA_Calculator", "ERROR", f"Error calculating Daily SMA 200 for {symbol}: {str(e)}")
            return None

    def process_symbol(self, symbol):
        """
        Calculates intraday indicators for a symbol and returns a DataFrame of results.
        """
        try:
            # 1. Get Daily SMA 200 (Cached)
            sma_200 = self.fetch_daily_sma_200(symbol)

            # 2. Fetch Intraday Data (5m)
            # Fetch last 3000 rows (approx 10 days) to ensure enough context for indicators
            query = """
                SELECT timestamp, open, high, low, close, volume
                FROM market_data
                WHERE symbol = ? AND timeframe = '5m'
                ORDER BY timestamp DESC
                LIMIT 3000
            """
            df = pd.read_sql_query(query, self.get_connection(), params=(symbol,))

            if df.empty or len(df) < 50:
                return None

            # Reverse to ASC for calculation
            df = df.iloc[::-1].reset_index(drop=True)

            # Set datetime index
            df['datetime'] = pd.to_datetime(df['timestamp'])
            df.set_index('datetime', inplace=True)

            # 3. Vectorized Indicator Calculation (on full history)
            # SMA 50
            df['sma_50'] = df.ta.sma(length=50, close='close')

            # RSI 14
            df['rsi_14'] = df.ta.rsi(length=14, close='close')

            # ATR 14
            df['atr_14'] = df.ta.atr(length=14, high='high', low='low', close='close')

            # Volume SMA 20
            df['volume_sma_20'] = df.ta.sma(length=20, close='volume')

            # VWAP
            vwap = df.ta.vwap(anchor='D')
            if isinstance(vwap, pd.DataFrame):
                 df['vwap'] = vwap.iloc[:, 0]
            else:
                 df['vwap'] = vwap

            # Bollinger Bands (20, 2)
            bb = df.ta.bbands(length=20, std=2, close='close')
            if bb is not None:
                bbl_col = [c for c in bb.columns if c.startswith('BBL')][0]
                df['lower_bb'] = bb[bbl_col]
            else:
                df['lower_bb'] = None

            # 4. Filter for Current Day (Latest available date in the dataset)
            # This prevents overwriting history and ensures we only update the active session.
            latest_date = df.index.date.max()

            # Filter rows for this date
            df_current = df[df.index.date == latest_date].copy()

            if df_current.empty:
                return None

            # Broadcast SMA 200 (Forward Fill) ONLY to current day rows
            df_current['sma_200'] = sma_200 if sma_200 is not None else None

            # Filter for Output
            df_final = df_current.dropna(subset=['rsi_14', 'sma_50'])

            if df_final.empty:
                return None

            # Add metadata columns
            df_final['symbol'] = symbol
            df_final['timeframe'] = '5m'

            # Restore timestamp column (string format)
            df_final['timestamp'] = df_final.index.strftime('%Y-%m-%d %H:%M:%S')

            # Select and reorder columns
            cols_to_keep = [
                'symbol', 'timestamp', 'timeframe',
                'rsi_14', 'sma_50', 'sma_200', 'lower_bb', 'vwap', 'atr_14', 'volume_sma_20'
            ]

            # Return DF with None for NaNs (for SQL)
            return df_final[cols_to_keep].where(pd.notnull(df_final), None)

        except Exception as e:
            log_system_event("TA_Calculator", "ERROR", f"Error calculating indicators for {symbol}: {str(e)}")
            return None

    def run(self):
        # Ensure connection
        self.get_connection()

        try:
            count = 0

            # Get symbols
            all_symbols = list(set(SYMBOLS + ['SPY']))

            # Iterate
            for symbol in all_symbols:
                df_result = self.process_symbol(symbol)

                if df_result is not None and not df_result.empty:
                    # Bulk Insert
                    # Explicit conversion to native types to avoid SQLite InterfaceError with numpy types
                    # df.values.tolist() converts numpy types to python types (e.g. np.float64 -> float)
                    data_tuples = df_result.values.tolist()

                    self.conn.executemany("""
                        INSERT OR REPLACE INTO technical_indicators
                        (symbol, timestamp, timeframe, rsi_14, sma_50, sma_200, lower_bb, vwap, atr_14, volume_sma_20)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, data_tuples)
                    self.conn.commit()
                    count += 1

            print(f"✅ {count} symbols calculated.")
            log_system_event("TA_Calculator", "INFO", f"Calculated indicators for {count} symbols.")

        except Exception as e:
            print(f"❌ TA Processor Error: {e}")
            log_system_event("TA_Calculator", "ERROR", f"Critical Error: {str(e)}")
        finally:
            self.close_connection()

if __name__ == "__main__":
    calc = TACalculator()
    while True:
        calc.run()
        sleep_sec = get_sleep_seconds()
        print(f"💤 TA Sleeping for {sleep_sec} seconds...")
        smart_sleep(sleep_sec)
