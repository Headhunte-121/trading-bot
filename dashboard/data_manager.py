import pandas as pd
import streamlit as st
import sys
import os
import datetime

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection

class DataManager:
    """
    Manages all database interactions and data processing for the Dashboard.
    Enforces separation of concerns by keeping SQL and business logic out of the UI layer.
    """

    @staticmethod
    def _fetch_query(query: str, params: tuple = ()) -> pd.DataFrame:
        """
        Internal helper to execute a query and return a DataFrame.
        Handles connection lifecycle and error logging.

        Args:
            query (str): SQL query to execute.
            params (tuple): Parameters for the query.

        Returns:
            pd.DataFrame: Resulting data or empty DataFrame on error.
        """
        try:
            conn = get_db_connection()
            if conn:
                df = pd.read_sql_query(query, conn, params=params)
                conn.close()
                return df
            return pd.DataFrame()
        except Exception as e:
            print(f"[ERROR] Query failed: {e}", file=sys.stderr)
            return pd.DataFrame()

    @staticmethod
    def get_config_value(key: str, default: str = "AUTO") -> str:
        """
        Retrieves a configuration value from the system_config table.

        Args:
            key (str): The configuration key.
            default (str): Default value if key is not found.

        Returns:
            str: The configuration value.
        """
        try:
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("SELECT value FROM system_config WHERE key = ?", (key,))
                row = cursor.fetchone()
                conn.close()
                if row:
                    return row['value']
        except Exception as e:
            print(f"[ERROR] Failed to get config '{key}': {e}", file=sys.stderr)
        return default

    @staticmethod
    def set_config_value(key: str, value: str) -> bool:
        """
        Sets a configuration value in the system_config table.

        Args:
            key (str): The configuration key.
            value (str): The value to set.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            conn = get_db_connection()
            if conn:
                conn.execute("INSERT OR REPLACE INTO system_config (key, value) VALUES (?, ?)", (key, value))
                conn.commit()
                conn.close()
                return True
        except Exception as e:
            print(f"[ERROR] Failed to set config '{key}': {e}", file=sys.stderr)
        return False

    @staticmethod
    @st.cache_data(ttl=5)
    def get_gpu_load() -> int:
        """
        Estimates GPU load based on inference count from 'ai_predictions'.

        Returns:
            int: Estimated load percentage (0-100).
        """
        try:
            query = """
                SELECT COUNT(*) as count
                FROM ai_predictions
                WHERE timestamp > datetime('now', '-1 minute')
            """
            df = DataManager._fetch_query(query)
            if not df.empty:
                count = df['count'].iloc[0]
                # Assume max 50 symbols per minute = 100% load
                load = min(count * 2, 100)
                return int(load)
            return 0
        except:
            return 0

    @staticmethod
    @st.cache_data(ttl=5)
    def get_ticker_tape() -> pd.DataFrame:
        """
        Fetches the latest market data for the ticker tape.
        joins 'market_data' with latest timestamp per symbol.

        Returns:
            pd.DataFrame: Columns [symbol, close, open, volume, pct_change].
        """
        query = """
            SELECT m.symbol, m.close, m.open, m.volume
            FROM market_data m
            INNER JOIN (
                SELECT symbol, MAX(timestamp) as max_ts
                FROM market_data
                WHERE timeframe = '5m'
                GROUP BY symbol
            ) latest ON m.symbol = latest.symbol AND m.timestamp = latest.max_ts
            WHERE m.timeframe = '5m'
            ORDER BY m.volume DESC
            LIMIT 15
        """
        df = DataManager._fetch_query(query)
        if not df.empty:
            # Vectorized calculation for efficiency
            df['pct_change'] = ((df['close'] - df['open']) / df['open']) * 100.0
            return df
        return pd.DataFrame()

    @staticmethod
    @st.cache_data(ttl=10)
    def get_ensemble_radar() -> pd.DataFrame:
        """
        Fetches latest AI predictions and technical indicators to build the Radar view.
        Calculates 'Conviction' score based on magnitude and agreement.

        Returns:
            pd.DataFrame: Enriched dataframe with conviction scores and direction.
        """
        query = """
            WITH LatestPred AS (
                SELECT p.*
                FROM ai_predictions p
                INNER JOIN (
                    SELECT symbol, MAX(timestamp) as max_ts
                    FROM ai_predictions
                    GROUP BY symbol
                ) max_p ON p.symbol = max_p.symbol AND p.timestamp = max_p.max_ts
            ),
            LatestTech AS (
                SELECT t.symbol, t.rsi_14
                FROM technical_indicators t
                INNER JOIN (
                    SELECT symbol, MAX(timestamp) as max_ts
                    FROM technical_indicators
                    GROUP BY symbol
                ) max_t ON t.symbol = max_t.symbol AND t.timestamp = max_t.max_ts
            )
            SELECT
                p.symbol,
                p.current_price,
                p.ensemble_predicted_price,
                p.small_predicted_price,
                p.large_predicted_price,
                t.rsi_14
            FROM LatestPred p
            LEFT JOIN LatestTech t ON p.symbol = t.symbol
        """
        df = DataManager._fetch_query(query)
        if not df.empty:
            # Logic: Conviction Score Calculation
            # Magnitude: |(pred - curr) / curr|
            df['magnitude'] = (df['ensemble_predicted_price'] - df['current_price']) / df['current_price']

            # Agreement: |(large - small) / curr| (Lower difference is better)
            df['agreement_diff'] = abs(df['large_predicted_price'] - df['small_predicted_price']) / df['current_price']

            # Normalize Magnitude (0.0 to 0.05 map to 0-100 approx)
            mag_score = df['magnitude'].abs() * 2000

            # Normalize Agreement (0.0 to 0.02 map to 100-0 approx)
            agree_score = (1 - (df['agreement_diff'] * 50)) * 100

            # Combined Score
            df['conviction'] = (mag_score + agree_score) / 2
            df['conviction'] = df['conviction'].clip(0, 100)

            # Direction
            df['direction'] = df.apply(
                lambda x: 'UP' if x['ensemble_predicted_price'] > x['current_price'] else 'DOWN',
                axis=1
            )

            # Agreement Icon
            def get_icon(row):
                s_dir = 1 if row['small_predicted_price'] > row['current_price'] else -1
                l_dir = 1 if row['large_predicted_price'] > row['current_price'] else -1
                return '🤝' if s_dir == l_dir else '⚠️'

            df['agreement'] = df.apply(get_icon, axis=1)

            return df.sort_values(by='conviction', ascending=False)

        return pd.DataFrame()

    @staticmethod
    @st.cache_data(ttl=10)
    def get_technical_heatmap() -> pd.DataFrame:
        """
        Fetches latest technical indicators for the heatmap.
        Filters for the default timeframe (assumed 5m based on context).

        Returns:
            pd.DataFrame: Columns [symbol, rsi_14, sma_50, sma_200, timestamp].
        """
        query = """
            SELECT t.symbol, t.rsi_14, t.sma_50, t.sma_200, t.timestamp
            FROM technical_indicators t
            INNER JOIN (
                SELECT symbol, MAX(timestamp) as max_ts
                FROM technical_indicators
                WHERE timeframe = '5m'
                GROUP BY symbol
            ) latest ON t.symbol = latest.symbol AND t.timestamp = latest.max_ts
            WHERE t.timeframe = '5m'
            ORDER BY t.rsi_14 ASC
        """
        return DataManager._fetch_query(query)

    @staticmethod
    @st.cache_data(ttl=10)
    def get_chart_data(symbol: str) -> pd.DataFrame:
        """
        Fetches historical market data and technical indicators for a specific symbol.
        Used for plotting the main chart.

        Args:
            symbol (str): The ticker symbol.

        Returns:
            pd.DataFrame: Time-series data sorted by timestamp ascending.
        """
        query = """
            SELECT m.timestamp, m.open, m.high, m.low, m.close, t.sma_200, t.sma_50, t.rsi_14
            FROM market_data m
            LEFT JOIN technical_indicators t ON m.symbol = t.symbol AND m.timestamp = t.timestamp
            WHERE m.symbol = ? AND m.timeframe = '5m'
            ORDER BY m.timestamp DESC
            LIMIT 200
        """
        df = DataManager._fetch_query(query, params=(symbol,))
        if not df.empty:
            return df.sort_values(by='timestamp', ascending=True)
        return pd.DataFrame()

    @staticmethod
    def get_system_logs() -> pd.DataFrame:
        """
        Fetches the latest system logs.
        Not cached to ensure real-time debugging.

        Returns:
            pd.DataFrame: Columns [timestamp, service_name, log_level, message].
        """
        query = """
            SELECT timestamp, service_name, log_level, message
            FROM system_logs
            ORDER BY timestamp DESC
            LIMIT 50
        """
        return DataManager._fetch_query(query)

    @staticmethod
    def get_ledger() -> pd.DataFrame:
        """
        Fetches executed trades.

        Returns:
            pd.DataFrame: Recent trades.
        """
        query = "SELECT * FROM executed_trades ORDER BY timestamp DESC LIMIT 20"
        return DataManager._fetch_query(query)

    @staticmethod
    def get_active_signals() -> pd.DataFrame:
        """
        Fetches active trade signals (PENDING or SIZED).

        Returns:
            pd.DataFrame: Active signals.
        """
        query = "SELECT * FROM trade_signals WHERE status IN ('PENDING', 'SIZED') ORDER BY timestamp DESC"
        return DataManager._fetch_query(query)

    @staticmethod
    @st.cache_data(ttl=60)
    def get_available_symbols() -> list:
        """
        Fetches a list of all distinct symbols in the market data.

        Returns:
            list: List of symbol strings.
        """
        df = DataManager._fetch_query("SELECT DISTINCT symbol FROM market_data ORDER BY symbol")
        if not df.empty:
            return df['symbol'].tolist()
        return []
