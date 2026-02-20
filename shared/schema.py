import sqlite3
import os
import sys

# Ensure shared package is available if run directly
if __name__ == "__main__":
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.db_utils import get_db_connection, DB_PATH

def add_column_if_not_exists(cursor, table, column, definition):
    try:
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        print(f"Added column '{column}' to table '{table}'.")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            print(f"Column '{column}' already exists in table '{table}'.")
        else:
            print(f"Error adding column '{column}' to table '{table}': {e}")

def setup_database():
    """Initializes the database and creates tables if they do not exist."""
    print(f"Setting up database at {DB_PATH}...")

    conn = get_db_connection()
    cursor = conn.cursor()

    # --- DROP LEGACY TABLES ---
    tables_to_drop = ["raw_news", "market_data_daily", "chart_analysis_requests"]
    for table in tables_to_drop:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        print(f"Dropped legacy table: {table}")

    # --- MARKET DATA ---
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS market_data (
            symbol TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            PRIMARY KEY (symbol, timestamp, timeframe)
        )
    """)

    # --- TECHNICAL INDICATORS (Recreated with timeframe & new indicators) ---
    cursor.execute("DROP TABLE IF EXISTS technical_indicators")
    cursor.execute("""
        CREATE TABLE technical_indicators (
            symbol TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            timeframe TEXT NOT NULL DEFAULT '5m',
            rsi_14 REAL,
            sma_50 REAL,
            sma_200 REAL,
            lower_bb REAL,
            vwap REAL,
            atr_14 REAL,
            volume_sma_20 REAL,
            PRIMARY KEY (symbol, timestamp, timeframe)
        )
    """)
    print("Recreated technical_indicators table with new columns.")

    # --- AI PREDICTIONS ---
    # We keep the existing table if it exists, assuming schema is stable
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ai_predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            current_price REAL,
            small_predicted_price REAL,
            large_predicted_price REAL,
            ensemble_predicted_price REAL,
            ensemble_pct_change REAL,
            UNIQUE(symbol, timestamp)
        )
    """)

    # --- TRADE SIGNALS ---
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trade_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            signal_type TEXT,
            size REAL,
            stop_loss REAL,
            status TEXT,
            order_id TEXT
        )
    """)
    # Add new column 'atr' to trade_signals
    add_column_if_not_exists(cursor, "trade_signals", "atr", "REAL")

    # --- EXECUTED TRADES ---
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS executed_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            price REAL,
            qty REAL,
            side TEXT
        )
    """)
    # Add new column 'signal_type' to executed_trades
    add_column_if_not_exists(cursor, "executed_trades", "signal_type", "TEXT")

    # --- SYSTEM LOGS ---
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS system_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            service_name TEXT NOT NULL,
            log_level TEXT NOT NULL,
            message TEXT NOT NULL
        )
    """)

    conn.commit()
    conn.close()
    print("Database setup complete.")

if __name__ == "__main__":
    setup_database()
