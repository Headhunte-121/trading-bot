import sqlite3
import os
import sys

# Ensure shared package is available if run directly
if __name__ == "__main__":
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.db_utils import get_db_connection, DB_PATH

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
    # Check for new schema or recreate
    # We will assume a fresh start or simple existence check
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

    # --- TECHNICAL INDICATORS ---
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS technical_indicators (
            symbol TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            rsi_14 REAL,
            sma_50 REAL,
            sma_200 REAL,
            lower_bb REAL,
            PRIMARY KEY (symbol, timestamp)
        )
    """)

    # --- AI PREDICTIONS (Updated for Dual-Model Ensemble) ---
    # We drop the old table to ensure schema update
    cursor.execute("DROP TABLE IF EXISTS ai_predictions")

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
    print("Created ai_predictions table with Ensemble support.")

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

    conn.commit()
    conn.close()
    print("Database setup complete.")

if __name__ == "__main__":
    setup_database()
