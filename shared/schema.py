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
    # Modified to support dual-timeframes
    # Primary Key: (symbol, timestamp, timeframe)
    # If the table exists but has the old schema, we drop it to re-initialize correctly.
    # Check if 'timeframe' column exists
    cursor.execute("PRAGMA table_info(market_data)")
    columns = [info[1] for info in cursor.fetchall()]
    if 'timeframe' not in columns and 'market_data' in tables_to_drop:
        # Logic above handles explicit drops, but for market_data we might need to be more aggressive if we want to enforce the new PK
        pass

    # Actually, let's just DROP market_data to ensure clean state for the new PK structure
    # The harvester will refill it.
    cursor.execute("DROP TABLE IF EXISTS market_data")

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
    # Added sma_200 and sma_50
    # Primary Key: (symbol, timestamp)
    # We will drop and recreate to ensure schema compliance
    cursor.execute("DROP TABLE IF EXISTS technical_indicators")

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

    # --- AI PREDICTIONS ---
    # New Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ai_predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            current_price REAL,
            predicted_price REAL,
            predicted_pct_change REAL,
            UNIQUE(symbol, timestamp)
        )
    """)

    # --- TRADE SIGNALS ---
    # Kept largely the same, ensuring order_id exists
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
