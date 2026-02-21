"""
Service: Database Schema Management
Role: Manages database initialization, table creation, and schema migrations.
Dependencies: sqlite3, shared.db_utils
"""
import sqlite3
import os
import sys
import re

# Ensure shared package is available if run directly
if __name__ == "__main__":
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.db_utils import get_db_connection, DB_PATH


def _is_safe_identifier(identifier):
    """
    Validates that an identifier contains only alphanumeric characters and underscores.
    """
    return bool(re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", identifier))


def _is_safe_definition(definition):
    """
    Validates that a column definition does not contain dangerous SQL characters.
    """
    # Simple blacklist for basic SQL injection prevention in definitions
    unsafe_chars = [";", "--", "/*"]
    return not any(char in definition for char in unsafe_chars)


def add_column_if_not_exists(cursor, table, column, definition):
    """
    Adds a column to a table if it does not already exist.

    Args:
        cursor: SQLite cursor object.
        table (str): Table name.
        column (str): Column name.
        definition (str): Column definition (e.g., "REAL", "TEXT").
    """
    if not _is_safe_identifier(table) or not _is_safe_identifier(column):
        print(f"Error: Invalid identifier '{table}' or '{column}'.")
        return

    if not _is_safe_definition(definition):
        print(f"Error: Invalid definition '{definition}'.")
        return

    try:
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        print(f"Added column '{column}' to table '{table}'.")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            pass  # Column already exists
        else:
            print(f"Error adding column '{column}' to table '{table}': {e}")


def setup_database():
    """Initializes the database and creates tables if they do not exist."""
    print(f"Setting up database at {DB_PATH}...")

    conn = get_db_connection()
    if not conn:
        return

    cursor = conn.cursor()

    # --- DROP DEPRECATED TABLES ---
    deprecated_tables = ["raw_news", "market_data_daily", "chart_analysis_requests"]
    for table in deprecated_tables:
        if _is_safe_identifier(table):
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
            print(f"Dropped deprecated table: {table}")

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

    # --- TECHNICAL INDICATORS ---
    # Recreate to ensure schema consistency
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
    print("Recreated technical_indicators table.")

    # --- AI PREDICTIONS ---
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

    # --- SYSTEM CONFIG ---
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS system_config (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    # Initialize default configuration
    cursor.execute("INSERT OR IGNORE INTO system_config (key, value) VALUES ('sleep_mode', 'AUTO')")

    conn.commit()
    conn.close()
    print("Database setup complete.")


if __name__ == "__main__":
    setup_database()
