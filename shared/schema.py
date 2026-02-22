"""
Service: Database Schema Management
Role: Manages database initialization, table creation, and schema migrations.
Dependencies: psycopg2, shared.db_utils
"""
import psycopg2
import os
import sys
import re

# Ensure shared package is available if run directly
if __name__ == "__main__":
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.db_utils import get_db_connection


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
        cursor: Postgres cursor object.
        table (str): Table name.
        column (str): Column name.
        definition (str): Column definition (e.g., "DOUBLE PRECISION", "TEXT").
    """
    if not _is_safe_identifier(table) or not _is_safe_identifier(column):
        print(f"Error: Invalid identifier '{table}' or '{column}'.")
        return

    if not _is_safe_definition(definition):
        print(f"Error: Invalid definition '{definition}'.")
        return

    try:
        # Postgres supports IF NOT EXISTS for ADD COLUMN since 9.6
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {definition}")
        print(f"Ensured column '{column}' exists in table '{table}'.")
    except psycopg2.Error as e:
        print(f"Error adding column '{column}' to table '{table}': {e}")


def setup_database():
    """Initializes the database and creates tables if they do not exist."""
    print("Setting up Postgres database...")

    conn = get_db_connection()
    if not conn:
        print("Failed to connect to database during setup.")
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
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            close DOUBLE PRECISION,
            volume DOUBLE PRECISION,
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
            rsi_14 DOUBLE PRECISION,
            sma_50 DOUBLE PRECISION,
            sma_200 DOUBLE PRECISION,
            lower_bb DOUBLE PRECISION,
            vwap DOUBLE PRECISION,
            atr_14 DOUBLE PRECISION,
            volume_sma_20 DOUBLE PRECISION,
            PRIMARY KEY (symbol, timestamp, timeframe)
        )
    """)
    print("Recreated technical_indicators table.")

    # --- AI PREDICTIONS ---
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ai_predictions (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            current_price DOUBLE PRECISION,
            small_predicted_price DOUBLE PRECISION,
            large_predicted_price DOUBLE PRECISION,
            ensemble_predicted_price DOUBLE PRECISION,
            ensemble_pct_change DOUBLE PRECISION,
            UNIQUE(symbol, timestamp)
        )
    """)

    # --- TRADE SIGNALS ---
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trade_signals (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            signal_type TEXT,
            size DOUBLE PRECISION,
            stop_loss DOUBLE PRECISION,
            status TEXT,
            order_id TEXT
        )
    """)
    add_column_if_not_exists(cursor, "trade_signals", "atr", "DOUBLE PRECISION")

    # --- EXECUTED TRADES ---
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS executed_trades (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            price DOUBLE PRECISION,
            qty DOUBLE PRECISION,
            side TEXT
        )
    """)
    add_column_if_not_exists(cursor, "executed_trades", "signal_type", "TEXT")

    # --- SYSTEM LOGS ---
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS system_logs (
            id SERIAL PRIMARY KEY,
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
    cursor.execute("INSERT INTO system_config (key, value) VALUES ('sleep_mode', 'AUTO') ON CONFLICT (key) DO NOTHING")

    conn.commit()
    conn.close()
    print("Database setup complete.")


if __name__ == "__main__":
    setup_database()
