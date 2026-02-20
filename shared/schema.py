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

    # market_data
    # Composite Primary Key (symbol, timestamp)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS market_data (
            symbol TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            PRIMARY KEY (symbol, timestamp)
        )
    """)

    # raw_news
    # Auto-Incrementing ID
    # Added UNIQUE constraint for deduplication: symbol, timestamp, headline
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_news (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            headline TEXT,
            sentiment_score REAL,
            relevance REAL,
            urgency INTEGER,
            UNIQUE(symbol, timestamp, headline)
        )
    """)

    # technical_indicators
    # Composite Primary Key (symbol, timestamp)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS technical_indicators (
            symbol TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            rsi_14 REAL,
            lower_bb REAL,
            PRIMARY KEY (symbol, timestamp)
        )
    """)

    # trade_signals
    # Auto-Incrementing ID
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trade_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            signal_type TEXT,
            size REAL,
            stop_loss REAL,
            status TEXT
        )
    """)

    # executed_trades
    # Auto-Incrementing ID
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

    # Enable WAL mode explicitly on setup
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;")

    conn.commit()
    conn.close()
    print("Database setup complete.")

if __name__ == "__main__":
    setup_database()
