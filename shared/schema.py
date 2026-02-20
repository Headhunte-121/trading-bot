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

    # market_data (5-minute candles)
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

    # market_data_daily (Daily candles with SMA 200)
    # Composite Primary Key (symbol, date)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS market_data_daily (
            symbol TEXT NOT NULL,
            date TEXT NOT NULL,
            close REAL,
            sma_200 REAL,
            PRIMARY KEY (symbol, date)
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

    # technical_indicators (5-minute technicals)
    # Composite Primary Key (symbol, timestamp)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS technical_indicators (
            symbol TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            rsi_14 REAL,
            lower_bb REAL,
            sma_200 REAL,
            PRIMARY KEY (symbol, timestamp)
        )
    """)

    # Attempt migration for sma_200
    try:
        cursor.execute("ALTER TABLE technical_indicators ADD COLUMN sma_200 REAL")
        print("Migrated technical_indicators table: Added sma_200 column.")
    except sqlite3.OperationalError:
        # Column likely already exists
        pass

    # chart_analysis_requests (New for AI Brain)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS chart_analysis_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            technical_summary TEXT,
            status TEXT DEFAULT 'PENDING',
            ai_prediction TEXT,
            ai_confidence REAL,
            ai_reasoning TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # trade_signals
    # Auto-Incrementing ID
    # Added order_id for tracking Alpaca orders
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

    # Attempt migration for order_id if table already existed without it
    try:
        cursor.execute("ALTER TABLE trade_signals ADD COLUMN order_id TEXT")
        print("Migrated trade_signals table: Added order_id column.")
    except sqlite3.OperationalError:
        # Column likely already exists
        pass

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
