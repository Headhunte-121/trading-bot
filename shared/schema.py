import sqlite3
import os

# Define the path to the database
DB_NAME = "trade_history.db"
# Use os.path to determine the root directory relative to this script
# schema.py is in /shared/schema.py, so root is ..
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, DB_NAME)

def setup_database():
    """Initializes the database and creates tables if they do not exist."""
    # Ensure the data directory exists
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    conn = sqlite3.connect(DB_PATH)
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
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_news (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            headline TEXT,
            sentiment_score REAL
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

    conn.commit()
    conn.close()
    print(f"Database initialized at {DB_PATH}")

if __name__ == "__main__":
    setup_database()
