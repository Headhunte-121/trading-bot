import sqlite3
import os

# Define the path to the database
# Using absolute path resolution relative to this file
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_NAME = "trade_history.db"
DB_PATH = os.path.join(DATA_DIR, DB_NAME)

def get_db_connection(db_path=None, timeout=60.0):
    """
    Establishes a connection to the SQLite database with configured timeout and journal mode.

    Args:
        db_path (str, optional): Path to the database file. Defaults to shared.db_utils.DB_PATH.
        timeout (float): Timeout in seconds for waiting for the database lock.

    Returns:
        sqlite3.Connection: A connection object.
    """
    if db_path is None:
        db_path = DB_PATH

    # Ensure the directory exists
    db_dir = os.path.dirname(db_path)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    conn = sqlite3.connect(db_path, timeout=timeout)

    # Enable WAL mode for better concurrency
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;") # Recommended for WAL mode
        conn.execute("PRAGMA busy_timeout=5000;") # Wait up to 5000ms for a lock
    except sqlite3.Error:
        # Might fail if database is locked, but connection should handle it via timeout
        pass

    return conn

def log_system_event(service_name, log_level, message):
    """
    Logs a system event to the database.

    Args:
        service_name (str): Name of the service (e.g., "MarketHarvester").
        log_level (str): Level of the log (e.g., "INFO", "ERROR").
        message (str): The message content.
    """
    import datetime

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

        cursor.execute("""
            INSERT INTO system_logs (timestamp, service_name, log_level, message)
            VALUES (?, ?, ?, ?)
        """, (timestamp, service_name, log_level, message))

        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Failed to log system event: {e}")
