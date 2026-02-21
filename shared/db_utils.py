"""
Service: Shared Utilities
Role: Provides core database connectivity and system logging for the Deep Quant Terminal.
Dependencies: sqlite3, os, sys, datetime
"""
import sqlite3
import os
import sys
import datetime
import time

# Define the path to the database
# Using absolute path resolution relative to this file
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_NAME = "trade_history.db"
DB_PATH = os.path.join(DATA_DIR, DB_NAME)


def get_db_connection(db_path=None, timeout=60.0):
    """
    Establishes a connection to the SQLite database with configured timeout and journal mode.
    Implements a retry mechanism for transient errors (e.g., locking).

    Args:
        db_path (str, optional): Path to the database file. Defaults to shared.db_utils.DB_PATH.
        timeout (float): Timeout in seconds for waiting for the database lock.

    Returns:
        sqlite3.Connection: A connection object, or None if connection fails.
    """
    if db_path is None:
        db_path = DB_PATH

    # Ensure the directory exists
    db_dir = os.path.dirname(db_path)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    max_retries = 3
    retry_delay = 0.5

    for attempt in range(1, max_retries + 1):
        try:
            conn = sqlite3.connect(db_path, timeout=timeout)
            conn.row_factory = sqlite3.Row  # Enable column access by name

            # Enable WAL mode for better concurrency
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")  # Recommended for WAL mode
            conn.execute("PRAGMA busy_timeout=5000;")  # Wait up to 5000ms for a lock
            return conn
        except sqlite3.OperationalError as e:
            # Handle transient locking or file access errors
            if attempt < max_retries:
                time.sleep(retry_delay)
                continue
            else:
                print(f"[ERROR] Failed to connect to database at {db_path} after {max_retries} attempts: {e}", file=sys.stderr)
                return None
        except sqlite3.Error as e:
            # Non-recoverable error
            print(f"[ERROR] Failed to connect to database at {db_path}: {e}", file=sys.stderr)
            return None

    return None


def execute_query(query, params=(), db_path=None):
    """
    Executes a read-only query and returns the results.

    Args:
        query (str): The SQL query to execute.
        params (tuple, optional): Parameters to substitute into the query. Defaults to ().
        db_path (str, optional): Path to the database file. Defaults to shared.db_utils.DB_PATH.

    Returns:
        list: A list of sqlite3.Row objects.
    """
    conn = get_db_connection(db_path)
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            results = cursor.fetchall()
            conn.close()
            return results
        except sqlite3.Error as e:
            print(f"[ERROR] Query failed: {query} with params {params}. Error: {e}", file=sys.stderr)
            conn.close()
            return []
    return []


def log_system_event(service_name, log_level, message):
    """
    Logs a system event to the database.

    Args:
        service_name (str): Name of the service (e.g., "MarketHarvester").
        log_level (str): Level of the log (e.g., "INFO", "ERROR").
        message (str): The message content.
    """
    try:
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

            cursor.execute("""
                INSERT INTO system_logs (timestamp, service_name, log_level, message)
                VALUES (?, ?, ?, ?)
            """, (timestamp, service_name, log_level, message))

            conn.commit()
            conn.close()
    except Exception as e:
        print(f"[ERROR] Failed to log system event: {e}", file=sys.stderr)
