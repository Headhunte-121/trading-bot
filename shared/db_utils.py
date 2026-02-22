"""
Service: Shared Utilities
Role: Provides core database connectivity and system logging for the Deep Quant Terminal.
Dependencies: psycopg2, os, sys, datetime
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import numpy as np
from psycopg2.extensions import register_adapter, AsIs
import os
import sys
import datetime
import time

def add_numpy_adapters():
    """Registers Numpy types with Psycopg2 to allow automatic adaptation."""
    register_adapter(np.float64, AsIs)
    register_adapter(np.float32, AsIs)
    register_adapter(np.int64, AsIs)
    register_adapter(np.int32, AsIs)

add_numpy_adapters()

# Define the path to the database (Deprecated for Postgres, kept for reference if needed)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_db_connection(db_path=None, timeout=60.0, log_error=True):
    """
    Establishes a connection to the PostgreSQL database using environment variables.

    Args:
        db_path (str, optional): Ignored. Kept for backward compatibility.
        timeout (float): Ignored. Postgres handles timeouts differently.
        log_error (bool): Whether to log connection errors to stderr. Defaults to True.

    Returns:
        psycopg2.extensions.connection: A connection object, or None if connection fails.
    """
    max_retries = 3
    retry_delay = 1.0

    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "postgres_db"),
                port=os.getenv("DB_PORT", "5432"),
                database=os.getenv("DB_NAME", "trade_history"),
                user=os.getenv("DB_USER", "quant_user"),
                password=os.getenv("DB_PASS", "quant_password_123"),
                cursor_factory=RealDictCursor
            )
            # Auto-commit is NOT enabled by default in psycopg2 (unlike sqlite3 in some wrappers, but python sqlite3 also requires commit)
            # We will rely on explicit commits as before.
            return conn
        except psycopg2.OperationalError as e:
            # Handle transient connection errors
            if attempt < max_retries:
                time.sleep(retry_delay)
                continue
            else:
                if log_error:
                    print(f"[ERROR] Failed to connect to Postgres after {max_retries} attempts: {e}", file=sys.stderr)
                return None
        except psycopg2.Error as e:
            # Non-recoverable error
            if log_error:
                print(f"[ERROR] Failed to connect to Postgres: {e}", file=sys.stderr)
            return None

    return None


def execute_query(query, params=(), db_path=None):
    """
    Executes a read-only query and returns the results.

    Args:
        query (str): The SQL query to execute.
        params (tuple, optional): Parameters to substitute into the query. Defaults to ().
        db_path (str, optional): Ignored.

    Returns:
        list: A list of RealDictRow objects (dict-like).
    """
    conn = get_db_connection(db_path)
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            results = cursor.fetchall()
            conn.close()
            return results
        except psycopg2.Error as e:
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
                VALUES (%s, %s, %s, %s)
            """, (timestamp, service_name, log_level, message))

            conn.commit()
            conn.close()
    except Exception as e:
        print(f"[ERROR] Failed to log system event: {e}", file=sys.stderr)
