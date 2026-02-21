"""
Service: Smart Sleep Mechanism
Role: dynamic sleep intervals based on market hours and system power mode.
Dependencies: time, datetime, zoneinfo, shared.db_utils
"""
import time
import sys
import os
import sqlite3
from datetime import datetime
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

# Ensure shared package is available if run directly
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import log_system_event, get_db_connection

MARKET_OPEN_HOUR = 9
MARKET_OPEN_MINUTE = 30
MARKET_CLOSE_HOUR = 16
MARKET_CLOSE_MINUTE = 0  # 16:00:00 exactly

SLEEP_ACTIVE = 300
SLEEP_PASSIVE = 3600


def get_config_value(key, default):
    """
    Retrieves a configuration value from the database with a short timeout.

    Args:
        key (str): The configuration key to retrieve.
        default (any): The default value if key is not found or error occurs.

    Returns:
        any: The configuration value or default.
    """
    conn = None
    try:
        conn = get_db_connection(timeout=1.0, log_error=False)  # Short timeout, silent fail
        if conn is None:
            return default

        cursor = conn.cursor()
        cursor.execute("SELECT value FROM system_config WHERE key = ?", (key,))
        row = cursor.fetchone()
        if row:
            return row[0]
        return default
    except sqlite3.OperationalError:
        # Database locked or busy
        return default
    except Exception as e:
        print(f"Error reading config: {e}")
        return default
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def get_market_status():
    """
    Determines the current market status and appropriate sleep duration.

    Returns:
        dict: A dictionary containing:
            - 'is_open' (bool): Whether the market is currently open.
            - 'status_message' (str): A descriptive status message.
            - 'sleep_seconds' (int): The recommended sleep duration in seconds.
    """
    try:
        # Check System Config Override
        sleep_mode = get_config_value("sleep_mode", "AUTO")

        if sleep_mode == "FORCE_AWAKE":
            return {
                'is_open': True,
                'status_message': "⚡ Force Awake - Active Mode (5m)",
                'sleep_seconds': SLEEP_ACTIVE
            }
        elif sleep_mode == "FORCE_SLEEP":
            return {
                'is_open': False,
                'status_message': "🌙 Force Sleep - Sleep Mode (1h)",
                'sleep_seconds': SLEEP_PASSIVE
            }

        # Get current time in New York
        ny_time = datetime.now(ZoneInfo("America/New_York"))

        # Check if it's a weekday (Monday=0, Sunday=6)
        is_weekday = ny_time.weekday() < 5

        # Define market hours for today
        market_open = ny_time.replace(hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MINUTE, second=0, microsecond=0)
        market_close = ny_time.replace(hour=MARKET_CLOSE_HOUR, minute=MARKET_CLOSE_MINUTE, second=0, microsecond=0)

        # Check if current time is within market hours
        # Exclusive of 16:00:00 means < market_close
        is_market_hours = market_open <= ny_time < market_close

        if is_weekday and is_market_hours:
            return {
                'is_open': True,
                'status_message': "🟢 Market Open - Active Mode (5m)",
                'sleep_seconds': SLEEP_ACTIVE
            }
        else:
            # Calculate time until market open
            if is_weekday and ny_time < market_open:
                # Same day, before market open
                seconds_until_open = (market_open - ny_time).total_seconds()
                sleep_seconds = min(SLEEP_PASSIVE, int(seconds_until_open))
                return {
                    'is_open': False,
                    'status_message': f"🟠 Market Closed - Opening in {int(seconds_until_open)}s",
                    'sleep_seconds': sleep_seconds
                }

            return {
                'is_open': False,
                'status_message': "🔴 Market Closed - Sleep Mode (1h)",
                'sleep_seconds': SLEEP_PASSIVE
            }

    except Exception as e:
        print(f"Error checking market status: {e}")
        # Default to safe mode (1 hour sleep)
        return {
            'is_open': False,
            'status_message': "⚠️ Error - Defaulting to Sleep Mode (1h)",
            'sleep_seconds': SLEEP_PASSIVE
        }


def get_sleep_seconds():
    """
    Returns the sleep duration in seconds based on market status.

    Returns:
        int: Sleep duration in seconds.
    """
    return get_market_status()['sleep_seconds']


def smart_sleep(seconds):
    """
    Sleeps for the specified duration but checks for 'FORCE_AWAKE' every second.
    If 'FORCE_AWAKE' is detected AND the sleep duration is long (> 300s), it wakes up immediately.
    Short sleeps (<= 300s) are respected to prevent rapid looping.

    Args:
        seconds (int): Total seconds to sleep.
    """
    seconds = int(seconds)
    # Cast to int to ensure range works
    for _ in range(seconds):
        sleep_mode = get_config_value("sleep_mode", "AUTO")

        # Only interrupt if we are in a LONG sleep (e.g. market closed)
        # If we are already in active mode (sleeping 300s), let it sleep!
        if sleep_mode == "FORCE_AWAKE" and seconds > SLEEP_ACTIVE:
            print("⚡ Force Awake Detected! Waking up...")
            return

        time.sleep(1)


if __name__ == "__main__":
    status = get_market_status()
    print(f"{status['status_message']}")

    # Log the sleep event
    log_level = "INFO" if status['is_open'] else "WARNING"
    log_system_event("SmartSleeper", log_level, f"{status['status_message']} - Sleeping {status['sleep_seconds']}s")

    print(f"Sleeping for {status['sleep_seconds']} seconds...")
    smart_sleep(status['sleep_seconds'])
