"""
Service: Smart Sleep Mechanism
Role: dynamic sleep intervals based on market hours and system power mode.
Dependencies: time, datetime, zoneinfo, shared.db_utils
"""
import time
import sys
import os
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
        cursor.execute("SELECT value FROM system_config WHERE key = %s", (key,))
        row = cursor.fetchone()
        if row:
            return row['value']
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


def get_raw_market_status():
    """
    Determines the current market status based solely on time, ignoring configuration overrides.

    Returns:
        dict: A dictionary containing:
            - 'is_open' (bool): Whether the market is currently open.
            - 'seconds_until_open' (int or None): Seconds until market opens, if applicable.
    """
    try:
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
            return {'is_open': True, 'seconds_until_open': 0}

        seconds_until_open = None
        if is_weekday and ny_time < market_open:
             seconds_until_open = int((market_open - ny_time).total_seconds())

        return {'is_open': False, 'seconds_until_open': seconds_until_open}

    except Exception as e:
        print(f"Error checking raw market status: {e}")
        return {'is_open': False, 'seconds_until_open': None}


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

        # Use Raw Status Logic
        raw = get_raw_market_status()

        if raw['is_open']:
            return {
                'is_open': True,
                'status_message': "🟢 Market Open - Active Mode (5m)",
                'sleep_seconds': SLEEP_ACTIVE
            }
        else:
            if raw['seconds_until_open'] is not None:
                sleep_seconds = min(SLEEP_PASSIVE, raw['seconds_until_open'])
                return {
                    'is_open': False,
                    'status_message': f"🟠 Market Closed - Opening in {raw['seconds_until_open']}s",
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


def get_sleep_time_to_next_candle(interval_minutes=5, offset_seconds=0):
    """
    Calculates the sleep duration to sync with the next candle interval.
    Used to implement 'Offset Sleep Strategy' (Harvester at :00, TA at :20, Strategy at :40).

    Args:
        interval_minutes (int): Candle timeframe in minutes.
        offset_seconds (int): Additional delay after the candle mark.

    Returns:
        int: Seconds to sleep.
    """
    status = get_market_status()

    # If market is closed (and not forced awake), sleep standard passive duration
    if not status['is_open']:
        return status['sleep_seconds']

    # If market is open (or forced awake), sync to clock
    now = datetime.now()
    timestamp = now.timestamp()
    interval_seconds = interval_minutes * 60

    # Calculate time remaining in current interval
    # timestamp % interval_seconds gives time elapsed in current interval (relative to epoch)
    # This aligns to XX:00, XX:05, etc.
    elapsed = timestamp % interval_seconds
    remaining = interval_seconds - elapsed

    # Add offset
    sleep_time = remaining + offset_seconds

    return int(sleep_time)


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

    # Test new function
    sync_sleep = get_sleep_time_to_next_candle(offset_seconds=20)
    print(f"Sync Sleep (Offset 20s): {sync_sleep}s")

    # Log the sleep event
    log_level = "INFO" if status['is_open'] else "WARNING"
    log_system_event("SmartSleeper", log_level, f"{status['status_message']} - Sleeping {status['sleep_seconds']}s")

    print(f"Sleeping for {status['sleep_seconds']} seconds...")
    smart_sleep(status['sleep_seconds'])
