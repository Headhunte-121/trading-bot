import time
import sys
import os
from datetime import datetime
try:
    from zoneinfo import ZoneInfo
except ImportError:
    # Fallback for older python versions if needed, though 3.12 has it.
    from backports.zoneinfo import ZoneInfo

# Ensure shared package is available if run directly
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import log_system_event

MARKET_OPEN_HOUR = 9
MARKET_OPEN_MINUTE = 30
MARKET_CLOSE_HOUR = 16
MARKET_CLOSE_MINUTE = 0  # 16:00:00 exactly

SLEEP_ACTIVE = 300
SLEEP_PASSIVE = 3600

def get_market_status():
    """
    Returns a dictionary with status details:
    - 'is_open': bool
    - 'status_message': str
    - 'sleep_seconds': int
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
            return {
                'is_open': True,
                'status_message': "🟢 Market Open - Active Mode (5m)",
                'sleep_seconds': SLEEP_ACTIVE
            }
        else:
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
    """Returns the sleep duration in seconds based on market status."""
    return get_market_status()['sleep_seconds']

if __name__ == "__main__":
    status = get_market_status()
    print(f"{status['status_message']}")

    # Log the sleep event
    log_level = "INFO" if status['is_open'] else "WARNING"
    log_system_event("SmartSleeper", log_level, f"{status['status_message']} - Sleeping {status['sleep_seconds']}s")

    print(f"Sleeping for {status['sleep_seconds']} seconds...")
    time.sleep(status['sleep_seconds'])
