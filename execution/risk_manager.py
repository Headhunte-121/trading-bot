"""
Service: Risk Manager
Role: Validates trade signals and calculates position sizing based on risk parameters.
Dependencies: sqlite3, shared.db_utils, shared.config
"""
import sqlite3
import os
import math
import sys
from dataclasses import dataclass
from typing import List, Tuple
from datetime import datetime, timezone

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection, log_system_event
from shared.smart_sleep import get_sleep_seconds, smart_sleep


@dataclass
class RiskConfig:
    """Configuration for Risk Management."""
    account_size: float
    risk_pct: float
    max_signal_age_minutes: int

    @classmethod
    def from_env(cls):
        """Loads configuration from environment variables or defaults."""
        return cls(
            account_size=float(os.getenv("ACCOUNT_SIZE", 100000)),
            risk_pct=float(os.getenv("RISK_PCT", 0.01)),
            max_signal_age_minutes=int(os.getenv("MAX_SIGNAL_AGE_MINUTES", 60))
        )


# Global config instance for module-level access (backward compatibility)
_CONFIG = RiskConfig.from_env()


def calculate_position_size(close_price: float, account_size: float = None, risk_pct: float = None) -> int:
    """
    Calculates the position size (number of shares).

    Logic:
    Position Value = Account Size * Risk %
    Shares = floor(Position Value / Close Price)

    Args:
        close_price (float): The current price of the asset.
        account_size (float): Account equity (defaults to global config).
        risk_pct (float): Risk percentage per trade (defaults to global config).

    Returns:
        int: Number of shares to buy (floor). Returns 0 if price <= 0.
    """
    if account_size is None:
        account_size = _CONFIG.account_size
    if risk_pct is None:
        risk_pct = _CONFIG.risk_pct

    if close_price <= 0:
        return 0

    target_position_value = account_size * risk_pct
    return math.floor(target_position_value / close_price)


class RiskManager:
    """
    Manages risk by sizing pending trade signals and filtering out stale ones.
    """
    def __init__(self):
        self.config = RiskConfig.from_env()
        self.service_name = "RiskManager"

    def process_pending_signals(self):
        """
        Fetches pending signals, calculates sizes, and updates them in batch.
        """
        conn = get_db_connection()
        if not conn:
            return

        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        try:
            # Optimize: select specific columns
            # Using subquery to fetch latest available 5m close price to handle timestamp mismatches
            query = """
                SELECT
                    ts.id,
                    ts.symbol,
                    ts.timestamp,
                    ts.signal_type,
                    (
                        SELECT close
                        FROM market_data md
                        WHERE md.symbol = ts.symbol
                        AND md.timeframe = '5m'
                        ORDER BY md.timestamp DESC
                        LIMIT 1
                    ) as close
                FROM trade_signals ts
                WHERE ts.status = 'PENDING'
            """
            cursor.execute(query)
            pending_signals = cursor.fetchall()

            if not pending_signals:
                return

            log_system_event(self.service_name, "INFO", f"Found {len(pending_signals)} pending signals.")

            updates: List[Tuple[int, int]] = []
            expired_updates: List[Tuple[int]] = []

            now = datetime.now(timezone.utc)

            for signal in pending_signals:
                signal_id = signal['id']
                symbol = signal['symbol']
                signal_type = signal['signal_type']
                close_price = signal['close']
                timestamp_str = signal['timestamp']

                # Check for staleness
                try:
                    # Parse timestamp (handle Z for UTC if present)
                    signal_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    if signal_time.tzinfo is None:
                        signal_time = signal_time.replace(tzinfo=timezone.utc)

                    age_minutes = (now - signal_time).total_seconds() / 60.0

                    if age_minutes > self.config.max_signal_age_minutes:
                        expired_updates.append((signal_id,))
                        log_system_event(self.service_name, "WARNING", f"Expiring stale signal {signal_id} ({symbol}): {age_minutes:.1f} min old.")
                        continue
                except ValueError as e:
                    log_system_event(self.service_name, "ERROR", f"Error parsing timestamp for signal {signal_id}: {e}")
                    continue

                # --- Exit Signal Logic ---
                if 'EXIT' in signal_type:
                    # Exits do not need sizing; the executor will sell ALL shares.
                    updates.append((0, signal_id))
                    log_system_event(self.service_name, "INFO", f"Approved EXIT signal {signal_id}: {symbol} ({signal_type})")
                    continue

                # --- Buy Signal Sizing Logic ---
                if close_price is None:
                    log_system_event(self.service_name, "WARNING", f"Skipping signal {signal_id} ({symbol}): No market data.")
                    continue

                size = calculate_position_size(close_price, self.config.account_size, self.config.risk_pct)

                if size > 0:
                    updates.append((size, signal_id))
                    log_system_event(self.service_name, "INFO", f"Sized signal {signal_id}: {symbol} @ {close_price:.2f} -> {size} shares")
                else:
                    log_system_event(self.service_name, "WARNING", f"Skipping signal {signal_id}: Calculated size is 0 (Price: {close_price})")

            # Execute batch updates
            if expired_updates:
                expired_query = "UPDATE trade_signals SET status = 'EXPIRED' WHERE id = ?"
                cursor.executemany(expired_query, expired_updates)
                log_system_event(self.service_name, "INFO", f"Expired {len(expired_updates)} stale signals.")

            if updates:
                update_query = "UPDATE trade_signals SET size = ?, status = 'SIZED' WHERE id = ?"
                cursor.executemany(update_query, updates)
                log_system_event(self.service_name, "INFO", f"Successfully sized {len(updates)} signals.")

            if expired_updates or updates:
                conn.commit()

        except sqlite3.Error as e:
            log_system_event(self.service_name, "ERROR", f"Database error: {e}")
            print(f"Database error: {e}", file=sys.stderr)
        except Exception as e:
            log_system_event(self.service_name, "ERROR", f"Unexpected error: {e}")
            print(f"Unexpected error: {e}", file=sys.stderr)
        finally:
            conn.close()

    def run(self):
        """Main execution loop."""
        print(f"Starting {self.service_name}...")
        log_system_event(self.service_name, "INFO", "Service started.")

        while True:
            self.process_pending_signals()

            sleep_sec = get_sleep_seconds()
            print(f"💤 {self.service_name} Sleeping for {sleep_sec} seconds...")
            smart_sleep(sleep_sec)


# Backward compatibility wrapper for testing or legacy calls
def run_risk_manager():
    manager = RiskManager()
    manager.process_pending_signals()


if __name__ == "__main__":
    RiskManager().run()
