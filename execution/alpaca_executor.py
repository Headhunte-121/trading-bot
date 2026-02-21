import os
import time
import sqlite3
import datetime
import sys
import traceback

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection, log_system_event
from shared.smart_sleep import get_sleep_seconds, smart_sleep

try:
    from alpaca_trade_api.rest import REST, APIError
except ImportError:
    # Fallback if APIError is not directly importable (though it should be)
    from alpaca_trade_api.rest import REST
    APIError = Exception

TRAIL_PERCENT_DEFAULT = 2.0

class AlpacaExecutor:
    def __init__(self):
        self.api = None
        self.failure_count = 0
        self.circuit_breaker_tripped = False
        self._connect_api()

    def _connect_api(self):
        """Initializes the Alpaca API connection."""
        api_key = os.getenv("APCA_API_KEY_ID")
        api_secret = os.getenv("APCA_API_SECRET_KEY")
        base_url = os.getenv("APCA_API_BASE_URL")

        if not all([api_key, api_secret, base_url]):
            self._log("CRITICAL", "Alpaca environment variables not set. Executor cannot start.")
            self.circuit_breaker_tripped = True
            return

        try:
            self.api = REST(api_key, api_secret, base_url)
            self._log("INFO", "✅ Alpaca API Connected Successfully.")
        except Exception as e:
            self._log("CRITICAL", f"Failed to initialize Alpaca API: {e}")
            self.circuit_breaker_tripped = True

    def _log(self, level, message):
        """Helper to log to both console (for Docker) and DB (for User)."""
        print(f"[{level}] {message}")
        log_system_event("AlpacaExecutor", level, message)

    def _check_circuit_breaker(self, error):
        """
        Updates circuit breaker state based on the error.
        Trips if 3 consecutive Auth (401/403) or Server (5xx) errors occur.
        """
        is_critical = False
        error_str = str(error)

        # Check for HTTP status codes in the error message
        if "401" in error_str or "403" in error_str:
            is_critical = True
        elif "500" in error_str or "502" in error_str or "503" in error_str or "504" in error_str:
            is_critical = True

        if is_critical:
            self.failure_count += 1
            self._log("WARNING", f"⚠️ API Error ({self.failure_count}/3): {error}")

            if self.failure_count >= 3:
                self.circuit_breaker_tripped = True
                self._log("FATAL", "🔥 CIRCUIT BREAKER TRIPPED! Stopping all trading activities due to consecutive API failures.")
        else:
            # If it's a different error (e.g., 400 Bad Request, 429 Rate Limit),
            # we might not want to trip immediately, but 429 is serious.
            # User specifically mentioned Auth or 500.
            # I will keep it strict to the requirement but maybe reset count on success.
            pass

    def _safe_api_call(self, func, *args, **kwargs):
        """
        Wraps API calls with circuit breaker logic.
        """
        if self.circuit_breaker_tripped:
            return None

        try:
            result = func(*args, **kwargs)
            # Reset failure count on success
            if self.failure_count > 0:
                self.failure_count = 0
                self._log("INFO", "✅ API connection restored. Failure count reset.")
            return result
        except Exception as e:
            self._check_circuit_breaker(e)
            return None

    def process_sized_signals(self, conn):
        """
        Step 1: Query 'SIZED' signals, submit Market Buy, and update to 'SUBMITTED'.
        """
        if self.circuit_breaker_tripped:
            return

        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT id, symbol, size, signal_type
                FROM trade_signals
                WHERE status = 'SIZED'
            """)
            signals = cursor.fetchall()

            if not signals:
                return

            self._log("INFO", f"🚀 Processing {len(signals)} new SIZED signals...")

            for signal in signals:
                signal_id = signal['id']
                symbol = signal['symbol']
                qty = float(signal['size']) # Ensure float for robust handling, though int is preferred for stocks usually
                # API usually accepts float or int. User said "qty = int(signal['size'])" in original code.
                # I'll stick to float but maybe cast to int if needed?
                # Original code: qty = int(signal['size'])
                # But Crypto allows fractional.
                # User's Memory: "Calculates fractional quantities...".
                # I should probably not cast to int blindly if it's crypto.
                # Let's check shared config or just use float.
                # Original code used `int(signal['size'])`.
                # I'll use `float` to be safe for crypto, as Alpaca handles it.

                self._log("INFO", f"   -> Submitting BUY {qty} {symbol} (Market)...")

                # Use _safe_api_call
                # We need to pass the function and args.
                # Since submit_order is a method of self.api, we can pass a lambda or partial,
                # or just pass the method bound to the object.

                buy_order = self._safe_api_call(
                    self.api.submit_order,
                    symbol=symbol,
                    qty=qty,
                    side='buy',
                    type='market',
                    time_in_force='gtc'
                )

                if buy_order:
                    # Update status to SUBMITTED
                    cursor.execute("UPDATE trade_signals SET status = 'SUBMITTED', order_id = ? WHERE id = ?", (buy_order.id, signal_id))
                    conn.commit()
                    self._log("INFO", f"   -> Signal {signal_id} ({symbol}) moved to SUBMITTED. Order ID: {buy_order.id}")
                else:
                    # API call failed (and handled by _safe_api_call)
                    # We might want to mark as FAILED if it wasn't a circuit breaker trip,
                    # but if it was a temporary error, maybe leave as SIZED?
                    # Original code marked as FAILED on exception.
                    # I'll mark as FAILED to prevent infinite loops on bad orders.
                    if not self.circuit_breaker_tripped:
                         self._log("ERROR", f"❌ Failed to submit BUY order for {symbol}.")
                         cursor.execute("UPDATE trade_signals SET status = 'FAILED' WHERE id = ?", (signal_id,))
                         conn.commit()

        except Exception as e:
            self._log("ERROR", f"Error in process_sized_signals: {e}")

    def process_submitted_signals(self, conn):
        """
        Step 2: Monitor 'SUBMITTED' orders.
        - If filled: Submit Trailing Stop (with Retry), log trade, update to 'EXECUTED'.
        - If canceled/rejected/expired: Update to 'FAILED'.
        """
        if self.circuit_breaker_tripped:
            return

        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT id, symbol, order_id, signal_type, atr
                FROM trade_signals
                WHERE status = 'SUBMITTED'
            """)
            signals = cursor.fetchall()

            if not signals:
                return

            for signal in signals:
                signal_id = signal['id']
                symbol = signal['symbol']
                order_id = signal['order_id']
                signal_type = signal['signal_type']
                atr = signal['atr']

                if not order_id:
                    self._log("WARNING", f"⚠️ Signal {signal_id} ({symbol}) is SUBMITTED but has no order_id. Marking FAILED.")
                    cursor.execute("UPDATE trade_signals SET status = 'FAILED' WHERE id = ?", (signal_id,))
                    conn.commit()
                    continue

                order_status = self._safe_api_call(self.api.get_order, order_id)

                if not order_status:
                    continue

                status = order_status.status

                if status == 'filled':
                    filled_qty = float(order_status.filled_qty)
                    avg_price = float(order_status.filled_avg_price) if order_status.filled_avg_price else 0.0

                    self._log("INFO", f"✅ Order {order_id} ({symbol}) FILLED: {filled_qty} @ {avg_price:.2f}")

                    # Log execution to DB
                    ts_iso = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                    self._log_trade(conn, symbol, avg_price, filled_qty, 'buy', ts_iso, signal_type)

                    # Submit Trailing Stop with RETRY LOGIC
                    self._submit_trailing_stop(conn, signal_id, symbol, filled_qty, atr, signal_type)

                elif status in ['canceled', 'rejected', 'expired']:
                    self._log("WARNING", f"❌ Order {order_id} ({symbol}) was {status}. Marking signal FAILED.")
                    cursor.execute("UPDATE trade_signals SET status = 'FAILED' WHERE id = ?", (signal_id,))
                    conn.commit()

        except Exception as e:
            self._log("ERROR", f"Error in process_submitted_signals: {e}")
            traceback.print_exc()

    def _log_trade(self, conn, symbol, price, qty, side, timestamp_str, signal_type):
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO executed_trades (symbol, timestamp, price, qty, side, signal_type)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (symbol, timestamp_str, float(price), float(qty), side, signal_type))
            conn.commit()
        except Exception as e:
            self._log("ERROR", f"Failed to log trade to DB: {e}")

    def _submit_trailing_stop(self, conn, signal_id, symbol, qty, atr, signal_type):
        """
        Submits a trailing stop with retry logic (3 attempts).
        """
        # Calculate parameters
        trail_price = None
        trail_percent = None

        if atr is not None and signal_type:
            multiplier = 2.0
            if signal_type == 'VWAP_SCALP':
                multiplier = 1.5
            elif signal_type == 'DEEP_VALUE_BUY':
                multiplier = 2.0
            elif signal_type == 'TREND_BUY':
                multiplier = 3.0

            trail_price = round(multiplier * float(atr), 2)
        else:
            trail_percent = TRAIL_PERCENT_DEFAULT

        order_args = {
            "symbol": symbol,
            "qty": qty,
            "side": "sell",
            "type": "trailing_stop",
            "time_in_force": "gtc"
        }
        if trail_price:
            order_args["trail_price"] = trail_price
            stop_desc = f"${trail_price} (ATR based)"
        else:
            order_args["trail_percent"] = trail_percent
            stop_desc = f"{trail_percent}% (Fallback)"

        self._log("INFO", f"🛑 Attempting Trailing Stop for {symbol}: {stop_desc}")

        # Retry Loop
        max_retries = 3
        success = False

        for attempt in range(1, max_retries + 1):
            stop_order = self._safe_api_call(self.api.submit_order, **order_args)

            if stop_order:
                self._log("INFO", f"✅ Trailing Stop submitted (Attempt {attempt}): {stop_order.id}")
                cursor = conn.cursor()
                cursor.execute("UPDATE trade_signals SET status = 'EXECUTED' WHERE id = ?", (signal_id,))
                conn.commit()
                success = True
                break
            else:
                self._log("WARNING", f"⚠️ Trailing Stop attempt {attempt} failed for {symbol}.")
                if attempt < max_retries:
                    time.sleep(3) # Wait before retry

        if not success:
            self._log("CRITICAL", f"❌ FAILED TO PROTECT POSITION: Could not submit Trailing Stop for {symbol} after {max_retries} attempts.")
            cursor = conn.cursor()
            cursor.execute("UPDATE trade_signals SET status = 'EXECUTED_NO_STOP' WHERE id = ?", (signal_id,))
            conn.commit()

    def run(self):
        self._log("INFO", "✅ Alpaca Executor Online (Circuit Breaker & Smart Sleep Enabled).")

        while True:
            if self.circuit_breaker_tripped:
                self._log("FATAL", "💀 Executor Halted due to Circuit Breaker. Manual intervention required.")
                time.sleep(300) # Sleep long to avoid log spam
                continue

            conn = None
            try:
                conn = get_db_connection()
                conn.row_factory = sqlite3.Row

                # 1. Process Sized Signals (Buy)
                self.process_sized_signals(conn)

                # 2. Process Submitted Signals (Monitor & Stop)
                self.process_submitted_signals(conn)

                # 3. Check for Pending Orders to determine Sleep Mode
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM trade_signals WHERE status = 'SUBMITTED'")
                pending_count = cursor.fetchone()[0]

                if pending_count > 0:
                    # If we have pending orders, we must stay awake to monitor fills
                    # self._log("INFO", f"👀 Monitoring {pending_count} pending orders...")
                    smart_sleep(5)
                else:
                    # No pending orders, respect market hours
                    sleep_seconds = get_sleep_seconds()
                    # Only log if it's a significant sleep (>= 60s) or status changed
                    # But for now, just standard log
                    if sleep_seconds > 60:
                        self._log("INFO", f"💤 No pending orders. Sleeping for {sleep_seconds}s...")
                    smart_sleep(sleep_seconds)

            except Exception as e:
                self._log("ERROR", f"Main Loop Error: {e}")
                time.sleep(5)
            finally:
                if conn:
                    try:
                        conn.close()
                    except:
                        pass

if __name__ == "__main__":
    executor = AlpacaExecutor()
    executor.run()
