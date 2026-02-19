import os
import time
import sqlite3
import datetime
from alpaca_trade_api.rest import REST, TimeFrame

# Define database path relative to this script
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "trade_history.db")

def get_db_connection(max_retries=5, retry_delay=1):
    """Establishes a database connection with retry logic."""
    for attempt in range(max_retries):
        try:
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            return conn
        except sqlite3.OperationalError as e:
            if "locked" in str(e):
                print(f"Database locked, retrying ({attempt + 1}/{max_retries})...")
                time.sleep(retry_delay)
            else:
                raise e
    raise sqlite3.OperationalError(f"Could not acquire database lock after {max_retries} attempts")

def process_signals():
    """Reads SIZED signals, executes them on Alpaca, and logs trades."""

    # Initialize Alpaca API
    api_key = os.getenv("APCA_API_KEY_ID")
    api_secret = os.getenv("APCA_API_SECRET_KEY")
    base_url = os.getenv("APCA_API_BASE_URL")

    if not all([api_key, api_secret, base_url]):
        print("Error: Alpaca environment variables not set.")
        return

    api = REST(api_key, api_secret, base_url)

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Fetch SIZED signals
        cursor.execute("SELECT id, symbol, size, stop_loss FROM trade_signals WHERE status = 'SIZED'")
        signals = cursor.fetchall()

        print(f"Found {len(signals)} signals to process.")

        for signal in signals:
            signal_id = signal["id"]
            symbol = signal["symbol"]
            qty = signal["size"]
            stop_loss_price = signal["stop_loss"]

            print(f"Processing signal {signal_id}: BUY {qty} {symbol} with SL {stop_loss_price}")

            try:
                # Submit Market Order with OTO Stop Loss
                order = api.submit_order(
                    symbol=symbol,
                    qty=qty,
                    side='buy',
                    type='market',
                    time_in_force='gtc',
                    order_class='oto',
                    stop_loss={'stop_price': stop_loss_price}
                )

                print(f"Order submitted: {order.id}")

                # Wait for execution
                time.sleep(2)

                # Retrieve updated order details
                updated_order = api.get_order(order.id)

                # Use filled_avg_price if available, otherwise it might still be pending/partial
                # The prompt implies we should log the final fill price.
                # If it's not filled, we log what we have or maybe we should wait longer?
                # Instruction: "Wait 2 seconds... then query... to get the exact filled_avg_price."
                # We will proceed with the value from updated_order.

                fill_price = updated_order.filled_avg_price
                if fill_price is None:
                    print(f"Warning: Order {order.id} not filled yet after 2s. Status: {updated_order.status}")
                    # If not filled, we probably shouldn't mark it EXECUTED?
                    # But the prompt says "Once Alpaca confirms the order, UPDATE... and log".
                    # Order confirmation (submit_order success) happened.
                    # But if price is None, we can't log price.
                    # Let's assume for simulation it fills. If None, we skip DB update to avoid crashing.
                    continue

                filled_qty = updated_order.filled_qty

                # Format timestamp to strict UTC ISO 8601 string (YYYY-MM-DDTHH:MM:SSZ)
                # Alpaca returns ISO strings usually. We ensure it ends in Z if it is UTC.
                # updated_order.filled_at is a string or datetime depending on library version.
                # alpaca-trade-api usually returns datetime objects if parsed, or strings.
                # Let's inspect the type or handle both.

                filled_at = updated_order.filled_at

                if isinstance(filled_at, str):
                    # Parse string to ensure we format it correctly or just use it if it matches?
                    # Example: '2023-10-27T15:00:00.123456Z'
                    # We want YYYY-MM-DDTHH:MM:SSZ
                    # Let's parse and reformat to be safe and consistent.
                    dt = datetime.datetime.fromisoformat(filled_at.replace("Z", "+00:00"))
                elif isinstance(filled_at, datetime.datetime):
                    dt = filled_at
                else:
                    dt = datetime.datetime.now(datetime.timezone.utc) # Fallback if None

                # Convert to UTC and format
                dt = dt.astimezone(datetime.timezone.utc)
                formatted_timestamp = dt.strftime('%Y-%m-%dT%H:%M:%SZ')

                # Update trade_signals status
                cursor.execute("UPDATE trade_signals SET status = 'EXECUTED' WHERE id = ?", (signal_id,))

                # Log executed trade
                cursor.execute("""
                    INSERT INTO executed_trades (symbol, timestamp, price, qty, side)
                    VALUES (?, ?, ?, ?, ?)
                """, (symbol, formatted_timestamp, float(fill_price), float(filled_qty), 'buy'))

                conn.commit()
                print(f"Signal {signal_id} executed and logged.")

            except Exception as e:
                print(f"Error processing signal {signal_id}: {e}")
                # We do not rollback the fetch, but maybe we should rollback any partial transaction?
                # We commit per signal, so if one fails, others proceed.
                # But if the INSERT fails after UPDATE, we have an issue.
                # The commit is at the end of the block, so it's atomic per signal.
                conn.rollback()

    except Exception as e:
        print(f"Global error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    process_signals()
