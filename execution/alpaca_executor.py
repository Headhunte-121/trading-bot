import os
import time
import sqlite3
import datetime
import sys
from alpaca_trade_api.rest import REST

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection

def get_alpaca_api():
    api_key = os.getenv("APCA_API_KEY_ID")
    api_secret = os.getenv("APCA_API_SECRET_KEY")
    base_url = os.getenv("APCA_API_BASE_URL")

    if not all([api_key, api_secret, base_url]):
        print("Error: Alpaca environment variables not set.")
        return None

    try:
        return REST(api_key, api_secret, base_url)
    except Exception as e:
        print(f"Failed to initialize Alpaca API: {e}")
        return None

def log_trade(conn, symbol, price, qty, side, timestamp_str):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO executed_trades (symbol, timestamp, price, qty, side)
            VALUES (?, ?, ?, ?, ?)
        """, (symbol, timestamp_str, float(price), float(qty), side))
        conn.commit()
        print(f"✅ Logged trade: {side.upper()} {qty} {symbol} @ {price}")
    except Exception as e:
        print(f"Failed to log trade: {e}")

def check_submitted_orders(api, conn):
    """
    Checks the status of orders marked as 'SUBMITTED' in the database.
    If filled, logs the trade and updates status to 'EXECUTED'.
    If canceled/rejected, updates status to 'FAILED'.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT id, order_id, symbol FROM trade_signals WHERE status = 'SUBMITTED'")
    submitted_signals = cursor.fetchall()

    if not submitted_signals:
        return

    print(f"Checking status of {len(submitted_signals)} SUBMITTED orders...")

    for signal in submitted_signals:
        signal_id = signal['id']
        order_id = signal['order_id']
        symbol = signal['symbol']

        if not order_id:
            print(f"⚠️ Signal {signal_id} has status SUBMITTED but no order_id. Marking FAILED.")
            cursor.execute("UPDATE trade_signals SET status = 'FAILED' WHERE id = ?", (signal_id,))
            conn.commit()
            continue

        try:
            order = api.get_order(order_id)
            status = order.status

            if status == 'filled':
                fill_price = order.filled_avg_price
                filled_qty = order.filled_qty
                filled_at = order.filled_at

                # Timestamp parsing
                if isinstance(filled_at, str):
                    dt = datetime.datetime.fromisoformat(filled_at.replace("Z", "+00:00"))
                elif isinstance(filled_at, datetime.datetime):
                    dt = filled_at
                else:
                    dt = datetime.datetime.now(datetime.timezone.utc)

                ts_iso = dt.astimezone(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

                log_trade(conn, symbol, fill_price, filled_qty, 'buy', ts_iso)
                
                cursor.execute("UPDATE trade_signals SET status = 'EXECUTED' WHERE id = ?", (signal_id,))
                conn.commit()
                print(f"💰 Order {order_id} filled for {symbol}!")

            elif status in ['canceled', 'rejected', 'expired']:
                print(f"❌ Order {order_id} for {symbol} was {status}.")
                cursor.execute("UPDATE trade_signals SET status = 'FAILED' WHERE id = ?", (signal_id,))
                conn.commit()

            # If 'new', 'partially_filled', 'accepted', etc., do nothing and wait for next cycle.

        except Exception as e:
            print(f"Error checking order {order_id}: {e}")

def process_new_signals(api, conn):
    """Reads SIZED signals and submits them to Alpaca."""
    cursor = conn.cursor()
    cursor.execute("SELECT id, symbol, size, stop_loss FROM trade_signals WHERE status = 'SIZED'")
    signals = cursor.fetchall()

    if len(signals) > 0:
        print(f"Found {len(signals)} new signals to process.")

    for signal in signals:
        try:
            signal_id = signal['id']
            symbol = signal['symbol']
            qty = int(signal['size'])
            stop_loss_val = signal['stop_loss']

            if stop_loss_val is None:
                print(f"Skipping signal {signal_id}: Stop loss is None.")
                continue

            stop_loss_price = round(float(stop_loss_val), 2)

            print(f"Processing signal {signal_id}: BUY {qty} {symbol} with SL {stop_loss_price}")

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

            print(f"✅ Order submitted successfully: {order.id}")

            # Mark as SUBMITTED immediately and save order_id
            cursor.execute("UPDATE trade_signals SET status = 'SUBMITTED', order_id = ? WHERE id = ?", (order.id, signal_id))
            conn.commit()

        except Exception as e:
            print(f"❌ Error processing signal {signal['id']} with Alpaca: {e}")
            cursor.execute("UPDATE trade_signals SET status = 'FAILED' WHERE id = ?", (signal['id'],))
            conn.commit()

def main():
    api = get_alpaca_api()
    if not api:
        return

    conn = get_db_connection()
    conn.row_factory = sqlite3.Row # CRITICAL: Enable column access by name

    try:
        # First check existing orders
        check_submitted_orders(api, conn)

        # Then process new signals
        process_new_signals(api, conn)

    except Exception as e:
        print(f"Global executor error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
