import os
import time
import sqlite3
import datetime
import sys
from alpaca_trade_api.rest import REST

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection

def process_signals():
    """Reads SIZED signals, executes them on Alpaca, and logs trades."""

    # Initialize Alpaca API
    api_key = os.getenv("APCA_API_KEY_ID")
    api_secret = os.getenv("APCA_API_SECRET_KEY")
    base_url = os.getenv("APCA_API_BASE_URL")

    # Ensure all keys exist
    if not all([api_key, api_secret, base_url]):
        print("Error: Alpaca environment variables not set.")
        return

    try:
        api = REST(api_key, api_secret, base_url)
    except Exception as e:
        print(f"Failed to initialize Alpaca API: {e}")
        return

    conn = get_db_connection()
    conn.row_factory = sqlite3.Row # CRITICAL: This makes results behave like dictionaries
    cursor = conn.cursor()

    try:
        # Fetch SIZED signals
        cursor.execute("SELECT id, symbol, size, stop_loss FROM trade_signals WHERE status = 'SIZED'")
        signals = cursor.fetchall()

        if len(signals) > 0:
            print(f"Found {len(signals)} signals to process.")

        for signal in signals:
            try:
                # Correctly access columns by name using the Row factory
                signal_id = signal['id']
                symbol = signal['symbol']
                qty = int(signal['size']) # Convert to int for safety

                # Correctly access the stop_loss column before converting to float
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

                # Mark as SUBMITTED immediately
                cursor.execute("UPDATE trade_signals SET status = 'SUBMITTED' WHERE id = ?", (signal_id,))
                conn.commit()

                # Wait for execution to see if it fills instantly
                time.sleep(2)

                # Retrieve updated order details
                updated_order = api.get_order(order.id)
                fill_price = updated_order.filled_avg_price
                filled_qty = updated_order.filled_qty

                if fill_price is None or float(filled_qty) == 0:
                    print(f"Order {order.id} is accepted but not filled yet.")
                    continue

                filled_at = updated_order.filled_at

                # Timestamp handling
                if isinstance(filled_at, str):
                    dt = datetime.datetime.fromisoformat(filled_at.replace("Z", "+00:00"))
                elif isinstance(filled_at, datetime.datetime):
                    dt = filled_at
                else:
                    dt = datetime.datetime.now(datetime.timezone.utc)

                dt = dt.astimezone(datetime.timezone.utc)
                formatted_timestamp = dt.strftime('%Y-%m-%dT%H:%M:%SZ')

                # Log executed trade
                cursor.execute("""
                    INSERT INTO executed_trades (symbol, timestamp, price, qty, side)
                    VALUES (?, ?, ?, ?, ?)
                """, (symbol, formatted_timestamp, float(fill_price), float(filled_qty), 'buy'))
                
                # Final Status Update
                cursor.execute("UPDATE trade_signals SET status = 'EXECUTED' WHERE id = ?", (signal_id,))
                conn.commit()
                
                print(f"💰 Signal {signal_id} EXECUTED and logged! 💰")

            except Exception as e:
                print(f"❌ Error processing signal {signal['id']} with Alpaca: {e}")
                cursor.execute("UPDATE trade_signals SET status = 'FAILED' WHERE id = ?", (signal['id'],))
                conn.commit()

    except Exception as e:
        print(f"Global error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    process_signals()
