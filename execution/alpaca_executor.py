import os
import time
import sqlite3
import datetime
import sys
from alpaca_trade_api.rest import REST

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection
from shared.smart_sleep import get_sleep_seconds

TRAIL_PERCENT_DEFAULT = 2.0

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

def log_trade(conn, symbol, price, qty, side, timestamp_str, signal_type):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO executed_trades (symbol, timestamp, price, qty, side, signal_type)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (symbol, timestamp_str, float(price), float(qty), side, signal_type))
        conn.commit()
        print(f"✅ Logged trade: {side.upper()} {qty} {symbol} @ {price} ({signal_type})")
    except Exception as e:
        print(f"Failed to log trade: {e}")

def process_sized_signals(api, conn):
    """
    Step 1: Query 'SIZED' signals, submit Market Buy, and update to 'SUBMITTED'.
    Does NOT wait for fill.
    """
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

        print(f"🚀 Processing {len(signals)} new SIZED signals...")

        for signal in signals:
            signal_id = signal['id']
            symbol = signal['symbol']
            qty = int(signal['size'])

            try:
                print(f"   -> Submitting BUY {qty} {symbol} (Market)...")
                buy_order = api.submit_order(
                    symbol=symbol,
                    qty=qty,
                    side='buy',
                    type='market',
                    time_in_force='gtc'
                )
                
                # CRITICAL: Do NOT wait for fill.
                # Update status to SUBMITTED and save order_id
                cursor.execute("UPDATE trade_signals SET status = 'SUBMITTED', order_id = ? WHERE id = ?", (buy_order.id, signal_id))
                conn.commit()
                print(f"   -> Signal {signal_id} ({symbol}) moved to SUBMITTED. Order ID: {buy_order.id}")

            except Exception as e:
                print(f"❌ Failed to submit BUY order for {symbol}: {e}")
                cursor.execute("UPDATE trade_signals SET status = 'FAILED' WHERE id = ?", (signal_id,))
                conn.commit()

    except Exception as e:
        print(f"Error in process_sized_signals: {e}")

def process_submitted_signals(api, conn):
    """
    Step 2: Monitor 'SUBMITTED' signals.
    - If filled: Submit Trailing Stop, log trade, update to 'EXECUTED'.
    - If canceled/rejected/expired: Update to 'FAILED'.
    - If new/accepted/partially_filled: Do nothing.
    """
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
                print(f"⚠️ Signal {signal_id} ({symbol}) is SUBMITTED but has no order_id. Marking FAILED.")
                cursor.execute("UPDATE trade_signals SET status = 'FAILED' WHERE id = ?", (signal_id,))
                conn.commit()
                continue

            try:
                order_status = api.get_order(order_id)
                status = order_status.status

                if status == 'filled':
                    filled_qty = float(order_status.filled_qty)
                    avg_price = float(order_status.filled_avg_price) if order_status.filled_avg_price else 0.0

                    print(f"✅ Order {order_id} ({symbol}) FILLED: {filled_qty} @ {avg_price:.2f}")

                    # Log execution
                    ts_iso = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                    log_trade(conn, symbol, avg_price, filled_qty, 'buy', ts_iso, signal_type)

                    # Submit Trailing Stop
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
                        print(f"🛑 Dynamic Stop ({signal_type}): {multiplier}x ATR ({atr}) = ${trail_price}")
                    else:
                        trail_percent = TRAIL_PERCENT_DEFAULT
                        print(f"🛑 Fallback Stop: {trail_percent}% (ATR Missing)")

                    try:
                        print(f"🛑 Submitting Trailing Stop for {symbol}...")
                        order_args = {
                            "symbol": symbol,
                            "qty": filled_qty,
                            "side": "sell",
                            "type": "trailing_stop",
                            "time_in_force": "gtc"
                        }
                        if trail_price:
                            order_args["trail_price"] = trail_price
                        else:
                            order_args["trail_percent"] = trail_percent

                        stop_order = api.submit_order(**order_args)
                        print(f"✅ Trailing Stop submitted: {stop_order.id}")

                        cursor.execute("UPDATE trade_signals SET status = 'EXECUTED' WHERE id = ?", (signal_id,))
                        conn.commit()

                    except Exception as stop_error:
                        print(f"❌ FAILED to submit Trailing Stop for {symbol}: {stop_error}")
                        # Mark as EXECUTED_NO_STOP so we know we bought it but have no protection
                        cursor.execute("UPDATE trade_signals SET status = 'EXECUTED_NO_STOP' WHERE id = ?", (signal_id,))
                        conn.commit()

                elif status in ['canceled', 'rejected', 'expired']:
                    print(f"❌ Order {order_id} ({symbol}) was {status}. Marking signal FAILED.")
                    cursor.execute("UPDATE trade_signals SET status = 'FAILED' WHERE id = ?", (signal_id,))
                    conn.commit()

                elif status in ['new', 'accepted', 'partially_filled', 'calculated', 'pending_new']:
                    # Do nothing, wait for next loop
                    pass

                else:
                    print(f"ℹ️ Order {order_id} ({symbol}) status: {status}")

            except Exception as e:
                print(f"⚠️ Error checking order {order_id} for {symbol}: {e}")

    except Exception as e:
        print(f"Error in process_submitted_signals: {e}")

def run_executor():
    api = get_alpaca_api()
    if not api:
        print("❌ Alpaca API connection failed. Exiting.")
        return

    print("✅ Alpaca Executor Online (Async Mode).")

    while True:
        try:
            conn = get_db_connection()
            conn.row_factory = sqlite3.Row

            process_sized_signals(api, conn)
            process_submitted_signals(api, conn)

            conn.close()
        except Exception as e:
            print(f"Main Loop Error: {e}")

        time.sleep(5)

if __name__ == "__main__":
    run_executor()
