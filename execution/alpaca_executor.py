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
MAX_FILL_RETRIES = 10
FILL_POLL_INTERVAL = 1.0

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

def process_signals(api):
    """
    Reads SIZED signals, submits Market Buy, waits for fill, submits Trailing Stop Sell.
    """
    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        # Get SIZED signals with ATR and Signal Type
        cursor.execute("""
            SELECT id, symbol, size, signal_type, atr
            FROM trade_signals
            WHERE status = 'SIZED'
        """)
        signals = cursor.fetchall()

        if not signals:
            return

        print(f"🚀 Processing {len(signals)} new signals...")

        for signal in signals:
            signal_id = signal['id']
            symbol = signal['symbol']
            qty = int(signal['size'])
            signal_type = signal['signal_type']
            atr = signal['atr']

            try:
                # 1. Submit Market Buy Order
                print(f"   -> Submitting BUY {qty} {symbol} (Market)...")
                try:
                    buy_order = api.submit_order(
                        symbol=symbol,
                        qty=qty,
                        side='buy',
                        type='market',
                        time_in_force='gtc'
                    )
                except Exception as e:
                    print(f"❌ Failed to submit BUY order for {symbol}: {e}")
                    cursor.execute("UPDATE trade_signals SET status = 'FAILED' WHERE id = ?", (signal_id,))
                    conn.commit()
                    continue
                
                # Update status to SUBMITTED to track progress
                cursor.execute("UPDATE trade_signals SET status = 'SUBMITTED', order_id = ? WHERE id = ?", (buy_order.id, signal_id))
                conn.commit()

                # 2. Wait for Fill
                filled_qty = 0
                avg_price = 0.0
                filled = False

                for attempt in range(MAX_FILL_RETRIES):
                    try:
                        # Poll order status
                        order_status = api.get_order(buy_order.id)
                        if order_status.status == 'filled':
                            filled_qty = float(order_status.filled_qty)
                            avg_price = float(order_status.filled_avg_price) if order_status.filled_avg_price else 0.0
                            filled = True
                            break
                        elif order_status.status in ['canceled', 'rejected', 'expired']:
                            print(f"❌ Order {buy_order.id} was {order_status.status}.")
                            cursor.execute("UPDATE trade_signals SET status = 'FAILED' WHERE id = ?", (signal_id,))
                            conn.commit()
                            break
                    except Exception as e:
                        print(f"⚠️ Error polling order {buy_order.id}: {e}")

                    time.sleep(FILL_POLL_INTERVAL)

                if filled and filled_qty > 0:
                    print(f"✅ Order filled: {filled_qty} shares @ {avg_price:.2f}")

                    # Log execution
                    ts_iso = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                    log_trade(conn, symbol, avg_price, filled_qty, 'buy', ts_iso, signal_type)

                    # 3. Submit Trailing Stop Sell
                    # Determine Trailing Stop Parameter (Price vs Percent)
                    trail_price = None
                    trail_percent = None

                    if atr is not None and signal_type:
                        multiplier = 2.0 # Default fallback within ATR logic
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

                    print(f"🛑 Submitting Trailing Stop for {symbol}...")
                    try:
                        # Construct arguments
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

                        # Update signal to EXECUTED
                        cursor.execute("UPDATE trade_signals SET status = 'EXECUTED' WHERE id = ?", (signal_id,))
                        conn.commit()

                    except Exception as stop_error:
                        print(f"❌ FAILED to submit Trailing Stop: {stop_error}")
                        cursor.execute("UPDATE trade_signals SET status = 'EXECUTED_NO_STOP' WHERE id = ?", (signal_id,))
                        conn.commit()

                elif not filled:
                    print(f"⚠️ Order {buy_order.id} timed out waiting for fill.")
                    # Cancel the open order to avoid stale fills
                    try:
                        api.cancel_order(buy_order.id)
                    except:
                        pass
                    cursor.execute("UPDATE trade_signals SET status = 'TIMEOUT' WHERE id = ?", (signal_id,))
                    conn.commit()

            except Exception as e:
                print(f"❌ Execution Error for {symbol}: {e}")
                cursor.execute("UPDATE trade_signals SET status = 'FAILED' WHERE id = ?", (signal_id,))
                conn.commit()

    except Exception as e:
        print(f"Alpaca Executor Loop Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    # print("Starting Alpaca Executor...")
    api = get_alpaca_api()
    if api:
        print("✅ Alpaca Executor Online.")
        while True:
            process_signals(api)
            time.sleep(5)
    else:
        print("❌ Alpaca API connection failed. Exiting.")
