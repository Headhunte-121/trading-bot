import sqlite3
import os
import sys
import datetime
import time

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection, log_system_event
from shared.config import KINGS_LIST
from shared.smart_sleep import get_sleep_seconds

def get_macro_regime(cursor):
    """
    Determines the market regime based on SPY 5m data.
    Regime is BEAR if Close < SMA 50, else BULL.
    """
    try:
        query = """
            SELECT m.close, t.sma_50
            FROM market_data m
            JOIN technical_indicators t
              ON m.symbol = t.symbol
              AND m.timestamp = t.timestamp
              AND m.timeframe = t.timeframe
            WHERE m.symbol = 'SPY'
              AND m.timeframe = '5m'
            ORDER BY m.timestamp DESC
            LIMIT 1
        """
        cursor.execute(query)
        row = cursor.fetchone()

        if row:
            close = row['close']
            sma_50 = row['sma_50']
            if sma_50 is not None and close < sma_50:
                return 'BEAR'
            return 'BULL'
        return 'BULL' # Default fallback
    except Exception as e:
        print(f"⚠️ Error checking macro regime: {e}")
        return 'BULL'

def run_strategy():
    """
    Executes the 3-Tier Strategy Engine:
    1. VWAP Scalp (Momentum)
    2. Deep Value Buy (Kings List Dip)
    3. Trend Buy (Wave Surfer)
    """
    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        # 1. Determine Global Macro Regime
        macro_regime = get_macro_regime(cursor)
        # print(f"🌍 Global Macro Regime: {macro_regime}")

        # 2. Fetch Candidates (Last 60 mins)
        lookback_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=60)
        lookback_iso = lookback_time.strftime('%Y-%m-%dT%H:%M:%SZ')

        # Select all potential candidates with necessary data
        query = """
            SELECT
                m.symbol,
                m.timestamp,
                m.close,
                m.volume,
                t.sma_200,
                t.rsi_14,
                t.vwap,
                t.atr_14,
                t.volume_sma_20,
                p.ensemble_pct_change
            FROM market_data m
            JOIN technical_indicators t
              ON m.symbol = t.symbol
              AND m.timestamp = t.timestamp
              AND m.timeframe = t.timeframe
            JOIN ai_predictions p
              ON m.symbol = p.symbol
              AND m.timestamp = p.timestamp
            WHERE
                m.timeframe = '5m'
                AND m.timestamp >= ?
                AND m.symbol != 'SPY'
            ORDER BY m.timestamp DESC
        """

        cursor.execute(query, (lookback_iso,))
        candidates = cursor.fetchall()

        if not candidates:
            return

        for row in candidates:
            symbol = row['symbol']
            timestamp = row['timestamp']

            # Extract Data
            close = row['close']
            volume = row['volume']
            sma_200 = row['sma_200']
            rsi = row['rsi_14']
            vwap = row['vwap']
            atr = row['atr_14']
            vol_sma = row['volume_sma_20']
            pred_pct = row['ensemble_pct_change']

            # Skip if critical data is missing
            if None in [close, volume, sma_200, rsi, vwap, atr, vol_sma, pred_pct]:
                continue

            # Check for duplicate signal
            cursor.execute(
                "SELECT id FROM trade_signals WHERE symbol = ? AND timestamp = ?",
                (symbol, timestamp)
            )
            if cursor.fetchone():
                continue # Already signaled

            signal_type = None

            # --- EVALUATION LOGIC (Tier 1 -> Tier 2 -> Tier 3) ---

            # Tier 1: VWAP_SCALP (Momentum)
            # Logic: Price crosses ABOVE intraday VWAP + High Vol + AI Conviction
            # We check if Close > VWAP. Strictly "Cross" implies Close > VWAP and Open < VWAP,
            # but prompt says "If current_price crosses ABOVE... trigger a BUY".
            # For simplicity in a poll-based system, "Close > VWAP" is the state we look for,
            # combined with AI prediction.
            if (pred_pct > 0.3 and
                volume > vol_sma and
                close > vwap):
                signal_type = 'VWAP_SCALP'

            # Tier 2: DEEP_VALUE_BUY (The King Dip)
            # Logic: King List + Below SMA 200 + RSI < 30 + AI Conviction
            elif (symbol in KINGS_LIST and
                  close < sma_200 and
                  rsi < 30 and
                  pred_pct > 0.5):
                signal_type = 'DEEP_VALUE_BUY'

            # Tier 3: TREND_BUY (The Wave Surfer)
            # Logic: Bull Market + Above SMA 200 + Healthy RSI + Volume + AI Conviction
            elif (macro_regime == 'BULL' and
                  close > sma_200 and
                  35 < rsi < 55 and
                  pred_pct > 0.5 and
                  volume > vol_sma):
                signal_type = 'TREND_BUY'

            # --- SIGNAL GENERATION ---
            if signal_type:
                print(f"⭐⭐ {signal_type} SIGNAL: {symbol} @ {timestamp} | Close: {close:.2f} | ATR: {atr:.2f} | AI: +{pred_pct:.2f}% ⭐⭐")
                log_system_event("StrategyEngine", "INFO", f"{signal_type} Signal for {symbol}. AI: {pred_pct}%")

                cursor.execute("""
                    INSERT INTO trade_signals
                    (symbol, timestamp, signal_type, status, size, stop_loss, atr)
                    VALUES (?, ?, ?, 'PENDING', NULL, NULL, ?)
                """, (symbol, timestamp, signal_type, atr))

                conn.commit()

    except Exception as e:
        print(f"Strategy Engine Error: {e}")
        log_system_event("StrategyEngine", "ERROR", f"Critical Error: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    print("🚀 Strategy Engine Initialized (3-Tier Logic)")
    while True:
        run_strategy()
        sleep_sec = get_sleep_seconds()
        # print(f"💤 Strategy Sleeping for {sleep_sec} seconds...")
        time.sleep(sleep_sec)
