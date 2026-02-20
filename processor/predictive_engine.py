import os
import sys
import time
import torch
import pandas as pd
import numpy as np
import sqlite3
from chronos import ChronosPipeline

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection
from shared.config import SYMBOLS
from shared.smart_sleep import get_sleep_seconds

MODEL_SMALL = "amazon/chronos-t5-small"
MODEL_LARGE = "amazon/chronos-t5-large"

def get_device():
    if torch.cuda.is_available():
        print(f"🚀 GPU DETECTED: {torch.cuda.get_device_name(0)}")
        return "cuda"
    print("⚠️ No GPU detected. Using CPU (Slow).")
    return "cpu"

def load_models():
    """Loads both Small and Large Chronos models."""
    print(f"Loading Ensemble: {MODEL_SMALL} + {MODEL_LARGE}...")
    device = get_device()
    dtype = torch.bfloat16 if device == "cuda" else torch.float32

    # Load Small Model
    print(f"   -> Loading {MODEL_SMALL}...")
    pipeline_small = ChronosPipeline.from_pretrained(
        MODEL_SMALL,
        device_map=device,
        torch_dtype=dtype,
    )

    # Load Large Model
    print(f"   -> Loading {MODEL_LARGE}...")
    pipeline_large = ChronosPipeline.from_pretrained(
        MODEL_LARGE,
        device_map=device,
        torch_dtype=dtype,
    )

    return pipeline_small, pipeline_large

def fetch_context_data(conn):
    """
    Fetches the last 64 closing prices for all symbols.
    Returns:
        contexts (list of tensors): List of 1D tensors, one per symbol.
        valid_symbols (list of str): List of symbols that had valid data.
        last_prices (list of float): List of the most recent close price for each valid symbol.
        timestamps (list of str): List of the most recent timestamp for each valid symbol.
    """
    contexts = []
    valid_symbols = []
    last_prices = []
    timestamps = []

    cursor = conn.cursor()

    for symbol in SYMBOLS:
        try:
            # Fetch last 64 candles (5m)
            query = """
                SELECT timestamp, close
                FROM market_data
                WHERE symbol = ? AND timeframe = '5m'
                ORDER BY timestamp DESC
                LIMIT 64
            """
            df = pd.read_sql_query(query, conn, params=(symbol,))

            if df.empty or len(df) < 10:
                continue

            # Sort chronological (ASC)
            df = df.sort_values(by='timestamp', ascending=True)

            # Check for NaNs and fill
            if df['close'].isnull().any():
                df['close'] = df['close'].ffill().bfill()

            if df['close'].isnull().any():
                continue

            # Convert to tensor
            prices = df['close'].values
            context_tensor = torch.tensor(prices, dtype=torch.float32)

            contexts.append(context_tensor)
            valid_symbols.append(symbol)
            last_prices.append(prices[-1])
            timestamps.append(df['timestamp'].iloc[-1])

        except Exception as e:
            print(f"Error fetching context for {symbol}: {e}")
            continue

    return contexts, valid_symbols, last_prices, timestamps

def run_predictions(pipeline_small, pipeline_large):
    conn = get_db_connection()
    try:
        print("🔮 Fetching market context for Ensemble Inference...")
        contexts, symbols, last_prices, timestamps = fetch_context_data(conn)

        if not contexts:
            print("⚠️ No valid context data found. Waiting...")
            return

        print(f"🧠 Running Ensemble on {len(contexts)} symbols...")

        # --- SMALL MODEL INFERENCE ---
        print(f"   Running {MODEL_SMALL}...")
        forecasts_small = pipeline_small.predict(
            contexts, # Positional argument
            prediction_length=6,
            num_samples=20
        )
        # Extract mean of 6th step
        preds_small = torch.median(forecasts_small[:, :, 5], dim=1).values.tolist()

        # --- LARGE MODEL INFERENCE ---
        print(f"   Running {MODEL_LARGE}...")
        forecasts_large = pipeline_large.predict(
            contexts, # Positional argument
            prediction_length=6,
            num_samples=20
        )
        # Extract mean of 6th step
        preds_large = torch.median(forecasts_large[:, :, 5], dim=1).values.tolist()

        results = []
        for i, symbol in enumerate(symbols):
            current_price = last_prices[i]
            timestamp = timestamps[i]

            p_small = preds_small[i]
            p_large = preds_large[i]

            # Ensemble Logic: 0.7 * Large + 0.3 * Small
            ensemble_price = (0.7 * p_large) + (0.3 * p_small)

            if current_price == 0:
                ensemble_pct = 0.0
            else:
                ensemble_pct = ((ensemble_price - current_price) / current_price) * 100.0

            results.append((
                symbol,
                timestamp,
                current_price,
                p_small,
                p_large,
                ensemble_price,
                ensemble_pct
            ))

            if ensemble_pct > 0.4:
                print(f"   -> {symbol}: {current_price:.2f} -> ENS: {ensemble_price:.2f} ({ensemble_pct:+.2f}%) | S: {p_small:.2f} L: {p_large:.2f}")

        # Write to DB
        cursor = conn.cursor()
        cursor.executemany("""
            INSERT OR REPLACE INTO ai_predictions
            (symbol, timestamp, current_price, small_predicted_price, large_predicted_price, ensemble_predicted_price, ensemble_pct_change)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, results)
        conn.commit()
        print(f"✅ Saved {len(results)} ensemble predictions.")
        print("[BRAIN] RTX 5050 inference cycle complete.")

    except Exception as e:
        print(f"❌ Ensemble Engine Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    p_small, p_large = load_models()

    while True:
        run_predictions(p_small, p_large)

        sleep_sec = get_sleep_seconds()
        print(f"💤 Sleeping for {sleep_sec} seconds...")
        time.sleep(sleep_sec)
