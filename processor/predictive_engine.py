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

MODEL_NAME = "amazon/chronos-t5-small"

def get_device():
    if torch.cuda.is_available():
        print(f"🚀 GPU DETECTED: {torch.cuda.get_device_name(0)}")
        return "cuda"
    print("⚠️ No GPU detected. Using CPU (Slow).")
    return "cpu"

def load_model():
    print(f"Loading {MODEL_NAME}...")
    device = get_device()
    # optimized dtype for inference
    dtype = torch.bfloat16 if device == "cuda" else torch.float32

    pipeline = ChronosPipeline.from_pretrained(
        MODEL_NAME,
        device_map=device,
        torch_dtype=dtype,
    )
    return pipeline

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

            if df.empty or len(df) < 10: # Require at least 10 data points
                continue

            # Sort chronological (ASC)
            df = df.sort_values(by='timestamp', ascending=True)

            # Check for NaNs and fill
            # Critical Step: Forward Fill then Backward Fill
            if df['close'].isnull().any():
                df['close'] = df['close'].ffill().bfill()

            # If still NaN (e.g. all NaN), skip
            if df['close'].isnull().any():
                continue

            # Convert to tensor
            # value sequence
            prices = df['close'].values
            context_tensor = torch.tensor(prices, dtype=torch.float32)

            contexts.append(context_tensor)
            valid_symbols.append(symbol)
            last_prices.append(prices[-1])
            timestamps.append(df['timestamp'].iloc[-1]) # Keep track of "current" time

        except Exception as e:
            print(f"Error fetching context for {symbol}: {e}")
            continue

    return contexts, valid_symbols, last_prices, timestamps

def run_predictions(pipeline):
    conn = get_db_connection()
    try:
        print("🔮 Fetching market context for batch inference...")
        contexts, symbols, last_prices, timestamps = fetch_context_data(conn)

        if not contexts:
            print("⚠️ No valid context data found. Waiting...")
            return

        print(f"🧠 Running Chronos Inference on {len(contexts)} symbols...")

        # Batch Predict
        # pipeline.predict accepts a list of tensors
        forecasts = pipeline.predict(
            contexts,
            prediction_length=6, # 6 steps ahead (30 mins)
            num_samples=20       # 20 sample paths
        )

        # Forecasts is a tensor of shape (batch_size, num_samples, prediction_length)
        # We want the MEAN of the 6th step (index 5) across samples
        # shape: [batch, 20, 6]

        # Extract 6th step predictions
        # slice: [:, :, 5] -> (batch, 20)
        step_6_preds = forecasts[:, :, 5]

        # Calculate mean or median
        # median is robust to outliers
        predicted_prices = torch.median(step_6_preds, dim=1).values.tolist()

        results = []
        for i, symbol in enumerate(symbols):
            current_price = last_prices[i]
            predicted_price = predicted_prices[i]
            timestamp = timestamps[i]

            # Calculate % change
            if current_price == 0:
                pct_change = 0.0
            else:
                pct_change = ((predicted_price - current_price) / current_price) * 100.0

            results.append((
                symbol,
                timestamp,
                current_price,
                predicted_price,
                pct_change
            ))

            print(f"   -> {symbol}: {current_price:.2f} -> {predicted_price:.2f} ({pct_change:+.2f}%)")

        # Write to DB
        cursor = conn.cursor()
        cursor.executemany("""
            INSERT OR REPLACE INTO ai_predictions
            (symbol, timestamp, current_price, predicted_price, predicted_pct_change)
            VALUES (?, ?, ?, ?, ?)
        """, results)
        conn.commit()
        print(f"✅ Saved {len(results)} predictions.")

    except Exception as e:
        print(f"❌ Prediction Engine Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    pipeline = load_model()

    while True:
        run_predictions(pipeline)

        sleep_sec = get_sleep_seconds()
        print(f"💤 Sleeping for {sleep_sec} seconds...")
        time.sleep(sleep_sec)
