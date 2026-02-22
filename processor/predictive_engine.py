"""
Service: Swarm Intelligence Brain (Predictive Engine)
Role: Runs an ensemble of Chronos T5 models (Small + Large) to forecast future price movements.
Dependencies: torch, pandas, chronos, shared.db_utils
"""
import os
import sys
import torch
import pandas as pd
from chronos import ChronosPipeline

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection, log_system_event
from shared.config import SYMBOLS
from shared.smart_sleep import get_sleep_time_to_next_candle, smart_sleep

MODEL_SMALL = "amazon/chronos-t5-small"
MODEL_LARGE = "amazon/chronos-t5-large"


def get_device():
    """Detects and returns the optimal computation device (CUDA/CPU)."""
    if torch.cuda.is_available():
        print(f"🚀 GPU DETECTED: {torch.cuda.get_device_name(0)}")
        return "cuda"
    print("⚠️ No GPU detected. Using CPU (Slow).")
    return "cpu"


def load_models():
    """
    Loads both Small and Large Chronos models for the ensemble.

    Returns:
        tuple: (pipeline_small, pipeline_large)
    """
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
    Fetches the last 64 closing prices for all tracked symbols to build the context tensor.

    Args:
        conn: Database connection object.

    Returns:
        tuple: (contexts, valid_symbols, last_prices, timestamps)
            - contexts: List of 1D tensors (float32), one per symbol.
            - valid_symbols: List of symbol strings.
            - last_prices: List of the most recent close prices (float).
            - timestamps: List of the most recent timestamps (str).
    """
    contexts = []
    valid_symbols = []
    last_prices = []
    timestamps = []

    for symbol in SYMBOLS:
        try:
            # Fetch last 64 candles (5m timeframe)
            # We need a fixed context window for the transformer model.
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

            # Sort chronologically (ASC) because the model expects time-series data
            df = df.sort_values(by='timestamp', ascending=True)

            # Data Cleaning: Forward Fill then Backward Fill
            # Transformer models cannot handle NaNs. We use forward fill to propagate
            # the last known price, then backward fill for any initial gaps.
            if df['close'].isnull().any():
                df['close'] = df['close'].ffill().bfill()

            if df['close'].isnull().any():
                continue

            # Convert to Tensor
            # The Chronos model expects a 1D tensor of float values as input context.
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
    """
    Executes the inference cycle:
    1. Fetches market context.
    2. Runs inference on both Small and Large models.
    3. Calculates an Ensemble prediction (Weighted Average).
    4. Saves results to the database.
    """
    conn = get_db_connection()
    if not conn:
        return

    try:
        print("🔮 Fetching market context for Ensemble Inference...")
        contexts, symbols, last_prices, timestamps = fetch_context_data(conn)

        if not contexts:
            print("⚠️ No valid context data found. Waiting...")
            return

        print(f"🧠 Running Ensemble on {len(contexts)} symbols...")

        # --- SMALL MODEL INFERENCE ---
        # Chronos-Small is faster but less nuanced. Used for "gut check".
        print(f"   Running {MODEL_SMALL}...")
        forecasts_small = pipeline_small.predict(
            contexts,
            prediction_length=6,  # Forecast 6 steps (30 mins at 5m intervals)
            num_samples=20        # Monte Carlo samples for probabilistic forecast
        )
        # Extract mean of the 6th step (T+30)
        # We use the median of the samples to be robust against outliers.
        preds_small = torch.median(forecasts_small[:, :, 5], dim=1).values.tolist()

        # --- LARGE MODEL INFERENCE ---
        # Chronos-Large has deeper context understanding but is slower.
        print(f"   Running {MODEL_LARGE}...")
        forecasts_large = pipeline_large.predict(
            contexts,
            prediction_length=6,
            num_samples=20
        )
        preds_large = torch.median(forecasts_large[:, :, 5], dim=1).values.tolist()

        results = []
        for i, symbol in enumerate(symbols):
            current_price = last_prices[i]
            timestamp = timestamps[i]

            p_small = preds_small[i]
            p_large = preds_large[i]

            # --- ENSEMBLE LOGIC ---
            # Weighted Average: 70% Large Model, 30% Small Model.
            # Rationale: The Large model generally provides higher accuracy for
            # complex patterns, while the Small model adds a layer of variance reduction.
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

            # Only log significant predictions to keep console clean
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
        log_system_event("PredictiveEngine", "ERROR", f"Inference Error: {str(e)}")
    finally:
        conn.close()


if __name__ == "__main__":
    p_small_model, p_large_model = load_models()

    while True:
        run_predictions(p_small_model, p_large_model)

        # AI runs at :20 (20s offset, same as TA)
        sleep_duration = get_sleep_time_to_next_candle(offset_seconds=20)
        print(f"💤 Sleeping for {sleep_duration} seconds...")
        smart_sleep(sleep_duration)
