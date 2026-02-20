import os
import time
import sqlite3
import torch
import sys
import re
import json
import logging
import warnings
from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig, pipeline, logging as hf_logging

# --- SILENCE ALL SPAM ---
hf_logging.set_verbosity_error()
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
warnings.filterwarnings("ignore", category=UserWarning)

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection
from shared.smart_sleep import get_market_status

MODEL_NAME = "unsloth/llama-3-8b-Instruct-bnb-4bit"

def get_device():
    if torch.cuda.is_available():
        device_name = torch.cuda.get_device_name(0)
        print(f"\n🚀 GPU DETECTED: Using {device_name} with 4-bit Quantization! 🚀\n")
        return "cuda"
    print("\n⚠️ No GPU detected. This model will be VERY slow on CPU. ⚠️\n")
    return "cpu"

def load_llm():
    print(f"Loading {MODEL_NAME} into memory...")
    
    bnb_config = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_use_double_quant=True,
        bnb_4bit_quant_type="nf4",
        bnb_4bit_compute_dtype=torch.float16
    )

    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    # Important for batch generation
    tokenizer.pad_token_id = tokenizer.eos_token_id
    tokenizer.padding_side = "left"
    
    model = AutoModelForCausalLM.from_pretrained(
        MODEL_NAME,
        quantization_config=bnb_config,
        device_map="auto",
        attn_implementation="sdpa", # Flash Attention 2
        local_files_only=False
    )
    
    text_generator = pipeline(
        "text-generation",
        model=model,
        tokenizer=tokenizer,
        batch_size=16, # Enable batching
        max_new_tokens=128,
        pad_token_id=tokenizer.eos_token_id
    )
    
    return text_generator

def build_prompt(headline, symbol):
    return f"""<|begin_of_text|><|start_header_id|>system<|end_header_id|>
Act as a high-frequency quant analyst. Your output must be a JSON object. For the given headline and ticker, provide:
relevance: 0.0 to 1.0 (Direct impact vs. noise).
sentiment: -1.0 to 1.0 (Calculated using decimal precision).
urgency: 0 to 5 (How fast must a trader react?).
Do NOT use rounded numbers like 0.5 or 0.8. Use the full decimal range based on the strength of the verbs used in the headline.

Return ONLY a JSON object: {{"relevance": <float>, "sentiment": <float>, "urgency": <int>}}
<|eot_id|><|start_header_id|>user<|end_header_id|>
Headline: "{headline}"
Ticker: {symbol}
<|eot_id|><|start_header_id|>assistant<|end_header_id|>"""

def parse_llm_output(output_text):
    try:
        # Extract JSON part - look for { ... }
        match = re.search(r"\{.*\}", output_text, re.DOTALL)
        if match:
            json_str = match.group(0)
            data = json.loads(json_str)
            return data
        else:
            return None
    except Exception:
        return None

def update_db_batch(conn, updates):
    """
    updates: list of tuples (id, sentiment_score, relevance, urgency)
    """
    if not updates:
        return
        
    query = "UPDATE raw_news SET sentiment_score = ?, relevance = ?, urgency = ? WHERE id = ?"
    try:
        # Convert updates to format (score, rel, urg, id)
        params = [(u[1], u[2], u[3], u[0]) for u in updates]
        conn.executemany(query, params)
        conn.commit()
    except sqlite3.OperationalError as e:
        print(f"DB Error: {e}")

def main():
    device = get_device()
    llm = load_llm()
    
    print("✅ Llama-3 AI Engine is Active. Monitoring for new news...")

    while True:
        conn = get_db_connection()
        if not conn:
            time.sleep(60)
            continue
            
        conn.row_factory = sqlite3.Row 

        try:
            cursor = conn.cursor()
            # Fetch up to 160 un-scored headlines
            cursor.execute("SELECT id, symbol, headline FROM raw_news WHERE sentiment_score IS NULL LIMIT 160")
            rows = cursor.fetchall()

            if rows:
                print(f"🧠 AI: Found {len(rows)} headlines. Analyzing in batches...")

                # Chunk into batches of 16
                batch_size = 16
                total_processed = 0
                
                for i in range(0, len(rows), batch_size):
                    batch_rows = rows[i:i+batch_size]
                    prompts = [build_prompt(row['headline'], row['symbol']) for row in batch_rows]
                    
                    # Run inference
                    # pipeline returns a list of results, each result is a list of generated dicts (usually 1)
                    results = llm(prompts, do_sample=False, temperature=None, top_p=None)

                    updates = []

                    for row, res in zip(batch_rows, results):
                        # res is like [{'generated_text': '...'}]
                        generated_text = res[0]['generated_text']
                        # The generated text includes the prompt. We need to parse the JSON at the end.
                        # The prompt ends with <|end_header_id|>assistant<|end_header_id|>
                        # But our regex in parse_llm_output finds the first { after that hopefully, or we can split.
                        # Since the prompt contains "JSON object: {...}", we need to be careful not to match that.
                        # We should split by "assistant<|end_header_id|>"

                        response_part = generated_text.split("assistant<|end_header_id|>")[-1]
                        data = parse_llm_output(response_part)

                        sentiment = 0.0
                        relevance = 0.0
                        urgency = 0

                        if data:
                            try:
                                sentiment = float(data.get("sentiment", 0.0))
                                relevance = float(data.get("relevance", 0.0))
                                urgency = int(data.get("urgency", 0))

                                # Clamp values
                                sentiment = max(-1.0, min(1.0, sentiment))
                                relevance = max(0.0, min(1.0, relevance))
                                urgency = max(0, min(5, urgency))
                            except (ValueError, TypeError):
                                pass

                        final_score = sentiment * relevance
                        updates.append((row['id'], final_score, relevance, urgency))

                        # Log high conviction
                        if abs(final_score) > 0.4:
                            print(f"   --> {row['symbol']}: {final_score:+.2f} (Rel: {relevance:.2f}, Urg: {urgency}) | {row['headline'][:50]}...")

                    # Update DB for this batch
                    update_db_batch(conn, updates)
                    total_processed += len(updates)

                print(f"✅ Finished cycle. Processed {total_processed} items.")

                if total_processed > 0:
                    print(f"🚀 Backlog detected. Continuing immediately...")
                    conn.close()
                    continue
            
        except Exception as e:
            print(f"Loop Error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # We already closed it above if we continued, but safe to close again (it's idempotent or check if open)
            # Actually sqlite3 connection objects aren't always idempotent on close, but let's be safe.
            # A better pattern is to close it in the finally block only if it wasn't closed.
            # However, the `continue` jumps to the top of the loop, skipping the finally block?
            # No, `continue` in a try/finally block executes the finally block before jumping.
            # So we can just rely on the finally block.
            try:
                conn.close()
            except:
                pass

        # Smart Sleep Logic
        status = get_market_status()
        print(f"{status['status_message']}")
        time.sleep(status['sleep_seconds'])

if __name__ == "__main__":
    main()
