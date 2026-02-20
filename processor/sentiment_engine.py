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
        batch_size=8, # Reduced batch size to prevent OOM
        max_new_tokens=128,
        pad_token_id=tokenizer.eos_token_id
    )
    
    return text_generator

def build_prompt(headline, symbol):
    return f"""<|begin_of_text|><|start_header_id|>system<|end_header_id|>
Act as a high-frequency quant analyst. You must output a valid JSON object.
Your task is to analyze the headline for the ticker {symbol}.

RULES:
1. SUBJECT CHECK: If the headline mentions a competitor or different company and NOT {symbol}, relevance is 0.0.
2. REASONING: You must explain your logic in one short sentence starting with "This headline..."
3. SCORING:
   - relevance: 0.0 (irrelevant) to 1.0 (direct impact)
   - sentiment: -1.0 (very negative) to 1.0 (very positive)
   - urgency: 0 (noise) to 5 (immediate action)
4. BE CRITICAL: If the news is bad for {symbol} (e.g., lawsuit, earnings miss, competitor gain), you MUST score sentiment between -1.0 and -0.1.

Output strict JSON only. No conversational filler.
Format: {{"reasoning": "...", "relevance": <float>, "sentiment": <float>, "urgency": <int>}}
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
            time.sleep(5)
            continue
            
        conn.row_factory = sqlite3.Row 

        try:
            cursor = conn.cursor()
            # Fetch up to 160 un-scored headlines
            # Changed to check relevance IS NULL to catch new items or reprocessing needs
            cursor.execute("SELECT id, symbol, headline FROM raw_news WHERE relevance IS NULL LIMIT 160")
            rows = cursor.fetchall()

            if rows:
                print(f"🧠 AI: Found {len(rows)} headlines. Analyzing in batches...")

                # Chunk into batches of 8 (reduced from 16)
                batch_size = 8
                total_processed = 0
                
                for i in range(0, len(rows), batch_size):
                    batch_rows = rows[i:i+batch_size]
                    prompts = [build_prompt(row['headline'], row['symbol']) for row in batch_rows]
                    
                    results = []
                    try:
                        # Run inference
                        # Using do_sample=True with low temp to avoid mode collapse/loops
                        # Increased temp to 0.3 to allow for more negativity variance
                        results = llm(prompts, do_sample=True, temperature=0.3, top_p=0.9)
                    except Exception as e:
                        print(f"❌ Inference Error for batch: {e}")
                        results = []

                    updates = []

                    # Iterate over batch_rows, check corresponding result
                    for idx, row in enumerate(batch_rows):
                        generated_text = ""
                        data = None

                        # Check if result exists for this row
                        if idx < len(results):
                            res = results[idx]
                            # Handle different pipeline output formats (list or dict)
                            if isinstance(res, list) and len(res) > 0:
                                generated_text = res[0].get('generated_text', '')
                            elif isinstance(res, dict):
                                generated_text = res.get('generated_text', '')

                            response_part = generated_text.split("assistant<|end_header_id|>")[-1]
                            data = parse_llm_output(response_part)
                        else:
                            print(f"⚠️ Missing result for {row['symbol']} (ID: {row['id']}). Marking as processed (0.0).")

                        sentiment = 0.0
                        relevance = 0.0
                        urgency = 0
                        reasoning = ""

                        if data:
                            try:
                                sentiment = float(data.get("sentiment", 0.0))
                                relevance = float(data.get("relevance", 0.0))
                                urgency = int(data.get("urgency", 0))
                                reasoning = data.get("reasoning", "")

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
                            print(f"   --> {row['symbol']}: {final_score:+.2f} (Rel: {relevance:.2f}) | {reasoning[:60]}...")

                    # Update DB for this batch
                    update_db_batch(conn, updates)
                    total_processed += len(updates)

                print(f"✅ Finished cycle. Processed {total_processed} items.")

                if total_processed > 0:
                    # If we processed items successfully, check immediately for more backlog
                    # BUT verify if updates stuck. If next fetch returns same rows, we might loop.
                    # Assuming update_db_batch worked.
                    print(f"🚀 Backlog detected. Continuing immediately...")
                    conn.close()
                    continue
            
        except Exception as e:
            print(f"Loop Error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # We already closed it above if we continued, but safe to close again (it's idempotent or check if open)
            try:
                conn.close()
            except:
                pass

        # Sleep logic: 15 minutes (900s) as requested
        print("💤 Queue empty. Sleeping 15 minutes...")
        time.sleep(900)

if __name__ == "__main__":
    main()
