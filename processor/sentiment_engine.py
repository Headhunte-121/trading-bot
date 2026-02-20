import os
import time
import sqlite3
import torch
import sys
import re
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
    
    model = AutoModelForCausalLM.from_pretrained(
        MODEL_NAME,
        quantization_config=bnb_config,
        device_map="auto",
        local_files_only=False
    )
    
    # FIX: We add model_kwargs to tell the engine to stop worrying about token conflicts
    text_generator = pipeline(
        "text-generation",
        model=model,
        tokenizer=tokenizer,
        max_new_tokens=64,
        pad_token_id=tokenizer.eos_token_id,
        model_kwargs={"max_length": 8192} 
    )
    
    return text_generator
    
def analyze_headline(headline, symbol, llm_pipeline):
    prompt = f"""<|begin_of_text|><|start_header_id|>system<|end_header_id|>
    You are a conservative Wall Street Analyst. 
    Task 1: Is this headline directly relevant to {symbol}?
    Task 2: Sentiment score from -1.0 to 1.0. 
    
    SCORING RUBRIC:
    - 0.1 to 0.3: Minor news, general updates, or slight positive/negative opinions.
    - 0.4 to 0.7: Significant events (New products, Analyst upgrades, strong earnings).
    - 0.8 to 1.0: Reserved ONLY for catastrophic or game-changing news.
    - If it's a "Top 10 stocks to buy" list, give it a 0.1. Don't overreact.

    Format:
    <|eot_id|><|start_header_id|>user<|end_header_id|>
    Headline: "{headline}"
    Ticker: {symbol}
    <|eot_id|><|start_header_id|>assistant<|end_header_id|>"""

    try:
        sequences = llm_pipeline(prompt, do_sample=False, temperature=None, top_p=None)
        output = sequences
        
        response = output.split("<|start_header_id|>assistant<|end_header_id|>").strip()
        
        if "" in response.upper():
            return 0.0 
            
        match = re.search(r"SCORE:\s?(-?\d+\.?\d*)", response, re.IGNORECASE)
        if match:
            score = float(match.group(1))
            return max(-1.0, min(1.0, score))
            
        return 0.0

    except Exception:
        return 0.0
        
def update_sentiment_score(conn, row_id, score):
    query = "UPDATE raw_news SET sentiment_score = ? WHERE id = ?"
    try:
        conn.execute(query, (score, row_id))
        conn.commit()
        return True
    except sqlite3.OperationalError:
        return False

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
            cursor.execute("SELECT id, symbol, headline FROM raw_news WHERE sentiment_score IS NULL")
            rows = cursor.fetchall()

            if rows:
                print(f"🧠 AI: Found {len(rows)} headlines. Analyzing...")
                processed_count = 0
                
                for row in rows:
                    score = analyze_headline(row['headline'], row['symbol'], llm)
                    
                    # Log only high-conviction finds to keep terminal clean
                    if abs(score) > 0.4:
                        print(f"   --> {row['symbol']}: {score:+.2f} | {row['headline'][:50]}...")

                    if update_sentiment_score(conn, row['id'], score):
                        processed_count += 1

                print(f"✅ Finished batch. Processed {processed_count} items.")
            
        except Exception as e:
            print(f"Loop Error: {e}")
        finally:
            conn.close()

        # Smart Sleep Logic
        status = get_market_status()
        print(f"{status['status_message']}")
        time.sleep(status['sleep_seconds'])

if __name__ == "__main__":
    main()
