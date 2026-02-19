import os
import time
import sqlite3
import torch
import sys
import re
from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig, pipeline

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection

# --- CONFIGURATION ---
# We use the 4-bit quantized version of Llama-3-8B-Instruct.
# This fits perfectly in an RTX 5050 (8GB VRAM).
MODEL_NAME = "unsloth/llama-3-8b-Instruct-bnb-4bit"

def get_device():
    """Checks for GPU availability."""
    if torch.cuda.is_available():
        device_name = torch.cuda.get_device_name(0)
        print(f"\n🚀 GPU DETECTED: Using {device_name} with 4-bit Quantization! 🚀\n")
        return "cuda"
    print("\n⚠️ No GPU detected. This model will be VERY slow on CPU. ⚠️\n")
    return "cpu"

def load_llm():
    """Loads the Llama-3 model with 4-bit quantization to save VRAM."""
    print(f"Loading {MODEL_NAME}... (This might take a while the first time)")
    
    # 4-Bit Config specifically for RTX Cards
    bnb_config = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_use_double_quant=True,
        bnb_4bit_quant_type="nf4",
        bnb_4bit_compute_dtype=torch.float16
    )

    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    
    # Load model with quantization
    model = AutoModelForCausalLM.from_pretrained(
        MODEL_NAME,
        quantization_config=bnb_config,
        device_map="auto",  # Automatically puts layers on GPU
        local_files_only=False # Set to True after first download to work offline
    )
    
    # Create a pipeline for easier generation
    text_generator = pipeline(
        "text-generation",
        model=model,
        tokenizer=tokenizer,
        max_new_tokens=64, # Short answer to save speed
        pad_token_id=tokenizer.eos_token_id
    )
    
    return text_generator

def analyze_headline(headline, symbol, llm_pipeline):
    """
    Uses Llama-3 to determine relevance and sentiment.
    Returns a score between -1.0 and 1.0.
    """
    
    # The Prompt: Forces the AI to be a strict financial analyst
    # We ask for a specific format: [RELEVANT: YES/NO] [SCORE: X.XX]
    prompt = f"""<|begin_of_text|><|start_header_id|>system<|end_header_id|>
    You are a strict financial trading algorithm. 
    Task 1: Determine if the headline is DIRECTLY relevant to the stock ticker {symbol}. 
    - If it is about a competitor (like Samsung) but implies nothing for {symbol}, it is NOT relevant.
    - If it is general market news (like "Mortgages rise"), it is NOT relevant.
    Task 2: If relevant, assign a sentiment score from -1.0 (Very Bearish) to 1.0 (Very Bullish).
    
    Respond in EXACTLY this format:
    [RELEVANT: YES/NO] [SCORE: <number>]
    <|eot_id|><|start_header_id|>user<|end_header_id|>
    Headline: "{headline}"
    Ticker: {symbol}
    <|eot_id|><|start_header_id|>assistant<|end_header_id|>"""

    try:
        # Generate response
        sequences = llm_pipeline(prompt, do_sample=False)
        output = sequences[0]['generated_text']
        
        # Extract the AI's response (remove the prompt)
        response = output.split("<|start_header_id|>assistant<|end_header_id|>")[-1].strip()
        
        # Parse Relevance
        if "[RELEVANT: NO]" in response.upper():
            return 0.0 # Irrelevant news gets a 0 score
            
        # Parse Score using Regex
        match = re.search(r"SCORE:\s?(-?\d+\.?\d*)", response, re.IGNORECASE)
        if match:
            score = float(match.group(1))
            # Clamp score between -1 and 1 just in case
            return max(-1.0, min(1.0, score))
            
        return 0.0 # Default if parse fails

    except Exception as e:
        print(f"LLM Error on '{headline}': {e}")
        return 0.0

def update_sentiment_score(conn, row_id, score):
    query = "UPDATE raw_news SET sentiment_score = ? WHERE id = ?"
    try:
        conn.execute(query, (score, row_id))
        conn.commit()
        return True
    except sqlite3.OperationalError as e:
        print(f"Update failed for {row_id}: {e}")
        return False

def main():
    get_device() # Just to print the GPU status
    llm = load_llm()
    
    print("✅ Llama-3 AI Engine is Active. Waiting for news...")

    while True:
        conn = get_db_connection()
        if not conn:
            time.sleep(60)
            continue
            
        conn.row_factory = sqlite3.Row 

        try:
            # Fetch headlines that haven't been scored yet
            cursor = conn.cursor()
            cursor.execute("SELECT id, symbol, headline FROM raw_news WHERE sentiment_score IS NULL")
            rows = cursor.fetchall()

            if rows:
                print(f"🧠 AI: Found {len(rows)} headlines. Analyzing with Llama-3...")
                processed_count = 0
                
                for row in rows:
                    row_id = row['id']
                    symbol = row['symbol']
                    headline = row['headline']

                    if not headline:
                        continue

                    score = analyze_headline(headline, symbol, llm)
                    
                    # Optional: Print interesting finds
                    if abs(score) > 0.5:
                        print(f"   --> {symbol}: {score:.2f} | {headline[:50]}...")

                    if update_sentiment_score(conn, row_id, score):
                        processed_count += 1

                print(f"✅ Batch complete. Analyzed {processed_count} headlines.")
            
        except Exception as e:
            print(f"Global Loop Error: {e}")
        finally:
            conn.close()

        # Sleep before checking again
        time.sleep(60)

if __name__ == "__main__":
    main()
