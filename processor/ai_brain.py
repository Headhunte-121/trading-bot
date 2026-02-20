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
        batch_size=16,
        max_new_tokens=128,
        pad_token_id=tokenizer.eos_token_id
    )

    return text_generator

def build_news_prompt(headline, symbol):
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

def build_chart_prompt(technical_summary, symbol):
    return f"""<|begin_of_text|><|start_header_id|>system<|end_header_id|>
Act as a veteran Technical Analyst. You must output a valid JSON object.
I will give you the technical data points for {symbol}.
Your job is to predict the probability of the price moving UP in the next 4 hours.

Analyze these factors:
1. Is the long-term trend fighting the short-term signal?
2. Does the Volume confirm the move? (Price up + Volume up = Strong).
3. Are the indicators conflicting?

Output strict JSON only. No conversational filler.
Format: {{"prediction": "BULLISH"|"BEARISH"|"NEUTRAL", "confidence": <float 0.0-1.0>, "reasoning": "..."}}
<|eot_id|><|start_header_id|>user<|end_header_id|>
Ticker: {symbol}
Data:
{technical_summary}
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

def process_news_batch(conn, llm, cursor):
    """
    Checks for un-scored headlines and processes them.
    Returns True if work was done (to prevent sleep), False otherwise.
    """
    cursor.execute("SELECT id, symbol, headline FROM raw_news WHERE relevance IS NULL LIMIT 16")
    rows = cursor.fetchall()

    if not rows:
        return False

    print(f"📰 News Brain: Analyzing {len(rows)} headlines...")
    prompts = [build_news_prompt(row['headline'], row['symbol']) for row in rows]

    # Run inference
    results = llm(prompts, do_sample=True, temperature=0.3, top_p=0.9)

    updates = []
    for row, res in zip(rows, results):
        generated_text = res[0]['generated_text']
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

    # Batch update
    if updates:
        query = "UPDATE raw_news SET sentiment_score = ?, relevance = ?, urgency = ? WHERE id = ?"
        params = [(u[1], u[2], u[3], u[0]) for u in updates]
        conn.executemany(query, params)
        conn.commit()

    return True

def process_chart_batch(conn, llm, cursor):
    """
    Checks for pending chart analysis requests and processes them.
    Returns True if work was done.
    """
    cursor.execute("SELECT id, symbol, technical_summary FROM chart_analysis_requests WHERE status = 'PENDING' LIMIT 16")
    rows = cursor.fetchall()

    if not rows:
        return False

    print(f"📈 Analyst Brain: Reviewing {len(rows)} charts...")
    prompts = [build_chart_prompt(row['technical_summary'], row['symbol']) for row in rows]

    # Run inference
    results = llm(prompts, do_sample=True, temperature=0.3, top_p=0.9)

    updates = []
    for row, res in zip(rows, results):
        generated_text = res[0]['generated_text']
        response_part = generated_text.split("assistant<|end_header_id|>")[-1]
        data = parse_llm_output(response_part)

        prediction = "NEUTRAL"
        confidence = 0.0
        reasoning = "Parsing error"

        if data:
            try:
                prediction = data.get("prediction", "NEUTRAL").upper()
                confidence = float(data.get("confidence", 0.0))
                reasoning = data.get("reasoning", "")
            except (ValueError, TypeError):
                pass

        updates.append((prediction, confidence, reasoning, row['id']))
        print(f"   --> {row['symbol']} Analyst: {prediction} ({confidence:.2f})")

    # Batch update
    if updates:
        query = """
            UPDATE chart_analysis_requests
            SET status = 'COMPLETED', ai_prediction = ?, ai_confidence = ?, ai_reasoning = ?
            WHERE id = ?
        """
        conn.executemany(query, updates)
        conn.commit()

    return True

def main():
    device = get_device()
    llm = load_llm()

    print("✅ Unified AI Brain (Llama-3) is Active. Monitoring News & Charts...")

    while True:
        conn = get_db_connection()
        if not conn:
            time.sleep(5)
            continue

        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        try:
            # Priority 1: News (Safety Shield)
            did_news = process_news_batch(conn, llm, cursor)

            # Priority 2: Charts (Opportunity)
            # Only process charts if we didn't just spend time processing a full batch of news
            # OR if we want to interleave. Let's process both but prioritize news in the loop.
            did_charts = process_chart_batch(conn, llm, cursor)

            if not did_news and not did_charts:
                # No work to do
                print("💤 Brain idle. Sleeping 2s...", end='\r')
                time.sleep(2)
            else:
                # If we did work, loop immediately to drain queue
                pass

        except Exception as e:
            print(f"Brain Loop Error: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(5)
        finally:
            try:
                conn.close()
            except:
                pass

if __name__ == "__main__":
    main()
