import os
import time
import sqlite3
import torch
import sys
import torch.nn.functional as F
from transformers import BertTokenizer, BertForSequenceClassification

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection

MODEL_NAME = "ProsusAI/finbert"

def get_device():
    # This will explicitly check for your RTX 5050 and print a success message!
    if torch.cuda.is_available():
        device_name = torch.cuda.get_device_name(0)
        print(f"\n🚀 GPU DETECTED: Using {device_name} for AI Inference! 🚀\n")
        return torch.device("cuda")
    print("\n⚠️ No GPU detected. Falling back to CPU. ⚠️\n")
    return torch.device("cpu")

def load_model(device):
    print(f"Loading {MODEL_NAME} into memory...")
    tokenizer = BertTokenizer.from_pretrained(MODEL_NAME)
    model = BertForSequenceClassification.from_pretrained(MODEL_NAME)
    model.to(device)
    model.eval()
    return tokenizer, model

def calculate_sentiment(headline, tokenizer, model, device):
    inputs = tokenizer(headline, return_tensors="pt", padding=True, truncation=True, max_length=512)
    inputs = {k: v.to(device) for k, v in inputs.items()}
    with torch.no_grad():
        outputs = model(**inputs)
        logits = outputs.logits
        probs = F.softmax(logits, dim=-1)

    id2label = model.config.id2label
    label2id = {v: k for k, v in id2label.items()}

    pos_idx = label2id.get('positive', label2id.get('Positive'))
    neg_idx = label2id.get('negative', label2id.get('Negative'))

    if pos_idx is None or neg_idx is None:
        return 0.0

    return probs.item() - probs.item()

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
    device = get_device()
    tokenizer, model = load_model(device) # Loads ONCE into GPU
    
    print("Sentiment Engine is live and waiting for new headlines...")

    # The Loop: Keeps the model in memory and polls the DB every 60 seconds
    while True:
        conn = get_db_connection()
        if not conn:
            time.sleep(60)
            continue
            
        conn.row_factory = sqlite3.Row 

        try:
            cursor = conn.cursor()
            cursor.execute("SELECT id, headline FROM raw_news WHERE sentiment_score IS NULL")
            rows = cursor.fetchall()

            if rows:
                print(f"Found {len(rows)} new headlines. Processing on {device}...")
                processed_count = 0
                for row in rows:
                    row_id = row
                    headline = row

                    if not headline:
                        continue

                    try:
                        score = calculate_sentiment(headline, tokenizer, model, device)
                        if update_sentiment_score(conn, row_id, score):
                            processed_count += 1
                    except Exception as e:
                        print(f"Error processing {row_id}: {e}")

                print(f"✅ Finished batch. Processed {processed_count} headlines.")
            
        except Exception as e:
            print(f"Global Error: {e}")
        finally:
            conn.close()

        # Sleep before checking again
        time.sleep(60)

if __name__ == "__main__":
    main()
