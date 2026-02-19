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
    if torch.cuda.is_available():
        return torch.device("cuda")
    return torch.device("cpu")

def load_model(device):
    print(f"Loading {MODEL_NAME} on {device}...")
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

    return probs[0][pos_idx].item() - probs[0][neg_idx].item()

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
    tokenizer, model = load_model(device)

    conn = get_db_connection()
    conn.row_factory = sqlite3.Row # Use Row factory for named access

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, headline FROM raw_news WHERE sentiment_score IS NULL")
        rows = cursor.fetchall()

        print(f"Found {len(rows)} headlines to process.")

        processed_count = 0
        for row in rows:
            row_id = row['id']
            headline = row['headline']

            if not headline:
                continue

            try:
                score = calculate_sentiment(headline, tokenizer, model, device)
                if update_sentiment_score(conn, row_id, score):
                    processed_count += 1
                    if processed_count % 10 == 0:
                        print(f"Processed {processed_count}/{len(rows)}...")
            except Exception as e:
                print(f"Error processing {row_id}: {e}")

        print(f"Finished. Processed {processed_count} headlines.")

    except Exception as e:
        print(f"Global Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
