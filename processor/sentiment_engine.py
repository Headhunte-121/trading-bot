import os
import time
import sqlite3
import torch
import sys
from transformers import BertTokenizer, BertForSequenceClassification
import torch.nn.functional as F

# -----------------------------------------------------------------------------
# Configuration & Setup
# -----------------------------------------------------------------------------

# Define paths relative to this script
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "trade_history.db")

MODEL_NAME = "ProsusAI/finbert"

def get_db_connection(timeout=20):
    """Establishes a connection to the SQLite database with a timeout."""
    if not os.path.exists(DB_PATH):
        print(f"Error: Database not found at {DB_PATH}")
        sys.exit(1)

    try:
        conn = sqlite3.connect(DB_PATH, timeout=timeout)
        conn.row_factory = sqlite3.Row  # Access columns by name
        return conn
    except sqlite3.Error as e:
        print(f"Failed to connect to database: {e}")
        sys.exit(1)

def get_device():
    """Returns the appropriate device (cuda or cpu)."""
    if torch.cuda.is_available():
        return torch.device("cuda")
    return torch.device("cpu")

def load_model(device):
    """Loads the FinBERT model and tokenizer."""
    print(f"Loading {MODEL_NAME} on {device}...")
    tokenizer = BertTokenizer.from_pretrained(MODEL_NAME)
    model = BertForSequenceClassification.from_pretrained(MODEL_NAME)
    model.to(device)
    model.eval()
    return tokenizer, model

def calculate_sentiment(headline, tokenizer, model, device):
    """
    Calculates a sentiment score between -1.0 and 1.0 for a given headline.

    Logic:
    FinBERT outputs logits for 3 classes: [positive, negative, neutral] or similar.
    We need to check the config to be sure.
    Usually ProsusAI/finbert labels are: 0: positive, 1: negative, 2: neutral (or similar).
    We will use the model's config to identify the indices.

    Score = Probability(Positive) - Probability(Negative)
    """
    inputs = tokenizer(headline, return_tensors="pt", padding=True, truncation=True, max_length=512)
    inputs = {k: v.to(device) for k, v in inputs.items()}

    with torch.no_grad():
        outputs = model(**inputs)
        logits = outputs.logits
        probs = F.softmax(logits, dim=-1)

    # Get label indices from config
    # typical id2label: {0: 'positive', 1: 'negative', 2: 'neutral'}
    # We need to map them correctly.
    id2label = model.config.id2label

    # Invert to map label name to index
    label2id = {v: k for k, v in id2label.items()}

    # Extract probabilities
    # Handle potential key variations (e.g. "Positive" vs "positive")
    pos_idx = label2id.get('positive', label2id.get('Positive'))
    neg_idx = label2id.get('negative', label2id.get('Negative'))
    neu_idx = label2id.get('neutral', label2id.get('Neutral'))

    if pos_idx is None or neg_idx is None:
        # Fallback if labels are unexpected
        print(f"Warning: Unexpected labels in model config: {id2label}. Defaulting to raw logits difference if possible, but safer to skip.")
        return 0.0

    prob_pos = probs[0][pos_idx].item()
    prob_neg = probs[0][neg_idx].item()
    # prob_neu = probs[0][neu_idx].item()

    score = prob_pos - prob_neg
    return score

def update_sentiment_score(conn, row_id, score):
    """Updates the sentiment score for a specific row with retry logic."""
    query = "UPDATE raw_news SET sentiment_score = ? WHERE id = ?"

    max_retries = 5
    for attempt in range(max_retries):
        try:
            cursor = conn.cursor()
            cursor.execute(query, (score, row_id))
            conn.commit()
            return True
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                print(f"Database locked. Retrying ({attempt + 1}/{max_retries})...")
                time.sleep(1) # Wait before retrying
            else:
                print(f"Error updating row {row_id}: {e}")
                return False
        except Exception as e:
             print(f"Unexpected error updating row {row_id}: {e}")
             return False

    print(f"Failed to update row {row_id} after {max_retries} retries.")
    return False

def main():
    device = get_device()
    tokenizer, model = load_model(device)

    conn = get_db_connection()
    cursor = conn.cursor()

    # Query for NULL sentiment scores
    try:
        cursor.execute("SELECT id, headline FROM raw_news WHERE sentiment_score IS NULL")
        rows = cursor.fetchall()
    except Exception as e:
        print(f"Error querying database: {e}")
        conn.close()
        return

    print(f"Found {len(rows)} headlines to process.")

    processed_count = 0
    for row in rows:
        row_id = row['id']
        headline = row['headline']

        if not headline:
            print(f"Skipping row {row_id}: Empty headline")
            continue

        try:
            score = calculate_sentiment(headline, tokenizer, model, device)
            success = update_sentiment_score(conn, row_id, score)
            if success:
                processed_count += 1
                # Optional: print progress
                if processed_count % 10 == 0:
                    print(f"Processed {processed_count}/{len(rows)}...")
        except Exception as e:
            print(f"Error processing headline id {row_id}: {e}")

    print(f"Finished. Processed {processed_count} headlines.")
    conn.close()

if __name__ == "__main__":
    main()
