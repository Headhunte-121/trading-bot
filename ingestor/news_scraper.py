import os
import sqlite3
import requests
import datetime
import sys
import time

# Ensure shared package is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.db_utils import get_db_connection, DB_PATH
from shared.config import SYMBOLS

# Configuration
FINNHUB_API_KEY = os.getenv('FINNHUB_API_KEY')
API_URL = "https://finnhub.io/api/v1/company-news"

def fetch_news(symbol):
    """Fetches company news for a given symbol from Finnhub."""
    if not FINNHUB_API_KEY:
        print("Error: FINNHUB_API_KEY environment variable not set.")
        return []

    # Fetch news for the last 2 days to ensure we get recent headlines
    today = datetime.date.today()
    start_date = today - datetime.timedelta(days=2)

    params = {
        'symbol': symbol,
        'from': start_date.strftime('%Y-%m-%d'),
        'to': today.strftime('%Y-%m-%d'),
        'token': FINNHUB_API_KEY
    }

    try:
        response = requests.get(API_URL, params=params)

        # Simple rate limit handling
        if response.status_code == 429:
            print(f"⚠️ Rate limit hit for {symbol}. Sleeping for 60s...")
            time.sleep(60)
            return fetch_news(symbol)

        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching news for {symbol}: {e}")
        return []

def save_news(conn, news_items, symbol):
    """Inserts news items into the raw_news table using batch insertion."""
    if not news_items:
        return

    cursor = conn.cursor()
    to_insert = []

    for item in news_items:
        try:
            # Finnhub provides a unix timestamp
            unix_ts = item.get('datetime')
            headline = item.get('headline')

            if unix_ts and headline:
                # Convert Unix timestamp to ISO 8601 (UTC)
                ts_iso = datetime.datetime.fromtimestamp(unix_ts, datetime.timezone.utc).isoformat()
                to_insert.append((symbol, ts_iso, headline))
        except Exception as e:
            print(f"Error processing news item: {e}")

    if not to_insert:
        return

    try:
        # Insert into database using INSERT OR IGNORE to handle duplicates based on UNIQUE constraint
        # Explicitly set relevance and urgency to NULL
        cursor.executemany("""
            INSERT OR IGNORE INTO raw_news (symbol, timestamp, headline, sentiment_score, relevance, urgency)
            VALUES (?, ?, ?, NULL, NULL, NULL)
        """, to_insert)

        count = cursor.rowcount
        conn.commit()
        if count > 0:
            print(f"✅ Inserted {count} new news items for {symbol}.")
    except Exception as e:
        print(f"❌ Database error during batch insert for {symbol}: {e}")

def main():
    print("Starting news scraper...")

    if not FINNHUB_API_KEY:
        print("CRITICAL: FINNHUB_API_KEY is missing.")
        return

    conn = get_db_connection()

    try:
        for symbol in SYMBOLS:
            print(f"Fetching news for {symbol}...")
            news_items = fetch_news(symbol)
            if news_items:
                save_news(conn, news_items, symbol)
            else:
                print(f"No news found for {symbol}.")

            # Rate limit buffer: 1.1s delay between calls
            time.sleep(1.1)

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        conn.close()
        print("News scraper finished.")

if __name__ == "__main__":
    main()
