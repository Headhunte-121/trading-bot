import os
import sqlite3
import requests
import datetime

# Configuration
SYMBOLS = ['AAPL', 'MSFT']
FINNHUB_API_KEY = os.getenv('FINNHUB_API_KEY')
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'data', 'trade_history.db')
API_URL = "https://finnhub.io/api/v1/company-news"

def get_db_connection():
    """Establishes a connection to the SQLite database with a timeout."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=15)
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        return None

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
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching news for {symbol}: {e}")
        return []

def save_news(conn, news_items, symbol):
    """Inserts news items into the raw_news table."""
    if not news_items:
        return

    cursor = conn.cursor()
    count = 0

    for item in news_items:
        try:
            # Finnhub provides a unix timestamp
            unix_ts = item.get('datetime')
            headline = item.get('headline')

            if unix_ts and headline:
                # Convert Unix timestamp to ISO 8601 (UTC)
                ts_iso = datetime.datetime.fromtimestamp(unix_ts, datetime.timezone.utc).isoformat()

                # Insert into database
                # raw_news schema: id, symbol, timestamp, headline, sentiment_score
                cursor.execute("""
                    INSERT INTO raw_news (symbol, timestamp, headline, sentiment_score)
                    VALUES (?, ?, ?, NULL)
                """, (symbol, ts_iso, headline))
                count += 1
        except Exception as e:
            print(f"Error processing news item: {e}")

    conn.commit()
    print(f"Inserted {count} news items for {symbol}.")

def main():
    print("Starting news scraper...")

    if not FINNHUB_API_KEY:
        print("CRITICAL: FINNHUB_API_KEY is missing.")
        return

    conn = get_db_connection()
    if not conn:
        return

    try:
        for symbol in SYMBOLS:
            print(f"Fetching news for {symbol}...")
            news_items = fetch_news(symbol)
            if news_items:
                save_news(conn, news_items, symbol)
            else:
                print(f"No news found for {symbol}.")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        conn.close()
        print("News scraper finished.")

if __name__ == "__main__":
    main()
