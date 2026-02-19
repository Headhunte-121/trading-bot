# Bot Architecture Rules

This is a modular algorithmic trading bot. 
Do NOT create monolith files. Always adhere to the microservice structure below:

1. `/ingestor`: Only handles fetching data (yfinance, News API). Writes to SQLite.
2. `/brain`: Only handles NLP (FinBERT) and Technical Analysis. Reads from SQLite.
3. `/execution`: Only handles Alpaca API and Risk Management.

- **Database Contract:** All services communicate via a shared `data/trade_history.db` SQLite database.
- **Secrets:** Never hardcode API keys. Always use `.env` files and `os.getenv()`.
