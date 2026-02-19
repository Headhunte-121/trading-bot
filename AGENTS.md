# AI Algorithmic Trading Bot - Architecture & Agent Rules

## Project Overview
This is a modular, microservice-based algorithmic trading bot. It uses NLP sentiment analysis (FinBERT) and technical indicators to execute mean-reversion trades via the Alpaca API. 

## 🛑 STRICT DIRECTIVES FOR AI AGENTS
You are working as part of a multi-agent swarm. To prevent merge conflicts and broken dependencies, you MUST adhere strictly to the following rules:

1. **Module Isolation:** - Never modify files outside of your explicitly assigned directory.
   - Modules must NEVER call each other's functions directly. 
   - All inter-module communication happens strictly by reading/writing to the shared SQLite database.

2. **The Data Contract (`/shared/schema.py`):**
   - The SQLite database (`data/trade_history.db`) is the single source of truth.
   - Do NOT alter table names, column names, or data types unless explicitly requested by the user. 
   - Always query the database defensively (handle empty tables or missing rows gracefully).

3. **Security & Secrets:**
   - NEVER hardcode API keys, tokens, or passwords.
   - Always use `python-dotenv` and fetch secrets using `os.getenv('ENV_VAR_NAME')`.

4. **Tech Stack & Libraries:**
   - Python 3.10+
   - Data Ingestion: `yfinance`, `beautifulsoup4`, `requests`
   - Data Processing: `pandas`, `pandas-ta`
   - AI/NLP: `transformers` (Hugging Face), `torch`
   - Execution: `alpaca-trade-api`
   - If you add a new library, you MUST append it to the `requirements.txt` file in the root directory.

## Directory Structure
- `/ingestor`: Scripts to fetch market data and news.
- `/processor`: TA calculations and FinBERT sentiment scoring.
- `/strategy`: The core trading logic (reads indicators/sentiment, outputs Buy/Sell signals).
- `/execution`: Alpaca API integration and risk management (position sizing, stop-losses).
- `/shared`: Contains `schema.py` (database structure) and shared utilities.
