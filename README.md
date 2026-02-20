# 🐝 SwarmTrade AI: Microservice Trading Bot

SwarmTrade AI is a modular, event-driven algorithmic trading system designed to execute mean-reversion strategies using sentiment analysis (LLM-based) and technical indicators. It operates as a "swarm" of isolated agents (microservices), each responsible for a specific task—ingesting market data, calculating technicals, analyzing news sentiment, or executing trades.

## 🏗 Architecture

The system is built on a **microservice architecture** where each component runs in its own Docker container. These services do not communicate directly with each other; instead, they share a state via a central **SQLite database** (`data/trade_history.db`) in WAL (Write-Ahead Log) mode for high concurrency.

### Core Components

1.  **Ingestors:**
    *   `market_harvester`: Fetches 5-minute OHLCV data from Yahoo Finance for a configured list of symbols.
    *   `news_scraper`: Scrapes company news from Finnhub (rate-limited to respect free tier).

2.  **Processors:**
    *   `ta_calculator`: Calculates RSI (14) and Bollinger Bands (20, 2) using `pandas-ta`.
    *   `sentiment_engine`: Uses a local LLM (Llama-3-8b via `unsloth` & `bitsandbytes`) to score news headlines for relevance, sentiment, and urgency. **Requires GPU.**

3.  **Strategy Engine:**
    *   `mean_reversion`: Generates BUY signals when Price < Lower Bollinger Band AND RSI < 30 AND News Sentiment > 0.3.

4.  **Execution:**
    *   `risk_manager`: Calculates position size based on 1% account risk per trade and a 2% stop loss, capped at 20% max allocation per position.
    *   `alpaca_executor`: Submits Market Orders with OTO (One-Triggers-Other) Stop Loss to Alpaca and tracks order status.

5.  **Dashboard:**
    *   `app`: A Streamlit-based "Pro Terminal" for real-time monitoring of market movers, sentiment heatmaps, technical anomalies, and trade logs.

## 🚀 Prerequisites

*   **Docker** and **Docker Compose** installed.
*   **Git** installed.
*   **NVIDIA GPU** (Optional but highly recommended for `sentiment_engine`. If no GPU, the sentiment analysis will be extremely slow or needs to be disabled/modified).
*   **API Keys**:
    *   [Alpaca](https://alpaca.markets/) (Trading) - Paper Trading recommended.
    *   [Finnhub](https://finnhub.io/) (News Data).

## ⚙️ Setup & Configuration

1.  **Clone the Repository**
    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```

2.  **Configure Environment Variables**
    Create a `.env` file in the root directory:

    ```env
    # Finnhub API Key
    FINNHUB_API_KEY=your_finnhub_key

    # Alpaca API Credentials
    APCA_API_KEY_ID=your_alpaca_key_id
    APCA_API_SECRET_KEY=your_alpaca_secret_key
    APCA_API_BASE_URL=https://paper-api.alpaca.markets
    ```

3.  **Configure Symbols**
    Edit `shared/config.py` to modify the list of symbols you want to trade:
    ```python
    SYMBOLS = ['NVDA', 'AAPL', 'MSFT', ...]
    ```

## ▶️ Running the Bot

The entire system is orchestrated via Docker Compose.

1.  **Build and Start**
    ```bash
    docker-compose up --build
    ```
    *Note: The first build will take some time to download the PyTorch and LLM dependencies.*

2.  **Access the Dashboard**
    Open your browser and navigate to: [http://localhost:8501](http://localhost:8501)

3.  **Stop the Bot**
    ```bash
    docker-compose down
    ```

## 🛠 How It Works (Data Flow)

1.  `market_harvester` & `news_scraper` populate `market_data` and `raw_news` tables.
2.  `ta_calculator` computes indicators and updates `technical_indicators`.
3.  `sentiment_engine` reads raw news, runs inference, and updates `sentiment_score`.
4.  `mean_reversion` queries for candidates (Price < Lower BB, RSI < 30, Sentiment > 0.3) and inserts `PENDING` signals into `trade_signals`.
5.  `risk_manager` reads `PENDING` signals, calculates size, and updates status to `SIZED`.
6.  `alpaca_executor` picks up `SIZED` signals, submits orders, updates status to `SUBMITTED`, and eventually `EXECUTED` upon fill.

## ❓ Troubleshooting

*   **Database Locked:** The system uses SQLite WAL mode to handle concurrency. Occasional locking errors in logs are handled by retry logic in the scripts.
*   **No Trades:**
    *   Check `trade_signals` table for PENDING/SIZED/FAILED signals.
    *   Ensure API keys are correct.
    *   Market might just be quiet (no setup meeting criteria).
*   **GPU Issues:** Ensure your Docker supports NVIDIA container toolkit (`--gpus all` is used in `docker-compose.yml`).
*   **Rate Limits:** `news_scraper` has a built-in delay to respect Finnhub's free tier (60 calls/min).

## ⚠️ Disclaimer

This software is for educational purposes only. Do not risk money you cannot afford to lose. The "Mean Reversion" strategy provided is a basic example and not financial advice.
