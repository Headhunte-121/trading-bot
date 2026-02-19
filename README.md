# SwarmTrade AI: Microservice Trading Bot

SwarmTrade AI is a modular, event-driven algorithmic trading system designed to execute mean-reversion strategies using sentiment analysis and technical indicators. It operates as a "swarm" of isolated agents (microservices), each responsible for a specific task—ingesting market data, calculating technicals, analyzing news sentiment, or executing trades.

## 🏗 Architecture

The system is built on a **microservice architecture** where each component runs in its own Docker container. These services do not communicate directly with each other. instead, they share a state via a central **SQLite database** (`data/trade_history.db`).

- **Ingestors:** Fetch market data (`market_harvester`) and news (`news_scraper`).
- **Processors:** Calculate technical indicators (`ta_calculator`) and sentiment scores (`sentiment_engine`).
- **Strategy Engine:** Analyzes data to generate Buy/Sell signals (`mean_reversion`).
- **Execution:** Manages risk (`risk_manager`) and executes orders via Alpaca (`alpaca_executor`).
- **Dashboard:** A Streamlit app for real-time monitoring.

This design ensures that if one component fails, the others continue to operate, and the system state is always preserved in the database.

## 🚀 Prerequisites

Before you begin, ensure you have the following installed:
- **Docker** and **Docker Compose**
- **Git**

You will also need API keys from the following providers:
- **Alpaca** (for trading): [Sign up here](https://alpaca.markets/)
- **Finnhub** (for news data): [Sign up here](https://finnhub.io/)

## ⚙️ Setup & Configuration

1. **Clone the Repository**
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. **Configure Environment Variables**
   Create a file named `.env` in the root directory of the project. This file is required for the containers to access your API keys.

   Add the following lines to your `.env` file:

   ```env
   # Finnhub API Key (for news data)
   FINNHUB_API_KEY=your_finnhub_api_key_here

   # Alpaca API Credentials
   APCA_API_KEY_ID=your_alpaca_api_key_here
   APCA_API_SECRET_KEY=your_alpaca_secret_key_here

   # Alpaca Base URL (Use paper-api for testing)
   APCA_API_BASE_URL=https://paper-api.alpaca.markets
   ```

   > **Note:** Never commit your `.env` file to version control.

## ▶️ Running the Bot

The entire system is orchestrated via Docker Compose. You do not need to run individual Python scripts.

1. **Build and Start the Swarm**
   Run the following command in your terminal:
   ```bash
   docker-compose up --build
   ```

   This command will:
   - Build the Docker images for all services.
   - Initialize the SQLite database.
   - Start all the agents in the background.

2. **Monitor Logs**
   To see what the agents are doing, you can view the logs:
   ```bash
   docker-compose logs -f
   ```

3. **Stop the Bot**
   To stop all services:
   ```bash
   docker-compose down
   ```

## 📊 Dashboard

Once the bot is running, you can access the live dashboard to view trade history, active signals, and system status.

**Open your browser and navigate to:**
[http://localhost:8501](http://localhost:8501)

## 🛠 How to Extend

The modular nature of SwarmTrade AI makes it easy to customize and extend.

### Adding New Technical Indicators
To add a new indicator (e.g., MACD):
1. Open `processor/ta_calculator.py`.
2. Locate the `calculate_indicators` function.
3. Use `pandas-ta` to calculate your new indicator:
   ```python
   # Example: Calculate MACD
   macd = symbol_df.ta.macd(fast=12, slow=26, signal=9)
   ```
4. Update the database schema in `shared/schema.py` to include columns for the new indicator.
5. Update the SQL `INSERT` statement in `ta_calculator.py` to store the new values.

### Adjusting Risk Parameters
To change the risk management rules:
1. Open `execution/risk_manager.py`.
2. Modify the constants at the top of the file:
   ```python
   ACCOUNT_SIZE = 100000  # Total account size for sizing calculations
   RISK_PCT = 0.02        # Risk 2% of account per trade
   STOP_LOSS_PCT = 0.05   # Set stop loss 5% below entry price
   ```
   *Note: These changes will apply to all future signals processed by the Risk Manager.*

## ❓ Troubleshooting

- **Database Locked Errors:** The system uses a shared SQLite file. Occasional "database locked" messages in the logs are normal as agents retry connections. If they persist, restart the containers.
- **Missing Data:** Ensure your API keys are correct and that you have an active internet connection. Check the logs for `market_harvester` or `news_scraper` for specific error messages.
- **Orders Not Filling:** If using Alpaca Paper Trading, ensure your `APCA_API_BASE_URL` is set to `https://paper-api.alpaca.markets`.
