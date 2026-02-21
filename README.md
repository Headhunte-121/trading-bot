# Deep Quant Terminal

**Deep Quant Terminal** is a sophisticated, microservices-based automated trading system designed for algorithmic equity trading. It leverages a 3-tier strategy engine combining technical analysis, deep value identification, and AI-driven price prediction to execute trades via the Alpaca API.

The system features a futuristic "Project Neon" dashboard for real-time monitoring, telemetry, and manual override controls.

## 🏗️ Architecture

The system is composed of several specialized microservices running in Docker containers:

*   **Market Harvester (`ingestor/market_harvester.py`)**: Responsible for fetching market data (1m/5m candles) from Yahoo Finance and syncing it to the local SQLite database. It implements "Eagle Eye" resolution for active symbols.
*   **TA Calculator (`processor/ta_calculator.py`)**: Computes technical indicators (SMA, RSI, VWAP, ATR, Bollinger Bands) in real-time.
*   **AI Brain (`processor/predictive_engine.py`)**: Runs an ensemble of Chronos T5 (Small + Large) models to forecast price movements 30 minutes into the future. **Requires NVIDIA GPU.**
*   **Strategy Engine (`strategy/trend_following.py`)**: Evaluates market data against a 3-tier logic (VWAP Scalp, Deep Value Buy, Trend Buy) to generate trade signals.
*   **Risk Manager (`execution/risk_manager.py`)**: Sizes positions based on account equity (1% risk per trade) and validates signals.
*   **Alpaca Executor (`execution/alpaca_executor.py`)**: Manages order execution (Market Buy) and risk management (Trailing Stops) via the Alpaca API.
*   **Dashboard (`dashboard/app.py`)**: A Streamlit-based UI for visualizing market data, AI predictions, active signals, and system logs.

## 🚀 Prerequisites

*   **Docker & Docker Compose**: For container orchestration.
*   **NVIDIA GPU**: The `predictive_engine` service requires an NVIDIA GPU with at least **8GB VRAM** (e.g., RTX 3080, RTX 4070, or better). The system loads ~7.6GB of model weights.
*   **Alpaca Trading Account**: API keys for a Paper or Live trading account.

## 🛠️ Installation & Setup

1.  **Clone the Repository**
    ```bash
    git clone <repository-url>
    cd deep-quant-terminal
    ```

2.  **Configure Environment Variables**
    Create a `.env` file in the root directory and add your Alpaca API credentials:
    ```ini
    APCA_API_KEY_ID=your_api_key_id
    APCA_API_SECRET_KEY=your_api_secret_key
    APCA_API_BASE_URL=https://paper-api.alpaca.markets
    ```
    *(Note: Use `https://api.alpaca.markets` for live trading)*

## ⚡ Running the System

Start the entire stack using Docker Compose:

```bash
docker-compose up --build
```

The system will initialize the database, download necessary AI models (first run only), and start all services.

## 🖥️ Dashboard

Once running, access the **Deep Quant Terminal** dashboard at:

**[http://localhost:8501](http://localhost:8501)**

### Key Features
*   **Prediction Radar**: Visualizes AI conviction, agreement between model ensembles, and projected targets.
*   **Ticker Tape**: Real-time scrolling ticker of 5m market movers.
*   **Interactive Charts**: Candlestick charts with overlaid SMAs, RSI, and AI forecast lines.
*   **System Telemetry**: Real-time GPU load monitoring and service status.

## 🎛️ System Power Modes

The dashboard sidebar allows you to control the system's sleep behavior:

*   **🤖 AUTO**: The system automatically sleeps when the market is closed (NYSE hours) to save resources and wakes up for the opening bell.
*   **⚡ FORCE AWAKE**: Forces all services to run continuously, regardless of market hours (useful for testing or crypto).
*   **🌙 FORCE SLEEP**: Puts the system into a deep sleep mode, pausing data ingestion and strategy processing.
