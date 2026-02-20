import streamlit as st
import pandas as pd
import sqlite3
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import sys
import datetime

# Database Path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "trade_history.db")

# Import shared modules
sys.path.append(BASE_DIR)
from shared.smart_sleep import get_market_status

# --- Data Fetching ---

def get_data(query, params=None):
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(query, conn, params=params)

def get_gpu_status():
    """Calculates 'load' based on news items processed in the last 5 minutes."""
    try:
        query = """
            SELECT COUNT(*) as count
            FROM raw_news
            WHERE timestamp > datetime('now', '-5 minutes')
        """
        df = get_data(query)
        if not df.empty:
            count = df['count'].iloc[0]
            # Map 0-10 items to 0-100%
            load = min(count * 10, 100)
            return load
        return 0
    except Exception:
        return 0

def get_unread_news_count():
    """Counts news items ingested in the last 60 minutes."""
    try:
        query = """
            SELECT COUNT(*) as count
            FROM raw_news
            WHERE timestamp > datetime('now', '-60 minutes')
        """
        df = get_data(query)
        if not df.empty:
            return df['count'].iloc[0]
        return 0
    except:
        return 0

def get_trend_scanner():
    """
    Scans for stocks in a healthy trend (Price > SMA 200).
    Sorts by distance from SMA (Ascending) -> "Buy the Dip" candidates.
    """
    try:
        # We need the latest close and latest SMA 200
        # market_data and technical_indicators join on symbol, timestamp
        query = """
            WITH LatestData AS (
                SELECT
                    m.symbol,
                    m.close,
                    t.sma_200,
                    ROW_NUMBER() OVER (PARTITION BY m.symbol ORDER BY m.timestamp DESC) as rn
                FROM market_data m
                JOIN technical_indicators t ON m.symbol = t.symbol AND m.timestamp = t.timestamp
                WHERE t.sma_200 IS NOT NULL
            )
            SELECT
                symbol,
                close,
                sma_200,
                ((close - sma_200) / sma_200) * 100 as pct_dist
            FROM LatestData
            WHERE rn = 1 AND close > sma_200
            ORDER BY pct_dist ASC
            LIMIT 15
        """
        return get_data(query)
    except Exception as e:
        return pd.DataFrame()

def get_all_symbols():
    """Fetches all unique symbols from the market_data table."""
    try:
        query = "SELECT DISTINCT symbol FROM market_data ORDER BY symbol"
        df = get_data(query)
        if not df.empty:
            return df['symbol'].tolist()
        return []
    except:
        return []

def get_recent_candles(symbol, limit=200):
    """Fetches the last N candles for a given symbol."""
    try:
        query = """
            SELECT * FROM market_data
            WHERE symbol = ?
            ORDER BY timestamp DESC
            LIMIT ?
        """
        df = get_data(query, params=(symbol, limit))
        return df.sort_values(by='timestamp', ascending=True)
    except:
        return pd.DataFrame()

def get_technicals(symbol, limit=200):
    """Fetches technical indicators for a given symbol."""
    try:
        query = """
            SELECT * FROM technical_indicators
            WHERE symbol = ?
            ORDER BY timestamp DESC
            LIMIT ?
        """
        df = get_data(query, params=(symbol, limit))
        return df.sort_values(by='timestamp', ascending=True)
    except:
        return pd.DataFrame()

def get_symbol_trades(symbol):
    """Fetches executed trades for a specific symbol to overlay on chart."""
    try:
        query = "SELECT * FROM executed_trades WHERE symbol = ? ORDER BY timestamp ASC"
        return get_data(query, params=(symbol,))
    except:
        return pd.DataFrame()

def get_raw_news(limit=50):
    """Fetches raw news for the feed."""
    try:
        query = """
            SELECT * FROM raw_news
            ORDER BY timestamp DESC
            LIMIT ?
        """
        return get_data(query, params=(limit,))
    except:
        return pd.DataFrame()

def get_executed_trades(limit=20):
    """Fetches executed trades for the ledger."""
    try:
        query = """
            SELECT * FROM executed_trades
            ORDER BY timestamp DESC
            LIMIT ?
        """
        return get_data(query, params=(limit,))
    except:
        return pd.DataFrame()

def get_latest_analysis(symbol):
    """Fetches the latest AI analysis for the symbol."""
    try:
        query = """
            SELECT * FROM chart_analysis_requests
            WHERE symbol = ? AND status = 'COMPLETED'
            ORDER BY timestamp DESC
            LIMIT 1
        """
        df = get_data(query, params=(symbol,))
        if not df.empty:
            return df.iloc[0]
        return None
    except:
        return None

# --- Main Application ---

def main():
    st.set_page_config(
        page_title="🚀 SwarmTrade Pro Terminal",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # --- CSS Styling (Project Neon) ---
    st.markdown("""
        <style>
        /* Global Reset & Font */
        @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&display=swap');

        * {
            font-family: 'JetBrains Mono', monospace !important;
        }

        /* Background */
        .stApp {
            background-color: #0B0E14;
            color: #E0E0E0;
        }

        /* Glassmorphism Containers */
        .glass-panel {
            background: rgba(20, 25, 35, 0.6);
            backdrop-filter: blur(8px);
            -webkit-backdrop-filter: blur(8px);
            border: 1px solid rgba(255, 255, 255, 0.1);
            border-radius: 8px;
            padding: 15px;
            margin-bottom: 15px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }

        /* Sidebar Styling */
        section[data-testid="stSidebar"] {
            background-color: #080a0f;
            border-right: 1px solid #1f2937;
        }

        /* Header / Ticker Tape */
        .ticker-container {
            display: flex;
            overflow-x: auto;
            gap: 15px;
            padding-bottom: 10px;
            scrollbar-width: thin;
        }

        .ticker-card {
            background: rgba(255, 255, 255, 0.03);
            border: 1px solid rgba(255, 255, 255, 0.05);
            border-radius: 4px;
            padding: 8px 12px;
            min-width: 140px;
            text-align: center;
        }

        .ticker-symbol { font-weight: bold; font-size: 1.1em; color: #FFFFFF; }
        .ticker-price { font-size: 0.9em; color: #AAAAAA; }
        .ticker-highlight { color: #00FF94; }

        /* News Cards */
        .news-card {
            background: rgba(0, 0, 0, 0.2);
            border-left: 3px solid #555;
            padding: 10px;
            margin-bottom: 10px;
            border-radius: 0 4px 4px 0;
            transition: all 0.2s;
        }
        .news-card:hover {
            background: rgba(255, 255, 255, 0.05);
        }
        .news-headline { font-weight: bold; font-size: 0.9em; color: #E0E0E0; margin-bottom: 4px; }
        .news-meta { font-size: 0.75em; color: #888; display: flex; justify-content: space-between; }
        .badge-sentiment { padding: 2px 6px; border-radius: 4px; font-size: 0.7em; font-weight: bold; }

        /* Urgency Pulsating Animation */
        @keyframes pulse-red-border {
            0% { box-shadow: 0 0 0 0 rgba(255, 59, 48, 0.4); border-color: #FF3B30; }
            70% { box-shadow: 0 0 0 6px rgba(255, 59, 48, 0); border-color: #FF3B30; }
            100% { box-shadow: 0 0 0 0 rgba(255, 59, 48, 0); border-color: #FF3B30; }
        }

        .urgent-news {
            animation: pulse-red-border 2s infinite;
            border: 1px solid #FF3B30 !important;
        }

        /* Analysis Box */
        .analysis-box {
            background: rgba(0, 212, 255, 0.05);
            border: 1px solid rgba(0, 212, 255, 0.2);
            border-radius: 8px;
            padding: 15px;
            margin-top: 10px;
        }
        .prediction-bullish { color: #00FF94; font-weight: bold; }
        .prediction-bearish { color: #FF3B30; font-weight: bold; }
        .prediction-neutral { color: #AAAAAA; font-weight: bold; }

        /* Status Heartbeat Animation */
        @keyframes pulse-green-glow {
            0% { box-shadow: 0 0 0 0 rgba(0, 255, 148, 0.4); }
            70% { box-shadow: 0 0 0 8px rgba(0, 255, 148, 0); }
            100% { box-shadow: 0 0 0 0 rgba(0, 255, 148, 0); }
        }
        @keyframes pulse-red-glow {
            0% { box-shadow: 0 0 0 0 rgba(255, 59, 48, 0.4); }
            70% { box-shadow: 0 0 0 8px rgba(255, 59, 48, 0); }
            100% { box-shadow: 0 0 0 0 rgba(255, 59, 48, 0); }
        }

        .status-dot-green {
            display: inline-block;
            width: 10px;
            height: 10px;
            background-color: #00FF94;
            border-radius: 50%;
            margin-right: 8px;
            animation: pulse-green-glow 2s infinite;
        }
        .status-dot-red {
            display: inline-block;
            width: 10px;
            height: 10px;
            background-color: #FF3B30;
            border-radius: 50%;
            margin-right: 8px;
            animation: pulse-red-glow 2s infinite;
        }

        /* Hide Streamlit Elements */
        #MainMenu {visibility: hidden;}
        header {visibility: hidden;}
        footer {visibility: hidden;}

        /* Custom Scrollbar */
        ::-webkit-scrollbar {
            width: 8px;
            height: 8px;
        }
        ::-webkit-scrollbar-track {
            background: #0B0E14;
        }
        ::-webkit-scrollbar-thumb {
            background: #333;
            border-radius: 4px;
        }
        ::-webkit-scrollbar-thumb:hover {
            background: #555;
        }

        </style>
    """, unsafe_allow_html=True)

    # --- Sidebar: System Nucleus ---
    st.sidebar.title("🧬 SYSTEM NUCLEUS")

    # Heartbeat
    market_status = get_market_status()
    st.sidebar.markdown("### SYSTEM STATUS")
    if market_status['is_open']:
        st.sidebar.markdown(
            "<div style='display:flex; align-items:center;'>"
            "<span class='status-dot-green'></span>"
            "<span>MARKET OPEN</span></div>",
            unsafe_allow_html=True
        )
    else:
        st.sidebar.markdown(
            f"<div style='display:flex; align-items:center;'>"
            f"<span class='status-dot-red'></span>"
            f"<span>MARKET CLOSED ({market_status['status_message']})</span></div>",
            unsafe_allow_html=True
        )

    st.sidebar.divider()

    # RTX 5050 Load
    load = get_gpu_status() # 0 to 100
    st.sidebar.markdown("### RTX 5050 LOAD")
    st.sidebar.progress(load / 100)
    st.sidebar.caption(f"Load: {load}% (AI Brain)")

    st.sidebar.divider()

    # Unread News
    unread = get_unread_news_count()
    st.sidebar.markdown("### UNREAD INTEL")
    st.sidebar.metric("Last 60m", unread, delta=None)

    st.sidebar.divider()

    # Target Focus (Symbol Selector)
    st.sidebar.markdown("### TARGET FOCUS")
    all_symbols = get_all_symbols()

    # Scanner Data
    scanner_df = get_trend_scanner()

    # Default symbol logic
    default_symbol = scanner_df.iloc[0]['symbol'] if not scanner_df.empty else (all_symbols[0] if all_symbols else None)

    target_index = 0
    if default_symbol and default_symbol in all_symbols:
        target_index = all_symbols.index(default_symbol)

    selected_symbol = st.sidebar.selectbox(
        "Select Asset",
        options=all_symbols,
        index=target_index,
        key="symbol_selector"
    )
    
    if st.sidebar.button("🔄 REFRESH SYSTEM"):
        st.rerun()

    # --- Zone A: Scanner (Trend Surfer) ---
    st.markdown("<div class='glass-panel'>", unsafe_allow_html=True)
    st.markdown("#### 🌊 TREND SURFER SCANNER (Price > SMA 200)")

    if not scanner_df.empty:
        # Create HTML for ticker tape style
        ticker_html = "<div class='ticker-container'>"
        for row in scanner_df.itertuples():
            dist = row.pct_dist
            ticker_html += f"""
                <div class='ticker-card'>
                    <div class='ticker-symbol'>{row.symbol}</div>
                    <div class='ticker-price'>${row.close:.2f}</div>
                    <div class='ticker-highlight'>+{dist:.2f}% > SMA</div>
                </div>
            """
        ticker_html += "</div>"
        st.markdown(ticker_html, unsafe_allow_html=True)
    else:
        st.info("No healthy trends detected. Market might be Bearish.")
    st.markdown("</div>", unsafe_allow_html=True)

    # --- Zone B: Main View (Chart & Technicals) ---
    col_chart, col_ai = st.columns([3, 1])

    with col_chart:
        st.markdown(f"### 🎯 TARGET: {selected_symbol}")
        
        if selected_symbol:
            df_candles = get_recent_candles(selected_symbol)
            df_tech = get_technicals(selected_symbol)
            df_trades = get_symbol_trades(selected_symbol)

            if not df_candles.empty:
                # Create Plotly Subplot
                fig = make_subplots(
                    rows=2, cols=1,
                    shared_xaxes=True,
                    vertical_spacing=0.05,
                    row_heights=[0.7, 0.3],
                    specs=[[{"secondary_y": False}], [{"secondary_y": False}]]
                )

                # Candlestick
                fig.add_trace(go.Candlestick(
                    x=df_candles['timestamp'],
                    open=df_candles['open'],
                    high=df_candles['high'],
                    low=df_candles['low'],
                    close=df_candles['close'],
                    name='OHLC',
                    increasing_line_color='#00FF94',
                    decreasing_line_color='#FF3B30'
                ), row=1, col=1)

                # SMA 200 Overlay
                if not df_tech.empty and 'sma_200' in df_tech.columns:
                     fig.add_trace(go.Scatter(
                        x=df_tech['timestamp'],
                        y=df_tech['sma_200'],
                        mode='lines',
                        name='SMA 200',
                        line=dict(color='#FFA500', width=2) # Orange for SMA
                    ), row=1, col=1)

                # Overlay Executed Trades
                if not df_trades.empty:
                    buys = df_trades[df_trades['side'] == 'buy']
                    sells = df_trades[df_trades['side'] == 'sell']

                    if not buys.empty:
                        fig.add_trace(go.Scatter(
                            x=buys['timestamp'],
                            y=buys['price'],
                            mode='markers',
                            name='Buy',
                            marker=dict(symbol='triangle-up', size=12, color='#00FF94', line=dict(width=1, color='white'))
                        ), row=1, col=1)

                    if not sells.empty:
                        fig.add_trace(go.Scatter(
                            x=sells['timestamp'],
                            y=sells['price'],
                            mode='markers',
                            name='Sell',
                            marker=dict(symbol='triangle-down', size=12, color='#FF3B30', line=dict(width=1, color='white'))
                        ), row=1, col=1)

                # RSI
                if not df_tech.empty:
                    fig.add_trace(go.Scatter(
                        x=df_tech['timestamp'],
                        y=df_tech['rsi_14'],
                        mode='lines',
                        name='RSI',
                        line=dict(color='#00d4ff', width=1)
                    ), row=2, col=1)

                    # RSI Levels (35 and 55 for Trend Surfer)
                    fig.add_hline(y=55, line_dash="dot", line_color="#FF3B30", row=2, col=1)
                    fig.add_hline(y=35, line_dash="dot", line_color="#00FF94", row=2, col=1)

                # Styling
                fig.update_layout(
                    template="plotly_dark",
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    margin=dict(l=10, r=10, t=10, b=10),
                    height=600,
                    showlegend=True,
                    legend=dict(x=0, y=1, orientation="h"),
                    xaxis_rangeslider_visible=False
                )

                # Remove range slider from candlestick
                fig.update_xaxes(rangeslider_visible=False)

                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No candle data available.")

    with col_ai:
        st.markdown(f"### 🧠 AI ANALYST")
        if selected_symbol:
            analysis = get_latest_analysis(selected_symbol)
            if analysis is not None:
                pred = analysis['ai_prediction']
                conf = analysis['ai_confidence']
                reason = analysis['ai_reasoning']

                pred_class = "prediction-neutral"
                if pred == 'BULLISH': pred_class = "prediction-bullish"
                elif pred == 'BEARISH': pred_class = "prediction-bearish"

                st.markdown(f"""
                    <div class='analysis-box'>
                        <div style='font-size: 1.2em;' class='{pred_class}'>{pred} ({conf:.2f})</div>
                        <div style='font-size: 0.9em; margin-top: 8px; color: #DDD;'>{reason}</div>
                        <div style='font-size: 0.7em; margin-top: 10px; color: #888;'>Generated: {analysis['timestamp']}</div>
                    </div>
                """, unsafe_allow_html=True)
            else:
                st.info("No AI Analysis available yet.")
        else:
            st.info("Select a symbol.")

    # --- Split Bottom Section ---
    col_news, col_ledger = st.columns([1, 1])

    # --- Zone C: Intelligence Feed ---
    with col_news:
        st.markdown("<div class='glass-panel'>", unsafe_allow_html=True)
        st.subheader("📡 INTELLIGENCE FEED")

        news_df = get_raw_news()
        if not news_df.empty:
            # Scrollable container using height
            with st.container(height=400):
                for row in news_df.itertuples():
                    urgency = getattr(row, 'urgency', 0)
                    urgency_class = "urgent-news" if urgency > 7 else ""

                    # Sentiment Color Logic
                    score = getattr(row, 'sentiment_score', 0)
                    if score > 0:
                        sentiment_color = "#00FF94" # Green
                    elif score < 0:
                        sentiment_color = "#FF3B30" # Red
                    else:
                        sentiment_color = "#AAAAAA" # Grey

                    st.markdown(f"""
                        <div class='news-card {urgency_class}' style='border-left-color: {sentiment_color};'>
                            <div class='news-headline'>{row.headline}</div>
                            <div class='news-meta'>
                                <span>{row.symbol} | {row.timestamp}</span>
                                <span class='badge-sentiment' style='background: {sentiment_color}20; color: {sentiment_color};'>
                                    Score: {score:.2f}
                                </span>
                            </div>
                        </div>
                    """, unsafe_allow_html=True)
        else:
            st.info("No intelligence data captured.")
        st.markdown("</div>", unsafe_allow_html=True)

    # --- Zone D: The Ledger ---
    with col_ledger:
        st.markdown("<div class='glass-panel'>", unsafe_allow_html=True)
        st.subheader("📒 EXECUTED TRADES")

        trades_df = get_executed_trades()
        if not trades_df.empty:
            
            def color_side(val):
                if isinstance(val, str):
                    color = '#00FF94' if val.lower() == 'buy' else '#FF3B30'
                    return f'color: {color}; font-weight: bold;'
                return ''

            styled_df = trades_df.style.map(color_side, subset=['side'])

            st.dataframe(
                styled_df,
                use_container_width=True,
                height=400,
                hide_index=True,
                column_config={
                    "price": st.column_config.NumberColumn("Price", format="$%.2f"),
                    "qty": st.column_config.NumberColumn("Size"),
                    "side": st.column_config.TextColumn("Side"),
                    "timestamp": st.column_config.DatetimeColumn("Time", format="%m-%d %H:%M")
                }
            )
        else:
            st.info("Ledger empty. No executions recorded.")
        st.markdown("</div>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()
