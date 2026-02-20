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

def get_biggest_movers():
    """Calculates top movers based on the last 5-minute close price difference."""
    try:
        # Fetch the last two records for every symbol
        query = """
            WITH RankedPrices AS (
                SELECT
                    symbol,
                    close,
                    timestamp,
                    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) as rn
                FROM market_data
            )
            SELECT
                now.symbol,
                now.close as current_price,
                prev.close as prev_price,
                ((now.close - prev.close) / prev.close) * 100 as pct_change
            FROM RankedPrices now
            JOIN RankedPrices prev ON now.symbol = prev.symbol AND prev.rn = 2
            WHERE now.rn = 1
            ORDER BY ABS(pct_change) DESC
            LIMIT 10
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

def get_trade_signals(symbol, limit=200):
    """Fetches trade signals for overlay."""
    try:
        query = """
            SELECT * FROM trade_signals
            WHERE symbol = ?
            ORDER BY timestamp DESC
            LIMIT ?
        """
        df = get_data(query, params=(symbol, limit))
        return df
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
            flex-shrink: 0; /* Prevent shrinking */
        }

        .ticker-symbol { font-weight: bold; font-size: 1.1em; color: #FFFFFF; }
        .ticker-price { font-size: 0.9em; color: #AAAAAA; }
        .ticker-up { color: #00FF94; }
        .ticker-down { color: #FF3B30; }

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
    st.sidebar.caption(f"Load: {load}% (News Processing)")

    st.sidebar.divider()

    # Unread News
    unread = get_unread_news_count()
    st.sidebar.markdown("### UNREAD INTEL")
    st.sidebar.metric("Last 60m", unread, delta=None)

    st.sidebar.divider()

    # Target Focus (Symbol Selector)
    st.sidebar.markdown("### TARGET FOCUS")
    all_symbols = get_all_symbols()

    # Determine default logic
    movers = get_biggest_movers()
    default_symbol = movers.iloc[0]['symbol'] if not movers.empty else (all_symbols[0] if all_symbols else None)

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

    # --- Zone A: Top Header (Ticker Tape) ---
    st.markdown("<div class='glass-panel'>", unsafe_allow_html=True)
    st.markdown("#### ⚡ BIGGEST MOVERS (5M)")

    if not movers.empty:
        # Create HTML for ticker tape
        ticker_html_list = []
        for row in movers.itertuples():
            color_class = "ticker-up" if row.pct_change >= 0 else "ticker-down"
            arrow = "▲" if row.pct_change >= 0 else "▼"
            card_html = f"""
                <div class='ticker-card'>
                    <div class='ticker-symbol'>{row.symbol}</div>
                    <div class='ticker-price'>${row.current_price:.2f}</div>
                    <div class='{color_class}'>{arrow} {row.pct_change:.2f}%</div>
                </div>
            """
            ticker_html_list.append(card_html)

        # Join list into a single string and wrap in container
        ticker_html_content = "".join(ticker_html_list)
        final_html = f"<div class='ticker-container'>{ticker_html_content}</div>"

        st.markdown(final_html, unsafe_allow_html=True)
    else:
        st.info("Awaiting Market Data...")
    st.markdown("</div>", unsafe_allow_html=True)

    # --- Zone B: Main View (Chart & Technicals) ---
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

            # Determine colors based on market status
            is_market_open = market_status['is_open']

            if is_market_open:
                inc_color = '#00FF94'
                dec_color = '#FF3B30'
            else:
                inc_color = '#555555' # Grayscale for closed
                dec_color = '#333333'

            # Candlestick
            fig.add_trace(go.Candlestick(
                x=df_candles['timestamp'],
                open=df_candles['open'],
                high=df_candles['high'],
                low=df_candles['low'],
                close=df_candles['close'],
                name='OHLC',
                increasing_line_color=inc_color,
                decreasing_line_color=dec_color
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

                # RSI Levels
                fig.add_hline(y=70, line_dash="dot", line_color="#FF3B30", row=2, col=1)
                fig.add_hline(y=30, line_dash="dot", line_color="#00FF94", row=2, col=1)

            # Styling
            fig.update_layout(
                template="plotly_dark",
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                margin=dict(l=10, r=10, t=10, b=10),
                height=600,
                showlegend=False,
                xaxis_rangeslider_visible=False
            )

            # Grid Polish & Axis Format
            fig.update_xaxes(
                showgrid=True,
                gridcolor='#1E222D',
                gridwidth=1,
                tickformat="%H:%M",
                rangeslider_visible=False,
                rangebreaks=[
                    dict(bounds=["sat", "mon"]),   # Hide weekends
                    dict(bounds=[16, 9.5], pattern="hour"), # Hide hours outside 9:30am - 4:00pm
                ]
            )
            fig.update_yaxes(showgrid=True, gridcolor='#1E222D', gridwidth=1)

            # Market Closed Annotation
            if not is_market_open:
                fig.add_annotation(
                    text="MARKET CLOSED",
                    xref="paper", yref="paper",
                    x=0.5, y=0.5,
                    showarrow=False,
                    font=dict(size=40, color="rgba(255, 255, 255, 0.1)")
                )

            st.plotly_chart(fig, width="stretch")
        else:
            st.warning("No candle data available for selected symbol.")
    else:
        st.warning("No symbols found in database.")

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
                width="stretch",
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
