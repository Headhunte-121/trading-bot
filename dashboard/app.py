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

# --- Utilities ---

def load_css(file_path):
    """Loads a CSS file and injects it into the Streamlit app."""
    if os.path.exists(file_path):
        with open(file_path) as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# --- Main Application ---

def main():
    st.set_page_config(
        page_title="🚀 SwarmTrade Pro Terminal",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # --- CSS Styling (Project Neon) ---
    css_path = os.path.join(os.path.dirname(__file__), "style.css")
    load_css(css_path)

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
        # Create HTML for ticker tape
        ticker_html_list = []
        for row in scanner_df.itertuples():
            pct_change = getattr(row, 'pct_dist', 0)
            color_class = "ticker-up" if pct_change >= 0 else "ticker-down"
            # arrow = "▲" if pct_change >= 0 else "▼" # Not used in HTML currently

            card_html = f"""
                <div class='ticker-card'>
                    <div class='ticker-symbol'>{row.symbol}</div>
                    <div class='ticker-price'>${row.close:.2f}</div>
                    <div class='ticker-highlight'>+{pct_change:.2f}% > SMA</div>
                </div>
            """
            ticker_html_list.append(card_html)

        # Join list into a single string and wrap in container
        ticker_html_content = "".join(ticker_html_list)
        final_html = f"<div class='ticker-container'>{ticker_html_content}</div>"

        st.markdown(final_html, unsafe_allow_html=True)
    else:
        st.info("No healthy trends detected. Market might be Bearish.")
    st.markdown("</div>", unsafe_allow_html=True)

    # --- Zone B: Main View (Chart & Technicals) ---
    col_chart, col_ai = st.columns([3, 1])

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

            # Fixed Colors (Neon) - Gap-less Dynamics
            inc_color = '#00FF94'
            dec_color = '#FF3B30'

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
                    dict(bounds=["sat", "mon"]),   # Hide weekends only
                ]
            )
            fig.update_yaxes(showgrid=True, gridcolor='#1E222D', gridwidth=1)

            # No Market Closed Annotation

            st.plotly_chart(fig, use_container_width=True)
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
                    if val.lower() == 'buy':
                        return 'background-color: #00FF94; color: white; border-radius: 10px; padding: 2px 8px; font-weight: bold;'
                    else:
                        return 'background-color: #FF3B30; color: white; border-radius: 10px; padding: 2px 8px; font-weight: bold;'
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
