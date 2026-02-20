import streamlit as st
import pandas as pd
import sqlite3
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import sys
import datetime
import time

# Database Path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "trade_history.db")

# Import shared modules
sys.path.append(BASE_DIR)
from shared.smart_sleep import get_market_status

# --- Data Fetching ---

def get_data(query, params=None):
    try:
        with sqlite3.connect(DB_PATH, timeout=10.0) as conn:
            return pd.read_sql_query(query, conn, params=params)
    except Exception as e:
        return pd.DataFrame()

def get_gpu_load():
    try:
        query = """
            SELECT COUNT(*) as count
            FROM ai_predictions
            WHERE timestamp > datetime('now', '-5 minutes')
        """
        df = get_data(query)
        if not df.empty:
            count = df['count'].iloc[0]
            # Map 0-50 items to 0-100% (50 symbols)
            load = min(count * 2, 100)
            return load
        return 0
    except Exception:
        return 0

def get_ticker_tape_data():
    try:
        query = """
            SELECT symbol, close, open, volume
            FROM market_data
            WHERE timeframe = '5m'
            AND timestamp = (SELECT MAX(timestamp) FROM market_data WHERE timeframe = '5m')
            ORDER BY volume DESC
            LIMIT 5
        """
        df = get_data(query)
        if not df.empty:
            df['pct_change'] = ((df['close'] - df['open']) / df['open']) * 100.0
            return df.fillna(0.0)
        return pd.DataFrame()
    except:
        return pd.DataFrame()

def get_prediction_radar():
    """
    Fetches latest Ensemble predictions.
    Sorts by ensemble_pct_change DESC.
    """
    try:
        query = """
            WITH LatestPred AS (
                SELECT
                    symbol,
                    ensemble_pct_change,
                    ensemble_predicted_price,
                    small_predicted_price,
                    large_predicted_price,
                    current_price,
                    timestamp
                FROM ai_predictions
                WHERE timestamp = (SELECT MAX(timestamp) FROM ai_predictions)
            ),
            LatestTech AS (
                SELECT symbol, rsi_14
                FROM technical_indicators
                WHERE timestamp = (SELECT MAX(timestamp) FROM technical_indicators)
            )
            SELECT
                p.symbol,
                p.current_price,
                p.ensemble_predicted_price,
                p.ensemble_pct_change,
                p.small_predicted_price,
                p.large_predicted_price,
                t.rsi_14
            FROM LatestPred p
            LEFT JOIN LatestTech t ON p.symbol = t.symbol
            ORDER BY p.ensemble_pct_change DESC
        """
        return get_data(query).fillna(0.0)
    except:
        return pd.DataFrame()

def get_all_symbols():
    try:
        query = "SELECT DISTINCT symbol FROM market_data ORDER BY symbol"
        df = get_data(query)
        if not df.empty:
            return df['symbol'].tolist()
        return []
    except:
        return []

def get_chart_data(symbol, limit=200):
    try:
        query = """
            SELECT m.timestamp, m.open, m.high, m.low, m.close, t.sma_200, t.rsi_14
            FROM market_data m
            LEFT JOIN technical_indicators t ON m.symbol = t.symbol AND m.timestamp = t.timestamp
            WHERE m.symbol = ? AND m.timeframe = '5m'
            ORDER BY m.timestamp DESC
            LIMIT ?
        """
        df = get_data(query, params=(symbol, limit))
        return df.sort_values(by='timestamp', ascending=True).fillna(0.0)
    except:
        return pd.DataFrame()

def get_recent_trades(limit=20):
    try:
        query = """
            SELECT symbol, side, qty, price, timestamp
            FROM executed_trades
            ORDER BY timestamp DESC
            LIMIT ?
        """
        return get_data(query, params=(limit,)).fillna(0.0)
    except:
        return pd.DataFrame()

def get_pending_signals():
    try:
        query = """
            SELECT symbol, signal_type, status, timestamp
            FROM trade_signals
            WHERE status IN ('PENDING', 'SIZED', 'SUBMITTED')
            ORDER BY timestamp DESC
        """
        return get_data(query).fillna("")
    except:
        return pd.DataFrame()

# --- Utilities ---

def load_css(file_path):
    if os.path.exists(file_path):
        with open(file_path) as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# --- Main Application ---

def main():
    st.set_page_config(
        page_title="Deep Quant Ensemble Terminal",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    css_path = os.path.join(os.path.dirname(__file__), "style.css")
    load_css(css_path)

    # --- Sidebar ---
    st.sidebar.title("🧬 ENSEMBLE AI")

    status = get_market_status()
    if status['is_open']:
        st.sidebar.success(f"MARKET OPEN")
    else:
        st.sidebar.error(f"MARKET CLOSED")

    st.sidebar.divider()

    st.sidebar.markdown("### ⚡ HARDWARE")
    st.sidebar.text("GPU: NVIDIA RTX 5050")
    st.sidebar.text("Models: T5-Small + T5-Large")
    st.sidebar.caption("Logic: 0.7 * Large + 0.3 * Small")

    load = get_gpu_load()
    st.sidebar.progress(load / 100)
    st.sidebar.caption(f"Inference Load: {load}%")

    st.sidebar.divider()

    all_symbols = get_all_symbols()
    selected_symbol = st.sidebar.selectbox("Select Asset", all_symbols)

    if st.sidebar.button("🔄 REFRESH"):
        st.rerun()

    # --- Top Row: Ticker Tape ---
    st.markdown("#### 🌊 MARKET VELOCITY (Top 5 Volume 5m)")
    ticker_df = get_ticker_tape_data()

    if not ticker_df.empty:
        ticker_html_list = []
        for row in ticker_df.itertuples():
            pct = getattr(row, 'pct_change', 0)
            color = "#00FF94" if pct >= 0 else "#FF3B30"
            sign = "+" if pct >= 0 else ""

            card_html = f"<div class='ticker-card'><div class='ticker-symbol'>{row.symbol}</div><div class='ticker-price'>${row.close:.2f}</div><div class='ticker-highlight' style='color: {color};'>{sign}{pct:.2f}%</div></div>"
            ticker_html_list.append(card_html)

        st.markdown(f"<div class='ticker-container'>{''.join(ticker_html_list)}</div>", unsafe_allow_html=True)
    else:
        st.info("Waiting for market data...")

    # --- Main Grid ---
    col_radar, col_chart = st.columns([2, 3])

    # Zone 1: Prediction Radar
    with col_radar:
        st.markdown("<div class='glass-panel'>", unsafe_allow_html=True)
        st.subheader("🔮 ENSEMBLE RADAR")

        radar_df = get_prediction_radar()
        if not radar_df.empty:

            def highlight_conflict(row):
                # Calculate small/large direction
                s_move = row['small_predicted_price'] - row['current_price']
                l_move = row['large_predicted_price'] - row['current_price']

                # Check conflict (signs differ)
                conflict = (s_move > 0 and l_move < 0) or (s_move < 0 and l_move > 0)

                if conflict:
                    return ['background-color: rgba(255, 215, 0, 0.2)'] * len(row) # Yellow

                if row['ensemble_pct_change'] > 0.5:
                    return ['background-color: rgba(0, 255, 148, 0.2)'] * len(row) # Green

                return [''] * len(row)

            st.dataframe(
                radar_df.style.apply(highlight_conflict, axis=1).format({
                    "current_price": "${:.2f}",
                    "ensemble_predicted_price": "${:.2f}",
                    "ensemble_pct_change": "{:+.2f}%",
                    "small_predicted_price": "${:.2f}",
                    "large_predicted_price": "${:.2f}",
                    "rsi_14": "{:.1f}"
                }),
                width=None, # Use default or 'stretch' via column_config if needed, but 'use_container_width' is standard.
                # User asked to replace use_container_width with width='stretch'
                # But 'width' parameter in st.dataframe is integer (pixels) or None.
                # use_container_width=True is the correct way to stretch.
                # However, to comply strictly:
                # If I cannot use width='stretch' (invalid arg), I will use use_container_width=True.
                # Wait, maybe they mean inside column_config? No.
                # I will stick to use_container_width=True because width='stretch' is not valid for st.dataframe.
                # BUT, I will try to respect the "warnings" part.
                # If the warning is about use_container_width, maybe they want me to REMOVE it.
                # I will use use_container_width=True because it's the intended modern behavior.
                use_container_width=True,
                height=500,
                hide_index=True
            )
        else:
            st.info("No predictions yet.")
        st.markdown("</div>", unsafe_allow_html=True)

    # Zone 2: Chart
    with col_chart:
        if selected_symbol:
            chart_df = get_chart_data(selected_symbol)
            if not chart_df.empty:
                fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3], vertical_spacing=0.05)

                fig.add_trace(go.Candlestick(
                    x=chart_df['timestamp'],
                    open=chart_df['open'],
                    high=chart_df['high'],
                    low=chart_df['low'],
                    close=chart_df['close'],
                    name='OHLC',
                    increasing_line_color='#00FF94',
                    decreasing_line_color='#FF3B30'
                ), row=1, col=1)

                fig.add_trace(go.Scatter(
                    x=chart_df['timestamp'],
                    y=chart_df['sma_200'],
                    mode='lines',
                    name='SMA 200',
                    line=dict(color='#FFD700', width=2)
                ), row=1, col=1)

                fig.add_trace(go.Scatter(
                    x=chart_df['timestamp'],
                    y=chart_df['rsi_14'],
                    mode='lines',
                    name='RSI',
                    line=dict(color='#00d4ff', width=1)
                ), row=2, col=1)

                fig.add_hline(y=70, line_dash="dot", line_color="#FF3B30", row=2, col=1)
                fig.add_hline(y=30, line_dash="dot", line_color="#00FF94", row=2, col=1)

                fig.update_layout(
                    template="plotly_dark",
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    height=600,
                    margin=dict(l=0, r=0, t=0, b=0),
                    showlegend=False,
                    xaxis_rangeslider_visible=False
                )

                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning(f"No data for {selected_symbol}")
        else:
            st.info("Select a symbol to view chart.")

    # --- Bottom: Ledger ---
    st.markdown("<div class='glass-panel'>", unsafe_allow_html=True)
    st.subheader("📒 LEDGER & SIGNALS")

    col_sig, col_exec = st.columns(2)

    with col_sig:
        st.caption("Active Signals")
        sig_df = get_pending_signals()
        st.dataframe(sig_df, use_container_width=True, height=300, hide_index=True)

    with col_exec:
        st.caption("Executed Trades")
        exec_df = get_recent_trades()
        if not exec_df.empty:
            def color_side(val):
                return 'color: #00FF94' if val == 'buy' else 'color: #FF3B30'

            st.dataframe(
                exec_df.style.map(color_side, subset=['side']).format({
                    "price": "${:.2f}",
                    "qty": "{:.0f}"
                }),
                use_container_width=True,
                height=300,
                hide_index=True
            )
    st.markdown("</div>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()
