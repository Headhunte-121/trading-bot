import streamlit as st
import pandas as pd
import sqlite3
import os
from datetime import datetime

# Database Path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "trade_history.db")

def get_data(query):
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(query, conn)

def main():
    st.set_page_config(page_title="🚀 SwarmTrade AI Dashboard", layout="wide")
    
    # --- CSS Styling ---
    st.markdown("""
        <style>
        .main { background-color: #0e1117; }
        .stMetric { background-color: #1e2130; padding: 15px; border-radius: 10px; border: 1px solid #3e4259; }
        </style>
    """, unsafe_allow_html=True)

    st.title("🚀 SwarmTrade AI | Quantum Swarm")
    st.sidebar.header("Control Panel")
    refresh = st.sidebar.button("🔄 Force Refresh Data")

    # --- Fetch Symbols ---
    try:
        symbols = get_data("SELECT DISTINCT symbol FROM market_data")['symbol'].tolist()
        selected_symbol = st.sidebar.selectbox("Select Asset to Inspect", ["ALL"] + symbols)
    except:
        symbols = ["AAPL", "MSFT"]
        selected_symbol = "ALL"

    # --- TOP ROW: Live Metrics ---
    st.subheader("📊 Market Pulse")
    col1, col2, col3, col4 = st.columns(4)

    try:
        latest_ta = get_data("SELECT symbol, rsi_14, lower_bb FROM technical_indicators ORDER BY timestamp DESC LIMIT 20")
        latest_sent = get_data("SELECT AVG(sentiment_score) as sent FROM raw_news WHERE sentiment_score IS NOT NULL")
        
        avg_rsi = latest_ta['rsi_14'].mean()
        avg_sent = latest_sent['sent'].iloc[0] if not latest_sent.empty else 0

        col1.metric("Avg Swarm RSI", f"{avg_rsi:.2f}", delta="-2.1" if avg_rsi < 30 else "Normal")
        col2.metric("AI Sentiment", f"{avg_sent:.2f}", delta="Bullish" if avg_sent > 0.2 else "Bearish")
        col3.metric("Active Agents", "9/9", delta="Running", delta_color="normal")
        col4.metric("GPU Load (RTX 5050)", "Active", delta="CUDA Enabled")
    except:
        st.warning("Waiting for data to populate...")

    # --- MIDDLE ROW: Sentiment Chart ---
    st.divider()
    st.subheader("🧠 Swarm Intelligence (News Sentiment)")
    
    news_query = "SELECT symbol, timestamp, sentiment_score FROM raw_news WHERE sentiment_score IS NOT NULL"
    news_df = get_data(news_query)

    if not news_df.empty:
        news_df['timestamp'] = pd.to_datetime(news_df['timestamp'])
        chart_data = news_df.pivot_table(index='timestamp', columns='symbol', values='sentiment_score', aggfunc='mean').ffill()
        if selected_symbol != "ALL":
            st.line_chart(chart_data[selected_symbol])
        else:
            st.line_chart(chart_data)
    else:
        st.info("AI is still reading the news. Check back in 1 minute.")

    # --- BOTTOM ROW: Technicals & Trades ---
    st.divider()
    left_col, right_col = st.columns([2, 1])

    with left_col:
        st.subheader("📉 Technical Scanner")
        tech_query = "SELECT symbol, timestamp, rsi_14, lower_bb FROM technical_indicators ORDER BY timestamp DESC LIMIT 50"
        df_tech = get_data(tech_query)
        
        # Color coding logic
        def highlight_rsi(val):
            color = 'red' if val < 30 else ('green' if val > 70 else 'white')
            return f'color: {color}; font-weight: bold'

        if not df_tech.empty:
            st.dataframe(df_tech.style.map(highlight_rsi, subset=['rsi_14']), width=None)
            
    with right_col:
        st.subheader("📜 Recent Executions")
        trade_query = "SELECT symbol, side, price, qty, timestamp FROM executed_trades ORDER BY timestamp DESC LIMIT 10"
        df_trades = get_data(trade_query)
        if not df_trades.empty:
            st.table(df_trades)
        else:
            st.info("No trades executed yet. Waiting for Buy Signal.")

if __name__ == "__main__":
    main()
