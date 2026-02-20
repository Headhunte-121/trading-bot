import streamlit as st
import pandas as pd
import sqlite3
import os
import datetime
from datetime import timedelta
import sys

# Database Path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "trade_history.db")

# Import shared modules
sys.path.append(BASE_DIR)
from shared.smart_sleep import get_market_status

def get_data(query, params=None):
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(query, conn, params=params)

def get_gpu_status():
    """Checks if new sentiment scores have been generated in the last 15 minutes."""
    try:
        query = """
            SELECT COUNT(*) as count
            FROM raw_news
            WHERE sentiment_score IS NOT NULL
            AND timestamp > datetime('now', '-15 minutes')
        """
        df = get_data(query)
        if not df.empty and df['count'].iloc[0] > 0:
            return True
        return False
    except Exception:
        return False

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
        # We need a window function or a self-join. Since SQLite window functions are supported:
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
        # Fallback if table empty or error
        return pd.DataFrame()

def main():
    st.set_page_config(page_title="🚀 SwarmTrade Pro Terminal", layout="wide", initial_sidebar_state="expanded")
    
    # --- CSS Styling for Pro Terminal Look ---
    st.markdown("""
        <style>
        .main { background-color: #0e1117; color: #c9d1d9; }
        .stMetric { background-color: #161b22; padding: 10px; border-radius: 5px; border: 1px solid #30363d; }
        div[data-testid="stSidebar"] { background-color: #0d1117; border-right: 1px solid #30363d; }
        h1, h2, h3 { color: #58a6ff; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
        .ticker-box {
            display: inline-block;
            background-color: #161b22;
            border: 1px solid #30363d;
            border-radius: 4px;
            padding: 5px 10px;
            margin-right: 10px;
            font-size: 0.9em;
            color: #c9d1d9;
        }
        .ticker-up { color: #3fb950; }
        .ticker-down { color: #f85149; }
        </style>
    """, unsafe_allow_html=True)

    # --- 1. Top "Ticker Tape" (Header) ---
    st.markdown("### 🏛️ Market Movers (Last 5 Min)")
    movers = get_biggest_movers()

    if not movers.empty:
        cols = st.columns(len(movers))
        for i, row in enumerate(movers.itertuples()):
            color = "ticker-up" if row.pct_change >= 0 else "ticker-down"
            arrow = "▲" if row.pct_change >= 0 else "▼"
            with cols[i]:
                st.markdown(
                    f"<div class='ticker-box'><span style='font-weight:bold'>{row.symbol}</span><br>"
                    f"${row.current_price:.2f} <span class='{color}'>{arrow} {row.pct_change:.2f}%</span></div>",
                    unsafe_allow_html=True
                )
    else:
        st.info("Waiting for market data to populate initial momentum...")

    st.divider()

    # --- 2. System Monitor (Left Sidebar) ---
    st.sidebar.title("🎛️ System Monitor")

    # Market Status
    status = get_market_status()
    st.sidebar.markdown(f"**Market Status:**")
    if status['is_open']:
        st.sidebar.success(f"🟢 {status['status_message']}")
    else:
        st.sidebar.error(f"🔴 {status['status_message']}")

    # GPU Status
    gpu_active = get_gpu_status()
    st.sidebar.markdown(f"**AI Inference Engine:**")
    if gpu_active:
        st.sidebar.success("🟢 GPU: ACTIVE (RTX 5050)")
    else:
        st.sidebar.warning("🟡 GPU: STANDBY / SLEEPING")

    # Unread News
    unread_count = get_unread_news_count()
    st.sidebar.metric("Unread News (1h)", f"{unread_count}", delta="Live Feed")

    if st.sidebar.button("🔄 Force Refresh"):
        st.rerun()

    # --- 3. Sentiment Heatmap (Main Top) ---
    st.subheader("🧠 Swarm Intelligence (Sentiment Heatmap)")
    
    # Query for heatmap data: Aggregate sentiment by 30-min buckets
    heatmap_query = """
        SELECT
            symbol,
            strftime('%Y-%m-%d %H:%M', timestamp) as time_bucket, -- Simplify to minute level first
            AVG(sentiment_score) as avg_score
        FROM raw_news
        WHERE sentiment_score IS NOT NULL
        GROUP BY symbol, strftime('%Y-%m-%d %H', timestamp), (strftime('%M', timestamp) / 30) -- Group by 30 min chunks approx
        ORDER BY timestamp DESC
        LIMIT 500
    """
    # Better approach for 30 min buckets in SQL is tricky across dialects, let's do pandas resampling
    raw_heatmap_query = """
        SELECT symbol, timestamp, sentiment_score
        FROM raw_news
        WHERE sentiment_score IS NOT NULL
        AND timestamp > datetime('now', '-24 hours')
    """

    df_sent = get_data(raw_heatmap_query)

    if not df_sent.empty:
        df_sent['timestamp'] = pd.to_datetime(df_sent['timestamp'])
        # Resample to 30T
        # We want Y-Axis (Index) to be Symbols, and X-Axis (Columns) to be Time.
        # unstack(level=0) makes Symbol the columns.
        # So we transpose it.
        heatmap_data = df_sent.set_index('timestamp').groupby('symbol')['sentiment_score'].resample('30T').mean().unstack(level=0).T
        
        # Sort columns (Time) descending so newest is left, or ascending so newest is right?
        # Standard financial charts have newest on the right.
        heatmap_data = heatmap_data.sort_index(axis=1, ascending=True)

        st.dataframe(
            heatmap_data.style.background_gradient(cmap='RdYlGn', vmin=-1, vmax=1).format("{:.2f}"),
            use_container_width=True,
            height=400
        )
    else:
        st.info("No sentiment data available for heatmap.")

    st.divider()

    # --- 4. The "Action" Zone (Main Bottom) ---
    col_left, col_right = st.columns([3, 2])

    with col_left:
        st.subheader("📉 Technical Scanner (RSI < 40 or > 70)")
        # Fetch latest technicals
        tech_query = """
            SELECT symbol, timestamp, rsi_14, lower_bb
            FROM technical_indicators
            WHERE timestamp = (SELECT MAX(timestamp) FROM technical_indicators)
            AND (rsi_14 < 40 OR rsi_14 > 70)
            ORDER BY rsi_14 ASC
        """
        try:
            df_tech = get_data(tech_query)
            
            def style_rsi(val):
                if val < 30: return 'color: #3fb950; font-weight: bold;' # Deep Green/Buy
                if val < 40: return 'color: #7ee787;' # Light Green
                if val > 70: return 'color: #f85149; font-weight: bold;' # Red/Sell
                return ''

            if not df_tech.empty:
                st.dataframe(
                    df_tech.style.map(style_rsi, subset=['rsi_14']),
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.success("No extreme RSI anomalies detected at this moment.")
        except Exception as e:
            st.error(f"Error loading technicals: {e}")

    with col_right:
        st.subheader("📜 Live Trade Ledger")
        trade_query = """
            SELECT
                time(timestamp) as time,
                symbol,
                side,
                qty,
                price
            FROM executed_trades
            ORDER BY timestamp DESC
            LIMIT 20
        """
        try:
            df_trades = get_data(trade_query)
            if not df_trades.empty:
                # Format for "Receipt" look
                for row in df_trades.itertuples():
                    color = "🟢" if row.side == 'buy' else "🔴"
                    st.markdown(
                        f"`{row.time}` {color} **{row.side.upper()}** {row.qty} **{row.symbol}** @ ${row.price:.2f}"
                    )
            else:
                st.info("Ledger is empty. Waiting for signals...")
        except Exception as e:
            st.error(f"Error loading trades: {e}")

    # Also show pending signals if any
    st.subheader("⚠️ Pending Signals")
    pending_query = "SELECT * FROM trade_signals WHERE status != 'EXECUTED' ORDER BY id DESC LIMIT 5"
    df_pending = get_data(pending_query)
    if not df_pending.empty:
        st.dataframe(df_pending)

if __name__ == "__main__":
    main()
