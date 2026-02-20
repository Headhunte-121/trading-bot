import streamlit as st
import pandas as pd
import sqlite3
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import sys
import datetime
import time
import warnings
import logging
from streamlit_autorefresh import st_autorefresh
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode

# Suppress Warnings (Aggressive)
warnings.filterwarnings("ignore")
os.environ["PYTHONWARNINGS"] = "ignore"

# Suppress Streamlit Logs
logging.getLogger("streamlit").setLevel(logging.ERROR)

# Database Path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "trade_history.db")

# Import shared modules
sys.path.append(BASE_DIR)
from shared.smart_sleep import get_market_status

# --- Configuration ---
st.set_page_config(
    page_title="Deep Quant Terminal",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="🧬"
)

# --- Utilities ---
def load_css(file_path):
    if os.path.exists(file_path):
        with open(file_path) as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

def get_db_connection():
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5.0)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        return None

def get_data(query, params=None):
    try:
        conn = get_db_connection()
        if conn:
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            return df
        return pd.DataFrame()
    except Exception as e:
        return pd.DataFrame()

def get_config_value(key, default="AUTO"):
    try:
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM system_config WHERE key = ?", (key,))
            row = cursor.fetchone()
            conn.close()
            if row:
                return row['value']
    except Exception as e:
        pass
    return default

def set_config_value(key, value):
    try:
        conn = get_db_connection()
        if conn:
            conn.execute("INSERT OR REPLACE INTO system_config (key, value) VALUES (?, ?)", (key, value))
            conn.commit()
            conn.close()
            return True
    except Exception as e:
        print(f"Error setting config: {e}")
    return False

# --- Data Fetching Functions ---
def get_gpu_load():
    # Estimation based on inference count
    try:
        query = """
            SELECT COUNT(*) as count
            FROM ai_predictions
            WHERE timestamp > datetime('now', '-1 minute')
        """
        df = get_data(query)
        if not df.empty:
            count = df['count'].iloc[0]
            # Assume max 50 symbols per minute = 100% load
            load = min(count * 2, 100)
            return load
        return 0
    except:
        return 0

def get_ticker_tape():
    query = """
        SELECT symbol, close, open, volume
        FROM market_data
        WHERE timeframe = '5m'
        AND timestamp = (SELECT MAX(timestamp) FROM market_data WHERE timeframe = '5m')
        ORDER BY volume DESC
        LIMIT 15
    """
    df = get_data(query)
    if not df.empty:
        df['pct_change'] = ((df['close'] - df['open']) / df['open']) * 100.0
        return df
    return pd.DataFrame()

def get_ensemble_radar():
    # Latest predictions + Latest RSI
    query = """
        WITH LatestPred AS (
            SELECT * FROM ai_predictions
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
            p.small_predicted_price,
            p.large_predicted_price,
            t.rsi_14
        FROM LatestPred p
        LEFT JOIN LatestTech t ON p.symbol = t.symbol
    """
    df = get_data(query)
    if not df.empty:
        # Calculate Conviction Score
        # Magnitude: |(pred - curr) / curr|
        # Agreement: |(large - small) / curr| (Lower is better)

        df['magnitude'] = (df['ensemble_predicted_price'] - df['current_price']) / df['current_price']
        df['agreement_diff'] = abs(df['large_predicted_price'] - df['small_predicted_price']) / df['current_price']

        # Heuristic:
        # Score = (Magnitude * 1000) - (AgreementDiff * 500)
        # We normalize to 0-100
        # This is a rough heuristic for "High Move + High Agreement"

        # Simpler approach as requested:
        # High magnitude + Low divergence = High Conviction

        # Normalized Magnitude (0.0 to 0.05 map to 0-100)
        mag_score = df['magnitude'].abs() * 2000

        # Normalized Agreement (0.0 to 0.02 map to 100-0)
        agree_score = (1 - (df['agreement_diff'] * 50)) * 100

        df['conviction'] = (mag_score + agree_score) / 2
        df['conviction'] = df['conviction'].clip(0, 100)

        # Direction
        df['direction'] = df.apply(lambda x: 'UP' if x['ensemble_predicted_price'] > x['current_price'] else 'DOWN', axis=1)

        # Agreement Icon
        def get_icon(row):
            s_dir = 1 if row['small_predicted_price'] > row['current_price'] else -1
            l_dir = 1 if row['large_predicted_price'] > row['current_price'] else -1
            return '🤝' if s_dir == l_dir else '⚠️'

        df['agreement'] = df.apply(get_icon, axis=1)

        return df.sort_values(by='conviction', ascending=False)

    return pd.DataFrame()

def get_technical_heatmap():
    query = """
        SELECT symbol, rsi_14, sma_50, sma_200, timestamp
        FROM technical_indicators
        WHERE timestamp = (SELECT MAX(timestamp) FROM technical_indicators)
        ORDER BY rsi_14 ASC
    """
    return get_data(query)

def get_chart_data(symbol):
    query = """
        SELECT m.timestamp, m.open, m.high, m.low, m.close, t.sma_200, t.sma_50, t.rsi_14
        FROM market_data m
        LEFT JOIN technical_indicators t ON m.symbol = t.symbol AND m.timestamp = t.timestamp
        WHERE m.symbol = ? AND m.timeframe = '5m'
        ORDER BY m.timestamp DESC
        LIMIT 200
    """
    df = get_data(query, params=(symbol,))
    if not df.empty:
        return df.sort_values(by='timestamp', ascending=True)
    return pd.DataFrame()

def get_system_logs():
    query = """
        SELECT timestamp, service_name, log_level, message
        FROM system_logs
        ORDER BY timestamp DESC
        LIMIT 50
    """
    return get_data(query)

def get_ledger():
    query = "SELECT * FROM executed_trades ORDER BY timestamp DESC LIMIT 20"
    return get_data(query)

def get_active_signals():
    query = "SELECT * FROM trade_signals WHERE status IN ('PENDING', 'SIZED') ORDER BY timestamp DESC"
    return get_data(query)

# --- Renderers ---

def render_sidebar():
    st.sidebar.markdown("## 🧬 QUANT TERMINAL")

    # --- System Power Mode Control ---
    current_mode = get_config_value("sleep_mode", "AUTO")

    mode_map = {
        "AUTO": "🤖 AUTO",
        "FORCE_AWAKE": "⚡ FORCE AWAKE",
        "FORCE_SLEEP": "🌙 FORCE SLEEP"
    }
    reverse_mode_map = {v: k for k, v in mode_map.items()}

    selected_label = st.sidebar.radio(
        "SYSTEM POWER MODE",
        options=list(mode_map.values()),
        index=list(mode_map.keys()).index(current_mode) if current_mode in mode_map else 0,
        key="power_mode_radio"
    )

    # Handle Change
    new_mode = reverse_mode_map[selected_label]
    if new_mode != current_mode:
        set_config_value("sleep_mode", new_mode)
        st.rerun()

    status = get_market_status()
    status_color = "status-open" if status['is_open'] else "status-closed"
    st.sidebar.markdown(f"""
        <div style="display: flex; align-items: center; margin-bottom: 5px;">
            <span class="{status_color} status-dot"></span>
            <span style="font-weight: bold; color: #E5E7EB;">{status['status_message'].split(' - ')[0]}</span>
        </div>
        <div style="font-size: 0.8em; color: #9CA3AF; margin-bottom: 20px;">
            Interval: {status['sleep_seconds']}s
        </div>
    """, unsafe_allow_html=True)

    st.sidebar.divider()

    st.sidebar.markdown("### 🖥️ TELEMETRY")
    load = get_gpu_load()
    st.sidebar.markdown(f"""
        <div style="font-size: 0.8em; color: #9CA3AF; margin-bottom: 5px;">RTX 5050 INF LOAD</div>
        <div class="stProgress">
            <div style="width: 100%; background-color: #374151; height: 8px; border-radius: 4px;">
                <div style="width: {load}%; background: linear-gradient(90deg, #00F0FF, #FF00FF); height: 100%; border-radius: 4px;"></div>
            </div>
        </div>
        <div style="text-align: right; font-size: 0.7em; color: #00F0FF; margin-top: 2px;">{int(load)}%</div>
    """, unsafe_allow_html=True)

    st.sidebar.divider()

    # Symbol Selector
    symbols = get_data("SELECT DISTINCT symbol FROM market_data ORDER BY symbol")
    if not symbols.empty:
        symbol_list = symbols['symbol'].tolist()
        # Default to first if not in state
        if 'selected_symbol' not in st.session_state:
            st.session_state.selected_symbol = symbol_list[0]

        st.session_state.selected_symbol = st.sidebar.selectbox(
            "ASSET FOCUS",
            symbol_list,
            index=symbol_list.index(st.session_state.selected_symbol) if st.session_state.selected_symbol in symbol_list else 0
        )

def render_ticker_tape():
    df = get_ticker_tape()
    if not df.empty:
        items = []
        for row in df.itertuples():
            color_class = "ticker-up" if row.pct_change >= 0 else "ticker-down"
            sign = "+" if row.pct_change >= 0 else ""
            html = f"""
                <div class="ticker-item">
                    <span style="font-weight: bold; color: #FFF;">{row.symbol}</span>
                    <span style="color: #9CA3AF;">${row.close:.2f}</span>
                    <span class="{color_class}">{sign}{row.pct_change:.2f}%</span>
                </div>
            """
            items.append(html)

        separator = '<div style="width: 20px;"></div>'
        full_html = f"""
            <div class="ticker-container">
                {separator.join(items)}
            </div>
        """
        # Bug 2 Fix: unsafe_allow_html=True is mandatory here
        st.markdown(full_html, unsafe_allow_html=True)

def render_radar(df):
    st.markdown("### 🔮 PREDICTION RADAR")
    if not df.empty:
        # Configure AgGrid
        gb = GridOptionsBuilder.from_dataframe(df[['symbol', 'current_price', 'ensemble_predicted_price', 'conviction', 'agreement', 'direction']])
        gb.configure_column("symbol", header_name="SYM", width=80, pinned="left")
        gb.configure_column("current_price", header_name="PRICE", width=100, type=["numericColumn"], valueFormatter="x.toFixed(2)")
        gb.configure_column("ensemble_predicted_price", header_name="TARGET (T+30)", width=120, type=["numericColumn"], valueFormatter="x.toFixed(2)")
        gb.configure_column("agreement", header_name="AGR", width=60)
        gb.configure_column("direction", header_name="DIR", width=80, cellStyle=JsCode("""
            function(params) {
                if (params.value == 'UP') {
                    return {'color': '#00FF94', 'font-weight': 'bold'};
                } else {
                    return {'color': '#FF3B30', 'font-weight': 'bold'};
                }
            }
        """))

        # Custom Progress Bar for Conviction
        gb.configure_column("conviction", header_name="CONVICTION", width=150, cellRenderer=JsCode("""
            class ProgressCellRenderer {
                init(params) {
                    this.eGui = document.createElement('div');
                    this.eGui.style.width = '100%';
                    this.eGui.style.height = '100%';
                    this.eGui.style.display = 'flex';
                    this.eGui.style.alignItems = 'center';

                    let value = params.value;
                    let color = value > 70 ? '#00FF94' : (value > 40 ? '#FFC400' : '#FF3B30');

                    this.eGui.innerHTML = `
                        <div style="width: 100%; background-color: #374151; height: 6px; border-radius: 3px;">
                            <div style="width: ${value}%; background-color: ${color}; height: 100%; border-radius: 3px;"></div>
                        </div>
                        <span style="margin-left: 5px; font-size: 0.8em;">${Math.round(value)}%</span>
                    `;
                }
                getGui() { return this.eGui; }
            }
        """))

        gb.configure_selection('single')
        gridOptions = gb.build()

        grid_response = AgGrid(
            df,
            gridOptions=gridOptions,
            allow_unsafe_jscode=True,
            height=400,
            theme='alpine-dark',
            update_mode='SELECTION_CHANGED',
            fit_columns_on_grid_load=True
        )

        # Bug 3 Fix: Handle AgGrid selection robustness
        selected_rows = grid_response['selected_rows']

        # Check if selected_rows exists AND has at least one row
        # (Handling DataFrame emptiness or List emptiness)
        if selected_rows is not None and len(selected_rows) > 0:
            import pandas as pd
            # Handle if AgGrid returns a DataFrame (new versions)
            if isinstance(selected_rows, pd.DataFrame):
                # Ensure we are not accessing an empty dataframe
                if not selected_rows.empty:
                    # Using iloc[0] to get the first row, then getting the 'symbol' column
                    # This assumes 'symbol' is a column name.
                    selected_ticker = selected_rows.iloc[0]['symbol']
                    if selected_ticker != st.session_state.selected_symbol:
                        st.session_state.selected_symbol = selected_ticker
                        st.rerun()
            # Handle if AgGrid returns a list of dicts (older versions)
            elif isinstance(selected_rows, list):
                selected_ticker = selected_rows[0].get('symbol')
                if selected_ticker and selected_ticker != st.session_state.selected_symbol:
                    st.session_state.selected_symbol = selected_ticker
                    st.rerun()

    else:
        st.info("No predictions available.")

def render_chart(symbol, radar_df):
    if not symbol:
        st.info("Select a symbol.")
        return

    df = get_chart_data(symbol)
    if df.empty:
        st.warning(f"No data for {symbol}")
        return

    # Get prediction for this symbol
    pred_row = radar_df[radar_df['symbol'] == symbol] if not radar_df.empty else pd.DataFrame()

    st.markdown(f"### 📈 {symbol} ANALYSIS")

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.75, 0.25], vertical_spacing=0.03)

    # Candlestick
    fig.add_trace(go.Candlestick(
        x=df['timestamp'],
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        name='Price',
        increasing_line_color='#00FF94',
        decreasing_line_color='#FF3B30'
    ), row=1, col=1)

    # SMAs
    fig.add_trace(go.Scatter(
        x=df['timestamp'], y=df['sma_200'],
        line=dict(color='rgba(255, 255, 255, 0.6)', width=2), name='SMA 200'
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=df['timestamp'], y=df['sma_50'],
        line=dict(color='rgba(0, 229, 255, 0.6)', width=1), name='SMA 50'
    ), row=1, col=1)

    # Chronos T+30 Prediction Line
    if not pred_row.empty:
        target_price = pred_row.iloc[0]['ensemble_predicted_price']
        last_time = pd.to_datetime(df['timestamp'].iloc[-1])
        future_time = last_time + datetime.timedelta(minutes=30)

        fig.add_trace(go.Scatter(
            x=[last_time, future_time],
            y=[df['close'].iloc[-1], target_price],
            mode='lines',
            line=dict(color='#E5E7EB', width=2, dash='dot'),
            name='T+30 Forecast'
        ), row=1, col=1)

        # Add marker at target
        fig.add_trace(go.Scatter(
            x=[future_time], y=[target_price],
            mode='markers',
            marker=dict(color='#00FF94' if target_price > df['close'].iloc[-1] else '#FF3B30', size=8),
            showlegend=False
        ), row=1, col=1)

    # RSI
    fig.add_trace(go.Scatter(
        x=df['timestamp'], y=df['rsi_14'],
        line=dict(color='#AB47BC', width=1.5), name='RSI'
    ), row=2, col=1)

    fig.add_hline(y=70, line_dash="dot", line_color="#FF3B30", row=2, col=1)
    fig.add_hline(y=35, line_dash="dot", line_color="#00FF94", row=2, col=1)

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=0, r=0, t=0, b=0),
        height=500,
        showlegend=False,
        xaxis_rangeslider_visible=False
    )

    # Hide grid
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=False)

    # Bug 1 Fix: Rangebreaks for dead air
    fig.update_xaxes(
        rangebreaks=[
            dict(bounds=["sat", "mon"]), # Hide weekends
            dict(bounds=[16, 9.5], pattern="hour"), # Hide after hours (assuming NY 16:00 to 09:30)
        ]
    )

    # Deprecation Fix: use_container_width -> width="stretch" (Not strictly valid for plotly_chart yet, sticking to use_container_width=True based on docs, but user asked for fix. Actually use_container_width is correct for st.plotly_chart. The warning was for st.dataframe. I will fix st.dataframe.)
    st.plotly_chart(fig, config={'displayModeBar': False})

def render_heatmap():
    st.markdown("### 🔥 RSI HEATMAP")
    df = get_technical_heatmap()
    if not df.empty:
        # Configure AgGrid for Heatmap to enable selection
        gb = GridOptionsBuilder.from_dataframe(df[['symbol', 'rsi_14', 'sma_50', 'sma_200']])
        gb.configure_column("symbol", header_name="SYM", width=80, pinned="left")
        gb.configure_column("rsi_14", header_name="RSI", width=80, type=["numericColumn"], valueFormatter="x.toFixed(1)", cellStyle=JsCode("""
            function(params) {
                if (params.value > 70) {
                    return {'backgroundColor': '#FF1744', 'color': 'white', 'fontWeight': 'bold'};
                } else if (params.value < 35) {
                    return {'backgroundColor': '#00E676', 'color': 'black', 'fontWeight': 'bold'};
                }
                return {'color': '#9CA3AF'};
            }
        """))
        gb.configure_column("sma_50", header_name="SMA50", width=90, type=["numericColumn"], valueFormatter="x.toFixed(2)")
        gb.configure_column("sma_200", header_name="SMA200", width=90, type=["numericColumn"], valueFormatter="x.toFixed(2)")

        gb.configure_selection('single')
        gridOptions = gb.build()

        grid_response = AgGrid(
            df,
            gridOptions=gridOptions,
            allow_unsafe_jscode=True,
            height=300,
            theme='alpine-dark',
            update_mode='SELECTION_CHANGED',
            fit_columns_on_grid_load=True,
            key='heatmap_grid'
        )

        # Bug 3 Fix: Same fix for Heatmap
        selected_rows = grid_response['selected_rows']

        if selected_rows is not None and len(selected_rows) > 0:
            import pandas as pd
            if isinstance(selected_rows, pd.DataFrame):
                if not selected_rows.empty:
                    selected_ticker = selected_rows.iloc[0]['symbol']
                    if selected_ticker != st.session_state.selected_symbol:
                        st.session_state.selected_symbol = selected_ticker
                        st.rerun()
            elif isinstance(selected_rows, list):
                selected_ticker = selected_rows[0].get('symbol')
                if selected_ticker and selected_ticker != st.session_state.selected_symbol:
                    st.session_state.selected_symbol = selected_ticker
                    st.rerun()
    else:
        st.info("No technical data.")

def render_logs():
    logs = get_system_logs()

    html_content = ["""
        <div style="background-color: #000; color: #00FF00; font-family: 'JetBrains Mono'; font-size: 0.8em; padding: 10px; height: 300px; overflow-y: auto; border: 1px solid #333;">
    """]

    if not logs.empty:
        for _, row in logs.iterrows():
            color = "#00FF00" # Default info
            if row['log_level'] == 'ERROR': color = "#FF1744"
            if row['log_level'] == 'WARNING': color = "#FFC400"

            try:
                ts = row['timestamp'].split('T')[1].split('.')[0] # Extract HH:MM:SS
            except:
                ts = row['timestamp']

            html_content.append(f"<div style='color: {color};'>[{ts}] [{row['service_name']}] {row['message']}</div>")
    else:
        html_content.append("<div>Waiting for system logs...</div>")

    html_content.append("</div>")

    st.markdown("".join(html_content), unsafe_allow_html=True)

# --- Main Layout ---
def main():
    # 5-second Auto Refresh
    st_autorefresh(interval=5000, limit=None, key="market_pulse")

    load_css(os.path.join(os.path.dirname(__file__), "style.css"))

    render_sidebar()
    render_ticker_tape()

    # Quad Layout
    # Row 1
    c1, c2 = st.columns([4, 6]) # 40% / 60% split

    radar_data = get_ensemble_radar()

    with c1:
        render_radar(radar_data)

    with c2:
        selected_symbol = st.session_state.get('selected_symbol', None)
        render_chart(selected_symbol, radar_data)

    st.divider()

    # Row 2
    c3, c4 = st.columns([4, 6])

    with c3:
        render_heatmap()

    with c4:
        st.markdown("### 🎛️ SYSTEM CONTROL")
        tab1, tab2, tab3 = st.tabs(["ACTIVE SIGNALS", "EXECUTION LEDGER", "SWARM LOGS"])

        with tab1:
            st.dataframe(get_active_signals(), height=300)

        with tab2:
            st.dataframe(get_ledger(), height=300)

        with tab3:
            render_logs()

if __name__ == "__main__":
    main()
