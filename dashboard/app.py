import sqlite3
import pandas as pd
import streamlit as st
import os

# Define database path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "trade_history.db")

def get_db_connection():
    """Create a database connection to the SQLite database specified by DB_PATH."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        st.error(f"Error connecting to database: {e}")
        return None

def fetch_data(query):
    """Fetch data from the database using the provided SQL query."""
    conn = get_db_connection()
    if conn:
        try:
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
        except pd.io.sql.DatabaseError as e:
            st.error(f"Error executing query: {e}")
            conn.close()
            return pd.DataFrame()
    return pd.DataFrame()

def main():
    st.set_page_config(page_title="Trading Dashboard", layout="wide")
    st.title("Trading Dashboard")

    # Section 1: Latest Executed Trades
    st.header("Latest Executed Trades")
    try:
        trades_query = "SELECT * FROM executed_trades ORDER BY timestamp DESC LIMIT 100"
        trades_df = fetch_data(trades_query)
        if not trades_df.empty:
             st.table(trades_df) # Use st.table for a static view
        else:
            st.info("No executed trades found.")
    except Exception as e:
        st.error(f"An error occurred while fetching executed trades: {e}")

    # Section 2: News Sentiment History
    st.header("News Sentiment History")
    try:
        news_query = "SELECT timestamp, sentiment_score FROM raw_news ORDER BY timestamp DESC LIMIT 100"
        news_df = fetch_data(news_query)
        if not news_df.empty:
            # Set timestamp as index for the chart
            news_df['timestamp'] = pd.to_datetime(news_df['timestamp'])
            news_df = news_df.set_index('timestamp').sort_index()
            st.line_chart(news_df['sentiment_score'])
        else:
             st.info("No news sentiment data found.")
    except Exception as e:
        st.error(f"An error occurred while fetching news sentiment: {e}")

    # Section 3: Technical Indicators (RSI & Lower BB)
    st.header("Technical Indicators (RSI & Lower BB)")
    try:
        tech_query = "SELECT symbol, timestamp, rsi_14, lower_bb FROM technical_indicators ORDER BY timestamp DESC LIMIT 100"
        tech_df = fetch_data(tech_query)
        if not tech_df.empty:
            st.dataframe(tech_df) # Use st.dataframe for interactive view
        else:
            st.info("No technical indicator data found.")
    except Exception as e:
        st.error(f"An error occurred while fetching technical indicators: {e}")

if __name__ == "__main__":
    main()
