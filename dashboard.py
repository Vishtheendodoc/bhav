import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px

# Database file
DB_FILE = "bhavcopy.db"

# Function to fetch filtered stocks
def get_filtered_stocks(volume_threshold=0, n_days=3):
    conn = sqlite3.connect(DB_FILE)
    query = f"""
        SELECT symbol, date, deliv_per, total_traded_qty
        FROM bhavcopy 
        WHERE series = 'EQ' AND deliv_per > 60
        ORDER BY symbol, date
    """
    df = pd.read_sql(query, conn)
    conn.close()

    # Apply volume filter
    df = df[df["total_traded_qty"] >= volume_threshold]

    # Identify accumulating stocks (increasing delivery %)
    accumulating_stocks = []
    for symbol in df["symbol"].unique():
        stock_df = df[df["symbol"] == symbol].sort_values("date")
        if stock_df["deliv_per"].is_monotonic_increasing:  # Check if increasing
            accumulating_stocks.append(symbol)

    return df, accumulating_stocks

# Streamlit UI
st.title("📊 Bhavcopy Stock Screener")
st.sidebar.header("Filters")

# Volume filter
volume_threshold = st.sidebar.number_input("Minimum Volume (TOT_TRD_QTY)", min_value=0, value=1000000, step=100000)

# Fetch filtered stocks
df, accumulating_stocks = get_filtered_stocks(volume_threshold)

# Show filtered stocks
st.subheader("📌 Filtered Stocks")
st.dataframe(df)

# Show accumulating stocks
st.subheader("📈 Accumulating Stocks")
st.write(accumulating_stocks if accumulating_stocks else "No stocks showing increasing delivery percentage trend.")

# Stock selection for trend visualization
selected_stock = st.selectbox("Select a Stock for Trend Analysis", df["symbol"].unique() if not df.empty else [])

if selected_stock:
    stock_df = df[df["symbol"] == selected_stock].sort_values("date")
    fig = px.line(stock_df, x="date", y="deliv_per", title=f"{selected_stock} - Delivery Percentage Trend")
    st.plotly_chart(fig)
