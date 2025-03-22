import streamlit as st
import pandas as pd
import sqlite3
import requests
import os
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
# Database setup
db_path = "bhavcopy_data.db"

# Create table if not exists
def setup_database():
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS bhavcopy (
                            date TEXT,
                            symbol TEXT,
                            series TEXT,
                            deliv_per REAL,
                            deliv_qty INTEGER,
                            ttl_trd_qnty INTEGER,
                            no_of_trades INTEGER,  -- ✅ Added column here
                            close_price REAL,
                            PRIMARY KEY (date, symbol)
                        )''')
        conn.commit()

setup_database()  # Ensure DB is set up at the start

# Function to check if a date is a trading day
def is_trading_day(date):
    return date.weekday() < 5  # Monday to Friday are trading days

# Function to download Bhavcopy
def download_bhavcopy(date):
    """Download NSE Bhavcopy CSV. If today's file is missing, fetch the latest available file."""
    attempts = 3  # Maximum attempts to find the latest available Bhavcopy
    
    while attempts > 0:
        date_str = date.strftime("%d%m%Y")  # Correct format
        url = f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv"
        file_path = f"bhavcopy_{date_str}.csv"

        if os.path.exists(file_path):
            return file_path  # Use cached file

        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                with open(file_path, "wb") as f:
                    f.write(response.content)
                st.success(f"✅ Successfully downloaded Bhavcopy for {date.strftime('%d-%m-%Y')}")
                return file_path
            else:
                print(f"⚠️ Bhavcopy not available for {date.strftime('%d-%m-%Y')}. Trying previous trading day...")
                date -= timedelta(days=1)  # Move to the previous day
                while not is_trading_day(date):  # Skip weekends
                    date -= timedelta(days=1)
                attempts -= 1  # Reduce attempts
        except requests.RequestException as e:
            st.error(f"❌ Error downloading Bhavcopy: {e}")
            return None

    st.warning("❌ No recent Bhavcopy available.")
    return None


# Function to process and store data
def process_bhavcopy(file_path, date, db_path):
    try:
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip().str.upper()  # Normalize column names
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)  # Remove spaces in values

        print("🔍 CSV Columns:", df.columns.tolist())  
        st.write("🔍 CSV Columns:", df.columns.tolist())

        # ✅ Rename DATE1 → DATE
        if "DATE1" in df.columns:
            df.rename(columns={"DATE1": "DATE"}, inplace=True)

        # ✅ Convert DATE to YYYY-MM-DD format
        if "DATE" in df.columns:
            df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce").dt.strftime("%Y-%m-%d")

        # ✅ Check for missing columns
        # Add NO_OF_TRADES to required columns
        required_cols = {"SYMBOL", "SERIES", "DELIV_PER", "DELIV_QTY", "TTL_TRD_QNTY", "CLOSE_PRICE", "NO_OF_TRADES", "DATE"}
        missing_cols = required_cols - set(df.columns)

        if missing_cols:
            st.error(f"❌ Missing columns in Bhavcopy file: {missing_cols}")
            return None

        # ✅ Show raw data before filtering
        print("🔹 Raw Data Before Filtering:")
        st.write(df.head())

        # ✅ Check unique SERIES values
        print("🔹 Unique SERIES values:", df["SERIES"].unique())

        # ✅ Filter only EQ stocks (ensure no spaces in SERIES column)
        df = df[df["SERIES"] == "EQ"]

        # ✅ Fix Delivery Percentage Issue
        print("🔹 DELIV_PER Column Before Conversion:")
        print(df["DELIV_PER"].head(10))  # Debugging line

        df["DELIV_PER"] = df["DELIV_PER"].astype(str).str.replace(",", "").str.replace("%", "").astype(float)
        df["DELIV_QTY"] = pd.to_numeric(df["DELIV_QTY"], errors="coerce")

        # ✅ Drop NaN delivery percentage values
        df = df.dropna(subset=["DELIV_PER"])

        # ✅ Show min & max delivery percentage
        print("🔹 Min & Max DELIV_PER:", df["DELIV_PER"].min(), df["DELIV_PER"].max())

        # ✅ Filter stocks with delivery percentage >60%
        df = df[df["DELIV_PER"] > 60]

        if df.empty:
            st.warning("⚠️ No stocks found with DELIV_PER > 60%. Try lowering the threshold.")
            return None

        # ✅ Select required columns
        df = df[["DATE", "SYMBOL", "SERIES", "DELIV_PER", "DELIV_QTY", "TTL_TRD_QNTY", "CLOSE_PRICE", "NO_OF_TRADES"]]

        # ✅ Debugging: Show DataFrame before inserting
        st.write("🔹 DataFrame before inserting into DB:", df.head())

        # ✅ Insert into SQLite database (prevent duplicates)
        with sqlite3.connect(db_path) as conn:
            existing_records = pd.read_sql("SELECT date, symbol FROM bhavcopy", conn)
            df = df[~df[["DATE", "SYMBOL"]].apply(tuple, axis=1).isin(existing_records.apply(tuple, axis=1))]
            if not df.empty:
                df.to_sql("bhavcopy", conn, if_exists="append", index=False)

        return df
    except Exception as e:
        st.error(f"Error processing Bhavcopy file: {e}")
        return None

# Function to get accumulation stocks
def get_accumulation_stocks(days=5):
    try:
        with sqlite3.connect(db_path) as conn:
            query = f'''
                SELECT symbol, 
                       AVG(deliv_per) AS avg_deliv_per, 
                       AVG(deliv_qty) AS avg_deliv_qty, 
                       AVG(no_of_trades) AS avg_trades
                FROM bhavcopy
                WHERE date >= date('now', '-{days} days')
                GROUP BY symbol
                HAVING avg_deliv_per > 60 AND avg_trades > 100000  -- ✅ Only include liquid stocks
                ORDER BY avg_deliv_qty DESC
            '''
            return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Error fetching accumulation stocks: {e}")
        return pd.DataFrame()

# Streamlit UI
st.title("NSE Bhavcopy Analysis - High Delivery & Accumulation")

# Fetch last 30 days' data
days = 30
for i in range(days):
    date = datetime.today() - timedelta(days=i)

    file_path = download_bhavcopy(date)
    
    if file_path:
        df = process_bhavcopy(file_path, date, db_path)  # ✅ Corrected
        if df is not None:
            st.write(f"✅ Processed data for {date.strftime('%Y-%m-%d')} successfully.")
        else:
            st.warning(f"❌ Processing failed for {date.strftime('%Y-%m-%d')}.")

# Verify Database Insertion
with sqlite3.connect(db_path) as conn:
    verify_df = pd.read_sql("SELECT * FROM bhavcopy LIMIT 5", conn)
    st.write("🔹 Sample data from database:", verify_df)

# Display results
st.subheader("Stocks with High Delivery Percentage & Accumulation")
accumulation_df = get_accumulation_stocks(days)

if not accumulation_df.empty:
    st.dataframe(accumulation_df)
else:
    st.warning("No accumulation stocks found.")

# Send Telegram Alert for top accumulation stocks
if not accumulation_df.empty:
    try:
        top_stock = accumulation_df.iloc[0]["symbol"]
        message = f"🚀 High Accumulation Alert: {top_stock}\nAverage Delivery %: {accumulation_df.iloc[0]['avg_deliv_per']:.2f}%\nAverage Delivery Volume: {accumulation_df.iloc[0]['avg_deliv_qty']:.2f}"
        send_telegram_alert(message)
    except Exception as e:
        st.error(f"Error in sending Telegram alert: {e}")

# Plot accumulation trends
st.subheader("Accumulation Trend of Selected Stock")

# Fetch unique stocks from database
with sqlite3.connect(db_path) as conn:
    stock_list = pd.read_sql("SELECT DISTINCT symbol FROM bhavcopy", conn)["symbol"].tolist()

# User selects a stock from dropdown
selected_stock = st.selectbox("📌 Select a stock to view accumulation trend:", stock_list)

if selected_stock:
    with sqlite3.connect(db_path) as conn:
        trend_df = pd.read_sql(f"SELECT date, deliv_per, deliv_qty FROM bhavcopy WHERE symbol = '{selected_stock}' ORDER BY date", conn)

    if not trend_df.empty:
        # Convert 'date' column to proper datetime format
        trend_df["date"] = pd.to_datetime(trend_df["date"], format="%Y-%m-%d")
        
        fig, ax1 = plt.subplots(figsize=(12, 6))
        ax2 = ax1.twinx()
        
        # Plot Delivery %
        ax1.plot(trend_df["date"], trend_df["deliv_per"], marker='o', linestyle='-', color='blue', label='Delivery %')
        
        # Plot Delivery Quantity
        ax2.plot(trend_df["date"], trend_df["deliv_qty"], marker='s', linestyle='--', color='red', label='Delivery Volume')
        
        # Set labels and title
        ax1.set_xlabel("Date")
        ax1.set_ylabel("Delivery %", color='blue')
        ax2.set_ylabel("Delivery Volume", color='red')
        ax1.set_title(f"Accumulation Trend for {selected_stock}")
        
        # Create a custom date formatter that uses short month names with just 1 character
        def custom_date_fmt(x, pos=None):
            date = mdates.num2date(x)
            if date.day == 1:   # Show full format for 1st of month
                return date.strftime('%d-%b')
            else:
                return date.strftime('%d')  # Only show day number for other dates
        
        ax1.xaxis.set_major_locator(mdates.DayLocator())
        ax1.xaxis.set_major_formatter(plt.FuncFormatter(custom_date_fmt))
        
        # Rotate the labels
        plt.xticks(rotation=90, fontsize=8)
        
        # Add extra space at the bottom
        plt.subplots_adjust(bottom=0.2)
        
        # Add legend
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
        
        ax1.grid()
        
        st.pyplot(fig)
    else:
        st.warning(f"No trend data available for {selected_stock}.")

