import pandas as pd
import sqlite3
import requests
import os
from datetime import datetime

# Database file
DB_FILE = "bhavcopy.db"

# NSE Bhavcopy URL (Modify date dynamically)
def get_bhavcopy_url():
    today = datetime.today().strftime("%d%m%Y")
    return f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{today}.csv"

# Function to download Bhavcopy
def download_bhavcopy():
    url = get_bhavcopy_url()
    file_path = f"sec_bhavdata_full_{datetime.today().strftime('%Y%m%d')}.csv"
    
    response = requests.get(url)
    if response.status_code == 200:
        with open(file_path, "wb") as f:
            f.write(response.content)
        return file_path
    else:
        print("Failed to download Bhavcopy")
        return None

# Function to store Bhavcopy in SQLite
def process_bhavcopy(file_path):
    df = pd.read_csv(file_path)

    # Ensure required columns exist
    if not {"SYMBOL", "SERIES", "DELIV_QTY", "TOT_TRD_QTY", "DELIV_PER"}.issubset(df.columns):
        raise ValueError("Missing required columns in Bhavcopy file!")

    # Convert numeric fields
    df["DELIV_QTY"] = pd.to_numeric(df["DELIV_QTY"], errors="coerce")
    df["TOT_TRD_QTY"] = pd.to_numeric(df["TOT_TRD_QTY"], errors="coerce")
    df["DELIV_PER"] = pd.to_numeric(df["DELIV_PER"], errors="coerce")

    # Filter for SERIES = EQ and Delivery % > 60%
    df_filtered = df[(df["SERIES"] == "EQ") & (df["DELIV_PER"] > 60)]

    # Add date column
    df_filtered["date"] = datetime.today().strftime("%Y-%m-%d")

    # Select relevant columns
    df_filtered = df_filtered[["date", "SYMBOL", "SERIES", "DELIV_QTY", "TOT_TRD_QTY", "DELIV_PER"]]

    # Store in SQLite database
    conn = sqlite3.connect(DB_FILE)
    df_filtered.to_sql("bhavcopy", conn, if_exists="append", index=False)
    conn.close()

    print(f"Processed {len(df_filtered)} stocks for {datetime.today().strftime('%Y-%m-%d')}.")

# Automate the workflow
file_path = download_bhavcopy()
if file_path:
    process_bhavcopy(file_path)
