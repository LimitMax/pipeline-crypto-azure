import yfinance as yf
import pandas as pd
import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import pyodbc

# ===============================
# CONFIG
# ===============================
CRYPTOS = ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "DOGE-USD","ADA-USD","BNB-USD"]
START = "2025-08-24"
END   = "2025-09-28"
INTERVAL = "1h"

CONTAINER_NAME = "crypto-raw"   # pastikan sudah ada di Blob

# Load .env
load_dotenv()

BLOB_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
SQL_SERVER    = os.getenv("SQL_SERVER")
SQL_DATABASE  = os.getenv("SQL_DATABASE")
SQL_USERNAME  = os.getenv("SQL_USERNAME")
SQL_PASSWORD  = os.getenv("SQL_PASSWORD")
DRIVER        = '{ODBC Driver 18 for SQL Server}'

# ===============================
# UTILS
# ===============================

def connect_blob():
    return BlobServiceClient.from_connection_string(BLOB_CONN_STR)

def connect_sql():
    return pyodbc.connect(
        f"DRIVER={DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};PWD={SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
    )

def fetch_and_save(symbol, start, end, interval="1h"):
    print(f"🔄 Fetching {symbol}...")
    df = yf.download(symbol, start=start, end=end, interval=interval)

    df.reset_index(inplace=True)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if c[0] != '' else c[1] for c in df.columns]

    # Tambah kolom custom
    df['date'] = df['Datetime'].dt.date
    df['hourx'] = df['Datetime'].dt.hour
    df['crypto'] = symbol

    curated = df[['date','hourx','crypto','Open','High','Low','Close','Volume']]

    #Simpan CSV bersih (hanya 1 header)
    file_name = f"data/{symbol}_backfill.csv"
    curated.to_csv(file_name, index=False, header=True, sep=",", encoding="utf-8")
    print(f"Saved clean CSV: {file_name}")

    return file_name


def upload_to_blob(file_name):
    blob_service = connect_blob()
    container = blob_service.get_container_client(CONTAINER_NAME)
    blob_name = f"bulkload/{file_name}"

    with open(file_name, "rb") as data:
        container.upload_blob(name=blob_name, data=data, overwrite=True)

    print(f"Uploaded to Blob: {blob_name}")
    return blob_name

def bulk_insert_sql(blob_name):
    conn = connect_sql()
    cursor = conn.cursor()

    # 1. Bulk load ke staging table
    query = f"""
    BULK INSERT CryptoPrice_staging
    FROM '{blob_name}'
    WITH (
        DATA_SOURCE = 'MyAzureBlob',
        FORMAT = 'CSV',
        FIRSTROW = 2,  -- skip header
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a',
        TABLOCK
    );
    """
    print(f"⚡ Running BULK INSERT into CryptoPrice_staging for {blob_name}...")
    cursor.execute(query)
    conn.commit()

    # 2. Insert unik ke main table
    dedup_query = """
    INSERT INTO CryptoPrice ([date],[hourx],[crypto],[Open],[High],[Low],[Close],[Volume])
    SELECT s.[date], s.[hourx], s.[crypto], s.[Open], s.[High], s.[Low], s.[Close], s.[Volume]
    FROM CryptoPrice_staging s
    WHERE NOT EXISTS (
        SELECT 1 FROM CryptoPrice c
        WHERE c.[date] = s.[date]
          AND c.[hourx] = s.[hourx]
          AND c.[crypto] = s.[crypto]
    );
    """
    print("🧹 Deduplicating and inserting into main table...")
    cursor.execute(dedup_query)
    conn.commit()

    # 3. Kosongkan staging setelah dipakai
    cursor.execute("TRUNCATE TABLE CryptoPrice_staging;")
    conn.commit()

    cursor.close()
    conn.close()
    print(f"Bulk load + dedup insert completed for {blob_name}")

# ===============================
# MAIN
# ===============================

def main():
    for symbol in CRYPTOS:
        local_csv = fetch_and_save(symbol, START, END, INTERVAL)
        blob_name = upload_to_blob(local_csv)
        bulk_insert_sql(blob_name)

    print("All bulk inserts completed!")

if __name__ == "__main__":
    main()
