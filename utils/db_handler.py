import datetime
import pyodbc
import pandas as pd
from contextlib import contextmanager
from utils import config
from utils.logger import logger
from utils.news_coin_mapper import detect_coin


# --- Context Manager untuk koneksi SQL ---
@contextmanager
def sql_cursor():
    """Context manager untuk koneksi SQL, otomatis close"""
    conn = pyodbc.connect(config.SQL_CONN_STRING, autocommit=True)
    try:
        yield conn.cursor()
    finally:
        conn.close()


# --- Watermark Management ---
def get_last_success(cursor, source):
    """Ambil watermark terakhir dari IngestionMetadata, fallback ke MAX dari CryptoPrice"""
    cursor.execute("SELECT last_success FROM IngestionMetadata WHERE source=?", source)
    row = cursor.fetchone()
    if row and row[0]:
        return row[0]

    cursor.execute("""
        SELECT MAX(DATEADD(HOUR, hourx, CAST(date AS DATETIME2))) 
        FROM CryptoPrice WHERE crypto=?""", source)
    row2 = cursor.fetchone()
    return row2[0] if row2 and row2[0] else datetime.datetime(2024, 1, 1)


def update_last_success(cursor, source, new_ts):
    """Update watermark di IngestionMetadata"""
    cursor.execute("""
        MERGE IngestionMetadata AS target
        USING (SELECT ? AS source, ? AS last_success) AS src
        ON target.source = src.source
        WHEN MATCHED THEN 
            UPDATE SET last_success=src.last_success, updated_at=GETDATE()
        WHEN NOT MATCHED THEN 
            INSERT (source, last_success) VALUES (src.source, src.last_success);
    """, source, new_ts)


# --- Insert Incremental Data (CryptoPrice) ---
def insert_incremental(cursor, df, crypto):
    """Insert incremental data ke CryptoPrice"""
    if df.empty:
        return 0

    rows = [
        (
            row['date'],
            int(row['hourx']) if pd.notna(row['hourx']) else None,
            str(row['crypto']),
            float(row['Open']) if pd.notna(row['Open']) else None,
            float(row['High']) if pd.notna(row['High']) else None,
            float(row['Low']) if pd.notna(row['Low']) else None,
            float(row['Close']) if pd.notna(row['Close']) else None,
            int(row['Volume']) if pd.notna(row['Volume']) else None,
            row['date'], int(row['hourx']) if pd.notna(row['hourx']) else None, str(row['crypto'])
        )
        for _, row in df.iterrows()
    ]

    sql = """
        INSERT INTO CryptoPrice (date,hourx,crypto,[Open],[High],[Low],[Close],[Volume])
        SELECT ?,?,?,?,?,?,?,?
        WHERE NOT EXISTS (
            SELECT 1 FROM CryptoPrice WHERE date=? AND hourx=? AND crypto=?
        )
    """

    inserted_count = 0
    batch_size = 200 if len(rows) < 2000 else 500

    try:
        cursor.fast_executemany = True
        cursor.executemany(sql, rows)
        inserted_count = cursor.rowcount if cursor.rowcount != -1 else len(rows)
    except Exception as batch_err:
        logger.error(f"Batch insert failed for {crypto}: {batch_err}")
        logger.info("Fallback ke row-by-row insert...")
        for r in rows:
            try:
                cursor.execute(sql, r)
                inserted_count += cursor.rowcount
            except Exception as row_err:
                logger.error(f"Failed row insert for {crypto}: {row_err} | Data={r}")

    logger.info(f"{crypto}: {inserted_count} new rows inserted (batch={batch_size}).")
    return inserted_count


# --- Logging Ingestion ---
def log_ingestion(cursor, source, status, message="", rows_inserted=0, started_at=None, finished_at=None):
    now = datetime.datetime.utcnow()
    started_at = started_at or now
    finished_at = finished_at or now

    cursor.execute("""
        INSERT INTO IngestionLog (source, status, message, rows_inserted, started_at, finished_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, source, status, message, rows_inserted, started_at, finished_at)


# --- Logging Data Quality Issues ---
def log_data_quality_issue(cursor, source, issue_type, issue_detail):
    cursor.execute("""
        INSERT INTO DataQualityIssues (source, issue_type, issue_detail, detected_at)
        VALUES (?, ?, ?, GETDATE())
    """, source, issue_type, issue_detail)


# --- Insert News Data ---
def insert_news(df: pd.DataFrame):
    """Insert berita ke CryptoNews dengan dedup berdasarkan URL"""
    inserted = 0
    with sql_cursor() as cursor:
        for _, row in df.iterrows():
            coin = detect_coin(f"{row['title']} {row['content']}")
            published_at = pd.to_datetime(row["publishedAt"])
            news_date = published_at.date()

            cursor.execute("SELECT COUNT(*) FROM CryptoNews WHERE url = ?", row["url"])
            exists = cursor.fetchone()[0]

            if exists == 0:
                cursor.execute("""
                    INSERT INTO CryptoNews (coin, title, description, content, publishedAt, news_date, source, url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    coin, row["title"], row["description"], row["content"],
                    published_at, news_date, row["source"], row["url"]
                ))
                inserted += 1

    logger.info(f"Inserted {inserted} new news articles")
    return inserted
