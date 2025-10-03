import os, datetime, logging
import azure.functions as func
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed

# import utils
from utils.data_fetcher import fetch_data
from utils.blob_handler import connect_blob, save_raw_to_blob
from utils.db_handler import (
    connect_sql, insert_incremental, log_ingestion, log_data_quality_issue,
    get_last_success, update_last_success
)

from dotenv import load_dotenv
load_dotenv()

# symbols crypto
CRYPTOS = os.getenv("CRYPTO_SYMBOLS", "BTC-USD,ETH-USD").split(",")
INTERVAL = "1h"

logging.basicConfig(level=logging.INFO)

# helper logging
def log_header(title: str):
    logging.info("="*80)
    logging.info(title)
    logging.info("="*80)

def log_summary(results, started_at, finished_at):
    logging.info("📊 ETL SUMMARY")
    for r in results:
        logging.info(r)

# proses tiap simbol
def process_symbol(symbol, blob_client, wib_now):
    conn = connect_sql()
    cursor = conn.cursor()
    logging.info(f"🚀 Start {symbol}")

    try:
        last_ts = get_last_success(cursor, symbol)
        logging.info(f"📍 Last success = {last_ts}")

        if last_ts >= wib_now:
            logging.info(f"⏩ {symbol} skipped (up-to-date)")
            return {"symbol": symbol, "status": "SKIPPED", "rows": 0}

        df = fetch_data(symbol, last_ts, wib_now, INTERVAL)
        if df.empty:
            log_ingestion(cursor, symbol, "WARNING", "No data", 0, last_ts, wib_now)
            return {"symbol": symbol, "status": "SKIPPED", "rows": 0}

        save_raw_to_blob(blob_client, symbol, df)
        rows = insert_incremental(cursor, df, symbol)
        log_ingestion(cursor, symbol, "SUCCESS", "Ingest OK", rows, last_ts, wib_now)
        update_last_success(cursor, symbol, wib_now)

        return {"symbol": symbol, "status": "SUCCESS", "rows": rows}

    except Exception as e:
        logging.error(f"❌ Error process {symbol}: {e}", exc_info=True)
        return {"symbol": symbol, "status": "FAILED", "rows": 0}

    finally:
        cursor.close(); conn.close()

# MAIN function Azure
def main(timer: func.TimerRequest) -> None:
    try:
        started_at = datetime.datetime.utcnow()
        utc_now = datetime.datetime.now(datetime.timezone.utc).replace(minute=0, second=0, microsecond=0)
        wib_now = utc_now.astimezone(ZoneInfo("Asia/Jakarta")).replace(tzinfo=None)

        log_header(f"TimerCryptoIngest started at {wib_now}")

        blob_client = connect_blob()
        connect_sql().close()  # test koneksi

        results = []
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_to_symbol = {executor.submit(process_symbol, s, blob_client, wib_now): s for s in CRYPTOS}
            for future in as_completed(future_to_symbol):
                results.append(future.result())

        finished_at = datetime.datetime.utcnow()
        log_summary(results, started_at, finished_at)

    except Exception as fatal:
        logging.error(f"🔥 Fatal error in TimerCryptoIngest: {fatal}", exc_info=True)
