import os, datetime, logging
import azure.functions as func
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.data_fetcher import fetch_data
from utils.blob_handler import connect_blob, save_raw_to_blob
from utils.db_handler import (
    connect_sql, insert_incremental, log_ingestion, log_data_quality_issue,
    get_last_success, update_last_success
)
from dotenv import load_dotenv

load_dotenv()
CRYPTOS = os.getenv("CRYPTO_SYMBOLS", "BTC-USD,ETH-USD,SOL-USD,XRP-USD,DOGE-USD").split(",")
INTERVAL = "1h"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# --- Helper logging ---
def log_header(title: str):
    logging.info("="*80)
    logging.info(f"🚀 {title}")
    logging.info("="*80)

def log_summary(results, started_at, finished_at):
    total_success = sum(1 for r in results if r and r["status"] == "SUCCESS")
    total_failed  = sum(1 for r in results if r and r["status"] == "FAILED")
    total_skipped = sum(1 for r in results if r and r["status"] == "SKIPPED")
    total_rows    = sum(r["rows"] for r in results if r)

    logging.info("\n📊 ETL SUMMARY")
    logging.info("-"*80)
    logging.info(f"{'Asset':<10} | {'Status':<8} | {'Rows':<6}")
    logging.info("-"*80)
    for r in results:
        if not r: continue
        status_icon = "✅" if r["status"]=="SUCCESS" else "⚠️" if r["status"]=="SKIPPED" else "❌"
        logging.info(f"{r['symbol']:<10} | {status_icon} {r['status']:<8} | {r['rows']:<6}")
    logging.info("-"*80)
    logging.info(f"TOTAL       | Success={total_success} | Failed={total_failed} | Skipped={total_skipped} | Rows={total_rows}")
    logging.info(f"⏱ Duration: {(finished_at - started_at).total_seconds():.2f}s")
    logging.info("="*80)


# --- ETL per symbol ---
def process_symbol(symbol, blob_client, wib_now):
    start_time = datetime.datetime.utcnow()
    conn = connect_sql()
    cursor = conn.cursor()
    logging.info(f"\n{'-'*20} 🪙 START {symbol} {'-'*20}")

    try:
        # ✅ Ambil watermark dari IngestionMetadata (fallback ke CryptoPrice)
        last_ts = get_last_success(cursor, symbol)
        logging.info(f"📍 Last success = {last_ts}")

        if last_ts >= wib_now:
            log_ingestion(cursor, symbol, "SKIPPED", "Data up-to-date", 0, start_time, datetime.datetime.utcnow())
            logging.info(f"⏩ {symbol} skipped (up to date)")
            return {"symbol": symbol, "status": "SKIPPED", "rows": 0}

        # Backfill per 6 jam
        BATCH_HOURS, batch_start, total_rows = 6, last_ts + datetime.timedelta(hours=1), 0
        while batch_start <= wib_now:
            batch_end = min(batch_start + datetime.timedelta(hours=BATCH_HOURS-1), wib_now)
            logging.info(f"🔄 Backfilling {symbol} {batch_start} → {batch_end}")
            df = fetch_data(symbol, batch_start, batch_end, INTERVAL)

            if df.empty:
                log_ingestion(cursor, symbol, "WARNING", f"No data {batch_start}-{batch_end}", 0, batch_start, batch_end)
            else:
                if (df['Close'] <= 0).any():
                    log_data_quality_issue(cursor, symbol, "InvalidValue", "Close price <= 0 detected")

                save_raw_to_blob(blob_client, symbol, df, "incremental")
                rows = insert_incremental(cursor, df, symbol)
                total_rows += rows

                log_ingestion(cursor, symbol, "SUCCESS", f"Ingest OK {batch_start}-{batch_end}", rows, batch_start, batch_end)
                update_last_success(cursor, symbol, batch_end)  # ✅ update watermark

            batch_start = batch_end + datetime.timedelta(hours=1)

        return {"symbol": symbol, "status": "SUCCESS", "rows": total_rows}

    except Exception as e:
        logging.error(f"❌ {symbol} failed: {e}")
        try:
            err_conn = connect_sql()
            err_cur = err_conn.cursor()
            log_ingestion(err_cur, symbol, "FAILED", str(e), 0, start_time, datetime.datetime.utcnow())
            err_cur.close(); err_conn.close()
        except Exception as log_err:
            logging.error(f"⚠️ Failed to log ingestion error: {log_err}")
        return {"symbol": symbol, "status": "FAILED", "rows": 0}

    finally:
        cursor.close(); conn.close()
        logging.info(f"⏱ Finished {symbol} in {(datetime.datetime.utcnow()-start_time).total_seconds():.2f}s")
        logging.info(f"{'-'*20} 🪙 END {symbol} {'-'*20}\n")


# --- Main ETL function ---
def main(timer: func.TimerRequest) -> None:
    started_at = datetime.datetime.utcnow()
    utc_now = datetime.datetime.now(datetime.timezone.utc).replace(minute=0, second=0, microsecond=0)
    wib_now = utc_now.astimezone(ZoneInfo("Asia/Jakarta")).replace(tzinfo=None)

    log_header(f"TimerCryptoIngest started at {wib_now.strftime('%Y-%m-%d %H:%M:%S')} WIB")
    logging.info(f"Target symbols: {CRYPTOS} | Interval: {INTERVAL}")

    try:
        blob_client = connect_blob()
        connect_sql().close()
    except Exception as e:
        logging.error(f"❌ Connection error: {e}")
        return

    max_workers = 2 if len(CRYPTOS) <= 5 else (3 if len(CRYPTOS) <= 10 else 4)
    logging.info(f"⚡ Using max_workers={max_workers} for {len(CRYPTOS)} symbols")

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_symbol = {executor.submit(process_symbol, symbol, blob_client, wib_now): symbol for symbol in CRYPTOS}
        for future in as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            try:
                results.append(future.result())
            except Exception as e:
                logging.error(f"❌ {symbol} failed unexpectedly: {e}")
                results.append({"symbol": symbol, "status": "FAILED", "rows": 0})

    finished_at = datetime.datetime.utcnow()
    log_summary(results, started_at, finished_at)

    # ✅ Global summary ke IngestionLog
    try:
        conn = connect_sql()
        cur = conn.cursor()

        total_success = sum(1 for r in results if r and r["status"] == "SUCCESS")
        total_failed  = sum(1 for r in results if r and r["status"] == "FAILED")
        total_skipped = sum(1 for r in results if r and r["status"] == "SKIPPED")
        total_rows    = sum(r["rows"] for r in results if r)

        message = f"Success={total_success} | Failed={total_failed} | Skipped={total_skipped} | Rows={total_rows}"
        log_ingestion(cur, "GLOBAL", "SUMMARY", message, total_rows, started_at, finished_at)
        conn.commit()
        cur.close(); conn.close()
        logging.info("📝 Global summary written to IngestionLog")
    except Exception as e:
        logging.error(f"⚠️ Failed to log global summary: {e}")

if __name__ == "__main__":
    main(None)
