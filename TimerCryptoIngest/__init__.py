import datetime
import azure.functions as func
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils import config
from utils.logger import logger
from utils.data_fetcher import fetch_data
from utils.blob_handler import connect_blob, save_raw_to_blob
from utils.db_handler import (
    sql_cursor, insert_incremental, log_ingestion,
    log_data_quality_issue, get_last_success, update_last_success
)
from utils.etl_logger import log_summary
from utils.retry import with_retry


def process_symbol(symbol, blob_client, wib_now):
    """ETL untuk satu symbol crypto"""
    start_time = datetime.datetime.utcnow()
    logger.info(f"🪙 Start ETL {symbol}")

    with sql_cursor() as cursor:
        try:
            # Ambil watermark terakhir
            last_ts = get_last_success(cursor, symbol)
            logger.info(f"📍 {symbol} Last success = {last_ts}")

            if last_ts >= wib_now:
                log_ingestion(cursor, symbol, "SKIPPED", "Data up-to-date", 0, start_time, datetime.datetime.utcnow())
                logger.info(f"⏩ {symbol} skipped (already up-to-date)")
                return {"symbol": symbol, "status": "SKIPPED", "rows": 0}

            # Backfill per batch
            batch_start = last_ts + datetime.timedelta(hours=1)
            total_rows = 0

            while batch_start <= wib_now:
                batch_end = min(batch_start + datetime.timedelta(hours=config.CRYPTO_BATCH_HOURS-1), wib_now)
                logger.info(f"🔄 Fetching {symbol} {batch_start} → {batch_end}")

                df = fetch_data(symbol, batch_start, batch_end, config.INTERVAL)

                if df.empty:
                    log_ingestion(cursor, symbol, "WARNING", f"No data {batch_start}-{batch_end}", 0, batch_start, batch_end)
                else:
                    # Data Quality check
                    if (df['Close'] <= 0).any():
                        log_data_quality_issue(cursor, symbol, "InvalidValue", "Close price <= 0 detected")

                    # Save ke blob (retry 3x)
                    with_retry(lambda: save_raw_to_blob(blob_client, symbol, df, "incremental"))

                    # Insert ke DB
                    rows = with_retry(lambda: insert_incremental(cursor, df, symbol))
                    total_rows += rows

                    # Log ingestion
                    log_ingestion(cursor, symbol, "SUCCESS", f"Ingest OK {batch_start}-{batch_end}", rows, batch_start, batch_end)
                    update_last_success(cursor, symbol, batch_end)

                batch_start = batch_end + datetime.timedelta(hours=1)

            return {"symbol": symbol, "status": "SUCCESS", "rows": total_rows}

        except Exception as e:
            log_ingestion(cursor, symbol, "FAILED", str(e), 0, start_time, datetime.datetime.utcnow())
            logger.error(f"❌ {symbol} failed: {e}")
            return {"symbol": symbol, "status": "FAILED", "rows": 0}

        finally:
            logger.info(f"⏱ Finished {symbol} in {(datetime.datetime.utcnow()-start_time).total_seconds():.2f}s")


def main(timer: func.TimerRequest) -> None:
    """Main Azure Function entrypoint"""
    started_at = datetime.datetime.utcnow()
    utc_now = datetime.datetime.now(datetime.timezone.utc).replace(minute=0, second=0, microsecond=0)
    wib_now = utc_now.astimezone(ZoneInfo("Asia/Jakarta")).replace(tzinfo=None)

    logger.info(f"🚀 TimerCryptoIngest started at {wib_now} WIB")
    logger.info(f"Target symbols: {config.CRYPTO_SYMBOLS} | Interval: {config.INTERVAL}")

    # Tes koneksi blob
    try:
        blob_client = connect_blob()
    except Exception as e:
        logger.error(f"❌ Blob connection error: {e}")
        return

    results = []
    max_workers = min(len(config.CRYPTO_SYMBOLS), 4)
    logger.info(f"⚡ Using max_workers={max_workers} for {len(config.CRYPTO_SYMBOLS)} symbols")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_symbol = {executor.submit(process_symbol, s, blob_client, wib_now): s for s in config.CRYPTO_SYMBOLS}
        for future in as_completed(future_to_symbol):
            results.append(future.result())

    finished_at = datetime.datetime.utcnow()
    log_summary(results, started_at, finished_at, entity="Crypto")
