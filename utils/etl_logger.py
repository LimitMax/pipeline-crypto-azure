from utils.logger import logger

def log_summary(results, started_at, finished_at, entity="ETL"):
    """Cetak ringkasan hasil ETL (Crypto, News, dll)"""

    total_success = sum(1 for r in results if r and r["status"] == "SUCCESS")
    total_failed  = sum(1 for r in results if r and r["status"] == "FAILED")
    total_skipped = sum(1 for r in results if r and r["status"] == "SKIPPED")
    total_rows    = sum(r["rows"] for r in results if r)

    logger.info(f"\n📊 {entity} SUMMARY")
    logger.info("-" * 80)

    # Pilih header berdasarkan entity
    header = "Asset" if entity.lower() == "crypto" else "Source"
    logger.info(f"{header:<15} | {'Status':<8} | {'Rows':<6}")
    logger.info("-" * 80)

    for r in results:
        if not r: 
            continue
        icon = "✅" if r["status"] == "SUCCESS" else "⚠️" if r["status"] == "SKIPPED" else "❌"
        name_key = "symbol" if entity.lower() == "crypto" else "source"
        logger.info(f"{r[name_key]:<15} | {icon} {r['status']:<8} | {r['rows']:<6}")

    logger.info("-" * 80)
    logger.info(
        f"TOTAL | Success={total_success} | Failed={total_failed} | "
        f"Skipped={total_skipped} | Rows={total_rows}"
    )
    logger.info(f"⏱ Duration: {(finished_at - started_at).total_seconds():.2f}s")
    logger.info("=" * 80)
