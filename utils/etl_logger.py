import logging

def log_etl_summary(assets: list):
    logging.info("📊 ETL SUMMARY")
    logging.info("-" * 80)
    logging.info(f"{'Asset':<12} | {'Status':<10} | {'Rows':<6}")
    logging.info("-" * 80)
    for asset in assets:
        logging.info(f"{asset['asset']:<12} | {asset['status']:<10} | {asset['rows']:<6}")
    logging.info("-" * 80)
