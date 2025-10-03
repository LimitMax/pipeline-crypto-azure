import datetime, requests, pandas as pd
import azure.functions as func

from utils import config
from utils.logger import logger
from utils.db_handler import insert_news, sql_cursor, log_ingestion
from utils.etl_logger import log_summary
from utils.retry import with_retry


def fetch_crypto_news(query="crypto OR bitcoin OR ethereum"):
    """Ambil berita crypto dari NewsAPI"""
    url = "https://newsapi.org/v2/everything"
    from_date = (datetime.datetime.utcnow() - datetime.timedelta(days=config.NEWSAPI_DAYS)).strftime("%Y-%m-%d")

    params = {
        "q": query,
        "from": from_date,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": config.NEWSAPI_PAGESIZE,
        "apiKey": config.NEWSAPI_KEY
    }

    def _call_api():
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        if "articles" not in data:
            raise ValueError(f"NewsAPI error: {data}")
        return data

    # Retry API call
    data = with_retry(_call_api, max_attempts=3)

    return pd.DataFrame([{
        "title": a["title"],
        "description": a.get("description"),
        "content": a.get("content"),
        "publishedAt": a["publishedAt"],
        "source": a["source"]["name"],
        "url": a["url"]
    } for a in data["articles"]])


def process_news(query="crypto OR bitcoin OR ethereum"):
    """ETL untuk berita dari NewsAPI"""
    start_time = datetime.datetime.utcnow()
    logger.info(f"📰 Start News Fetch: query='{query}'")

    try:
        df_news = fetch_crypto_news(query)
        if df_news.empty:
            logger.warning("⚠️ No news fetched")
            return {"source": "NewsAPI", "status": "FAILED", "rows": 0}

        rows = with_retry(lambda: insert_news(df_news))
        return {"source": "NewsAPI", "status": "SUCCESS", "rows": rows}

    except Exception as e:
        logger.error(f"❌ News fetch failed: {e}")
        return {"source": "NewsAPI", "status": "FAILED", "rows": 0}

    finally:
        logger.info("✅ End News Fetch\n")


def main(timer: func.TimerRequest) -> None:
    """Main Azure Function entrypoint"""
    started_at = datetime.datetime.utcnow()
    logger.info("🚀 TimerNewsIngest started")

    # Tes koneksi DB sebelum jalan
    try:
        with sql_cursor() as cur:
            cur.execute("SELECT 1")
    except Exception as e:
        logger.error(f"❌ SQL connection error: {e}")
        return

    # Jalankan ETL News
    results = [process_news()]

    finished_at = datetime.datetime.utcnow()
    log_summary(results, started_at, finished_at, entity="News")

    # Log global ke IngestionLog
    try:
        with sql_cursor() as cur:
            r = results[0]
            log_ingestion(
                cur, "NEWSAPI", r["status"],
                f"Rows={r['rows']}", r["rows"],
                started_at, finished_at
            )
        logger.info("📝 Global summary written to IngestionLog")
    except Exception as e:
        logger.error(f"⚠️ Failed to log global summary: {e}")
