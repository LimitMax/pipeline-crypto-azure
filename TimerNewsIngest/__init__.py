import os, datetime, logging
import azure.functions as func
import requests, pandas as pd
from dotenv import load_dotenv
from utils.db_handler import connect_sql, insert_news

load_dotenv()
API_KEY = os.getenv("NEWSAPI_KEY")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# --- Helper logging ---
def log_header(title: str):
    logging.info("="*80)
    logging.info(f"📰 {title}")
    logging.info("="*80)

def log_summary(results, started_at, finished_at):
    total_success = sum(1 for r in results if r and r["status"] == "SUCCESS")
    total_failed  = sum(1 for r in results if r and r["status"] == "FAILED")
    total_rows    = sum(r["rows"] for r in results if r)

    logging.info("\n📊 ETL SUMMARY")
    logging.info("-"*80)
    logging.info(f"{'Source':<15} | {'Status':<8} | {'Rows':<6}")
    logging.info("-"*80)
    for r in results:
        if not r: continue
        status_icon = "✅" if r["status"]=="SUCCESS" else "❌"
        logging.info(f"{r['source']:<15} | {status_icon} {r['status']:<8} | {r['rows']:<6}")
    logging.info("-"*80)
    logging.info(f"TOTAL       | Success={total_success} | Failed={total_failed} | Rows={total_rows}")
    logging.info(f"⏱ Duration: {(finished_at - started_at).total_seconds():.2f}s")
    logging.info("="*80)

# --- Fetch news from NewsAPI ---
def fetch_crypto_news(query="crypto OR bitcoin OR ethereum", from_days=1, page_size=100):
    url = "https://newsapi.org/v2/everything"
    from_date = (datetime.datetime.utcnow() - datetime.timedelta(days=from_days)).strftime("%Y-%m-%d")

    params = {
        "q": query,
        "from": from_date,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": page_size,
        "apiKey": API_KEY
    }

    r = requests.get(url, params=params)
    data = r.json()
    if r.status_code != 200 or "articles" not in data:
        raise Exception(f"Error fetch news: {data}")

    return pd.DataFrame([{
        "title": a["title"],
        "description": a.get("description"),
        "content": a.get("content"),
        "publishedAt": a["publishedAt"],
        "source": a["source"]["name"],
        "url": a["url"]
    } for a in data["articles"]])

# --- ETL per news fetch job ---
def process_news(query="crypto OR bitcoin OR ethereum"):
    start_time = datetime.datetime.utcnow()
    logging.info(f"\n{'-'*20} 📰 START NEWS FETCH {'-'*20}")

    try:
        df_news = fetch_crypto_news(query=query, from_days=3, page_size=30)
        if df_news.empty:
            logging.warning("⚠️ No news fetched")
            return {"source": "NewsAPI", "status": "FAILED", "rows": 0}

        rows = insert_news(df_news)
        return {"source": "NewsAPI", "status": "SUCCESS", "rows": rows}

    except Exception as e:
        logging.error(f"❌ News fetch failed: {e}")
        return {"source": "NewsAPI", "status": "FAILED", "rows": 0}

    finally:
        logging.info(f"{'-'*20} 📰 END NEWS FETCH {'-'*20}\n")

# --- Main ETL function ---
def main(timer: func.TimerRequest) -> None:
    started_at = datetime.datetime.utcnow()
    log_header("TimerNewsIngest started")

    try:
        connect_sql().close()  # test connection
    except Exception as e:
        logging.error(f"❌ Connection error: {e}")
        return

    results = [process_news()]

    finished_at = datetime.datetime.utcnow()
    log_summary(results, started_at, finished_at)
