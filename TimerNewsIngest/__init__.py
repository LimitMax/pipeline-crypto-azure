import os, datetime, logging
import azure.functions as func
import requests, pandas as pd
from utils.db_handler import connect_sql, insert_news
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("NEWSAPI_KEY")
logging.basicConfig(level=logging.INFO)

def log_header(title: str):
    logging.info("="*80)
    logging.info(title)
    logging.info("="*80)

def fetch_crypto_news(query="crypto OR bitcoin", from_days=1, page_size=10):
    url = "https://newsapi.org/v2/everything"
    from_date = (datetime.datetime.utcnow() - datetime.timedelta(days=from_days)).strftime("%Y-%m-%d")

    params = {"q": query, "from": from_date, "language": "en", "pageSize": page_size, "apiKey": API_KEY}
    r = requests.get(url, params=params)
    data = r.json()
    if r.status_code != 200 or "articles" not in data:
        raise Exception(f"NewsAPI error: {data}")

    return pd.DataFrame([{
        "title": a["title"],
        "description": a.get("description"),
        "content": a.get("content"),
        "publishedAt": a["publishedAt"],
        "source": a["source"]["name"],
        "url": a["url"]
    } for a in data["articles"]])

def process_news():
    try:
        df_news = fetch_crypto_news(from_days=1, page_size=5)
        if df_news.empty:
            return {"source": "NewsAPI", "status": "SKIPPED", "rows": 0}
        rows = insert_news(df_news)
        return {"source": "NewsAPI", "status": "SUCCESS", "rows": rows}
    except Exception as e:
        logging.error(f"❌ Error fetch news: {e}", exc_info=True)
        return {"source": "NewsAPI", "status": "FAILED", "rows": 0}

def main(timer: func.TimerRequest) -> None:
    try:
        started_at = datetime.datetime.utcnow()
        log_header("TimerNewsIngest started")

        connect_sql().close()  # test koneksi
        result = process_news()
        logging.info(f"📊 Result: {result}")

    except Exception as fatal:
        logging.error(f"🔥 Fatal error in TimerNewsIngest: {fatal}", exc_info=True)
