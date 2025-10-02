import yfinance as yf
import pandas as pd
import logging, time
import datetime

def fetch_data(symbol, start, end, interval="1h", adjusted=False, retries=3):
    if start >= end:
        logging.warning(f"⚠️ Invalid range for {symbol}: start {start} >= end {end}")
        return pd.DataFrame()

    attempt, df = 0, pd.DataFrame()
    while attempt < retries:
        try:
            t0 = time.time()
            df = yf.download(
                symbol, start=start, end=end,
                interval=interval, auto_adjust=adjusted,
                progress=False, threads=True
            )
            elapsed = round(time.time() - t0, 2)
            if not df.empty:
                break
        except Exception as e:
            logging.error(f"❌ Error fetching {symbol} (attempt {attempt+1}): {e}")
        attempt += 1
        time.sleep(2 * attempt)

    if df.empty:
        logging.warning(f"⚠️ No data returned for {symbol} {start} - {end} after {retries} retries")
        return pd.DataFrame()

    # Normalize columns
    df.reset_index(inplace=True)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if c[0] else c[1] for c in df.columns]

    ts_col = "Datetime" if "Datetime" in df.columns else "Date"
    df['date'] = df[ts_col].dt.date
    df['hourx'] = df[ts_col].dt.hour
    df['crypto'] = symbol   # ✅ konsisten dengan DB

    curated = df[['date','hourx','crypto','Open','High','Low','Close','Volume']]
    logging.info(f"✅ {symbol}: fetched {len(curated)} rows ({start} → {end}) in {elapsed}s")
    return curated
