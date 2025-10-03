import time, datetime
import yfinance as yf
import pandas as pd
from utils import config
from utils.logger import logger


def fetch_data(symbol, start, end, interval=None, adjusted=False, retries=3, tz="UTC"):
    """Fetch OHLCV data dari yfinance untuk 1 symbol"""

    interval = interval or config.INTERVAL

    if start >= end:
        logger.warning(f"Invalid range for {symbol}: start {start} >= end {end}")
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
            logger.error(f"Error fetching {symbol} (attempt {attempt+1}): {e}")
        attempt += 1
        time.sleep(2 * attempt)  # exponential backoff

    if df.empty:
        logger.warning(f"No data returned for {symbol} {start} - {end} after {retries} retries")
        return pd.DataFrame()

    # Normalize columns
    df.reset_index(inplace=True)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if c[0] else c[1] for c in df.columns]

    ts_col = "Datetime" if "Datetime" in df.columns else "Date"

    # Apply timezone if needed
    if tz and ts_col in df.columns:
        try:
            df[ts_col] = df[ts_col].dt.tz_localize("UTC").dt.tz_convert(tz).dt.tz_localize(None)
        except Exception:
            # kalau sudah ada timezone, langsung convert
            df[ts_col] = df[ts_col].dt.tz_convert(tz).dt.tz_localize(None)

    df['date'] = df[ts_col].dt.date
    df['hourx'] = df[ts_col].dt.hour
    df['crypto'] = symbol   # konsisten dengan DB schema

    curated = df[['date','hourx','crypto','Open','High','Low','Close','Volume']]
    logger.info(f"{symbol}: fetched {len(curated)} rows ({start} → {end}) in {elapsed}s (interval={interval}, tz={tz})")

    return curated
