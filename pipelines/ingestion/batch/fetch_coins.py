# pipelines/ingestion/batch/fetch_coins.py

import requests
import zipfile
import io
import pandas as pd
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from configs.data import INTERVAL, START_DATE

BASE_URL = "https://data.binance.vision/data/spot/daily/klines"

COLUMNS = [
    'open_time', 'open', 'high', 'low', 'close', 'volume',
    'close_time', 'quote_volume', 'trades',
    'taker_buy_base', 'taker_buy_quote', 'ignore'
]

MAX_WORKERS = 10  # Tune this (8–20 is usually safe)


# -----------------------------
# Utils
# -----------------------------
def daterange(start_date, end_date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)

def convert_timestamp(series):
    max_val = series.max()

    if max_val > 1e15:
        return pd.to_datetime(series, unit="us", errors="coerce")
    elif max_val > 1e12:
        return pd.to_datetime(series, unit="ms", errors="coerce")
    else:
        return pd.to_datetime(series, unit="s", errors="coerce")

# -----------------------------
# Core worker (ONE TASK)
# -----------------------------
def fetch_single_day(symbol, interval, single_date, logger):
    date_str = single_date.strftime("%Y-%m-%d")
    url = f"{BASE_URL}/{symbol}/{interval}/{symbol}-{interval}-{date_str}.zip"

    try:
        response = requests.get(url, timeout=10)

        if response.status_code != 200:
            logger.warning(f"❌ Missing: {symbol} {date_str}")
            return None

        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            file_name = z.namelist()[0]

            with z.open(file_name) as f:
                df = pd.read_csv(f, header=None)

                df = df.iloc[:, :12]
                df.columns = COLUMNS

                # Safe timestamp handling
                df["open_time"] = pd.to_numeric(df["open_time"], errors="coerce")
                df["close_time"] = pd.to_numeric(df["close_time"], errors="coerce")

                df = df.dropna(subset=["open_time", "close_time"])

                df["open_time"] = convert_timestamp(df["open_time"])
                df["close_time"] = convert_timestamp(df["close_time"])

                df = df.dropna(subset=["open_time", "close_time"])

                df["symbol"] = symbol

                logger.info(f"✅ Done: {symbol} {date_str}")
                return df

    except Exception as e:
        logger.error(f"⚠️ Error: {symbol} {date_str} → {e}")
        return None


# -----------------------------
# Parallel downloader
# -----------------------------
def fetch_coins_data(symbols, interval, start_date, end_date, logger):
    tasks = []
    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        # Submit all jobs
        for symbol in symbols:
            for single_date in daterange(start_date, end_date):
                tasks.append(
                    executor.submit(fetch_single_day, symbol, interval, single_date, logger)
                )

        # Collect results
        for future in as_completed(tasks):
            result = future.result()
            if result is not None:
                results.append(result)

    if results:
        return pd.concat(results, ignore_index=True)
    else:
        return pd.DataFrame()
