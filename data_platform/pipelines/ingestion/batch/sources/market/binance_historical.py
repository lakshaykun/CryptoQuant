# pipelines/ingestion/batch/sources/market/binance_historical.py

import requests
import zipfile
import io
import pandas as pd
from pipelines.storage.local.csv import (
    ensure_dir,
    build_filename,
    build_path,
    file_exists,
    save_csv
)

BASE_URL = "https://data.binance.vision/data/spot/daily/klines"

COLUMNS = [
    'open_time', 'open', 'high', 'low', 'close', 'volume',
    'close_time', 'quote_volume', 'trades',
    'taker_buy_base', 'taker_buy_quote', 'ignore'
]


def fetch_single_day(symbol, interval, single_date, base_path, logger):
    date_str = single_date.strftime("%Y-%m-%d")

    filename = build_filename(symbol, interval, date_str)
    file_path = build_path(base_path, filename)

    ensure_dir(base_path)

    if file_exists(file_path):
        logger.info(f"⏭️ Skipped: {filename}")
        return

    url = f"{BASE_URL}/{symbol}/{interval}/{symbol}-{interval}-{date_str}.zip"

    try:
        response = requests.get(url, timeout=10)

        if response.status_code != 200:
            logger.warning(f"❌ Missing: {filename}")
            return

        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            with z.open(z.namelist()[0]) as f:
                df = pd.read_csv(f, header=None)

                df = df.iloc[:, :12]
                df.columns = COLUMNS
                df["symbol"] = symbol

                save_csv(df, file_path)

        logger.info(f"💾 Saved: {filename}")

    except Exception as e:
        logger.error(f"⚠️ Error: {filename} → {e}")
