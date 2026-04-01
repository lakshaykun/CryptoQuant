import requests
import zipfile
import io
import pandas as pd
from datetime import datetime, timedelta

BASE_URL = "https://data.binance.vision/data/spot/daily/klines"

COLUMNS = ['open_time',
 'open',
 'high',
 'low',
 'close',
 'volume',
 'close_time',
 'quote_volume',
 'trades',
 'taker_buy_base',
 'taker_buy_quote',
 'ignore'
 ]


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
    

def download_binance_klines(symbol, interval, start_date, end_date):
    all_data = []

    for single_date in daterange(start_date, end_date):
        date_str = single_date.strftime("%Y-%m-%d")
        url = f"{BASE_URL}/{symbol}/{interval}/{symbol}-{interval}-{date_str}.zip"

        print(f"Downloading: {url}")

        try:
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                print(f"❌ Missing: {date_str}")
                continue

            # Read zip in memory
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                file_name = z.namelist()[0]

                with z.open(file_name) as f:
                    df = pd.read_csv(f, header=None)

                    # Ensure correct shape
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

                    all_data.append(df)

        except Exception as e:
            print(f"⚠️ Error on {date_str}: {e}")

    # Combine all
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        return final_df
    else:
        return pd.DataFrame()


if __name__ == "__main__":
    symbol = "BTCUSDT"
    interval = "5m"

    start_date = datetime(2026, 3, 25)
    end_date = datetime(2026, 3, 31)

    df = download_binance_klines(symbol, interval, start_date, end_date)

    print(df.head())
    df.to_csv(f"{symbol}_{interval}_combined.csv", index=False)