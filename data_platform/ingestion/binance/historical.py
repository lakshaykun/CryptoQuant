# data_platform/ingestion/binance/historical.py

import requests
from datetime import datetime, timezone
from .parser import parse_historical_kline

BASE_URL = "https://api.binance.com/api/v3/klines"

def iter_today_klines(symbol: str, interval: str):
    """
    Yields parsed Binance kline dictionaries for the current day up to now.
    """
    now = datetime.now(timezone.utc)
    start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)

    start_ts = int(start.timestamp() * 1000)
    end_ts = int(now.timestamp() * 1000)

    while start_ts < end_ts:
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "startTime": start_ts,
            "limit": 1000,
        }

        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if not data:
            break

        for row in data:
            yield parse_historical_kline(row, symbol)

        # Move forward using the close_time of the last candle
        start_ts = data[-1][6] + 1


def fetch_today_klines(symbol: str, interval: str):
    """
    Returns a list of all parsed kline dictionaries for the current day.
    """
    return list(iter_today_klines(symbol, interval))
