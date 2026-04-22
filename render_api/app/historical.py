# render_api/app/historical.py

import requests
from datetime import datetime, timezone

BASE_URL = "https://api.binance.com/api/v3/klines"

COLUMNS = [
    'open_time', 'open', 'high', 'low', 'close', 'volume',
    'close_time', 'quote_volume', 'trades',
    'taker_buy_base', 'taker_buy_quote', 'ignore'
]


def _row_to_dict(row):
    return {
        "open_time": row[0],
        "open": float(row[1]),
        "high": float(row[2]),
        "low": float(row[3]),
        "close": float(row[4]),
        "volume": float(row[5]),
        "close_time": row[6],
        "quote_volume": float(row[7]),
        "trades": int(row[8]),
        "taker_buy_base": float(row[9]),
        "taker_buy_quote": float(row[10]),
        "ignore": 0,
    }


def iter_today_klines(symbol: str, interval: str):
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
        data = response.json()

        if not data:
            break

        for row in data:
            yield _row_to_dict(row)

        # move forward
        start_ts = data[-1][6] + 1


def fetch_today_klines(symbol: str, interval: str):
    return list(iter_today_klines(symbol, interval))