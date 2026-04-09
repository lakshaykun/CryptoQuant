# render_api/app/historical.py

import requests
from datetime import datetime, timezone

BASE_URL = "https://api.binance.com/api/v3/klines"

COLUMNS = [
    'open_time', 'open', 'high', 'low', 'close', 'volume',
    'close_time', 'quote_volume', 'trades',
    'taker_buy_base', 'taker_buy_quote', 'ignore'
]


def fetch_today_klines(symbol: str, interval: str):
    now = datetime.now(timezone.utc)
    start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)

    start_ts = int(start.timestamp() * 1000)
    end_ts = int(now.timestamp() * 1000)

    all_data = []

    while start_ts < end_ts:
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "startTime": start_ts,
            "limit": 1000
        }

        response = requests.get(BASE_URL, params=params, timeout=10)
        data = response.json()

        if not data:
            break

        all_data.extend(data)

        # move forward
        start_ts = data[-1][6] + 1

    # Convert to dict format (same as WS)
    result = []
    for row in all_data:
        result.append({
            "symbol": symbol.upper(),
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
            "ignore": 0
        })

    return result