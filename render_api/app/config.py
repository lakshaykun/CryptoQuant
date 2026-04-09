# render_api/app/config.py

import os

BASE_BINANCE_WS = "wss://stream.binance.com:9443/stream"

DEFAULT_SYMBOLS = os.getenv("SYMBOLS", "btcusdt,ethusdt").split(",")
DEFAULT_STREAM = os.getenv("STREAM_TYPE", "trade")

RECONNECT_DELAY = 2