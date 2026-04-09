# render_api/app/binance_ws.py

import json
import asyncio
import websockets
from .config import BASE_BINANCE_WS, RECONNECT_DELAY


def build_stream_url(symbols, stream_type):
    streams = "/".join([f"{s.lower()}@{stream_type}" for s in symbols])
    return f"{BASE_BINANCE_WS}?streams={streams}"


def parse_kline(data):
    """
    Extract kline data into your required schema
    """
    if "data" not in data:
        return None

    d = data["data"]

    # Only process kline events
    if d.get("e") != "kline":
        return None

    k = d["k"]

    return {
        "symbol": d.get("s"),
        "open_time": k["t"],
        "open": float(k["o"]),
        "high": float(k["h"]),
        "low": float(k["l"]),
        "close": float(k["c"]),
        "volume": float(k["v"]),
        "close_time": k["T"],
        "quote_volume": float(k["q"]),
        "trades": int(k["n"]),
        "taker_buy_base": float(k["V"]),
        "taker_buy_quote": float(k["Q"]),
        "is_closed": k["x"],  # useful for filtering final candles
        "ignore": 0
    }


async def stream_binance(symbols, stream_type, send_callback):
    """
    Main streaming loop with:
    - reconnect logic
    - parsing
    - filtering
    """

    url = build_stream_url(symbols, stream_type)

    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5
            ) as ws:

                print(f"[Binance] Connected → {url}")

                while True:
                    message = await ws.recv()
                    raw = json.loads(message)

                    parsed = parse_kline(raw)

                    # Skip irrelevant messages
                    if parsed is None:
                        continue

                    # only send closed candles
                    if not parsed["is_closed"]:
                        continue
                    
                    parsed.pop("is_closed")

                    await send_callback(parsed)

        except Exception as e:
            print(f"[Binance] Reconnecting due to: {e}")
            await asyncio.sleep(RECONNECT_DELAY)