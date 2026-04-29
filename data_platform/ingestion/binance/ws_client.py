# data_platform/ingestion/binance/ws_client.py

import json
import asyncio
import websockets
from .parser import parse_ws_kline

RECONNECT_DELAY = 2

def build_stream_url(base_url, symbols, interval):
    """
    Builds the multi-stream URL for Binance WebSocket.
    Note: For multiple streams we use the /stream endpoint.
    If the base_url is wss://stream.binance.com:9443/ws, we convert it to /stream.
    """
    stream_type = f"kline_{interval}"
    streams = "/".join([f"{s.lower()}@{stream_type}" for s in symbols])
    
    # ensure we use /stream endpoint for multiple streams
    if base_url.endswith("/ws"):
        base_url = base_url[:-3] + "/stream"
        
    return f"{base_url}?streams={streams}"

async def stream_binance(base_url: str, symbols: list, interval: str, logger):
    """
    Main streaming loop with:
    - reconnect logic
    - parsing
    - filtering (only yields closed candles)
    """
    url = build_stream_url(base_url, symbols, interval)

    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5
            ) as ws:
                logger.info(f"[Binance WS] Connected → {url}")

                while True:
                    message = await ws.recv()
                    raw = json.loads(message)

                    parsed = parse_ws_kline(raw)

                    if parsed is None:
                        continue

                    if not parsed["is_closed"]:
                        continue
                    
                    parsed.pop("is_closed")

                    yield parsed

        except Exception as e:
            logger.warning(f"[Binance WS] Reconnecting due to: {e}")
            await asyncio.sleep(RECONNECT_DELAY)
