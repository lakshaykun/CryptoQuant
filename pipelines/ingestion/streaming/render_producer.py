# pipelines/ingestion/streaming/render_producer.py

import asyncio
import websockets

async def main():
    uri = "wss://cryptoquantproducer.onrender.com/ws/binance?symbols=btcusdt,ethusdt&interval=1m"

    async with websockets.connect(uri) as ws:
        while True:
            print(await ws.recv())

asyncio.run(main())