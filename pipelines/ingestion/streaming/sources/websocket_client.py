# pipelines/ingestion/streaming/sources/websocket_client.py

import websockets
import asyncio
import json

class WebSocketClient:
    def __init__(self, uri, logger):
        self.uri = uri
        self.logger = logger

    async def connect(self):
        while True:
            try:
                async with websockets.connect(self.uri) as ws:
                    self.logger.info("Connected")

                    async for msg in ws:
                        yield json.loads(msg)

            except Exception as e:
                self.logger.error(f"WS error: {e}")
                await asyncio.sleep(5)