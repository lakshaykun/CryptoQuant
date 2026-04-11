# pipelines/ingestion/streaming/source/binance_source.py

from pipelines.ingestion.streaming.sources.websocket_client import WebSocketClient

class BinanceSource:
    def __init__(self, logger, data_config: dict):
        # Load config and URI for Binance WebSocket
        self.config = data_config
        self.uri = self.config.get("binance_ws_uri")
        symbols = self.config.get("symbols", ["BTCUSDT", "ETHUSDT"])
        interval = self.config.get("interval", "1m")
        self.uri += f"?symbols={','.join(symbols).lower()}&interval={interval}"

        self.client = WebSocketClient(self.uri, logger)


    async def stream(self):
        async for data in self.client.connect():
            if data.get("type") != "heartbeat":
                yield data