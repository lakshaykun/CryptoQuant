# pipelines/ingestion/streaming/source/binance_source.py

from ingestion.binance.ws_client import stream_binance

class BinanceSource:
    def __init__(self, logger, data_config: dict):
        self.config = data_config
        self.logger = logger
        self.symbols = self.config.get("symbols", ["BTCUSDT", "ETHUSDT"])
        self.interval = self.config.get("interval", "1m")
        self.uri = self.config.get("binance", {}).get("ws_url", "wss://stream.binance.com:9443/ws")

    async def stream(self):
        async for data in stream_binance(self.uri, self.symbols, self.interval, self.logger):
            self.logger.info(f"Received data: {data}")
            yield data