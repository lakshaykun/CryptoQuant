import asyncio

from pipelines.ingestion.streaming.producers.kafka_producer import CryptoProducer
from pipelines.ingestion.streaming.sources.binance_source import BinanceSource
from pipelines.transformers.raw.market import RawMarketTransformer
from utils.config_loader import load_config
from utils.logger import get_logger

logger = get_logger("stream_job")


async def run():
    data_config = load_config("configs/data.yaml")
    source = BinanceSource(logger, data_config)
    producer = CryptoProducer()
    transformer = RawMarketTransformer()

    async for raw in source.stream():
        data = transformer.transform(raw)
        producer.send_price(data)


if __name__ == "__main__":
    asyncio.run(run())
