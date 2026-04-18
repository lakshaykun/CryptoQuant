import logging
import time

from pipelines.ingestion.streaming.producers.kafka_producer import CryptoProducer
from pipelines.ingestion.streaming.sources.sentiment.news.source import fetch_news_events, news_interval_seconds

TOPIC = "btc_news"
logger = logging.getLogger(__name__)


def run_forever(interval_seconds: int | None = None):
    interval = interval_seconds or news_interval_seconds()
    producer = CryptoProducer()

    while True:
        events = fetch_news_events()
        for event in events:
            producer.send_message(TOPIC, event)

        logger.info("Published %d news events", len(events))
        time.sleep(interval)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run_forever()
