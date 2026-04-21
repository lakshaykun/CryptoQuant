import logging
import time
import argparse

from pipelines.ingestion.streaming.producers.kafka_producer import CryptoProducer
from pipelines.ingestion.streaming.sources.sentiment.news.source import fetch_news_events, news_interval_seconds

TOPIC = "btc_news"
logger = logging.getLogger(__name__)


def run_once() -> int:
    producer = CryptoProducer()
    events = fetch_news_events()
    for event in events:
        producer.send_message(TOPIC, event)

    producer.flush()
    producer.close()
    logger.info("Published %d news events", len(events))
    return len(events)


def run_forever(interval_seconds: int | None = None):
    interval = interval_seconds if interval_seconds is not None else 60

    while True:
        run_once()
        time.sleep(interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="News sentiment Kafka producer")
    parser.add_argument("--once", action="store_true", help="Fetch and publish one cycle, then exit")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    if args.once:
        run_once()
    else:
        run_forever()
