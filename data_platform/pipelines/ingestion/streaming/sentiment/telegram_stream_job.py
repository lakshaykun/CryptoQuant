import argparse
import time

from pipelines.ingestion.streaming.producers.kafka_producer import CryptoProducer
from pipelines.ingestion.streaming.sources.sentiment.telegram.source import (
    fetch_telegram_events,
    telegram_interval_seconds,
)

TOPIC = "btc_telegram"


def run_once() -> int:
    producer = CryptoProducer()
    events = fetch_telegram_events()
    for event in events:
        producer.send_message(TOPIC, event)

    producer.flush()
    producer.close()
    print(f"Published {len(events)} telegram events")
    return len(events)


def run_forever(interval_seconds: int | None = None):
    interval = interval_seconds if interval_seconds is not None else telegram_interval_seconds()
    while True:
        run_once()
        time.sleep(interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Telegram sentiment Kafka producer")
    parser.add_argument("--once", action="store_true", help="Fetch and publish one cycle, then exit")
    args = parser.parse_args()

    if args.once:
        run_once()
    else:
        run_forever()