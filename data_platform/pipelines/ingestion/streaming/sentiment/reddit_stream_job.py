import time
import argparse

from pipelines.ingestion.streaming.producers.kafka_producer import CryptoProducer
from pipelines.ingestion.streaming.sources.sentiment.reddit.source import fetch_reddit_events

TOPIC = "btc_reddit"


def run_once() -> int:
    producer = CryptoProducer()
    events = fetch_reddit_events()
    for event in events:
        producer.send_message(TOPIC, event)
    producer.flush()
    producer.close()
    print(f"Published {len(events)} reddit events")
    return len(events)


def run_forever(interval_seconds: int = 60):
    while True:
        run_once()
        time.sleep(interval_seconds)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reddit sentiment Kafka producer")
    parser.add_argument("--once", action="store_true", help="Fetch and publish one cycle, then exit")
    args = parser.parse_args()

    if args.once:
        run_once()
    else:
        run_forever()
