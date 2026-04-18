import time

from pipelines.ingestion.streaming.producers.kafka_producer import CryptoProducer
from pipelines.ingestion.streaming.sources.sentiment.reddit.source import fetch_reddit_events

TOPIC = "btc_reddit"


def run_forever(interval_seconds: int = 300):
    producer = CryptoProducer()
    while True:
        events = fetch_reddit_events()
        for event in events:
            producer.send_message(TOPIC, event)
        print(f"Published {len(events)} reddit events")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    run_forever()
