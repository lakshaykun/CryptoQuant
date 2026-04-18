import time

from pipelines.ingestion.streaming.producers.kafka_producer import CryptoProducer
from pipelines.ingestion.streaming.sources.sentiment.youtube.source import fetch_youtube_events

TOPIC = "btc_yt"


def run_forever(interval_seconds: int = 180):
    producer = CryptoProducer()
    while True:
        events = fetch_youtube_events()
        for event in events:
            producer.send_message(TOPIC, event)
        print(f"Published {len(events)} youtube events")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    run_forever()
