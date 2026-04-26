import argparse
import time
from datetime import datetime, timezone

from kafka.errors import KafkaError
from pipelines.ingestion.streaming.producers.kafka_producer import CryptoProducer
from pipelines.ingestion.streaming.sentiment.delta_sink import write_events_to_bronze_delta
from pipelines.ingestion.streaming.sources.sentiment.telegram.source import (
    fetch_telegram_events,
)
from utils.config_loader import load_config

TOPIC = "btc_telegram"


def _streaming_lookback_minutes(default_minutes: int = 30) -> int:
    config = load_config("configs/data.yaml") or {}
    sentiment_cfg = config.get("sentiment", {}) if isinstance(config, dict) else {}
    try:
        return max(1, int(sentiment_cfg.get("streaming_lookback_minutes", default_minutes)))
    except (TypeError, ValueError):
        return default_minutes


def _dedupe_events(events: list[dict]) -> list[dict]:
    unique: dict[str, dict] = {}
    for event in events:
        event_id = str(event.get("id", "")).strip()
        if not event_id:
            continue
        unique[event_id] = event
    return list(unique.values())


def run_once() -> int:
    events = _dedupe_events(
        fetch_telegram_events(lookback_minutes=_streaming_lookback_minutes())
    )
    kafka_published = 0
    try:
        producer = CryptoProducer()
        for event in events:
            producer.send_message(TOPIC, event)
        producer.flush()
        producer.close()
        kafka_published = len(events)
    except KafkaError:
        kafka_published = 0
    except Exception:
        kafka_published = 0

    delta_written = write_events_to_bronze_delta(events)
    print(
        f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC] "
        f"telegram: fetched={len(events)} kafka={kafka_published} delta={delta_written}"
    )
    return len(events)


def run_forever(interval_seconds: int | None = None, stats_queue=None):
    interval = interval_seconds if interval_seconds is not None else 60
    while True:
        try:
            count = run_once()
        except Exception:
            count = 0
        if stats_queue is not None:
            stats_queue.put(("telegram", count, int(time.time())))
        time.sleep(interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Telegram sentiment Kafka producer")
    parser.add_argument("--once", action="store_true", help="Fetch and publish one cycle, then exit")
    args = parser.parse_args()

    if args.once:
        run_once()
    else:
        run_forever()
