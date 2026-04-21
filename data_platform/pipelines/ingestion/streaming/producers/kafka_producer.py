# pipelines/ingestion/streaming/producer/kafka_producer.py

import os

from kafka import KafkaProducer
import json
from utils_global.config_loader import load_config


class CryptoProducer:
    def __init__(self, config_path="configs/kafka.yaml"):
        self.config = load_config(config_path)

        env = os.getenv("ENV", "host")
        brokers = self.config["brokers"][env]

        self.producer = KafkaProducer(
            bootstrap_servers=brokers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=5,
            linger_ms=100,
            batch_size=16384,
            request_timeout_ms=15000,
        )

    def send_message(self, topic: str, data: dict):
        """Send a message; returns the RecordMetadata future result."""
        future = self.producer.send(topic, data)
        try:
            return future.get(timeout=15)
        except Exception as e:
            raise e

    def flush(self, timeout: int = 30):
        """Flush all pending messages and block until they are acknowledged."""
        self.producer.flush(timeout=timeout)

    def close(self):
        """Flush and close the underlying KafkaProducer."""
        self.producer.flush(timeout=30)
        self.producer.close()

    def send_price(self, data):
        return self.send_message(self.config.get("crypto_topic", "crypto_prices"), data)