# pipelines/ingestion/streaming/producer/kafka_producer.py

from kafka import KafkaProducer
import json
from pipelines.utils.config_loader import load_config

class CryptoProducer:
    def __init__(self, config_path="configs/kafka.yaml"):
        self.config = load_config(config_path)
        brokers = self.config.get("brokers", "localhost:9092")

        self.producer = KafkaProducer(
            bootstrap_servers=brokers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=5,
            linger_ms=10,
            batch_size=16384,
            compression_type="snappy"
        )

    def send_price(self, data):
        future = self.producer.send(self.config.get("crypto_topic", "crypto_prices"), data)

        try:
            record_metadata = future.get(timeout=10)
        except Exception as e:
            # log + retry
            raise e