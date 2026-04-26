# pipelines/ingestion/streaming/producer/kafka_producer.py

import os

from kafka import KafkaProducer
import json
from pipelines.utils.config_loader import load_config

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
            linger_ms=5000,
            batch_size=16384
        )

    def send_price(self, data):
        future = self.producer.send(self.config.get("crypto_topic", "crypto_prices"), data)

        try:
            record_metadata = future.get(timeout=10)
        except Exception as e:
            # raise
            raise e