# pipelines/ingestion/streaming/producer/kafka_producer.py

from kafka import KafkaProducer
import json
from utils_global.config_loader import load_config

class CryptoProducer:
    def __init__(self, config_path="configs/kafka.yaml"):
        self.config = load_config(config_path)
        
        brokers = self.config["brokers"]

        self.producer = KafkaProducer(
            bootstrap_servers=brokers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            
            acks="all",
            retries=10,
            linger_ms=20,
            batch_size=32768,
            
            enable_idempotence=True,
            max_in_flight_requests_per_connection=1
        )

    def send_price(self, data):
        self.producer.send(self.config.get("crypto_topic", "crypto_prices"), data)