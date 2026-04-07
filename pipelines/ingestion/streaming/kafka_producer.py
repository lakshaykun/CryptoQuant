from kafka import KafkaProducer
import json

class CryptoProducer:
    def __init__(self, brokers="localhost:9092"):
        self.producer = KafkaProducer(
            bootstrap_servers=brokers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    def send(self, topic, data):
        self.producer.send(topic, data)
        self.producer.flush()