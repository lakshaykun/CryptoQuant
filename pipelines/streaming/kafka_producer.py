from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

i = 1
while True:
    data = {
        "symbol": "BTC",
        "price": random.randint(60000, 70000),
        "iteration": i
    }
    producer.send('crypto_prices', data)
    print("Sent:", data)
    time.sleep(2)
    i += 1