from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    bootstrap_servers='localhost:9092',
    group_id='dev-group-4',
    auto_offset_reset='latest',
    api_version=(3, 7, 0),
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
)

consumer.subscribe(['crypto_prices'])

print("Assigned partitions:", consumer.assignment())

consumer.poll(1.0)

print("Assigned partitions after poll:", consumer.assignment())

i = 1
for msg in consumer:
    print("RAW:", msg.value)  # debug

    data = msg.value
    prediction = data["price"] * 1.01
    iteration = data.get("iteration", 0)

    print(f"{i}th Prediction for {data}: {prediction}")
    i += 1