# docker/kafka/entrypoint.sh

#!/bin/bash

echo "Waiting for Kafka..."

until /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --list; do
  sleep 2
done

echo "Creating topic..."

/opt/kafka/bin/kafka-topics.sh \
  --create --if-not-exists \
  --topic crypto_prices \
  --bootstrap-server kafka:9092 \
  --partitions 3 \
  --replication-factor 1

echo "Done!"