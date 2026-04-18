#!/usr/bin/env sh
set -eu

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVERS:-crypto-kafka:29092}"

for topic in crypto_prices btc_reddit btc_yt btc_news; do
  /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
    --topic "$topic" \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --partitions 3 \
    --replication-factor 1

done

echo "Kafka topics initialized on ${BOOTSTRAP_SERVER}"
