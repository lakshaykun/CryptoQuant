#!/bin/sh

set -eu

# In Docker Compose, kafka_bootstrap should use the internal Kafka listener.
BROKER="${KAFKA_BOOTSTRAP_SERVERS:-kafka:19092}"
TOPICS="btc_reddit btc_yt btc_news"

if command -v kafka-topics >/dev/null 2>&1; then
	KAFKA_TOPICS_CMD="kafka-topics"
elif command -v kafka-topics.sh >/dev/null 2>&1; then
	KAFKA_TOPICS_CMD="kafka-topics.sh"
elif [ -x /opt/kafka/bin/kafka-topics.sh ]; then
	KAFKA_TOPICS_CMD="/opt/kafka/bin/kafka-topics.sh"
else
	echo "Kafka topics CLI not found (tried kafka-topics, kafka-topics.sh, /opt/kafka/bin/kafka-topics.sh)." >&2
	exit 127
fi

for topic in $TOPICS; do
	"$KAFKA_TOPICS_CMD" --bootstrap-server "$BROKER" \
		--create \
		--if-not-exists \
		--topic "$topic" \
		--partitions 3 \
		--replication-factor 1
done

echo "Kafka topics ensured: $TOPICS"
